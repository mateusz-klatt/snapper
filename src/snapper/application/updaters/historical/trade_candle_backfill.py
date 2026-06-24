"""Calculated 1-minute candle backfill from persisted trades."""

from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from datetime import timedelta

from loguru import logger

from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.process_parameters import TradeCandleBackfillParameters
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.core.types import ExchangeEnum
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.data.repository import Repository
from snapper.data.repository import get_repository
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import TradeRow
from snapper.infrastructure.exchanges._trade_candle_builder import TradeCandleBuilder
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.utils.logging import set_log_context

_CANDLE_STREAM = "candles"
_ORD_TYPE = "market"
_SOURCE_CALCULATED = "calculated"
_TIMEFRAME = "1m"


def _utc_now() -> datetime:
    """Return the current UTC wall-clock timestamp.

    Returns:
        Timezone-aware UTC timestamp.
    """
    return datetime.now(UTC)


def _as_utc(value: datetime) -> datetime:
    """Normalize a datetime to timezone-aware UTC.

    Args:
        value: Naive or timezone-aware datetime.

    Returns:
        Timezone-aware datetime converted to UTC.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _floor_minute(value: datetime) -> datetime:
    """Floor a UTC-normalized datetime to a minute boundary.

    Args:
        value: Datetime to normalize and floor.

    Returns:
        UTC datetime with seconds and microseconds cleared.
    """
    return _as_utc(value).replace(second=0, microsecond=0)


def _dedupe_symbols(symbols: list[str]) -> list[str]:
    """Return symbols in first-seen order without duplicates.

    Args:
        symbols: Candidate native symbols.

    Returns:
        Deduplicated symbol list.
    """
    seen: set[str] = set()
    result: list[str] = []
    for symbol in symbols:
        if symbol not in seen:
            seen.add(symbol)
            result.append(symbol)
    return result


@register_process(
    "trade_candle_backfill",
    method="start",
    description="Calculated 1m candle backfill from persisted trades",
    priority=24,
    lifecycle=ProcessLifecycleEnum.ONE_SHOT,
    role=ProcessRoleEnum.TASK,
    tags=("trades", "candles", "backfill", "historical"),
    parameters_model=TradeCandleBackfillParameters,
    enabled=False,
    mode=ProcessModeEnum.THREAD,
)
class TradeCandleBackfillService(RegisterableProcess):
    """Backfill calculated 1-minute candles from trade rows.

    Trades are streamed per instrument in event-time order and folded
    through :class:`TradeCandleBuilder`. During the stream, completed
    event minutes are flushed as soon as a later minute is observed. At
    the end of each instrument, the final flush uses the floored window
    end as the completion boundary, so the trailing minute is emitted
    only when the requested ``end`` has moved to a later minute.

    Attributes:
        BATCH_COMMIT_SIZE: Maximum rows per ``upsert_candles`` call.
    """

    BATCH_COMMIT_SIZE: int = 500

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, object]:
        """Get conservative process defaults.

        Args:
            settings: Application settings.

        Returns:
            Default one-hour Kraken all-symbol backfill parameters.
        """
        end = _floor_minute(datetime.now(UTC))
        start = end - timedelta(hours=1)
        return {
            "exchange": ExchangeEnum.KRAKEN,
            "symbols": settings.instruments.get(ExchangeEnum.KRAKEN, []),
            "all_symbols": True,
            "start": start.isoformat(),
            "end": end.isoformat(),
        }

    def __init__(
        self,
        exchange: ExchangeEnum | str,
        start: datetime,
        end: datetime,
        symbols: list[str] | None = None,
        all_symbols: bool = False,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        """Initialize the trade candle backfill service.

        Args:
            exchange: Exchange whose active instruments should be read.
            start: Inclusive UTC event-time lower bound.
            end: Inclusive UTC event-time upper bound.
            symbols: Optional native symbols to backfill.
            all_symbols: If True, backfill every active instrument on
                the exchange.
            clock: Wall-clock provider used for as-of and bus-time
                provenance.

        Raises:
            ValueError: When the exchange, window, or symbol selection is
                invalid.
        """
        self._exchange = ExchangeEnum(exchange)
        self._start = _as_utc(start)
        self._end = _as_utc(end)
        if self._start >= self._end:
            raise ValueError("start must be before end")
        self._requested_symbols = _dedupe_symbols(list(symbols) if symbols else [])
        self._all_symbols = all_symbols
        if not self._all_symbols and not self._requested_symbols:
            raise ValueError("pass all_symbols=True or at least one symbol")
        self._clock = clock
        self.settings = get_settings()
        self._db: Repository | None = None
        self._tracker: SequenceTracker = SequenceTracker()

    async def start(self) -> None:
        """Run the backfill process."""
        set_log_context("bf:trade_candles")
        self._db = get_repository(self.settings.db_url)
        as_of = _as_utc(self._clock())
        instruments = await self._resolve_instruments(as_of)
        if not instruments:
            logger.warning(f"No active instruments matched for {self._exchange.value}")
            return
        await self._process_exchange_stream(instruments, as_of)

    async def _resolve_instruments(self, as_of: datetime) -> list[tuple[str, str]]:
        """Resolve selected native symbols to active instrument IDs.

        Args:
            as_of: Point-in-time read timestamp.

        Returns:
            Pairs of native symbol and instrument public ID.

        Raises:
            ValueError: When a requested symbol is not active on the exchange.
        """
        assert self._db is not None
        active_symbols = await self._db.get_exchange_instruments(self._exchange.value, as_of)
        if self._all_symbols:
            selected_symbols = active_symbols
        else:
            active_set = set(active_symbols)
            missing = [symbol for symbol in self._requested_symbols if symbol not in active_set]
            if missing:
                raise ValueError(
                    f"symbols are not active on {self._exchange.value}: {', '.join(missing)}"
                )
            selected_symbols = self._requested_symbols
        selected_symbols = _dedupe_symbols(selected_symbols)
        if not selected_symbols:
            return []
        instrument_ids = await self._db.get_instrument_public_ids_by_symbols(
            set(selected_symbols), self._exchange.value, as_of
        )
        unresolved = [symbol for symbol in selected_symbols if symbol not in instrument_ids]
        if unresolved:
            logger.warning(
                f"Skipping symbols without active instruments on {self._exchange.value}: "
                f"{', '.join(unresolved)}"
            )
        return [
            (symbol, instrument_ids[symbol])
            for symbol in selected_symbols
            if symbol in instrument_ids
        ]

    async def _process_exchange_stream(
        self,
        instruments: list[tuple[str, str]],
        as_of: datetime,
    ) -> None:
        """Backfill selected instruments from one grouped trade stream.

        Args:
            instruments: Pairs of native symbol and instrument public ID.
            as_of: Point-in-time read timestamp.
        """
        assert self._db is not None
        instrument_public_ids = [instrument_public_id for _, instrument_public_id in instruments]
        symbol_by_instrument_public_id = {
            instrument_public_id: native_symbol
            for native_symbol, instrument_public_id in instruments
        }
        seen_instrument_public_ids: set[str] = set()
        current_instrument_public_id: str | None = None
        builder: TradeCandleBuilder | None = None
        batch: list[CandleUpsertRow] = []
        current_trades = 0
        current_rows = 0
        current_changed = 0
        async for instrument_public_id, trade in self._db.iter_exchange_trades(
            exchange=self._exchange.value,
            start=self._start,
            end=self._end,
            as_of=as_of,
            instrument_public_ids=instrument_public_ids,
        ):
            if current_instrument_public_id != instrument_public_id:
                if current_instrument_public_id is not None and builder is not None:
                    changed, rows = await self._finish_instrument(
                        builder, current_instrument_public_id, batch
                    )
                    current_changed += changed
                    current_rows += rows
                    self._log_instrument_complete(
                        symbol_by_instrument_public_id[current_instrument_public_id],
                        current_changed,
                        current_rows,
                        current_trades,
                    )
                current_instrument_public_id = instrument_public_id
                seen_instrument_public_ids.add(instrument_public_id)
                builder = TradeCandleBuilder(interval_seconds=60)
                current_trades = 0
                current_rows = 0
                current_changed = 0
            event_time = self._trade_event_time(trade)
            assert builder is not None
            builder.update(self._build_trade_update(trade, instrument_public_id, event_time))
            current_trades += 1
            changed, rows = await self._append_completed_candles(
                builder.pop_completed(event_time), instrument_public_id, batch
            )
            current_changed += changed
            current_rows += rows
        if current_instrument_public_id is not None and builder is not None:
            changed, rows = await self._finish_instrument(
                builder, current_instrument_public_id, batch
            )
            current_changed += changed
            current_rows += rows
            self._log_instrument_complete(
                symbol_by_instrument_public_id[current_instrument_public_id],
                current_changed,
                current_rows,
                current_trades,
            )
        for native_symbol, instrument_public_id in instruments:
            if instrument_public_id not in seen_instrument_public_ids:
                self._log_instrument_complete(native_symbol, 0, 0, 0)

    async def _finish_instrument(
        self,
        builder: TradeCandleBuilder,
        instrument_public_id: str,
        batch: list[CandleUpsertRow],
    ) -> tuple[int, int]:
        """Flush the final completed candles for one streamed instrument.

        Args:
            builder: Builder containing the current instrument buckets.
            instrument_public_id: Instrument ID used by candle rows.
            batch: Mutable pending upsert batch.

        Returns:
            Pair of changed-row count and appended-row count.
        """
        changed, rows = await self._append_completed_candles(
            builder.pop_completed(_floor_minute(self._end)), instrument_public_id, batch
        )
        changed += await self._flush_batch(batch)
        return changed, rows

    @staticmethod
    def _log_instrument_complete(
        native_symbol: str,
        total_changed: int,
        total_rows: int,
        total_trades: int,
    ) -> None:
        """Log one instrument's backfill summary.

        Args:
            native_symbol: Native symbol represented by the completed stream.
            total_changed: Repository-reported changed candle rows.
            total_rows: Built candle rows before idempotent upsert comparison.
            total_trades: Folded trade count.
        """
        logger.info(
            f"Trade candle backfill complete for {native_symbol}: "
            f"{total_changed} changed from {total_rows} candles and {total_trades} trades"
        )

    def _build_trade_update(
        self,
        trade: TradeRow,
        instrument_public_id: str,
        event_time: datetime,
    ) -> TradeUpdate:
        """Convert a repository trade row into a builder trade update.

        Args:
            trade: Repository trade row.
            instrument_public_id: Instrument ID used as the builder's
                bucket key.
            event_time: Normalized event timestamp.

        Returns:
            TradeUpdate ready for TradeCandleBuilder.
        """
        return TradeUpdate(
            symbol=instrument_public_id,
            side=trade["side"],
            quantity=float(trade["size"]),
            price=float(trade["price"]),
            ord_type=_ORD_TYPE,
            timestamp=event_time,
            trade_id=trade["trade_id"],
        )

    @staticmethod
    def _trade_event_time(trade: TradeRow) -> datetime:
        """Return the normalized event timestamp for a trade row.

        Args:
            trade: Repository trade row.

        Returns:
            ``executed_at`` when present, otherwise ``timestamp``,
            normalized to UTC.
        """
        executed_at = trade["executed_at"]
        if executed_at is not None:
            return _as_utc(executed_at)
        return _as_utc(trade["timestamp"])

    async def _append_completed_candles(
        self,
        candles: list[CandleUpdate],
        instrument_public_id: str,
        batch: list[CandleUpsertRow],
    ) -> tuple[int, int]:
        """Append completed candles to the upsert batch and flush if full.

        Args:
            candles: Completed builder candles.
            instrument_public_id: Instrument ID for candle rows.
            batch: Mutable pending upsert batch.

        Returns:
            Pair of changed-row count and appended-row count.
        """
        changed = 0
        appended = 0
        for candle in sorted(candles, key=lambda item: item.interval_begin):
            batch.append(self._build_candle_row(candle, instrument_public_id))
            appended += 1
            if len(batch) >= self.BATCH_COMMIT_SIZE:
                changed += await self._flush_batch(batch)
        return changed, appended

    async def _flush_batch(self, batch: list[CandleUpsertRow]) -> int:
        """Flush a pending candle batch through the repository.

        Args:
            batch: Mutable pending upsert rows.

        Returns:
            Number of changed rows reported by the repository.
        """
        if not batch:
            return 0
        assert self._db is not None
        rows = list(batch)
        batch.clear()
        return await self._db.upsert_candles(rows)

    def _build_candle_row(
        self,
        candle: CandleUpdate,
        instrument_public_id: str,
    ) -> CandleUpsertRow:
        """Build a calculated 1-minute candle upsert row.

        Args:
            candle: Completed builder candle.
            instrument_public_id: Instrument ID for the persisted row.

        Returns:
            CandleUpsertRow ready for idempotent SCD2 upsert.
        """
        return {
            "instrument_public_id": instrument_public_id,
            "open_at": _as_utc(candle.interval_begin),
            "timestamp": _as_utc(self._clock()),
            "timeframe": _TIMEFRAME,
            "open": candle.open,
            "high": candle.high,
            "low": candle.low,
            "close": candle.close,
            "volume": candle.volume,
            "vwap": candle.vwap,
            "trades": candle.trades,
            "source": _SOURCE_CALCULATED,
            "complete": True,
            "session_id": self._tracker.session_id,
            "sequence_id": self._tracker.next_sequence(_CANDLE_STREAM),
        }
