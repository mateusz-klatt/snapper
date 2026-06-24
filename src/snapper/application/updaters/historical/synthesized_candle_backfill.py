"""Synthesized higher-timeframe candle backfill from persisted 1m candles."""

from collections.abc import Callable
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta

from loguru import logger

from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.process_parameters import (
    SynthesizedCandleBackfillParameters,
)
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.core.types import ExchangeEnum
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.data.repository import Repository
from snapper.data.repository import get_repository
from snapper.data.repository_types import CandleRow
from snapper.data.repository_types import CandleUpsertRow
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.publishers.candle_aggregator import SUPPORTED_SYNTHESIS_TIMEFRAMES
from snapper.messaging.publishers.candle_aggregator import CandleAggregator
from snapper.utils.logging import set_log_context

_CANDLE_STREAM = "candles"
_DEFAULT_TIMEFRAMES: tuple[str, ...] = ("5m", "15m", "30m", "1h", "4h", "1d")
_INPUT_TIMEFRAME = "1m"
_SOURCE_SYNTHESIZED = "synthesized"


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


def _cut_datetime(cut_date: date) -> datetime:
    """Return the UTC midnight ownership boundary for ``cut_date``.

    Args:
        cut_date: First UTC day synthesized ``1d`` rows may own.

    Returns:
        Timezone-aware UTC midnight.
    """
    return datetime.combine(cut_date, time.min, tzinfo=UTC)


def _dedupe_strings(values: list[str]) -> list[str]:
    """Return strings in first-seen order without duplicates.

    Args:
        values: Candidate strings.

    Returns:
        Deduplicated string list.
    """
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _normalize_timeframes(timeframes: list[str] | None) -> list[str]:
    """Normalize and validate requested synthesis timeframes.

    Args:
        timeframes: Requested timeframe labels.

    Returns:
        Deduplicated timeframe labels.

    Raises:
        ValueError: When no timeframe or an unsupported timeframe is requested.
    """
    requested = list(_DEFAULT_TIMEFRAMES) if timeframes is None else timeframes
    normalized = _dedupe_strings([item.strip() for item in requested if item.strip()])
    if not normalized:
        raise ValueError("at least one timeframe is required")
    unsupported = [tf for tf in normalized if tf not in SUPPORTED_SYNTHESIS_TIMEFRAMES]
    if unsupported:
        raise ValueError(f"unsupported synthesis timeframes: {', '.join(unsupported)}")
    return normalized


@register_process(
    "synthesized_candle_backfill",
    method="start",
    description="Synthesized higher-timeframe candle backfill from persisted 1m candles",
    priority=25,
    lifecycle=ProcessLifecycleEnum.ONE_SHOT,
    role=ProcessRoleEnum.TASK,
    tags=("candles", "backfill", "historical", "synthesis"),
    parameters_model=SynthesizedCandleBackfillParameters,
    enabled=False,
    mode=ProcessModeEnum.THREAD,
)
class SynthesizedCandleBackfillService(RegisterableProcess):
    """Backfill synthesized higher-timeframe candles from persisted 1m rows.

    The service streams each selected instrument's persisted 1m candle plane in
    ascending order, rebuilds higher-timeframe bars with
    :class:`CandleAggregator`, and upserts those bars with
    ``source='synthesized'``. The ``1d`` writer boundary is guarded by
    ``cut_date`` because the daily candle unique key does not include
    provenance.

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
            "start": start,
            "end": end,
            "timeframes": list(_DEFAULT_TIMEFRAMES),
            "cut_date": None,
        }

    def __init__(
        self,
        exchange: ExchangeEnum | str,
        start: datetime,
        end: datetime,
        symbols: list[str] | None = None,
        all_symbols: bool = False,
        timeframes: list[str] | None = None,
        cut_date: date | None = None,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        """Initialize the synthesized candle backfill service.

        Args:
            exchange: Exchange whose active instruments should be read.
            start: Inclusive UTC 1m candle lower bound.
            end: UTC upper bound used to seal closed higher-timeframe windows.
            symbols: Optional native symbols to backfill.
            all_symbols: If True, backfill every active instrument on the exchange.
            timeframes: Higher timeframe labels to synthesize.
            cut_date: First UTC day synthesized ``1d`` rows may own.
            clock: Wall-clock provider used for as-of and bus-time provenance.

        Raises:
            ValueError: When the exchange, window, symbol selection, timeframe
                set, or ``1d`` cut boundary is invalid.
        """
        self._exchange = ExchangeEnum(exchange)
        self._start = _as_utc(start)
        self._end = _as_utc(end)
        if self._start >= self._end:
            raise ValueError("start must be before end")
        self._requested_symbols = _dedupe_strings(list(symbols) if symbols else [])
        self._all_symbols = all_symbols
        if not self._all_symbols and not self._requested_symbols:
            raise ValueError("pass all_symbols=True or at least one symbol")
        self._timeframes = _normalize_timeframes(timeframes)
        if "1d" in self._timeframes and cut_date is None:
            raise ValueError("cut_date is required when 1d is requested")
        self._cut_dt = _cut_datetime(cut_date) if cut_date is not None else None
        self._clock = clock
        self.settings = get_settings()
        self._db: Repository | None = None
        self._tracker: SequenceTracker = SequenceTracker()

    async def start(self) -> None:
        """Run the backfill process."""
        set_log_context("bf:synth_candles")
        self._db = get_repository(self.settings.db_url)
        as_of = _as_utc(self._clock())
        instruments = await self._resolve_instruments(as_of)
        if not instruments:
            logger.warning(f"No active instruments matched for {self._exchange.value}")
            return
        for native_symbol, instrument_public_id in instruments:
            await self._process_instrument(native_symbol, instrument_public_id, as_of)

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
        selected_symbols = _dedupe_strings(selected_symbols)
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

    async def _process_instrument(
        self,
        native_symbol: str,
        instrument_public_id: str,
        as_of: datetime,
    ) -> None:
        """Backfill one instrument from its persisted 1m candle plane.

        Args:
            native_symbol: Native symbol used by repository candle reads.
            instrument_public_id: Active instrument public ID used by upserts.
            as_of: Point-in-time read timestamp.
        """
        assert self._db is not None
        aggregator = CandleAggregator(
            self._timeframes,
            live_epoch=self._start,
            forward_fill=True,
        )
        batch: list[CandleUpsertRow] = []
        observed_windows: set[tuple[str, datetime]] = set()
        total_1m = 0
        total_rows = 0
        total_changed = 0
        candles = await self._db.get_candles(
            instrument=native_symbol,
            timeframe=_INPUT_TIMEFRAME,
            start=self._start,
            end=self._end,
            exchange=self._exchange,
            as_of=as_of,
            order="asc",
        )
        for row in candles:
            candle = self._build_1m_update(row, native_symbol)
            self._record_observed_windows(aggregator, candle, observed_windows)
            total_1m += 1
            changed, rows = await self._append_emitted_candles(
                aggregator.fold(candle),
                instrument_public_id,
                observed_windows,
                batch,
            )
            total_changed += changed
            total_rows += rows
        changed, rows = await self._append_emitted_candles(
            aggregator.flush(self._end),
            instrument_public_id,
            observed_windows,
            batch,
        )
        total_changed += changed
        total_rows += rows
        total_changed += await self._flush_batch(batch)
        logger.info(
            f"Synthesized candle backfill complete for {native_symbol}: "
            f"{total_changed} changed from {total_rows} candles and {total_1m} 1m candles"
        )

    def _record_observed_windows(
        self,
        aggregator: CandleAggregator,
        candle: CandleUpdate,
        observed_windows: set[tuple[str, datetime]],
    ) -> None:
        """Record higher-timeframe windows backed by at least one 1m row.

        Args:
            aggregator: Aggregator used for canonical window calculation.
            candle: Persisted 1m candle being folded.
            observed_windows: Mutable set of observed higher-timeframe windows.
        """
        for timeframe in self._timeframes:
            window = aggregator.window_start(timeframe, candle.interval_begin)
            if window is not None:
                observed_windows.add((timeframe, window))

    def _build_1m_update(self, row: CandleRow, native_symbol: str) -> CandleUpdate:
        """Convert a repository 1m candle row into an aggregator input.

        Args:
            row: Persisted 1m candle row.
            native_symbol: Native symbol used as the aggregator key.

        Returns:
            A 1m CandleUpdate suitable for :meth:`CandleAggregator.fold`.
        """
        vwap = row["vwap"] if row["vwap"] is not None else row["close"]
        trades = row["trades"] if row["trades"] is not None else 0
        return CandleUpdate(
            symbol=native_symbol,
            open=row["open"],
            high=row["high"],
            low=row["low"],
            close=row["close"],
            vwap=vwap,
            trades=trades,
            volume=row["volume"],
            interval_begin=_as_utc(row["open_at"]),
            interval=60,
            complete=True,
        )

    async def _append_emitted_candles(
        self,
        emitted: list[tuple[str, CandleUpdate]],
        instrument_public_id: str,
        observed_windows: set[tuple[str, datetime]],
        batch: list[CandleUpsertRow],
    ) -> tuple[int, int]:
        """Append emitted synthesized candles to the upsert batch.

        Args:
            emitted: Aggregator-emitted higher-timeframe candles.
            instrument_public_id: Instrument ID for candle rows.
            observed_windows: Higher-timeframe windows backed by source 1m rows.
            batch: Mutable pending upsert batch.

        Returns:
            Pair of changed-row count and appended-row count.
        """
        changed = 0
        appended = 0
        ordered = sorted(emitted, key=lambda item: (item[1].interval_begin, item[0]))
        for timeframe, candle in ordered:
            if not self._should_upsert(timeframe, candle, observed_windows):
                continue
            batch.append(self._build_candle_row(timeframe, candle, instrument_public_id))
            appended += 1
            if len(batch) >= self.BATCH_COMMIT_SIZE:
                changed += await self._flush_batch(batch)
        return changed, appended

    def _should_upsert(
        self,
        timeframe: str,
        candle: CandleUpdate,
        observed_windows: set[tuple[str, datetime]],
    ) -> bool:
        """Return whether an emitted candle is inside this backfill's write range.

        Args:
            timeframe: Emitted higher-timeframe label.
            candle: Emitted synthesized candle.
            observed_windows: Higher-timeframe windows backed by source 1m rows.

        Returns:
            True when the emitted candle should be persisted.
        """
        open_at = _as_utc(candle.interval_begin)
        if open_at < self._start:
            return False
        if (timeframe, open_at) not in observed_windows:
            return False
        return not (timeframe == "1d" and self._cut_dt is not None and open_at < self._cut_dt)

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
        timeframe: str,
        candle: CandleUpdate,
        instrument_public_id: str,
    ) -> CandleUpsertRow:
        """Build a synthesized candle upsert row.

        Args:
            timeframe: Higher-timeframe label emitted by the aggregator.
            candle: Completed synthesized candle.
            instrument_public_id: Instrument ID for the persisted row.

        Returns:
            CandleUpsertRow ready for idempotent SCD2 upsert.
        """
        return {
            "instrument_public_id": instrument_public_id,
            "open_at": _as_utc(candle.interval_begin),
            "timestamp": _as_utc(self._clock()),
            "timeframe": timeframe,
            "open": candle.open,
            "high": candle.high,
            "low": candle.low,
            "close": candle.close,
            "volume": candle.volume,
            "vwap": candle.vwap,
            "trades": candle.trades,
            "source": _SOURCE_SYNTHESIZED,
            "complete": candle.complete,
            "session_id": self._tracker.session_id,
            "sequence_id": self._tracker.next_sequence(_CANDLE_STREAM),
        }
