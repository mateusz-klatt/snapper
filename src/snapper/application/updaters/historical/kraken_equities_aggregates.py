"""Kraken Equities (TradFi FCM futures) OHLCV candle backfill service.

Downloads historical candle data from the internal
``iapi.kraken.com/api/internal/markets/{ws_symbol}/ticker/history``
endpoint via ``KrakenEquitiesExchangeClient.get_ohlcv()`` and persists
to the ``candles`` table. Market-data-only — no order placement.

Modelled on ``KrakenFuturesAggregatesBackfillService``. Differences:

- Uses native-to-WS symbol conversion (``MNQM6-CME`` → ``MNQM6.CME``)
  rather than CCXT-style conversion; performed inside ``get_ohlcv``.
- Source endpoint is undocumented / internal — per-chunk failures
  propagate as ``RuntimeError`` (see ``get_ohlcv`` raise contract) so
  the operator can distinguish upstream outage from empty windows.
- Default ``days_back=30`` reflects the bounded server window on the
  iapi ticker/history endpoint.
"""

import asyncio
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any

from loguru import logger

from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.process_parameters import KrakenEquitiesBackfillParameters
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
from snapper.infrastructure.exchanges.contracts import OhlcvSnapshot
from snapper.infrastructure.exchanges.implementations.kraken_equities import (
    KrakenEquitiesExchangeClient,
)
from snapper.infrastructure.exchanges.implementations.kraken_equities import _timeframe_to_interval
from snapper.infrastructure.symbols.functions import get_available_kraken_equities_symbols
from snapper.infrastructure.symbols.functions import resolve_symbol_public_id
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.utils.logging import set_log_context

_RATE_LIMIT_DELAY = 1.0


@register_process(
    "kraken_equities_aggregates_backfill",
    method="start",
    description="Kraken Equities (TradFi FCM) OHLCV backfill",
    priority=25,
    lifecycle=ProcessLifecycleEnum.ONE_SHOT,
    role=ProcessRoleEnum.TASK,
    tags=("kraken_equities", "backfill", "historical", "maintenance"),
    parameters_model=KrakenEquitiesBackfillParameters,
    enabled=False,
    mode=ProcessModeEnum.THREAD,
)
class KrakenEquitiesAggregatesBackfillService(RegisterableProcess):
    """Backfill historical OHLCV candles for Kraken Equities instruments.

    Downloads candle data via ``KrakenEquitiesExchangeClient.get_ohlcv()``
    and persists to the database in batches. Supports resume from the
    latest stored candle timestamp.

    Attributes:
        BATCH_COMMIT_SIZE: Maximum rows per ``upsert_candles`` call.
    """

    BATCH_COMMIT_SIZE: int = 500

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Get default parameters from settings.

        Args:
            settings: Application settings.

        Returns:
            Default parameters with symbols, timeframe, days_back.
        """
        return {
            "symbols": settings.instruments.get(ExchangeEnum.KRAKEN_EQUITIES, []),
            "all_symbols": False,
            "timeframe": "1h",
            "days_back": 30,
            "resume": True,
        }

    def __init__(
        self,
        symbols: list[str] | None = None,
        all_symbols: bool = False,
        timeframe: str = "1h",
        days_back: int = 30,
        resume: bool = True,
    ) -> None:
        """Initialize the backfill service.

        Args:
            symbols: Native symbols to backfill. None uses settings default.
            all_symbols: If True, backfill all Kraken Equities symbols.
            timeframe: Candle interval string accepted by
                ``KrakenEquitiesExchangeClient.get_ohlcv``.
            days_back: Number of days to backfill from today.
            resume: Whether to skip already-fetched candles.

        Raises:
            ValueError: When ``timeframe`` is not supported by the iapi
                endpoint (raised eagerly during construction via
                ``_timeframe_to_interval``).
        """
        _timeframe_to_interval(timeframe)
        self._requested_symbols = list(symbols) if symbols else []
        self._all_symbols = all_symbols
        self._timeframe = timeframe
        self._days_back = days_back
        self._resume = resume
        self.settings = get_settings()
        self._db: Repository | None = None
        self._instrument_cache: dict[str, str] = {}
        self._tracker: SequenceTracker = SequenceTracker()

    async def start(self) -> None:
        """Run the backfill process.

        Resolves symbols, connects to exchange, processes each symbol
        sequentially with rate limiting between requests.
        """
        set_log_context("bf:kq_agg")
        self._db = get_repository(self.settings.db_url)
        symbols = self._resolve_symbols()
        if not symbols:
            logger.warning("No Kraken Equities symbols configured for backfill")
            return
        client = KrakenEquitiesExchangeClient()
        await client.connect()
        try:
            for symbol in symbols:
                await self._process_symbol(client, symbol)
        finally:
            await client.disconnect()

    def _resolve_symbols(self) -> list[str]:
        """Determine which symbols to backfill.

        Wildcard ``["*"]`` in ``settings.instruments[KRAKEN_EQUITIES]``
        expands the same way as the ``--all`` CLI flag — every
        currently-mapped Kraken Equities symbol via
        ``get_available_kraken_equities_symbols()``. This mirrors the
        publisher's ``_validate_symbols`` wildcard behavior so the
        single sentinel ``["*"]`` consistently means "all venues"
        regardless of which consumer reads it.

        Returns:
            List of native symbols to process.
        """
        if self._all_symbols:
            symbols = get_available_kraken_equities_symbols()
            logger.info(f"Resolved {len(symbols)} Kraken Equities symbols for backfill")
            return symbols
        if self._requested_symbols:
            return self._requested_symbols
        configured = self.settings.instruments.get(ExchangeEnum.KRAKEN_EQUITIES, [])
        if configured == ["*"]:
            symbols = get_available_kraken_equities_symbols()
            logger.info(f"Resolved {len(symbols)} Kraken Equities symbols from wildcard settings")
            return symbols
        return configured

    async def _ensure_instrument(self, native_symbol: str) -> str | None:
        """Ensure instrument exists in database, return its public_id.

        Args:
            native_symbol: Native symbol (e.g., ``MNQM6-CME``).

        Returns:
            The instrument_public_id, or None if symbol not in database.
        """
        assert self._db is not None
        if native_symbol in self._instrument_cache:
            return self._instrument_cache[native_symbol]
        now = datetime.now(UTC)
        symbol_pid = await resolve_symbol_public_id(self._db, native_symbol, as_of=now)
        if symbol_pid is None:
            logger.warning(f"No active Symbol row for {native_symbol}, skipping")
            return None
        _id, instrument_public_id = await self._db.ensure_instrument(
            symbol_public_id=symbol_pid,
            exchange=ExchangeEnum.KRAKEN_EQUITIES,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence("instruments"),
            timestamp=now,
        )
        self._instrument_cache[native_symbol] = instrument_public_id
        return instrument_public_id

    async def _get_resume_since(self, instrument_public_id: str) -> datetime | None:
        """Get the latest candle timestamp for resume.

        Args:
            instrument_public_id: Instrument to check.

        Returns:
            Latest candle open_at timestamp, or None if no candles stored.
        """
        assert self._db is not None
        candles = await self._db.get_candles(
            instrument=instrument_public_id,
            timeframe=self._timeframe,
            start=None,
            end=None,
            exchange=ExchangeEnum.KRAKEN_EQUITIES,
            as_of=datetime.now(UTC),
            limit=1,
            order="desc",
        )
        if not candles:
            return None
        latest = max(c["open_at"] for c in candles)
        return latest

    async def _process_symbol(
        self, client: KrakenEquitiesExchangeClient, native_symbol: str
    ) -> None:
        """Backfill candles for a single symbol.

        Determines the resume cursor, fetches the full bounded window
        from the iapi endpoint in one call, and batch-upserts to the
        database. Propagates ``RuntimeError`` raised by ``get_ohlcv`` on
        upstream failure so the operator can distinguish outage from an
        empty window.

        Throttle semantics: ``_RATE_LIMIT_DELAY`` is always awaited once
        per symbol that reaches the ``get_ohlcv`` call — including on
        the empty-window early-return path and when ``get_ohlcv`` raises.
        This prevents a catch-up run over already-current symbols from
        hammering ``iapi`` back-to-back (the sleep lives inside
        ``try/finally``). Symbols that are skipped earlier in the flow
        (no Symbol row, no instrument row) never reach this point and
        therefore do not sleep.

        Args:
            client: Connected Kraken Equities exchange client.
            native_symbol: Native symbol (e.g., ``MNQM6-CME``).
        """
        assert self._db is not None
        instrument_pid = await self._ensure_instrument(native_symbol)
        if instrument_pid is None:
            return
        now = datetime.now(UTC)
        start_dt = now - timedelta(days=self._days_back)
        since_ms: int | None = int(start_dt.timestamp() * 1000)
        if self._resume:
            resume_ts = await self._get_resume_since(instrument_pid)
            if resume_ts is not None:
                since_ms = int(resume_ts.timestamp() * 1000) + 1
                logger.info(f"Resuming {native_symbol} from {resume_ts.isoformat()}")
        logger.info(
            f"Starting backfill for {native_symbol} ({self._timeframe}, {self._days_back} days)"
        )
        try:
            candles = await client.get_ohlcv(
                symbol=native_symbol,
                timeframe=self._timeframe,
                since=since_ms,
                limit=None,
            )
            if not candles:
                logger.info(f"No new candles returned for {native_symbol}")
                return
            rows = self._build_candle_rows(
                candles,
                instrument_pid,
                self._timeframe,
                self._tracker.session_id,
                lambda: self._tracker.next_sequence("candles"),
                now,
            )
            total_inserted = 0
            for i in range(0, len(rows), self.BATCH_COMMIT_SIZE):
                batch = rows[i : i + self.BATCH_COMMIT_SIZE]
                inserted = await self._db.upsert_candles(batch)
                total_inserted += inserted
            logger.info(
                f"Backfill complete for {native_symbol}: "
                f"{total_inserted} inserted from {len(candles)} fetched"
            )
        finally:
            await asyncio.sleep(_RATE_LIMIT_DELAY)

    @staticmethod
    def _build_candle_rows(
        candles: list[OhlcvSnapshot],
        instrument_public_id: str,
        timeframe: str,
        session_id: str,
        sequence_id_fn: Callable[[], int],
        bus_time: datetime,
    ) -> list[CandleUpsertRow]:
        """Convert OhlcvSnapshot list to CandleUpsertRow list.

        Args:
            candles: Raw OHLCV snapshots from exchange.
            instrument_public_id: Stable instrument identity.
            timeframe: Timeframe label string.
            session_id: Session identifier for provenance.
            sequence_id_fn: Callable returning next sequence number.
            bus_time: Wall-clock load time stamped as each row's
                ``timestamp`` (bus-time), kept distinct from ``open_at``
                (the bar event-time) so an SCD2 amend closes the prior
                version with a non-empty validity interval (no lookahead
                bias in as-of reads).

        Returns:
            List of row dicts ready for database upsert.
        """
        return [
            {
                "instrument_public_id": instrument_public_id,
                "open_at": datetime.fromtimestamp(candle.timestamp, tz=UTC),
                "timestamp": bus_time,
                "timeframe": timeframe,
                "open": candle.open,
                "high": candle.high,
                "low": candle.low,
                "close": candle.close,
                "volume": candle.volume,
                "vwap": None,
                "trades": None,
                "session_id": session_id,
                "sequence_id": sequence_id_fn(),
            }
            for candle in candles
        ]
