"""Polygon grouped-daily cache loader: persist 1d native candles.

Candle Phase 3 slice 3a (``proprietary/plans/plan_2026_06_16_candle_phase3_persistence.md``
§3.8/§4d/§4e). Loads the on-disk Polygon GROUPED-daily cache — the identical
corpus the A3 strategy warmup reads (``load_recent_grouped_daily``) — and upserts
each configured leg's days as ``1d, source='native', complete=True`` candles, so
the persisted plane holds the historical daily history the single-source read
cutover (slice 4) and the DB-first warmup (slice 5) depend on. Cache-only: it
NEVER calls the Polygon API.

Persisted under the live VENUE, not Polygon (§4e exchange-model audit). The
``exchange`` is a REQUIRED parameter naming the venue identity the persisted bars
must live under — the SAME exchange the leg's live read / DB-first warmup
resolves (e.g. ``kraken`` for FET/RENDER). Instrument identity is per
``(symbol, exchange)``, and live synthesized higher-TF bars persist under the
live publisher's ``_get_exchange_name()`` venue; a Polygon-keyed backfill would
be an ORPHANED plane no live read or warmup ever resolves. ``polygon`` here is
ONLY the CSV cache market segment that supplies the OHLCV — never the persisted
instrument identity (no Polygon streaming publisher exists).

Writer-ownership guard (slice-2 review finding F1): because the candle unique key
``(instrument_public_id, timeframe, open_at)`` excludes ``source``, once native
backfill and live synthesis share one ``(symbol, exchange)`` instrument a
native-backfilled ``1d`` row and a synthesized-live ``1d`` row sharing one
``open_at`` would SCD2 version-thrash (the no-op matcher compares ``source``;
1d boundaries align to 00:00 UTC on both writers). This loader confines the
backfill to historical days strictly BEFORE an explicit ``cut_date`` (the first
UTC day synthesized live persistence may own) and runs an ownership PREFLIGHT
(reading back UNDER THE SAME venue it writes) that fails closed if the persisted
plane already violates the disjoint-range invariant — so the native and
synthesized ``1d`` ranges are provably non-overlapping. (Note: the per-symbol
day-aggregate ``polygon-load-csv`` path also writes ``1d source='native'`` under
its own venue; keep its target disjoint from this one to avoid a native-vs-native
same-key overlap.)

The grouped crypto cache is keyed by the ``X:{BASE}{QUOTE}`` Polygon ticker (the
convention the warmup uses), which is distinct from ``native_to_polygon_rest``'s
``C:`` crypto product; the per-symbol transform here matches the cache layout.
"""

from collections.abc import Sequence
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta
from pathlib import Path

from loguru import logger

from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.process_parameters import GroupedCandleLoadParameters
from snapper.application.process_manager.registry import register_process
from snapper.application.services.settings import get_settings_service
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_with_service
from snapper.core.types import ExchangeEnum
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import Repository
from snapper.data.repository import get_repository
from snapper.data.repository_types import CandleUpsertRow
from snapper.infrastructure.historical.polygon.loader import GroupedDailyRow
from snapper.infrastructure.historical.polygon.loader import load_recent_grouped_daily
from snapper.infrastructure.symbols.functions import get_available_polygon_symbols
from snapper.infrastructure.symbols.mapper import SymbolMapperService
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.utils.logging import set_log_context

__all__ = ["OwnershipViolationError", "PolygonGroupedCandleLoaderService"]
_CACHE_ROOT = Path("data/polygon/cache")
_GROUPED_MARKET_TYPE = "crypto"
_TIMEFRAME_1D = "1d"
_LOAD_COUNT_CAP = 100_000


class OwnershipViolationError(RuntimeError):
    """Raised when the persisted plane violates the native/synthesized 1d split.

    The backfill owns ``1d`` days strictly before ``cut_date`` and live synthesis
    owns days at or after it. This is raised (fail closed) when the DB already
    holds a synthesized ``1d`` row before the cut or a native ``1d`` row at/after
    it, so the operator resolves the overlap before any backfill write lands.
    """


@register_process(
    "polygon_grouped_candle_load",
    method="start",
    description="Polygon grouped-daily 1d native candle loader",
    priority=35,
    lifecycle=ProcessLifecycleEnum.ONE_SHOT,
    role=ProcessRoleEnum.TASK,
    tags=("polygon", "load", "historical", "candles"),
    parameters_model=GroupedCandleLoadParameters,
    enabled=False,
    mode=ProcessModeEnum.THREAD,
)
class PolygonGroupedCandleLoaderService(RegisterableProcess):
    """Load cached Polygon grouped-daily rows into the candles table as 1d native.

    Reads the grouped-daily CSV cache and upserts each configured leg's days
    strictly before ``cut_date`` as ``source='native', complete=True`` ``1d``
    candles (idempotent SCD2). Cache-only: it never contacts the Polygon API.

    Attributes:
        BATCH_COMMIT_SIZE: Maximum rows per ``upsert_candles`` call. Each call
            runs in its own DB transaction, so smaller values reduce SQLite
            write-lock hold time at the cost of more round-trips.
    """

    BATCH_COMMIT_SIZE: int = 500

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, object]:
        """Get default parameters from settings.

        Args:
            settings: Application settings.

        Returns:
            Default parameters for the constructor. ``exchange`` and ``cut_date``
            default to None so a registry-spawned instance fails fast rather than
            guessing a venue/boundary; the CLI requires explicit values.
        """
        return {
            "symbols": settings.instruments.get(ExchangeEnum.POLYGON, []),
            "exchange": None,
            "cut_date": None,
            "all_mapped": False,
            "lookback_days": 800,
        }

    def __init__(
        self,
        symbols: Sequence[str] | None = None,
        exchange: ExchangeEnum | None = None,
        cut_date: date | None = None,
        all_mapped: bool = False,
        lookback_days: int = 800,
    ) -> None:
        """Initialize the grouped-daily candle loader.

        Args:
            symbols: Native symbols to load (defaults to settings Polygon list).
            exchange: Venue identity the persisted ``1d`` bars live under (the
                leg's live-read / warmup venue, e.g. ``kraken``). None makes
                :meth:`start` fail fast. Never ``polygon`` (cache corpus only).
            cut_date: First UTC day synthesized live persistence may own; the
                backfill writes only days strictly before it. None makes
                :meth:`start` fail fast.
            all_mapped: If True, load every Polygon-mapped native symbol.
            lookback_days: Calendar-day cap on the backward cache walk per symbol.
        """
        self._requested_symbols = list(symbols) if symbols is not None else None
        self._exchange = exchange
        self._cut_date = cut_date
        self._all_mapped = all_mapped
        self._lookback_days = lookback_days
        self.settings = get_settings()
        self._db_sync: DatabaseRepository | None = None
        self._db_async: Repository | None = None
        self._archive_symbols: dict[str, str] = {}
        self._symbol_mapper = SymbolMapperService.get_instance()
        self._tracker: SequenceTracker = SequenceTracker()

    async def start(self) -> None:
        """Start the grouped-daily candle load process.

        Initializes connections, resolves the target legs, runs the ownership
        preflight, and upserts each leg's pre-cut daily history.

        Raises:
            ValueError: When ``exchange`` or ``cut_date`` was not provided (no
                guessed default).
            OwnershipViolationError: When the persisted plane already violates
                the native/synthesized ``1d`` split for a target instrument.
        """
        set_log_context("ld:poly_grouped")
        if self._exchange is None:
            raise ValueError("exchange is required for the grouped-daily candle backfill")
        if self._cut_date is None:
            raise ValueError("cut_date is required for the grouped-daily candle backfill")
        try:
            settings_service = await get_settings_service(
                self.settings.db_url,
                self.settings.zmq_broker_xsub,
            )
            self.settings = get_settings_with_service(settings_service)
            self._db_sync = DatabaseRepository(self.settings.db_url)
            self._db_async = get_repository(self.settings.db_url)
            self._archive_symbols = self._db_sync.get_archive_symbols()
            self._symbol_mapper.load_cache_if_needed()
            targets = self._resolve_targets()
            if not targets:
                logger.warning("No Polygon symbols selected for grouped-daily candle load")
                return
            for native_symbol, symbol_public_id in targets:
                await self._load_symbol(native_symbol, symbol_public_id)
        finally:
            await self._dispose_resources()

    async def _dispose_resources(self) -> None:
        """Dispose repositories allocated during service startup."""
        async_repo = self._db_async
        sync_repo = self._db_sync
        self._db_async = None
        self._db_sync = None
        engine = getattr(async_repo, "engine", None)
        if engine is not None:
            await engine.dispose()
        sync_dispose = getattr(sync_repo, "dispose", None)
        if callable(sync_dispose):
            sync_dispose()

    def _resolve_targets(self) -> list[tuple[str, str]]:
        """Resolve target legs to ``(native_symbol, symbol_public_id)`` pairs.

        ``--all`` (or the settings wildcard ``["*"]``) enumerates every
        Polygon-mapped native symbol; otherwise the requested ``--symbol``
        values are used, defaulting to the settings-configured Polygon
        instruments. Symbols without an active Symbol identity are skipped with
        a warning so a stale config never crashes the load.

        Returns:
            List of ``(native_symbol, symbol_public_id)`` pairs.
        """
        symbols = self._requested_symbols
        if symbols is None:
            symbols = list(self.settings.instruments.get(ExchangeEnum.POLYGON, []))
        if self._all_mapped or symbols == ["*"]:
            symbols = get_available_polygon_symbols()
        if not symbols:
            logger.warning("No Polygon symbols configured for grouped-daily candle load")
            return []
        targets: list[tuple[str, str]] = []
        for native_symbol in symbols:
            symbol_public_id = self._resolve_symbol_public_id(native_symbol)
            if symbol_public_id is None:
                logger.warning(
                    f"Skipping symbol '{native_symbol}' with no active Symbol identity",
                    symbol=native_symbol,
                )
                continue
            targets.append((native_symbol, symbol_public_id))
        return targets

    def _resolve_symbol_public_id(self, native_symbol: str) -> str | None:
        """Resolve a native symbol to its active Symbol public_id.

        Args:
            native_symbol: Current native symbol (e.g. ``FET-USD``).

        Returns:
            Symbol public_id, or None when no active Symbol / archive mapping
            exists for the native symbol.
        """
        assert self._db_sync is not None
        archive_symbol = self._db_sync.resolve_native_to_archive_symbol(native_symbol)
        if archive_symbol is None:
            return None
        for public_id, archive in self._archive_symbols.items():
            if archive == archive_symbol:
                return public_id
        return None

    @staticmethod
    def _native_to_grouped_ticker(native_symbol: str) -> str | None:
        """Derive the grouped-daily crypto cache ticker for a native symbol.

        Pure transform (``FET-USD`` -> ``X:FETUSD``) matching the grouped crypto
        cache layout and the warmup's ticker convention.

        Args:
            native_symbol: Native ``BASE-QUOTE`` symbol.

        Returns:
            The ``X:{BASE}{QUOTE}`` ticker, or None when the symbol is not a
            single ``BASE-QUOTE`` pair.
        """
        parts = native_symbol.split("-")
        if len(parts) != 2 or not parts[0] or not parts[1]:
            return None
        return f"X:{parts[0].upper()}{parts[1].upper()}"

    def _cut_datetime(self) -> datetime:
        """Return ``cut_date`` as the UTC midnight boundary (first owned day)."""
        assert self._cut_date is not None
        return datetime.combine(self._cut_date, time.min, tzinfo=UTC)

    async def _load_symbol(self, native_symbol: str, symbol_public_id: str) -> None:
        """Load one leg's pre-cut grouped-daily history into the candles table.

        Args:
            native_symbol: Native symbol to load.
            symbol_public_id: Active Symbol public_id for instrument resolution.
        """
        assert self._db_async is not None
        ticker = self._native_to_grouped_ticker(native_symbol)
        if ticker is None:
            logger.warning(
                f"Skipping '{native_symbol}': not a single BASE-QUOTE crypto pair",
                symbol=native_symbol,
            )
            return
        instrument_public_id = await self._ensure_instrument(symbol_public_id)
        await self._ownership_preflight(native_symbol)
        as_of = self._cut_date - timedelta(days=1) if self._cut_date is not None else None
        assert as_of is not None
        grouped = load_recent_grouped_daily(
            _CACHE_ROOT,
            ticker,
            _LOAD_COUNT_CAP,
            as_of,
            market_type=_GROUPED_MARKET_TYPE,
            max_lookback_days=self._lookback_days,
        )
        rows = self._build_rows(grouped, instrument_public_id)
        if not rows:
            logger.info(f"No pre-cut grouped-daily rows cached for {native_symbol}", symbol=ticker)
            return
        total_inserted = 0
        for i in range(0, len(rows), self.BATCH_COMMIT_SIZE):
            batch = rows[i : i + self.BATCH_COMMIT_SIZE]
            total_inserted += await self._db_async.upsert_candles(batch)
        oldest = rows[0]["open_at"].date().isoformat()
        newest = rows[-1]["open_at"].date().isoformat()
        logger.info(
            f"Loaded {total_inserted}/{len(rows)} 1d native candles for {native_symbol} "
            f"spanning {oldest}..{newest} (lookback_days={self._lookback_days}; if the oldest "
            f"day is short of expected history, raise --lookback-days)",
            symbol=ticker,
            cut_date=self._cut_date.isoformat() if self._cut_date is not None else None,
        )

    async def _ownership_preflight(self, native_symbol: str) -> None:
        """Fail closed if the persisted 1d plane violates the native/synth split.

        Args:
            native_symbol: Native symbol whose active ``1d`` rows are checked.

        Raises:
            OwnershipViolationError: When a synthesized ``1d`` row exists before
                ``cut_date`` (backfill would invade synthesized territory) or a
                native ``1d`` row exists at/after it (a prior backfill invaded
                synthesis territory).
        """
        assert self._db_async is not None
        assert self._exchange is not None
        cut_dt = self._cut_datetime()
        existing = await self._db_async.get_candles(
            native_symbol,
            _TIMEFRAME_1D,
            None,
            None,
            self._exchange,
            datetime.now(UTC),
            order="asc",
        )
        for row in existing:
            open_at = row["open_at"]
            source = row.get("source", "native")
            if source == "synthesized" and open_at < cut_dt:
                raise OwnershipViolationError(
                    f"{native_symbol}: synthesized 1d row at {open_at.isoformat()} "
                    f"predates cut_date {self._cut_date}; resolve before backfilling"
                )
            if source == "native" and open_at >= cut_dt:
                raise OwnershipViolationError(
                    f"{native_symbol}: native 1d row at {open_at.isoformat()} "
                    f"is at/after cut_date {self._cut_date}; a prior backfill overran"
                )

    def _build_rows(
        self, grouped: list[GroupedDailyRow], instrument_public_id: str
    ) -> list[CandleUpsertRow]:
        """Build pre-cut ``1d`` native candle rows from grouped-daily rows.

        ``open_at`` is the row's ``closing_timestamp`` floored to UTC midnight,
        matching the synthesized live ``1d`` boundary so the persisted series is
        continuous. Days at or after ``cut_date`` are dropped (synthesis owns
        them) — the cut guard that keeps native and synthesized ranges disjoint.

        Args:
            grouped: Cached grouped-daily rows (ascending by closing time).
            instrument_public_id: Resolved instrument identity.

        Returns:
            ``CandleUpsertRow`` dicts tagged ``source='native', complete=True``.
        """
        cut_dt = self._cut_datetime()
        bus_time = datetime.now(UTC)
        rows: list[CandleUpsertRow] = []
        for row in grouped:
            open_at = row.closing_timestamp.astimezone(UTC).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            if open_at >= cut_dt:
                continue
            rows.append(
                {
                    "instrument_public_id": instrument_public_id,
                    "open_at": open_at,
                    "timestamp": bus_time,
                    "timeframe": _TIMEFRAME_1D,
                    "open": float(row.open),
                    "high": float(row.high),
                    "low": float(row.low),
                    "close": float(row.close),
                    "volume": float(row.volume),
                    "vwap": float(row.vwap) if row.vwap is not None else None,
                    "trades": row.total_trades,
                    "source": "native",
                    "complete": True,
                    "session_id": self._tracker.session_id,
                    "sequence_id": self._tracker.next_sequence("candles"),
                }
            )
        return rows

    async def _ensure_instrument(self, symbol_public_id: str) -> str:
        """Ensure an instrument exists for a symbol, return its public_id.

        Args:
            symbol_public_id: Active Symbol public_id.

        Returns:
            The instrument_public_id string.
        """
        assert self._db_async is not None
        assert self._exchange is not None
        _id, instrument_public_id = await self._db_async.ensure_instrument(
            symbol_public_id=symbol_public_id,
            exchange=self._exchange,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence("instruments"),
            timestamp=datetime.now(UTC),
        )
        return instrument_public_id
