"""Polygon CSV cache loader service module.

Loads already-downloaded Polygon aggregate candles from the on-disk CSV
cache into the database. This is the cache-only counterpart to
``PolygonAggregatesBackfillService``: it NEVER calls the Polygon API.

The service walks
``{cache_root}/{timespan}/{archive_symbol}/{year}/...`` cache files, maps
each ``archive_symbol`` directory back to an active instrument identity via
the database archive-symbol map, reads each CSV (skipping header-only
marker files), and batch-upserts the candle rows.

Symbols with no current instrument mapping are skipped with a warning so
a stale cache directory never crashes the load.
"""

from collections.abc import Sequence
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from pathlib import Path

from loguru import logger

from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.process_parameters import CsvLoadParameters
from snapper.application.process_manager.registry import register_process
from snapper.application.services.settings import get_settings_service
from snapper.application.updaters.historical.aggregates import _timeframe_label
from snapper.application.updaters.historical.candle_rows import build_candle_rows
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
from snapper.infrastructure.historical.polygon.loader import PolygonHistoricalLoader
from snapper.infrastructure.historical.polygon.loader import read_aggregate_csv
from snapper.infrastructure.symbols.mapper import SymbolMapperService
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.utils.logging import set_log_context

__all__ = ["PolygonCsvLoaderService"]
_CACHE_ROOT = Path("data/polygon/cache")


@register_process(
    "polygon_csv_load",
    method="start",
    description="Polygon CSV cache loader",
    priority=34,
    lifecycle=ProcessLifecycleEnum.ONE_SHOT,
    role=ProcessRoleEnum.TASK,
    tags=("polygon", "load", "historical"),
    parameters_model=CsvLoadParameters,
    enabled=False,
    mode=ProcessModeEnum.THREAD,
)
class PolygonCsvLoaderService(RegisterableProcess):
    """Service for loading cached Polygon CSV candles into the database.

    Reads the existing CSV cache produced by the aggregates backfill and
    upserts the candles into the ``candles`` table. Cache-only: it never
    contacts the Polygon API.

    Attributes:
        BATCH_COMMIT_SIZE: Maximum rows per ``upsert_candles`` call. Each
            call runs in its own DB transaction, so smaller values reduce
            SQLite write-lock hold time at the cost of more round-trips.
    """

    BATCH_COMMIT_SIZE: int = 500

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, object]:
        """Get default parameters from settings.

        Args:
            settings: Application settings.

        Returns:
            Default parameters for the constructor.
        """
        return {
            "symbols": settings.instruments.get(ExchangeEnum.POLYGON, []),
            "all_mapped": False,
            "timespan": "day",
            "since": None,
            "until": None,
        }

    def __init__(
        self,
        symbols: Sequence[str] | None = None,
        all_mapped: bool = False,
        timespan: str = "day",
        since: date | None = None,
        until: date | None = None,
    ) -> None:
        """Initialize the CSV loader service.

        Args:
            symbols: Specific symbols to load (native or Polygon format).
            all_mapped: If True, load every archive symbol present in cache.
            timespan: Timespan unit selecting the cache subtree.
            since: Earliest day to (re)load (inclusive). None means no lower
                bound.
            until: Latest day to (re)load (inclusive). None means no upper
                bound.
        """
        self._requested_symbols = list(symbols) if symbols is not None else None
        self._all_mapped = all_mapped
        self._timespan = timespan
        self._since = since
        self._until = until
        self.settings = get_settings()
        self._db_sync: DatabaseRepository | None = None
        self._db_async: Repository | None = None
        self._loader: PolygonHistoricalLoader | None = None
        self._archive_symbols: dict[str, str] = {}
        self._symbol_mapper = SymbolMapperService.get_instance()
        self._tracker: SequenceTracker = SequenceTracker()

    async def start(self) -> None:
        """Start the CSV cache load process.

        Initializes database connections, resolves the archive-symbol map,
        and loads cached candles for each selected archive symbol.
        """
        set_log_context("ld:poly_csv")
        try:
            settings_service = await get_settings_service(
                self.settings.db_url,
                self.settings.zmq_broker_xsub,
            )
            self.settings = get_settings_with_service(settings_service)
            self._db_sync = DatabaseRepository(self.settings.db_url)
            self._db_async = get_repository(self.settings.db_url)
            self._archive_symbols = self._db_sync.get_archive_symbols()
            self._loader = PolygonHistoricalLoader(None, cache_root=_CACHE_ROOT)
            self._symbol_mapper.load_cache_if_needed()
            targets = self._resolve_targets()
            if not targets:
                logger.warning("No archive symbols selected for CSV load")
                return
            for archive_symbol, symbol_public_id in targets:
                await self._load_archive_symbol(archive_symbol, symbol_public_id)
        finally:
            await self._dispose_resources()

    async def _dispose_resources(self) -> None:
        """Dispose repositories allocated during service startup."""
        async_repo = self._db_async
        sync_repo = self._db_sync
        self._db_async = None
        self._db_sync = None
        self._loader = None
        engine = getattr(async_repo, "engine", None)
        if engine is not None:
            await engine.dispose()
        sync_dispose = getattr(sync_repo, "dispose", None)
        if callable(sync_dispose):
            sync_dispose()

    def _resolve_targets(self) -> list[tuple[str, str]]:
        """Resolve the archive symbols to load with their symbol public IDs.

        Selection mirrors :class:`PolygonAggregatesBackfillService`:

        - ``--all`` (``all_mapped``) enumerates every archive symbol present
          in the cache subtree for the timespan.
        - Otherwise the requested ``--symbol`` values are used, defaulting
          to the settings-configured Polygon instruments when no symbol was
          passed. The wildcard sentinel ``["*"]`` from settings is treated
          as ``--all`` so a wildcard-configured deployment loads the whole
          cache, while an explicit settings list resolves only those
          symbols. This keeps the documented "from settings" default
          accurate instead of silently loading every cached directory.

        Returns:
            List of ``(archive_symbol, symbol_public_id)`` pairs. Archive
            symbols without a current instrument mapping are skipped with a
            warning.
        """
        reverse = {archive: pid for pid, archive in self._archive_symbols.items()}
        if self._all_mapped:
            return self._resolve_cache_targets(reverse)
        symbols = self._requested_symbols
        if symbols is None:
            symbols = list(self.settings.instruments.get(ExchangeEnum.POLYGON, []))
        if symbols == ["*"]:
            logger.info("Wildcard Polygon settings: loading every cached archive symbol")
            return self._resolve_cache_targets(reverse)
        if not symbols:
            logger.warning("No Polygon symbols configured for CSV load")
            return []
        return self._resolve_requested_targets(reverse, symbols)

    def _resolve_cache_targets(self, reverse: dict[str, str]) -> list[tuple[str, str]]:
        """Resolve every archive symbol present in the cache subtree.

        Args:
            reverse: ``{archive_symbol: symbol_public_id}`` mapping.

        Returns:
            List of ``(archive_symbol, symbol_public_id)`` pairs for cache
            directories with a current instrument mapping.
        """
        subtree = _CACHE_ROOT / self._timespan
        if not subtree.exists():
            logger.warning(f"No cache subtree for timespan '{self._timespan}'", path=str(subtree))
            return []
        targets: list[tuple[str, str]] = []
        for entry in sorted(subtree.iterdir()):
            if not entry.is_dir():
                continue
            archive_symbol = entry.name
            symbol_public_id = reverse.get(archive_symbol)
            if symbol_public_id is None:
                logger.warning(
                    f"Skipping unmapped archive symbol '{archive_symbol}' "
                    "(no current Symbol identity)",
                    archive_symbol=archive_symbol,
                )
                continue
            targets.append((archive_symbol, symbol_public_id))
        return targets

    def _resolve_requested_targets(
        self, reverse: dict[str, str], symbols: Sequence[str]
    ) -> list[tuple[str, str]]:
        """Resolve explicitly requested symbols to archive targets.

        Args:
            reverse: ``{archive_symbol: symbol_public_id}`` mapping (unused
                for explicit symbols; archive symbols are derived from the
                active Symbol identity instead).
            symbols: Resolved symbol strings to load (explicit ``--symbol``
                values or the settings-configured Polygon instruments).

        Returns:
            List of ``(archive_symbol, symbol_public_id)`` pairs. Symbols
            without a current archive mapping are skipped with a warning.
        """
        _ = reverse
        targets: list[tuple[str, str]] = []
        for symbol in symbols:
            resolved = self._resolve_symbol_to_archive(symbol)
            if resolved is None:
                logger.warning(
                    f"Skipping symbol '{symbol}' with no current archive mapping",
                    symbol=symbol,
                )
                continue
            targets.append(resolved)
        return targets

    def _resolve_symbol_to_archive(self, symbol: str) -> tuple[str, str] | None:
        """Resolve a requested symbol string to an archive target.

        Accepts native symbols and Polygon-format symbols (``X:BTCUSD``).
        Uses the synchronous repository to resolve the native symbol to its
        active Symbol public_id, then looks up the archive symbol.

        Args:
            symbol: Symbol string (native or Polygon format).

        Returns:
            ``(archive_symbol, symbol_public_id)`` or None when no current
            mapping exists.
        """
        assert self._db_sync is not None
        native = symbol
        if ":" in symbol:
            mapped = self._symbol_mapper.polygon_rest_to_native.get(symbol)
            if mapped is None:
                return None
            native = mapped
        archive_symbol = self._db_sync.resolve_native_to_archive_symbol(native)
        if archive_symbol is None:
            return None
        symbol_public_id = self._archive_to_public_id(archive_symbol)
        if symbol_public_id is None:
            return None
        return archive_symbol, symbol_public_id

    def _archive_to_public_id(self, archive_symbol: str) -> str | None:
        """Reverse-resolve an archive symbol to its symbol public_id.

        Args:
            archive_symbol: Stable archive symbol.

        Returns:
            Symbol public_id or None when no mapping exists.
        """
        for public_id, archive in self._archive_symbols.items():
            if archive == archive_symbol:
                return public_id
        return None

    def _file_in_range(self, day: date) -> bool:
        """Return True when a cache file may hold candles within the window.

        ``day`` is the representative date a cache filename maps to (see
        :meth:`PolygonHistoricalLoader.iter_aggregate_csv_files`). For the
        ``day`` timespan a single monthly ``{YYYY}-{MM}.csv`` file is
        represented by the first of its month, so a file is selected when
        its whole month overlaps the ``[since, until]`` window rather than
        when that single representative day falls inside it.

        For sub-day timespans each file already represents an exact day, so
        the month-broadening is skipped and the representative day is tested
        directly against the window. This avoids reading and parsing whole
        months of per-day files only for :meth:`_candle_in_range` to drop
        the out-of-window rows. The per-candle boundary in
        :meth:`_candle_in_range` remains the correctness backstop for both
        granularities.

        Args:
            day: Representative day of a cache file.

        Returns:
            True when the file's coverage overlaps the bounds, else False.
        """
        if self._timespan.lower() != "day":
            return self._candle_in_range(day)
        month_start = day.replace(day=1)
        if day.month == 12:
            next_month_start = day.replace(year=day.year + 1, month=1, day=1)
        else:
            next_month_start = day.replace(month=day.month + 1, day=1)
        month_end = next_month_start - timedelta(days=1)
        if self._since is not None and month_end < self._since:
            return False
        return not (self._until is not None and month_start > self._until)

    def _candle_in_range(self, candle_day: date) -> bool:
        """Return True when a single candle's day is within the bounds.

        Applies the inclusive ``[since, until]`` day window to one candle,
        so candles outside the requested window are dropped even when their
        enclosing monthly file was selected by :meth:`_file_in_range`.

        Args:
            candle_day: Calendar day of an individual candle.

        Returns:
            True when within bounds (inclusive), else False.
        """
        if self._since is not None and candle_day < self._since:
            return False
        return not (self._until is not None and candle_day > self._until)

    async def _load_archive_symbol(self, archive_symbol: str, symbol_public_id: str) -> None:
        """Load all in-range cache files for a single archive symbol.

        Args:
            archive_symbol: Stable archive symbol for the cache directory.
            symbol_public_id: Active Symbol public_id for instrument resolution.
        """
        assert self._loader is not None
        assert self._db_async is not None
        timeframe = _timeframe_label(1, self._timespan)
        instrument_public_id = await self._ensure_instrument(symbol_public_id)
        load_time = datetime.now(UTC)
        files = self._loader.iter_aggregate_csv_files(archive_symbol, self._timespan)
        total_inserted = 0
        total_rows = 0
        for csv_path, day in files:
            if not self._file_in_range(day):
                continue
            candles = read_aggregate_csv(csv_path, archive_symbol)
            in_range = [c for c in candles if self._candle_in_range(c.timestamp.date())]
            if not in_range:
                continue
            rows = build_candle_rows(
                in_range,
                instrument_public_id,
                timeframe,
                self._tracker.session_id,
                lambda: self._tracker.next_sequence("candles"),
                load_time,
            )
            total_rows += len(rows)
            for i in range(0, len(rows), self.BATCH_COMMIT_SIZE):
                batch = rows[i : i + self.BATCH_COMMIT_SIZE]
                inserted = await self._db_async.upsert_candles(batch)
                total_inserted += inserted
        logger.info(
            f"Loaded {total_inserted}/{total_rows} candles for {archive_symbol} ({timeframe})",
            archive_symbol=archive_symbol,
            files=len(files),
        )

    async def _ensure_instrument(self, symbol_public_id: str) -> str:
        """Ensure an instrument exists for a symbol, return its public_id.

        Args:
            symbol_public_id: Active Symbol public_id.

        Returns:
            The instrument_public_id string.
        """
        assert self._db_async is not None
        ensure_time = datetime.now(UTC)
        _id, instrument_public_id = await self._db_async.ensure_instrument(
            symbol_public_id=symbol_public_id,
            exchange=ExchangeEnum.POLYGON,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence("instruments"),
            timestamp=ensure_time,
        )
        return instrument_public_id
