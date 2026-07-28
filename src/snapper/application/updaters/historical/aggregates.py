"""Polygon aggregates backfill download service module.

This module provides batch downloading of historical OHLCV candle data
from Polygon.io API. It is download-only: it writes the on-disk CSV
cache and NEVER touches the database. Loading the cached CSVs into the
``candles`` table is a separate step handled by
``PolygonCsvLoaderService`` (the ``polygon-load-csv`` command).

It supports:
- Per-symbol download with configurable date ranges
- CSV caching to avoid re-downloading
- Resume capability for interrupted downloads via the filesystem cache
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any

from loguru import logger
from sqlalchemy import select

from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.process_parameters import AggregatesBackfillParameters
from snapper.application.process_manager.registry import register_process
from snapper.application.services.settings import get_settings_service
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_with_service
from snapper.core.json_types import JsonObject
from snapper.core.types import AliasChannelEnum
from snapper.core.types import ExchangeEnum
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.repository import DatabaseRepository
from snapper.infrastructure.exchanges.implementations.polygon import PolygonExchangeClient
from snapper.infrastructure.historical.polygon.loader import PolygonHistoricalLoader
from snapper.infrastructure.symbols.mapper import SymbolMapperService
from snapper.utils.logging import set_log_context

__all__ = ["PolygonAggregatesBackfillService", "_timeframe_label"]
_CACHE_ROOT = Path("data/polygon/cache")


@dataclass(slots=True)
class _SymbolContext:
    """Context for symbol resolution during backfill.

    Attributes:
        native_symbol: Internal normalized symbol (e.g., "BTC-USD").
        polygon_symbol: Polygon API symbol (e.g., "X:BTCUSD").
        base_currency: Base currency code.
        quote_currency: Quote currency code (optional).
        archive_symbol: Stable filesystem key for CSV cache paths.
        symbol_public_id: Stable public identity of the active Symbol row.
    """

    native_symbol: str
    polygon_symbol: str
    base_currency: str
    quote_currency: str | None
    archive_symbol: str
    symbol_public_id: str = ""


@dataclass(slots=True)
class _RunStats:
    """Observable aggregate-download outcomes for one invocation."""

    selected_symbols: int = 0
    fetched_symbols: int = 0
    symbols_with_data: int = 0
    symbols_without_data: int = 0
    skipped_symbols: int = 0

    def as_dict(self) -> JsonObject:
        """Return JSON-compatible counters for process run persistence."""
        return {
            "selected_symbols": self.selected_symbols,
            "fetched_symbols": self.fetched_symbols,
            "symbols_with_data": self.symbols_with_data,
            "symbols_without_data": self.symbols_without_data,
            "skipped_symbols": self.skipped_symbols,
        }


def _timeframe_label(multiplier: int, timespan: str) -> str:
    """Convert timeframe components to short label.

    Args:
        multiplier: Time multiplier (e.g., 1, 5, 15).
        timespan: Timespan string.

    Returns:
        Short label like "1m", "5m", "1h", "1d".
    """
    normalized = timespan.lower()
    match normalized:
        case "minute":
            suffix = "m"
        case "hour":
            suffix = "h"
        case "day":
            suffix = "d"
        case _:
            suffix = normalized[:1]
    return f"{multiplier}{suffix}"


@register_process(
    "polygon_aggregates_backfill",
    method="start",
    description="Polygon aggregates backfill",
    priority=32,
    lifecycle=ProcessLifecycleEnum.ONE_SHOT,
    role=ProcessRoleEnum.TASK,
    tags=("polygon", "backfill", "historical"),
    parameters_model=AggregatesBackfillParameters,
    enabled=False,
    mode=ProcessModeEnum.THREAD,
)
class PolygonAggregatesBackfillService(RegisterableProcess):
    """Service for downloading Polygon aggregate (OHLCV) data to CSV.

    Downloads historical candle data from Polygon.io API and writes it
    to the on-disk CSV cache only. It never touches the database;
    loading the cache into the ``candles`` table is the separate
    responsibility of ``PolygonCsvLoaderService``.

    Supports
    Configurable timeframes
    Resume from cached CSVs on disk
    All mapped symbols or specific symbol list
    Rate limiting and chunked requests
    Registered as one-shot task process.
    """

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Get default parameters from settings.

        Args:
            settings: Application settings.

        Returns:
            Default parameters including symbols, timeframe, days_back.
        """
        return {
            "symbols": settings.instruments.get(ExchangeEnum.POLYGON, []),
            "multiplier": 1,
            "timespan": "minute",
            "days_back": settings.backfill_days,
            "resume": True,
            "save_csv": True,
        }

    def __init__(
        self,
        symbols: Sequence[str] | None = None,
        all_mapped: bool = False,
        multiplier: int = 1,
        timespan: str = "minute",
        days_back: int = 7,
        resume: bool = True,
        save_csv: bool = True,
    ) -> None:
        """Initialize the download service.

        Args:
            symbols: Specific symbols to download. Defaults to settings.
            all_mapped: If True, download all symbols with Polygon mapping.
            multiplier: Timeframe multiplier (e.g., 1, 5).
            timespan: Timespan string.
            days_back: Number of days to download.
            resume: Whether to skip days already cached on disk.
            save_csv: Whether to save CSV files.
        """
        self._requested_symbols = list(symbols) if symbols is not None else None
        self._all_mapped = all_mapped
        self._multiplier = multiplier
        self._timespan = timespan
        self._days_back = days_back
        self._resume = resume
        self._save_csv = save_csv
        self.settings = get_settings()
        self._db_sync: DatabaseRepository | None = None
        self._loader: PolygonHistoricalLoader | None = None
        self._archive_symbols: dict[str, str] = {}
        self._symbol_mapper = SymbolMapperService.get_instance()
        self._run_stats = _RunStats()

    def get_run_result(self) -> JsonObject:
        """Return counters suitable for process run persistence.

        Returns:
            JSON-compatible symbol outcome counters.
        """
        return self._run_stats.as_dict()

    async def start(self) -> None:
        """Start the download process.

        Loads settings, resolves the archive-symbol map for cache paths,
        and downloads each configured symbol's candles to CSV
        sequentially. Supports both specific symbols list and all_mapped
        mode. Never writes to the database.
        """
        set_log_context("bf:poly_agg")
        client: PolygonExchangeClient | None = None
        try:
            settings_service = await get_settings_service(
                self.settings.db_url,
                self.settings.zmq_broker_xsub,
            )
            self.settings = get_settings_with_service(settings_service)
            api_key = self.settings.polygon_api_key
            if not api_key:
                raise ValueError("Polygon API key not configured in settings")
            self._db_sync = DatabaseRepository(self.settings.db_url)
            self._archive_symbols = self._db_sync.get_archive_symbols()
            client = PolygonExchangeClient(api_key=api_key)
            self._loader = PolygonHistoricalLoader(client, cache_root=_CACHE_ROOT)
            symbols = self._resolve_symbols_for_start()
            self._run_stats.selected_symbols = len(symbols)
            if not symbols:
                return
            for symbol in symbols:
                context = self._resolve_symbol_context(symbol)
                if context is None:
                    self._run_stats.skipped_symbols += 1
                    logger.warning("Skipping symbol without context", symbol=symbol)
                    continue
                fetched = await self._process_symbol(context)
                if fetched is None:
                    continue
                self._run_stats.fetched_symbols += 1
                if fetched:
                    self._run_stats.symbols_with_data += 1
                else:
                    self._run_stats.symbols_without_data += 1
        finally:
            if client is not None:
                await client.disconnect()
            self._dispose_resources()

    def _resolve_symbols_for_start(self) -> list[str]:
        """Given configured symbol options, when starting, then choose symbols.

        Args:
            None.

        Returns:
            Symbols selected for aggregate download, or an empty list when
            startup should stop after the same warning the inline path emitted.

        Raises:
            This helper does not raise directly.
        """
        if self._all_mapped:
            symbols = self._get_all_mapped_symbols()
            if not symbols:
                logger.warning("No symbols with Polygon mapping found in database")
                return []
            logger.info(f"Fetched {len(symbols)} symbols with Polygon mapping from database")
            return symbols
        symbols = self._requested_symbols or self.settings.instruments.get(ExchangeEnum.POLYGON, [])
        if symbols == ["*"]:
            mapped_symbols = self._get_all_mapped_symbols()
            if not mapped_symbols:
                logger.warning("Wildcard settings but no Polygon-mapped symbols in database")
                return []
            logger.info(
                f"Resolved {len(mapped_symbols)} Polygon symbols from wildcard settings "
                "(same as --all)"
            )
            return mapped_symbols
        if not symbols:
            logger.warning("No Polygon symbols configured for backfill")
            return []
        return list(symbols)

    def _dispose_resources(self) -> None:
        """Dispose the synchronous repository allocated during startup."""
        sync_repo = self._db_sync
        self._db_sync = None
        self._loader = None

        sync_dispose = getattr(sync_repo, "dispose", None)
        if callable(sync_dispose):
            sync_dispose()

    def _get_all_mapped_symbols(self) -> list[str]:
        """Get all symbols with Polygon mapping from database.

        Returns:
            List of polygon exchange_symbol strings.
        """
        assert self._db_sync is not None
        with self._db_sync.get_session() as session:
            now = datetime.now(UTC)
            stmt = (
                select(SymbolAlias.exchange_symbol)
                .where(
                    SymbolAlias.exchange == ExchangeEnum.POLYGON,
                    SymbolAlias.channel == AliasChannelEnum.REST,
                    SymbolAlias.exchange_symbol.is_not(None),
                    SymbolAlias.timestamp <= now,
                    SymbolAlias.known_to > now,
                )
                .order_by(SymbolAlias.exchange_symbol)
            )
            result = session.execute(stmt).scalars().all()
            return [str(symbol) for symbol in result if symbol]

    def _compute_date_range(self) -> tuple[date, date, datetime]:
        """Compute the backfill date range and max timestamp.

        Returns:
            Tuple of (start_date, end_date, max_timestamp).
        """
        now_utc = datetime.now(UTC)
        yesterday = (now_utc - timedelta(days=1)).date()
        max_ts = datetime.combine(yesterday, datetime.max.time(), tzinfo=UTC)
        end_date = yesterday
        free_tier_limit_days = 730
        oldest_allowed = now_utc.date() - timedelta(days=free_tier_limit_days)
        start_date = end_date - timedelta(days=self._days_back - 1)
        if start_date < oldest_allowed:
            logger.warning(
                f"Requested start date {start_date.isoformat()} exceeds free tier limit "
                f"({free_tier_limit_days} days). Capping to {oldest_allowed.isoformat()}"
            )
            start_date = oldest_allowed
        return start_date, end_date, max_ts

    def _compute_chunk_days(self, start_date: date, end_date: date) -> int:
        """Compute the chunk size in days based on timespan.

        Args:
            start_date: Backfill start date.
            end_date: Backfill end date.

        Returns:
            Number of days per chunk.
        """
        if self._timespan == "minute":
            return 730
        if self._timespan == "hour":
            return 1825
        return min((end_date - start_date).days + 1, 36500)

    def _bars_per_day(self) -> int:
        """Return estimated bars per day for the current timespan.

        Returns:
            Number of bars per day.
        """
        if self._timespan == "minute":
            return 1440
        if self._timespan == "hour":
            return 24
        return 1

    def _all_csv_exist_in_range(self, archive_symbol: str, day_start: date, day_end: date) -> bool:
        """Check whether all daily CSV files exist in a date range.

        Args:
            archive_symbol: Stable archive symbol for cache directory.
            day_start: First date to check.
            day_end: Last date to check.

        Returns:
            True if every day in the range has a CSV file.
        """
        assert self._loader is not None
        check_day = day_start
        while check_day <= day_end:
            if not self._loader.is_aggregate_day_settled(archive_symbol, self._timespan, check_day):
                return False
            check_day += timedelta(days=1)
        return True

    def _find_missing_csv_boundary(
        self,
        archive_symbol: str,
        day_start: date,
        day_end: date,
        forward: bool,
    ) -> date:
        """Scan from one end of a date range to find the first missing CSV.

        Args:
            archive_symbol: Stable archive symbol for cache directory.
            day_start: Start of the search range.
            day_end: End of the search range.
            forward: If True scan from day_start forward, else from day_end backward.

        Returns:
            The date of the first missing CSV file.
        """
        assert self._loader is not None
        current = day_start if forward else day_end
        step = timedelta(days=1) if forward else timedelta(days=-1)
        while (forward and current <= day_end) or (not forward and current >= day_start):
            if not self._loader.is_aggregate_day_settled(archive_symbol, self._timespan, current):
                return current
            current += step
        return current

    def _optimize_chunk_small(
        self,
        archive_symbol: str,
        chunk_start: date,
        chunk_end: date,
    ) -> date | None:
        """Attempt skip optimization for small chunks where all CSVs may exist.

        Args:
            archive_symbol: Stable archive symbol for cache directory.
            chunk_start: Chunk start date.
            chunk_end: Chunk end date.

        Returns:
            None if the chunk should be skipped entirely, otherwise the chunk_end
            value is unchanged (returns chunk_end as-is to indicate no skip).
        """
        chunk_days_count = (chunk_end - chunk_start).days + 1
        estimated_bars = chunk_days_count * self._bars_per_day()
        if self._all_csv_exist_in_range(archive_symbol, chunk_start, chunk_end):
            logger.info(
                f" Skipping chunk {chunk_start.isoformat()} -> "
                f"{chunk_end.isoformat()} ({chunk_days_count}d) - "
                f"all CSV files exist",
                symbol=archive_symbol,
            )
            return None
        logger.debug(
            f"Chunk {chunk_start.isoformat()}->{chunk_end.isoformat()} "
            f"({chunk_days_count}d ~ {estimated_bars} bars) fits in 1 API call - "
            f"fetching all to capture adjustments",
            symbol=archive_symbol,
        )
        return chunk_end

    def _optimize_chunk_large(
        self,
        archive_symbol: str,
        chunk_start: date,
        chunk_end: date,
        chunk_days: int,
        start_date: date,
    ) -> tuple[date, date] | None:
        """Trim edges of a large chunk where CSVs already exist.

        Args:
            archive_symbol: Stable archive symbol for cache directory.
            chunk_start: Original chunk start date.
            chunk_end: Original chunk end date.
            chunk_days: Chunk size in days.
            start_date: Overall backfill start date.

        Returns:
            Tuple of (optimized_start, optimized_end) or None to skip entirely.
        """
        optimized_end = self._find_missing_csv_boundary(
            archive_symbol, chunk_start, chunk_end, forward=False
        )
        optimized_start = self._find_missing_csv_boundary(
            archive_symbol, chunk_start, optimized_end, forward=True
        )
        if optimized_end < chunk_start or optimized_start > optimized_end:
            logger.info(
                f" Skipping chunk {chunk_start.isoformat()} -> "
                f"{chunk_end.isoformat()} - all CSV files exist",
                symbol=archive_symbol,
            )
            return None
        original_chunk_end = chunk_end
        original_start = max(original_chunk_end - timedelta(days=chunk_days - 1), start_date)
        original_days = (original_chunk_end - original_start).days + 1
        final_days = (optimized_end - optimized_start).days + 1
        if final_days < original_days:
            logger.info(
                f"Optimized: {original_start.isoformat()}->"
                f"{original_chunk_end.isoformat()} ({original_days}d) -> "
                f"{optimized_start.isoformat()}->{optimized_end.isoformat()} "
                f"({final_days}d) - skipping edge CSVs",
                symbol=archive_symbol,
            )
        return optimized_start, optimized_end

    def _apply_resume_optimization(
        self,
        archive_symbol: str,
        chunk_start: date,
        chunk_end: date,
        chunk_days: int,
        start_date: date,
    ) -> tuple[date, date] | None:
        """Apply CSV-based resume optimization to a chunk.

        Args:
            archive_symbol: Stable archive symbol for cache directory.
            chunk_start: Chunk start date.
            chunk_end: Chunk end date.
            chunk_days: Chunk size in days.
            start_date: Overall backfill start date.

        Returns:
            Optimized (chunk_start, chunk_end) or None to skip the chunk.
        """
        chunk_days_count = (chunk_end - chunk_start).days + 1
        estimated_bars = chunk_days_count * self._bars_per_day()
        skip_optimization = estimated_bars <= 50000
        if skip_optimization:
            result = self._optimize_chunk_small(archive_symbol, chunk_start, chunk_end)
            if result is None:
                return None
            return chunk_start, chunk_end
        large_result = self._optimize_chunk_large(
            archive_symbol, chunk_start, chunk_end, chunk_days, start_date
        )
        if large_result is None:
            return None
        return large_result

    async def _fetch_chunk(
        self,
        context: _SymbolContext,
        chunk_start: date,
        chunk_end: date,
        max_ts: datetime,
    ) -> bool:
        """Download candle data for a chunk to the CSV cache.

        Writes the on-disk CSV cache only. Does not touch the database;
        loading the cache into the ``candles`` table is the separate
        responsibility of ``PolygonCsvLoaderService``.

        Args:
            context: Symbol context.
            chunk_start: Chunk start date.
            chunk_end: Chunk end date.
            max_ts: Maximum allowed timestamp.
        """
        assert self._loader is not None
        from_ts = datetime.combine(chunk_start, datetime.min.time(), tzinfo=UTC)
        to_ts = datetime.combine(chunk_end, datetime.max.time(), tzinfo=UTC)
        if to_ts > max_ts:
            to_ts = max_ts
        range_days = (chunk_end - chunk_start).days + 1
        logger.info(
            f"Fetching chunk: {chunk_start.isoformat()} -> {chunk_end.isoformat()} "
            f"({range_days} days, {from_ts.isoformat()} -> {to_ts.isoformat()})",
            symbol=context.polygon_symbol,
        )
        candles = await self._loader.fetch_aggregates(
            context.polygon_symbol,
            self._multiplier,
            self._timespan,
            from_ts=from_ts,
            to_ts=to_ts,
            archive_symbol=context.archive_symbol,
            resume_from=None,
            save_csv=self._save_csv,
            limit=50000,
        )
        if not candles:
            logger.info(
                f"No candles for chunk {chunk_start.isoformat()} -> {chunk_end.isoformat()}",
                symbol=context.polygon_symbol,
            )
            return False
        logger.info(
            f"Downloaded {len(candles)} candles for {context.native_symbol}",
            chunk=f"{chunk_start.isoformat()} -> {chunk_end.isoformat()}",
            first_ts=candles[0].timestamp.isoformat(),
            last_ts=candles[-1].timestamp.isoformat(),
        )
        return True

    def _resolve_chunk_range(
        self,
        context: _SymbolContext,
        chunk_start: date,
        chunk_end: date,
        chunk_days: int,
        start_date: date,
    ) -> tuple[date, date] | None:
        """Resolve the effective chunk range after resume optimization.

        The filesystem resume-skip optimization decides a day is already
        cached by checking that its per-day CSV path exists. For the ``day``
        timespan every day of a month resolves to the same monthly
        ``{YYYY}-{MM}.csv`` file, so a partial monthly file (e.g. only the
        last day of the month present) would make every day of that month
        look cached and silently skip the missing days. The daily path
        therefore bypasses the resume-skip optimization entirely and always
        fetches the requested range; the loader's read-merge-write keeps the
        re-fetch idempotent and daily volume is tiny so the extra fetch is
        cheap. Sub-day timespans keep the per-day resume-skip optimization
        unchanged.

        Args:
            context: Symbol context with Polygon symbol.
            chunk_start: Initial chunk start date.
            chunk_end: Initial chunk end date.
            chunk_days: Chunk size in days.
            start_date: Overall backfill start date.

        Returns:
            Tuple of (chunk_start, chunk_end) or None to skip this chunk.
        """
        if not (self._resume and self._save_csv):
            return chunk_start, chunk_end
        if self._timespan.lower() == "day":
            return chunk_start, chunk_end
        return self._apply_resume_optimization(
            context.archive_symbol,
            chunk_start,
            chunk_end,
            chunk_days,
            start_date,
        )

    async def _process_symbol(self, context: _SymbolContext) -> bool | None:
        """Download a single symbol's candles to the CSV cache.

        Handles chunked date ranges and CSV-based resume optimization.
        Never writes to the database.

        Args:
            context: Symbol context with native and Polygon symbols.
        """
        assert self._loader is not None
        start_date, end_date, max_ts = self._compute_date_range()
        logger.info(
            f"Starting download for {context.polygon_symbol}",
            symbol=context.polygon_symbol,
            timespan=self._timespan,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            days_total=(end_date - start_date).days + 1,
        )
        chunk_days = self._compute_chunk_days(start_date, end_date)
        chunk_end: date = end_date
        fetched_any = False
        fetched_data = False
        while chunk_end >= start_date:
            chunk_start = max(chunk_end - timedelta(days=chunk_days - 1), start_date)
            resolved = self._resolve_chunk_range(
                context, chunk_start, chunk_end, chunk_days, start_date
            )
            if resolved is None:
                chunk_end = chunk_start - timedelta(days=1)
                continue
            chunk_start, chunk_end = resolved
            fetched_data = (
                await self._fetch_chunk(
                    context,
                    chunk_start,
                    chunk_end,
                    max_ts,
                )
                or fetched_data
            )
            fetched_any = True
            chunk_end = chunk_start - timedelta(days=1)
        return fetched_data if fetched_any else None

    def _lookup_context_by_native(self, native_symbol: str) -> _SymbolContext | None:
        """Look up active symbol and polygon alias for a native symbol.

        Queries Symbol for base/quote, then SymbolAlias for the
        polygon rest exchange_symbol.

        Args:
            native_symbol: Internal normalized symbol (e.g., "BTC-USD").

        Returns:
            _SymbolContext or None if symbol or polygon alias not found.
        """
        assert self._db_sync is not None
        with self._db_sync.get_session() as session:
            now = datetime.now(UTC)
            symbol = session.execute(
                select(Symbol).where(
                    Symbol.native_symbol == native_symbol,
                    Symbol.timestamp <= now,
                    Symbol.known_to > now,
                )
            ).scalar_one_or_none()
            if not symbol:
                return None
            alias = session.execute(
                select(SymbolAlias)
                .where(SymbolAlias.symbol_public_id == symbol.public_id)
                .where(SymbolAlias.exchange == ExchangeEnum.POLYGON)
                .where(SymbolAlias.channel == AliasChannelEnum.REST)
                .where(SymbolAlias.timestamp <= now)
                .where(SymbolAlias.known_to > now)
            ).scalar_one_or_none()
            if not alias:
                return None
            arch_sym = self._archive_symbols.get(symbol.public_id)
            if arch_sym is None:
                raise ValueError(
                    f"No archive_symbol for symbol public_id={symbol.public_id} "
                    f"({symbol.native_symbol}). Ensure resolve_archive_symbols ran at startup."
                )
            return _SymbolContext(
                native_symbol=symbol.native_symbol,
                polygon_symbol=alias.exchange_symbol,
                base_currency=symbol.base,
                quote_currency=symbol.quote or symbol.base,
                symbol_public_id=symbol.public_id,
                archive_symbol=arch_sym,
            )

    def _lookup_context_by_polygon_symbol(self, polygon_symbol: str) -> _SymbolContext | None:
        """Look up alias and active symbol for a polygon exchange symbol.

        Queries SymbolAlias for polygon rest alias, then Symbol
        for base/quote via symbol_public_id.

        Args:
            polygon_symbol: Polygon API symbol (e.g., "X:BTCUSD").

        Returns:
            _SymbolContext or None if alias or symbol not found.
        """
        assert self._db_sync is not None
        with self._db_sync.get_session() as session:
            now = datetime.now(UTC)
            alias = session.execute(
                select(SymbolAlias)
                .where(SymbolAlias.exchange == ExchangeEnum.POLYGON)
                .where(SymbolAlias.channel == AliasChannelEnum.REST)
                .where(SymbolAlias.exchange_symbol == polygon_symbol)
                .where(SymbolAlias.timestamp <= now)
                .where(SymbolAlias.known_to > now)
            ).scalar_one_or_none()
            if not alias:
                return None
            symbol = session.execute(
                select(Symbol).where(
                    Symbol.public_id == alias.symbol_public_id,
                    Symbol.timestamp <= now,
                    Symbol.known_to > now,
                )
            ).scalar_one_or_none()
            if not symbol:
                return None
            arch_sym = self._archive_symbols.get(symbol.public_id)
            if arch_sym is None:
                raise ValueError(
                    f"No archive_symbol for symbol public_id={symbol.public_id} "
                    f"({symbol.native_symbol}). Ensure resolve_archive_symbols ran at startup."
                )
            return _SymbolContext(
                native_symbol=symbol.native_symbol,
                polygon_symbol=alias.exchange_symbol,
                base_currency=symbol.base,
                quote_currency=symbol.quote or symbol.base,
                symbol_public_id=symbol.public_id,
                archive_symbol=arch_sym,
            )

    def _resolve_polygon_symbol(self, symbol: str) -> _SymbolContext | None:
        """Resolve a Polygon-format symbol (contains ':') to context.

        Args:
            symbol: Polygon symbol string.

        Returns:
            _SymbolContext or None.
        """
        native_symbol = self._symbol_mapper.polygon_rest_to_native.get(symbol)
        if native_symbol:
            return self._lookup_context_by_native(native_symbol)
        return None

    def _resolve_native_symbol_context(self, symbol: str) -> _SymbolContext | None:
        """Resolve a native-format symbol to context.

        Tries direct polygon exchange_symbol lookup, then mapper-based lookup.

        Args:
            symbol: Native symbol string.

        Returns:
            _SymbolContext or None.
        """
        context = self._lookup_context_by_polygon_symbol(symbol)
        if context:
            return context
        polygon_symbol = self._symbol_mapper.native_to_polygon_rest.get(symbol)
        if polygon_symbol:
            return self._lookup_context_by_polygon_symbol(polygon_symbol)
        return None

    def _resolve_symbol_context(self, symbol: str) -> _SymbolContext | None:
        """Resolve symbol string to context with Polygon mapping.

        Handles both native symbols and Polygon symbols (with ':' prefix).
        Looks up mapping in database and symbol mapper cache.

        Args:
            symbol: Symbol string (native or Polygon format).

        Returns:
            Symbol context or None if not found.
        """
        self._symbol_mapper.load_cache_if_needed()
        if ":" in symbol:
            context = self._resolve_polygon_symbol(symbol)
        else:
            context = self._resolve_native_symbol_context(symbol)
        if context:
            return context
        logger.warning(
            f"Symbol '{symbol}' not found in symbol mappings - skipping. "
            "Add symbol to database via symbol mapper before downloading.",
            symbol=symbol,
        )
        return None
