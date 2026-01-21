"""Polygon aggregates backfill service module.

This module provides batch downloading of historical OHLCV candle data
from Polygon.io API. It supports:
- Per-symbol backfill with configurable date ranges
- CSV caching to avoid re-downloading
- Database storage for query access
- Resume capability for interrupted downloads
"""

from collections.abc import Iterable
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from pathlib import Path

from loguru import logger
from sqlalchemy import select

from snapper.application.process_manager.enums import ProcessLifecycleEnum
from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.registry import register_process
from snapper.application.services.settings import get_settings_service
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_with_service
from snapper.data.models import SymbolMapping
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import Repository
from snapper.data.repository import get_repository
from snapper.infrastructure.exchanges.implementations.polygon import PolygonExchangeClient
from snapper.infrastructure.historical.polygon.loader import AggregateCandle
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
    """

    native_symbol: str
    polygon_symbol: str
    base_currency: str
    quote_currency: str | None


def _timeframe_label(multiplier: int, timespan: str) -> str:
    """Convert timeframe components to short label.

    Args:
        multiplier: Time multiplier (e.g., 1, 5, 15).
        timespan: Timespan string (minute, hour, day).

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
    description="Download Polygon aggregates to CSV.gz and database",
    priority=32,
    lifecycle=ProcessLifecycleEnum.ONE_SHOT,
    role=ProcessRoleEnum.TASK,
    tags=("polygon", "backfill", "historical"),
    enabled=False,
    mode="thread",
    args=[],
)
class PolygonAggregatesBackfillService(RegisterableProcess):
    """Service for backfilling Polygon aggregate (OHLCV) data.

    Downloads historical candle data from Polygon.io API and stores it
    in both compressed CSV files and the database. Supports:
    - Configurable timeframes (minute, hour, day)
    - Resume from last downloaded data
    - All mapped symbols or specific symbol list
    - Rate limiting and chunked requests

    Registered as one-shot task process.
    """

    @staticmethod
    def get_default_kwargs(settings: AppSettings) -> dict[str, object]:
        """Get default constructor kwargs from settings.

        Args:
            settings: Application settings.

        Returns:
            Default kwargs including symbols, timeframe, days_back.
        """
        return {
            "symbols": settings.instruments.get("polygon", []),
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
        """Initialize the backfill service.

        Args:
            symbols: Specific symbols to backfill. Defaults to settings.
            all_mapped: If True, backfill all symbols with Polygon mapping.
            multiplier: Timeframe multiplier (e.g., 1, 5).
            timespan: Timespan string ("minute", "hour", "day").
            days_back: Number of days to backfill.
            resume: Whether to skip existing data.
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
        self._db_async: Repository | None = None
        self._loader: PolygonHistoricalLoader | None = None
        self._instrument_cache: dict[str, int] = {}
        self._symbol_mapper = SymbolMapperService.get_instance()

    async def start(self) -> None:
        """Start the backfill process.

        Initializes database connections, loads settings, and processes
        each configured symbol sequentially. Supports both specific
        symbols list and all_mapped mode.
        """
        set_log_context("bf:poly_agg")
        settings_service = await get_settings_service(
            self.settings.db_url,
            self.settings.zmq_broker_xpub,
            self.settings.master_password,
            self.settings.encryption_salt,
        )
        self.settings = get_settings_with_service(settings_service)
        api_key = self.settings.polygon_api_key
        if not api_key:
            raise ValueError("Polygon API key not configured in settings")
        self._db_sync = DatabaseRepository(self.settings.db_url)
        self._db_async = get_repository(self.settings.db_url)
        client = PolygonExchangeClient(api_key=api_key)
        self._loader = PolygonHistoricalLoader(client, cache_root=_CACHE_ROOT)
        if self._all_mapped:
            symbols = self._get_all_mapped_symbols()
            if not symbols:
                logger.warning("No symbols with Polygon mapping found in database")
                return
            logger.info(f"Fetched {len(symbols)} symbols with Polygon mapping from database")
        else:
            symbols = self._requested_symbols or self.settings.instruments.get("polygon", [])
            if not symbols:
                logger.warning("No Polygon symbols configured for backfill")
                return
        for symbol in symbols:
            context = self._resolve_symbol_context(symbol)
            if context is None:
                logger.warning("Skipping symbol without context", symbol=symbol)
                continue
            await self._process_symbol(context)

    def _get_all_mapped_symbols(self) -> list[str]:
        """Get all symbols with Polygon mapping from database.

        Returns:
            List of polygon_symbol strings.
        """
        assert self._db_sync is not None
        with self._db_sync.get_session() as session:
            stmt = (
                select(SymbolMapping.polygon_symbol)
                .where(SymbolMapping.polygon_symbol.is_not(None))
                .order_by(SymbolMapping.polygon_symbol)
            )
            result = session.execute(stmt).scalars().all()
            return [str(symbol) for symbol in result if symbol]

    async def _process_symbol(self, context: _SymbolContext) -> None:
        """Process backfill for a single symbol.

        Handles chunked date ranges, resume optimization,
        and database persistence.

        Args:
            context: Symbol context with native and Polygon symbols.
        """
        assert self._loader is not None
        assert self._db_async is not None
        timeframe = _timeframe_label(self._multiplier, self._timespan)
        instrument_id = await self._ensure_instrument(context)
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
        logger.info(
            f"Starting backfill for {context.polygon_symbol}",
            symbol=context.polygon_symbol,
            timespan=self._timespan,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            days_total=(end_date - start_date).days + 1,
        )
        if self._timespan == "minute":
            chunk_days = 730
        elif self._timespan == "hour":
            chunk_days = 1825
        else:
            chunk_days = min((end_date - start_date).days + 1, 36500)
        chunk_end: date = end_date
        while chunk_end >= start_date:
            chunk_start = max(chunk_end - timedelta(days=chunk_days - 1), start_date)
            if self._resume and self._save_csv:
                chunk_days_count = (chunk_end - chunk_start).days + 1
                if self._timespan == "minute":
                    bars_per_day = 1440
                elif self._timespan == "hour":
                    bars_per_day = 24
                else:
                    bars_per_day = 1
                estimated_bars = chunk_days_count * bars_per_day
                skip_optimization = estimated_bars <= 50000
                if skip_optimization:
                    all_exist = True
                    check_day = chunk_start
                    while check_day <= chunk_end:
                        csv_path = self._loader.get_aggregate_csv_path_for_day(
                            context.polygon_symbol, self._timespan, check_day
                        )
                        if not csv_path or not csv_path.exists():
                            all_exist = False
                            break
                        check_day += timedelta(days=1)
                    if all_exist:
                        logger.info(
                            f" Skipping chunk {chunk_start.isoformat()} -> "
                            f"{chunk_end.isoformat()} ({chunk_days_count}d) - "
                            f"all CSV files exist",
                            symbol=context.polygon_symbol,
                        )
                        chunk_end = chunk_start - timedelta(days=1)
                        continue
                    logger.debug(
                        f"Chunk {chunk_start.isoformat()}->{chunk_end.isoformat()} "
                        f"({chunk_days_count}d ~ {estimated_bars} bars) fits in 1 API call - "
                        f"fetching all to capture adjustments",
                        symbol=context.polygon_symbol,
                    )
                else:
                    optimized_end = chunk_end
                    while optimized_end >= chunk_start:
                        csv_path = self._loader.get_aggregate_csv_path_for_day(
                            context.polygon_symbol, self._timespan, optimized_end
                        )
                        if not csv_path or not csv_path.exists():
                            break
                        optimized_end -= timedelta(days=1)
                    optimized_start = chunk_start
                    while optimized_start <= optimized_end:
                        csv_path = self._loader.get_aggregate_csv_path_for_day(
                            context.polygon_symbol, self._timespan, optimized_start
                        )
                        if not csv_path or not csv_path.exists():
                            break
                        optimized_start += timedelta(days=1)
                    if optimized_end < chunk_start or optimized_start > optimized_end:
                        logger.info(
                            f" Skipping chunk {chunk_start.isoformat()} -> "
                            f"{chunk_end.isoformat()} - all CSV files exist",
                            symbol=context.polygon_symbol,
                        )
                        chunk_end = chunk_start - timedelta(days=1)
                        continue
                    original_chunk_end = chunk_end
                    chunk_start = optimized_start
                    chunk_end = optimized_end
                    original_start = max(
                        original_chunk_end - timedelta(days=chunk_days - 1), start_date
                    )
                    original_days = (original_chunk_end - original_start).days + 1
                    final_days = (chunk_end - chunk_start).days + 1
                    if final_days < original_days:
                        logger.info(
                            f"Optimized: {original_start.isoformat()}->"
                            f"{original_chunk_end.isoformat()} ({original_days}d) -> "
                            f"{chunk_start.isoformat()}->{chunk_end.isoformat()} "
                            f"({final_days}d) - skipping edge CSVs",
                            symbol=context.polygon_symbol,
                        )
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
                resume_from=None,
                save_csv=self._save_csv,
                limit=50000,
            )
            if not candles:
                logger.info(
                    f"No candles for chunk {chunk_start.isoformat()} -> {chunk_end.isoformat()}",
                    symbol=context.polygon_symbol,
                )
            else:
                rows = self._build_candle_rows(candles, instrument_id, timeframe)
                batch_size = 3000
                total_inserted = 0
                for i in range(0, len(rows), batch_size):
                    batch = rows[i : i + batch_size]
                    inserted = await self._db_async.upsert_candles(batch)
                    total_inserted += inserted
                    logger.debug(
                        f"Inserted batch {i // batch_size + 1}: {inserted}/{len(batch)} candles",
                        symbol=context.native_symbol,
                    )
                logger.info(
                    f"Persisted {total_inserted}/{len(rows)} candles for "
                    f"{context.native_symbol} ({timeframe})",
                    chunk=f"{chunk_start.isoformat()} -> {chunk_end.isoformat()}",
                    first_ts=candles[0].timestamp.isoformat() if candles else None,
                    last_ts=candles[-1].timestamp.isoformat() if candles else None,
                )
            chunk_end = chunk_start - timedelta(days=1)

    def _resolve_symbol_context(self, symbol: str) -> _SymbolContext | None:
        """Resolve symbol string to context with Polygon mapping.

        Handles both native symbols and Polygon symbols (with ':' prefix).
        Looks up mapping in database and symbol mapper cache.

        Args:
            symbol: Symbol string (native or Polygon format).

        Returns:
            Symbol context or None if not found.
        """
        assert self._db_sync is not None
        self._symbol_mapper.load_cache_if_needed()
        if ":" in symbol:
            native_symbol = self._symbol_mapper.polygon_to_native.get(symbol)
            if native_symbol:
                with self._db_sync.get_session() as session:
                    stmt = select(SymbolMapping).where(SymbolMapping.native_symbol == native_symbol)
                    mapping = session.execute(stmt).scalar_one_or_none()
                    if mapping and mapping.polygon_symbol:
                        quote = mapping.quote_currency or mapping.base_currency
                        return _SymbolContext(
                            native_symbol=mapping.native_symbol,
                            polygon_symbol=mapping.polygon_symbol,
                            base_currency=mapping.base_currency,
                            quote_currency=quote,
                        )
        else:
            with self._db_sync.get_session() as session:
                stmt = select(SymbolMapping).where(SymbolMapping.polygon_symbol == symbol)
                mapping = session.execute(stmt).scalar_one_or_none()
                if mapping and mapping.polygon_symbol:
                    quote = mapping.quote_currency or mapping.base_currency
                    return _SymbolContext(
                        native_symbol=mapping.native_symbol,
                        polygon_symbol=mapping.polygon_symbol,
                        base_currency=mapping.base_currency,
                        quote_currency=quote,
                    )
            polygon_symbol = self._symbol_mapper.native_to_polygon.get(symbol)
            if polygon_symbol:
                with self._db_sync.get_session() as session:
                    stmt = select(SymbolMapping).where(
                        SymbolMapping.polygon_symbol == polygon_symbol
                    )
                    mapping = session.execute(stmt).scalar_one_or_none()
                    if mapping and mapping.polygon_symbol:
                        quote = mapping.quote_currency or mapping.base_currency
                        return _SymbolContext(
                            native_symbol=mapping.native_symbol,
                            polygon_symbol=mapping.polygon_symbol,
                            base_currency=mapping.base_currency,
                            quote_currency=quote,
                        )
        logger.warning(
            f"Symbol '{symbol}' not found in symbol mappings - skipping. "
            "Add symbol to database via symbol mapper before backfilling.",
            symbol=symbol,
        )
        return None

    async def _ensure_instrument(self, context: _SymbolContext) -> int:
        """Ensure instrument exists in database, return its ID.

        Uses cache to avoid repeated database lookups.

        Args:
            context: Symbol context.

        Returns:
            Database instrument ID.
        """
        assert self._db_async is not None
        if context.native_symbol in self._instrument_cache:
            return self._instrument_cache[context.native_symbol]
        quote_value = context.quote_currency or context.base_currency
        instrument_id = await self._db_async.upsert_instrument(
            symbol=context.native_symbol,
            base=context.base_currency,
            quote=quote_value,
            tick_size=0.0,
            lot_size=0.0,
        )
        self._instrument_cache[context.native_symbol] = instrument_id
        return instrument_id

    @staticmethod
    def _build_candle_rows(
        candles: Iterable[AggregateCandle],
        instrument_id: int,
        timeframe: str,
    ) -> list[dict[str, object]]:
        """Build database row dicts from candle objects.

        Args:
            candles: Iterable of AggregateCandle objects.
            instrument_id: Database instrument ID.
            timeframe: Timeframe label string.

        Returns:
            List of row dicts ready for database insertion.
        """
        return [
            {
                "instrument_id": instrument_id,
                "timestamp": candle.timestamp,
                "timeframe": timeframe,
                "open": float(candle.open),
                "high": float(candle.high),
                "low": float(candle.low),
                "close": float(candle.close),
                "volume": float(candle.volume),
                "vwap": float(candle.vwap) if candle.vwap is not None else None,
                "trades": candle.transactions,
            }
            for candle in candles
        ]
