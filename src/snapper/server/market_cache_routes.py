"""REST routes for the in-process :class:`MarketCacheService`.

Three endpoints surface the cache to the SPA + diagnostic dashboards:

- ``GET /api/market/cache/candles/{exchange}/{native_symbol}``
  - ``timeframe=1m``: deque read; ``source="cache"``.
  - ``timeframe in {5m, 15m, 30m}``: aggregated from the 1m deque;
    ``source="derived"``.
  - ``timeframe in {1h, 4h, 1d}``: falls through to the existing
    :meth:`Repository.get_candles` path; ``source="db"``.
- ``GET /api/market/cache/stats/{exchange_a}/{symbol_a}/{exchange_b}/{symbol_b}``
  - Returns a placeholder envelope with ``is_warm=false`` when the
    worker has yet to compute the pair; ``404`` only when the pair
    is not configured in ``market_stats_pairs``.
- ``GET /api/market/cache/health``
  - Diagnostic snapshot of how many instruments + pairs the cache is
    serving + the policy's persisted universe size.

RBAC: all three routes require :data:`Permission.READ_MARKET_DATA`,
matching the existing DB-backed market routes; no per-row operator-
scope filter at v1 (cache surfaces public price/volume only).
"""

import datetime as dt
from datetime import UTC
from datetime import datetime
from typing import Annotated
from typing import Literal
from typing import cast
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Path
from fastapi import Query
from fastapi import Request
from fastapi import status
from loguru import logger

from snapper.api.schemas.market_cache import CachedCandle
from snapper.api.schemas.market_cache import CachedCandlesPayload
from snapper.api.schemas.market_cache import CachedCandlesResponse
from snapper.api.schemas.market_cache import CachedStatsPayload
from snapper.api.schemas.market_cache import CachedStatsResponse
from snapper.api.schemas.market_cache import CacheHealthPayload
from snapper.api.schemas.market_cache import CacheHealthResponse
from snapper.application.services.market_cache import CandleSnap
from snapper.application.services.market_cache import MarketCacheService
from snapper.application.services.market_cache import PairStats
from snapper.application.services.market_stats import MarketStatsWorker
from snapper.auth.dependencies import require_permission
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.core.types import AllExchange
from snapper.core.types import ExchangeEnum
from snapper.data.repository import Repository
from snapper.data.repository_types import CandleRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency

router = APIRouter(prefix="/market/cache", tags=["market-cache"])

_REST_STREAM = "rest.market_cache"
_MARKET_CACHE_NOT_INITIALIZED_DETAIL = "Market cache not initialized"

_DERIVED_AGGREGATION_MAP: dict[str, int] = {
    "5m": 5,
    "15m": 15,
    "30m": 30,
}
"""Number of 1m bars that aggregate into one derived candle."""

_DB_FALLBACK_TIMEFRAMES: frozenset[str] = frozenset({"1h", "4h", "1d"})
"""Timeframes the cache cannot serve from 100 minutes of 1m bars."""

_VALID_TIMEFRAMES: frozenset[str] = frozenset({"1m", "5m", "15m", "30m", "1h", "4h", "1d"})
"""Full set of timeframes accepted by the candle route."""

_VALID_EXCHANGES: frozenset[str] = frozenset(
    {
        ExchangeEnum.PAPER,
        ExchangeEnum.KRAKEN,
        ExchangeEnum.KRAKEN_FUTURES,
        ExchangeEnum.KRAKEN_EQUITIES,
        ExchangeEnum.WALUTOMAT,
        ExchangeEnum.POLYGON,
    }
)


def _validate_exchange(exchange: str) -> AllExchange:
    """Reject path parameters outside :data:`AllExchange` with HTTP 400."""
    if exchange not in _VALID_EXCHANGES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown exchange: {exchange!r}",
        )
    return cast(AllExchange, exchange)


def _validate_timeframe(timeframe: str) -> str:
    """Reject unsupported timeframes with HTTP 400."""
    if timeframe not in _VALID_TIMEFRAMES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported timeframe: {timeframe!r}",
        )
    return timeframe


def _snap_to_payload(snap: CandleSnap, timeframe: str) -> CachedCandle:
    """Project an in-memory snap to the wire-shape :class:`CachedCandle`."""
    return CachedCandle(
        open_at_ms=snap.open_at_ms,
        timeframe=timeframe,
        open=snap.open,
        high=snap.high,
        low=snap.low,
        close=snap.close,
        volume=snap.volume,
    )


def _row_to_payload(row: CandleRow, timeframe: str) -> CachedCandle:
    """Project a DB-fallback :class:`CandleRow` to a :class:`CachedCandle`."""
    return CachedCandle(
        open_at_ms=int(row["open_at"].timestamp() * 1000),
        timeframe=timeframe,
        open=row["open"],
        high=row["high"],
        low=row["low"],
        close=row["close"],
        volume=row["volume"],
    )


def _derive_candles(snaps: list[CandleSnap], minutes_per_bar: int) -> list[CandleSnap]:
    """Aggregate consecutive 1m snaps into ``minutes_per_bar``-minute bars.

    Skips partial buckets at the tail (only full ``minutes_per_bar``-
    snap groups produce derived candles). The cache stores chronologically
    so we walk the list forward + chunk in fixed-size windows.

    Args:
        snaps: Chronological list of closed 1m :class:`CandleSnap`
            entries (the cache returns these in order).
        minutes_per_bar: Aggregation factor (5 / 15 / 30).

    Returns:
        Chronological list of derived :class:`CandleSnap` entries.
    """
    if not snaps or minutes_per_bar <= 1:
        return list(snaps)
    derived: list[CandleSnap] = []
    for start in range(0, len(snaps), minutes_per_bar):
        bucket = snaps[start : start + minutes_per_bar]
        if len(bucket) < minutes_per_bar:
            continue
        derived.append(
            CandleSnap(
                open_at_ms=bucket[0].open_at_ms,
                open=bucket[0].open,
                high=max(snap.high for snap in bucket),
                low=min(snap.low for snap in bucket),
                close=bucket[-1].close,
                volume=sum(snap.volume for snap in bucket),
            )
        )
    return derived


def _envelope_provenance(request: Request) -> tuple[str, int, str, datetime]:
    """Stamp REST tracker provenance fields onto a response envelope."""
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    return sid, seq, pid, ts


def _candles_envelope(
    request: Request,
    *,
    payload: CachedCandlesPayload,
) -> CachedCandlesResponse:
    """Wrap a :class:`CachedCandlesPayload` with the REST envelope."""
    sid, seq, pid, ts = _envelope_provenance(request)
    return CachedCandlesResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=payload,
    )


async def _read_one_minute_payload(
    cache: MarketCacheService,
    exchange: AllExchange,
    native_symbol: str,
    limit: int,
) -> CachedCandlesPayload:
    """Build a payload for the ``1m`` direct-read path."""
    snaps = await cache.get_1m_candles(exchange, native_symbol, limit=limit)
    candles = [_snap_to_payload(snap, "1m") for snap in snaps]
    return CachedCandlesPayload(
        candles=candles,
        sample_count=len(candles),
        is_warm=len(candles) >= limit,
        source="cache",
    )


async def _read_derived_payload(
    cache: MarketCacheService,
    exchange: AllExchange,
    native_symbol: str,
    timeframe: str,
    limit: int,
) -> CachedCandlesPayload:
    """Build a payload for the 5m / 15m / 30m aggregation path."""
    aggregation = _DERIVED_AGGREGATION_MAP[timeframe]
    one_min = await cache.get_1m_candles(
        exchange,
        native_symbol,
        limit=cache.cache_capacity_per_instrument(),
    )
    derived = _derive_candles(one_min, aggregation)
    sliced = derived[-limit:] if len(derived) > limit else derived
    candles = [_snap_to_payload(snap, timeframe) for snap in sliced]
    return CachedCandlesPayload(
        candles=candles,
        sample_count=len(candles),
        is_warm=len(candles) >= limit,
        source="derived",
    )


async def _read_db_fallback_payload(
    repo: Repository,
    exchange: AllExchange,
    native_symbol: str,
    timeframe: str,
    limit: int,
) -> CachedCandlesPayload:
    """Build a payload for the 1h / 4h / 1d DB fallback path."""
    as_of = datetime.now(UTC)
    rows = await repo.get_candles(
        instrument=native_symbol,
        timeframe=timeframe,
        start=None,
        end=None,
        exchange=exchange,
        as_of=as_of,
        limit=limit,
        order="desc",
    )
    candles = [_row_to_payload(row, timeframe) for row in reversed(rows)]
    return CachedCandlesPayload(
        candles=candles,
        sample_count=len(candles),
        is_warm=len(candles) >= limit,
        source="db",
    )


@router.get("/candles/{exchange}/{native_symbol}")
async def get_cached_candles(
    request: Request,
    _principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_MARKET_DATA))],
    exchange: Annotated[str, Path(description="Source exchange identifier")],
    native_symbol: Annotated[str, Path(description="Native symbol (e.g. BTC-USD)")],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    timeframe: Annotated[str, Query(description="Candle timeframe: 1m/5m/15m/30m/1h/4h/1d")] = "1m",
    limit: Annotated[
        int,
        Query(ge=1, le=1000, description="Number of candles to return"),
    ] = 100,
) -> CachedCandlesResponse:
    """Return up to ``limit`` candles for the given ``(exchange, symbol, timeframe)``.

    Routing logic:

    - ``1m``: read the live cache deque (``source="cache"``).
    - ``5m`` / ``15m`` / ``30m``: aggregate from the 1m deque
      (``source="derived"``). Partial buckets at the tail are skipped.
    - ``1h`` / ``4h`` / ``1d``: fall through to
      :meth:`Repository.get_candles` (``source="db"``).

    ``is_warm`` is ``True`` when ``sample_count >= limit`` so the SPA
    can render a "warming up" banner only when it strictly needs to.
    """
    exchange_value = _validate_exchange(exchange)
    timeframe_value = _validate_timeframe(timeframe)
    cache: MarketCacheService | None = getattr(request.app.state, "market_cache", None)
    if timeframe_value == "1m":
        if cache is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=_MARKET_CACHE_NOT_INITIALIZED_DETAIL,
            )
        payload = await _read_one_minute_payload(cache, exchange_value, native_symbol, limit)
    elif timeframe_value in _DERIVED_AGGREGATION_MAP:
        if cache is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=_MARKET_CACHE_NOT_INITIALIZED_DETAIL,
            )
        payload = await _read_derived_payload(
            cache, exchange_value, native_symbol, timeframe_value, limit
        )
    else:
        payload = await _read_db_fallback_payload(
            repo, exchange_value, native_symbol, timeframe_value, limit
        )
    return _candles_envelope(request, payload=payload)


@router.get(
    "/stats/{exchange_a}/{symbol_a}/{exchange_b}/{symbol_b}",
)
async def get_cached_pair_stats(
    request: Request,
    _principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_MARKET_DATA))],
    exchange_a: Annotated[str, Path(description="Left-leg exchange")],
    symbol_a: Annotated[str, Path(description="Left-leg native symbol")],
    exchange_b: Annotated[str, Path(description="Right-leg exchange")],
    symbol_b: Annotated[str, Path(description="Right-leg native symbol")],
) -> CachedStatsResponse:
    """Return Pearson + cointegration for one configured pair.

    The route returns ``404`` only when the pair is not in
    ``market_stats_pairs``; a configured-but-not-yet-computed pair
    returns a placeholder envelope with ``is_warm=false`` so the SPA
    can render a stable "computing…" state instead of cycling between
    404 and 200.
    """
    _validate_exchange(exchange_a)
    _validate_exchange(exchange_b)
    left_str = f"{exchange_a}:{symbol_a}"
    right_str = f"{exchange_b}:{symbol_b}"
    worker: MarketStatsWorker | None = getattr(request.app.state, "market_stats_worker", None)
    cache: MarketCacheService | None = getattr(request.app.state, "market_cache", None)
    if worker is None or cache is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Stats worker not initialized",
        )
    configured = any(
        spec.left_str == left_str and spec.right_str == right_str
        for spec in worker.configured_pairs()
    )
    if not configured:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Pair not configured: {left_str} | {right_str}",
        )
    stats = await cache.get_pair_stats(left_str, right_str) or PairStats()
    payload = CachedStatsPayload(
        left=left_str,
        right=right_str,
        pearson_r=stats.pearson_r,
        pearson_n=stats.pearson_n,
        coint_t=stats.coint_t,
        coint_pvalue=stats.coint_pvalue,
        coint_critical_values=stats.coint_critical_values,
        computed_at=stats.computed_at,
        sample_count=stats.sample_count,
        is_warm=stats.is_warm,
    )
    sid, seq, pid, ts = _envelope_provenance(request)
    return CachedStatsResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=payload,
    )


@router.get("/health")
async def get_cache_health(
    request: Request,
    _principal: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_MARKET_DATA))],
) -> CacheHealthResponse:
    """Diagnostic snapshot of the cache + stats worker + persist policy."""
    cache: MarketCacheService | None = getattr(request.app.state, "market_cache", None)
    if cache is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=_MARKET_CACHE_NOT_INITIALIZED_DETAIL,
        )
    instruments_cached = await cache.instruments_cached()
    pair_keys = await cache.pair_stats_keys()
    policy = getattr(request.app.state, "market_persist_policy", None)
    persist_universe_size = 0
    if policy is not None:
        try:
            persist_universe_size = sum(1 for _ in policy.iter_persisted_instruments("candles"))
        except Exception as exc:
            logger.warning("MarketCacheRoutes health: policy iter failed: {}", exc)
    payload = CacheHealthPayload(
        instruments_cached=instruments_cached,
        pairs_cached=len(pair_keys),
        persist_universe_size=persist_universe_size,
    )
    sid, seq, pid, ts = _envelope_provenance(request)
    return CacheHealthResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=payload,
    )


__all__: list[Literal["router"]] = ["router"]
