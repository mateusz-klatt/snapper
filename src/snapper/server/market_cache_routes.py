"""REST routes for the in-process :class:`MarketCacheService` diagnostics.

Two endpoints surface the cache + stats worker to the SPA and ops
dashboards:

- ``GET /api/market/cache/stats/{exchange_a}/{symbol_a}/{exchange_b}/{symbol_b}``
  - Returns a placeholder envelope with ``is_warm=false`` when the
    worker has yet to compute the pair; ``404`` only when the pair
    is not configured in ``market_stats_pairs``.
- ``GET /api/market/cache/health``
  - Diagnostic snapshot of how many instruments + pairs the cache is
    serving + the policy's persisted universe size.

The cache candle read used to live at
``GET /api/market/cache/candles/{exchange}/{native_symbol}`` but was
migrated 2026-05-14 to ``GET /api/candles/cache?...`` (query-param
parity with the smart ``/api/candles`` router). See
:mod:`snapper.application.services.candle_query` for the shared read
helpers; this module deliberately holds only the pair-stats + health
diagnostics.

RBAC: both routes require :data:`Permission.READ_MARKET_DATA`,
matching the existing DB-backed market routes; no per-row operator-
scope filter (cache + stats surface public price/volume only).
"""

import datetime as dt
from datetime import datetime
from typing import Annotated
from typing import Literal
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Path
from fastapi import Request
from fastapi import status
from loguru import logger

from snapper.api.schemas.market_cache import CachedStatsPayload
from snapper.api.schemas.market_cache import CachedStatsResponse
from snapper.api.schemas.market_cache import CacheHealthPayload
from snapper.api.schemas.market_cache import CacheHealthResponse
from snapper.application.services.market_cache import MarketCacheService
from snapper.application.services.market_cache import PairStats
from snapper.application.services.market_stats import MarketStatsWorker
from snapper.auth.dependencies import require_permission
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.core.types import ExchangeEnum
from snapper.messaging.infrastructure.publisher import SequenceTracker

router = APIRouter(prefix="/market/cache", tags=["market-cache"])

_REST_STREAM = "rest.market_cache"

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


def _validate_exchange(exchange: str) -> str:
    """Reject path parameters outside the supported exchange set with HTTP 400."""
    if exchange not in _VALID_EXCHANGES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unknown exchange: {exchange!r}",
        )
    return exchange


def _envelope_provenance(request: Request) -> tuple[str, int, str, datetime]:
    """Stamp REST tracker provenance fields onto a response envelope."""
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    return sid, seq, pid, ts


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
            detail="Market cache not initialized",
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
