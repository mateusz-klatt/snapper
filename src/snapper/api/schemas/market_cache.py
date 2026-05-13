"""REST response schemas for the in-process market cache routes.

The cache surfaces three flavours of payload:

- **Candle lists** for chart fetches, carrying an ``is_warm`` flag so
  the SPA can render a "warming up" banner when the cache has fewer
  than the requested ``limit`` bars, and a ``source`` discriminator
  so log readers can tell which path served the response.
- **Pair-stats singletons** for Pearson + cointegration consumers.
- **Health snapshots** for diagnostics + dashboards.

The schemas inherit the project's :class:`PayloadResponse` /
:class:`PayloadListResponse` envelopes so REST tracker provenance
fields (``session_id`` / ``sequence_id`` / ``public_id`` /
``timestamp``) ride alongside the payload like every other route.
"""

from datetime import datetime
from typing import Literal

from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictBody


class CachedCandle(StrictBody):
    """Single candle row projected out of the cache.

    Mirrors the on-deque :class:`snapper.application.services.market_cache.CandleSnap`
    plus a ``timeframe`` field so derived 5m/15m/30m bars carry their
    aggregation tag (the deque only stores 1m).

    Attributes:
        open_at_ms: Candle interval start as integer milliseconds
            since the UTC epoch.
        timeframe: Resolved timeframe (``"1m"`` / ``"5m"`` / ``"15m"`` /
            ``"30m"`` / ``"1h"`` / ``"4h"`` / ``"1d"``).
        open: Opening price.
        high: Highest price during the candle.
        low: Lowest price during the candle.
        close: Closing price.
        volume: Total traded volume during the candle.
    """

    open_at_ms: int
    timeframe: str
    open: float
    high: float
    low: float
    close: float
    volume: float


class CachedCandlesPayload(StrictBody):
    """Inner payload for :class:`CachedCandlesResponse`.

    The cache route returns the candles plus a few fields that let the
    SPA reason about warmth + provenance without a separate health
    round-trip.

    Attributes:
        candles: Chronological list of resolved candles.
        sample_count: Number of items in ``candles``.
        is_warm: ``True`` when ``sample_count >= limit`` (cache has
            served the full request) and ``False`` when the cache is
            still filling.
        source: Path the response took — ``"cache"`` for direct
            ``1m`` reads, ``"derived"`` for 5m/15m/30m aggregation,
            ``"db"`` when the cache fell through to the repository.
    """

    candles: list[CachedCandle]
    sample_count: int
    is_warm: bool
    source: Literal["cache", "derived", "db"]


class CachedCandlesResponse(PayloadResponse[Literal["cached_candles"], CachedCandlesPayload]):
    """Wraps :class:`CachedCandlesPayload` with envelope provenance."""

    type: Literal["cached_candles"] = "cached_candles"


class CachedStatsPayload(StrictBody):
    """Inner payload for :class:`CachedStatsResponse`.

    Either Pearson or cointegration fields can be ``None`` for
    pair-keys configured but not yet computed; the SPA renders a
    placeholder until ``is_warm`` flips.

    Attributes:
        left: Canonical ``"{exchange}:{symbol}"`` for the left leg.
        right: Canonical ``"{exchange}:{symbol}"`` for the right leg.
        pearson_r: Most recent Pearson correlation coefficient.
        pearson_n: Sample count contributing to ``pearson_r``.
        coint_t: Most recent Engle-Granger ``coint_t`` statistic.
        coint_pvalue: Most recent asymptotic p-value.
        coint_critical_values: Critical-value triple (1%, 5%, 10%).
        computed_at: UTC timestamp of the latest successful compute.
        sample_count: Sample size at the most recent compute.
        is_warm: ``True`` when the configured threshold has been hit.
    """

    left: str
    right: str
    pearson_r: float | None
    pearson_n: int
    coint_t: float | None
    coint_pvalue: float | None
    coint_critical_values: tuple[float, float, float] | None
    computed_at: datetime | None
    sample_count: int
    is_warm: bool


class CachedStatsResponse(PayloadResponse[Literal["cached_stats"], CachedStatsPayload]):
    """Wraps :class:`CachedStatsPayload` with envelope provenance."""

    type: Literal["cached_stats"] = "cached_stats"


class CacheHealthPayload(StrictBody):
    """Inner payload for :class:`CacheHealthResponse`.

    Attributes:
        instruments_cached: Count of distinct
            ``(exchange, native_symbol)`` keys with at least one
            closed bar.
        pairs_cached: Count of stats pairs the worker has materialised.
        persist_universe_size: Count of instruments the policy will
            persist (``"candles"`` data type) — a useful denominator
            for the operator deciding whether to widen scope.
    """

    instruments_cached: int
    pairs_cached: int
    persist_universe_size: int


class CacheHealthResponse(PayloadResponse[Literal["cache_health"], CacheHealthPayload]):
    """Wraps :class:`CacheHealthPayload` with envelope provenance."""

    type: Literal["cache_health"] = "cache_health"
