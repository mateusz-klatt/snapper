"""Smart candle read service shared by the three ``/api/candles`` routes.

Three REST endpoints surface the same underlying candle data:

- ``GET /api/candles`` — public façade. Cache-first for cache-eligible
  timeframes when ``as_of`` is absent; DB fallback for time-travel
  queries, cold cache, or 1h/4h/1d frames. Old iOS / snapper-mcp / the
  legacy frontend hook hit this URL unchanged and benefit
  transparently when :class:`MarketPersistPolicy` is OFF for the
  instrument.
- ``GET /api/candles/db`` — explicit DB-only escape hatch for ops
  ("is the cache lying?"). Always reads from the persisted ``candles``
  table.
- ``GET /api/candles/cache`` — explicit cache-only diagnostic route
  with ``is_warm`` / ``source`` / ``sample_count`` extras for the
  ``CacheWarmingBanner`` UI. Raises :class:`CacheUnavailableError`
  when the lifespan has not yet wired the cache and the timeframe is
  cache-eligible.

All three routes ride on the helpers in this module so projection +
routing logic lives in one place.

Routing decision tree (smart entry):

- ``as_of != None`` → DB only. The cache holds a live snapshot and
  cannot honour point-in-time queries; time-travel must never lie.
- Cache unavailable → DB only.
- ``timeframe in {"1h", "4h", "1d"}`` → DB only.
- ``timeframe == "1m"`` → cache; backfill OLDER bars from DB if the
  cache holds fewer than ``limit``. Cache rows win on overlap.
- ``timeframe in {"5m", "15m", "30m"}`` → derive from the cache 1m
  deque; backfill OLDER bars from DB if the derived series is short.

The returned :class:`CandleQueryResult` exposes a uniform
``list[CandleQueryRow]`` projection. Route handlers pick their own
wire shape: ``CandleData`` with envelope provenance for the
``CandleListResponse`` routes, ``CachedCandle`` for the diagnostic
route. Cache-sourced rows carry ``public_id=None`` etc.; the public
façade route mints deterministic provenance via
``uuid5(NAMESPACE, "{exchange}|{instrument}|{timeframe}|{open_at_ms}")``
plus a sentinel ``session_id="market-cache"`` and ``sequence_id``
equal to ``open_at_ms`` so the same logical bar serialises
identically across calls and old clients see byte-shape-compatible
payloads. See ``project_query_row_to_candle_data`` for the wire
projection.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from typing import Literal

from snapper.application.services.market_cache import CandleSnap
from snapper.application.services.market_cache import MarketCacheService
from snapper.core.types import AllExchange
from snapper.data.repository import Repository
from snapper.data.repository_types import CandleRow

CandleQuerySource = Literal["cache", "derived", "db"]
"""Discriminator surfaced by the diagnostic route's ``source`` field.

``"cache"`` whenever any cache row served the request (even when DB
backfill contributed older bars), ``"derived"`` for aggregated
5m/15m/30m series, and ``"db"`` when the response is purely DB-backed
(time-travel, cold cache + persist on, or 1h/4h/1d frames)."""

DERIVED_AGGREGATION_MAP: dict[str, int] = {
    "5m": 5,
    "15m": 15,
    "30m": 30,
}
"""Number of 1m bars that aggregate into one derived candle."""

DB_FALLBACK_TIMEFRAMES: frozenset[str] = frozenset({"1h", "4h", "1d"})
"""Timeframes the cache cannot serve from 100 minutes of 1m bars."""

CACHE_ELIGIBLE_TIMEFRAMES: frozenset[str] = frozenset({"1m", "5m", "15m", "30m"})
"""Timeframes the cache can serve directly or via 1m derivation."""

VALID_TIMEFRAMES: frozenset[str] = CACHE_ELIGIBLE_TIMEFRAMES | DB_FALLBACK_TIMEFRAMES
"""Full set of timeframes any candle route accepts."""


class CacheUnavailableError(RuntimeError):
    """Signalled by :func:`fetch_cache_only` when the cache is not wired.

    The diagnostic route maps this to HTTP ``503 Service Unavailable``.
    The public façade and DB-only routes never raise this because they
    can always fall back to the repository.
    """


@dataclass(frozen=True, slots=True)
class CandleQueryRow:
    """Uniform projection of one candle from cache or DB.

    Provenance fields (``public_id`` / ``timestamp`` / ``session_id``
    / ``sequence_id``) are ``None`` for cache-sourced rows because the
    cache only stores OHLCV state; the original ZMQ frame's identity
    is not retained on the deque. Route handlers wishing to project
    onto :class:`snapper.messaging.schemas.data.CandleData` mint
    synthetic provenance from the REST tracker.

    Attributes:
        open_at: Candle interval start (UTC).
        timeframe: Resolved timeframe (1m / 5m / 15m / 30m / 1h / 4h / 1d).
        open: Opening price.
        high: Highest price during the candle.
        low: Lowest price during the candle.
        close: Closing price.
        volume: Total traded volume during the candle.
        vwap: Volume-weighted average price (``None`` for cache-sourced
            rows; populated from the DB row when available).
        trades: Number of trades in the candle (``None`` for cache-
            sourced rows; populated from the DB row when available).
        public_id: Persisted candle row identity. ``None`` for cache-
            sourced rows.
        timestamp: Persisted candle row write time. ``None`` for cache-
            sourced rows.
        session_id: Producer publisher session id. ``None`` for cache-
            sourced rows.
        sequence_id: Producer publisher sequence number. ``None`` for
            cache-sourced rows.
    """

    open_at: datetime
    timeframe: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float | None
    trades: int | None
    public_id: str | None
    timestamp: datetime | None
    session_id: str | None
    sequence_id: int | None


@dataclass(frozen=True, slots=True)
class CandleQueryResult:
    """Combined return shape from the three fetch helpers.

    Attributes:
        rows: Chronological list of resolved candles (oldest first).
        source: Discriminator describing which path served the request.
            ``"cache"`` whenever any cache row contributed; ``"derived"``
            for aggregated 5m/15m/30m output; ``"db"`` only when zero
            cache rows participated.
        sample_count: ``len(rows)``.
        is_warm: ``True`` when ``sample_count >= limit``.
    """

    rows: list[CandleQueryRow]
    source: CandleQuerySource
    sample_count: int
    is_warm: bool


def row_from_snap(snap: CandleSnap, timeframe: str) -> CandleQueryRow:
    """Project a cache :class:`CandleSnap` to the uniform query row.

    Cache snaps carry no original WS-frame provenance; the resulting
    row leaves ``public_id`` / ``timestamp`` / ``session_id`` /
    ``sequence_id`` / ``vwap`` / ``trades`` as ``None`` so callers
    project to wire types know to mint synthetic provenance.

    Args:
        snap: Compact in-memory snapshot from the deque.
        timeframe: Resolved timeframe label to stamp on the row.

    Returns:
        :class:`CandleQueryRow` with OHLCV populated and provenance
        slots set to ``None``.
    """
    return CandleQueryRow(
        open_at=datetime.fromtimestamp(snap.open_at_ms / 1000.0, tz=UTC),
        timeframe=timeframe,
        open=snap.open,
        high=snap.high,
        low=snap.low,
        close=snap.close,
        volume=snap.volume,
        vwap=None,
        trades=None,
        public_id=None,
        timestamp=None,
        session_id=None,
        sequence_id=None,
    )


def row_from_db(row: CandleRow, timeframe: str) -> CandleQueryRow:
    """Project a persisted :class:`CandleRow` to the uniform query row.

    DB rows already have full provenance from the original ZMQ frame
    write; this helper passes ``public_id`` / ``timestamp`` /
    ``session_id`` / ``sequence_id`` / ``vwap`` / ``trades`` through
    so route handlers can emit the canonical identifiers.

    Args:
        row: One persisted :class:`CandleRow` dict.
        timeframe: Resolved timeframe label to stamp on the row.

    Returns:
        :class:`CandleQueryRow` with OHLCV and full provenance.
    """
    return CandleQueryRow(
        open_at=row["open_at"],
        timeframe=timeframe,
        open=row["open"],
        high=row["high"],
        low=row["low"],
        close=row["close"],
        volume=row["volume"],
        vwap=row["vwap"],
        trades=row["trades"],
        public_id=row["public_id"],
        timestamp=row["timestamp"],
        session_id=row["session_id"],
        sequence_id=row["sequence_id"],
    )


def derive_snaps(snaps: Sequence[CandleSnap], minutes_per_bar: int) -> list[CandleSnap]:
    """Aggregate 1m snaps into canonical-boundary ``minutes_per_bar`` bars.

    Each derived bar starts on a real timeframe boundary
    (``open_at_ms`` divisible by ``minutes_per_bar * 60_000``) and
    requires ``minutes_per_bar`` consecutive gap-free 1m bars to
    materialise. Off-boundary leads + gap-fragmented runs + partial
    tails are all skipped, so the derived series is a strict subset
    of canonical bars and dedupes against DB rows by ``open_at_ms``
    without false negatives.

    Args:
        snaps: Chronological 1m snaps from the cache.
        minutes_per_bar: Aggregation factor (5 / 15 / 30).

    Returns:
        Chronological list of canonical-aligned :class:`CandleSnap`
        entries. Each bar's ``open_at_ms`` is on the canonical
        timeframe boundary.
    """
    if not snaps or minutes_per_bar <= 1:
        return list(snaps)
    bucket_ms = minutes_per_bar * 60_000
    derived: list[CandleSnap] = []
    index = 0
    while index < len(snaps):
        head = snaps[index]
        if head.open_at_ms % bucket_ms != 0:
            index += 1
            continue
        if index + minutes_per_bar > len(snaps):
            break
        bucket = list(snaps[index : index + minutes_per_bar])
        expected_ms = head.open_at_ms
        contiguous = True
        for member in bucket:
            if member.open_at_ms != expected_ms:
                contiguous = False
                break
            expected_ms += 60_000
        if not contiguous:
            index += 1
            continue
        derived.append(
            CandleSnap(
                open_at_ms=head.open_at_ms,
                open=bucket[0].open,
                high=max(member.high for member in bucket),
                low=min(member.low for member in bucket),
                close=bucket[-1].close,
                volume=sum(member.volume for member in bucket),
            )
        )
        index += minutes_per_bar
    return derived


async def fetch_db_only(
    *,
    repo: Repository,
    exchange: AllExchange,
    native_symbol: str,
    timeframe: str,
    limit: int,
    as_of: datetime | None = None,
) -> CandleQueryResult:
    """Always read from the persisted ``candles`` table.

    Used by ``/api/candles/db`` (explicit DB escape hatch) and by the
    smart router when time-travel is requested, the cache is offline,
    or the timeframe falls outside the cache's coverage.

    Args:
        repo: Repository handle.
        exchange: Resolved venue identifier.
        native_symbol: Canonical native symbol (e.g. ``"BTC-USD"``).
        timeframe: One of the seven supported timeframes. Validation
            happens at route boundaries.
        limit: Maximum bars to return.
        as_of: Optional point-in-time. When omitted, reads as of
            :func:`datetime.now`.

    Returns:
        :class:`CandleQueryResult` with ``source="db"``.
    """
    processing_date = as_of or datetime.now(UTC)
    rows = await repo.get_candles(
        instrument=native_symbol,
        timeframe=timeframe,
        start=None,
        end=None,
        exchange=exchange,
        as_of=processing_date,
        limit=limit,
        order="desc",
    )
    chrono = [row_from_db(row, timeframe) for row in reversed(rows)]
    return CandleQueryResult(
        rows=chrono,
        source="db",
        sample_count=len(chrono),
        is_warm=len(chrono) >= limit,
    )


async def _read_cache_snaps(
    *,
    cache: MarketCacheService,
    exchange: AllExchange,
    native_symbol: str,
    timeframe: str,
    limit: int,
) -> tuple[list[CandleSnap], CandleQuerySource]:
    """Return cache snaps + the source discriminator for a cache-eligible read."""
    if timeframe == "1m":
        snaps = await cache.get_1m_candles(exchange, native_symbol, limit=limit)
        return snaps, "cache"
    minutes_per_bar = DERIVED_AGGREGATION_MAP[timeframe]
    one_min = await cache.get_1m_candles(
        exchange,
        native_symbol,
        limit=cache.cache_capacity_per_instrument(),
    )
    derived = derive_snaps(one_min, minutes_per_bar)
    sliced = derived[-limit:] if len(derived) > limit else derived
    return sliced, "derived"


async def fetch_cache_only(
    *,
    cache: MarketCacheService | None,
    repo: Repository,
    exchange: AllExchange,
    native_symbol: str,
    timeframe: str,
    limit: int,
) -> CandleQueryResult:
    """Cache-only diagnostic read with strict ``is_warm`` semantics.

    Used by ``/api/candles/cache``. Mirrors the original
    ``/api/market/cache/candles`` contract:

    - 1m → cache deque only (no DB backfill so ``is_warm`` reveals the
      cache's actual state honestly).
    - 5m / 15m / 30m → derived from cache only (no DB backfill).
    - 1h / 4h / 1d → DB fallback because the cache literally cannot
      serve long frames; the discriminator surfaces this as
      ``source="db"``.

    Args:
        cache: The in-process cache. ``None`` raises
            :class:`CacheUnavailableError` for cache-eligible
            timeframes; long frames fall back to DB regardless.
        repo: Repository handle (used for the 1h/4h/1d fallback).
        exchange: Resolved venue identifier.
        native_symbol: Canonical native symbol.
        timeframe: One of the seven supported timeframes.
        limit: Maximum bars to return.

    Raises:
        CacheUnavailableError: When ``cache is None`` and ``timeframe``
            is cache-eligible (1m/5m/15m/30m). The diagnostic route
            maps this to HTTP 503.

    Returns:
        :class:`CandleQueryResult` with ``source`` reflecting the
        served path.
    """
    if timeframe in DB_FALLBACK_TIMEFRAMES:
        return await fetch_db_only(
            repo=repo,
            exchange=exchange,
            native_symbol=native_symbol,
            timeframe=timeframe,
            limit=limit,
        )
    if cache is None:
        raise CacheUnavailableError("Market cache not initialized")
    cache_snaps, cache_source = await _read_cache_snaps(
        cache=cache,
        exchange=exchange,
        native_symbol=native_symbol,
        timeframe=timeframe,
        limit=limit,
    )
    rows = [row_from_snap(snap, timeframe) for snap in cache_snaps]
    return CandleQueryResult(
        rows=rows,
        source=cache_source,
        sample_count=len(rows),
        is_warm=len(rows) >= limit,
    )


async def fetch_candles(
    *,
    cache: MarketCacheService | None,
    repo: Repository,
    exchange: AllExchange,
    native_symbol: str,
    timeframe: str,
    limit: int,
    as_of: datetime | None = None,
    single_source: bool = False,
) -> CandleQueryResult:
    """Smart-routing entry used by ``/api/candles``.

    Old iOS / snapper-mcp / the legacy frontend hook hit this function
    via the public façade route and automatically benefit from the
    cache when the persist policy is OFF for an instrument.

    See the module docstring for the full routing tree.

    Args:
        cache: The in-process market cache, or ``None`` when the
            lifespan has not yet wired it (early-startup probes fall
            back to DB).
        repo: Repository handle for the DB path.
        exchange: Resolved venue identifier.
        native_symbol: Canonical native symbol. The legacy route's
            ``instrument`` query param maps 1:1 onto this field.
        timeframe: One of the seven supported timeframes.
        limit: Maximum bars to return.
        as_of: Optional point-in-time. When set, hard-routes to the
            DB path so cache (live snapshot only) cannot silently
            mis-serve time-travel queries.
        single_source: Candle Phase 3 slice-4 read cutover. When True, the
            derived frames (``5m/15m/30m``) serve single-source from the
            persisted ``candles`` plane instead of on-read ``derive_snaps``,
            closing the dual-source hazard. ``1m`` stays cache-served. Gated
            OFF by default until the persisted plane is verified populated
            (``verify-candle-coverage``); flipping it before then serves empty.

    Returns:
        :class:`CandleQueryResult` with chronological rows + a source
        discriminator describing which path served the request.
    """
    if as_of is not None:
        return await fetch_db_only(
            repo=repo,
            exchange=exchange,
            native_symbol=native_symbol,
            timeframe=timeframe,
            limit=limit,
            as_of=as_of,
        )
    serves_from_db = (
        cache is None
        or timeframe not in CACHE_ELIGIBLE_TIMEFRAMES
        or (single_source and timeframe in DERIVED_AGGREGATION_MAP)
    )
    if serves_from_db:
        return await fetch_db_only(
            repo=repo,
            exchange=exchange,
            native_symbol=native_symbol,
            timeframe=timeframe,
            limit=limit,
        )
    assert cache is not None
    cache_snaps, cache_source = await _read_cache_snaps(
        cache=cache,
        exchange=exchange,
        native_symbol=native_symbol,
        timeframe=timeframe,
        limit=limit,
    )
    if len(cache_snaps) >= limit:
        chrono = [row_from_snap(snap, timeframe) for snap in cache_snaps]
        return CandleQueryResult(
            rows=chrono,
            source=cache_source,
            sample_count=len(chrono),
            is_warm=True,
        )
    now = datetime.now(UTC)
    db_rows = await repo.get_candles(
        instrument=native_symbol,
        timeframe=timeframe,
        start=None,
        end=None,
        exchange=exchange,
        as_of=now,
        limit=limit,
        order="desc",
    )
    cache_open_ms: set[int] = {snap.open_at_ms for snap in cache_snaps}
    merged: list[CandleQueryRow] = []
    for row in reversed(db_rows):
        open_ms = int(row["open_at"].timestamp() * 1000)
        if open_ms in cache_open_ms:
            continue
        merged.append(row_from_db(row, timeframe))
    for snap in cache_snaps:
        merged.append(row_from_snap(snap, timeframe))
    merged.sort(key=lambda r: int(r.open_at.timestamp() * 1000))
    trimmed = merged[-limit:] if len(merged) > limit else merged
    final_source: CandleQuerySource = cache_source if cache_snaps else "db"
    return CandleQueryResult(
        rows=trimmed,
        source=final_source,
        sample_count=len(trimmed),
        is_warm=len(trimmed) >= limit,
    )
