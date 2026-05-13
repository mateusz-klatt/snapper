"""MarketCacheService — in-process 1-minute candle cache.

Holds the most recent N closed 1m candles for every instrument the
publishers stream so chart routes can serve sub-millisecond reads
without hitting the database. The cache is fed live from the broker's
market.* topics; persisted instruments are additionally pre-warmed
from :class:`Repository` at lifespan start so a freshly-rebooted
server has chart data immediately without waiting on the first close.

Design constraints from plan v6:

- **1-minute candles only.** Other timeframes either derive from the
  1m deque (5m, 15m, 30m via the read route) or fall through to the
  DB (1h, 4h, 1d). The cache MUST refuse to mix timeframes inside the
  per-instrument deque.
- **Closed bars only.** :class:`CandleData` carries no ``closed``
  flag; closure is derived from the ``open_at`` transition. An
  in-flight bar lives in :attr:`_current` until a frame with a newer
  ``open_at`` arrives, at which point the prior :attr:`_current` is
  promoted to the deque.
- **All mutations under one** :class:`asyncio.Lock`. Reads use the
  same lock to give callers a coherent snapshot of the deque tuple
  rather than a partially-mutated state mid-promote.
- **Stale-key prune.** A 60-second background loop drops instruments
  not seen in 6 hours, freeing memory for cold instruments without
  blocking the hot path.
"""

import asyncio
import contextlib
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import zmq
import zmq.asyncio
from loguru import logger

from snapper.application.services.market_persist_policy import MarketPersistPolicy
from snapper.core.types import AllExchange
from snapper.data.repository import Repository
from snapper.data.repository_types import CandleRow
from snapper.messaging.infrastructure.validated_socket import HWM_MARKET_DATA
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.data import CandleData

_CACHE_CANDLE_LIMIT = 100
"""Per-instrument deque cap. 100 minutes is enough to derive 30m × 3
cells for the chart route while keeping cache memory predictable
(~100 instruments × 100 candles × ~96 bytes = ~1 MB)."""

_PRUNE_LOOP_INTERVAL_S = 60.0
"""How often the stale-key prune loop walks ``_last_seen_at``."""

_STALE_AFTER_S = 6 * 60 * 60
"""Drop instruments not seen in this many seconds. 6 hours covers
overnight kraken-spot lulls without losing prewarmed memory."""

_TARGET_TIMEFRAME = "1m"
"""Cache only ingests the 1m timeframe; other frames are filtered out."""

_LISTEN_RECV_BACKOFF_S = 0.1
"""Backoff after a transient ``recv_multipart`` failure."""

_MARKET_TOPIC_PREFIX = "market."
"""ZMQ SUB filter prefix. Narrower filters (``.candles.1m``) live
inside :meth:`_should_ingest_topic` because ZMQ filters match bytes
at a fixed offset and ``.candles.1m`` sits at a variable offset."""

_TARGET_TOPIC_SUFFIX = ".candles.1m"
"""Python-side suffix gate so the SUB socket can use the cheap
``market.`` byte prefix without parsing every tick / trade payload."""


@dataclass(frozen=True, slots=True)
class CandleSnap:
    """Compact frozen snapshot of one closed 1m candle.

    Stores ``open_at`` as an integer millisecond epoch so deque
    membership checks + chronological dedup avoid the cost of
    constructing :class:`datetime` instances on every frame.

    Attributes:
        open_at_ms: Candle interval start as integer milliseconds
            since the UTC epoch.
        open: Opening price.
        high: Highest price during the candle.
        low: Lowest price during the candle.
        close: Closing price.
        volume: Total traded volume.
    """

    open_at_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True, slots=True)
class PairStats:
    """Computed Pearson + Engle-Granger cointegration for a pair of legs.

    Populated by :class:`snapper.application.services.market_stats.MarketStatsWorker`
    on its 60 s (Pearson) and 300 s (cointegration) cadences. Routes
    serialize this directly into the stats response envelope.

    Attributes:
        pearson_r: Pearson correlation coefficient on the latest
            aligned-close sample (``None`` until first compute).
        pearson_n: Sample count contributing to ``pearson_r``.
        coint_t: Engle-Granger ``coint_t`` statistic (``None`` until
            the cointegration cadence has produced a value).
        coint_pvalue: Engle-Granger asymptotic p-value (``None`` until
            first compute).
        coint_critical_values: Critical-value triple (1%, 5%, 10%)
            from ``statsmodels.tsa.stattools.coint``.
        computed_at: UTC timestamp of the most recent successful
            compute (whichever cadence ran last).
        sample_count: Sample size of the most recent compute.
        is_warm: ``True`` once a sample exceeds the per-cadence
            threshold (≥ 30 for Pearson, ≥ 60 for cointegration).
    """

    pearson_r: float | None = None
    pearson_n: int = 0
    coint_t: float | None = None
    coint_pvalue: float | None = None
    coint_critical_values: tuple[float, float, float] | None = None
    computed_at: datetime | None = None
    sample_count: int = 0
    is_warm: bool = False


def _snap_from_candle_data(candle: CandleData) -> CandleSnap:
    """Project a :class:`CandleData` ZMQ frame to a :class:`CandleSnap`."""
    return CandleSnap(
        open_at_ms=int(candle.open_at.timestamp() * 1000),
        open=candle.open,
        high=candle.high,
        low=candle.low,
        close=candle.close,
        volume=candle.volume,
    )


def _snap_from_candle_row(row: CandleRow) -> CandleSnap:
    """Project a :class:`CandleRow` DB row to a :class:`CandleSnap`."""
    return CandleSnap(
        open_at_ms=int(row["open_at"].timestamp() * 1000),
        open=row["open"],
        high=row["high"],
        low=row["low"],
        close=row["close"],
        volume=row["volume"],
    )


class MarketCacheService:
    """In-process 1m candle cache fed by the market.* broker topics.

    Instantiated once per process from the FastAPI lifespan AFTER
    :class:`MarketPersistPolicy` is ready (the cache's prewarm path
    asks the policy which instruments to fetch from the DB) and
    BEFORE :meth:`ProcessLauncherService.start_all_processes` so the
    SUB socket is connected to the broker before the first candle
    frame leaves a publisher.

    Reads (:meth:`get_1m_candles`, :meth:`current_for`) take the same
    :attr:`_lock` as mutations so callers see a coherent snapshot of
    each per-instrument deque rather than a partially-mutated state
    mid-promote.

    Attributes:
        repository: Source of the prewarm candle backfill.
        persist_policy: Drives the prewarm instrument set.
    """

    def __init__(
        self,
        repository: Repository,
        persist_policy: MarketPersistPolicy,
    ) -> None:
        """Initialise empty state. :meth:`start` performs the heavy work.

        Args:
            repository: Async repository handle for the prewarm path.
            persist_policy: Configured policy supplying
                :meth:`iter_persisted_instruments`.
        """
        self.repository = repository
        self.persist_policy = persist_policy
        self._lock = asyncio.Lock()
        self._candles: dict[tuple[AllExchange, str], deque[CandleSnap]] = {}
        self._current: dict[tuple[AllExchange, str], CandleSnap] = {}
        self._last_seen_at: dict[tuple[AllExchange, str], float] = {}
        self._stats: dict[tuple[str, str], PairStats] = {}
        self._zmq_context: zmq.asyncio.Context | None = None
        self._subscriber: ValidatedSubscriber | None = None
        self._ingest_task: asyncio.Task[None] | None = None
        self._prune_task: asyncio.Task[None] | None = None
        self._running = False
        self._listener_lock = asyncio.Lock()

    async def start(self, zmq_broker_xpub: str) -> None:
        """Open the SUB socket, prewarm from DB, and spawn the ingest + prune tasks.

        Idempotent + restart-safe via :attr:`_listener_lock`. Empty
        ``zmq_broker_xpub`` skips socket setup entirely (test mode);
        the cache still serves prewarmed data + accepts manual writes
        through :meth:`record_candle_for_test`.

        Args:
            zmq_broker_xpub: Address of the broker's XPUB endpoint.
                Empty string skips the listener.
        """
        async with self._listener_lock:
            if self._ingest_task is not None and not self._ingest_task.done():
                return
            if self._ingest_task is not None:
                await self._reap_unlocked()
            await self._prewarm()
            if not zmq_broker_xpub:
                logger.info("MarketCacheService: empty broker XPUB, ingest task skipped")
                self._prune_task = asyncio.create_task(self._prune_loop())
                return
            self._zmq_context = zmq.asyncio.Context()
            raw_sub_socket = self._zmq_context.socket(zmq.SUB)
            apply_hwm(raw_sub_socket, rcvhwm=HWM_MARKET_DATA)
            raw_sub_socket.connect(zmq_broker_xpub)
            self._subscriber = ValidatedSubscriber(raw_sub_socket)
            self._subscriber.subscribe(_MARKET_TOPIC_PREFIX)
            self._running = True
            self._ingest_task = asyncio.create_task(self._ingest_loop())
            self._prune_task = asyncio.create_task(self._prune_loop())
            logger.info(
                "MarketCacheService: subscribed to {}* on {} (prewarmed {} instruments)",
                _MARKET_TOPIC_PREFIX,
                zmq_broker_xpub,
                len(self._candles),
            )

    async def stop(self) -> None:
        """Cancel ingest + prune tasks and dispose ZMQ resources. Idempotent."""
        async with self._listener_lock:
            await self._reap_unlocked()

    async def _reap_unlocked(self) -> None:
        """Tear down resources. Caller MUST hold :attr:`_listener_lock`."""
        self._running = False
        ingest_task = self._ingest_task
        prune_task = self._prune_task
        subscriber = self._subscriber
        context = self._zmq_context
        self._ingest_task = None
        self._prune_task = None
        self._subscriber = None
        self._zmq_context = None
        for task in (ingest_task, prune_task):
            if task is None:
                continue
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        if subscriber is not None:
            with contextlib.suppress(Exception):
                subscriber.close()
        if context is not None:
            with contextlib.suppress(Exception):
                context.term()

    async def get_1m_candles(
        self, exchange: AllExchange, native_symbol: str, *, limit: int
    ) -> list[CandleSnap]:
        """Return up to ``limit`` most-recent closed 1m candles, chronological.

        Returns an empty list when the cache has never seen this
        instrument or when prewarm did not surface it. Callers (the
        read route) decide whether to fall through to the DB or
        return an empty payload with ``is_warm=false``.

        Args:
            exchange: Source exchange.
            native_symbol: Native-format symbol (e.g. ``"BTC-USD"``).
            limit: Maximum number of bars; pulled from the right side
                of the deque so the most recent bars are always
                included.

        Returns:
            List of :class:`CandleSnap` in chronological order
            (oldest first). Length ≤ ``limit``.
        """
        key = (exchange, native_symbol)
        async with self._lock:
            cached = self._candles.get(key)
            if cached is None:
                return []
            if limit >= len(cached):
                return list(cached)
            return list(cached)[-limit:]

    async def instruments_cached(self) -> int:
        """Return the count of distinct ``(exchange, symbol)`` keys in the cache.

        Returns:
            Number of instruments with at least one closed bar on the deque.
        """
        async with self._lock:
            return len(self._candles)

    async def get_pair_stats(self, left_key: str, right_key: str) -> PairStats | None:
        """Return the computed stats for ``(left_key, right_key)`` or ``None``.

        Pair keys are the canonical ``"{exchange}:{native_symbol}"``
        form produced by :class:`MarketStatsWorker`. Routes call this
        accessor and surface a placeholder envelope if the worker has
        not yet produced a value for a configured pair.

        Args:
            left_key: Canonical pair key for the left leg.
            right_key: Canonical pair key for the right leg.

        Returns:
            The most recent :class:`PairStats` snapshot, or ``None``
            when the pair is not (yet) in the stats map.
        """
        async with self._lock:
            return self._stats.get((left_key, right_key))

    async def set_pair_stats(self, left_key: str, right_key: str, stats: PairStats) -> None:
        """Atomic write of a :class:`PairStats` entry by canonical pair key.

        Args:
            left_key: Canonical pair key for the left leg.
            right_key: Canonical pair key for the right leg.
            stats: Updated :class:`PairStats` snapshot to install.
        """
        async with self._lock:
            self._stats[(left_key, right_key)] = stats

    async def pair_stats_keys(self) -> list[tuple[str, str]]:
        """Return a snapshot of all stat pair keys for diagnostic routes.

        Returns:
            Shallow copy of the ``_stats`` dict keys so callers can
            iterate without holding the cache lock.
        """
        async with self._lock:
            return list(self._stats.keys())

    async def _prewarm(self) -> None:
        """Backfill the cache from the DB for every persisted instrument.

        Iterates the policy's persisted set for the ``"candles"`` data
        type, fetches up to ``_CACHE_CANDLE_LIMIT`` most-recent 1m
        rows per instrument, and seeds the deque in chronological
        order. Per-instrument failures are logged + skipped so one
        bad symbol cannot block warmup.
        """
        as_of = datetime.now(UTC)
        warmed = 0
        for exchange, native_symbol in self.persist_policy.iter_persisted_instruments("candles"):
            try:
                rows = await self.repository.get_candles(
                    instrument=native_symbol,
                    timeframe=_TARGET_TIMEFRAME,
                    start=None,
                    end=None,
                    exchange=exchange,
                    as_of=as_of,
                    limit=_CACHE_CANDLE_LIMIT,
                    order="desc",
                )
            except Exception as exc:
                logger.warning(
                    "MarketCacheService prewarm failed for {} {}: {}",
                    exchange,
                    native_symbol,
                    exc,
                )
                continue
            if not rows:
                continue
            chronological: Iterable[CandleRow] = reversed(rows)
            snaps = [_snap_from_candle_row(row) for row in chronological]
            key = (exchange, native_symbol)
            async with self._lock:
                self._candles[key] = deque(snaps, maxlen=_CACHE_CANDLE_LIMIT)
                self._last_seen_at[key] = asyncio.get_event_loop().time()
            warmed += 1
        logger.info("MarketCacheService prewarm complete: {} instruments warmed", warmed)

    async def _ingest_loop(self) -> None:
        """Consume market.* frames, filter to closed 1m candles, dispatch."""
        subscriber = self._subscriber
        if subscriber is None:
            return
        try:
            while self._running:
                frame = await self._recv_one_frame(subscriber)
                if frame is None:
                    continue
                topic, payload = frame
                if not self._should_ingest_topic(topic):
                    continue
                candle = self._parse_candle(payload)
                if candle is None:
                    continue
                if candle.timeframe != _TARGET_TIMEFRAME:
                    continue
                await self._dispatch_candle(candle)
        except asyncio.CancelledError:
            logger.info("MarketCacheService: ingest loop cancelled")
            raise

    async def _recv_one_frame(self, subscriber: ValidatedSubscriber) -> tuple[str, bytes] | None:
        """Receive one (topic, payload) frame; ``None`` on transient failure."""
        try:
            topic_bytes, payload_bytes = await subscriber.recv_multipart()
            topic = topic_bytes.decode() if isinstance(topic_bytes, bytes) else str(topic_bytes)
            payload = (
                payload_bytes if isinstance(payload_bytes, bytes) else str(payload_bytes).encode()
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("MarketCacheService listener recv failed: {}", exc)
            await asyncio.sleep(_LISTEN_RECV_BACKOFF_S)
            return None
        return topic, payload

    @staticmethod
    def _should_ingest_topic(topic: str) -> bool:
        """Cheap suffix gate before parsing the payload."""
        return topic.endswith(_TARGET_TOPIC_SUFFIX)

    @staticmethod
    def _parse_candle(payload: bytes) -> CandleData | None:
        """Parse a JSON payload into :class:`CandleData`; ``None`` on failure."""
        try:
            return CandleData.model_validate_json(payload)
        except Exception as exc:
            logger.debug("MarketCacheService: candle parse failed: {}", exc)
            return None

    async def _dispatch_candle(self, candle: CandleData) -> None:
        """Apply the closed-bar dedup state machine to ``candle``.

        State machine per ``(exchange, native_symbol)`` key:

        - No ``_current`` slot: seed it. Nothing appended to the deque.
        - Incoming ``open_at`` equals current: upsert (the live bar
          is still being built; replace OHLCV).
        - Incoming ``open_at`` greater than current: promote — append
          the prior current to the deque (now closed by construction),
          then set current to the new frame.
        - Incoming ``open_at`` less than current: drop as stale; rare,
          debug-log only.
        """
        key = (candle.exchange, candle.instrument)
        snap = _snap_from_candle_data(candle)
        async with self._lock:
            self._last_seen_at[key] = asyncio.get_event_loop().time()
            current = self._current.get(key)
            if current is None:
                self._current[key] = snap
                return
            if snap.open_at_ms == current.open_at_ms:
                self._current[key] = snap
                return
            if snap.open_at_ms > current.open_at_ms:
                cached = self._candles.setdefault(key, deque(maxlen=_CACHE_CANDLE_LIMIT))
                cached.append(current)
                self._current[key] = snap
                return
            logger.debug(
                "MarketCacheService: stale candle for {} {} (open_at_ms={} < current={})",
                candle.exchange,
                candle.instrument,
                snap.open_at_ms,
                current.open_at_ms,
            )

    async def _prune_loop(self) -> None:
        """Walk ``_last_seen_at`` every minute and drop entries idle for 6h."""
        try:
            while self._running or self._prune_task is None:
                await asyncio.sleep(_PRUNE_LOOP_INTERVAL_S)
                await self._prune_once()
        except asyncio.CancelledError:
            logger.info("MarketCacheService: prune loop cancelled")
            raise

    async def _prune_once(self) -> None:
        """One pass of the stale-key prune."""
        cutoff = asyncio.get_event_loop().time() - _STALE_AFTER_S
        async with self._lock:
            stale = [key for key, last in self._last_seen_at.items() if last < cutoff]
            for key in stale:
                self._candles.pop(key, None)
                self._current.pop(key, None)
                self._last_seen_at.pop(key, None)
        if stale:
            logger.info("MarketCacheService prune: dropped {} stale instruments", len(stale))

    async def record_candle_for_test(self, candle: CandleData) -> None:
        """Test-only hook to push a candle through the dispatch path.

        Production code never calls this; the ingest loop owns frames
        in flight. Tests use this entry point to verify the dedup
        state machine without spinning up a real broker.

        Args:
            candle: Decoded :class:`CandleData` to feed through the
                :meth:`_dispatch_candle` state machine.
        """
        if candle.timeframe != _TARGET_TIMEFRAME:
            return
        await self._dispatch_candle(candle)

    @property
    def last_seen_for_test(self) -> dict[tuple[AllExchange, str], float]:
        """Test-only view of ``_last_seen_at`` (the loop-time stamps).

        Returns:
            Shallow copy of the internal map so tests can inspect
            without risk of mutating live state.
        """
        return dict(self._last_seen_at)

    def force_stale_for_test(self, key: tuple[AllExchange, str], age_s: float) -> None:
        """Test-only hook to age a key beyond the prune cutoff.

        Used by the prune loop test to simulate the 6-hour stale window
        without actually waiting.

        Args:
            key: ``(exchange, native_symbol)`` cache key to back-date.
            age_s: Number of seconds in the past to set the
                ``_last_seen_at`` stamp.
        """
        self._last_seen_at[key] = asyncio.get_event_loop().time() - age_s

    def staleness_window_seconds(self) -> int:
        """Return the configured stale-after window for diagnostics.

        Returns:
            Configured :data:`_STALE_AFTER_S` value in seconds.
        """
        return _STALE_AFTER_S

    def cache_capacity_per_instrument(self) -> int:
        """Return the per-instrument deque cap for diagnostics.

        Returns:
            Configured :data:`_CACHE_CANDLE_LIMIT` value.
        """
        return _CACHE_CANDLE_LIMIT


def _format_stale_age(seconds: float) -> str:
    """Render a stale age as ``H:MM:SS`` for log messages."""
    return str(timedelta(seconds=int(seconds)))
