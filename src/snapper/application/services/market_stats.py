"""MarketStatsWorker — Pearson + Engle-Granger cointegration over the cache.

Runs two background cadences against :class:`MarketCacheService`:

- **Pearson (60 s)**: for every configured ``market_stats_pairs`` entry,
  pull the most-recent N aligned 1-minute closes, compute the Pearson
  correlation, and write back to the cache's stats slot.
- **Cointegration (300 s)**: same pair set; once at least 60 aligned
  samples are available, run ``statsmodels.tsa.stattools.coint``
  inside :func:`asyncio.to_thread` so the synchronous statsmodels call
  never blocks the event loop, and write the
  ``(coint_t, p-value, critical_values)`` triple back to the same
  slot.

Configuration lives in two settings the operator edits via the admin
UI:

- ``market_stats_pairs``: list of
  ``"{exchange}:{native_symbol}|{exchange}:{native_symbol}"`` pipe-
  separated pair specs. The worker validates the cap (≤ 50 entries
  per plan v6) + the exchange Literal at load time; malformed pairs
  trigger a warning and are silently dropped from the working set so
  one bad config row cannot break the entire worker.
- ``system.settings`` bus event triggers a re-read so a live edit
  through the admin UI surfaces within one event cycle.

Single-flight discipline is enforced per pair: a long-running
cointegration tick for ``(BTC-USD, ETH-USD)`` does not block the
60 s Pearson tick for unrelated pairs.
"""

import asyncio
import contextlib
import math
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast

import numpy as np
import zmq
import zmq.asyncio
from loguru import logger
from statsmodels.tsa.stattools import coint as _engle_granger_coint

from snapper.application.services.market_cache import CandleSnap
from snapper.application.services.market_cache import MarketCacheService
from snapper.application.services.market_cache import PairStats
from snapper.application.services.settings import SettingsService
from snapper.core.types import AllExchange
from snapper.core.types import ExchangeEnum
from snapper.messaging.infrastructure.validated_socket import HWM_AUDIT
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm

_PEARSON_INTERVAL_S = 60.0
"""Cadence of the Pearson worker tick."""

_COINT_INTERVAL_S = 300.0
"""Cadence of the cointegration worker tick (Engle-Granger is expensive)."""

_PEARSON_MIN_SAMPLES = 30
"""Minimum aligned sample count below which Pearson skips + flags
``is_warm=false``."""

_COINT_MIN_SAMPLES = 60
"""Minimum aligned sample count below which cointegration skips."""

_MAX_STATS_PAIRS = 50
"""Hard cap on configured pairs per plan v6 (rejects on overflow)."""

_STATS_PAIRS_KEY = "market_stats_pairs"
"""Settings key holding the pipe-separated pair config."""

_SYSTEM_SETTINGS_TOPIC = "system.settings"
"""Bus topic that triggers a config reload."""

_LISTEN_RECV_BACKOFF_S = 0.1
"""Backoff after a transient ``recv_multipart`` failure."""

_ZERO_VARIANCE_ABS_TOL = 1e-12
"""Absolute tolerance for treating a close series as constant."""

_KNOWN_EXCHANGES: frozenset[str] = frozenset(
    {
        ExchangeEnum.PAPER,
        ExchangeEnum.KRAKEN,
        ExchangeEnum.KRAKEN_FUTURES,
        ExchangeEnum.KRAKEN_EQUITIES,
        ExchangeEnum.WALUTOMAT,
        ExchangeEnum.POLYGON,
    }
)


PairKey = tuple[AllExchange, str]
"""Pair key as ``(exchange, native_symbol)`` for cache lookup."""


@dataclass(frozen=True, slots=True)
class PairSpec:
    """One configured stats pair after validation.

    Attributes:
        left: Left-leg cache key.
        right: Right-leg cache key.
        left_str: Canonical ``"{exchange}:{symbol}"`` for routes + stats map.
        right_str: Canonical ``"{exchange}:{symbol}"`` for routes + stats map.
    """

    left: PairKey
    right: PairKey
    left_str: str
    right_str: str


class StatsPairConfigError(ValueError):
    """Raised when ``market_stats_pairs`` is malformed beyond rescue."""


def parse_pair_key(raw: str) -> PairKey:
    """Parse ``"exchange:native_symbol"`` into a :data:`PairKey` tuple.

    Args:
        raw: Pair key string. Must contain exactly one ``:`` separator
            and have non-empty exchange + symbol parts.

    Returns:
        ``(exchange, native_symbol)`` with ``exchange`` narrowed to
        :data:`AllExchange`.

    Raises:
        StatsPairConfigError: Malformed input (missing separator,
            empty fragment, unknown exchange).
    """
    if not isinstance(raw, str):
        raise StatsPairConfigError(f"pair key must be a string: {raw!r}")
    if raw.count(":") != 1:
        raise StatsPairConfigError(f"pair key must contain exactly one ':' separator: {raw!r}")
    exchange_raw, native_symbol = raw.split(":", 1)
    exchange_raw = exchange_raw.strip()
    native_symbol = native_symbol.strip()
    if not exchange_raw or not native_symbol:
        raise StatsPairConfigError(f"pair key has empty fragment: {raw!r}")
    if exchange_raw not in _KNOWN_EXCHANGES:
        raise StatsPairConfigError(f"pair key references unknown exchange: {raw!r}")
    return cast(AllExchange, exchange_raw), native_symbol


def parse_pair_spec(raw: str) -> PairSpec:
    """Parse a ``"left|right"`` pipe-separated pair spec.

    Both halves must be valid :func:`parse_pair_key` inputs.

    Args:
        raw: Pair spec string, e.g. ``"kraken:BTC-USD|kraken:ETH-USD"``.

    Returns:
        :class:`PairSpec` carrying both halves + their canonical
        ``"{exchange}:{symbol}"`` form for the stats-map keys.

    Raises:
        StatsPairConfigError: Either half is malformed, the spec is
            self-referential (left == right), or the separator is
            wrong.
    """
    if not isinstance(raw, str) or raw.count("|") != 1:
        raise StatsPairConfigError(
            f"pair spec must be 'left|right' with one '|' separator: {raw!r}"
        )
    left_str, right_str = (part.strip() for part in raw.split("|", 1))
    left = parse_pair_key(left_str)
    right = parse_pair_key(right_str)
    if left == right:
        raise StatsPairConfigError(f"pair spec is self-referential: {raw!r}")
    return PairSpec(left=left, right=right, left_str=left_str, right_str=right_str)


def parse_pair_specs(raw_list: Any) -> list[PairSpec]:
    """Parse + validate the ``market_stats_pairs`` setting.

    Drops individual malformed pair specs with a warning so one bad
    row cannot disable the entire worker. Raises only when the input
    shape is wholly wrong (not a list) or when the cap is exceeded —
    a 51-entry config is a configuration error the operator must fix.

    Args:
        raw_list: Setting value pulled from :class:`SettingsService`.

    Returns:
        Validated list of :class:`PairSpec`, length ≤ 50.

    Raises:
        StatsPairConfigError: ``raw_list`` is not a list, or contains
            more than :data:`_MAX_STATS_PAIRS` entries.
    """
    if not isinstance(raw_list, list):
        raise StatsPairConfigError(f"market_stats_pairs must be a list ({type(raw_list).__name__})")
    if len(raw_list) > _MAX_STATS_PAIRS:
        raise StatsPairConfigError(
            f"market_stats_pairs exceeds cap of {_MAX_STATS_PAIRS}: got {len(raw_list)}"
        )
    parsed: list[PairSpec] = []
    for raw in raw_list:
        try:
            parsed.append(parse_pair_spec(raw))
        except StatsPairConfigError as exc:
            logger.warning("MarketStatsWorker: dropping malformed pair {!r}: {}", raw, exc)
    return parsed


def _align_closes(
    left: Iterable[CandleSnap], right: Iterable[CandleSnap]
) -> tuple[np.ndarray, np.ndarray]:
    """Intersect two candle streams on ``open_at_ms`` and return aligned closes.

    Both inputs are assumed chronologically ordered. The aligned
    arrays carry only the closes whose ``open_at_ms`` exists in both
    streams.

    Args:
        left: Chronological 1m candles for the left leg.
        right: Chronological 1m candles for the right leg.

    Returns:
        ``(left_closes, right_closes)`` as float64 ``numpy`` arrays of
        equal length.
    """
    left_by_open = {snap.open_at_ms: snap.close for snap in left}
    right_by_open = {snap.open_at_ms: snap.close for snap in right}
    shared = sorted(left_by_open.keys() & right_by_open.keys())
    if not shared:
        return np.array([], dtype=np.float64), np.array([], dtype=np.float64)
    left_closes = np.array([left_by_open[k] for k in shared], dtype=np.float64)
    right_closes = np.array([right_by_open[k] for k in shared], dtype=np.float64)
    return left_closes, right_closes


def _safe_pearson(left: np.ndarray, right: np.ndarray) -> float | None:
    """Pearson correlation; returns ``None`` for zero-variance inputs.

    ``numpy.corrcoef`` warns + returns NaN when either column is
    constant. The cache route would surface NaN as a JSON quirk, so
    we project to ``None`` instead — a more honest "no signal" answer
    for the SPA.
    """
    if left.size < 2:
        return None
    if math.isclose(float(np.std(left)), 0.0, abs_tol=_ZERO_VARIANCE_ABS_TOL) or math.isclose(
        float(np.std(right)), 0.0, abs_tol=_ZERO_VARIANCE_ABS_TOL
    ):
        return None
    matrix = np.corrcoef(left, right)
    value = float(matrix[0, 1])
    if math.isnan(value):
        return None
    return value


class MarketStatsWorker:
    """Background worker producing Pearson + cointegration on cached pairs.

    Wired into the FastAPI lifespan after :class:`MarketCacheService`
    so the worker can read 1m candles by ``(exchange, native_symbol)``
    key and write its results onto the cache's shared stats slot.

    The worker:

    - Loads ``market_stats_pairs`` at :meth:`start` time, validates
      it, and seeds a placeholder :class:`PairStats` entry per pair.
    - Spawns two cadence tasks (60 s Pearson, 300 s cointegration) +
      a settings listener that re-runs configuration on
      ``system.settings``.
    - Holds a per-pair :class:`asyncio.Lock` so the cointegration
      tick (potentially slow) does not block the next Pearson tick on
      the same pair (single-flight) but never serialises unrelated
      pairs.
    """

    def __init__(
        self,
        *,
        cache: MarketCacheService,
        settings_service: SettingsService,
    ) -> None:
        """Initialise an empty worker. :meth:`start` performs the heavy work.

        Args:
            cache: Source of 1m candles + sink for the computed stats.
            settings_service: Reader for the ``market_stats_pairs``
                config.
        """
        self.cache = cache
        self.settings_service = settings_service
        self._pairs: list[PairSpec] = []
        self._pair_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._pairs_lock = asyncio.Lock()
        self._listener_lock = asyncio.Lock()
        self._pearson_task: asyncio.Task[None] | None = None
        self._coint_task: asyncio.Task[None] | None = None
        self._listen_task: asyncio.Task[None] | None = None
        self._zmq_context: zmq.asyncio.Context | None = None
        self._subscriber: ValidatedSubscriber | None = None
        self._running = False

    async def start(self, zmq_broker_xpub: str) -> None:
        """Load config, seed placeholders, spawn cadence + listener tasks.

        Idempotent + restart-safe via :attr:`_listener_lock`. Empty
        ``zmq_broker_xpub`` skips the settings listener (test mode);
        the cadence tasks still run against the seed config.

        Args:
            zmq_broker_xpub: Address of the broker's XPUB endpoint
                for the ``system.settings`` listener. Empty string
                skips the listener entirely.
        """
        async with self._listener_lock:
            if self._pearson_task is not None and not self._pearson_task.done():
                return
            if self._pearson_task is not None:
                await self._reap_unlocked()
            await self._reload_config()
            self._running = True
            self._pearson_task = asyncio.create_task(self._pearson_loop())
            self._coint_task = asyncio.create_task(self._coint_loop())
            if not zmq_broker_xpub:
                logger.info("MarketStatsWorker: empty broker XPUB, settings listener skipped")
                return
            self._zmq_context = zmq.asyncio.Context()
            raw_sub = self._zmq_context.socket(zmq.SUB)
            apply_hwm(raw_sub, rcvhwm=HWM_AUDIT)
            raw_sub.connect(zmq_broker_xpub)
            self._subscriber = ValidatedSubscriber(raw_sub)
            self._subscriber.subscribe(_SYSTEM_SETTINGS_TOPIC)
            self._listen_task = asyncio.create_task(self._listen_loop())
            logger.info(
                "MarketStatsWorker: started ({} pair(s) configured; listening on {})",
                len(self._pairs),
                zmq_broker_xpub,
            )

    async def stop(self) -> None:
        """Cancel all tasks + dispose ZMQ resources. Idempotent."""
        async with self._listener_lock:
            await self._reap_unlocked()

    async def _reap_unlocked(self) -> None:
        """Tear down resources. Caller MUST hold :attr:`_listener_lock`."""
        self._running = False
        tasks = [self._pearson_task, self._coint_task, self._listen_task]
        subscriber = self._subscriber
        context = self._zmq_context
        self._pearson_task = None
        self._coint_task = None
        self._listen_task = None
        self._subscriber = None
        self._zmq_context = None
        for task in tasks:
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

    async def _reload_config(self) -> None:
        """Re-read ``market_stats_pairs``, swap the working set under lock.

        On a malformed top-level shape the worker preserves the
        previous pair set so one bad admin edit cannot blank the
        worker. Per-pair drops (within a list of otherwise-valid
        entries) stay as warnings and silently shrink the working set.
        """
        raw = self.settings_service.get_setting(_STATS_PAIRS_KEY, default=[])
        try:
            new_pairs = parse_pair_specs(raw)
        except StatsPairConfigError as exc:
            logger.warning(
                "MarketStatsWorker config reload aborted ({}); previous pair set preserved",
                exc,
            )
            return
        async with self._pairs_lock:
            self._pairs = new_pairs
            self._pair_locks = {
                (spec.left_str, spec.right_str): self._pair_locks.get(
                    (spec.left_str, spec.right_str), asyncio.Lock()
                )
                for spec in new_pairs
            }
        for spec in new_pairs:
            existing = await self.cache.get_pair_stats(spec.left_str, spec.right_str)
            if existing is None:
                await self.cache.set_pair_stats(spec.left_str, spec.right_str, PairStats())

    async def _pearson_loop(self) -> None:
        """Run a Pearson tick every :data:`_PEARSON_INTERVAL_S` seconds."""
        try:
            while self._running:
                await asyncio.sleep(_PEARSON_INTERVAL_S)
                await self.run_pearson_once()
        except asyncio.CancelledError:
            logger.info("MarketStatsWorker: pearson loop cancelled")
            raise

    async def _coint_loop(self) -> None:
        """Run a cointegration tick every :data:`_COINT_INTERVAL_S` seconds."""
        try:
            while self._running:
                await asyncio.sleep(_COINT_INTERVAL_S)
                await self.run_cointegration_once()
        except asyncio.CancelledError:
            logger.info("MarketStatsWorker: coint loop cancelled")
            raise

    async def run_pearson_once(self) -> None:
        """One pass of the Pearson cadence across the configured pair set."""
        async with self._pairs_lock:
            pairs_snapshot = list(self._pairs)
        for spec in pairs_snapshot:
            await self._tick_pearson_for_pair(spec)

    async def run_cointegration_once(self) -> None:
        """One pass of the cointegration cadence across the configured pair set."""
        async with self._pairs_lock:
            pairs_snapshot = list(self._pairs)
        for spec in pairs_snapshot:
            await self._tick_coint_for_pair(spec)

    async def _tick_pearson_for_pair(self, spec: PairSpec) -> None:
        """Compute Pearson for one pair under its single-flight lock."""
        lock = self._pair_locks.get((spec.left_str, spec.right_str))
        if lock is None or lock.locked():
            return
        async with lock:
            left_closes, right_closes = await self._pair_aligned_closes(spec)
            sample_count = int(left_closes.size)
            if sample_count < _PEARSON_MIN_SAMPLES:
                await self._merge_stats(
                    spec,
                    pearson_r=None,
                    pearson_n=sample_count,
                    sample_count=sample_count,
                    is_warm=False,
                )
                return
            r = _safe_pearson(left_closes, right_closes)
            await self._merge_stats(
                spec,
                pearson_r=r,
                pearson_n=sample_count,
                sample_count=sample_count,
                is_warm=True,
                computed_at=datetime.now(UTC),
            )

    async def _tick_coint_for_pair(self, spec: PairSpec) -> None:
        """Compute Engle-Granger cointegration for one pair under its lock."""
        lock = self._pair_locks.get((spec.left_str, spec.right_str))
        if lock is None or lock.locked():
            return
        async with lock:
            left_closes, right_closes = await self._pair_aligned_closes(spec)
            sample_count = int(left_closes.size)
            if sample_count < _COINT_MIN_SAMPLES:
                return
            try:
                coint_t, p_value, crit = await asyncio.to_thread(
                    _engle_granger_coint, left_closes, right_closes
                )
            except Exception as exc:
                logger.warning(
                    "MarketStatsWorker coint failed for {}|{}: {}",
                    spec.left_str,
                    spec.right_str,
                    exc,
                )
                return
            await self._merge_stats(
                spec,
                coint_t=float(coint_t),
                coint_pvalue=float(p_value),
                coint_critical_values=(
                    float(crit[0]),
                    float(crit[1]),
                    float(crit[2]),
                ),
                sample_count=sample_count,
                is_warm=True,
                computed_at=datetime.now(UTC),
            )

    async def _pair_aligned_closes(self, spec: PairSpec) -> tuple[np.ndarray, np.ndarray]:
        """Pull both legs from the cache + return their aligned-close arrays."""
        left_candles = await self.cache.get_1m_candles(
            spec.left[0], spec.left[1], limit=self.cache.cache_capacity_per_instrument()
        )
        right_candles = await self.cache.get_1m_candles(
            spec.right[0],
            spec.right[1],
            limit=self.cache.cache_capacity_per_instrument(),
        )
        return _align_closes(left_candles, right_candles)

    async def _merge_stats(
        self,
        spec: PairSpec,
        *,
        pearson_r: float | None = None,
        pearson_n: int | None = None,
        coint_t: float | None = None,
        coint_pvalue: float | None = None,
        coint_critical_values: tuple[float, float, float] | None = None,
        computed_at: datetime | None = None,
        sample_count: int | None = None,
        is_warm: bool | None = None,
    ) -> None:
        """Read-modify-write the cache's stats slot, preserving untouched fields.

        The Pearson tick only writes the Pearson fields; the
        cointegration tick only writes its own. ``None`` arguments
        signal "keep the previous value" so partial updates do not
        clobber a freshly-computed Pearson with a placeholder coint
        result.
        """
        existing = await self.cache.get_pair_stats(spec.left_str, spec.right_str)
        if existing is None:
            existing = PairStats()
        updated = PairStats(
            pearson_r=existing.pearson_r if pearson_r is None else pearson_r,
            pearson_n=existing.pearson_n if pearson_n is None else pearson_n,
            coint_t=existing.coint_t if coint_t is None else coint_t,
            coint_pvalue=existing.coint_pvalue if coint_pvalue is None else coint_pvalue,
            coint_critical_values=(
                existing.coint_critical_values
                if coint_critical_values is None
                else coint_critical_values
            ),
            computed_at=existing.computed_at if computed_at is None else computed_at,
            sample_count=(existing.sample_count if sample_count is None else sample_count),
            is_warm=existing.is_warm if is_warm is None else is_warm,
        )
        await self.cache.set_pair_stats(spec.left_str, spec.right_str, updated)

    async def _listen_loop(self) -> None:
        """Re-load configuration when a ``system.settings`` frame arrives."""
        subscriber = self._subscriber
        if subscriber is None:
            return
        try:
            while self._running:
                frame = await self._recv_one_frame(subscriber)
                if frame is None:
                    continue
                topic, _payload = frame
                if topic == _SYSTEM_SETTINGS_TOPIC:
                    try:
                        await self._reload_config()
                    except Exception as exc:
                        logger.error("MarketStatsWorker config reload raised: {}", exc)
        except asyncio.CancelledError:
            logger.info("MarketStatsWorker: settings listener cancelled")
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
            logger.error("MarketStatsWorker listener recv failed: {}", exc)
            await asyncio.sleep(_LISTEN_RECV_BACKOFF_S)
            return None
        return topic, payload

    def configured_pairs(self) -> list[PairSpec]:
        """Diagnostic snapshot of the live pair set for route responses.

        Returns:
            Shallow copy of :attr:`_pairs` so callers can iterate
            without holding the worker lock.
        """
        return list(self._pairs)
