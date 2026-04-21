"""Per-exchange REST call observability.

`RestCallTracker` is a process-scoped singleton that records every
outgoing REST call to an external exchange and exposes rolling-window
rates + utilization against the published per-exchange limit. The goal
is to close the "we have no idea how close we are to 429" blind spot
that every exchange-integration roadmap has flagged since
Known upstream limits (public API docs)
Walutomat: 20 req/s per account.
Kraken Spot (REST): 15 req/s per nonce window.
Polygon.io: 5 req/min on the free tier.
Kraken Futures / Kraken Equities / Zonda: not publicly documented
  as a flat req/s; left as `None` and the tracker reports only raw
  rates (no utilization).
Design
Per-exchange deque of monotonic timestamps. On `record_call`, the
  tracker appends `time.monotonic()` and trims entries older than the
  longest configured window (60 s).
`get_rate(exchange, window_s)` counts entries newer than
  `now - window_s` and returns `count / window_s` (req/s).
`get_utilization(exchange)` returns the 1 s rate divided by the
  exchange's configured limit, or `None` when no limit is configured.
`snapshot()` emits a flat dict suitable for JSON APIs / logs.
Threading
The tracker uses a `threading.Lock` so it is safe from async code
running on different threads (the publisher + executor stacks run in
separate `ProcessModeEnum.THREAD` workers). All operations are O(1)
amortised — the deque only trims entries older than the 60 s window
so memory is bounded at ~60 x max_rps per exchange.
The tracker is intentionally NOT auto-backoff. Enforcement lives in
each exchange's `_with_retry` / ccxt rate-limit handler. The tracker
is observability only — logging and (future) metrics endpoints read
from it, but no outgoing request is blocked by this module.
"""

import asyncio
import threading
import time
from collections import deque
from typing import Final

from loguru import logger

from snapper.core.types import ExchangeEnum

_WINDOW_S_1: Final[float] = 1.0
_WINDOW_S_10: Final[float] = 10.0
_WINDOW_S_60: Final[float] = 60.0
_MAX_WINDOW_S: Final[float] = 60.0

REST_RATE_LIMITS_PER_SECOND: Final[dict[str, float]] = {
    ExchangeEnum.WALUTOMAT: 20.0,
    ExchangeEnum.KRAKEN: 15.0,
    ExchangeEnum.POLYGON: 5.0 / 60.0,
}
"""Published per-second REST rate limits per exchange.

Entries absent from this mapping are tracked (rate reported) but do not
report utilization because no authoritative published limit exists.
"""

_UTIL_WARN_THRESHOLD: Final[float] = 0.80
_UTIL_PAGE_THRESHOLD: Final[float] = 0.95


class RestCallTracker:
    """Process-scoped tracker of outgoing REST calls per exchange.

    Attributes:
        _events: Per-exchange deque of monotonic timestamps for calls
            in the last 60 s.
        _limits: Per-exchange rate limit in req/s. Missing entry means
            no utilization is reported.
        _lock: Thread lock guarding ``_events`` mutations.
        _warned: Per-exchange timestamp of the most recent
            high-utilization warning. Used to rate-limit the warning
            log itself at 60 s.
    """

    def __init__(self, limits: dict[str, float] | None = None) -> None:
        """Initialise with optional override of published limits.

        Args:
            limits: Override map of ``exchange -> req/s``. When omitted,
                ``REST_RATE_LIMITS_PER_SECOND`` is used. Mostly intended
                for tests.
        """
        self._events: dict[str, deque[float]] = {}
        self._limits: dict[str, float] = dict(
            limits if limits is not None else REST_RATE_LIMITS_PER_SECOND
        )
        self._lock = threading.Lock()
        self._warned: dict[str, float] = {}
        self._async_locks: dict[str, asyncio.Lock] = {}

    def record_call(self, exchange: str) -> None:
        """Record a REST call to ``exchange``.

        Args:
            exchange: Exchange identifier (must match ``ExchangeEnum``
                values for limit lookup; other strings still track
                rates but report no utilization).
        """
        now = time.monotonic()
        with self._lock:
            events = self._events.get(exchange)
            if events is None:
                events = deque()
                self._events[exchange] = events
            events.append(now)
            cutoff = now - _MAX_WINDOW_S
            while events and events[0] < cutoff:
                events.popleft()
        utilization = self.get_utilization(exchange)
        if utilization is not None and utilization >= _UTIL_WARN_THRESHOLD:
            self._maybe_warn(exchange, utilization)

    def get_rate(self, exchange: str, window_s: float) -> float:
        """Return the average req/s over the last ``window_s`` seconds.

        Args:
            exchange: Exchange identifier.
            window_s: Lookback window. Must be > 0 and <= 60.

        Returns:
            Average requests-per-second over the window. Zero when
            the exchange has never been recorded.
        """
        if window_s <= 0 or window_s > _MAX_WINDOW_S:
            raise ValueError(f"window_s must be in (0, {_MAX_WINDOW_S}]; got {window_s}")
        now = time.monotonic()
        cutoff = now - window_s
        with self._lock:
            events = self._events.get(exchange)
            if not events:
                return 0.0
            count = sum(1 for ts in events if ts >= cutoff)
        return count / window_s

    def get_utilization(self, exchange: str) -> float | None:
        """Return the 1 s rate as a fraction of the published limit.

        Args:
            exchange: Exchange identifier.

        Returns:
            Utilization in ``[0.0, +inf)`` (values > 1.0 mean the
            exchange is already rate-limiting us). ``None`` when no
            published limit exists for this exchange.
        """
        limit = self._limits.get(exchange)
        if limit is None or limit <= 0:
            return None
        return self.get_rate(exchange, _WINDOW_S_1) / limit

    def snapshot(self) -> dict[str, dict[str, float | None]]:
        """Return a JSON-serialisable snapshot of all tracked exchanges.

        Returns:
            Map of ``exchange -> {"rps_1s", "rps_10s", "rps_60s",
            "limit_rps", "utilization"}``. ``limit_rps`` and
            ``utilization`` are ``None`` for exchanges without a
            published limit.
        """
        with self._lock:
            exchanges = tuple(self._events.keys())
        out: dict[str, dict[str, float | None]] = {}
        for exchange in exchanges:
            limit = self._limits.get(exchange)
            rps_1 = self.get_rate(exchange, _WINDOW_S_1)
            out[exchange] = {
                "rps_1s": rps_1,
                "rps_10s": self.get_rate(exchange, _WINDOW_S_10),
                "rps_60s": self.get_rate(exchange, _WINDOW_S_60),
                "limit_rps": limit,
                "utilization": (rps_1 / limit) if limit is not None and limit > 0 else None,
            }
        return out

    def reset(self) -> None:
        """Drop every recorded event and warning state.

        Intended for test isolation; production callers should never
        need this.
        """
        with self._lock:
            self._events.clear()
            self._warned.clear()
            self._async_locks.clear()

    async def acquire(self, exchange: str) -> None:
        """Pre-emptively throttle to stay under the published limit.

        Token-bucket-style wait: if the 1 s window already holds
        ``limit`` or more recorded calls for this exchange, sleep until
        the oldest entry falls out of the window, then record the call
        atomically. Exchanges without a published limit pass through
        unchanged (record immediately) — for those we have no ground
        truth to pre-empt against and the existing
        ``_with_retry`` / ccxt handlers remain the authoritative 429
        response.

        Serialisation per exchange: an ``asyncio.Lock`` guarantees
        that concurrent callers do not all pass the capacity check in
        the same microsecond and then all commit over the limit. The
        lock is cheap — we serialise only the check-and-record slice,
        not the actual outgoing REST I/O.

        Args:
            exchange: Exchange identifier (``ExchangeEnum`` value).
        """
        limit = self._limits.get(exchange)
        if limit is None or limit <= 0:
            self.record_call(exchange)
            return
        lock = self._async_locks.get(exchange)
        if lock is None:
            lock = asyncio.Lock()
            self._async_locks[exchange] = lock
        async with lock:
            while True:
                now = time.monotonic()
                cutoff = now - _WINDOW_S_1
                with self._lock:
                    events = self._events.get(exchange)
                    if events is None:
                        events = deque()
                        self._events[exchange] = events
                    while events and events[0] < cutoff:
                        events.popleft()
                    count = len(events)
                    oldest = events[0] if events else now
                if count < limit:
                    break
                sleep_for = max(0.005, oldest + _WINDOW_S_1 - now)
                logger.debug(
                    "REST pre-emptive backoff: exchange={} count={} limit={:.2f} sleep={:.3f}s",
                    exchange,
                    count,
                    limit,
                    sleep_for,
                )
                await asyncio.sleep(sleep_for)
            self.record_call(exchange)

    def _maybe_warn(self, exchange: str, utilization: float) -> None:
        """Emit a warning log when utilization crosses a threshold.

        Rate-limited to one warning per exchange per 60 s so a sustained
        high-rate burst doesn't flood the logs. The 95 % bucket uses a
        stronger severity to make it easy to grep for near-throttle
        conditions in post-incident analysis.

        Args:
            exchange: Exchange identifier.
            utilization: Observed utilization in ``[0.0, +inf)``.
        """
        now = time.monotonic()
        last = self._warned.get(exchange, 0.0)
        if now - last < 60.0:
            return
        self._warned[exchange] = now
        severity = "error" if utilization >= _UTIL_PAGE_THRESHOLD else "warning"
        message = (
            f"REST utilization {utilization * 100:.0f}% of limit for exchange "
            f"{exchange!r} (1s rate)"
        )
        if severity == "error":
            logger.error(message)
        else:
            logger.warning(message)


class _SingletonHolder:
    """Container for the process-scoped tracker instance.

    Holding the singleton on a class attribute (instead of a bare module
    global) sidesteps the ``PLW0603`` lint warning and keeps the
    double-checked-locking pattern easy to reason about in one place.
    """

    instance: RestCallTracker | None = None
    lock: threading.Lock = threading.Lock()


def get_rest_call_tracker() -> RestCallTracker:
    """Return the process-scoped tracker, constructing it on first use.

    Returns:
        The shared ``RestCallTracker`` singleton.
    """
    if _SingletonHolder.instance is None:
        with _SingletonHolder.lock:
            if _SingletonHolder.instance is None:
                _SingletonHolder.instance = RestCallTracker()
    return _SingletonHolder.instance


def reset_rest_call_tracker_for_tests() -> None:
    """Drop the singleton so each test gets a clean slate.

    Intended solely for the test suite.
    """
    with _SingletonHolder.lock:
        _SingletonHolder.instance = None
