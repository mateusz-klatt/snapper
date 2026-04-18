"""Tests for RestCallTracker sliding-window rate + utilization semantics."""

import asyncio
import threading
import time
from collections import deque
from collections.abc import Iterator
from typing import Any
from unittest.mock import patch

import pytest
from loguru import logger

import snapper.infrastructure.rest.tracker as tracker_module
from snapper.core.types import ExchangeEnum
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.rest.tracker import REST_RATE_LIMITS_PER_SECOND
from snapper.infrastructure.rest.tracker import RestCallTracker
from snapper.infrastructure.rest.tracker import get_rest_call_tracker
from snapper.infrastructure.rest.tracker import reset_rest_call_tracker_for_tests


@pytest.fixture(autouse=True)
def _reset_tracker() -> None:
    """Reset the process-scoped tracker so tests don't leak state."""
    reset_rest_call_tracker_for_tests()


@pytest.fixture
def loguru_sink() -> Iterator[list[str]]:
    """Capture loguru messages into a list for assertion.

    ``caplog`` does not intercept loguru by default; this fixture
    attaches a direct sink so tests can inspect the warning/error
    messages the tracker emits.
    """
    messages: list[str] = []

    def _sink(message: object) -> None:
        messages.append(str(message))

    handler_id = logger.add(_sink, level="WARNING")
    try:
        yield messages
    finally:
        logger.remove(handler_id)


class TestRestCallTrackerRateAndUtilization:
    """Core sliding-window + utilization behaviours."""

    def test_empty_tracker_reports_zero(self) -> None:
        """Given no recorded calls, rate is zero and utilization is zero or None.

        Given:
            A freshly constructed tracker,

        When:
            ``get_rate`` / ``get_utilization`` are called for any
            exchange,

        Then:
            Rate returns ``0.0`` and utilization returns ``0.0`` for
            exchanges with a published limit (1 s rate 0 / limit =
            0) and ``None`` for exchanges without a limit.
        """
        tracker = RestCallTracker()
        assert tracker.get_rate(ExchangeEnum.KRAKEN, 1.0) == pytest.approx(0.0)
        assert tracker.get_utilization(ExchangeEnum.KRAKEN) == pytest.approx(0.0)
        assert tracker.get_utilization(ExchangeEnum.ZONDA) is None

    def test_record_call_increments_rate_for_exchange(self) -> None:
        """Given 5 recorded calls, the 1 s rate reports 5 req/s.

        Given:
            5 ``record_call`` invocations for Kraken within the
            tracker's 1 s window,

        When:
            ``get_rate(KRAKEN, 1.0)`` is queried,

        Then:
            The returned value equals ``5 / 1.0 = 5.0`` req/s.
        """
        tracker = RestCallTracker()
        for _ in range(5):
            tracker.record_call(ExchangeEnum.KRAKEN)
        assert tracker.get_rate(ExchangeEnum.KRAKEN, 1.0) == pytest.approx(5.0)

    def test_rate_is_exchange_scoped(self) -> None:
        """Recording calls for exchange A does not inflate rates for exchange B.

        Given:
            3 calls recorded for Kraken and 0 for Walutomat,

        When:
            ``get_rate`` is queried for each exchange,

        Then:
            Kraken reports 3, Walutomat reports 0 — per-exchange
            sliding windows are isolated.
        """
        tracker = RestCallTracker()
        for _ in range(3):
            tracker.record_call(ExchangeEnum.KRAKEN)
        assert tracker.get_rate(ExchangeEnum.KRAKEN, 1.0) == pytest.approx(3.0)
        assert tracker.get_rate(ExchangeEnum.WALUTOMAT, 1.0) == pytest.approx(0.0)

    def test_utilization_respects_published_limit(self) -> None:
        """Given 10 calls in the 1 s window, utilization = 10 / configured_limit.

        Given:
            Tracker with Walutomat limit of 20 req/s, 10 calls
            recorded,

        When:
            ``get_utilization(WALUTOMAT)`` is queried,

        Then:
            Returns ``0.5`` (10 / 20).
        """
        tracker = RestCallTracker(limits={ExchangeEnum.WALUTOMAT: 20.0})
        for _ in range(10):
            tracker.record_call(ExchangeEnum.WALUTOMAT)
        assert tracker.get_utilization(ExchangeEnum.WALUTOMAT) == pytest.approx(0.5)

    def test_utilization_returns_none_for_unknown_exchange(self) -> None:
        """Exchanges without a published limit never report utilization.

        Given:
            A tracker with no ``zonda`` entry in ``_limits``,

        When:
            Calls are recorded and utilization queried,

        Then:
            Utilization is ``None`` — the caller must interpret that
            as "no limit known", not "zero utilization".
        """
        tracker = RestCallTracker(limits={})
        tracker.record_call(ExchangeEnum.ZONDA)
        assert tracker.get_utilization(ExchangeEnum.ZONDA) is None

    def test_window_trims_old_events(self) -> None:
        """Events older than 60 s are trimmed on the next record_call.

        Given:
            A tracker with a manually injected event older than 60
            seconds ago,

        When:
            A fresh call is recorded,

        Then:
            The stale entry is trimmed and ``get_rate`` does not
            include it — the tracker is bounded memory.
        """
        tracker = RestCallTracker()
        stale = time.monotonic() - 120.0
        tracker._events[ExchangeEnum.KRAKEN] = deque([stale])
        tracker.record_call(ExchangeEnum.KRAKEN)
        events = tracker._events[ExchangeEnum.KRAKEN]
        assert len(events) == 1

    def test_get_rate_rejects_invalid_window(self) -> None:
        """Windows outside (0, 60] raise ValueError.

        Given:
            Any tracker,

        When:
            ``get_rate`` is called with a non-positive window or a
            window greater than 60 s,

        Then:
            ``ValueError`` is raised — the tracker does not extrapolate
            beyond its bounded history.
        """
        tracker = RestCallTracker()
        with pytest.raises(ValueError, match="window_s"):
            tracker.get_rate(ExchangeEnum.KRAKEN, 0.0)
        with pytest.raises(ValueError, match="window_s"):
            tracker.get_rate(ExchangeEnum.KRAKEN, 120.0)


class TestRestCallTrackerSnapshot:
    """Snapshot shape + semantics for API / logging consumers."""

    def test_snapshot_returns_empty_for_fresh_tracker(self) -> None:
        """Fresh tracker returns an empty dict from ``snapshot()``.

        Given:
            A tracker with no recorded events,

        When:
            ``snapshot()`` is queried,

        Then:
            An empty dict is returned (no exchange keys).
        """
        tracker = RestCallTracker()
        assert tracker.snapshot() == {}

    def test_snapshot_exposes_all_fields_per_exchange(self) -> None:
        """Snapshot per exchange includes rps_1s/10s/60s + limit_rps + utilization.

        Given:
            Calls recorded for an exchange with a known limit,

        When:
            ``snapshot()`` is queried,

        Then:
            The per-exchange dict carries all 5 fields with correct
            types and non-negative values.
        """
        tracker = RestCallTracker(limits={ExchangeEnum.KRAKEN: 15.0})
        for _ in range(3):
            tracker.record_call(ExchangeEnum.KRAKEN)
        snap = tracker.snapshot()
        assert set(snap) == {ExchangeEnum.KRAKEN}
        row = snap[ExchangeEnum.KRAKEN]
        assert set(row) == {"rps_1s", "rps_10s", "rps_60s", "limit_rps", "utilization"}
        assert row["limit_rps"] == pytest.approx(15.0)
        assert row["rps_1s"] == pytest.approx(3.0)
        assert row["utilization"] == pytest.approx(3.0 / 15.0)


class TestRestCallTrackerReset:
    """Reset helper clears all mutable tracker state."""

    def test_reset_clears_events_warnings_and_async_locks(self) -> None:
        """Reset drops recorded events, warnings, and throttling locks.

        Given:
            A tracker with mutable runtime state populated,

        When:
            ``reset`` is called,

        Then:
            Every mutable container becomes empty.
        """
        tracker = RestCallTracker(limits={ExchangeEnum.KRAKEN: 15.0})
        tracker.record_call(ExchangeEnum.KRAKEN)
        tracker._warned[ExchangeEnum.KRAKEN] = time.monotonic()
        tracker._async_locks[ExchangeEnum.KRAKEN] = asyncio.Lock()

        tracker.reset()

        assert tracker._events == {}
        assert tracker._warned == {}
        assert tracker._async_locks == {}


class TestRestCallTrackerWarningLog:
    """High-utilization warnings fire but stay rate-limited."""

    def test_warning_emitted_at_warn_threshold(self, loguru_sink: list[str]) -> None:
        """Crossing 80 % utilization emits a warning log line.

        Given:
            Tracker with Kraken limit of 10 req/s, and 8 calls
            recorded in the 1 s window,

        When:
            The 8th ``record_call`` triggers the utilization check,

        Then:
            A warning log entry is emitted naming the exchange and
            the utilization percentage (80 % crosses the WARN
            threshold).
        """
        tracker = RestCallTracker(limits={ExchangeEnum.KRAKEN: 10.0})
        for _ in range(8):
            tracker.record_call(ExchangeEnum.KRAKEN)
        assert any("utilization" in msg.lower() for msg in loguru_sink)

    def test_warning_not_emitted_below_threshold(self, loguru_sink: list[str]) -> None:
        """Utilization below 80 % stays silent.

        Given:
            Kraken limit of 10 req/s and 5 calls recorded,

        When:
            ``record_call`` is invoked,

        Then:
            No warning log entry is emitted — the tracker does not
            flood logs for normal operation.
        """
        tracker = RestCallTracker(limits={ExchangeEnum.KRAKEN: 10.0})
        for _ in range(5):
            tracker.record_call(ExchangeEnum.KRAKEN)
        assert not any("utilization" in msg.lower() for msg in loguru_sink)

    def test_warning_rate_limited_per_exchange(self, loguru_sink: list[str]) -> None:
        """Back-to-back high-util windows emit at most one warning per 60 s.

        Given:
            Tracker with Kraken limit of 5 req/s, driven to high
            utilization by recording 5 calls in quick succession,

        When:
            A second burst of 5 calls follows immediately (still
            inside the 60 s warning-rate-limit window),

        Then:
            Only the first burst emits a warning — subsequent
            crossings reuse the ``_warned`` timestamp and stay
            silent until 60 s elapse.
        """
        tracker = RestCallTracker(limits={ExchangeEnum.KRAKEN: 5.0})
        for _ in range(5):
            tracker.record_call(ExchangeEnum.KRAKEN)
        first_burst_count = sum(1 for msg in loguru_sink if "utilization" in msg.lower())
        for _ in range(5):
            tracker.record_call(ExchangeEnum.KRAKEN)
        total_count = sum(1 for msg in loguru_sink if "utilization" in msg.lower())
        assert first_burst_count >= 1
        assert total_count == first_burst_count


class TestRestCallTrackerConcurrency:
    """Thread-safety of ``record_call`` under concurrent pressure."""

    def test_concurrent_record_calls_do_not_lose_events(self) -> None:
        """Given 20 threads each recording 100 calls, total count is exactly 2000.

        Given:
            A bare tracker and 20 worker threads,

        When:
            Each thread calls ``record_call`` 100 times in a tight
            loop,

        Then:
            The stored deque holds exactly ``20 * 100 = 2000``
            entries — no events are lost to a race between append +
            trim.
        """
        tracker = RestCallTracker()
        workers = 20
        calls_per_worker = 100

        def worker() -> None:
            for _ in range(calls_per_worker):
                tracker.record_call(ExchangeEnum.KRAKEN)

        threads = [threading.Thread(target=worker) for _ in range(workers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(tracker._events[ExchangeEnum.KRAKEN]) == workers * calls_per_worker


class TestSingletonAccess:
    """``get_rest_call_tracker`` returns the same instance across calls."""

    def test_returns_same_instance_across_calls(self) -> None:
        """Given two consecutive ``get_rest_call_tracker`` calls, the instance is the same.

        Given:
            Process-scoped singleton factory,

        When:
            ``get_rest_call_tracker`` is invoked twice,

        Then:
            The same ``RestCallTracker`` object is returned — this is
            the shared process-scoped tracker that every exchange
            client writes to.
        """
        first = get_rest_call_tracker()
        second = get_rest_call_tracker()
        assert first is second

    def test_returns_seeded_instance_when_created_before_inner_check(self) -> None:
        """The inner singleton check reuses an instance seeded during lock entry.

        Given:
            The outer singleton check observed ``None``,

        When:
            Another execution path seeds the singleton before the
            inner check runs,

        Then:
            ``get_rest_call_tracker`` returns that seeded instance.
        """
        seeded = RestCallTracker()
        tracker_module._SingletonHolder.instance = None

        class _SeedLock:
            def __enter__(self) -> None:
                tracker_module._SingletonHolder.instance = seeded

            def __exit__(
                self,
                _exc_type: type[BaseException] | None,
                _exc: BaseException | None,
                _tb: object | None,
            ) -> None:
                return None

        with patch("snapper.infrastructure.rest.tracker._SingletonHolder.lock", _SeedLock()):
            resolved = get_rest_call_tracker()

        assert resolved is seeded

    def test_reset_clears_singleton(self) -> None:
        """Reset helper gives each test a fresh tracker.

        Given:
            Two ``get_rest_call_tracker`` calls separated by a
            ``reset_rest_call_tracker_for_tests`` invocation,

        When:
            The second call fetches the tracker after reset,

        Then:
            It returns a different instance from the pre-reset
            tracker — ensures per-test isolation.
        """
        first = get_rest_call_tracker()
        reset_rest_call_tracker_for_tests()
        second = get_rest_call_tracker()
        assert first is not second

    def test_published_limits_include_expected_exchanges(self) -> None:
        """Constant ``REST_RATE_LIMITS_PER_SECOND`` covers Walutomat + Kraken + Polygon.

        Given:
            Public API docs for the three documented limits,

        When:
            The constant is inspected,

        Then:
            Walutomat = 20 req/s, Kraken = 15 req/s, Polygon is
            configured based on the 5 req/min free tier
            (approximately 0.083 req/s). Undocumented exchanges
            (Zonda, Kraken Futures, Kraken Equities) are absent.
        """
        limits: dict[str, Any] = dict(REST_RATE_LIMITS_PER_SECOND)
        assert limits[ExchangeEnum.WALUTOMAT] == pytest.approx(20.0)
        assert limits[ExchangeEnum.KRAKEN] == pytest.approx(15.0)
        assert limits[ExchangeEnum.POLYGON] == pytest.approx(5.0 / 60.0)
        assert ExchangeEnum.ZONDA not in limits
        assert ExchangeEnum.KRAKEN_FUTURES not in limits
        assert ExchangeEnum.KRAKEN_EQUITIES not in limits


class TestRestCallTrackerAcquire:
    """Pre-emptive auto-backoff via ``acquire(exchange)``."""

    @pytest.mark.asyncio
    async def test_acquire_passes_through_when_no_limit(self) -> None:
        """Exchanges without a published limit record immediately with no sleep.

        Given:
            Tracker has no entry for Zonda,

        When:
            ``acquire(ZONDA)`` is awaited,

        Then:
            The call is recorded and the coroutine returns quickly
            (no asyncio.sleep fires) — observability-only for
            undocumented exchanges.
        """
        tracker = RestCallTracker(limits={})
        start = time.monotonic()
        await tracker.acquire(ExchangeEnum.ZONDA)
        elapsed = time.monotonic() - start
        assert tracker.get_rate(ExchangeEnum.ZONDA, 1.0) == pytest.approx(1.0)
        assert elapsed < 0.05

    @pytest.mark.asyncio
    async def test_acquire_does_not_sleep_under_limit(self) -> None:
        """Utilization below 100 % returns without waiting.

        Given:
            Tracker with Kraken limit 15/s and 3 calls already
            recorded,

        When:
            ``acquire(KRAKEN)`` is awaited,

        Then:
            The call returns in under 50 ms because 4/15 is well
            below the 1 s-window threshold.
        """
        tracker = RestCallTracker(limits={ExchangeEnum.KRAKEN: 15.0})
        for _ in range(3):
            tracker.record_call(ExchangeEnum.KRAKEN)
        start = time.monotonic()
        await tracker.acquire(ExchangeEnum.KRAKEN)
        elapsed = time.monotonic() - start
        assert elapsed < 0.05
        assert tracker.get_rate(ExchangeEnum.KRAKEN, 1.0) == pytest.approx(4.0)

    @pytest.mark.asyncio
    async def test_acquire_sleeps_when_at_limit(self) -> None:
        """At the limit, acquire sleeps until the oldest event ages out.

        Given:
            Tracker with Kraken limit 2/s and 2 events injected at
            roughly 400 ms ago,

        When:
            ``acquire(KRAKEN)`` is awaited,

        Then:
            The coroutine sleeps ~600 ms (1 s - 400 ms) so the
            oldest event falls out of the window before the new
            call is recorded — never exceeds the published limit.
        """
        tracker = RestCallTracker(limits={ExchangeEnum.KRAKEN: 2.0})
        now = time.monotonic()
        tracker._events[ExchangeEnum.KRAKEN] = deque([now - 0.4, now - 0.3])
        start = time.monotonic()
        await tracker.acquire(ExchangeEnum.KRAKEN)
        elapsed = time.monotonic() - start
        assert 0.4 < elapsed < 1.2
        assert tracker.get_rate(ExchangeEnum.KRAKEN, 1.0) >= 1.0

    @pytest.mark.asyncio
    async def test_acquire_serialises_concurrent_callers(self) -> None:
        """Concurrent ``acquire`` callers for the same exchange stay under the limit.

        Given:
            Tracker with Kraken limit 3/s, and 6 concurrent callers,

        When:
            All call ``acquire(KRAKEN)`` simultaneously,

        Then:
            The 4th-6th callers sleep until capacity frees up — total
            elapsed time is roughly ``ceil(6 / 3) - 1 = 1`` second
            because callers 4-6 have to wait for callers 1-3 to age
            out of the 1 s window. Final rate never exceeded the
            limit by more than 1 (acceptable micro-race window).
        """
        tracker = RestCallTracker(limits={ExchangeEnum.KRAKEN: 3.0})
        start = time.monotonic()
        await asyncio.gather(*[tracker.acquire(ExchangeEnum.KRAKEN) for _ in range(6)])
        elapsed = time.monotonic() - start
        assert elapsed >= 0.9
        assert tracker.get_rate(ExchangeEnum.KRAKEN, 1.0) <= 4.0


class TestExchangeClientBaseWiring:
    """``_record_rest_call`` on the base class delegates to the tracker."""

    def test_base_helper_records_with_exchange_name(self) -> None:
        """A subclass using ``_record_rest_call`` increments the tracker for its exchange_name.

        Given:
            A minimal ``ExchangeClientBase`` subclass constructed
            with ``exchange_name=ExchangeEnum.WALUTOMAT``,

        When:
            ``_record_rest_call`` is invoked,

        Then:
            The shared tracker reports a rate for Walutomat, proving
            the helper wired through ``exchange_name`` correctly.
        """

        class _Stub(ExchangeClientBase):
            async def connect(self) -> None: ...
            async def disconnect(self) -> None: ...
            async def get_ticker(self, symbol: str) -> Any: ...
            async def get_ohlcv(
                self, symbol: str, timeframe: str = "1m", since: Any = None, limit: Any = None
            ) -> Any: ...
            async def create_order(self, request: Any) -> Any: ...
            async def cancel_order(self, order_id: str, symbol: Any = None) -> Any: ...
            async def get_order(self, order_id: str, symbol: Any = None) -> Any: ...
            async def get_orders(
                self, symbol: Any = None, status: Any = None, limit: Any = None
            ) -> Any: ...
            async def get_balance(self, currency: Any = None) -> Any: ...
            def subscribe_ticks(self, symbols: list[str]) -> Any: ...
            def subscribe_candles(self, symbols: list[str], timeframe: str = "1m") -> Any: ...
            def subscribe_trades(self, symbols: list[str]) -> Any: ...
            def subscribe_executions(self) -> Any: ...
            def subscribe_instruments(self, **kwargs: Any) -> Any: ...

        stub = _Stub(repository=None, exchange_name=ExchangeEnum.WALUTOMAT)
        stub._record_rest_call()
        assert get_rest_call_tracker().get_rate(ExchangeEnum.WALUTOMAT, 1.0) == pytest.approx(1.0)
