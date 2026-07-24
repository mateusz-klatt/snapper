"""Tests for the AI-delegate liveness and response watchdog."""

import asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.ai_review import watchdog as watchdog_module
from snapper.application.ai_review.watchdog import DEFAULT_STARTUP_GRACE_SECONDS
from snapper.application.ai_review.watchdog import DEFAULT_TIMEOUT_COUNT_THRESHOLD
from snapper.application.ai_review.watchdog import DEFAULT_TIMEOUT_WINDOW_SECONDS
from snapper.application.ai_review.watchdog import LIVENESS_WINDOW_SECONDS
from snapper.application.ai_review.watchdog import AiDelegateWatchdog
from snapper.core.types import HealthStatusEnum
from snapper.data.repository import Repository
from snapper.messaging.schemas.data import HeartbeatData

_NOW = datetime(2026, 7, 21, 12, 0, tzinfo=UTC)


class _TrackerStub:
    """Minimal sequence tracker satisfying the publisher protocol."""

    def __init__(self) -> None:
        """Start transport sequences at zero."""
        self.session_id = "session-ai-watchdog"
        self.streams: list[str] = []
        self._sequence = 0

    def next_sequence(self, stream: str) -> int:
        """Record a stream and return its next transport sequence.

        Args:
            stream: Topic being published.

        Returns:
            Next monotonically increasing sequence.
        """
        self.streams.append(stream)
        self._sequence += 1
        return self._sequence


def _make_publisher() -> MagicMock:
    """Build a publisher with synchronous tracking and async sending.

    Returns:
        Configured publisher mock.
    """
    publisher = MagicMock()
    publisher.tracker = _TrackerStub()
    publisher.send = AsyncMock()
    return publisher


def _make_repo(*, has_live_delegate: bool, timeout_count: int) -> MagicMock:
    """Build a repository mock for both watchdog scalar reads.

    Args:
        has_live_delegate: Liveness result returned to the watchdog.
        timeout_count: Recent timeout count returned to the watchdog.

    Returns:
        Configured repository mock.
    """
    repo = MagicMock()
    repo.has_live_ai_delegate = AsyncMock(return_value=has_live_delegate)
    repo.count_ai_review_timeouts_since = AsyncMock(return_value=timeout_count)
    return repo


def _make_watchdog(
    repo: MagicMock,
    publisher: MagicMock | None,
    *,
    interval_seconds: float = 30.0,
    startup_grace_seconds: float = 0.0,
    timeout_window_seconds: int = DEFAULT_TIMEOUT_WINDOW_SECONDS,
    timeout_count_threshold: int = DEFAULT_TIMEOUT_COUNT_THRESHOLD,
) -> AiDelegateWatchdog:
    """Build a watchdog with explicit deterministic configuration.

    Args:
        repo: Repository mock.
        publisher: Publisher mock or ``None``.
        interval_seconds: Poll cadence.
        startup_grace_seconds: Delay before the initial liveness read.
        timeout_window_seconds: Timeout lookback.
        timeout_count_threshold: Warning trigger count.

    Returns:
        Configured watchdog.
    """
    return AiDelegateWatchdog(
        repo=cast(Repository, repo),
        msg_publisher=publisher,
        interval_seconds=interval_seconds,
        startup_grace_seconds=startup_grace_seconds,
        timeout_window_seconds=timeout_window_seconds,
        timeout_count_threshold=timeout_count_threshold,
    )


async def _no_sleep(delay: float) -> None:
    """Replace sleep with an immediate coroutine.

    Args:
        delay: Ignored requested delay.
    """


class _StopAfter:
    """Event stand-in that reports stopped after a call budget."""

    def __init__(self, false_calls: int) -> None:
        """Set the number of leading false results.

        Args:
            false_calls: Number of ``is_set`` calls returning false.
        """
        self._false_calls = false_calls
        self.calls = 0

    def is_set(self) -> bool:
        """Return false until the configured call budget is exhausted.

        Returns:
            Whether the synthetic event is set.
        """
        self.calls += 1
        return self.calls > self._false_calls

    def set(self) -> None:
        """Satisfy the event interface without changing the call budget."""

    def clear(self) -> None:
        """Satisfy the event interface without changing the call budget."""


class TestLifecycle:
    """Lifecycle parity with other API-lifespan monitors."""

    @pytest.mark.asyncio
    async def test_start_checks_after_grace_and_stop_cancels_loop(self) -> None:
        """Start schedules the initial read and stop clears its task.

        Given: A healthy repository and no publisher,
        When: The watchdog starts and stops,
        Then: Both DB signals are read once and the loop task is cleared.
        """
        repo = _make_repo(has_live_delegate=True, timeout_count=0)
        watchdog = _make_watchdog(repo, None, interval_seconds=17.0)

        watchdog.start()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert watchdog.interval_seconds == 17.0
        assert watchdog._loop_task is not None
        repo.has_live_ai_delegate.assert_awaited_once()
        repo.count_ai_review_timeouts_since.assert_awaited_once()
        await watchdog.stop()
        assert watchdog._loop_task is None

    @pytest.mark.asyncio
    async def test_startup_grace_prevents_restart_false_page(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A delegate gets one reconnect interval before offline paging.

        Given: Stale persisted liveness while the API is still starting,
        When: The watchdog task enters its default startup grace,
        Then: It publishes nothing until the grace ends and then detects offline state.
        """
        repo = _make_repo(has_live_delegate=False, timeout_count=0)
        publisher = _make_publisher()
        grace_started = asyncio.Event()
        release_grace = asyncio.Event()
        first_frame_sent = asyncio.Event()
        real_sleep = asyncio.sleep

        async def controlled_sleep(delay: float) -> None:
            """Hold only the first default-grace sleep.

            Args:
                delay: Requested sleep duration.
            """
            if delay == DEFAULT_STARTUP_GRACE_SECONDS and not grace_started.is_set():
                grace_started.set()
                await release_grace.wait()
                return
            await real_sleep(delay)

        async def record_send(stream_key: str, data: HeartbeatData) -> None:
            """Record the first post-grace heartbeat frame.

            Args:
                stream_key: Heartbeat topic being published.
                data: Heartbeat frame being published.
            """
            first_frame_sent.set()

        monkeypatch.setattr(watchdog_module.asyncio, "sleep", controlled_sleep)
        publisher.send = AsyncMock(side_effect=record_send)
        watchdog = AiDelegateWatchdog(
            repo=cast(Repository, repo),
            msg_publisher=publisher,
        )

        watchdog.start()
        await asyncio.wait_for(grace_started.wait(), timeout=1.0)

        repo.has_live_ai_delegate.assert_not_awaited()
        publisher.send.assert_not_awaited()
        release_grace.set()
        await asyncio.wait_for(first_frame_sent.wait(), timeout=1.0)
        repo.has_live_ai_delegate.assert_awaited_once()
        publisher.send.assert_awaited_once()
        await watchdog.stop()

    @pytest.mark.asyncio
    async def test_start_does_not_wait_for_warning_publication(self) -> None:
        """A slow eager warning burst does not delay API readiness.

        Given: An offline delegate and a publisher blocked on its first frame,
        When: The watchdog starts,
        Then: Start returns while the owned task remains inside publication.
        """
        repo = _make_repo(has_live_delegate=False, timeout_count=0)
        publisher = _make_publisher()
        publication_started = asyncio.Event()
        publication_blocked = asyncio.Event()

        async def blocked_send(stream_key: str, data: HeartbeatData) -> None:
            """Hold the first frame until the watchdog task is cancelled.

            Args:
                stream_key: Ignored heartbeat topic.
                data: Ignored heartbeat frame.
            """
            publication_started.set()
            await publication_blocked.wait()

        publisher.send = AsyncMock(side_effect=blocked_send)
        watchdog = _make_watchdog(repo, publisher)

        watchdog.start()
        await asyncio.wait_for(publication_started.wait(), timeout=1.0)

        assert watchdog._loop_task is not None
        assert not watchdog._loop_task.done()
        publisher.send.assert_awaited_once()
        await watchdog.stop()
        assert watchdog._loop_task is None

    @pytest.mark.asyncio
    async def test_initial_read_failure_does_not_block_loop_start(self) -> None:
        """A transient initial failure is swallowed and the monitor continues.

        Given: A repository whose liveness query raises,
        When: The watchdog starts,
        Then: Startup succeeds with a loop task ready to retry.
        """
        repo = _make_repo(has_live_delegate=True, timeout_count=0)
        repo.has_live_ai_delegate = AsyncMock(side_effect=RuntimeError("database unavailable"))
        watchdog = _make_watchdog(repo, None)

        watchdog.start()
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert watchdog._loop_task is not None
        repo.has_live_ai_delegate.assert_awaited_once()
        repo.count_ai_review_timeouts_since.assert_not_awaited()
        await watchdog.stop()

    @pytest.mark.asyncio
    async def test_stop_tolerates_missing_and_finished_tasks(self) -> None:
        """Stop handles both never-started and already-finished states.

        Given: One never-started watchdog and one completed task,
        When: Stop runs for both states,
        Then: Both return with an empty task slot.
        """
        watchdog = _make_watchdog(_make_repo(has_live_delegate=True, timeout_count=0), None)
        await watchdog.stop()
        assert watchdog._loop_task is None

        async def finish_immediately() -> None:
            """Return immediately to create a completed task."""

        task = asyncio.create_task(finish_immediately())
        await task
        watchdog._loop_task = task
        await watchdog.stop()
        assert watchdog._loop_task is None


class TestLoop:
    """Deterministic poll-loop mechanics."""

    @pytest.mark.asyncio
    async def test_loop_ticks_once_then_observes_stop(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One permitted loop iteration performs one full watchdog tick.

        Given: A stopper allowing one iteration and sleep patched out,
        When: The loop runs,
        Then: Both repository signals are queried once.
        """
        repo = _make_repo(has_live_delegate=True, timeout_count=0)
        watchdog = _make_watchdog(repo, None)
        monkeypatch.setattr(watchdog_module.asyncio, "sleep", _no_sleep)
        watchdog._stopping = cast(asyncio.Event, _StopAfter(2))

        await watchdog._loop()

        repo.has_live_ai_delegate.assert_awaited_once()
        repo.count_ai_review_timeouts_since.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_loop_skips_tick_when_stopped_during_sleep(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A stop arriving during sleep prevents another DB read.

        Given: A stopper that flips after the first loop check,
        When: The loop wakes from its patched sleep,
        Then: It exits before querying the repository.
        """
        repo = _make_repo(has_live_delegate=True, timeout_count=0)
        watchdog = _make_watchdog(repo, None)
        monkeypatch.setattr(watchdog_module.asyncio, "sleep", _no_sleep)
        watchdog._stopping = cast(asyncio.Event, _StopAfter(1))

        await watchdog._loop()

        repo.has_live_ai_delegate.assert_not_awaited()
        repo.count_ai_review_timeouts_since.assert_not_awaited()


class TestDetection:
    """Status selection and heartbeat publishing for both signals."""

    @pytest.mark.asyncio
    async def test_healthy_tick_publishes_one_reset_frame(self) -> None:
        """A live answering delegate emits a single HEALTHY heartbeat.

        Given: A live delegate and no recent timeout,
        When: A tick uses the wall clock default,
        Then: One HEALTHY frame resets the critical rule's warning state.
        """
        repo = _make_repo(has_live_delegate=True, timeout_count=0)
        publisher = _make_publisher()
        watchdog = _make_watchdog(repo, publisher)

        await watchdog._tick()

        publisher.send.assert_awaited_once()
        topic, frame = publisher.send.await_args.args
        assert topic == "system.heartbeats.ai_delegate.global"
        assert isinstance(frame, HeartbeatData)
        assert frame.component == "ai_delegate.global"
        assert frame.status == HealthStatusEnum.HEALTHY
        assert frame.sequence == 1
        assert frame.session_id == "session-ai-watchdog"
        assert frame.lag_ms == 0
        assert frame.meta["reason"] == "healthy"
        assert frame.meta["has_live_delegate"] is True
        assert frame.meta["timeout_count"] == 0

    @pytest.mark.asyncio
    async def test_offline_delegate_publishes_warning_burst(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Missing liveness alone opens the existing three-frame gate.

        Given: No live delegate and no timeout rows,
        When: A tick evaluates the global scope,
        Then: Three spaced WARNING frames carry liveness forensics.
        """
        repo = _make_repo(has_live_delegate=False, timeout_count=0)
        publisher = _make_publisher()
        watchdog = _make_watchdog(repo, publisher)
        sleeps: list[float] = []

        async def record_sleep(delay: float) -> None:
            """Record each inter-frame delay.

            Args:
                delay: Requested sleep duration.
            """
            sleeps.append(delay)

        monkeypatch.setattr(watchdog_module.asyncio, "sleep", record_sleep)

        await watchdog._tick(now=_NOW)

        assert publisher.send.await_count == 3
        assert sleeps == [2.0, 2.0]
        topic, frame = publisher.send.await_args.args
        assert topic == "system.heartbeats.ai_delegate.global"
        assert frame.status == HealthStatusEnum.WARNING
        assert frame.sequence == 3
        assert frame.meta["synthetic"] is True
        assert frame.meta["origin"] == "ai_delegate_watchdog"
        assert frame.meta["scope"] == "global"
        assert frame.meta["reason"] == "no_live_delegate"
        assert frame.meta["has_live_delegate"] is False
        assert frame.meta["heartbeat_window_seconds"] == LIVENESS_WINDOW_SECONDS
        assert frame.meta["timeout_count_threshold"] == 1
        assert frame.meta["timeout_window_seconds"] == 3600
        assert frame.meta["evaluated_at"] == _NOW.isoformat()
        assert publisher.tracker.streams == ["system.heartbeats.ai_delegate.global"] * 3

    @pytest.mark.asyncio
    async def test_timeout_threshold_publishes_warning_with_live_delegate(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A connected but non-answering delegate emits WARNING.

        Given: A live delegate and one recent timeout at the threshold,
        When: A tick runs,
        Then: The response-failure reason produces one warning burst.
        """
        repo = _make_repo(has_live_delegate=True, timeout_count=1)
        publisher = _make_publisher()
        watchdog = _make_watchdog(repo, publisher)
        monkeypatch.setattr(watchdog_module.asyncio, "sleep", _no_sleep)

        await watchdog._tick(now=_NOW)

        assert publisher.send.await_count == 3
        _, frame = publisher.send.await_args.args
        assert frame.meta["reason"] == "timeout_no_response"
        repo.count_ai_review_timeouts_since.assert_awaited_once_with(
            since=_NOW - timedelta(seconds=DEFAULT_TIMEOUT_WINDOW_SECONDS),
            as_of=_NOW,
        )

    @pytest.mark.asyncio
    async def test_both_failures_share_one_combined_warning_burst(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Simultaneous signals do not duplicate the critical-rule burst.

        Given: No live delegate and multiple recent timeouts,
        When: A tick evaluates both signals,
        Then: One three-frame burst carries the combined reason.
        """
        repo = _make_repo(has_live_delegate=False, timeout_count=3)
        publisher = _make_publisher()
        watchdog = _make_watchdog(repo, publisher)
        monkeypatch.setattr(watchdog_module.asyncio, "sleep", _no_sleep)

        await watchdog._tick(now=_NOW)

        assert publisher.send.await_count == 3
        _, frame = publisher.send.await_args.args
        assert frame.meta["reason"] == "no_live_delegate_and_timeout_no_response"

    @pytest.mark.asyncio
    async def test_timeout_below_custom_threshold_stays_healthy(self) -> None:
        """A configurable higher count leaves a sub-threshold tick healthy.

        Given: A live delegate, one timeout, and a threshold of two,
        When: A tick evaluates the signals,
        Then: One HEALTHY frame is published.
        """
        repo = _make_repo(has_live_delegate=True, timeout_count=1)
        publisher = _make_publisher()
        watchdog = _make_watchdog(repo, publisher, timeout_count_threshold=2)

        await watchdog._tick(now=_NOW)

        publisher.send.assert_awaited_once()
        _, frame = publisher.send.await_args.args
        assert frame.status == HealthStatusEnum.HEALTHY

    @pytest.mark.asyncio
    async def test_missing_publisher_retains_detection_without_transport(self) -> None:
        """Publisher absence is a clean logging-only mode.

        Given: An offline result and no publisher,
        When: A tick evaluates the signals,
        Then: Both reads complete without a transport error.
        """
        repo = _make_repo(has_live_delegate=False, timeout_count=0)
        watchdog = _make_watchdog(repo, None)

        await watchdog._tick(now=_NOW)

        repo.has_live_ai_delegate.assert_awaited_once_with(
            heartbeat_window_seconds=LIVENESS_WINDOW_SECONDS,
            as_of=_NOW,
        )
        repo.count_ai_review_timeouts_since.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_frame_failures_are_swallowed_and_sequences_continue(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failed frame does not prevent later burst frames or retries.

        Given: A publisher failing on the first and third sends,
        When: An offline warning burst is published,
        Then: All three sends are attempted and sequences still advance.
        """
        repo = _make_repo(has_live_delegate=False, timeout_count=0)
        publisher = _make_publisher()
        publisher.send = AsyncMock(side_effect=[RuntimeError("first"), None, RuntimeError("third")])
        watchdog = _make_watchdog(repo, publisher)
        monkeypatch.setattr(watchdog_module.asyncio, "sleep", _no_sleep)

        await watchdog._tick(now=_NOW)

        assert publisher.send.await_count == 3
        assert watchdog._sequence == 3
