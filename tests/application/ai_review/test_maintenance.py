"""Tests for the periodic AI-review maintenance driver and public seam."""

import asyncio
from collections.abc import Callable
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from typing import cast
from unittest.mock import MagicMock

import pytest

from snapper.application.ai_review import maintenance as maintenance_module
from snapper.application.ai_review.maintenance import DEFAULT_INTERVAL_SECONDS
from snapper.application.ai_review.maintenance import MIN_INTERVAL_SECONDS
from snapper.application.ai_review.maintenance import AiReviewMaintenanceService
from snapper.application.ai_review.maintenance import _resolve_interval
from snapper.application.ai_review.service import AiReviewService
from snapper.data.repository import Repository

_NOW = datetime(2026, 7, 22, 12, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _clear_singleton() -> Iterator[None]:
    """Reset the AI-review singleton around every case."""
    AiReviewService.clear_instance()
    yield
    AiReviewService.clear_instance()


class _RecordingMaintenanceTarget:
    """Record maintenance calls and optionally fail leading calls."""

    def __init__(
        self,
        *,
        counts: tuple[int, int] = (2, 3),
        failures: int = 0,
    ) -> None:
        """Store deterministic outcomes.

        Args:
            counts: Successful maintenance result.
            failures: Number of leading calls that raise.
        """
        self._counts = counts
        self._failures_remaining = failures
        self.calls: list[tuple[Repository, datetime | None]] = []

    async def maintenance_tick(
        self,
        *,
        repo: Repository,
        now: datetime | None = None,
    ) -> tuple[int, int]:
        """Record one call and return or raise the configured outcome."""
        self.calls.append((repo, now))
        if self._failures_remaining > 0:
            self._failures_remaining -= 1
            raise RuntimeError("maintenance unavailable")
        return self._counts


class _StopAfter:
    """Report stopped after a configured number of false reads."""

    def __init__(self, false_calls: int) -> None:
        """Store the number of leading false reads.

        Args:
            false_calls: Number of calls that report not stopped.
        """
        self._false_calls = false_calls
        self.calls = 0

    def is_set(self) -> bool:
        """Return whether the false-read budget has been exhausted."""
        self.calls += 1
        return self.calls > self._false_calls

    def set(self) -> None:
        """Satisfy the event interface."""

    def clear(self) -> None:
        """Satisfy the event interface."""


def _factory_for(repo: Repository) -> Callable[[], Repository]:
    """Return a repository factory for one sentinel.

    Args:
        repo: Repository sentinel returned by the factory.

    Returns:
        Zero-argument repository factory.
    """

    def _factory() -> Repository:
        """Return the configured sentinel."""
        return repo

    return _factory


def _make_driver(
    target: _RecordingMaintenanceTarget,
    repo: Repository,
) -> AiReviewMaintenanceService:
    """Build a driver with an explicit test cadence.

    Args:
        target: Recording maintenance seam.
        repo: Repository sentinel.

    Returns:
        Configured maintenance driver.
    """
    return AiReviewMaintenanceService(
        service=target,
        repository_factory=_factory_for(repo),
        interval_seconds=60.0,
    )


async def _no_sleep(_delay: float) -> None:
    """Replace ``asyncio.sleep`` without delaying a test."""


class TestIntervalResolution:
    """Environment cadence parsing."""

    @pytest.mark.parametrize(
        ("env_value", "expected"),
        [
            (None, DEFAULT_INTERVAL_SECONDS),
            ("", DEFAULT_INTERVAL_SECONDS),
            ("invalid", DEFAULT_INTERVAL_SECONDS),
            ("0", DEFAULT_INTERVAL_SECONDS),
            ("-5", DEFAULT_INTERVAL_SECONDS),
            ("1", MIN_INTERVAL_SECONDS),
            ("120", 120.0),
        ],
    )
    def test_resolve_interval(self, env_value: str | None, expected: float) -> None:
        """Invalid values default and positive values respect the floor."""
        assert _resolve_interval(env_value) == expected

    def test_constructor_reads_env_and_explicit_override_wins(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The environment is used only without an explicit cadence."""
        monkeypatch.setenv("AI_REVIEW_MAINTENANCE_INTERVAL_SECONDS", "90")
        target = _RecordingMaintenanceTarget()
        repo = cast(Repository, MagicMock())

        from_env = AiReviewMaintenanceService(
            service=target,
            repository_factory=_factory_for(repo),
        )
        explicit = AiReviewMaintenanceService(
            service=target,
            repository_factory=_factory_for(repo),
            interval_seconds=120.0,
        )

        assert from_env.interval_seconds == 90.0
        assert explicit.interval_seconds == 120.0


class TestMaintenanceTick:
    """Public seam and driver delegation."""

    @pytest.mark.asyncio
    async def test_public_seam_runs_reaper_then_offline_scanner(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Both private algorithms run in order with one repository and clock."""
        service = AiReviewService.get_instance()
        repo = cast(Repository, MagicMock())
        calls: list[str] = []

        async def _reaper(
            *,
            repo: Repository,
            now: datetime | None = None,
        ) -> int:
            """Record the reaper invocation."""
            assert repo is expected_repo
            assert now == _NOW
            calls.append("reaper")
            return 4

        async def _offline_scanner(
            *,
            repo: Repository,
            now: datetime | None = None,
        ) -> int:
            """Record the offline-scanner invocation."""
            assert repo is expected_repo
            assert now == _NOW
            calls.append("offline_scanner")
            return 5

        expected_repo = repo
        monkeypatch.setattr(service, "_reaper_tick", _reaper)
        monkeypatch.setattr(service, "_offline_scanner_tick", _offline_scanner)

        result = await service.maintenance_tick(repo=repo, now=_NOW)

        assert result == (4, 5)
        assert calls == ["reaper", "offline_scanner"]

    @pytest.mark.asyncio
    async def test_driver_tick_resolves_repository_and_calls_public_seam(self) -> None:
        """One driver pass delegates through the single public service seam."""
        target = _RecordingMaintenanceTarget(counts=(7, 11))
        repo = cast(Repository, MagicMock())
        driver = _make_driver(target, repo)

        result = await driver._tick(now=_NOW)

        assert result == (7, 11)
        assert target.calls == [(repo, _NOW)]


class TestLifecycle:
    """Research-trigger-shaped start, loop, and stop behavior."""

    @pytest.mark.asyncio
    async def test_start_takes_eager_tick_and_stop_cancels_loop(self) -> None:
        """Startup runs immediately and shutdown clears the owned task."""
        target = _RecordingMaintenanceTarget()
        repo = cast(Repository, MagicMock())
        driver = _make_driver(target, repo)

        await driver.start()

        assert driver._loop_task is not None
        assert target.calls == [(repo, None)]
        await driver.stop()
        assert driver._loop_task is None

    @pytest.mark.asyncio
    async def test_eager_tick_failure_does_not_block_loop_start(self) -> None:
        """A transient eager failure is isolated and leaves the loop live."""
        target = _RecordingMaintenanceTarget(failures=1)
        driver = _make_driver(target, cast(Repository, MagicMock()))

        await driver.start()

        assert driver._loop_task is not None
        assert len(target.calls) == 1
        await driver.stop()

    @pytest.mark.asyncio
    async def test_stop_before_start_and_after_done_task_is_tolerated(self) -> None:
        """Shutdown handles absent and already-completed loop tasks."""
        driver = _make_driver(
            _RecordingMaintenanceTarget(),
            cast(Repository, MagicMock()),
        )

        await driver.stop()
        assert driver._loop_task is None

        async def _instant() -> None:
            """Complete immediately."""

        task = asyncio.create_task(_instant())
        await task
        driver._loop_task = task

        await driver.stop()

        assert driver._loop_task is None

    @pytest.mark.asyncio
    async def test_raising_tick_does_not_kill_loop(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The loop retries after one maintenance pass raises."""
        target = _RecordingMaintenanceTarget(failures=1)
        driver = _make_driver(target, cast(Repository, MagicMock()))
        monkeypatch.setattr(maintenance_module.asyncio, "sleep", _no_sleep)
        driver._stopping = cast(asyncio.Event, _StopAfter(4))

        await driver._loop()

        assert len(target.calls) == 2

    @pytest.mark.asyncio
    async def test_loop_skips_tick_when_stopped_during_sleep(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A stop observed after sleep returns before maintenance."""
        target = _RecordingMaintenanceTarget()
        driver = _make_driver(target, cast(Repository, MagicMock()))
        monkeypatch.setattr(maintenance_module.asyncio, "sleep", _no_sleep)
        driver._stopping = cast(asyncio.Event, _StopAfter(1))

        await driver._loop()

        assert target.calls == []
