"""Tests for the periodic AI-research trigger service."""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from uuid import uuid7

import pytest
from sqlalchemy import func
from sqlalchemy import select

from snapper.application.ai_research import trigger as trigger_module
from snapper.application.ai_research.trigger import DEFAULT_INTERVAL_SECONDS
from snapper.application.ai_research.trigger import MIN_INTERVAL_SECONDS
from snapper.application.ai_research.trigger import PERIODIC_TRIGGER
from snapper.application.ai_research.trigger import AiResearchTriggerService
from snapper.application.ai_research.trigger import _resolve_interval
from snapper.data.models import AiResearchRound
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import AiResearchRoundInsertRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import AiResearchRequestFrameData
from snapper.messaging.topics.validation import validate_topic

_T0 = datetime(2026, 7, 22, 8, 0, tzinfo=UTC)
_T1 = datetime(2026, 7, 22, 8, 30, tzinfo=UTC)


@pytest.fixture()
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create and dispose one isolated SQLite repository."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'trigger.db'}")
    await repo.create_all()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


def _make_publisher() -> MagicMock:
    """Build a publisher mock with real sequence tracking."""
    publisher = MagicMock()
    publisher.tracker = SequenceTracker()
    publisher.send = AsyncMock()
    return publisher


def _make_repo(round_public_id: str | None = None) -> MagicMock:
    """Build a repository mock returning one stable round identifier."""
    repo = MagicMock()
    repo.create_ai_research_round = AsyncMock(
        return_value=round_public_id if round_public_id is not None else str(uuid7())
    )
    return repo


def _make_trigger(
    repo: MagicMock,
    publisher: MagicMock | None,
) -> AiResearchTriggerService:
    """Build a trigger with explicit test configuration."""
    return AiResearchTriggerService(
        repo=cast(Repository, repo),
        msg_publisher=publisher,
        interval_seconds=60.0,
    )


async def _no_sleep(delay: float) -> None:
    """Replace ``asyncio.sleep`` without delaying a test."""


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
            ("900", 900.0),
        ],
    )
    def test_resolve_interval(self, env_value: str | None, expected: float) -> None:
        """Invalid values default and positive values respect the floor."""
        assert _resolve_interval(env_value) == expected

    def test_constructor_reads_env_and_explicit_override_wins(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The environment is used only when no explicit cadence is supplied."""
        monkeypatch.setenv("AI_RESEARCH_TRIGGER_INTERVAL_SECONDS", "90")
        repo = cast(Repository, _make_repo())

        from_env = AiResearchTriggerService(repo=repo)
        explicit = AiResearchTriggerService(repo=repo, interval_seconds=120.0)

        assert from_env.interval_seconds == 90.0
        assert explicit.interval_seconds == 120.0


class TestTick:
    """Round creation, supersession, and wake behavior."""

    @pytest.mark.asyncio
    async def test_tick_creates_pending_round_and_publishes_valid_wake(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """An empty plane gains one pending round and a validated wake frame."""
        publisher = _make_publisher()
        service = AiResearchTriggerService(
            repo=repository,
            msg_publisher=publisher,
            interval_seconds=60.0,
        )

        await service._tick(now=_T1)

        publisher.send.assert_awaited_once()
        topic, frame = publisher.send.await_args.args
        assert isinstance(topic, str)
        assert isinstance(frame, AiResearchRequestFrameData)
        valid, error = validate_topic(topic)
        assert valid, error
        assert topic == f"ai_research.{frame.round_public_id}.request"
        assert frame.type == "ai_research.request"
        assert frame.trigger == PERIODIC_TRIGGER
        assert frame.timestamp == _T1
        assert frame.session_id == publisher.tracker.session_id
        assert frame.sequence_id == 1
        round_row = await repository.get_ai_research_round(frame.round_public_id)
        assert round_row is not None
        assert round_row["status"] == "pending"
        assert round_row["trigger"] == PERIODIC_TRIGGER
        assert round_row["created_at"] == _T1

    @pytest.mark.asyncio
    async def test_tick_atomically_supersedes_existing_pending_round(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """Latest-wins creation leaves the predecessor terminal and one pending row."""
        original_id = await repository.create_ai_research_round(
            {"trigger": PERIODIC_TRIGGER, "created_at": _T0}
        )
        publisher = _make_publisher()
        service = AiResearchTriggerService(
            repo=repository,
            msg_publisher=publisher,
            interval_seconds=60.0,
        )

        await service._tick(now=_T1)

        original = await repository.get_ai_research_round(original_id)
        assert original is not None
        assert original["status"] == "superseded"
        assert original["resolved_at"] == _T1
        _topic, frame = publisher.send.await_args.args
        assert isinstance(frame, AiResearchRequestFrameData)
        successor = await repository.get_ai_research_round(frame.round_public_id)
        assert successor is not None
        assert successor["status"] == "pending"
        async with repository.session() as session:
            pending_count = await session.scalar(
                select(func.count(AiResearchRound.id)).where(AiResearchRound.status == "pending")
            )
        assert pending_count == 1

    @pytest.mark.asyncio
    async def test_publish_failure_keeps_committed_round(
        self,
        repository: SQLAlchemyRepository,
    ) -> None:
        """A transport failure cannot abort the persisted primary contract."""
        publisher = _make_publisher()
        publisher.send = AsyncMock(side_effect=RuntimeError("broker unavailable"))
        service = AiResearchTriggerService(
            repo=repository,
            msg_publisher=publisher,
            interval_seconds=60.0,
        )

        await service._tick(now=_T1)

        publisher.send.assert_awaited_once()
        _topic, frame = publisher.send.await_args.args
        assert isinstance(frame, AiResearchRequestFrameData)
        round_row = await repository.get_ai_research_round(frame.round_public_id)
        assert round_row is not None
        assert round_row["status"] == "pending"

    @pytest.mark.asyncio
    async def test_wall_clock_tick_tolerates_missing_publisher(self) -> None:
        """The default clock persists a round even without wake transport."""
        repo = _make_repo()
        service = _make_trigger(repo, None)
        before = datetime.now(UTC)

        await service._tick()

        after = datetime.now(UTC)
        row = cast(AiResearchRoundInsertRow, repo.create_ai_research_round.await_args.args[0])
        assert row["trigger"] == PERIODIC_TRIGGER
        assert before <= row["created_at"] <= after


class TestLifecycle:
    """Watchdog-style start, loop, and stop semantics."""

    @pytest.mark.asyncio
    async def test_start_takes_eager_tick_and_stop_cancels_loop(self) -> None:
        """Startup creates immediately and shutdown clears the owned task."""
        repo = _make_repo()
        service = _make_trigger(repo, None)

        await service.start()

        assert service._loop_task is not None
        repo.create_ai_research_round.assert_awaited_once()
        await service.stop()
        assert service._loop_task is None

    @pytest.mark.asyncio
    async def test_eager_tick_failure_does_not_block_loop_start(self) -> None:
        """A transient startup failure is isolated and retried by the loop."""
        repo = _make_repo()
        repo.create_ai_research_round = AsyncMock(side_effect=RuntimeError("database unavailable"))
        service = _make_trigger(repo, None)

        await service.start()

        assert service._loop_task is not None
        await service.stop()

    @pytest.mark.asyncio
    async def test_stop_before_start_and_after_done_task_is_tolerated(self) -> None:
        """Shutdown handles both absent and already-completed loop tasks."""
        service = _make_trigger(_make_repo(), None)

        await service.stop()
        assert service._loop_task is None

        async def instant() -> None:
            """Complete immediately."""

        task = asyncio.create_task(instant())
        await task
        service._loop_task = task

        await service.stop()

        assert service._loop_task is None

    @pytest.mark.asyncio
    async def test_loop_ticks_once_then_observes_stop(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """One open iteration sleeps and performs one guarded tick."""
        repo = _make_repo()
        service = _make_trigger(repo, None)
        monkeypatch.setattr(trigger_module.asyncio, "sleep", _no_sleep)
        service._stopping = cast(asyncio.Event, _StopAfter(2))

        await service._loop()

        repo.create_ai_research_round.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_loop_skips_tick_when_stopped_during_sleep(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A stop observed after sleep returns before creating a round."""
        repo = _make_repo()
        service = _make_trigger(repo, None)
        monkeypatch.setattr(trigger_module.asyncio, "sleep", _no_sleep)
        service._stopping = cast(asyncio.Event, _StopAfter(1))

        await service._loop()

        repo.create_ai_research_round.assert_not_awaited()
