"""Repository-owned notification attempt allocation and stale-writer fences."""

from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.notify.retry_schedule import RetryScheduleUpdate
from snapper.application.notify.retry_schedule import persist_retry_schedule
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AlertDelivery
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import AlertDeliveryInsertRow
from snapper.data.repository_types import AlertDeliveryRow


def _at(minutes: int = 0) -> datetime:
    """Return a deterministic transition time."""
    return datetime(2026, 10, 1, tzinfo=UTC) + timedelta(minutes=minutes)


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncGenerator[SQLAlchemyRepository]:
    """Provide a real isolated SQLite repository."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path}/attempts.db")
    await repository.create_all()
    try:
        yield repository
    finally:
        await repository.engine.dispose()


async def _seed(repo: SQLAlchemyRepository, attempt_count: int = 0) -> str:
    """Seed one queued delivery with explicit attempt history."""
    return await repo.insert_alert_delivery(
        AlertDeliveryInsertRow(
            alert_event_public_id="event-attempt",
            device_public_id="device-attempt",
            user_public_id="user-attempt",
            status="queued",
            attempt_count=attempt_count,
            created_at=_at(),
            timestamp=_at(),
            session_id="seed",
            sequence_id=1,
        )
    )


async def _history(repo: SQLAlchemyRepository, public_id: str) -> list[AlertDelivery]:
    """Read all versions in durable insertion order."""
    async with repo.session() as session:
        result = await session.execute(
            select(AlertDelivery)
            .where(AlertDelivery.public_id == public_id)
            .order_by(AlertDelivery.id)
        )
        return list(result.scalars().all())


@pytest.mark.asyncio
@pytest.mark.parametrize("second_minute", [1, 2])
async def test_committed_attempts_allocate_monotonic_numbers(
    repo: SQLAlchemyRepository, second_minute: int
) -> None:
    """Commits own distinct numbers.

    Given a queued delivery,
    When twice allocated,
    Then commits own distinct numbers.
    """
    public_id = await _seed(repo)
    first = await repo.begin_delivery_attempt(
        public_id, transition_at=_at(1), session_id="worker-a", sequence_id=2
    )
    second = await repo.begin_delivery_attempt(
        public_id, transition_at=_at(second_minute), session_id="worker-b", sequence_id=3
    )
    assert (first, second) == (1, 2)
    rows = await _history(repo, public_id)
    assert [row.attempt_count for row in rows] == [0, 1, 2]
    assert [row.timestamp for row in rows] == [_at(), _at(1), _at(second_minute)]
    assert [row.known_to for row in rows] == [_at(1), _at(second_minute), KNOWN_TO_MAX]
    assert [row.last_attempt_at for row in rows[1:]] == [_at(1), _at(second_minute)]
    assert [row.session_id for row in rows] == ["seed", "worker-a", "worker-b"]
    assert all(row.status == "queued" for row in rows)


@pytest.mark.asyncio
async def test_stale_schedule_cannot_rewind_committed_attempt(repo: SQLAlchemyRepository) -> None:
    """No history changes.

    Given attempt two,
    When attempt one's reply arrives,
    Then no history changes.
    """
    public_id = await _seed(repo, attempt_count=2)
    assert await repo.update_delivery_retry_schedule(
        public_id,
        attempt_count=2,
        next_attempt_at=_at(10),
        error_reason="newer",
        transition_at=_at(2),
        session_id="newer",
        sequence_id=2,
    )
    result = await repo.update_delivery_retry_schedule(
        public_id,
        attempt_count=1,
        next_attempt_at=_at(30),
        error_reason="stale",
        transition_at=_at(3),
        session_id="stale",
        sequence_id=3,
    )
    assert result is False
    rows = await _history(repo, public_id)
    assert len(rows) == 2
    assert rows[0].known_to == _at(2)
    assert rows[1].known_to == KNOWN_TO_MAX
    assert rows[1].attempt_count == 2
    assert rows[1].next_attempt_at == _at(10)
    assert rows[1].error_reason == "newer"
    assert rows[1].timestamp == _at(2)
    assert rows[1].session_id == "newer"


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["missing", "sent", "cancelled_scope"])
async def test_allocation_and_schedule_refuse_nonqueued(
    repo: SQLAlchemyRepository, status: str
) -> None:
    """History is preserved.

    Given no queued version,
    When allocation or scheduling runs,
    Then history is preserved.
    """
    public_id = await _seed(repo)
    if status == "sent":
        await repo.mark_delivery_sent(
            public_id, "apns-sent", transition_at=_at(1), session_id="terminal", sequence_id=2
        )
    elif status == "cancelled_scope":
        await repo.mark_delivery_cancelled(
            public_id, "revoked", transition_at=_at(1), session_id="terminal", sequence_id=2
        )
    else:
        public_id = "missing"
    before = await _history(repo, public_id)
    assert (
        await repo.begin_delivery_attempt(
            public_id, transition_at=_at(2), session_id="worker", sequence_id=3
        )
        is None
    )
    assert (
        await repo.update_delivery_retry_schedule(
            public_id,
            attempt_count=0,
            next_attempt_at=_at(30),
            error_reason="late",
            transition_at=_at(3),
            session_id="worker",
            sequence_id=4,
        )
        is False
    )
    after = await _history(repo, public_id)
    assert [(row.id, row.known_to, row.status) for row in after] == [
        (row.id, row.known_to, row.status) for row in before
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("allocate", [False, True])
async def test_selected_version_loses_to_committed_successor(
    repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch, allocate: bool
) -> None:
    """The stale CAS loses.

    Given a selected version,
    When another worker replaces it,
    Then the stale CAS loses.
    """
    public_id = await _seed(repo, attempt_count=2)
    execute = AsyncSession.execute
    raced = False

    async def intercept(session: AsyncSession, statement: Any, *args: Any, **kwargs: Any) -> Any:
        """Commit a real competing successor just before the stale close statement."""
        nonlocal raced
        if not raced and str(statement).startswith("UPDATE alert_deliveries"):
            raced = True
            assert await repo.update_delivery_retry_schedule(
                public_id,
                attempt_count=2,
                next_attempt_at=_at(20),
                error_reason="winner",
                transition_at=_at(1),
                session_id="winner",
                sequence_id=2,
            )
        return await execute(session, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "execute", intercept)
    if allocate:
        assert (
            await repo.begin_delivery_attempt(
                public_id, transition_at=_at(2), session_id="loser", sequence_id=3
            )
            is None
        )
    else:
        assert (
            await repo.update_delivery_retry_schedule(
                public_id,
                attempt_count=2,
                next_attempt_at=_at(30),
                error_reason="loser",
                transition_at=_at(2),
                session_id="loser",
                sequence_id=3,
            )
            is False
        )
    assert raced
    rows = await _history(repo, public_id)
    assert len(rows) == 2
    assert rows[0].known_to == _at(1)
    assert rows[1].known_to == KNOWN_TO_MAX
    assert rows[1].timestamp == _at(1)
    assert rows[1].session_id == "winner"
    assert rows[1].attempt_count == 2
    assert rows[1].next_attempt_at == _at(20)
    assert rows[1].error_reason == "winner"


@pytest.mark.asyncio
async def test_matching_zero_attempt_schedule_reports_success(repo: SQLAlchemyRepository) -> None:
    """Success is true and count stays zero.

    Given count zero,
    When the matching schedule commits,
    Then success is true and count stays zero.
    """
    public_id = await _seed(repo)
    assert (
        await repo.update_delivery_retry_schedule(
            public_id,
            attempt_count=0,
            next_attempt_at=_at(10),
            error_reason="deferred",
            transition_at=_at(1),
            session_id="scheduler",
            sequence_id=2,
        )
        is True
    )
    rows = await _history(repo, public_id)
    assert len(rows) == 2
    assert [row.attempt_count for row in rows] == [0, 0]
    assert rows[0].known_to == _at(1)
    assert rows[1].known_to == KNOWN_TO_MAX
    assert rows[1].next_attempt_at == _at(10)


@pytest.mark.asyncio
async def test_retry_schedule_rejects_stale_transition_at_and_accepts_non_stale(
    repo: SQLAlchemyRepository,
) -> None:
    """Retry schedules reject stale timestamps and accept monotonic ones.

    Given: An active delivery version whose ``timestamp`` is ``_at(10)`` and
        ``attempt_count == 1``.
    When: ``update_delivery_retry_schedule`` is called first with a stale
        ``transition_at`` of ``_at(3)`` and then with ``_at(10)`` and
        ``_at(12)``.
    Then: The stale write returns ``False`` without modifying the active row,
        while non-stale writes (``>= existing.timestamp``) return ``True``
        and extend the SCD2 history monotonically.
    """
    public_id = await _seed(repo)
    assert (
        await repo.begin_delivery_attempt(
            public_id, transition_at=_at(10), session_id="worker", sequence_id=2
        )
        == 1
    )
    assert (
        await repo.update_delivery_retry_schedule(
            public_id,
            attempt_count=1,
            next_attempt_at=_at(25),
            error_reason="stale-delayed",
            transition_at=_at(3),
            session_id="recovery",
            sequence_id=3,
        )
        is False
    )
    rows_after_reject = await _history(repo, public_id)
    assert len(rows_after_reject) == 2
    assert rows_after_reject[1].timestamp == _at(10)
    assert rows_after_reject[1].known_to == KNOWN_TO_MAX
    assert rows_after_reject[1].next_attempt_at is None
    assert rows_after_reject[1].error_reason is None

    assert (
        await repo.update_delivery_retry_schedule(
            public_id,
            attempt_count=1,
            next_attempt_at=_at(25),
            error_reason="delayed-at-boundary",
            transition_at=_at(10),
            session_id="recovery",
            sequence_id=4,
        )
        is True
    )
    assert (
        await repo.update_delivery_retry_schedule(
            public_id,
            attempt_count=1,
            next_attempt_at=_at(30),
            error_reason="delayed-later",
            transition_at=_at(12),
            session_id="recovery",
            sequence_id=5,
        )
        is True
    )
    rows = await _history(repo, public_id)
    assert len(rows) == 4
    assert [row.timestamp for row in rows] == [_at(0), _at(10), _at(10), _at(12)]
    assert [row.known_to for row in rows] == [_at(10), _at(10), _at(12), KNOWN_TO_MAX]
    assert rows[3].next_attempt_at == _at(30)
    assert rows[3].error_reason == "delayed-later"
    assert rows[3].last_attempt_at == _at(12)


@pytest.mark.asyncio
async def test_retry_schedule_reapply_preserves_competing_schedule(
    repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep a schedule committed between the helper's read and reapply.

    Given: The first retry schedule write uses an older timestamp than the active attempt.
    When: Another writer schedules that attempt after the helper reads the unscheduled row.
    Then: The reapply refuses to replace the winner and leaves its version active.
    """
    public_id = await _seed(repo)
    assert (
        await repo.begin_delivery_attempt(
            public_id, transition_at=_at(10), session_id="worker", sequence_id=2
        )
        == 1
    )
    get_delivery = repo.get_delivery_by_public_id

    async def read_then_schedule(delivery_id: str) -> AlertDeliveryRow | None:
        """Return the preceding snapshot after committing a schedule in a new transaction."""
        active = await get_delivery(delivery_id)
        assert active is not None
        assert active["next_attempt_at"] is None
        assert await repo.update_delivery_retry_schedule(
            delivery_id,
            attempt_count=1,
            next_attempt_at=_at(20),
            error_reason="winner",
            transition_at=_at(11),
            session_id="winner",
            sequence_id=3,
        )
        return active

    monkeypatch.setattr(repo, "get_delivery_by_public_id", read_then_schedule)
    recovery_writes = iter([("recovery", 4), ("recovery", 5)])
    result = await persist_retry_schedule(
        repo,
        RetryScheduleUpdate(
            delivery_public_id=public_id,
            attempt_count=1,
            next_attempt_at=_at(30),
            error_reason="stale-delayed",
            transition_at=_at(3),
        ),
        clock=lambda: _at(12),
        allocate_write=lambda: next(recovery_writes),
    )

    assert result is False
    rows = await _history(repo, public_id)
    assert [row.timestamp for row in rows] == [_at(), _at(10), _at(11)]
    assert [row.known_to for row in rows] == [_at(10), _at(11), KNOWN_TO_MAX]
    assert [row.attempt_count for row in rows] == [0, 1, 1]
    assert rows[-1].status == "queued"
    assert rows[-1].session_id == "winner"
    assert rows[-1].next_attempt_at == _at(20)
    assert rows[-1].error_reason == "winner"
