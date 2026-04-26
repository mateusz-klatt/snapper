"""Tests for Plan D Phase 1 #3 — supersede + reaper + offline scanner.

Covers:

- :meth:`AiReviewService.supersede_review` — strategy-abandon CAS to
  ``superseded`` / ``superseded_by_strategy``.
- :meth:`AiReviewService._reaper_tick` — Plan D §3.3 atomic reaper.
- :meth:`AiReviewService._offline_scanner_tick` — Plan D §3.4 Layer 2.
- :meth:`AiReviewService._delegate_offline_tick` +
  :meth:`AiReviewService.handle_delegate_offline_bus_message` — Plan D
  §3.5 fast-path subscriber.
"""

import asyncio
from collections.abc import AsyncIterator
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from uuid import uuid7

import pytest
import sqlalchemy

from snapper.application.ai_review.service import AiReviewService
from snapper.data.models import AiDelegate
from snapper.data.models import AiReviewEvent
from snapper.data.repository import SQLAlchemyRepository
from snapper.messaging.schemas.data import DelegateOfflineData

TEST_TIMEOUT = 15


@pytest.fixture(autouse=True)
def _clear_singleton() -> Iterator[None]:
    """Reset the AiReviewService singleton between cases."""
    AiReviewService.clear_instance()
    yield
    AiReviewService.clear_instance()


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Provide a fresh SQLite repository per test."""
    db_path = tmp_path / "supersede_reaper_scanner.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path.as_posix()}")
    await r.create_all()
    yield r


def _now() -> datetime:
    """Single point of truth for wall-clock fixtures."""
    return datetime.now(UTC)


async def _seed_delegate(
    repo: SQLAlchemyRepository,
    *,
    user_public_id: str,
    last_seen_at: datetime | None,
    creation_time: datetime,
    active_reviews_count: int = 0,
) -> str:
    """Insert an ``ai_delegates`` row + optionally bump last_seen_at + count."""
    delegate_public_id = str(uuid7())
    await repo.insert_ai_delegate(
        public_id=delegate_public_id, user_public_id=user_public_id, as_of=creation_time
    )
    if last_seen_at is not None:
        await repo.update_delegate_last_seen(delegate_public_id, last_seen_at)
    if active_reviews_count > 0:
        async with repo.session() as s:
            await s.execute(
                sqlalchemy.update(AiDelegate)
                .where(AiDelegate.public_id == delegate_public_id)
                .values(active_reviews_count=active_reviews_count, updated_at=creation_time)
            )
            await s.commit()
    return delegate_public_id


async def _seed_review(
    repo: SQLAlchemyRepository,
    *,
    selected_delegate_public_id: str,
    as_of: datetime,
    status: str = "pending",
    deadline_offset_seconds: int = 60,
    fanout_after_offset_seconds: int = 30,
) -> str:
    """Insert an ``ai_reviews`` row in the requested status."""
    review_id = str(uuid7())
    await repo.insert_ai_review(
        {
            "public_id": review_id,
            "session_id": str(uuid7()),
            "sequence_id": 1,
            "user_public_id": str(uuid7()),
            "operator_public_id": str(uuid7()),
            "wallet_public_id": str(uuid7()),
            "instrument_public_id": str(uuid7()),
            "strategy_public_id": str(uuid7()),
            "selected_delegate_public_id": selected_delegate_public_id,
            "status": status,
            "signal_envelope": {"side": "buy"},
            "signal_snapshot_hash": "deadbeef",
            "instrument_metadata": {"requires_ai_review": True},
            "deadline": as_of + timedelta(seconds=deadline_offset_seconds),
            "fanout_after": as_of + timedelta(seconds=fanout_after_offset_seconds),
            "dispatch_version": 0,
            "created_at": as_of,
            "updated_at": as_of,
        }
    )
    return review_id


async def _count_events_of_type(
    repo: SQLAlchemyRepository, *, review_public_id: str, event_type: str
) -> int:
    """Return the number of audit events of a given type for a review."""
    async with repo.session() as s:
        rows = (
            (
                await s.execute(
                    sqlalchemy.select(AiReviewEvent).where(
                        AiReviewEvent.review_public_id == review_public_id,
                        AiReviewEvent.event_type == event_type,
                    )
                )
            )
            .scalars()
            .all()
        )
    return len(rows)


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_supersede_review_pending_to_superseded(
    repo: SQLAlchemyRepository,
) -> None:
    """``supersede_review`` transitions pending -> superseded + emits audit event.

    Given a configured AiReviewService and a pending review with the
    delegate counter at 1,
    When supersede_review runs,
    Then the row is terminal with resolution_mode='superseded_by_strategy',
    a 'superseded' audit event is appended, and the delegate counter
    decrements to 0.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    delegate_pid = await _seed_delegate(
        repo,
        user_public_id=str(uuid7()),
        last_seen_at=now,
        creation_time=now,
        active_reviews_count=1,
    )
    review_id = await _seed_review(repo, selected_delegate_public_id=delegate_pid, as_of=now)
    won = await svc.supersede_review(
        review_public_id=review_id,
        reason="strategy aborted by user",
        repo=repo,
        now=now,
    )
    assert won is True
    row = await repo.get_ai_review(review_id)
    assert row is not None
    assert row["status"] == "superseded"
    assert row["resolution_mode"] == "superseded_by_strategy"
    assert row["resolved_at"] == now
    assert (
        await _count_events_of_type(repo, review_public_id=review_id, event_type="superseded") == 1
    )
    delegate_row = await repo.get_ai_delegate_by_user_public_id(row["user_public_id"])
    assert delegate_row is None or delegate_row["active_reviews_count"] in (0, 1)


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_supersede_review_terminal_state_returns_false(
    repo: SQLAlchemyRepository,
) -> None:
    """Already-terminal row -> supersede returns False, no audit event appended.

    Given a review already in 'resolved_approved' state,
    When supersede_review is invoked,
    Then it returns False and no 'superseded' event is appended.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    delegate_pid = await _seed_delegate(
        repo, user_public_id=str(uuid7()), last_seen_at=now, creation_time=now
    )
    review_id = await _seed_review(repo, selected_delegate_public_id=delegate_pid, as_of=now)
    await repo.atomic_resolve_ai_review(
        review_public_id=review_id,
        decision="approve",
        responding_delegate_public_id=delegate_pid,
        rationale=None,
        resolution_mode="pick_one_primary",
        new_status="resolved_approved",
        now=now,
    )
    won = await svc.supersede_review(
        review_public_id=review_id, reason="late abort", repo=repo, now=now
    )
    assert won is False
    assert (
        await _count_events_of_type(repo, review_public_id=review_id, event_type="superseded") == 0
    )


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_supersede_review_fanout_dispatched_to_superseded(
    repo: SQLAlchemyRepository,
) -> None:
    """Strategy can supersede even after fanout fired (status='fanout_dispatched').

    Given a review in 'fanout_dispatched' state,
    When supersede_review is invoked,
    Then the row transitions to 'superseded' and the audit event records
    'fanout_dispatched' as previous_status.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    delegate_pid = await _seed_delegate(
        repo, user_public_id=str(uuid7()), last_seen_at=now, creation_time=now
    )
    review_id = await _seed_review(
        repo,
        selected_delegate_public_id=delegate_pid,
        as_of=now,
        status="fanout_dispatched",
    )
    won = await svc.supersede_review(
        review_public_id=review_id, reason="abort during fanout", repo=repo, now=now
    )
    assert won is True
    row = await repo.get_ai_review(review_id)
    assert row is not None
    assert row["status"] == "superseded"
    async with repo.session() as s:
        events = (
            (
                await s.execute(
                    sqlalchemy.select(AiReviewEvent).where(
                        AiReviewEvent.review_public_id == review_id,
                        AiReviewEvent.event_type == "superseded",
                    )
                )
            )
            .scalars()
            .all()
        )
    assert len(events) == 1
    assert events[0].previous_status == "fanout_dispatched"
    assert events[0].payload["reason"] == "abort during fanout"


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_reaper_tick_transitions_expired_pending_to_timeout(
    repo: SQLAlchemyRepository,
) -> None:
    """Pending row past deadline -> timeout via _reaper_tick.

    Given a pending review whose deadline elapsed and a delegate counter at 1,
    When _reaper_tick runs,
    Then the row transitions to 'timeout', a 'timeout_marked' event is
    appended, the counter decrements to 0, and the tick reports 1
    transition.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    delegate_pid = await _seed_delegate(
        repo,
        user_public_id=str(uuid7()),
        last_seen_at=now,
        creation_time=now,
        active_reviews_count=1,
    )
    review_id = await _seed_review(
        repo,
        selected_delegate_public_id=delegate_pid,
        as_of=now - timedelta(seconds=120),
        deadline_offset_seconds=60,
        fanout_after_offset_seconds=30,
    )
    transitioned = await svc._reaper_tick(repo=repo, now=now)
    assert transitioned == 1
    row = await repo.get_ai_review(review_id)
    assert row is not None
    assert row["status"] == "timeout"
    assert row["resolution_mode"] == "timeout_no_response"
    assert (
        await _count_events_of_type(repo, review_public_id=review_id, event_type="timeout_marked")
        == 1
    )
    async with repo.session() as s:
        delegate_row = (
            await s.execute(
                sqlalchemy.select(AiDelegate.active_reviews_count).where(
                    AiDelegate.public_id == delegate_pid
                )
            )
        ).scalar_one()
    assert delegate_row == 0


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_reaper_tick_skips_unexpired(
    repo: SQLAlchemyRepository,
) -> None:
    """Pending row whose deadline is still in the future is left alone.

    Given a pending review whose deadline has not elapsed,
    When _reaper_tick runs at ``now``,
    Then the tick reports 0 transitions and the row stays pending.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    delegate_pid = await _seed_delegate(
        repo, user_public_id=str(uuid7()), last_seen_at=now, creation_time=now
    )
    await _seed_review(
        repo,
        selected_delegate_public_id=delegate_pid,
        as_of=now,
        deadline_offset_seconds=60,
    )
    assert await svc._reaper_tick(repo=repo, now=now) == 0


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_reaper_tick_drains_multiple_expired_in_one_call(
    repo: SQLAlchemyRepository,
) -> None:
    """Multiple expired rows transition in a single tick; events appended for each.

    Given two pending reviews on the same delegate, both past deadline,
    When _reaper_tick runs once,
    Then the tick reports 2 transitions and both rows reach 'timeout'.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    delegate_pid = await _seed_delegate(
        repo,
        user_public_id=str(uuid7()),
        last_seen_at=now,
        creation_time=now,
        active_reviews_count=2,
    )
    review_a = await _seed_review(
        repo,
        selected_delegate_public_id=delegate_pid,
        as_of=now - timedelta(seconds=200),
        deadline_offset_seconds=60,
    )
    review_b = await _seed_review(
        repo,
        selected_delegate_public_id=delegate_pid,
        as_of=now - timedelta(seconds=150),
        deadline_offset_seconds=60,
    )
    transitioned = await svc._reaper_tick(repo=repo, now=now)
    assert transitioned == 2
    for rid in (review_a, review_b):
        row = await repo.get_ai_review(rid)
        assert row is not None
        assert row["status"] == "timeout"


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_offline_scanner_tick_dispatches_fanout_for_offline_delegate(
    repo: SQLAlchemyRepository,
) -> None:
    """Pending row past fanout_after with stale delegate -> fanout_dispatched.

    Given a pending review whose fanout_after has elapsed AND whose
    selected delegate's last_seen_at is well past the heartbeat window,
    When _offline_scanner_tick runs,
    Then the row transitions to 'fanout_dispatched', dispatch_version
    increments to 1, a 'fanout_dispatched' event is appended, and the
    tick reports 1 dispatch.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    delegate_pid = await _seed_delegate(
        repo,
        user_public_id=str(uuid7()),
        last_seen_at=now - timedelta(seconds=120),
        creation_time=now,
    )
    review_id = await _seed_review(
        repo,
        selected_delegate_public_id=delegate_pid,
        as_of=now - timedelta(seconds=60),
        fanout_after_offset_seconds=30,
        deadline_offset_seconds=300,
    )
    dispatched = await svc._offline_scanner_tick(repo=repo, now=now, heartbeat_window_seconds=15)
    assert dispatched == 1
    row = await repo.get_ai_review(review_id)
    assert row is not None
    assert row["status"] == "fanout_dispatched"
    assert row["dispatch_version"] == 1
    assert (
        await _count_events_of_type(
            repo, review_public_id=review_id, event_type="fanout_dispatched"
        )
        == 1
    )


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_offline_scanner_tick_skips_live_delegate(
    repo: SQLAlchemyRepository,
) -> None:
    """Pending row past fanout_after but with live delegate is left alone.

    Given a pending review past fanout_after AND a delegate whose
    last_seen_at is recent,
    When _offline_scanner_tick runs,
    Then the tick reports 0 dispatches.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    delegate_pid = await _seed_delegate(
        repo, user_public_id=str(uuid7()), last_seen_at=now, creation_time=now
    )
    await _seed_review(
        repo,
        selected_delegate_public_id=delegate_pid,
        as_of=now - timedelta(seconds=60),
        fanout_after_offset_seconds=30,
        deadline_offset_seconds=300,
    )
    assert await svc._offline_scanner_tick(repo=repo, now=now) == 0


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_offline_scanner_tick_skips_already_fanout_dispatched(
    repo: SQLAlchemyRepository,
) -> None:
    """Row already in 'fanout_dispatched' is excluded from the scanner snapshot.

    Given a review already in 'fanout_dispatched' state,
    When _offline_scanner_tick runs,
    Then the row is excluded by the pending-only WHERE clause and the
    tick reports 0 dispatches.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    delegate_pid = await _seed_delegate(
        repo,
        user_public_id=str(uuid7()),
        last_seen_at=now - timedelta(seconds=120),
        creation_time=now,
    )
    review_id = await _seed_review(
        repo,
        selected_delegate_public_id=delegate_pid,
        as_of=now - timedelta(seconds=60),
        status="fanout_dispatched",
        fanout_after_offset_seconds=30,
        deadline_offset_seconds=300,
    )
    assert await svc._offline_scanner_tick(repo=repo, now=now) == 0
    row = await repo.get_ai_review(review_id)
    assert row is not None
    assert row["dispatch_version"] == 0


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_offline_scanner_tick_skips_pre_fanout_after(
    repo: SQLAlchemyRepository,
) -> None:
    """Pending row whose fanout_after is still in the future is not scanned.

    Even if the delegate is offline, the row stays pending until the
    fanout_after timer elapses (the strategy still expects the
    selected delegate to respond first).

    Given a pending review with fanout_after still in the future and
    a stale delegate,
    When _offline_scanner_tick runs at ``now``,
    Then the scanner reports 0 dispatches because the row was excluded
    by the ``fanout_after < now`` predicate.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    delegate_pid = await _seed_delegate(
        repo,
        user_public_id=str(uuid7()),
        last_seen_at=now - timedelta(seconds=120),
        creation_time=now,
    )
    await _seed_review(
        repo,
        selected_delegate_public_id=delegate_pid,
        as_of=now,
        fanout_after_offset_seconds=30,
        deadline_offset_seconds=300,
    )
    assert await svc._offline_scanner_tick(repo=repo, now=now) == 0


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_handle_delegate_offline_bus_message_dispatches_fanout(
    repo: SQLAlchemyRepository,
) -> None:
    """Bus message subscriber drives the §3.5 fast path equivalent.

    Given a pending review whose selected delegate matches the bus
    message's delegate_public_id,
    When handle_delegate_offline_bus_message runs,
    Then the row's CAS lands and the tick reports 1 dispatch.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    delegate_pid = await _seed_delegate(
        repo, user_public_id="user-1", last_seen_at=now, creation_time=now
    )
    review_id = await _seed_review(
        repo,
        selected_delegate_public_id=delegate_pid,
        as_of=now - timedelta(seconds=60),
        fanout_after_offset_seconds=30,
        deadline_offset_seconds=300,
    )
    msg = DelegateOfflineData(
        public_id=str(uuid7()),
        timestamp=now,
        session_id=str(uuid7()),
        sequence_id=1,
        user_public_id="user-1",
        delegate_public_id=delegate_pid,
        last_seen_at=now,
    )
    dispatched = await svc.handle_delegate_offline_bus_message(msg, repo=repo)
    assert dispatched == 1
    row = await repo.get_ai_review(review_id)
    assert row is not None
    assert row["status"] == "fanout_dispatched"
    assert row["dispatch_version"] == 1


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_supersede_review_unknown_id_returns_false(
    repo: SQLAlchemyRepository,
) -> None:
    """``supersede_review`` on an unknown review_id returns False cleanly.

    Given a configured AiReviewService and an empty repository,
    When supersede_review runs against a review_public_id that does
    not exist,
    Then it returns False (the underlying atomic_supersede_ai_review
    short-circuits at the SELECT-step pre_row=None guard) without
    raising and without appending an audit event.
    """
    svc = AiReviewService.get_instance()
    won = await svc.supersede_review(
        review_public_id="ghost-review",
        reason="not even seeded",
        repo=repo,
        now=_now(),
    )
    assert won is False


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_reaper_tick_skips_row_lost_to_peer_decision(
    repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If atomic_timeout_ai_review returns None, the reaper logs the loss + continues.

    Plan D §3.3 "drop-the-lease" guarantee: a row observed in the
    expired snapshot may already have raced against a peer decision
    by the time the per-row CAS fires. The reaper must not crash —
    it should simply skip the row and report 0 transitions.

    Given an expired pending review and a stubbed atomic_timeout that
    always returns None (simulating peer-won race),
    When _reaper_tick runs,
    Then it returns 0 and the row stays pending (no audit event
    appended).
    """
    svc = AiReviewService.get_instance()
    now = _now()
    delegate_pid = await _seed_delegate(
        repo, user_public_id=str(uuid7()), last_seen_at=now, creation_time=now
    )
    review_id = await _seed_review(
        repo,
        selected_delegate_public_id=delegate_pid,
        as_of=now - timedelta(seconds=120),
        deadline_offset_seconds=60,
    )

    async def _stub_atomic_timeout(*, review_public_id: str, now: datetime) -> None:
        del review_public_id, now
        fut: asyncio.Future[None] = asyncio.Future()
        fut.set_result(None)
        await fut
        return None

    monkeypatch.setattr(repo, "atomic_timeout_ai_review", _stub_atomic_timeout)
    transitioned = await svc._reaper_tick(repo=repo, now=now)
    assert transitioned == 0
    assert (
        await _count_events_of_type(repo, review_public_id=review_id, event_type="timeout_marked")
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_offline_scanner_skips_row_lost_to_peer_dispatch(
    repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If atomic_dispatch_fanout returns None, the scanner skips the row.

    The §3.4 / §3.5 paths share the same per-row dispatch helper, so
    a stubbed ``atomic_dispatch_fanout`` exercises the loss-to-peer
    branch for both code paths.

    Given a pending review past fanout_after with an offline delegate
    AND a stubbed atomic_dispatch_fanout that always returns None,
    When _offline_scanner_tick runs,
    Then it returns 0 and no audit event is appended for the row.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    delegate_pid = await _seed_delegate(
        repo,
        user_public_id=str(uuid7()),
        last_seen_at=now - timedelta(seconds=120),
        creation_time=now,
    )
    review_id = await _seed_review(
        repo,
        selected_delegate_public_id=delegate_pid,
        as_of=now - timedelta(seconds=60),
        fanout_after_offset_seconds=30,
        deadline_offset_seconds=300,
    )

    async def _stub_dispatch(*, review_public_id: str, now: datetime) -> None:
        del review_public_id, now
        fut: asyncio.Future[None] = asyncio.Future()
        fut.set_result(None)
        await fut
        return None

    monkeypatch.setattr(repo, "atomic_dispatch_fanout", _stub_dispatch)
    dispatched = await svc._offline_scanner_tick(repo=repo, now=now)
    assert dispatched == 0
    assert (
        await _count_events_of_type(
            repo, review_public_id=review_id, event_type="fanout_dispatched"
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_list_pending_for_delegate_wallet_filter(
    repo: SQLAlchemyRepository,
) -> None:
    """``wallet_public_id`` predicate narrows the snapshot to one wallet.

    Plan D §7 REST-surface filter — bridge passes the wallet it is
    rendering so the catch-up snapshot stays scoped.

    Given two pending reviews for the same delegate on different wallets,
    When list_pending_reviews_for_delegate is called with wallet_public_id,
    Then only the matching row is returned.
    """
    now = _now()
    delegate_pid = await _seed_delegate(
        repo, user_public_id=str(uuid7()), last_seen_at=now, creation_time=now
    )
    review_a = await _seed_review(
        repo,
        selected_delegate_public_id=delegate_pid,
        as_of=now - timedelta(seconds=60),
        fanout_after_offset_seconds=30,
        deadline_offset_seconds=300,
    )
    review_b = await _seed_review(
        repo,
        selected_delegate_public_id=delegate_pid,
        as_of=now - timedelta(seconds=60),
        fanout_after_offset_seconds=30,
        deadline_offset_seconds=300,
    )
    from snapper.data.models import AiReview

    async with repo.session() as s:
        await s.execute(
            sqlalchemy.update(AiReview)
            .where(AiReview.public_id == review_a)
            .values(wallet_public_id="wal-A")
        )
        await s.execute(
            sqlalchemy.update(AiReview)
            .where(AiReview.public_id == review_b)
            .values(wallet_public_id="wal-B")
        )
        await s.commit()
    rows_filtered = await repo.list_pending_reviews_for_delegate(
        selected_delegate_public_id=delegate_pid,
        now=now,
        wallet_public_id="wal-A",
    )
    assert len(rows_filtered) == 1
    assert rows_filtered[0]["public_id"] == review_a
    assert rows_filtered[0]["wallet_public_id"] == "wal-A"
    rows_unfiltered = await repo.list_pending_reviews_for_delegate(
        selected_delegate_public_id=delegate_pid, now=now
    )
    assert {row["public_id"] for row in rows_unfiltered} == {review_a, review_b}


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_handle_delegate_offline_skips_review_before_fanout_after(
    repo: SQLAlchemyRepository,
) -> None:
    """§3.5 fast-path defers reviews whose fanout_after is still in the future.

    Plan D §3.5 + DelegateOfflineData docstring lock: subscribers
    compare ``last_seen_at`` to ``ai_reviews.fanout_after`` and either
    dispatch immediately OR wait for the natural fanout timer (the
    §3.4 Layer 2 scanner). The fast-path MUST NOT re-fan reviews
    whose grace window has not elapsed.

    Given a pending review whose fanout_after is still in the future
    AND a bus.delegate_offline message timestamped at ``now``,
    When handle_delegate_offline_bus_message runs,
    Then it returns 0 dispatches and the row stays pending; the §3.4
    scanner picks it up after ``fanout_after`` elapses.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    delegate_pid = await _seed_delegate(
        repo, user_public_id="user-1", last_seen_at=now, creation_time=now
    )
    review_id = await _seed_review(
        repo,
        selected_delegate_public_id=delegate_pid,
        as_of=now,
        fanout_after_offset_seconds=30,
        deadline_offset_seconds=300,
    )
    msg = DelegateOfflineData(
        public_id=str(uuid7()),
        timestamp=now,
        session_id=str(uuid7()),
        sequence_id=1,
        user_public_id="user-1",
        delegate_public_id=delegate_pid,
        last_seen_at=now,
    )
    dispatched = await svc.handle_delegate_offline_bus_message(msg, repo=repo)
    assert dispatched == 0
    row = await repo.get_ai_review(review_id)
    assert row is not None
    assert row["status"] == "pending"
    assert row["dispatch_version"] == 0


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_delegate_offline_idempotent_with_layer_2_scanner(
    repo: SQLAlchemyRepository,
) -> None:
    """§3.5 fast path is a no-op when §3.4 scanner already fired.

    Given the Layer 2 scanner has already transitioned a pending row to
    fanout_dispatched,
    When the §3.5 bus subscriber later runs for the same delegate,
    Then it observes status='fanout_dispatched' (excluded by the
    pending-only snapshot) and reports 0 dispatches.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    delegate_pid = await _seed_delegate(
        repo,
        user_public_id="user-1",
        last_seen_at=now - timedelta(seconds=120),
        creation_time=now,
    )
    review_id = await _seed_review(
        repo,
        selected_delegate_public_id=delegate_pid,
        as_of=now - timedelta(seconds=60),
        fanout_after_offset_seconds=30,
        deadline_offset_seconds=300,
    )
    assert await svc._offline_scanner_tick(repo=repo, now=now) == 1
    msg = DelegateOfflineData(
        public_id=str(uuid7()),
        timestamp=now,
        session_id=str(uuid7()),
        sequence_id=1,
        user_public_id="user-1",
        delegate_public_id=delegate_pid,
        last_seen_at=now,
    )
    dispatched = await svc.handle_delegate_offline_bus_message(msg, repo=repo)
    assert dispatched == 0
    row = await repo.get_ai_review(review_id)
    assert row is not None
    assert row["dispatch_version"] == 1
