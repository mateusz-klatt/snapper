"""Tests for :meth:`AiReviewService.submit_decision` (Plan D §3.2).

Covers the full state machine:
- Caller delegate resolve (Q9 step 1).
- Review existence + scope check.
- Terminal-state shortcuts (idempotent retry vs peer-resolved).
- Q9 step 3.5 inline late-decision timeout.
- Atomic CAS resolve + audit-event append + Q10 counter decrement.
- Resolution-mode classification across pending vs fanout_dispatched.
- Phase 2 #3: post-commit ``bus.ai_review_decision`` fast-path emission.
"""

from collections.abc import AsyncIterator
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from uuid import uuid7

import pytest

from snapper.application.ai_review.service import ERROR_DECISION_ALREADY_RECORDED
from snapper.application.ai_review.service import ERROR_NOT_AUTHORIZED
from snapper.application.ai_review.service import ERROR_PEER_RESOLVED
from snapper.application.ai_review.service import ERROR_REVIEW_EXPIRED
from snapper.application.ai_review.service import ERROR_REVIEW_NOT_FOUND
from snapper.application.ai_review.service import AiReviewService
from snapper.core.types import AiReviewDecisionEnum
from snapper.core.types import AiReviewResolutionModeEnum
from snapper.core.types import AiReviewStatusEnum
from snapper.data.repository import SQLAlchemyRepository
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import AiReviewDecisionData

TEST_TIMEOUT = 15


class _FakeScopeGrantService:
    """Stub that returns a pre-configured allow/deny verdict.

    Plan D §10 has the real :class:`ScopeGrantService.has_grant_for_delegate`
    forward to the repository. The submit_decision path consults the service,
    so mocking just the verdict (rather than the full repo wiring) keeps the
    unit tests focused on the state machine behaviour.
    """

    def __init__(self, allow: bool = True) -> None:
        """Configure whether the next call to ``has_grant_for_delegate`` allows."""
        self.allow = allow
        self.calls: int = 0

    async def has_grant_for_delegate(
        self,
        *,
        delegate_public_id: str,
        wallet_public_id: str,
        instrument_public_id: str,
        as_of: datetime,
    ) -> bool:
        """Return the pre-configured verdict; record call count for assertions."""
        del delegate_public_id, wallet_public_id, instrument_public_id, as_of
        self.calls += 1
        return self.allow


@pytest.fixture(autouse=True)
def _clear_singleton() -> Iterator[None]:
    """Reset the AiReviewService singleton between cases."""
    AiReviewService.clear_instance()
    yield
    AiReviewService.clear_instance()


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Provide a fresh SQLite repository per test."""
    db_path = tmp_path / "submit_decision.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path.as_posix()}")
    await r.create_all()
    yield r


def _now() -> datetime:
    """Single point of truth for wall-clock fixtures."""
    return datetime.now(UTC)


async def _seed_delegate(
    repo: SQLAlchemyRepository, *, user_public_id: str, as_of: datetime
) -> str:
    """Insert an ``ai_delegates`` row; return delegate_public_id."""
    delegate_public_id = str(uuid7())
    await repo.insert_ai_delegate(
        public_id=delegate_public_id, user_public_id=user_public_id, as_of=as_of
    )
    return delegate_public_id


async def _seed_pending_review(
    repo: SQLAlchemyRepository,
    *,
    selected_delegate_public_id: str,
    as_of: datetime,
    deadline_offset_seconds: int = 60,
    initial_status: str = "pending",
) -> str:
    """Insert a pending (or fanout_dispatched) review row; return review_public_id."""
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
            "status": initial_status,
            "signal_envelope": {"side": "buy"},
            "signal_snapshot_hash": "deadbeef",
            "instrument_metadata": {"requires_ai_review": True},
            "deadline": as_of + timedelta(seconds=deadline_offset_seconds),
            "fanout_after": as_of + timedelta(seconds=30),
            "dispatch_version": 0,
            "created_at": as_of,
            "updated_at": as_of,
        }
    )
    return review_id


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_unknown_caller_delegate_returns_not_authorized(
    repo: SQLAlchemyRepository,
) -> None:
    """A caller user with no ai_delegates row gets ``not_authorized``.

    Given a configured AiReviewService and seeded repository,
    When submit_decision is invoked with the test parameters,
    Then the service returns the envelope asserted in the test body.
    """
    svc = AiReviewService.get_instance()
    result = await svc.submit_decision(
        review_public_id=str(uuid7()),
        caller_user_public_id="ghost-user",
        decision=AiReviewDecisionEnum.APPROVE,
        rationale=None,
        repo=repo,
        scope_grant_service=_FakeScopeGrantService(),
    )
    assert result.error_code == ERROR_NOT_AUTHORIZED
    assert result.status is None
    assert result.dispatch_version is None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_unknown_review_returns_review_not_found(
    repo: SQLAlchemyRepository,
) -> None:
    """Caller is a registered delegate but the review id is bogus.

    Given a configured AiReviewService and seeded repository,
    When submit_decision is invoked with the test parameters,
    Then the service returns the envelope asserted in the test body.
    """
    svc = AiReviewService.get_instance()
    user_pid = str(uuid7())
    await _seed_delegate(repo, user_public_id=user_pid, as_of=_now())
    result = await svc.submit_decision(
        review_public_id="does-not-exist",
        caller_user_public_id=user_pid,
        decision=AiReviewDecisionEnum.APPROVE,
        rationale=None,
        repo=repo,
        scope_grant_service=_FakeScopeGrantService(),
    )
    assert result.error_code == ERROR_REVIEW_NOT_FOUND
    assert result.status is None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_scope_denied_returns_not_authorized_with_review_status(
    repo: SQLAlchemyRepository,
) -> None:
    """Scope check failure returns ``not_authorized`` plus the row's status snapshot.

    Given a configured AiReviewService and seeded repository,
    When submit_decision is invoked with the test parameters,
    Then the service returns the envelope asserted in the test body.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    user_pid = str(uuid7())
    delegate_pid = await _seed_delegate(repo, user_public_id=user_pid, as_of=now)
    review_id = await _seed_pending_review(
        repo, selected_delegate_public_id=delegate_pid, as_of=now
    )
    result = await svc.submit_decision(
        review_public_id=review_id,
        caller_user_public_id=user_pid,
        decision=AiReviewDecisionEnum.APPROVE,
        rationale=None,
        repo=repo,
        scope_grant_service=_FakeScopeGrantService(allow=False),
    )
    assert result.error_code == ERROR_NOT_AUTHORIZED
    assert result.status == AiReviewStatusEnum.PENDING
    assert result.resolution_mode is None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_first_approve_pending_returns_resolved_approved_pick_one_primary(
    repo: SQLAlchemyRepository,
) -> None:
    """Happy path — pending + first approve from selected delegate.

    Given a configured AiReviewService and seeded repository,
    When submit_decision is invoked with the test parameters,
    Then the service returns the envelope asserted in the test body.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    user_pid = str(uuid7())
    delegate_pid = await _seed_delegate(repo, user_public_id=user_pid, as_of=now)
    review_id = await _seed_pending_review(
        repo, selected_delegate_public_id=delegate_pid, as_of=now
    )
    result = await svc.submit_decision(
        review_public_id=review_id,
        caller_user_public_id=user_pid,
        decision=AiReviewDecisionEnum.APPROVE,
        rationale="LGTM",
        repo=repo,
        scope_grant_service=_FakeScopeGrantService(),
        now=now,
    )
    assert result.error_code is None
    assert result.status == AiReviewStatusEnum.RESOLVED_APPROVED
    assert result.resolution_mode == AiReviewResolutionModeEnum.PICK_ONE_PRIMARY
    assert result.dispatch_version == 0
    row = await repo.get_ai_review(review_id)
    assert row is not None
    assert row["status"] == "resolved_approved"
    assert row["decision"] == "approve"
    assert row["responding_delegate_public_id"] == delegate_pid


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_first_reject_pending_returns_resolved_rejected(
    repo: SQLAlchemyRepository,
) -> None:
    """Reject path stores ``resolved_rejected`` + decision='reject'.

    Given a configured AiReviewService and seeded repository,
    When submit_decision is invoked with the test parameters,
    Then the service returns the envelope asserted in the test body.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    user_pid = str(uuid7())
    delegate_pid = await _seed_delegate(repo, user_public_id=user_pid, as_of=now)
    review_id = await _seed_pending_review(
        repo, selected_delegate_public_id=delegate_pid, as_of=now
    )
    result = await svc.submit_decision(
        review_public_id=review_id,
        caller_user_public_id=user_pid,
        decision=AiReviewDecisionEnum.REJECT,
        rationale="too risky",
        repo=repo,
        scope_grant_service=_FakeScopeGrantService(),
        now=now,
    )
    assert result.error_code is None
    assert result.status == AiReviewStatusEnum.RESOLVED_REJECTED


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_idempotent_retry_returns_decision_already_recorded(
    repo: SQLAlchemyRepository,
) -> None:
    """Same delegate + same decision after a successful resolve -> idempotent retry.

    Given a configured AiReviewService and seeded repository,
    When submit_decision is invoked with the test parameters,
    Then the service returns the envelope asserted in the test body.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    user_pid = str(uuid7())
    delegate_pid = await _seed_delegate(repo, user_public_id=user_pid, as_of=now)
    review_id = await _seed_pending_review(
        repo, selected_delegate_public_id=delegate_pid, as_of=now
    )
    await svc.submit_decision(
        review_public_id=review_id,
        caller_user_public_id=user_pid,
        decision=AiReviewDecisionEnum.APPROVE,
        rationale="LGTM",
        repo=repo,
        scope_grant_service=_FakeScopeGrantService(),
        now=now,
    )
    second = await svc.submit_decision(
        review_public_id=review_id,
        caller_user_public_id=user_pid,
        decision=AiReviewDecisionEnum.APPROVE,
        rationale="LGTM",
        repo=repo,
        scope_grant_service=_FakeScopeGrantService(),
        now=now,
    )
    assert second.error_code == ERROR_DECISION_ALREADY_RECORDED
    assert second.status == AiReviewStatusEnum.RESOLVED_APPROVED


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_different_decision_after_resolve_returns_peer_resolved(
    repo: SQLAlchemyRepository,
) -> None:
    """Same delegate but different decision -> not idempotent; treated as peer-resolved.

    Given a configured AiReviewService and seeded repository,
    When submit_decision is invoked with the test parameters,
    Then the service returns the envelope asserted in the test body.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    user_pid = str(uuid7())
    delegate_pid = await _seed_delegate(repo, user_public_id=user_pid, as_of=now)
    review_id = await _seed_pending_review(
        repo, selected_delegate_public_id=delegate_pid, as_of=now
    )
    await svc.submit_decision(
        review_public_id=review_id,
        caller_user_public_id=user_pid,
        decision=AiReviewDecisionEnum.APPROVE,
        rationale="approve",
        repo=repo,
        scope_grant_service=_FakeScopeGrantService(),
        now=now,
    )
    flipped = await svc.submit_decision(
        review_public_id=review_id,
        caller_user_public_id=user_pid,
        decision=AiReviewDecisionEnum.REJECT,
        rationale="changed my mind",
        repo=repo,
        scope_grant_service=_FakeScopeGrantService(),
        now=now,
    )
    assert flipped.error_code == ERROR_PEER_RESOLVED


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_different_delegate_after_resolve_returns_peer_resolved(
    repo: SQLAlchemyRepository,
) -> None:
    """Second delegate sees terminal status from peer -> peer-resolved.

    Given a configured AiReviewService and seeded repository,
    When submit_decision is invoked with the test parameters,
    Then the service returns the envelope asserted in the test body.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    user_a = str(uuid7())
    user_b = str(uuid7())
    delegate_a = await _seed_delegate(repo, user_public_id=user_a, as_of=now)
    await _seed_delegate(repo, user_public_id=user_b, as_of=now)
    review_id = await _seed_pending_review(repo, selected_delegate_public_id=delegate_a, as_of=now)
    await svc.submit_decision(
        review_public_id=review_id,
        caller_user_public_id=user_a,
        decision=AiReviewDecisionEnum.APPROVE,
        rationale=None,
        repo=repo,
        scope_grant_service=_FakeScopeGrantService(),
        now=now,
    )
    second = await svc.submit_decision(
        review_public_id=review_id,
        caller_user_public_id=user_b,
        decision=AiReviewDecisionEnum.APPROVE,
        rationale=None,
        repo=repo,
        scope_grant_service=_FakeScopeGrantService(),
        now=now,
    )
    assert second.error_code == ERROR_PEER_RESOLVED


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_late_decision_pending_marks_timeout_returns_review_id_expired(
    repo: SQLAlchemyRepository,
) -> None:
    """Q9 step 3.5 — deadline elapsed and row still pending.

    Given a configured AiReviewService and seeded repository,
    When submit_decision is invoked with the test parameters,
    Then the service returns the envelope asserted in the test body.
    """
    svc = AiReviewService.get_instance()
    seed_at = _now() - timedelta(seconds=120)
    decision_at = _now()
    user_pid = str(uuid7())
    delegate_pid = await _seed_delegate(repo, user_public_id=user_pid, as_of=seed_at)
    review_id = await _seed_pending_review(
        repo,
        selected_delegate_public_id=delegate_pid,
        as_of=seed_at,
        deadline_offset_seconds=60,
    )
    result = await svc.submit_decision(
        review_public_id=review_id,
        caller_user_public_id=user_pid,
        decision=AiReviewDecisionEnum.APPROVE,
        rationale=None,
        repo=repo,
        scope_grant_service=_FakeScopeGrantService(),
        now=decision_at,
    )
    assert result.error_code == ERROR_REVIEW_EXPIRED
    assert result.status == AiReviewStatusEnum.TIMEOUT
    assert result.resolution_mode == AiReviewResolutionModeEnum.TIMEOUT_NO_RESPONSE
    row = await repo.get_ai_review(review_id)
    assert row is not None
    assert row["status"] == "timeout"
    assert row["resolution_mode"] == "timeout_no_response"


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_late_decision_after_already_terminal_skips_timeout_path(
    repo: SQLAlchemyRepository,
) -> None:
    """Late decision when row is already terminal -> peer-resolved (NOT expired).

    Given a configured AiReviewService and seeded repository,
    When submit_decision is invoked with the test parameters,
    Then the service returns the envelope asserted in the test body.
    """
    svc = AiReviewService.get_instance()
    seed_at = _now() - timedelta(seconds=120)
    decision_at = _now()
    user_pid = str(uuid7())
    delegate_pid = await _seed_delegate(repo, user_public_id=user_pid, as_of=seed_at)
    review_id = await _seed_pending_review(
        repo,
        selected_delegate_public_id=delegate_pid,
        as_of=seed_at,
        deadline_offset_seconds=60,
    )
    await repo.atomic_resolve_ai_review(
        review_public_id=review_id,
        decision="approve",
        responding_delegate_public_id=delegate_pid,
        rationale="early",
        resolution_mode="pick_one_primary",
        new_status="resolved_approved",
        now=seed_at + timedelta(seconds=10),
    )
    other_user = str(uuid7())
    await _seed_delegate(repo, user_public_id=other_user, as_of=seed_at)
    result = await svc.submit_decision(
        review_public_id=review_id,
        caller_user_public_id=other_user,
        decision=AiReviewDecisionEnum.REJECT,
        rationale="too late",
        repo=repo,
        scope_grant_service=_FakeScopeGrantService(),
        now=decision_at,
    )
    assert result.error_code == ERROR_PEER_RESOLVED
    assert result.status == AiReviewStatusEnum.RESOLVED_APPROVED


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_fanout_dispatched_selected_responds_returns_secondary_after_fanout(
    repo: SQLAlchemyRepository,
) -> None:
    """Fanout-dispatched + selected delegate responds -> SECONDARY_AFTER_FANOUT.

    Given a configured AiReviewService and seeded repository,
    When submit_decision is invoked with the test parameters,
    Then the service returns the envelope asserted in the test body.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    user_pid = str(uuid7())
    delegate_pid = await _seed_delegate(repo, user_public_id=user_pid, as_of=now)
    review_id = await _seed_pending_review(
        repo,
        selected_delegate_public_id=delegate_pid,
        as_of=now,
        initial_status="fanout_dispatched",
    )
    result = await svc.submit_decision(
        review_public_id=review_id,
        caller_user_public_id=user_pid,
        decision=AiReviewDecisionEnum.APPROVE,
        rationale=None,
        repo=repo,
        scope_grant_service=_FakeScopeGrantService(),
        now=now,
    )
    assert result.error_code is None
    assert result.resolution_mode == AiReviewResolutionModeEnum.SECONDARY_AFTER_FANOUT


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_fanout_dispatched_other_delegate_responds_returns_first_responder(
    repo: SQLAlchemyRepository,
) -> None:
    """Fanout-dispatched + non-selected delegate responds -> FANOUT_FIRST_RESPONDER.

    Given a configured AiReviewService and seeded repository,
    When submit_decision is invoked with the test parameters,
    Then the service returns the envelope asserted in the test body.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    user_a = str(uuid7())
    user_b = str(uuid7())
    delegate_a = await _seed_delegate(repo, user_public_id=user_a, as_of=now)
    await _seed_delegate(repo, user_public_id=user_b, as_of=now)
    review_id = await _seed_pending_review(
        repo,
        selected_delegate_public_id=delegate_a,
        as_of=now,
        initial_status="fanout_dispatched",
    )
    result = await svc.submit_decision(
        review_public_id=review_id,
        caller_user_public_id=user_b,
        decision=AiReviewDecisionEnum.APPROVE,
        rationale=None,
        repo=repo,
        scope_grant_service=_FakeScopeGrantService(),
        now=now,
    )
    assert result.error_code is None
    assert result.resolution_mode == AiReviewResolutionModeEnum.FANOUT_FIRST_RESPONDER


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_default_now_is_used_when_caller_omits_it(
    repo: SQLAlchemyRepository,
) -> None:
    """When ``now`` is omitted, the service uses the current wall-clock.

    Locks the default-arg behaviour: a missing ``now`` must NOT inadvertently
    push the row past its deadline (a regression here would silently break
    every production caller).

    Given a configured AiReviewService and seeded repository,
    When submit_decision is invoked with the test parameters,
    Then the service returns the envelope asserted in the test body.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    user_pid = str(uuid7())
    delegate_pid = await _seed_delegate(repo, user_public_id=user_pid, as_of=now)
    review_id = await _seed_pending_review(
        repo,
        selected_delegate_public_id=delegate_pid,
        as_of=now,
    )
    result = await svc.submit_decision(
        review_public_id=review_id,
        caller_user_public_id=user_pid,
        decision=AiReviewDecisionEnum.APPROVE,
        rationale=None,
        repo=repo,
        scope_grant_service=_FakeScopeGrantService(),
    )
    assert result.error_code is None
    assert result.status == AiReviewStatusEnum.RESOLVED_APPROVED


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_late_decision_lost_to_concurrent_resolve_falls_through_to_peer(
    repo: SQLAlchemyRepository,
) -> None:
    """Deadline-passed + already-terminal-by-peer: returns peer-resolved.

    Exercises the ``atomic_timeout_ai_review`` returns-None branch in the
    submit_decision late-decision path: the caller observes the deadline
    has elapsed but the timeout transition has nothing to do because a peer
    decision got there first.

    Given a configured AiReviewService and seeded repository,
    When submit_decision is invoked with the test parameters,
    Then the service returns the envelope asserted in the test body.
    """
    svc = AiReviewService.get_instance()
    seed_at = _now() - timedelta(seconds=120)
    decision_at = _now()
    user_pid = str(uuid7())
    delegate_pid = await _seed_delegate(repo, user_public_id=user_pid, as_of=seed_at)
    review_id = await _seed_pending_review(
        repo,
        selected_delegate_public_id=delegate_pid,
        as_of=seed_at,
        deadline_offset_seconds=60,
    )
    await repo.atomic_resolve_ai_review(
        review_public_id=review_id,
        decision="approve",
        responding_delegate_public_id=delegate_pid,
        rationale=None,
        resolution_mode="pick_one_primary",
        new_status="resolved_approved",
        now=seed_at + timedelta(seconds=10),
    )
    other_user = str(uuid7())
    await _seed_delegate(repo, user_public_id=other_user, as_of=seed_at)
    result = await svc.submit_decision(
        review_public_id=review_id,
        caller_user_public_id=other_user,
        decision=AiReviewDecisionEnum.APPROVE,
        rationale=None,
        repo=repo,
        scope_grant_service=_FakeScopeGrantService(),
        now=decision_at,
    )
    assert result.error_code == ERROR_PEER_RESOLVED


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_atomic_resolve_returns_none_falls_through_to_peer(
    repo: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ``atomic_resolve_ai_review`` returns ``None`` mid-flight, we re-read.

    Simulates a concurrent peer resolving between the initial scope check and
    the atomic UPDATE. The submit_decision path must re-read and surface
    ``review_already_resolved_by_peer`` (or ``decision_already_recorded`` if
    the caller happens to match — shouldn't here).

    Given a configured AiReviewService and seeded repository,
    When submit_decision is invoked with the test parameters,
    Then the service returns the envelope asserted in the test body.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    user_pid = str(uuid7())
    delegate_pid = await _seed_delegate(repo, user_public_id=user_pid, as_of=now)
    review_id = await _seed_pending_review(
        repo, selected_delegate_public_id=delegate_pid, as_of=now
    )

    original_atomic = repo.atomic_resolve_ai_review

    async def _atomic_then_peer(**kw: object) -> None:
        del kw
        await original_atomic(
            review_public_id=review_id,
            decision="reject",
            responding_delegate_public_id=delegate_pid,
            rationale="peer beat us",
            resolution_mode="pick_one_primary",
            new_status="resolved_rejected",
            now=now,
        )
        return None

    monkeypatch.setattr(repo, "atomic_resolve_ai_review", _atomic_then_peer)

    result = await svc.submit_decision(
        review_public_id=review_id,
        caller_user_public_id=user_pid,
        decision=AiReviewDecisionEnum.APPROVE,
        rationale="we tried",
        repo=repo,
        scope_grant_service=_FakeScopeGrantService(),
        now=now,
    )
    assert result.error_code == ERROR_PEER_RESOLVED
    assert result.status == AiReviewStatusEnum.RESOLVED_REJECTED


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_late_decision_atomic_timeout_returns_none_still_returns_expired(
    repo: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Late-decision path with atomic_timeout returning None (peer-resolved race).

    Covers the branch where the snapshot showed non-terminal but a peer
    flipped the row terminal between read and the atomic timeout call.
    Per Plan D §3.2 step 3.5 the response is still ``review_id_expired``;
    the audit-event + counter-decrement are skipped because there's no
    transition to record.

    Given a configured AiReviewService and seeded repository,
    When submit_decision is invoked with the test parameters,
    Then the service returns the envelope asserted in the test body.
    """
    svc = AiReviewService.get_instance()
    seed_at = _now() - timedelta(seconds=120)
    decision_at = _now()
    user_pid = str(uuid7())
    delegate_pid = await _seed_delegate(repo, user_public_id=user_pid, as_of=seed_at)
    review_id = await _seed_pending_review(
        repo,
        selected_delegate_public_id=delegate_pid,
        as_of=seed_at,
        deadline_offset_seconds=60,
    )

    async def _timeout_returns_none(**kw: object) -> None:
        del kw
        return None

    monkeypatch.setattr(repo, "atomic_timeout_ai_review", _timeout_returns_none)
    result = await svc.submit_decision(
        review_public_id=review_id,
        caller_user_public_id=user_pid,
        decision=AiReviewDecisionEnum.APPROVE,
        rationale=None,
        repo=repo,
        scope_grant_service=_FakeScopeGrantService(),
        now=decision_at,
    )
    assert result.error_code == ERROR_REVIEW_EXPIRED


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_atomic_resolve_returns_none_then_row_disappeared(
    repo: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Adversarial path — atomic returns None AND the row was deleted.

    Locks the fallback-to-original-review behaviour: if the post-resolve
    re-read returns None, the envelope still uses the cached (pre-resolve)
    review snapshot rather than crashing.

    Given a configured AiReviewService and seeded repository,
    When submit_decision is invoked with the test parameters,
    Then the service returns the envelope asserted in the test body.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    user_pid = str(uuid7())
    delegate_pid = await _seed_delegate(repo, user_public_id=user_pid, as_of=now)
    review_id = await _seed_pending_review(
        repo, selected_delegate_public_id=delegate_pid, as_of=now
    )

    async def _resolve_returns_none(**kw: object) -> None:
        del kw
        return None

    original_get = repo.get_ai_review
    call_count = {"n": 0}

    async def _get_then_disappear(rid: str) -> object:
        call_count["n"] += 1
        if call_count["n"] >= 2:
            return None
        return await original_get(rid)

    monkeypatch.setattr(repo, "atomic_resolve_ai_review", _resolve_returns_none)
    monkeypatch.setattr(repo, "get_ai_review", _get_then_disappear)

    result = await svc.submit_decision(
        review_public_id=review_id,
        caller_user_public_id=user_pid,
        decision=AiReviewDecisionEnum.APPROVE,
        rationale=None,
        repo=repo,
        scope_grant_service=_FakeScopeGrantService(),
        now=now,
    )
    assert result.error_code == ERROR_PEER_RESOLVED


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_submit_decision_publishes_bus_event_after_commit(
    repo: SQLAlchemyRepository,
) -> None:
    """Plan A §7.1 / Phase 2 #3 — post-commit bus.ai_review_decision fanout.

    The publisher MUST fire AFTER the atomic resolve commits + the
    audit event lands + the counter decrements. The bus event drives
    the strategy primitive's registered Future to resolve immediately
    (fast-path) instead of waiting for the next DB-poll jitter
    interval. Pin the post-commit ordering by asserting the row is
    already in terminal state when the publisher gets called.

    Given a wired publisher + a pending review,
    When submit_decision approves the review,
    Then publisher.send is awaited once with topic
        ``bus.ai_review_decision`` AND the payload's review_public_id
        + decision + new_status + dispatch_version match the resolved
        row.
    """
    svc = AiReviewService.get_instance()
    publisher = MagicMock()
    publisher.send = AsyncMock()
    publisher.tracker = SequenceTracker()
    svc.set_msg_publisher(cast(MessagePublisher, publisher))
    now = _now()
    user_pid = str(uuid7())
    delegate_pid = await _seed_delegate(repo, user_public_id=user_pid, as_of=now)
    review_id = await _seed_pending_review(
        repo, selected_delegate_public_id=delegate_pid, as_of=now
    )
    result = await svc.submit_decision(
        review_public_id=review_id,
        caller_user_public_id=user_pid,
        decision=AiReviewDecisionEnum.APPROVE,
        rationale="LGTM",
        repo=repo,
        scope_grant_service=_FakeScopeGrantService(),
        now=now,
    )
    assert result.error_code is None
    publisher.send.assert_awaited_once()
    topic, payload = publisher.send.await_args.args
    assert topic == "bus.ai_review_decision"
    assert isinstance(payload, AiReviewDecisionData)
    assert payload.review_public_id == review_id
    assert payload.responding_delegate_public_id == delegate_pid
    assert payload.decision == "approve"
    assert payload.new_status == "resolved_approved"
    assert payload.resolution_mode == "pick_one_primary"
    assert payload.dispatch_version == result.dispatch_version
    row = await repo.get_ai_review(review_id)
    assert row is not None
    assert row["status"] == "resolved_approved"


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_submit_decision_does_not_publish_on_terminal_shortcut(
    repo: SQLAlchemyRepository,
) -> None:
    """Idempotent retry / peer-resolved shortcuts skip the publish branch.

    Terminal-state shortcuts return ``decision_already_recorded`` or
    ``review_already_resolved_by_peer`` WITHOUT touching
    atomic_resolve_ai_review (no commit, no state change). The
    publisher therefore must not fire — there is no fresh decision to
    fan out and the original decision's bus event already fired.

    Given a wired publisher + a review that already resolved on a
        prior submit_decision call,
    When the same delegate retries the identical decision,
    Then the second call returns ``decision_already_recorded`` AND
        publisher.send is NOT awaited a second time.
    """
    svc = AiReviewService.get_instance()
    publisher = MagicMock()
    publisher.send = AsyncMock()
    publisher.tracker = SequenceTracker()
    svc.set_msg_publisher(cast(MessagePublisher, publisher))
    now = _now()
    user_pid = str(uuid7())
    delegate_pid = await _seed_delegate(repo, user_public_id=user_pid, as_of=now)
    review_id = await _seed_pending_review(
        repo, selected_delegate_public_id=delegate_pid, as_of=now
    )
    first = await svc.submit_decision(
        review_public_id=review_id,
        caller_user_public_id=user_pid,
        decision=AiReviewDecisionEnum.APPROVE,
        rationale="LGTM",
        repo=repo,
        scope_grant_service=_FakeScopeGrantService(),
        now=now,
    )
    assert first.error_code is None
    publisher.send.reset_mock()
    second = await svc.submit_decision(
        review_public_id=review_id,
        caller_user_public_id=user_pid,
        decision=AiReviewDecisionEnum.APPROVE,
        rationale="LGTM again",
        repo=repo,
        scope_grant_service=_FakeScopeGrantService(),
        now=now,
    )
    assert second.error_code == ERROR_DECISION_ALREADY_RECORDED
    publisher.send.assert_not_awaited()
