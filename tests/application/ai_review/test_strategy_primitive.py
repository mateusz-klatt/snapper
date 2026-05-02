"""Tests for the strategy-side await primitive.

Covers :func:`create_ai_review_and_await` end-to-end against a real
SQLite repository so the full await loop (DB poll + Future fast path
+ inline timeout) is exercised.

The primitive itself is a thin compose over already-shipped service
methods (``create_review`` + ``timeout_review``) plus the existing
futures registry on the singleton — these tests focus on the
await-loop semantics, not the underlying state machine.
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

from snapper.application.ai_review.service import AiReviewCreateRequest
from snapper.application.ai_review.service import AiReviewService
from snapper.application.ai_review.service import NoLiveDelegateError
from snapper.application.ai_review.strategy_primitive import create_ai_review_and_await
from snapper.core.types import AiReviewDecisionEnum
from snapper.core.types import AiReviewResolutionModeEnum
from snapper.core.types import AiReviewStatusEnum
from snapper.data.models import AiDelegate
from snapper.data.models import Instrument
from snapper.data.models import InstrumentUnderlyingMapping
from snapper.data.models import Symbol
from snapper.data.models import UnderlyingAsset
from snapper.data.models import User
from snapper.data.models import UserOperatorMembership
from snapper.data.models import WalletOperatorScopeGrant
from snapper.data.repository import SQLAlchemyRepository

TEST_TIMEOUT = 30
_FIXTURE_HASH = "bcrypt-fake"


@pytest.fixture(autouse=True)
def _clear_singleton() -> Iterator[None]:
    """Reset the AiReviewService singleton between cases."""
    AiReviewService.clear_instance()
    yield
    AiReviewService.clear_instance()


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Provide a fresh SQLite repository per test."""
    db_path = tmp_path / "strategy_primitive.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path.as_posix()}")
    await r.create_all()
    yield r


def _now() -> datetime:
    """Single point of truth for wall-clock fixtures."""
    return datetime.now(UTC)


def _decision_audit_event(
    *,
    review_id: str,
    delegate_pid: str,
    decision: AiReviewDecisionEnum,
    occurred_at: datetime,
    rationale: str | None,
) -> dict[str, object]:
    """Build the audit row expected by the combined resolve primitive."""
    return {
        "public_id": str(uuid7()),
        "review_public_id": review_id,
        "event_type": "decision_recorded",
        "actor_delegate_public_id": delegate_pid,
        "previous_status": "pending",
        "new_status": (
            "resolved_approved" if decision is AiReviewDecisionEnum.APPROVE else "resolved_rejected"
        ),
        "payload": {"decision": decision.value, "rationale": rationale},
        "occurred_at": occurred_at,
    }


async def _seed_eligible_setup(repo: SQLAlchemyRepository, *, as_of: datetime) -> dict[str, str]:
    """Seed user + role + membership + grant + live delegate for create_review."""
    user_public_id = str(uuid7())
    operator_public_id = str(uuid7())
    wallet_public_id = str(uuid7())
    underlying_public_id = str(uuid7())
    async with repo.session() as s:
        s.add(
            Symbol(
                native_symbol="BTC-USD",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                created_at=as_of,
                timestamp=as_of,
                session_id="seed",
                sequence_id=1,
            )
        )
        await s.flush()
        symbol = (await s.execute(sqlalchemy.select(Symbol))).scalar_one()
        s.add(
            Instrument(
                symbol_public_id=symbol.public_id,
                exchange="kraken",
                requires_ai_review=True,
                timestamp=as_of,
                session_id="seed",
                sequence_id=2,
            )
        )
        s.add(
            UnderlyingAsset(
                public_id=underlying_public_id,
                name="Bitcoin",
                ticker="BTC",
                asset_class="crypto",
                sector=None,
                description=None,
                timestamp=as_of,
                session_id="seed",
                sequence_id=3,
            )
        )
        await s.flush()
        instrument = (await s.execute(sqlalchemy.select(Instrument))).scalar_one()
        s.add(
            InstrumentUnderlyingMapping(
                instrument_public_id=instrument.public_id,
                underlying_public_id=underlying_public_id,
                relationship_type="exact",
                contract_family=None,
                timestamp=as_of,
                session_id="seed",
                sequence_id=4,
            )
        )
        s.add(
            User(
                public_id=user_public_id,
                username=f"u-{user_public_id}",
                email=None,
                password_hash=_FIXTURE_HASH,
                role="ai_delegate",
                is_active=True,
                created_at=as_of,
                timestamp=as_of,
                session_id="seed",
                sequence_id=99,
            )
        )
        s.add(
            UserOperatorMembership(
                user_public_id=user_public_id,
                operator_public_id=operator_public_id,
                is_primary=True,
                timestamp=as_of,
                session_id="seed",
                sequence_id=10,
            )
        )
        s.add(
            WalletOperatorScopeGrant(
                operator_public_id=operator_public_id,
                wallet_public_id=wallet_public_id,
                granted_by_user_public_id=user_public_id,
                scope_kind="instrument",
                instrument_public_id=instrument.public_id,
                timestamp=as_of,
                session_id="seed",
                sequence_id=20,
            )
        )
        await s.commit()
    delegate_public_id = str(uuid7())
    heartbeat_at = _now() + timedelta(minutes=10)
    await repo.insert_ai_delegate(
        public_id=delegate_public_id, user_public_id=user_public_id, as_of=as_of
    )
    await repo.update_delegate_last_seen(delegate_public_id, heartbeat_at)
    return {
        "user_public_id": user_public_id,
        "operator_public_id": operator_public_id,
        "wallet_public_id": wallet_public_id,
        "instrument_public_id": instrument.public_id,
        "delegate_public_id": delegate_public_id,
    }


def _make_request(ids: dict[str, str], deadline_seconds: int = 5) -> AiReviewCreateRequest:
    """Build a minimal valid request from seeded id-bag."""
    return AiReviewCreateRequest(
        user_public_id=ids["user_public_id"],
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        strategy_public_id=str(uuid7()),
        signal_envelope={"side": "buy", "qty": 1.0},
        instrument_metadata={"requires_ai_review": True},
        deadline_seconds=deadline_seconds,
        session_id=str(uuid7()),
        sequence_id=42,
    )


async def _resolve_review_after_delay(
    repo: SQLAlchemyRepository,
    *,
    delegate_pid: str,
    delay_seconds: float,
    decision: AiReviewDecisionEnum,
) -> str:
    """Background task helper: wait, then approve/reject the latest pending review."""
    await asyncio.sleep(delay_seconds)
    review_id = await _wait_for_latest_pending_review_id(repo)
    new_status = (
        "resolved_approved" if decision is AiReviewDecisionEnum.APPROVE else "resolved_rejected"
    )
    await repo.atomic_resolve_review_with_audit_and_counter(
        review_public_id=review_id,
        decision=decision.value,
        responding_delegate_public_id=delegate_pid,
        rationale="background",
        new_status=new_status,
        audit_event=_decision_audit_event(
            review_id=review_id,
            delegate_pid=delegate_pid,
            decision=decision,
            occurred_at=datetime.now(UTC),
            rationale="background",
        ),
        now=datetime.now(UTC),
    )
    return review_id


async def _wait_for_latest_pending_review_id(
    repo: SQLAlchemyRepository,
    *,
    timeout_seconds: float = 5.0,
    poll_seconds: float = 0.01,
) -> str:
    """Wait until the primitive has inserted a pending review row."""
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while True:
        review_id = await _latest_pending_review_id(repo)
        if review_id is not None:
            return review_id
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("pending ai_review row was not inserted before helper timeout")
        await asyncio.sleep(poll_seconds)


async def _latest_pending_review_id(repo: SQLAlchemyRepository) -> str | None:
    """Return the newest pending review id, if the primitive has inserted one."""
    async with repo.session() as s:
        row = (
            await s.execute(
                sqlalchemy.text(
                    "SELECT public_id FROM ai_reviews WHERE status='pending' "
                    "ORDER BY created_at DESC LIMIT 1"
                )
            )
        ).first()
    return row[0] if row else None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_happy_path_approve_resolves_via_db_poll(
    repo: SQLAlchemyRepository,
) -> None:
    """Approve landed via concurrent task -> primitive returns RESOLVED_APPROVED.

    Given a seeded eligible delegate AND a background task that
    transitions the review to ``resolved_approved`` after 0.2s,
    When the strategy primitive runs with a 5s deadline + 0.05-0.1s
    poll window,
    Then the await loop observes the terminal status and returns an
    AiReviewDecisionOutcome with status=RESOLVED_APPROVED.
    """
    now = _now()
    ids = await _seed_eligible_setup(repo, as_of=now)
    request = _make_request(ids, deadline_seconds=5)
    background = asyncio.create_task(
        _resolve_review_after_delay(
            repo,
            delegate_pid=ids["delegate_public_id"],
            delay_seconds=0.2,
            decision=AiReviewDecisionEnum.APPROVE,
        )
    )
    outcome = await create_ai_review_and_await(
        request,
        repo=repo,
        deadline_seconds=5,
        poll_min_seconds=0.05,
        poll_max_seconds=0.1,
    )
    await background
    assert outcome.status is AiReviewStatusEnum.RESOLVED_APPROVED
    assert outcome.decision is AiReviewDecisionEnum.APPROVE
    assert outcome.resolution_mode is AiReviewResolutionModeEnum.PICK_ONE_PRIMARY
    assert outcome.responding_delegate_public_id == ids["delegate_public_id"]


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_reject_path_returns_resolved_rejected(
    repo: SQLAlchemyRepository,
) -> None:
    """Reject landed via concurrent task -> outcome carries RESOLVED_REJECTED.

    Strategy MUST treat reject as a VETO (no fall-through).

    Given a background task that rejects after 0.2s,
    When the strategy primitive runs,
    Then the outcome has status=RESOLVED_REJECTED and the strategy
    can branch on it.
    """
    now = _now()
    ids = await _seed_eligible_setup(repo, as_of=now)
    request = _make_request(ids, deadline_seconds=5)
    background = asyncio.create_task(
        _resolve_review_after_delay(
            repo,
            delegate_pid=ids["delegate_public_id"],
            delay_seconds=0.2,
            decision=AiReviewDecisionEnum.REJECT,
        )
    )
    outcome = await create_ai_review_and_await(
        request,
        repo=repo,
        deadline_seconds=5,
        poll_min_seconds=0.05,
        poll_max_seconds=0.1,
    )
    await background
    assert outcome.status is AiReviewStatusEnum.RESOLVED_REJECTED
    assert outcome.decision is AiReviewDecisionEnum.REJECT


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_inline_timeout_fires_when_deadline_elapses(
    repo: SQLAlchemyRepository,
) -> None:
    """No decision before deadline -> inline timeout transitions row.

    Given an eligible delegate that NEVER decides,
    When the primitive's deadline (1 second) elapses,
    Then it transitions the row to ``timeout`` itself, decrements the
    delegate counter, and returns status=TIMEOUT with
    resolution_mode=TIMEOUT_NO_RESPONSE.
    """
    now = _now()
    ids = await _seed_eligible_setup(repo, as_of=now)
    request = _make_request(ids, deadline_seconds=1)
    outcome = await create_ai_review_and_await(
        request,
        repo=repo,
        deadline_seconds=1,
        poll_min_seconds=0.1,
        poll_max_seconds=0.2,
    )
    assert outcome.status is AiReviewStatusEnum.TIMEOUT
    assert outcome.resolution_mode is AiReviewResolutionModeEnum.TIMEOUT_NO_RESPONSE
    assert outcome.decision is None
    async with repo.session() as s:
        delegate_row = (
            await s.execute(
                sqlalchemy.select(AiDelegate.active_reviews_count).where(
                    AiDelegate.public_id == ids["delegate_public_id"]
                )
            )
        ).scalar_one()
    assert delegate_row == 0


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_supersede_terminal_state_observed_via_poll(
    repo: SQLAlchemyRepository,
) -> None:
    """Background supersede -> primitive observes SUPERSEDED via the poll loop.

    Given a background task that supersedes the review (e.g. signal
    expired) after 0.2s,
    When the primitive polls,
    Then it observes the terminal state and returns SUPERSEDED.
    """
    now = _now()
    ids = await _seed_eligible_setup(repo, as_of=now)
    svc = AiReviewService.get_instance()

    async def _supersede_after() -> None:
        await asyncio.sleep(0.2)
        review_id = await _wait_for_latest_pending_review_id(repo)
        await svc.supersede_review(
            review_public_id=review_id,
            reason="signal expired",
            repo=repo,
            now=datetime.now(UTC),
        )

    background = asyncio.create_task(_supersede_after())
    request = _make_request(ids, deadline_seconds=5)
    outcome = await create_ai_review_and_await(
        request,
        repo=repo,
        deadline_seconds=5,
        poll_min_seconds=0.05,
        poll_max_seconds=0.1,
    )
    await background
    assert outcome.status is AiReviewStatusEnum.SUPERSEDED
    assert outcome.resolution_mode is AiReviewResolutionModeEnum.SUPERSEDED_BY_STRATEGY


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_no_live_delegate_propagates_from_create_review(
    repo: SQLAlchemyRepository,
) -> None:
    """No eligible delegate -> NoLiveDelegateError raised by create_review.

    Given a seeded scope graph WITHOUT any live delegate,
    When the primitive invokes create_review,
    Then the error propagates BEFORE the await loop runs (so the
    strategy can fall through).
    """
    now = _now()
    user_public_id = str(uuid7())
    operator_public_id = str(uuid7())
    wallet_public_id = str(uuid7())
    async with repo.session() as s:
        s.add(
            Symbol(
                native_symbol="ETH-USD",
                base="ETH",
                quote="USD",
                asset_type="crypto",
                created_at=now,
                timestamp=now,
                session_id="seed",
                sequence_id=1,
            )
        )
        await s.flush()
        symbol = (await s.execute(sqlalchemy.select(Symbol))).scalar_one()
        s.add(
            Instrument(
                symbol_public_id=symbol.public_id,
                exchange="kraken",
                requires_ai_review=True,
                timestamp=now,
                session_id="seed",
                sequence_id=2,
            )
        )
        await s.flush()
        instrument = (await s.execute(sqlalchemy.select(Instrument))).scalar_one()
        await s.commit()
    request = AiReviewCreateRequest(
        user_public_id=user_public_id,
        operator_public_id=operator_public_id,
        wallet_public_id=wallet_public_id,
        instrument_public_id=instrument.public_id,
        strategy_public_id=str(uuid7()),
        signal_envelope={"side": "buy"},
        instrument_metadata={"requires_ai_review": True},
        deadline_seconds=5,
        session_id=str(uuid7()),
        sequence_id=1,
    )
    with pytest.raises(NoLiveDelegateError):
        await create_ai_review_and_await(request, repo=repo, deadline_seconds=5)


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_future_resolved_externally_skips_remaining_poll(
    repo: SQLAlchemyRepository,
) -> None:
    """Pre-resolved future short-circuits the await loop.

    Fast path — when the (future-deferred) bus listener fires
    bus.ai_review_decision and resolves the registered Future,
    the primitive exits the wait_for early. Because the bus listener
    isn't wired yet, we simulate the path by resolving the future
    directly.

    Given a registered future that is set as soon as the loop schedules,
    When the primitive's wait_for fires,
    Then the next iteration reads the row (which a concurrent task
    has resolved to terminal) and returns the outcome without waiting
    out the full poll window.
    """
    now = _now()
    ids = await _seed_eligible_setup(repo, as_of=now)

    async def _resolve_fast() -> None:
        await asyncio.sleep(0.05)
        review_id = await _wait_for_latest_pending_review_id(repo)
        await repo.atomic_resolve_review_with_audit_and_counter(
            review_public_id=review_id,
            decision="approve",
            responding_delegate_public_id=ids["delegate_public_id"],
            rationale="fast",
            new_status="resolved_approved",
            audit_event=_decision_audit_event(
                review_id=review_id,
                delegate_pid=ids["delegate_public_id"],
                decision=AiReviewDecisionEnum.APPROVE,
                occurred_at=datetime.now(UTC),
                rationale="fast",
            ),
            now=datetime.now(UTC),
        )
        svc = AiReviewService.get_instance()
        fut = svc.get_future(review_id)
        if fut is not None and not fut.done():
            fut.set_result(None)

    background = asyncio.create_task(_resolve_fast())
    request = _make_request(ids, deadline_seconds=5)
    outcome = await create_ai_review_and_await(
        request,
        repo=repo,
        deadline_seconds=5,
        poll_min_seconds=10.0,
        poll_max_seconds=12.0,
    )
    await background
    assert outcome.status is AiReviewStatusEnum.RESOLVED_APPROVED


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_unregisters_future_on_completion(
    repo: SQLAlchemyRepository,
) -> None:
    """Successful await leaves no stale future entry on the singleton.

    Given a happy-path approve,
    When the primitive returns,
    Then the AiReviewService futures registry has no entry keyed on
    the resolved review_public_id.
    """
    now = _now()
    ids = await _seed_eligible_setup(repo, as_of=now)
    request = _make_request(ids, deadline_seconds=5)
    background = asyncio.create_task(
        _resolve_review_after_delay(
            repo,
            delegate_pid=ids["delegate_public_id"],
            delay_seconds=0.1,
            decision=AiReviewDecisionEnum.APPROVE,
        )
    )
    outcome = await create_ai_review_and_await(
        request,
        repo=repo,
        deadline_seconds=5,
        poll_min_seconds=0.05,
        poll_max_seconds=0.1,
    )
    await background
    svc = AiReviewService.get_instance()
    assert svc.get_future(outcome.review_public_id) is None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_deadline_override_on_request_carried_through(
    repo: SQLAlchemyRepository,
) -> None:
    """When ``deadline_seconds`` differs from request, override is honoured.

    Given a request with deadline_seconds=30 but the primitive called
    with deadline_seconds=1,
    When the primitive runs,
    Then the override fires the inline timeout at ~1s, NOT 30s; the
    persisted row's ``deadline`` column reflects the override (1s
    after now).
    """
    now = _now()
    ids = await _seed_eligible_setup(repo, as_of=now)
    request = _make_request(ids, deadline_seconds=30)
    outcome = await create_ai_review_and_await(
        request,
        repo=repo,
        deadline_seconds=1,
        poll_min_seconds=0.05,
        poll_max_seconds=0.1,
    )
    assert outcome.status is AiReviewStatusEnum.TIMEOUT
    row = await repo.get_ai_review(outcome.review_public_id)
    assert row is not None
    elapsed = (row["deadline"] - row["created_at"]).total_seconds()
    assert 0.5 <= elapsed <= 1.5


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_timeout_review_skips_when_already_terminal(
    repo: SQLAlchemyRepository,
) -> None:
    """Direct ``timeout_review`` no-op on already-terminal rows.

    Given a row already resolved by a peer,
    When AiReviewService.timeout_review runs,
    Then it returns False without appending an audit event.
    """
    now = _now()
    ids = await _seed_eligible_setup(repo, as_of=now)
    request = _make_request(ids, deadline_seconds=10)
    svc = AiReviewService.get_instance()
    creation = await svc.create_review(request, repo=repo)
    await repo.atomic_resolve_review_with_audit_and_counter(
        review_public_id=creation.review_public_id,
        decision="approve",
        responding_delegate_public_id=ids["delegate_public_id"],
        rationale="peer",
        new_status="resolved_approved",
        audit_event=_decision_audit_event(
            review_id=creation.review_public_id,
            delegate_pid=ids["delegate_public_id"],
            decision=AiReviewDecisionEnum.APPROVE,
            occurred_at=now,
            rationale="peer",
        ),
        now=now,
    )
    won = await svc.timeout_review(
        review_public_id=creation.review_public_id, repo=repo, now=_now()
    )
    assert won is False
