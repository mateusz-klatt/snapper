"""Tests for :meth:`AiReviewService.create_review` (Plan A v1.4 Q10 + Plan D §3.1).

Covers admission control end-to-end:
- Candidate listing through the repository's eligibility query.
- ``NoLiveDelegateError`` when the eligibility list is empty.
- ``DelegateBusyError`` when every candidate's CAS UPDATE misses.
- Happy path: atomic claim + INSERT + audit event with signal hash payload.
- Wall-clock override for deterministic deadline + fanout_after timing.
- Policy override for heartbeat / fanout windows.
- Signal-envelope hash determinism + dict-ordering canonicalisation.
"""

from collections.abc import AsyncIterator
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from uuid import uuid7

import pytest
import sqlalchemy

from snapper.application.ai_review.service import MAX_SIGNAL_ENVELOPE_BYTES
from snapper.application.ai_review.service import AiReviewAdmissionPolicy
from snapper.application.ai_review.service import AiReviewCreateRequest
from snapper.application.ai_review.service import AiReviewService
from snapper.application.ai_review.service import DelegateBusyError
from snapper.application.ai_review.service import NoLiveDelegateError
from snapper.application.ai_review.service import SignalEnvelopeTooLargeError
from snapper.application.ai_review.service import _signal_envelope_hash
from snapper.data.models import AiDelegate
from snapper.data.models import AiReviewEvent
from snapper.data.models import Instrument
from snapper.data.models import InstrumentUnderlyingMapping
from snapper.data.models import Symbol
from snapper.data.models import UnderlyingAsset
from snapper.data.models import User
from snapper.data.models import UserOperatorMembership
from snapper.data.models import WalletOperatorScopeGrant
from snapper.data.repository import SQLAlchemyRepository

TEST_TIMEOUT = 15

_FIXTURE_HASH = "bcrypt-fake"
"""Test-only placeholder for the ``users.password_hash`` column.

Mirrors the ``_BCRYPT_FAKE_DIGEST`` pattern in ``tests/auth/`` so the row
inserts type-check without inviting hard-coded-credential lint warnings.
"""


@pytest.fixture(autouse=True)
def _clear_singleton() -> Iterator[None]:
    """Reset the AiReviewService singleton between cases."""
    AiReviewService.clear_instance()
    yield
    AiReviewService.clear_instance()


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Provide a fresh SQLite repository per test."""
    db_path = tmp_path / "create_review.db"
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path.as_posix()}")
    await r.create_all()
    yield r


def _now() -> datetime:
    """Single point of truth for wall-clock fixtures."""
    return datetime.now(UTC)


async def _seed_scope_graph(repo: SQLAlchemyRepository, *, as_of: datetime) -> dict[str, str]:
    """Seed Symbol + Instrument + Underlying + mapping; return id-bag.

    Mirrors the helper in ``test_ai_review_repository.py`` so the service
    tests can reuse the operator/wallet/instrument tuple shape without
    importing private helpers across packages.
    """
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
        await s.commit()
    return {
        "user_public_id": user_public_id,
        "operator_public_id": operator_public_id,
        "wallet_public_id": wallet_public_id,
        "instrument_public_id": instrument.public_id,
        "underlying_public_id": underlying_public_id,
    }


async def _seed_user(
    repo: SQLAlchemyRepository,
    *,
    user_public_id: str,
    role: str,
    as_of: datetime,
) -> None:
    """Insert a ``users`` row with the requested role + public_id."""
    async with repo.session() as s:
        s.add(
            User(
                public_id=user_public_id,
                username=f"u-{user_public_id}",
                email=None,
                password_hash=_FIXTURE_HASH,
                role=role,
                is_active=True,
                created_at=as_of,
                timestamp=as_of,
                session_id="seed",
                sequence_id=99,
            )
        )
        await s.commit()


async def _seed_membership(
    repo: SQLAlchemyRepository,
    *,
    user_public_id: str,
    operator_public_id: str,
    as_of: datetime,
) -> None:
    """Insert an active ``UserOperatorMembership`` row."""
    async with repo.session() as s:
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
        await s.commit()


async def _seed_grant(
    repo: SQLAlchemyRepository,
    *,
    operator_public_id: str,
    wallet_public_id: str,
    instrument_public_id: str,
    granted_by_user_public_id: str,
    as_of: datetime,
) -> None:
    """Insert an active instrument-direct scope grant."""
    async with repo.session() as s:
        s.add(
            WalletOperatorScopeGrant(
                operator_public_id=operator_public_id,
                wallet_public_id=wallet_public_id,
                granted_by_user_public_id=granted_by_user_public_id,
                scope_kind="instrument",
                instrument_public_id=instrument_public_id,
                timestamp=as_of,
                session_id="seed",
                sequence_id=20,
            )
        )
        await s.commit()


async def _seed_live_delegate(
    repo: SQLAlchemyRepository,
    *,
    user_public_id: str,
    last_seen_at: datetime,
) -> str:
    """Insert AiDelegate then update ``last_seen_at`` to make it live."""
    delegate_public_id = str(uuid7())
    await repo.insert_ai_delegate(
        public_id=delegate_public_id,
        user_public_id=user_public_id,
        as_of=last_seen_at,
    )
    await repo.update_delegate_last_seen(delegate_public_id, last_seen_at)
    return delegate_public_id


async def _bump_active_count(
    repo: SQLAlchemyRepository, *, delegate_public_id: str, count: int, as_of: datetime
) -> None:
    """Force ``ai_delegates.active_reviews_count`` to a non-zero value."""
    async with repo.session() as s:
        await s.execute(
            sqlalchemy.update(AiDelegate)
            .where(AiDelegate.public_id == delegate_public_id)
            .values(active_reviews_count=count, updated_at=as_of)
        )
        await s.commit()


async def _seed_eligible_delegate(repo: SQLAlchemyRepository, *, as_of: datetime) -> dict[str, str]:
    """Seed user + role + membership + grant + live delegate end-to-end."""
    ids = await _seed_scope_graph(repo, as_of=as_of)
    await _seed_user(repo, user_public_id=ids["user_public_id"], role="ai_delegate", as_of=as_of)
    await _seed_membership(
        repo,
        user_public_id=ids["user_public_id"],
        operator_public_id=ids["operator_public_id"],
        as_of=as_of,
    )
    await _seed_grant(
        repo,
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        granted_by_user_public_id=ids["user_public_id"],
        as_of=as_of,
    )
    delegate_public_id = await _seed_live_delegate(
        repo, user_public_id=ids["user_public_id"], last_seen_at=as_of
    )
    ids["delegate_public_id"] = delegate_public_id
    return ids


def _make_request(ids: dict[str, str]) -> AiReviewCreateRequest:
    """Build a minimal valid request from a seeded id-bag."""
    return AiReviewCreateRequest(
        user_public_id=ids["user_public_id"],
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        strategy_public_id=str(uuid7()),
        signal_envelope={"side": "buy", "qty": 1.0},
        instrument_metadata={"requires_ai_review": True},
        deadline_seconds=15,
        session_id=str(uuid7()),
        sequence_id=42,
    )


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_no_eligible_delegates_raises_no_live_delegate(
    repo: SQLAlchemyRepository,
) -> None:
    """No grant/role/membership matches -> :class:`NoLiveDelegateError`.

    Given a configured AiReviewService and an empty eligibility set,
    When create_review runs admission control,
    Then it raises NoLiveDelegateError before touching ``ai_reviews``.
    """
    svc = AiReviewService.get_instance()
    ids = await _seed_scope_graph(repo, as_of=_now())
    request = _make_request(ids)
    with pytest.raises(NoLiveDelegateError):
        await svc.create_review(request, repo=repo)


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_all_busy_raises_delegate_busy(
    repo: SQLAlchemyRepository,
) -> None:
    """All candidates have ``active_reviews_count > 0`` -> :class:`DelegateBusyError`.

    Given a configured AiReviewService and one eligible-but-busy delegate,
    When create_review attempts the CAS claim,
    Then it raises DelegateBusyError without inserting an ``ai_reviews`` row.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    ids = await _seed_eligible_delegate(repo, as_of=now)
    await _bump_active_count(repo, delegate_public_id=ids["delegate_public_id"], count=1, as_of=now)
    request = _make_request(ids)
    with pytest.raises(DelegateBusyError):
        await svc.create_review(request, repo=repo, now=now)


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_happy_path_returns_created_envelope_and_persists_row(
    repo: SQLAlchemyRepository,
) -> None:
    """Single eligible idle delegate -> review created end-to-end.

    Given a configured AiReviewService and one eligible idle delegate,
    When create_review runs,
    Then the envelope returns the freshly inserted review_public_id
    plus the claimed delegate, the row is persisted with
    ``status='pending'`` and ``dispatch_version=0``, and the delegate
    counter is incremented to 1.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    ids = await _seed_eligible_delegate(repo, as_of=now)
    request = _make_request(ids)
    result = await svc.create_review(request, repo=repo, now=now)
    assert result.selected_delegate_public_id == ids["delegate_public_id"]
    review_row = await repo.get_ai_review(result.review_public_id)
    assert review_row is not None
    assert review_row["status"] == "pending"
    assert review_row["dispatch_version"] == 0
    assert review_row["selected_delegate_public_id"] == ids["delegate_public_id"]
    assert review_row["responding_delegate_public_id"] is None
    assert review_row["resolution_mode"] is None
    delegate_row = await repo.get_ai_delegate_by_user_public_id(ids["user_public_id"])
    assert delegate_row is not None
    assert delegate_row["active_reviews_count"] == 1


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_review_row_persists_scope_payload_and_timing(
    repo: SQLAlchemyRepository,
) -> None:
    """Review row carries the request scope, deadline + fanout_after, hash, JSON.

    Given a configured AiReviewService with a fixed wall-clock,
    When create_review runs,
    Then the persisted row's deadline equals ``now + deadline_seconds``,
    fanout_after equals ``now + policy.fanout_after_seconds`` (default 30s),
    signal_envelope/instrument_metadata round-trip through JSON, and the
    signal_snapshot_hash is the canonical SHA-256 of the envelope.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    ids = await _seed_eligible_delegate(repo, as_of=now)
    request = _make_request(ids)
    result = await svc.create_review(request, repo=repo, now=now)
    row = await repo.get_ai_review(result.review_public_id)
    assert row is not None
    assert row["user_public_id"] == ids["user_public_id"]
    assert row["operator_public_id"] == ids["operator_public_id"]
    assert row["wallet_public_id"] == ids["wallet_public_id"]
    assert row["instrument_public_id"] == ids["instrument_public_id"]
    assert row["session_id"] == request.session_id
    assert row["sequence_id"] == request.sequence_id
    assert row["deadline"] == now + timedelta(seconds=request.deadline_seconds)
    assert row["fanout_after"] == now + timedelta(seconds=30)
    assert row["signal_envelope"] == request.signal_envelope
    assert row["instrument_metadata"] == request.instrument_metadata
    assert row["signal_snapshot_hash"] == _signal_envelope_hash(request.signal_envelope)


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_created_event_appended_with_signal_hash_payload(
    repo: SQLAlchemyRepository,
) -> None:
    """A ``created`` audit event is appended carrying the hash + dispatch_version.

    Given a configured AiReviewService and a successful create_review,
    When the audit log is read for the new review,
    Then exactly one ``created`` event exists with the matching hash
    and ``dispatch_version=0``.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    ids = await _seed_eligible_delegate(repo, as_of=now)
    request = _make_request(ids)
    result = await svc.create_review(request, repo=repo, now=now)
    async with repo.session() as s:
        events = (
            (
                await s.execute(
                    sqlalchemy.select(AiReviewEvent).where(
                        AiReviewEvent.review_public_id == result.review_public_id
                    )
                )
            )
            .scalars()
            .all()
        )
    assert len(events) == 1
    event = events[0]
    assert event.event_type == "created"
    assert event.new_status == "pending"
    assert event.previous_status is None
    assert event.actor_delegate_public_id is None
    assert event.payload["signal_snapshot_hash"] == _signal_envelope_hash(request.signal_envelope)
    assert event.payload["dispatch_version"] == 0


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_signal_envelope_hash_is_deterministic_canonical_sha256() -> None:
    """Hash is stable across dict-key ordering and matches SHA-256 of canonical JSON.

    Given two equivalent envelopes with different key orders,
    When _signal_envelope_hash is computed for each,
    Then both yield identical 64-char hex SHA-256 digests.
    """
    envelope_a = {"side": "buy", "qty": 1.5, "instrument": "BTC-USD"}
    envelope_b = {"qty": 1.5, "instrument": "BTC-USD", "side": "buy"}
    hash_a = _signal_envelope_hash(envelope_a)
    hash_b = _signal_envelope_hash(envelope_b)
    assert hash_a == hash_b
    assert len(hash_a) == 64
    assert all(c in "0123456789abcdef" for c in hash_a)


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_default_now_uses_datetime_now_utc(
    repo: SQLAlchemyRepository,
) -> None:
    """Omitted ``now`` kwarg falls back to ``datetime.now(UTC)``.

    Given a configured AiReviewService and a request without an explicit now,
    When create_review runs,
    Then the persisted row's created_at is within ``[before, after]`` wall-clock
    bounds and the deadline is ``created_at + deadline_seconds``.
    """
    svc = AiReviewService.get_instance()
    seed_time = _now() - timedelta(seconds=2)
    ids = await _seed_eligible_delegate(repo, as_of=seed_time)
    request = _make_request(ids)
    before = datetime.now(UTC)
    result = await svc.create_review(request, repo=repo)
    after = datetime.now(UTC)
    row = await repo.get_ai_review(result.review_public_id)
    assert row is not None
    assert before <= row["created_at"] <= after
    assert row["deadline"] == row["created_at"] + timedelta(seconds=request.deadline_seconds)


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_custom_policy_extends_heartbeat_window_admits_stale_delegate(
    repo: SQLAlchemyRepository,
) -> None:
    """A delegate seen 30s ago is excluded by default but admitted with a 60s policy.

    Given a configured AiReviewService and a delegate seen 30s ago,
    When create_review runs with the default 15s heartbeat window,
    Then the call raises NoLiveDelegateError;
    And when the same call uses a policy with ``heartbeat_window_seconds=60``
    (with a strictly-greater fanout to satisfy the Q17 invariant),
    Then admission succeeds and the delegate is claimed.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    ids = await _seed_eligible_delegate(repo, as_of=now)
    async with repo.session() as s:
        await s.execute(
            sqlalchemy.update(AiDelegate)
            .where(AiDelegate.public_id == ids["delegate_public_id"])
            .values(last_seen_at=now - timedelta(seconds=30))
        )
        await s.commit()
    request = _make_request(ids)
    with pytest.raises(NoLiveDelegateError):
        await svc.create_review(request, repo=repo, now=now)
    result = await svc.create_review(
        request,
        repo=repo,
        now=now,
        policy=AiReviewAdmissionPolicy(heartbeat_window_seconds=60, fanout_after_seconds=120),
    )
    assert result.selected_delegate_public_id == ids["delegate_public_id"]


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_custom_policy_overrides_fanout_after_seconds(
    repo: SQLAlchemyRepository,
) -> None:
    """Custom ``fanout_after_seconds`` lands on the persisted row.

    Given a configured AiReviewService and a custom policy with
    ``heartbeat_window_seconds=5`` + ``fanout_after_seconds=10`` (Q17
    invariant satisfied),
    When create_review runs,
    Then the persisted row's fanout_after equals ``now + 10s``.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    ids = await _seed_eligible_delegate(repo, as_of=now)
    request = _make_request(ids)
    result = await svc.create_review(
        request,
        repo=repo,
        now=now,
        policy=AiReviewAdmissionPolicy(heartbeat_window_seconds=5, fanout_after_seconds=10),
    )
    row = await repo.get_ai_review(result.review_public_id)
    assert row is not None
    assert row["fanout_after"] == now + timedelta(seconds=10)


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_admission_policy_rejects_invalid_fanout_window() -> None:
    """``fanout_after_seconds <= heartbeat_window_seconds`` raises ValueError.

    Given the Q17 cross-plan invariant requires
    ``fanout_after > heartbeat_window``,
    When AiReviewAdmissionPolicy is constructed with the relation flipped
    (less-than) or equal,
    Then ``__post_init__`` raises ValueError before the offending policy
    can reach :meth:`AiReviewService.create_review`.
    """
    with pytest.raises(ValueError, match="fanout_after_seconds"):
        AiReviewAdmissionPolicy(heartbeat_window_seconds=15, fanout_after_seconds=10)
    with pytest.raises(ValueError, match="fanout_after_seconds"):
        AiReviewAdmissionPolicy(heartbeat_window_seconds=15, fanout_after_seconds=15)
    with pytest.raises(ValueError, match="heartbeat_window_seconds"):
        AiReviewAdmissionPolicy(heartbeat_window_seconds=0, fanout_after_seconds=30)


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_skips_busy_candidate_picks_next_idle(
    repo: SQLAlchemyRepository,
) -> None:
    """Iteration order: busy is skipped, the next idle delegate wins the claim.

    Given two eligible delegates where the most-recently-seen is busy,
    When create_review iterates the candidate list,
    Then the second delegate's CAS UPDATE wins and the review is claimed
    against it; the busy delegate's counter remains unchanged.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    ids = await _seed_eligible_delegate(repo, as_of=now)
    busy_pid = ids["delegate_public_id"]
    await _bump_active_count(repo, delegate_public_id=busy_pid, count=1, as_of=now)
    second_user_pid = str(uuid7())
    await _seed_user(repo, user_public_id=second_user_pid, role="ai_delegate", as_of=now)
    await _seed_membership(
        repo,
        user_public_id=second_user_pid,
        operator_public_id=ids["operator_public_id"],
        as_of=now,
    )
    idle_pid = await _seed_live_delegate(
        repo, user_public_id=second_user_pid, last_seen_at=now - timedelta(seconds=3)
    )
    request = _make_request(ids)
    result = await svc.create_review(request, repo=repo, now=now)
    assert result.selected_delegate_public_id == idle_pid
    busy_row = await repo.get_ai_delegate_by_user_public_id(ids["user_public_id"])
    assert busy_row is not None
    assert busy_row["active_reviews_count"] == 1
    idle_row = await repo.get_ai_delegate_by_user_public_id(second_user_pid)
    assert idle_row is not None
    assert idle_row["active_reviews_count"] == 1
    assert busy_pid != idle_pid


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_signal_envelope_with_nan_raises_value_error(
    repo: SQLAlchemyRepository,
) -> None:
    """Non-finite floats in ``signal_envelope`` -> ValueError before any DB write.

    Given a configured AiReviewService and an envelope containing ``NaN``,
    When create_review runs admission control,
    Then it raises ValueError because ``allow_nan=False`` rejects the
    serialisation, and no review row is inserted.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    ids = await _seed_eligible_delegate(repo, as_of=now)
    bad_request = AiReviewCreateRequest(
        user_public_id=ids["user_public_id"],
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        strategy_public_id=str(uuid7()),
        signal_envelope={"side": "buy", "qty": float("nan")},
        instrument_metadata={"requires_ai_review": True},
        deadline_seconds=15,
        session_id=str(uuid7()),
        sequence_id=42,
    )
    with pytest.raises(ValueError):
        await svc.create_review(bad_request, repo=repo, now=now)
    delegate_row = await repo.get_ai_delegate_by_user_public_id(ids["user_public_id"])
    assert delegate_row is not None
    assert delegate_row["active_reviews_count"] == 0


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_signal_envelope_over_16kb_raises_too_large(
    repo: SQLAlchemyRepository,
) -> None:
    """Canonical-JSON > 16KB -> :class:`SignalEnvelopeTooLargeError`.

    Given an envelope whose canonical form exceeds the Plan A v1.4
    risk-register cap,
    When create_review runs,
    Then it raises SignalEnvelopeTooLargeError BEFORE the eligibility query
    so the bitemporal store is never touched and the delegate counter
    stays at 0.
    """
    svc = AiReviewService.get_instance()
    now = _now()
    ids = await _seed_eligible_delegate(repo, as_of=now)
    oversized = "x" * (MAX_SIGNAL_ENVELOPE_BYTES + 100)
    bad_request = AiReviewCreateRequest(
        user_public_id=ids["user_public_id"],
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        strategy_public_id=str(uuid7()),
        signal_envelope={"side": "buy", "blob": oversized},
        instrument_metadata={"requires_ai_review": True},
        deadline_seconds=15,
        session_id=str(uuid7()),
        sequence_id=42,
    )
    with pytest.raises(SignalEnvelopeTooLargeError):
        await svc.create_review(bad_request, repo=repo, now=now)
    delegate_row = await repo.get_ai_delegate_by_user_public_id(ids["user_public_id"])
    assert delegate_row is not None
    assert delegate_row["active_reviews_count"] == 0


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_envelope_at_16kb_boundary_succeeds(
    repo: SQLAlchemyRepository,
) -> None:
    """Canonical-JSON exactly at the cap is admitted (boundary is inclusive).

    Given a payload sized so the canonical JSON serialises to exactly
    :data:`MAX_SIGNAL_ENVELOPE_BYTES` bytes,
    When create_review runs,
    Then the row is persisted (the guard is ``> max``, not ``>= max``).
    """
    svc = AiReviewService.get_instance()
    now = _now()
    ids = await _seed_eligible_delegate(repo, as_of=now)
    overhead = len(b'{"blob":""}')
    payload_chars = MAX_SIGNAL_ENVELOPE_BYTES - overhead
    request = AiReviewCreateRequest(
        user_public_id=ids["user_public_id"],
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        strategy_public_id=str(uuid7()),
        signal_envelope={"blob": "x" * payload_chars},
        instrument_metadata={"requires_ai_review": True},
        deadline_seconds=15,
        session_id=str(uuid7()),
        sequence_id=42,
    )
    result = await svc.create_review(request, repo=repo, now=now)
    row = await repo.get_ai_review(result.review_public_id)
    assert row is not None
