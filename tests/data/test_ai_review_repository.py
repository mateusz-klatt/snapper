"""Tests for the AI-review repository CRUD primitives + delegate scope check.

Plan A v1.4 + Plan D v1.1 — covers ``has_grant_for_delegate``
(four-step resolve: delegate -> user -> operator memberships ->
instrument-direct or underlying-kind grant) plus the six AiReview /
AiDelegate CRUD methods that back the AiReviewService.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any as _Any
from uuid import uuid7

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.dml import Update
from sqlalchemy.sql.expression import Select  # local import; only used here

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AiDelegate
from snapper.data.models import AiReview
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
"""Test-only placeholder for ``users.password_hash`` (mirrors tests/auth/)."""


def _now() -> datetime:
    """Return a single ``datetime.now(UTC)`` per call (helper for as_of args)."""
    return datetime.now(UTC)


async def _build_repo(tmp_path: Path, name: str = "ai_review_test.db") -> SQLAlchemyRepository:
    """Construct a fresh SQLite repository with the full schema."""
    db_path = tmp_path / name
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path.as_posix()}")
    await repo.create_all()
    return repo


async def _seed_user_operator_wallet_instrument(
    repo: SQLAlchemyRepository,
    *,
    as_of: datetime,
) -> dict[str, str]:
    """Seed the entity graph needed by ``has_grant_for_delegate``.

    Inserts a Symbol + Instrument, an UnderlyingAsset + mapping, plus a
    user_public_id and operator_public_id (the IDs are returned but the
    ``users`` / ``operators`` rows are NOT created — the helper sticks to
    what ``has_grant_for_delegate`` actually queries).
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
        symbol = (await s.execute(__import__("sqlalchemy").select(Symbol))).scalar_one()
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
        instrument = (await s.execute(__import__("sqlalchemy").select(Instrument))).scalar_one()
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


async def _add_membership(
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


async def _add_instrument_grant(
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


async def _add_underlying_grant(
    repo: SQLAlchemyRepository,
    *,
    operator_public_id: str,
    wallet_public_id: str,
    underlying_public_id: str,
    granted_by_user_public_id: str,
    as_of: datetime,
) -> None:
    """Insert an active underlying-kind scope grant."""
    async with repo.session() as s:
        s.add(
            WalletOperatorScopeGrant(
                operator_public_id=operator_public_id,
                wallet_public_id=wallet_public_id,
                granted_by_user_public_id=granted_by_user_public_id,
                scope_kind="underlying",
                underlying_public_id=underlying_public_id,
                timestamp=as_of,
                session_id="seed",
                sequence_id=21,
            )
        )
        await s.commit()


async def _seed_delegate(
    repo: SQLAlchemyRepository,
    *,
    user_public_id: str,
    as_of: datetime,
) -> str:
    """Seed an ``ai_delegates`` row for the given user; return delegate_public_id."""
    delegate_public_id = str(uuid7())
    await repo.insert_ai_delegate(
        public_id=delegate_public_id,
        user_public_id=user_public_id,
        as_of=as_of,
    )
    return delegate_public_id


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_has_grant_for_delegate_returns_false_when_delegate_unknown(
    tmp_path: Path,
) -> None:
    """Unknown delegate -> Step 1 short-circuit returns ``False``."""
    repo = await _build_repo(tmp_path)
    result = await repo.has_grant_for_delegate(
        delegate_public_id=str(uuid7()),
        wallet_public_id=str(uuid7()),
        instrument_public_id=str(uuid7()),
        as_of=_now(),
    )
    assert result is False


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_has_grant_for_delegate_returns_false_without_membership(
    tmp_path: Path,
) -> None:
    """Delegate exists but has no operator memberships -> Step 2 returns ``False``."""
    repo = await _build_repo(tmp_path)
    as_of = _now()
    ids = await _seed_user_operator_wallet_instrument(repo, as_of=as_of)
    delegate_public_id = await _seed_delegate(
        repo, user_public_id=ids["user_public_id"], as_of=as_of
    )
    result = await repo.has_grant_for_delegate(
        delegate_public_id=delegate_public_id,
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        as_of=as_of,
    )
    assert result is False


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_has_grant_for_delegate_returns_true_with_instrument_direct_grant(
    tmp_path: Path,
) -> None:
    """Active instrument-direct grant on the right wallet -> Step 3 returns ``True``."""
    repo = await _build_repo(tmp_path)
    as_of = _now()
    ids = await _seed_user_operator_wallet_instrument(repo, as_of=as_of)
    await _add_membership(
        repo,
        user_public_id=ids["user_public_id"],
        operator_public_id=ids["operator_public_id"],
        as_of=as_of,
    )
    await _add_instrument_grant(
        repo,
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        granted_by_user_public_id=ids["user_public_id"],
        as_of=as_of,
    )
    delegate_public_id = await _seed_delegate(
        repo, user_public_id=ids["user_public_id"], as_of=as_of
    )
    result = await repo.has_grant_for_delegate(
        delegate_public_id=delegate_public_id,
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        as_of=as_of,
    )
    assert result is True


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_has_grant_for_delegate_returns_true_with_underlying_grant(
    tmp_path: Path,
) -> None:
    """Underlying-kind grant + matching mapping -> Step 4 returns ``True``."""
    repo = await _build_repo(tmp_path)
    as_of = _now()
    ids = await _seed_user_operator_wallet_instrument(repo, as_of=as_of)
    await _add_membership(
        repo,
        user_public_id=ids["user_public_id"],
        operator_public_id=ids["operator_public_id"],
        as_of=as_of,
    )
    await _add_underlying_grant(
        repo,
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        underlying_public_id=ids["underlying_public_id"],
        granted_by_user_public_id=ids["user_public_id"],
        as_of=as_of,
    )
    delegate_public_id = await _seed_delegate(
        repo, user_public_id=ids["user_public_id"], as_of=as_of
    )
    result = await repo.has_grant_for_delegate(
        delegate_public_id=delegate_public_id,
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        as_of=as_of,
    )
    assert result is True


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_has_grant_for_delegate_returns_false_when_no_grant(
    tmp_path: Path,
) -> None:
    """Delegate + membership but no grant -> Step 4 falls through to ``False``."""
    repo = await _build_repo(tmp_path)
    as_of = _now()
    ids = await _seed_user_operator_wallet_instrument(repo, as_of=as_of)
    await _add_membership(
        repo,
        user_public_id=ids["user_public_id"],
        operator_public_id=ids["operator_public_id"],
        as_of=as_of,
    )
    delegate_public_id = await _seed_delegate(
        repo, user_public_id=ids["user_public_id"], as_of=as_of
    )
    result = await repo.has_grant_for_delegate(
        delegate_public_id=delegate_public_id,
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        as_of=as_of,
    )
    assert result is False


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_get_ai_review_returns_none_when_unknown(tmp_path: Path) -> None:
    """``get_ai_review`` on an unknown id returns ``None``."""
    repo = await _build_repo(tmp_path)
    result = await repo.get_ai_review("does-not-exist")
    assert result is None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_insert_and_get_ai_review_round_trip(tmp_path: Path) -> None:
    """Insert + read-back returns every field of the row.

    Locks the typed-dict shape: drift between the CRUD and the
    ``AiReviewRow`` TypedDict would surface here first.
    """
    repo = await _build_repo(tmp_path)
    now = _now()
    deadline = now + timedelta(seconds=60)
    review_id = str(uuid7())
    operator_public_id = str(uuid7())
    user_public_id = str(uuid7())
    delegate_public_id = str(uuid7())
    await repo.insert_ai_review(
        {
            "public_id": review_id,
            "session_id": str(uuid7()),
            "sequence_id": 1,
            "user_public_id": user_public_id,
            "operator_public_id": operator_public_id,
            "wallet_public_id": str(uuid7()),
            "instrument_public_id": str(uuid7()),
            "strategy_public_id": str(uuid7()),
            "selected_delegate_public_id": delegate_public_id,
            "status": "pending",
            "signal_envelope": {"side": "buy"},
            "signal_snapshot_hash": "deadbeef",
            "instrument_metadata": {"requires_ai_review": True},
            "deadline": deadline,
            "fanout_after": now + timedelta(seconds=30),
            "dispatch_version": 0,
            "created_at": now,
            "updated_at": now,
        }
    )
    row = await repo.get_ai_review(review_id)
    assert row is not None
    assert row["public_id"] == review_id
    assert row["status"] == "pending"
    assert row["dispatch_version"] == 0
    assert row["signal_envelope"] == {"side": "buy"}
    assert row["selected_delegate_public_id"] == delegate_public_id
    assert row["responding_delegate_public_id"] is None
    assert row["resolution_mode"] is None
    assert row["decision"] is None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_insert_ai_review_event_returns_event_id(tmp_path: Path) -> None:
    """Event insert returns the assigned ``public_id``."""
    repo = await _build_repo(tmp_path)
    now = _now()
    event_id = str(uuid7())
    returned = await repo.insert_ai_review_event(
        {
            "public_id": event_id,
            "review_public_id": str(uuid7()),
            "event_type": "decision_recorded",
            "actor_delegate_public_id": str(uuid7()),
            "previous_status": "pending",
            "new_status": "resolved_approved",
            "payload": {"decision": "approve"},
            "occurred_at": now,
        }
    )
    assert returned == event_id


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_get_ai_delegate_by_user_public_id_returns_none_when_unknown(
    tmp_path: Path,
) -> None:
    """Unknown user_public_id yields ``None``."""
    repo = await _build_repo(tmp_path)
    result = await repo.get_ai_delegate_by_user_public_id("ghost-user")
    assert result is None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_insert_ai_delegate_then_get_round_trip(tmp_path: Path) -> None:
    """Insert + read-back covers all stored fields including the Q10 counter."""
    repo = await _build_repo(tmp_path)
    now = _now()
    user_public_id = str(uuid7())
    delegate_public_id = str(uuid7())
    await repo.insert_ai_delegate(
        public_id=delegate_public_id,
        user_public_id=user_public_id,
        as_of=now,
    )
    row = await repo.get_ai_delegate_by_user_public_id(user_public_id)
    assert row is not None
    assert row["public_id"] == delegate_public_id
    assert row["user_public_id"] == user_public_id
    assert row["last_seen_at"] is None
    assert row["active_reviews_count"] == 0


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_update_delegate_last_seen_persists_timestamp(tmp_path: Path) -> None:
    """``update_delegate_last_seen`` updates both ``last_seen_at`` and ``updated_at``."""
    repo = await _build_repo(tmp_path)
    now = _now()
    user_public_id = str(uuid7())
    delegate_public_id = str(uuid7())
    await repo.insert_ai_delegate(
        public_id=delegate_public_id,
        user_public_id=user_public_id,
        as_of=now,
    )
    later = datetime(2099, 6, 15, 12, 0, 0, tzinfo=UTC)
    await repo.update_delegate_last_seen(delegate_public_id, later)
    row = await repo.get_ai_delegate_by_user_public_id(user_public_id)
    assert row is not None
    assert row["last_seen_at"] == later
    assert row["updated_at"] == later


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_models_imports_are_load_bearing() -> None:
    """Sanity import check.

    Guards against accidental drop of the new SQLAlchemy classes referenced
    by the CRUD layer.
    """
    assert AiReview.__tablename__ == "ai_reviews"
    assert AiReviewEvent.__tablename__ == "ai_review_events"
    assert AiDelegate.__tablename__ == "ai_delegates"
    assert KNOWN_TO_MAX is not None


async def _seed_pending_for_atomic(
    repo: SQLAlchemyRepository,
    *,
    as_of: datetime,
    deadline_offset: int = 60,
    initial_status: str = "pending",
) -> tuple[str, str]:
    """Seed a delegate + pending review row; return (review_public_id, delegate_public_id)."""
    user_pid = str(uuid7())
    delegate_pid = str(uuid7())
    review_pid = str(uuid7())
    await repo.insert_ai_delegate(public_id=delegate_pid, user_public_id=user_pid, as_of=as_of)
    await repo.insert_ai_review(
        {
            "public_id": review_pid,
            "session_id": str(uuid7()),
            "sequence_id": 1,
            "user_public_id": str(uuid7()),
            "operator_public_id": str(uuid7()),
            "wallet_public_id": str(uuid7()),
            "instrument_public_id": str(uuid7()),
            "strategy_public_id": str(uuid7()),
            "selected_delegate_public_id": delegate_pid,
            "status": initial_status,
            "signal_envelope": {"side": "buy"},
            "signal_snapshot_hash": "h",
            "instrument_metadata": {},
            "deadline": as_of + timedelta(seconds=deadline_offset),
            "fanout_after": as_of + timedelta(seconds=30),
            "dispatch_version": 0,
            "created_at": as_of,
            "updated_at": as_of,
        }
    )
    return review_pid, delegate_pid


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_atomic_resolve_returns_none_for_unknown_review(tmp_path: Path) -> None:
    """Atomic resolve with an unknown id returns None.

    Given a fresh repository with no reviews,
    When ``atomic_resolve_ai_review`` is called for an unknown id,
    Then it returns ``None`` (Step 1 short-circuit).
    """
    repo = await _build_repo(tmp_path, "atomic_unknown.db")
    result = await repo.atomic_resolve_ai_review(
        review_public_id="ghost",
        decision="approve",
        responding_delegate_public_id=str(uuid7()),
        rationale=None,
        resolution_mode="pick_one_primary",
        new_status="resolved_approved",
        now=datetime.now(UTC),
    )
    assert result is None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_atomic_resolve_returns_none_when_already_terminal(tmp_path: Path) -> None:
    """Atomic resolve returns None when status already terminal.

    Given a review row resolved in a prior call,
    When ``atomic_resolve_ai_review`` is called again,
    Then it returns ``None`` (the already-terminal status guard fires).
    """
    repo = await _build_repo(tmp_path, "atomic_terminal.db")
    now = datetime.now(UTC)
    review_pid, delegate_pid = await _seed_pending_for_atomic(repo, as_of=now)
    first = await repo.atomic_resolve_ai_review(
        review_public_id=review_pid,
        decision="approve",
        responding_delegate_public_id=delegate_pid,
        rationale=None,
        resolution_mode="pick_one_primary",
        new_status="resolved_approved",
        now=now,
    )
    assert first is not None
    second = await repo.atomic_resolve_ai_review(
        review_public_id=review_pid,
        decision="approve",
        responding_delegate_public_id=delegate_pid,
        rationale=None,
        resolution_mode="pick_one_primary",
        new_status="resolved_approved",
        now=now,
    )
    assert second is None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_atomic_resolve_returns_none_when_deadline_elapsed(tmp_path: Path) -> None:
    """Atomic resolve returns None when deadline has already elapsed.

    Given a pending review whose deadline is in the past,
    When ``atomic_resolve_ai_review`` is called,
    Then it returns ``None`` (the deadline-gate guard fires).
    """
    repo = await _build_repo(tmp_path, "atomic_late.db")
    seed_at = datetime.now(UTC) - timedelta(seconds=120)
    review_pid, delegate_pid = await _seed_pending_for_atomic(
        repo, as_of=seed_at, deadline_offset=60
    )
    result = await repo.atomic_resolve_ai_review(
        review_public_id=review_pid,
        decision="approve",
        responding_delegate_public_id=delegate_pid,
        rationale=None,
        resolution_mode="pick_one_primary",
        new_status="resolved_approved",
        now=datetime.now(UTC),
    )
    assert result is None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_atomic_resolve_with_for_update_falls_back_on_not_implemented(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SQLite without ``SELECT FOR UPDATE`` falls back to plain SELECT.

    Given a session executor that raises NotImplementedError on the
        with_for_update path,
    When ``atomic_resolve_ai_review`` is called,
    Then it retries without the lock and still wins the transition.
    """
    repo = await _build_repo(tmp_path, "atomic_for_update_fallback.db")
    now = datetime.now(UTC)
    review_pid, delegate_pid = await _seed_pending_for_atomic(repo, as_of=now)

    original_with_for_update = Select.with_for_update

    def _raise_not_implemented(self: Select) -> Select:
        raise NotImplementedError("test fallback")

    monkeypatch.setattr(Select, "with_for_update", _raise_not_implemented)
    try:
        result = await repo.atomic_resolve_ai_review(
            review_public_id=review_pid,
            decision="approve",
            responding_delegate_public_id=delegate_pid,
            rationale=None,
            resolution_mode="pick_one_primary",
            new_status="resolved_approved",
            now=now,
        )
    finally:
        monkeypatch.setattr(Select, "with_for_update", original_with_for_update)
    assert result is not None
    assert result["previous_status"] == "pending"


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_atomic_resolve_rowcount_zero_returns_none(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Atomic resolve returns None when the UPDATE rowcount is 0.

    Given a stub session.execute that returns a result with rowcount=0,
    When ``atomic_resolve_ai_review`` runs the UPDATE step,
    Then it returns ``None`` even though the pre-snapshot looked eligible.
    """
    repo = await _build_repo(tmp_path, "atomic_rowcount0.db")
    now = datetime.now(UTC)
    review_pid, delegate_pid = await _seed_pending_for_atomic(repo, as_of=now)

    original_execute = AsyncSession.execute

    async def _stub_execute(self: _Any, statement: _Any, *a: _Any, **kw: _Any) -> _Any:
        result = await original_execute(self, statement, *a, **kw)

        if isinstance(statement, Update):

            class _Wrap:
                rowcount = 0

                def __init__(self, inner: _Any) -> None:
                    self._inner = inner

                def __getattr__(self, name: str) -> _Any:
                    return getattr(self._inner, name)

            return _Wrap(result)
        return result

    monkeypatch.setattr(AsyncSession, "execute", _stub_execute)
    try:
        result = await repo.atomic_resolve_ai_review(
            review_public_id=review_pid,
            decision="approve",
            responding_delegate_public_id=delegate_pid,
            rationale=None,
            resolution_mode="pick_one_primary",
            new_status="resolved_approved",
            now=now,
        )
    finally:
        monkeypatch.setattr(AsyncSession, "execute", original_execute)
    assert result is None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_atomic_timeout_returns_none_for_unknown_review(tmp_path: Path) -> None:
    """Timeout for an unknown id is a no-op.

    Given a fresh repository,
    When ``atomic_timeout_ai_review`` is called for an unknown id,
    Then it returns ``None``.
    """
    repo = await _build_repo(tmp_path, "timeout_unknown.db")
    result = await repo.atomic_timeout_ai_review(
        review_public_id="ghost",
        now=datetime.now(UTC),
    )
    assert result is None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_atomic_timeout_returns_none_when_already_terminal(tmp_path: Path) -> None:
    """Timeout returns None when row already terminal.

    Given a row already resolved by atomic_resolve,
    When ``atomic_timeout_ai_review`` runs,
    Then it returns ``None``.
    """
    repo = await _build_repo(tmp_path, "timeout_terminal.db")
    now = datetime.now(UTC)
    review_pid, delegate_pid = await _seed_pending_for_atomic(repo, as_of=now)
    await repo.atomic_resolve_ai_review(
        review_public_id=review_pid,
        decision="approve",
        responding_delegate_public_id=delegate_pid,
        rationale=None,
        resolution_mode="pick_one_primary",
        new_status="resolved_approved",
        now=now,
    )
    result = await repo.atomic_timeout_ai_review(
        review_public_id=review_pid,
        now=now,
    )
    assert result is None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_atomic_timeout_wins_for_pending_row(tmp_path: Path) -> None:
    """Atomic timeout flips a pending row to ``timeout``.

    Given a pending review row,
    When ``atomic_timeout_ai_review`` is called,
    Then the row's status becomes ``timeout`` and the result returns
        the captured selected_delegate_public_id + dispatch_version.
    """
    repo = await _build_repo(tmp_path, "timeout_win.db")
    now = datetime.now(UTC)
    review_pid, delegate_pid = await _seed_pending_for_atomic(repo, as_of=now)
    result = await repo.atomic_timeout_ai_review(
        review_public_id=review_pid,
        now=now,
    )
    assert result is not None
    assert result["selected_delegate_public_id"] == delegate_pid
    assert result["previous_status"] == "pending"
    row = await repo.get_ai_review(review_pid)
    assert row is not None
    assert row["status"] == "timeout"
    assert row["resolution_mode"] == "timeout_no_response"


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_atomic_timeout_rowcount_zero_returns_none(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Atomic timeout returns None on UPDATE rowcount=0.

    Given a stub that forces UPDATE rowcount=0,
    When ``atomic_timeout_ai_review`` is called,
    Then it returns ``None`` rather than raising.
    """
    repo = await _build_repo(tmp_path, "timeout_rowcount0.db")
    now = datetime.now(UTC)
    review_pid, _ = await _seed_pending_for_atomic(repo, as_of=now)

    original_execute = AsyncSession.execute

    async def _stub(self: _Any, statement: _Any, *a: _Any, **kw: _Any) -> _Any:
        result = await original_execute(self, statement, *a, **kw)

        if isinstance(statement, Update):

            class _Wrap:
                rowcount = 0

                def __init__(self, inner: _Any) -> None:
                    self._inner = inner

                def __getattr__(self, name: str) -> _Any:
                    return getattr(self._inner, name)

            return _Wrap(result)
        return result

    monkeypatch.setattr(AsyncSession, "execute", _stub)
    try:
        result = await repo.atomic_timeout_ai_review(
            review_public_id=review_pid,
            now=now,
        )
    finally:
        monkeypatch.setattr(AsyncSession, "execute", original_execute)
    assert result is None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_decrement_counter_returns_false_when_already_decremented(
    tmp_path: Path,
) -> None:
    """Decrement returns False on the second call.

    Given a review whose counter slot was claimed by a prior decrement,
    When ``decrement_delegate_active_count_for_review`` is called again,
    Then it returns ``False`` (the CAS predicate misses).
    """
    repo = await _build_repo(tmp_path, "decrement_idempotent.db")
    now = datetime.now(UTC)
    review_pid, delegate_pid = await _seed_pending_for_atomic(repo, as_of=now)
    first = await repo.decrement_delegate_active_count_for_review(
        review_public_id=review_pid,
        selected_delegate_public_id=delegate_pid,
        now=now,
    )
    assert first is True
    second = await repo.decrement_delegate_active_count_for_review(
        review_public_id=review_pid,
        selected_delegate_public_id=delegate_pid,
        now=now,
    )
    assert second is False


async def _seed_user_row(
    repo: SQLAlchemyRepository,
    *,
    user_public_id: str,
    role: str,
    as_of: datetime,
    username: str | None = None,
) -> None:
    """Insert an active ``users`` row with the requested role + public_id.

    Plan A v1.4 Q10 admission control filters delegates by
    ``users.role = 'ai_delegate'``; tests for
    ``list_eligible_delegates_for_ai_review`` need real User rows so the
    JOIN can resolve. The default username is derived from the public_id
    fragment to keep ``uq_users_username`` happy across multi-user tests.
    """
    async with repo.session() as s:
        s.add(
            User(
                public_id=user_public_id,
                username=username if username is not None else f"u-{user_public_id}",
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


async def _seed_live_delegate(
    repo: SQLAlchemyRepository,
    *,
    user_public_id: str,
    last_seen_at: datetime,
    creation_time: datetime,
) -> str:
    """Insert AiDelegate then bump ``last_seen_at`` to the requested wall clock.

    The CRUD ``insert_ai_delegate`` always seeds ``last_seen_at = NULL``
    (matches the production code path where the field is populated by
    :class:`WebSocketAuthManager` on first connect); the eligibility query
    requires non-NULL ``last_seen_at``, so tests must set it explicitly.
    """
    delegate_public_id = await _seed_delegate(
        repo, user_public_id=user_public_id, as_of=creation_time
    )
    await repo.update_delegate_last_seen(delegate_public_id, last_seen_at)
    return delegate_public_id


async def _seed_busy_delegate(
    repo: SQLAlchemyRepository,
    *,
    user_public_id: str,
    last_seen_at: datetime,
    creation_time: datetime,
) -> str:
    """Insert a live AiDelegate and bump ``active_reviews_count`` to 1.

    Forces the Q10 admission CAS UPDATE to miss so claim attempts fail
    deterministically. Using a raw UPDATE rather than a real review row
    keeps the helper independent of the AiReview INSERT contract.
    """
    delegate_public_id = await _seed_live_delegate(
        repo,
        user_public_id=user_public_id,
        last_seen_at=last_seen_at,
        creation_time=creation_time,
    )
    async with repo.session() as s:
        await s.execute(
            __import__("sqlalchemy")
            .update(AiDelegate)
            .where(AiDelegate.public_id == delegate_public_id)
            .values(active_reviews_count=1, updated_at=last_seen_at)
        )
        await s.commit()
    return delegate_public_id


async def _seed_full_eligible_setup(
    repo: SQLAlchemyRepository,
    *,
    as_of: datetime,
) -> dict[str, str]:
    """Seed a single eligible delegate end-to-end (user + membership + grant + delegate).

    Returns the same id-bag as :func:`_seed_user_operator_wallet_instrument`
    plus ``delegate_public_id`` so callers can assert against the candidate
    that admission control should pick.
    """
    ids = await _seed_user_operator_wallet_instrument(repo, as_of=as_of)
    await _seed_user_row(
        repo, user_public_id=ids["user_public_id"], role="ai_delegate", as_of=as_of
    )
    await _add_membership(
        repo,
        user_public_id=ids["user_public_id"],
        operator_public_id=ids["operator_public_id"],
        as_of=as_of,
    )
    await _add_instrument_grant(
        repo,
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        granted_by_user_public_id=ids["user_public_id"],
        as_of=as_of,
    )
    delegate_public_id = await _seed_live_delegate(
        repo,
        user_public_id=ids["user_public_id"],
        last_seen_at=as_of,
        creation_time=as_of,
    )
    ids["delegate_public_id"] = delegate_public_id
    return ids


def _make_review_payload(
    *,
    public_id: str,
    delegate_public_id: str,
    operator_public_id: str,
    wallet_public_id: str,
    instrument_public_id: str,
    user_public_id: str,
    as_of: datetime,
) -> dict[str, _Any]:
    """Build a valid AiReviewInsertRow payload mirroring the production seed."""
    return {
        "public_id": public_id,
        "session_id": str(uuid7()),
        "sequence_id": 1,
        "user_public_id": user_public_id,
        "operator_public_id": operator_public_id,
        "wallet_public_id": wallet_public_id,
        "instrument_public_id": instrument_public_id,
        "strategy_public_id": str(uuid7()),
        "selected_delegate_public_id": delegate_public_id,
        "status": "pending",
        "signal_envelope": {"side": "buy"},
        "signal_snapshot_hash": "deadbeef",
        "instrument_metadata": {"requires_ai_review": True},
        "deadline": as_of + timedelta(seconds=60),
        "fanout_after": as_of + timedelta(seconds=30),
        "dispatch_version": 0,
        "created_at": as_of,
        "updated_at": as_of,
    }


def _make_event_payload(
    *,
    public_id: str,
    review_public_id: str,
    as_of: datetime,
) -> dict[str, _Any]:
    """Build a valid ``ai_review_events.created`` payload."""
    return {
        "public_id": public_id,
        "review_public_id": review_public_id,
        "event_type": "created",
        "actor_delegate_public_id": None,
        "previous_status": None,
        "new_status": "pending",
        "payload": {"signal_snapshot_hash": "deadbeef", "dispatch_version": 0},
        "occurred_at": as_of,
    }


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_list_eligible_returns_empty_when_operator_has_no_grant(
    tmp_path: Path,
) -> None:
    """Operator without a matching scope grant -> empty list.

    Plan A Q10 admission control short-circuits before considering any
    delegate liveness, since the operator has no authority to act on the
    requested ``(wallet, instrument)`` tuple.
    """
    repo = await _build_repo(tmp_path, "eligible_no_grant.db")
    as_of = _now()
    ids = await _seed_user_operator_wallet_instrument(repo, as_of=as_of)
    await _seed_user_row(
        repo, user_public_id=ids["user_public_id"], role="ai_delegate", as_of=as_of
    )
    await _add_membership(
        repo,
        user_public_id=ids["user_public_id"],
        operator_public_id=ids["operator_public_id"],
        as_of=as_of,
    )
    await _seed_live_delegate(
        repo,
        user_public_id=ids["user_public_id"],
        last_seen_at=as_of,
        creation_time=as_of,
    )
    result = await repo.list_eligible_delegates_for_ai_review(
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        heartbeat_window_seconds=15,
        as_of=as_of,
    )
    assert result == []


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_list_eligible_returns_empty_when_user_role_is_not_ai_delegate(
    tmp_path: Path,
) -> None:
    """Live AiDelegate row + grant + membership but ``users.role='viewer'`` -> empty.

    Defends against the row-soup scenario where someone backfills an
    ``ai_delegates`` row without flipping the user's role; the JOIN
    requires the role check to pass.
    """
    repo = await _build_repo(tmp_path, "eligible_wrong_role.db")
    as_of = _now()
    ids = await _seed_user_operator_wallet_instrument(repo, as_of=as_of)
    await _seed_user_row(repo, user_public_id=ids["user_public_id"], role="viewer", as_of=as_of)
    await _add_membership(
        repo,
        user_public_id=ids["user_public_id"],
        operator_public_id=ids["operator_public_id"],
        as_of=as_of,
    )
    await _add_instrument_grant(
        repo,
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        granted_by_user_public_id=ids["user_public_id"],
        as_of=as_of,
    )
    await _seed_live_delegate(
        repo,
        user_public_id=ids["user_public_id"],
        last_seen_at=as_of,
        creation_time=as_of,
    )
    result = await repo.list_eligible_delegates_for_ai_review(
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        heartbeat_window_seconds=15,
        as_of=as_of,
    )
    assert result == []


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_list_eligible_excludes_offline_delegates(tmp_path: Path) -> None:
    """``last_seen_at`` outside the heartbeat window -> delegate filtered out.

    The boundary is strict: a delegate whose ``last_seen_at`` is
    ``as_of - heartbeat_window_seconds`` (exactly on the threshold) does
    NOT pass because the SQL uses ``last_seen_at > threshold``.
    """
    repo = await _build_repo(tmp_path, "eligible_offline.db")
    as_of = _now()
    ids = await _seed_full_eligible_setup(repo, as_of=as_of)
    async with repo.session() as s:
        await s.execute(
            __import__("sqlalchemy")
            .update(AiDelegate)
            .where(AiDelegate.public_id == ids["delegate_public_id"])
            .values(last_seen_at=as_of - timedelta(seconds=60))
        )
        await s.commit()
    result = await repo.list_eligible_delegates_for_ai_review(
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        heartbeat_window_seconds=15,
        as_of=as_of,
    )
    assert result == []


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_list_eligible_excludes_delegate_without_membership(
    tmp_path: Path,
) -> None:
    """AI_DELEGATE user but no operator membership -> empty list."""
    repo = await _build_repo(tmp_path, "eligible_no_membership.db")
    as_of = _now()
    ids = await _seed_user_operator_wallet_instrument(repo, as_of=as_of)
    await _seed_user_row(
        repo, user_public_id=ids["user_public_id"], role="ai_delegate", as_of=as_of
    )
    await _add_instrument_grant(
        repo,
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        granted_by_user_public_id=ids["user_public_id"],
        as_of=as_of,
    )
    await _seed_live_delegate(
        repo,
        user_public_id=ids["user_public_id"],
        last_seen_at=as_of,
        creation_time=as_of,
    )
    result = await repo.list_eligible_delegates_for_ai_review(
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        heartbeat_window_seconds=15,
        as_of=as_of,
    )
    assert result == []


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_list_eligible_returns_delegate_with_instrument_grant(
    tmp_path: Path,
) -> None:
    """Happy path with an instrument-direct grant returns the live delegate."""
    repo = await _build_repo(tmp_path, "eligible_instrument_grant.db")
    as_of = _now()
    ids = await _seed_full_eligible_setup(repo, as_of=as_of)
    result = await repo.list_eligible_delegates_for_ai_review(
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        heartbeat_window_seconds=15,
        as_of=as_of,
    )
    assert len(result) == 1
    assert result[0]["public_id"] == ids["delegate_public_id"]
    assert result[0]["user_public_id"] == ids["user_public_id"]
    assert result[0]["active_reviews_count"] == 0
    assert result[0]["last_seen_at"] is not None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_list_eligible_returns_delegate_with_underlying_grant(
    tmp_path: Path,
) -> None:
    """Underlying-kind grant + matching mapping -> delegate eligible too."""
    repo = await _build_repo(tmp_path, "eligible_underlying_grant.db")
    as_of = _now()
    ids = await _seed_user_operator_wallet_instrument(repo, as_of=as_of)
    await _seed_user_row(
        repo, user_public_id=ids["user_public_id"], role="ai_delegate", as_of=as_of
    )
    await _add_membership(
        repo,
        user_public_id=ids["user_public_id"],
        operator_public_id=ids["operator_public_id"],
        as_of=as_of,
    )
    await _add_underlying_grant(
        repo,
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        underlying_public_id=ids["underlying_public_id"],
        granted_by_user_public_id=ids["user_public_id"],
        as_of=as_of,
    )
    delegate_public_id = await _seed_live_delegate(
        repo,
        user_public_id=ids["user_public_id"],
        last_seen_at=as_of,
        creation_time=as_of,
    )
    result = await repo.list_eligible_delegates_for_ai_review(
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        heartbeat_window_seconds=15,
        as_of=as_of,
    )
    assert len(result) == 1
    assert result[0]["public_id"] == delegate_public_id


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_list_eligible_orders_candidates_by_last_seen_desc(
    tmp_path: Path,
) -> None:
    """Multiple eligible delegates returned most-recently-seen first.

    Plan A v1.4 Q10 specifies ``ORDER BY last_seen_at DESC`` so admission
    control prefers the freshest live connection. The test seeds two
    delegates 3 seconds apart and asserts the newer one comes first.
    """
    repo = await _build_repo(tmp_path, "eligible_order.db")
    as_of = _now()
    ids = await _seed_user_operator_wallet_instrument(repo, as_of=as_of)
    await _seed_user_row(
        repo, user_public_id=ids["user_public_id"], role="ai_delegate", as_of=as_of
    )
    await _add_membership(
        repo,
        user_public_id=ids["user_public_id"],
        operator_public_id=ids["operator_public_id"],
        as_of=as_of,
    )
    await _add_instrument_grant(
        repo,
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        granted_by_user_public_id=ids["user_public_id"],
        as_of=as_of,
    )
    older = await _seed_live_delegate(
        repo,
        user_public_id=ids["user_public_id"],
        last_seen_at=as_of - timedelta(seconds=5),
        creation_time=as_of,
    )
    newer_user_pid = str(uuid7())
    await _seed_user_row(repo, user_public_id=newer_user_pid, role="ai_delegate", as_of=as_of)
    await _add_membership(
        repo,
        user_public_id=newer_user_pid,
        operator_public_id=ids["operator_public_id"],
        as_of=as_of,
    )
    newer = await _seed_live_delegate(
        repo,
        user_public_id=newer_user_pid,
        last_seen_at=as_of - timedelta(seconds=2),
        creation_time=as_of,
    )
    result = await repo.list_eligible_delegates_for_ai_review(
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        heartbeat_window_seconds=30,
        as_of=as_of,
    )
    assert [d["public_id"] for d in result] == [newer, older]


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_claim_and_insert_returns_first_candidate_on_success(
    tmp_path: Path,
) -> None:
    """Single idle candidate -> CAS wins, INSERT lands, returns id.

    Verifies the happy path end-to-end: ``ai_delegates`` row's counter
    goes 0 -> 1, the ``ai_reviews`` row is persisted with the claimed
    delegate as ``selected_delegate_public_id``, and the matching
    ``ai_review_events`` ``created`` row is appended.
    """
    repo = await _build_repo(tmp_path, "claim_first_success.db")
    as_of = _now()
    ids = await _seed_full_eligible_setup(repo, as_of=as_of)
    review_id = str(uuid7())
    event_id = str(uuid7())
    review_payload = _make_review_payload(
        public_id=review_id,
        delegate_public_id=ids["delegate_public_id"],
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        user_public_id=ids["user_public_id"],
        as_of=as_of,
    )
    event_payload = _make_event_payload(public_id=event_id, review_public_id=review_id, as_of=as_of)
    selected = await repo.claim_and_insert_ai_review(
        candidate_delegate_public_ids=[ids["delegate_public_id"]],
        review_data=review_payload,
        event_data=event_payload,
        now=as_of,
    )
    assert selected == ids["delegate_public_id"]
    delegate_row = await repo.get_ai_delegate_by_user_public_id(ids["user_public_id"])
    assert delegate_row is not None
    assert delegate_row["active_reviews_count"] == 1
    review_row = await repo.get_ai_review(review_id)
    assert review_row is not None
    assert review_row["selected_delegate_public_id"] == ids["delegate_public_id"]


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_claim_and_insert_skips_busy_candidate_picks_next(
    tmp_path: Path,
) -> None:
    """First busy candidate is skipped; second idle wins the claim.

    Confirms the iteration semantics — every CAS UPDATE that returns 0
    rowcount leaves no side effect, so the next candidate gets a clean
    attempt within the same transaction.
    """
    repo = await _build_repo(tmp_path, "claim_skip_busy.db")
    as_of = _now()
    ids = await _seed_user_operator_wallet_instrument(repo, as_of=as_of)
    await _seed_user_row(
        repo, user_public_id=ids["user_public_id"], role="ai_delegate", as_of=as_of
    )
    await _add_membership(
        repo,
        user_public_id=ids["user_public_id"],
        operator_public_id=ids["operator_public_id"],
        as_of=as_of,
    )
    await _add_instrument_grant(
        repo,
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        granted_by_user_public_id=ids["user_public_id"],
        as_of=as_of,
    )
    busy_pid = await _seed_busy_delegate(
        repo,
        user_public_id=ids["user_public_id"],
        last_seen_at=as_of,
        creation_time=as_of,
    )
    second_user_pid = str(uuid7())
    await _seed_user_row(repo, user_public_id=second_user_pid, role="ai_delegate", as_of=as_of)
    idle_pid = await _seed_live_delegate(
        repo,
        user_public_id=second_user_pid,
        last_seen_at=as_of,
        creation_time=as_of,
    )
    review_id = str(uuid7())
    event_id = str(uuid7())
    review_payload = _make_review_payload(
        public_id=review_id,
        delegate_public_id=busy_pid,
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        user_public_id=ids["user_public_id"],
        as_of=as_of,
    )
    event_payload = _make_event_payload(public_id=event_id, review_public_id=review_id, as_of=as_of)
    selected = await repo.claim_and_insert_ai_review(
        candidate_delegate_public_ids=[busy_pid, idle_pid],
        review_data=review_payload,
        event_data=event_payload,
        now=as_of,
    )
    assert selected == idle_pid
    review_row = await repo.get_ai_review(review_id)
    assert review_row is not None
    assert review_row["selected_delegate_public_id"] == idle_pid


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_claim_and_insert_returns_none_when_all_busy(tmp_path: Path) -> None:
    """Every candidate busy -> None, no review row, no counter change.

    Plan A v1.4 §3.1.d ``DelegateBusyError`` precondition: when the CAS
    misses on every candidate the iteration finishes without committing,
    so neither the review row nor any audit event is persisted.
    """
    repo = await _build_repo(tmp_path, "claim_all_busy.db")
    as_of = _now()
    ids = await _seed_user_operator_wallet_instrument(repo, as_of=as_of)
    await _seed_user_row(
        repo, user_public_id=ids["user_public_id"], role="ai_delegate", as_of=as_of
    )
    busy_one = await _seed_busy_delegate(
        repo,
        user_public_id=ids["user_public_id"],
        last_seen_at=as_of,
        creation_time=as_of,
    )
    second_user_pid = str(uuid7())
    await _seed_user_row(repo, user_public_id=second_user_pid, role="ai_delegate", as_of=as_of)
    busy_two = await _seed_busy_delegate(
        repo,
        user_public_id=second_user_pid,
        last_seen_at=as_of,
        creation_time=as_of,
    )
    review_id = str(uuid7())
    event_id = str(uuid7())
    review_payload = _make_review_payload(
        public_id=review_id,
        delegate_public_id=busy_one,
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        user_public_id=ids["user_public_id"],
        as_of=as_of,
    )
    event_payload = _make_event_payload(public_id=event_id, review_public_id=review_id, as_of=as_of)
    selected = await repo.claim_and_insert_ai_review(
        candidate_delegate_public_ids=[busy_one, busy_two],
        review_data=review_payload,
        event_data=event_payload,
        now=as_of,
    )
    assert selected is None
    assert await repo.get_ai_review(review_id) is None
    busy_one_row = await repo.get_ai_delegate_by_user_public_id(ids["user_public_id"])
    busy_two_row = await repo.get_ai_delegate_by_user_public_id(second_user_pid)
    assert busy_one_row is not None
    assert busy_two_row is not None
    assert busy_one_row["active_reviews_count"] == 1
    assert busy_two_row["active_reviews_count"] == 1


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_claim_and_insert_returns_none_for_empty_candidate_list(
    tmp_path: Path,
) -> None:
    """Empty input list -> None without touching the DB.

    Defensive contract — service callers pre-filter and raise
    ``NoLiveDelegateError`` themselves, but the repo method must not
    crash if it ever receives an empty list.
    """
    repo = await _build_repo(tmp_path, "claim_empty_list.db")
    as_of = _now()
    review_id = str(uuid7())
    event_id = str(uuid7())
    review_payload = _make_review_payload(
        public_id=review_id,
        delegate_public_id=str(uuid7()),
        operator_public_id=str(uuid7()),
        wallet_public_id=str(uuid7()),
        instrument_public_id=str(uuid7()),
        user_public_id=str(uuid7()),
        as_of=as_of,
    )
    event_payload = _make_event_payload(public_id=event_id, review_public_id=review_id, as_of=as_of)
    selected = await repo.claim_and_insert_ai_review(
        candidate_delegate_public_ids=[],
        review_data=review_payload,
        event_data=event_payload,
        now=as_of,
    )
    assert selected is None
    assert await repo.get_ai_review(review_id) is None
