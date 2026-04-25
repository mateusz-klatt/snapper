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
from snapper.data.models import UserOperatorMembership
from snapper.data.models import WalletOperatorScopeGrant
from snapper.data.repository import SQLAlchemyRepository

TEST_TIMEOUT = 15


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
