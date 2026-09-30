"""Tests for the AI-review repository CRUD primitives + delegate scope check.

Covers ``has_grant_for_delegate`` (four-step resolve: delegate -> user
-> operator memberships -> instrument-direct or underlying-kind grant)
plus the six AiReview / AiDelegate CRUD methods that back the
AiReviewService.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any as _Any
from uuid import uuid7

import pytest
from sqlalchemy import update as sqlalchemy_update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.dml import Update
from sqlalchemy.sql.expression import Select

from snapper.auth.domain.roles import UserRole
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
        symbol: Symbol = (await s.execute(__import__("sqlalchemy").select(Symbol))).scalar_one()
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
        instrument: Instrument = (
            await s.execute(__import__("sqlalchemy").select(Instrument))
        ).scalar_one()
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


async def _insert_review_row(
    repo: SQLAlchemyRepository,
    *,
    as_of: datetime,
    status: str = "pending",
    wallet_public_id: str | None = None,
    strategy_public_id: str | None = None,
    operator_public_id: str | None = None,
    created_at: datetime | None = None,
) -> str:
    """Insert a minimal AiReview row for ``list_ai_reviews`` tests; return public_id."""
    review_id = str(uuid7())
    created = created_at if created_at is not None else as_of
    await repo.insert_ai_review(
        {
            "public_id": review_id,
            "session_id": str(uuid7()),
            "sequence_id": 1,
            "user_public_id": str(uuid7()),
            "operator_public_id": (
                operator_public_id if operator_public_id is not None else str(uuid7())
            ),
            "wallet_public_id": wallet_public_id if wallet_public_id is not None else str(uuid7()),
            "instrument_public_id": str(uuid7()),
            "strategy_public_id": (
                strategy_public_id if strategy_public_id is not None else str(uuid7())
            ),
            "selected_delegate_public_id": str(uuid7()),
            "status": status,
            "signal_envelope": {"side": "buy"},
            "signal_snapshot_hash": "h",
            "instrument_metadata": {},
            "deadline": as_of + timedelta(seconds=60),
            "fanout_after": as_of + timedelta(seconds=30),
            "dispatch_version": 0,
            "created_at": created,
            "updated_at": created,
        }
    )
    return review_id


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_list_ai_reviews_returns_empty_on_empty_table(tmp_path: Path) -> None:
    """No rows -> empty list (no crash, no filters applied)."""
    repo = await _build_repo(tmp_path, "list_reviews_empty.db")
    result = await repo.list_ai_reviews(limit=100)
    assert result == []


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_list_ai_reviews_orders_newest_first_and_respects_limit(tmp_path: Path) -> None:
    """Rows come back ``created_at DESC`` and ``limit`` caps the snapshot."""
    repo = await _build_repo(tmp_path, "list_reviews_order.db")
    now = _now()
    older = await _insert_review_row(repo, as_of=now, created_at=now - timedelta(seconds=10))
    newer = await _insert_review_row(repo, as_of=now, created_at=now)
    result = await repo.list_ai_reviews(limit=100)
    assert [r["public_id"] for r in result] == [newer, older]
    limited = await repo.list_ai_reviews(limit=1)
    assert [r["public_id"] for r in limited] == [newer]


async def _mark_review_resolved(
    repo: SQLAlchemyRepository,
    *,
    review_public_id: str,
    decision: str,
    responding_delegate_public_id: str,
    as_of: datetime,
) -> None:
    """Flip a pending review to ``resolved_approved`` consistently (direct UPDATE).

    Satisfies ``ck_ai_reviews_status_consistency`` (terminal status needs a
    non-NULL decision / responding delegate / resolution_mode / resolved_at)
    without threading the full admission + resolve service path.
    """
    async with repo.session() as s:
        await s.execute(
            __import__("sqlalchemy")
            .update(AiReview)
            .where(AiReview.public_id == review_public_id)
            .values(
                status="resolved_approved",
                decision=decision,
                responding_delegate_public_id=responding_delegate_public_id,
                resolution_mode="pick_one_primary",
                resolved_at=as_of,
                updated_at=as_of,
            )
        )
        await s.commit()


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_list_ai_reviews_filters_by_status(tmp_path: Path) -> None:
    """The ``status`` filter narrows to matching rows only."""
    repo = await _build_repo(tmp_path, "list_reviews_status.db")
    now = _now()
    await _insert_review_row(repo, as_of=now, status="pending")
    dispatched = await _insert_review_row(repo, as_of=now, status="fanout_dispatched")
    result = await repo.list_ai_reviews(limit=100, status="fanout_dispatched")
    assert [r["public_id"] for r in result] == [dispatched]
    assert result[0]["status"] == "fanout_dispatched"


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_list_ai_reviews_surfaces_terminal_decision(tmp_path: Path) -> None:
    """A resolved review surfaces its ``decision`` — the operator-audit contract."""
    repo = await _build_repo(tmp_path, "list_reviews_decision.db")
    now = _now()
    review_id = await _insert_review_row(repo, as_of=now)
    await _mark_review_resolved(
        repo,
        review_public_id=review_id,
        decision="approve",
        responding_delegate_public_id=str(uuid7()),
        as_of=now,
    )
    result = await repo.list_ai_reviews(limit=100)
    assert len(result) == 1
    assert result[0]["status"] == "resolved_approved"
    assert result[0]["decision"] == "approve"
    assert result[0]["responding_delegate_public_id"] is not None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_list_ai_reviews_filters_by_wallet_and_strategy(tmp_path: Path) -> None:
    """The ``wallet_public_id`` and ``strategy_public_id`` filters both narrow exactly."""
    repo = await _build_repo(tmp_path, "list_reviews_wallet_strategy.db")
    now = _now()
    wallet = str(uuid7())
    strategy = str(uuid7())
    target = await _insert_review_row(
        repo, as_of=now, wallet_public_id=wallet, strategy_public_id=strategy
    )
    await _insert_review_row(repo, as_of=now)
    by_wallet = await repo.list_ai_reviews(limit=100, wallet_public_id=wallet)
    assert [r["public_id"] for r in by_wallet] == [target]
    by_strategy = await repo.list_ai_reviews(limit=100, strategy_public_id=strategy)
    assert [r["public_id"] for r in by_strategy] == [target]


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_list_ai_reviews_filters_by_operator_membership(tmp_path: Path) -> None:
    """``operator_public_ids`` restricts the snapshot to those operators' reviews."""
    repo = await _build_repo(tmp_path, "list_reviews_operator.db")
    now = _now()
    op_mine = str(uuid7())
    op_other = str(uuid7())
    mine = await _insert_review_row(repo, as_of=now, operator_public_id=op_mine)
    await _insert_review_row(repo, as_of=now, operator_public_id=op_other)
    scoped = await repo.list_ai_reviews(limit=100, operator_public_ids=[op_mine])
    assert [r["public_id"] for r in scoped] == [mine]
    empty = await repo.list_ai_reviews(limit=100, operator_public_ids=[])
    assert empty == []


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
    """Insert + read-back covers all stored fields including the active-reviews counter."""
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


async def _seed_user_row(
    repo: SQLAlchemyRepository,
    *,
    user_public_id: str,
    role: str,
    as_of: datetime,
    username: str | None = None,
) -> None:
    """Insert an active ``users`` row with the requested role + public_id.

    Admission control filters delegates by the shared AI review-principal
    roles; tests for
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

    Forces the admission CAS UPDATE to miss so claim attempts fail
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
    role: UserRole = UserRole.AI_DELEGATE,
) -> dict[str, str]:
    """Seed one eligible review principal with membership, grant, and delegate row.

    Returns the same id-bag as :func:`_seed_user_operator_wallet_instrument`
    plus ``delegate_public_id`` so callers can assert against the candidate
    that admission control should pick.
    """
    ids = await _seed_user_operator_wallet_instrument(repo, as_of=as_of)
    await _seed_user_row(
        repo,
        user_public_id=ids["user_public_id"],
        role=role.value,
        as_of=as_of,
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

    Admission control short-circuits before considering any
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
async def test_list_eligible_returns_empty_when_user_role_is_not_ai_review_principal(
    tmp_path: Path,
) -> None:
    """Live AiDelegate row + grant + membership but ``users.role='viewer'`` -> empty.

    Defends against the row-soup scenario where someone backfills an
    ``ai_delegates`` row without flipping the user's role; the JOIN
    requires membership in the shared AI review-principal role set.
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
async def test_list_eligible_returns_ai_reviewer_with_instrument_grant(
    tmp_path: Path,
) -> None:
    """AI_REVIEWER participates in review admission through the delegate lifecycle.

    Given: A live AI_REVIEWER with operator membership and an instrument grant.
    When: Eligible delegates are listed for that operator, wallet, and instrument.
    Then: The reviewer's shared AiDelegate operational row is returned.
    """
    repo = await _build_repo(tmp_path, "eligible_ai_reviewer.db")
    as_of = _now()
    ids = await _seed_full_eligible_setup(
        repo,
        as_of=as_of,
        role=UserRole.AI_REVIEWER,
    )

    result = await repo.list_eligible_delegates_for_ai_review(
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        heartbeat_window_seconds=15,
        as_of=as_of,
    )

    assert [row["public_id"] for row in result] == [ids["delegate_public_id"]]


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_list_eligible_excludes_deactivated_user(tmp_path: Path) -> None:
    """``users.is_active=False`` -> delegate filtered out even when fully scoped + live.

    User deactivation is implemented as a ``users.is_active=False``
    flip on the active SCD2 row (see ``UserService.deactivate_user``);
    the eligibility query MUST honour that flag so a recently-seen
    deactivated delegate cannot still be claimed during the heartbeat
    window.
    """
    repo = await _build_repo(tmp_path, "eligible_deactivated.db")
    as_of = _now()
    ids = await _seed_full_eligible_setup(repo, as_of=as_of)
    async with repo.session() as s:
        await s.execute(
            __import__("sqlalchemy")
            .update(User)
            .where(User.public_id == ids["user_public_id"])
            .values(is_active=False)
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
async def test_list_eligible_orders_candidates_by_immutable_creation_identity(
    tmp_path: Path,
) -> None:
    """Candidate base order ignores heartbeat timing and uses immutable identity.

    Given an earlier-created delegate with the older heartbeat and a
    later-created delegate with the fresher heartbeat,
    When eligible candidates are listed,
    Then immutable creation order wins and a heartbeat phase shift cannot
    reorder the list that owner-affinity selection canonicalises.
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
    first_created = await _seed_live_delegate(
        repo,
        user_public_id=ids["user_public_id"],
        last_seen_at=as_of - timedelta(seconds=5),
        creation_time=as_of - timedelta(seconds=10),
    )
    second_user_pid = str(uuid7())
    await _seed_user_row(repo, user_public_id=second_user_pid, role="ai_delegate", as_of=as_of)
    await _add_membership(
        repo,
        user_public_id=second_user_pid,
        operator_public_id=ids["operator_public_id"],
        as_of=as_of,
    )
    second_created = await _seed_live_delegate(
        repo,
        user_public_id=second_user_pid,
        last_seen_at=as_of - timedelta(seconds=2),
        creation_time=as_of - timedelta(seconds=5),
    )
    result = await repo.list_eligible_delegates_for_ai_review(
        operator_public_id=ids["operator_public_id"],
        wallet_public_id=ids["wallet_public_id"],
        instrument_public_id=ids["instrument_public_id"],
        heartbeat_window_seconds=30,
        as_of=as_of,
    )
    assert [delegate["public_id"] for delegate in result] == [first_created, second_created]


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

    ``DelegateBusyError`` precondition: when the CAS misses on every
    candidate the iteration finishes without committing, so neither
    the review row nor any audit event is persisted.
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


def _audit_event_for(
    *,
    review_pid: str,
    event_type: str,
    new_status: str,
    actor_delegate_public_id: str | None,
    payload: dict[str, _Any],
    occurred_at: datetime,
    previous_status: str = "pending",
) -> dict[str, _Any]:
    """Build a minimal :class:`AiReviewEventInsertRow` for the combined primitives.

    The combined-primitive tests below all need a complete
    ``AiReviewEventInsertRow`` payload but the ``previous_status`` field
    is overwritten by the primitive's SELECT-FOR-UPDATE result so the
    sentinel passed here is irrelevant for correctness — kept "pending"
    for readability.
    """
    return {
        "public_id": str(uuid7()),
        "review_public_id": review_pid,
        "event_type": event_type,
        "actor_delegate_public_id": actor_delegate_public_id,
        "previous_status": previous_status,
        "new_status": new_status,
        "payload": payload,
        "occurred_at": occurred_at,
    }


async def _fetch_audit_event_payloads(
    repo: SQLAlchemyRepository, *, review_pid: str
) -> list[dict[str, _Any]]:
    """Read every ``ai_review_events`` row for ``review_pid`` ordered by ``occurred_at``."""
    async with repo.session() as s:
        rows = (
            await s.execute(
                __import__("sqlalchemy")
                .select(
                    AiReviewEvent.event_type,
                    AiReviewEvent.previous_status,
                    AiReviewEvent.new_status,
                    AiReviewEvent.payload,
                    AiReviewEvent.occurred_at,
                    AiReviewEvent.actor_delegate_public_id,
                )
                .where(AiReviewEvent.review_public_id == review_pid)
                .order_by(AiReviewEvent.occurred_at.asc())
            )
        ).all()
    return [
        {
            "event_type": str(r[0]),
            "previous_status": str(r[1]) if r[1] is not None else None,
            "new_status": str(r[2]),
            "payload": dict(r[3]),
            "occurred_at": r[4],
            "actor_delegate_public_id": r[5],
        }
        for r in rows
    ]


async def _fetch_delegate_counter(
    repo: SQLAlchemyRepository, *, delegate_pid: str
) -> tuple[int, datetime | None]:
    """Return ``(active_reviews_count, ai_reviews.counter_decremented_at)`` for the test row."""
    async with repo.session() as s:
        delegate: int = (
            await s.execute(
                __import__("sqlalchemy")
                .select(AiDelegate.active_reviews_count)
                .where(AiDelegate.public_id == delegate_pid)
            )
        ).scalar_one()
        decremented = (
            await s.execute(
                __import__("sqlalchemy")
                .select(AiReview.counter_decremented_at)
                .where(AiReview.selected_delegate_public_id == delegate_pid)
            )
        ).scalar_one_or_none()
    return int(delegate), decremented


async def _seed_pending_with_active_counter(
    repo: SQLAlchemyRepository, *, as_of: datetime, deadline_offset: int = 60
) -> tuple[str, str]:
    """Seed a delegate with ``active_reviews_count = 1`` + a pending review row."""
    review_pid, delegate_pid = await _seed_pending_for_atomic(
        repo, as_of=as_of, deadline_offset=deadline_offset
    )
    async with repo.session() as s:
        await s.execute(
            __import__("sqlalchemy")
            .update(AiDelegate)
            .where(AiDelegate.public_id == delegate_pid)
            .values(active_reviews_count=1, updated_at=as_of)
        )
        await s.commit()
    return review_pid, delegate_pid


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_combined_resolve_with_audit_and_counter_atomic_happy_path(
    tmp_path: Path,
) -> None:
    """Combined resolve + audit + counter happens in ONE transaction.

    Given a pending review with the delegate's counter elevated to 1,
    When :meth:`atomic_resolve_review_with_audit_and_counter` wins the CAS,
    Then the row flips to ``resolved_approved``, the audit event lands with
    ``previous_status='pending'`` (captured BY the primitive, not the caller),
    and the delegate counter is decremented to 0 with
    ``counter_decremented_at`` non-NULL.
    """
    repo = await _build_repo(tmp_path, "combined_resolve.db")
    now = datetime.now(UTC)
    review_pid, delegate_pid = await _seed_pending_with_active_counter(repo, as_of=now)
    audit = _audit_event_for(
        review_pid=review_pid,
        event_type="decision_recorded",
        new_status="resolved_approved",
        actor_delegate_public_id=delegate_pid,
        payload={"decision": "approve", "rationale": None},
        occurred_at=now,
        previous_status="ignored-sentinel",
    )
    result = await repo.atomic_resolve_review_with_audit_and_counter(
        review_public_id=review_pid,
        decision="approve",
        responding_delegate_public_id=delegate_pid,
        rationale=None,
        new_status="resolved_approved",
        audit_event=audit,
        now=now,
    )
    assert result is not None
    assert result["selected_delegate_public_id"] == delegate_pid
    assert result["previous_status"] == "pending"
    row = await repo.get_ai_review(review_pid)
    assert row is not None
    assert row["status"] == "resolved_approved"
    audit_rows = await _fetch_audit_event_payloads(repo, review_pid=review_pid)
    assert len(audit_rows) == 1
    assert audit_rows[0]["event_type"] == "decision_recorded"
    assert audit_rows[0]["previous_status"] == "pending"
    counter, decremented_at = await _fetch_delegate_counter(repo, delegate_pid=delegate_pid)
    assert counter == 0
    assert decremented_at is not None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_combined_resolve_returns_none_when_already_terminal(tmp_path: Path) -> None:
    """Combined resolve no-ops on already-terminal rows + writes nothing.

    Given a row already resolved by a prior call,
    When :meth:`atomic_resolve_review_with_audit_and_counter` is called,
    Then it returns ``None``, no second audit event lands, and the
    delegate counter is not double-decremented.
    """
    repo = await _build_repo(tmp_path, "combined_resolve_terminal.db")
    now = datetime.now(UTC)
    review_pid, delegate_pid = await _seed_pending_with_active_counter(repo, as_of=now)
    first_audit = _audit_event_for(
        review_pid=review_pid,
        event_type="decision_recorded",
        new_status="resolved_approved",
        actor_delegate_public_id=delegate_pid,
        payload={"decision": "approve"},
        occurred_at=now,
    )
    await repo.atomic_resolve_review_with_audit_and_counter(
        review_public_id=review_pid,
        decision="approve",
        responding_delegate_public_id=delegate_pid,
        rationale=None,
        new_status="resolved_approved",
        audit_event=first_audit,
        now=now,
    )
    second_audit = _audit_event_for(
        review_pid=review_pid,
        event_type="decision_recorded",
        new_status="resolved_approved",
        actor_delegate_public_id=delegate_pid,
        payload={"decision": "approve"},
        occurred_at=now,
    )
    second = await repo.atomic_resolve_review_with_audit_and_counter(
        review_public_id=review_pid,
        decision="approve",
        responding_delegate_public_id=delegate_pid,
        rationale=None,
        new_status="resolved_approved",
        audit_event=second_audit,
        now=now,
    )
    assert second is None
    audit_rows = await _fetch_audit_event_payloads(repo, review_pid=review_pid)
    assert len(audit_rows) == 1
    counter, _ = await _fetch_delegate_counter(repo, delegate_pid=delegate_pid)
    assert counter == 0


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_combined_resolve_returns_none_when_deadline_elapsed(tmp_path: Path) -> None:
    """Combined resolve respects the deadline gate (no transition past deadline)."""
    repo = await _build_repo(tmp_path, "combined_resolve_late.db")
    seed_at = datetime.now(UTC) - timedelta(seconds=120)
    review_pid, delegate_pid = await _seed_pending_with_active_counter(
        repo, as_of=seed_at, deadline_offset=60
    )
    audit = _audit_event_for(
        review_pid=review_pid,
        event_type="decision_recorded",
        new_status="resolved_approved",
        actor_delegate_public_id=delegate_pid,
        payload={"decision": "approve"},
        occurred_at=datetime.now(UTC),
    )
    result = await repo.atomic_resolve_review_with_audit_and_counter(
        review_public_id=review_pid,
        decision="approve",
        responding_delegate_public_id=delegate_pid,
        rationale=None,
        new_status="resolved_approved",
        audit_event=audit,
        now=datetime.now(UTC),
    )
    assert result is None
    audit_rows = await _fetch_audit_event_payloads(repo, review_pid=review_pid)
    assert len(audit_rows) == 0
    counter, decremented = await _fetch_delegate_counter(repo, delegate_pid=delegate_pid)
    assert counter == 1
    assert decremented is None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_combined_timeout_with_audit_and_counter_atomic_happy_path(
    tmp_path: Path,
) -> None:
    """Combined timeout + audit + counter atomically transitions."""
    repo = await _build_repo(tmp_path, "combined_timeout.db")
    now = datetime.now(UTC)
    review_pid, delegate_pid = await _seed_pending_with_active_counter(repo, as_of=now)
    audit = _audit_event_for(
        review_pid=review_pid,
        event_type="timeout_marked",
        new_status="timeout",
        actor_delegate_public_id=None,
        payload={"trigger": "test"},
        occurred_at=now,
    )
    result = await repo.atomic_timeout_review_with_audit_and_counter(
        review_public_id=review_pid,
        audit_event=audit,
        now=now,
    )
    assert result is not None
    assert result["selected_delegate_public_id"] == delegate_pid
    assert result["previous_status"] == "pending"
    row = await repo.get_ai_review(review_pid)
    assert row is not None
    assert row["status"] == "timeout"
    assert row["resolution_mode"] == "timeout_no_response"
    audit_rows = await _fetch_audit_event_payloads(repo, review_pid=review_pid)
    assert len(audit_rows) == 1
    assert audit_rows[0]["event_type"] == "timeout_marked"
    counter, decremented = await _fetch_delegate_counter(repo, delegate_pid=delegate_pid)
    assert counter == 0
    assert decremented is not None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_combined_timeout_returns_none_when_already_terminal(tmp_path: Path) -> None:
    """Combined timeout no-ops on already-terminal rows."""
    repo = await _build_repo(tmp_path, "combined_timeout_terminal.db")
    now = datetime.now(UTC)
    review_pid, _delegate_pid = await _seed_pending_with_active_counter(repo, as_of=now)
    await repo.atomic_timeout_review_with_audit_and_counter(
        review_public_id=review_pid,
        audit_event=_audit_event_for(
            review_pid=review_pid,
            event_type="timeout_marked",
            new_status="timeout",
            actor_delegate_public_id=None,
            payload={"trigger": "first"},
            occurred_at=now,
        ),
        now=now,
    )
    second = await repo.atomic_timeout_review_with_audit_and_counter(
        review_public_id=review_pid,
        audit_event=_audit_event_for(
            review_pid=review_pid,
            event_type="timeout_marked",
            new_status="timeout",
            actor_delegate_public_id=None,
            payload={"trigger": "second"},
            occurred_at=now,
        ),
        now=now,
    )
    assert second is None
    audit_rows = await _fetch_audit_event_payloads(repo, review_pid=review_pid)
    assert len(audit_rows) == 1
    assert audit_rows[0]["payload"]["trigger"] == "first"


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_combined_supersede_with_audit_and_counter_atomic_happy_path(
    tmp_path: Path,
) -> None:
    """Combined supersede + audit + counter atomically transitions."""
    repo = await _build_repo(tmp_path, "combined_supersede.db")
    now = datetime.now(UTC)
    review_pid, delegate_pid = await _seed_pending_with_active_counter(repo, as_of=now)
    audit = _audit_event_for(
        review_pid=review_pid,
        event_type="superseded",
        new_status="superseded",
        actor_delegate_public_id=None,
        payload={"reason": "strategy abandoned"},
        occurred_at=now,
    )
    result = await repo.atomic_supersede_review_with_audit_and_counter(
        review_public_id=review_pid,
        audit_event=audit,
        now=now,
    )
    assert result is not None
    assert result["selected_delegate_public_id"] == delegate_pid
    row = await repo.get_ai_review(review_pid)
    assert row is not None
    assert row["status"] == "superseded"
    assert row["resolution_mode"] == "superseded_by_strategy"
    audit_rows = await _fetch_audit_event_payloads(repo, review_pid=review_pid)
    assert len(audit_rows) == 1
    assert audit_rows[0]["payload"]["reason"] == "strategy abandoned"
    counter, _ = await _fetch_delegate_counter(repo, delegate_pid=delegate_pid)
    assert counter == 0


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_combined_supersede_returns_none_when_already_terminal(tmp_path: Path) -> None:
    """Combined supersede no-ops on already-terminal rows."""
    repo = await _build_repo(tmp_path, "combined_supersede_terminal.db")
    now = datetime.now(UTC)
    review_pid, _delegate_pid = await _seed_pending_with_active_counter(repo, as_of=now)
    await repo.atomic_supersede_review_with_audit_and_counter(
        review_public_id=review_pid,
        audit_event=_audit_event_for(
            review_pid=review_pid,
            event_type="superseded",
            new_status="superseded",
            actor_delegate_public_id=None,
            payload={"reason": "first"},
            occurred_at=now,
        ),
        now=now,
    )
    second = await repo.atomic_supersede_review_with_audit_and_counter(
        review_public_id=review_pid,
        audit_event=_audit_event_for(
            review_pid=review_pid,
            event_type="superseded",
            new_status="superseded",
            actor_delegate_public_id=None,
            payload={"reason": "second"},
            occurred_at=now,
        ),
        now=now,
    )
    assert second is None
    audit_rows = await _fetch_audit_event_payloads(repo, review_pid=review_pid)
    assert len(audit_rows) == 1


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_combined_resolve_audit_uses_actual_previous_status_not_caller_sentinel(
    tmp_path: Path,
) -> None:
    """Primitive overrides caller-supplied previous_status.

    The audit-event row's ``previous_status`` field MUST come from the
    SELECT-FOR-UPDATE inside the primitive's transaction, not from the
    caller's ``audit_event["previous_status"]`` value (which could be
    stale if a peer transitioned mid-flight). This test passes a
    deliberately-wrong sentinel ('fanout_dispatched') for a row that's
    actually in 'pending' and verifies the audit row records 'pending'.
    """
    repo = await _build_repo(tmp_path, "combined_resolve_audit_truth.db")
    now = datetime.now(UTC)
    review_pid, delegate_pid = await _seed_pending_with_active_counter(repo, as_of=now)
    await repo.atomic_resolve_review_with_audit_and_counter(
        review_public_id=review_pid,
        decision="approve",
        responding_delegate_public_id=delegate_pid,
        rationale=None,
        new_status="resolved_approved",
        audit_event=_audit_event_for(
            review_pid=review_pid,
            event_type="decision_recorded",
            new_status="resolved_approved",
            actor_delegate_public_id=delegate_pid,
            payload={"decision": "approve"},
            occurred_at=now,
            previous_status="fanout_dispatched",
        ),
        now=now,
    )
    audit_rows = await _fetch_audit_event_payloads(repo, review_pid=review_pid)
    assert len(audit_rows) == 1
    assert audit_rows[0]["previous_status"] == "pending", (
        "primitive must capture previous_status from SELECT-FOR-UPDATE "
        "rather than trust the caller-supplied audit_event['previous_status'] sentinel"
    )


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_combined_dispatch_fanout_with_audit_atomic_happy_path(tmp_path: Path) -> None:
    """Combined dispatch fanout + audit happens in ONE transaction.

    Given a pending review past fanout_after,
    When :meth:`atomic_dispatch_fanout_with_audit` wins the CAS,
    Then the row flips to ``fanout_dispatched`` with version+1, AND the
    audit-event row is appended with ``payload.dispatch_version`` matching
    the post-UPDATE version (NOT whatever the caller put in the payload
    field).
    """
    repo = await _build_repo(tmp_path, "combined_fanout.db")
    now = datetime.now(UTC)
    review_pid, _delegate_pid = await _seed_pending_for_atomic(repo, as_of=now)
    audit = _audit_event_for(
        review_pid=review_pid,
        event_type="fanout_dispatched",
        new_status="fanout_dispatched",
        actor_delegate_public_id=None,
        payload={"trigger": "test", "dispatch_version": 99},
        occurred_at=now,
    )
    new_version = await repo.atomic_dispatch_fanout_with_audit(
        review_public_id=review_pid,
        audit_event=audit,
        now=now,
    )
    assert new_version == 1
    row = await repo.get_ai_review(review_pid)
    assert row is not None
    assert row["status"] == "fanout_dispatched"
    audit_rows = await _fetch_audit_event_payloads(repo, review_pid=review_pid)
    assert len(audit_rows) == 1
    assert audit_rows[0]["payload"]["dispatch_version"] == 1, (
        "primitive must overwrite caller's payload.dispatch_version with "
        "the actual post-UPDATE incremented version"
    )
    assert audit_rows[0]["payload"]["trigger"] == "test"


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_combined_dispatch_fanout_returns_none_for_non_pending_row(
    tmp_path: Path,
) -> None:
    """Combined dispatch_fanout returns None when row is not pending (peer race)."""
    repo = await _build_repo(tmp_path, "combined_fanout_race.db")
    now = datetime.now(UTC)
    review_pid, delegate_pid = await _seed_pending_with_active_counter(repo, as_of=now)
    await repo.atomic_resolve_review_with_audit_and_counter(
        review_public_id=review_pid,
        decision="approve",
        responding_delegate_public_id=delegate_pid,
        rationale=None,
        new_status="resolved_approved",
        audit_event=_audit_event_for(
            review_pid=review_pid,
            event_type="decision_recorded",
            new_status="resolved_approved",
            actor_delegate_public_id=delegate_pid,
            payload={"decision": "approve"},
            occurred_at=now,
        ),
        now=now,
    )
    audit = _audit_event_for(
        review_pid=review_pid,
        event_type="fanout_dispatched",
        new_status="fanout_dispatched",
        actor_delegate_public_id=None,
        payload={"trigger": "test", "dispatch_version": 0},
        occurred_at=now,
    )
    new_version = await repo.atomic_dispatch_fanout_with_audit(
        review_public_id=review_pid,
        audit_event=audit,
        now=now,
    )
    assert new_version is None
    audit_rows = await _fetch_audit_event_payloads(repo, review_pid=review_pid)
    assert len(audit_rows) == 1
    assert audit_rows[0]["event_type"] == "decision_recorded"


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_combined_resolve_with_for_update_falls_back_on_not_implemented(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SQLite without ``SELECT FOR UPDATE`` falls back to plain SELECT.

    The combined primitives share ``_select_for_update_pre_state``
    which catches NotImplementedError on engines without row-level
    locking and falls back to a plain SELECT (the connection-level
    write lock provides equivalent serialisation on SQLite).

    Given a session executor that raises NotImplementedError on the
    with_for_update path,
    When the combined resolve primitive runs,
    Then it retries without the lock and still wins the transition.
    """
    repo = await _build_repo(tmp_path, "combined_for_update_fallback.db")
    now = datetime.now(UTC)
    review_pid, delegate_pid = await _seed_pending_with_active_counter(repo, as_of=now)
    original_with_for_update = Select.with_for_update

    def _raise_not_implemented(self: Select) -> Select:
        raise NotImplementedError("test fallback")

    monkeypatch.setattr(Select, "with_for_update", _raise_not_implemented)
    try:
        result = await repo.atomic_resolve_review_with_audit_and_counter(
            review_public_id=review_pid,
            decision="approve",
            responding_delegate_public_id=delegate_pid,
            rationale=None,
            new_status="resolved_approved",
            audit_event=_audit_event_for(
                review_pid=review_pid,
                event_type="decision_recorded",
                new_status="resolved_approved",
                actor_delegate_public_id=delegate_pid,
                payload={"decision": "approve"},
                occurred_at=now,
            ),
            now=now,
        )
    finally:
        monkeypatch.setattr(Select, "with_for_update", original_with_for_update)
    assert result is not None
    assert result["previous_status"] == "pending"


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_combined_decrement_in_session_no_op_when_counter_already_claimed(
    tmp_path: Path,
) -> None:
    """Counter-claim CAS short-circuits when ``counter_decremented_at`` is already set.

    The helper :meth:`_decrement_delegate_counter_in_session` uses a
    CAS UPDATE with predicate ``counter_decremented_at IS NULL``.
    A peer that already claimed the slot leaves the predicate False, so
    the second caller's UPDATE rowcount is 0 and the helper returns
    False without touching the delegate counter (preventing a
    double-decrement).

    Given a review row whose counter_decremented_at is pre-populated,
    When the combined supersede primitive tries to claim the slot,
    Then the supersede transition still wins (status is set), the
    audit event lands, but the delegate counter is NOT decremented
    (since the slot was already claimed before this call).
    """
    repo = await _build_repo(tmp_path, "combined_decrement_already_claimed.db")
    now = datetime.now(UTC)
    review_pid, delegate_pid = await _seed_pending_with_active_counter(repo, as_of=now)
    async with repo.session() as s:
        await s.execute(
            __import__("sqlalchemy")
            .update(AiReview)
            .where(AiReview.public_id == review_pid)
            .values(counter_decremented_at=now, updated_at=now)
        )
        await s.commit()
    result = await repo.atomic_supersede_review_with_audit_and_counter(
        review_public_id=review_pid,
        audit_event=_audit_event_for(
            review_pid=review_pid,
            event_type="superseded",
            new_status="superseded",
            actor_delegate_public_id=None,
            payload={"reason": "test"},
            occurred_at=now,
        ),
        now=now,
    )
    assert result is not None
    counter, _ = await _fetch_delegate_counter(repo, delegate_pid=delegate_pid)
    assert counter == 1, (
        "delegate counter must NOT be decremented again when the per-review "
        "counter_decremented_at slot was already claimed by a peer"
    )


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_combined_resolve_rowcount_zero_rolls_back_and_returns_none(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Combined resolve rolls back + returns None when UPDATE rowcount is 0.

    Covers the rollback branch where the SELECT pre-state passed all
    gates (status pending, deadline > now) but the subsequent UPDATE
    found zero matching rows (peer transitioned the row in the
    SELECT-then-UPDATE gap on engines without row locks).

    Given a stub that forces every UPDATE to report rowcount=0,
    When the combined resolve primitive runs,
    Then it rolls back, returns None, and writes neither the audit row
    nor the counter decrement.
    """
    repo = await _build_repo(tmp_path, "combined_resolve_rowcount0.db")
    now = datetime.now(UTC)
    review_pid, delegate_pid = await _seed_pending_with_active_counter(repo, as_of=now)
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
        result = await repo.atomic_resolve_review_with_audit_and_counter(
            review_public_id=review_pid,
            decision="approve",
            responding_delegate_public_id=delegate_pid,
            rationale=None,
            new_status="resolved_approved",
            audit_event=_audit_event_for(
                review_pid=review_pid,
                event_type="decision_recorded",
                new_status="resolved_approved",
                actor_delegate_public_id=delegate_pid,
                payload={"decision": "approve"},
                occurred_at=now,
            ),
            now=now,
        )
    finally:
        monkeypatch.setattr(AsyncSession, "execute", original_execute)
    assert result is None
    audit_rows = await _fetch_audit_event_payloads(repo, review_pid=review_pid)
    assert len(audit_rows) == 0
    counter, decremented = await _fetch_delegate_counter(repo, delegate_pid=delegate_pid)
    assert counter == 1
    assert decremented is None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_combined_timeout_rowcount_zero_rolls_back_and_returns_none(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Combined timeout rolls back + returns None when UPDATE rowcount is 0."""
    repo = await _build_repo(tmp_path, "combined_timeout_rowcount0.db")
    now = datetime.now(UTC)
    review_pid, delegate_pid = await _seed_pending_with_active_counter(repo, as_of=now)
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
        result = await repo.atomic_timeout_review_with_audit_and_counter(
            review_public_id=review_pid,
            audit_event=_audit_event_for(
                review_pid=review_pid,
                event_type="timeout_marked",
                new_status="timeout",
                actor_delegate_public_id=None,
                payload={"trigger": "test"},
                occurred_at=now,
            ),
            now=now,
        )
    finally:
        monkeypatch.setattr(AsyncSession, "execute", original_execute)
    assert result is None
    counter, decremented = await _fetch_delegate_counter(repo, delegate_pid=delegate_pid)
    assert counter == 1
    assert decremented is None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_combined_supersede_rowcount_zero_rolls_back_and_returns_none(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Combined supersede rolls back + returns None when UPDATE rowcount is 0."""
    repo = await _build_repo(tmp_path, "combined_supersede_rowcount0.db")
    now = datetime.now(UTC)
    review_pid, delegate_pid = await _seed_pending_with_active_counter(repo, as_of=now)
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
        result = await repo.atomic_supersede_review_with_audit_and_counter(
            review_public_id=review_pid,
            audit_event=_audit_event_for(
                review_pid=review_pid,
                event_type="superseded",
                new_status="superseded",
                actor_delegate_public_id=None,
                payload={"reason": "test"},
                occurred_at=now,
            ),
            now=now,
        )
    finally:
        monkeypatch.setattr(AsyncSession, "execute", original_execute)
    assert result is None
    counter, decremented = await _fetch_delegate_counter(repo, delegate_pid=delegate_pid)
    assert counter == 1
    assert decremented is None


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_has_live_ai_delegate_uses_strict_admission_boundary(
    tmp_path: Path,
) -> None:
    """Global liveness matches the admission query's strict boundary.

    Given: An empty repository followed by one fully eligible delegate,
    When: Its heartbeat moves from now to exactly 15 seconds old,
    Then: Liveness changes from false to true and back to false.
    """
    repo = await _build_repo(tmp_path, "ai_watchdog_liveness.db")
    as_of = _now()
    assert not await repo.has_live_ai_delegate(
        heartbeat_window_seconds=15,
        as_of=as_of,
    )
    ids = await _seed_user_operator_wallet_instrument(repo, as_of=as_of)
    await _seed_user_row(
        repo,
        user_public_id=ids["user_public_id"],
        role="ai_delegate",
        as_of=as_of,
    )
    await _add_membership(
        repo,
        user_public_id=ids["user_public_id"],
        operator_public_id=ids["operator_public_id"],
        as_of=as_of,
    )
    ids["delegate_public_id"] = await _seed_live_delegate(
        repo,
        user_public_id=ids["user_public_id"],
        last_seen_at=as_of,
        creation_time=as_of,
    )
    assert not await repo.has_live_ai_delegate(
        heartbeat_window_seconds=15,
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
    assert await repo.has_live_ai_delegate(
        heartbeat_window_seconds=15,
        as_of=as_of,
    )

    async with repo.session() as session:
        await session.execute(
            sqlalchemy_update(AiDelegate)
            .where(AiDelegate.public_id == ids["delegate_public_id"])
            .values(last_seen_at=as_of - timedelta(seconds=15))
        )
        await session.commit()

    assert not await repo.has_live_ai_delegate(
        heartbeat_window_seconds=15,
        as_of=as_of,
    )


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_has_live_ai_delegate_recognizes_ai_reviewer(
    tmp_path: Path,
) -> None:
    """Global reviewer liveness includes the shared AI_REVIEWER lifecycle.

    Given: A scoped AI_REVIEWER with membership and a fresh delegate heartbeat.
    When: The repository checks for a globally live AI review principal.
    Then: The liveness query returns true for the reviewer.
    """
    repo = await _build_repo(tmp_path, "ai_reviewer_watchdog_liveness.db")
    as_of = _now()
    await _seed_full_eligible_setup(
        repo,
        as_of=as_of,
        role=UserRole.AI_REVIEWER,
    )

    assert await repo.has_live_ai_delegate(
        heartbeat_window_seconds=15,
        as_of=as_of,
    )


@pytest.mark.asyncio
@pytest.mark.timeout(TEST_TIMEOUT)
async def test_count_ai_review_timeouts_uses_closed_resolution_window(
    tmp_path: Path,
) -> None:
    """Timeout counting keys off the atomic resolution timestamp.

    Given: A pending review and an initially empty resolution window,
    When: The review times out exactly on both inclusive boundaries,
    Then: It counts once in that window and not outside either bound.
    """
    repo = await _build_repo(tmp_path, "ai_watchdog_timeouts.db")
    as_of = _now()
    review_public_id, _delegate_public_id = await _seed_pending_with_active_counter(
        repo,
        as_of=as_of,
    )
    assert (
        await repo.count_ai_review_timeouts_since(
            since=as_of - timedelta(hours=1),
            as_of=as_of,
        )
        == 0
    )
    result = await repo.atomic_timeout_review_with_audit_and_counter(
        review_public_id=review_public_id,
        audit_event=_audit_event_for(
            review_pid=review_public_id,
            event_type="timeout_marked",
            new_status="timeout",
            actor_delegate_public_id=None,
            payload={"trigger": "watchdog_test"},
            occurred_at=as_of,
        ),
        now=as_of,
    )
    assert result is not None
    assert await repo.count_ai_review_timeouts_since(since=as_of, as_of=as_of) == 1
    approved_review_public_id, approved_delegate_public_id = (
        await _seed_pending_with_active_counter(
            repo,
            as_of=as_of,
        )
    )
    approved = await repo.atomic_resolve_review_with_audit_and_counter(
        review_public_id=approved_review_public_id,
        decision="approve",
        responding_delegate_public_id=approved_delegate_public_id,
        rationale=None,
        new_status="resolved_approved",
        audit_event=_audit_event_for(
            review_pid=approved_review_public_id,
            event_type="decision_recorded",
            new_status="resolved_approved",
            actor_delegate_public_id=approved_delegate_public_id,
            payload={"decision": "approve"},
            occurred_at=as_of,
        ),
        now=as_of,
    )
    assert approved is not None
    assert await repo.count_ai_review_timeouts_since(since=as_of, as_of=as_of) == 1
    assert (
        await repo.count_ai_review_timeouts_since(
            since=as_of + timedelta(microseconds=1),
            as_of=as_of + timedelta(seconds=1),
        )
        == 0
    )
    assert (
        await repo.count_ai_review_timeouts_since(
            since=as_of - timedelta(seconds=1),
            as_of=as_of - timedelta(microseconds=1),
        )
        == 0
    )
