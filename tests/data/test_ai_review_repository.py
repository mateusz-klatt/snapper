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
from uuid import uuid7

import pytest

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
