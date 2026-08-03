"""Tests for AI-research rounds, immutable views, and replay eligibility."""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import PropertyMock
from unittest.mock import patch
from uuid import UUID
from uuid import uuid7

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.models import AiResearchRound
from snapper.data.models import MarketView
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import AiResearchRoundInsertRow
from snapper.data.repository_types import MarketViewInsertRow
from snapper.data.repository_types import MarketViewSourceInsertRow

_T0 = datetime(2026, 7, 21, 8, 0, tzinfo=UTC)


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create and dispose one isolated SQLite repository."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'ai-research.db'}")
    await repo.create_all()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


def _round_row(trigger: str, created_at: datetime) -> AiResearchRoundInsertRow:
    """Build one server-owned round insert row."""
    return {
        "public_id": str(uuid7()),
        "trigger": trigger,
        "created_at": created_at,
    }


def _view_row(
    round_public_id: str,
    *,
    as_of: datetime,
    valid_until: datetime,
    rationale: str = "Balanced conditions.",
) -> MarketViewInsertRow:
    """Build one valid market-view insert row."""
    return {
        "research_round_public_id": round_public_id,
        "as_of": as_of,
        "valid_until": valid_until,
        "regime": "neutral",
        "bias": "longs_ok",
        "confidence": 0.75,
        "horizon_hours": 2,
        "key_risks": ["Inflation surprise", "Liquidity deterioration"],
        "next_events": [
            {
                "when_utc": as_of + timedelta(hours=1),
                "name": "CPI release",
                "severity": "high",
            }
        ],
        "rationale": rationale,
    }


def _sources(retrieved_at: datetime) -> list[MarketViewSourceInsertRow]:
    """Build two ordered source citations."""
    return [
        {
            "url": "https://example.com/primary",
            "title": "Primary release",
            "retrieved_at": retrieved_at,
        },
        {
            "url": "https://example.com/context",
            "title": "Market context",
            "retrieved_at": retrieved_at + timedelta(minutes=1),
        },
    ]


@pytest.mark.asyncio
async def test_unknown_ai_research_records_return_none(
    repository: SQLAlchemyRepository,
) -> None:
    """Unknown round, view, latest view, and source reads are empty."""
    assert await repository.get_ai_research_round(str(uuid7())) is None
    assert await repository.get_market_view(str(uuid7())) is None
    assert await repository.get_market_view_source(str(uuid7())) is None
    assert await repository.get_latest_market_view(replay_at=_T0) is None


@pytest.mark.asyncio
async def test_new_round_atomically_supersedes_pending_and_cas_is_final(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Latest-wins creation leaves one pending row and terminal rows stay final."""
    transaction_starts = 0
    original_begin = repository._begin_ai_research_round_create_transaction

    async def record_transaction_start(session: AsyncSession) -> None:
        """Record and delegate each pending-slot transaction opener."""
        nonlocal transaction_starts
        transaction_starts += 1
        await original_begin(session)

    monkeypatch.setattr(
        repository,
        "_begin_ai_research_round_create_transaction",
        record_transaction_start,
    )
    first_id = await repository.create_ai_research_round(_round_row("periodic", _T0))
    second_at = _T0 + timedelta(minutes=5)
    second_id = await repository.create_ai_research_round(_round_row("market_move", second_at))

    first = await repository.get_ai_research_round(first_id)
    second = await repository.get_ai_research_round(second_id)
    assert first is not None
    assert first["status"] == "superseded"
    assert first["resolved_at"] == second_at
    assert second is not None
    assert second["status"] == "pending"
    assert second["resolved_at"] is None

    async with repository.session() as session:
        pending_count = await session.scalar(
            select(func.count(AiResearchRound.id)).where(AiResearchRound.status == "pending")
        )
    assert pending_count == 1

    expired_at = second_at + timedelta(minutes=5)
    assert await repository.transition_ai_research_round_status(
        second_id,
        expected_status="pending",
        new_status="expired",
        transitioned_at=expired_at,
    )
    assert not await repository.transition_ai_research_round_status(
        second_id,
        expected_status="pending",
        new_status="superseded",
        transitioned_at=expired_at + timedelta(seconds=1),
    )
    expired = await repository.get_ai_research_round(second_id)
    assert expired is not None
    assert expired["status"] == "expired"
    assert expired["resolved_at"] == expired_at
    assert transaction_starts == 2


@pytest.mark.asyncio
async def test_postgresql_round_creation_lock_uses_fresh_snapshot_and_advisory_lock(
    repository: SQLAlchemyRepository,
) -> None:
    """PostgreSQL serializes the global slot before its replacement query."""
    mock_session = AsyncMock(spec=AsyncSession)
    with patch.object(
        type(repository),
        "dialect_name",
        new_callable=PropertyMock,
        return_value="postgresql",
    ):
        await repository._begin_ai_research_round_create_transaction(
            cast(AsyncSession, mock_session)
        )

    statements = [str(call.args[0]) for call in mock_session.execute.await_args_list]
    assert statements == [
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "SELECT pg_advisory_xact_lock(hashtext('ai_research'), hashtext('pending_round'))",
    ]


@pytest.mark.asyncio
async def test_round_creation_lock_rejects_an_unknown_dialect(
    repository: SQLAlchemyRepository,
) -> None:
    """Unsupported databases cannot create an unprotected pending slot."""
    mock_session = AsyncMock(spec=AsyncSession)
    with (
        patch.object(
            type(repository),
            "dialect_name",
            new_callable=PropertyMock,
            return_value="mysql",
        ),
        pytest.raises(NotImplementedError, match="mysql"),
    ):
        await repository._begin_ai_research_round_create_transaction(
            cast(AsyncSession, mock_session)
        )
    mock_session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_round_creation_generates_uuid_when_server_does_not_preallocate_one(
    repository: SQLAlchemyRepository,
) -> None:
    """Repository creation supplies the optional server public identifier."""
    round_id = await repository.create_ai_research_round({"trigger": "periodic", "created_at": _T0})
    assert UUID(round_id).version == 7


@pytest.mark.asyncio
async def test_round_transition_rejects_non_lifecycle_requests(
    repository: SQLAlchemyRepository,
) -> None:
    """Only expiry and supersession may bypass artifact insertion."""
    with pytest.raises(ValueError, match="only from pending"):
        await repository.transition_ai_research_round_status(
            str(uuid7()),
            expected_status="completed",
            new_status="expired",
            transitioned_at=_T0,
        )
    round_id = await repository.create_ai_research_round(_round_row("periodic", _T0))
    for new_status in ("pending", "completed"):
        with pytest.raises(ValueError, match="must be superseded or expired"):
            await repository.transition_ai_research_round_status(
                round_id,
                expected_status="pending",
                new_status=new_status,
                transitioned_at=_T0,
            )
    pending = await repository.get_ai_research_round(round_id)
    assert pending is not None
    assert pending["status"] == "pending"


@pytest.mark.asyncio
async def test_partial_unique_index_rejects_a_second_pending_round(
    repository: SQLAlchemyRepository,
) -> None:
    """The ORM schema physically enforces the single pending round invariant."""
    await repository.create_ai_research_round(_round_row("periodic", _T0))
    async with repository.session() as session:
        session.add(
            AiResearchRound(
                public_id=str(uuid7()),
                trigger="market_move",
                status="pending",
                created_at=_T0 + timedelta(seconds=1),
                updated_at=_T0 + timedelta(seconds=1),
                resolved_at=None,
            )
        )
        with pytest.raises(IntegrityError):
            await session.commit()


@pytest.mark.asyncio
async def test_market_view_and_sources_round_trip_and_complete_round(
    repository: SQLAlchemyRepository,
) -> None:
    """One transaction persists every artifact field, citation, and completion."""
    round_id = await repository.create_ai_research_round(_round_row("periodic", _T0))
    authored_at = _T0 + timedelta(minutes=1)
    submitted_at = _T0 + timedelta(minutes=2)
    source_rows = _sources(_T0)
    view_id = await repository.insert_market_view(
        _view_row(
            round_id,
            as_of=authored_at,
            valid_until=_T0 + timedelta(hours=3),
        ),
        source_rows,
        submitted_at=submitted_at,
    )

    view = await repository.get_market_view(view_id)
    assert view is not None
    assert UUID(view["public_id"]).version == 7
    assert view["research_round_public_id"] == round_id
    assert view["trigger"] == "periodic"
    assert view["status"] == "completed"
    assert view["as_of"] == authored_at
    assert view["submitted_at"] == submitted_at
    assert view["valid_until"] == _T0 + timedelta(hours=3)
    assert view["regime"] == "neutral"
    assert view["bias"] == "longs_ok"
    assert view["confidence"] == 0.75
    assert view["horizon_hours"] == 2
    assert view["key_risks"] == ["Inflation surprise", "Liquidity deterioration"]
    assert view["next_events"] == [
        {
            "when_utc": authored_at + timedelta(hours=1),
            "name": "CPI release",
            "severity": "high",
        }
    ]
    assert view["rationale"] == "Balanced conditions."
    assert [source["ordinal"] for source in view["sources"]] == [0, 1]
    assert [source["url"] for source in view["sources"]] == [
        "https://example.com/primary",
        "https://example.com/context",
    ]
    assert all(UUID(source["public_id"]).version == 7 for source in view["sources"])

    first_source = await repository.get_market_view_source(view["sources"][0]["public_id"])
    assert first_source == view["sources"][0]
    completed_round = await repository.get_ai_research_round(round_id)
    assert completed_round is not None
    assert completed_round["status"] == "completed"
    assert completed_round["resolved_at"] == submitted_at


@pytest.mark.asyncio
async def test_market_view_insert_rejects_missing_sources_and_oversize_rationale(
    repository: SQLAlchemyRepository,
) -> None:
    """Repository invariants reject incomplete artifacts before persistence."""
    round_id = await repository.create_ai_research_round(_round_row("periodic", _T0))
    valid_until = _T0 + timedelta(hours=2)
    with pytest.raises(ValueError, match="at least one source"):
        await repository.insert_market_view(
            _view_row(round_id, as_of=_T0, valid_until=valid_until),
            [],
            submitted_at=_T0 + timedelta(minutes=1),
        )
    with pytest.raises(ValueError, match="2048 UTF-8 bytes"):
        await repository.insert_market_view(
            _view_row(
                round_id,
                as_of=_T0,
                valid_until=valid_until,
                rationale="ą" * 1024 + "x",
            ),
            _sources(_T0),
            submitted_at=_T0 + timedelta(minutes=1),
        )
    pending = await repository.get_ai_research_round(round_id)
    assert pending is not None
    assert pending["status"] == "pending"
    async with repository.session() as session:
        view_count = await session.scalar(select(func.count(MarketView.id)))
    assert view_count == 0


@pytest.mark.asyncio
async def test_market_view_insert_requires_a_pending_round(
    repository: SQLAlchemyRepository,
) -> None:
    """Unknown and terminal rounds cannot acquire an artifact."""
    missing_round_id = str(uuid7())
    with pytest.raises(ValueError, match="pending AI-research round"):
        await repository.insert_market_view(
            _view_row(
                missing_round_id,
                as_of=_T0,
                valid_until=_T0 + timedelta(hours=1),
            ),
            _sources(_T0),
            submitted_at=_T0,
        )

    round_id = await repository.create_ai_research_round(_round_row("periodic", _T0))
    assert await repository.transition_ai_research_round_status(
        round_id,
        expected_status="pending",
        new_status="expired",
        transitioned_at=_T0 + timedelta(minutes=1),
    )
    with pytest.raises(ValueError, match="pending AI-research round"):
        await repository.insert_market_view(
            _view_row(round_id, as_of=_T0, valid_until=_T0 + timedelta(hours=1)),
            _sources(_T0),
            submitted_at=_T0 + timedelta(minutes=2),
        )


@pytest.mark.asyncio
async def test_replay_excludes_late_submission_even_with_earlier_as_of(
    repository: SQLAlchemyRepository,
) -> None:
    """A backdated view remains invisible until its server submission time."""
    replay_at = _T0 + timedelta(hours=4)
    older_round = await repository.create_ai_research_round(_round_row("periodic", _T0))
    older_view_id = await repository.insert_market_view(
        _view_row(
            older_round,
            as_of=replay_at - timedelta(hours=2),
            valid_until=replay_at + timedelta(hours=2),
        ),
        _sources(replay_at - timedelta(hours=3)),
        submitted_at=replay_at - timedelta(minutes=30),
    )

    late_round = await repository.create_ai_research_round(
        _round_row("market_move", replay_at - timedelta(minutes=10))
    )
    late_view_id = await repository.insert_market_view(
        _view_row(
            late_round,
            as_of=replay_at - timedelta(hours=1),
            valid_until=replay_at + timedelta(hours=2),
        ),
        _sources(replay_at - timedelta(hours=1)),
        submitted_at=replay_at + timedelta(minutes=1),
    )

    visible_at_replay = await repository.get_latest_market_view(replay_at=replay_at)
    assert visible_at_replay is not None
    assert visible_at_replay["public_id"] == older_view_id
    assert visible_at_replay["as_of"] < replay_at

    visible_after_submission = await repository.get_latest_market_view(
        replay_at=replay_at + timedelta(minutes=1)
    )
    assert visible_after_submission is not None
    assert visible_after_submission["public_id"] == late_view_id


@pytest.mark.asyncio
async def test_replay_excludes_future_authored_and_expired_views(
    repository: SQLAlchemyRepository,
) -> None:
    """Both author time and validity independently participate in eligibility."""
    replay_at = _T0 + timedelta(hours=4)
    future_round = await repository.create_ai_research_round(_round_row("periodic", _T0))
    await repository.insert_market_view(
        _view_row(
            future_round,
            as_of=replay_at + timedelta(minutes=1),
            valid_until=replay_at + timedelta(hours=2),
        ),
        _sources(_T0),
        submitted_at=replay_at - timedelta(minutes=1),
    )
    assert await repository.get_latest_market_view(replay_at=replay_at) is None

    expired_round = await repository.create_ai_research_round(
        _round_row("periodic", _T0 + timedelta(minutes=1))
    )
    await repository.insert_market_view(
        _view_row(
            expired_round,
            as_of=replay_at - timedelta(hours=1),
            valid_until=replay_at,
        ),
        _sources(_T0),
        submitted_at=replay_at - timedelta(minutes=30),
    )
    assert await repository.get_latest_market_view(replay_at=replay_at) is None
