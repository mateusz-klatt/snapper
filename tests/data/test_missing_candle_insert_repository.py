"""Protect missing-only native candle insertion and historical knowledge cuts."""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from datetime import tzinfo
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import CandleUpsertRow

_OPEN = datetime(2026, 1, 10, 12, tzinfo=UTC)
_ACQUIRED = datetime(2026, 7, 1, tzinfo=UTC)
_INSTRUMENT = "00000000-0000-4000-8000-000000000101"
_SESSION = "00000000-0000-4000-8000-000000000102"


def _row(offset: int = 0) -> CandleUpsertRow:
    """Return a complete authoritative native minute with acquisition provenance."""
    return {
        "instrument_public_id": _INSTRUMENT,
        "open_at": _OPEN + timedelta(minutes=offset),
        "timestamp": _ACQUIRED,
        "timeframe": "1m",
        "open": 10.0,
        "high": 12.0,
        "low": 9.0,
        "close": 11.0,
        "volume": 3.0,
        "vwap": 10.75,
        "trades": 7,
        "source": "native",
        "complete": True,
        "price_basis": "last",
        "session_id": _SESSION,
        "sequence_id": offset + 1,
    }


@pytest_asyncio.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Provide a disposable file-backed repository for real transaction tests."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'missing-candles.db'}")
    await repo.create_all()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


async def _stored(repo: SQLAlchemyRepository) -> list[tuple[object, ...]]:
    """Snapshot every candle column so existing history cannot change unnoticed."""
    async with repo.session() as session:
        rows = (
            (await session.execute(select(Candle).order_by(Candle.open_at, Candle.timestamp)))
            .scalars()
            .all()
        )
        return [
            tuple(getattr(row, column.name) for column in Candle.__table__.columns) for row in rows
        ]


@pytest.mark.asyncio
async def test_insert_retains_full_fields_without_mutating_input_and_uses_knowledge_time(
    repository: SQLAlchemyRepository,
) -> None:
    """A missing closed minute appears only after acquisition and retains native fields."""
    row = _row()
    original = dict(row)

    inserted = await repository.insert_missing_candles([row])

    assert row == original
    assert len(inserted) == 1
    assert inserted[0] == row | {"public_id": inserted[0]["public_id"], "known_to": KNOWN_TO_MAX}
    async with repository.session() as session:
        before = await session.execute(
            select(Candle).where(Candle.timestamp <= _ACQUIRED - timedelta(microseconds=1))
        )
        assert before.scalars().all() == []
        stored = (await session.execute(select(Candle))).scalars().one()
        assert all(getattr(stored, field) == value for field, value in inserted[0].items())


@pytest.mark.asyncio
async def test_existing_current_future_authored_and_closed_history_are_all_untouched(
    repository: SQLAlchemyRepository,
) -> None:
    """Every historical natural key blocks catchup irrespective of temporal visibility."""
    async with repository.session() as session:
        for offset in range(3):
            row = _row(offset)
            row["timestamp"] = (
                _ACQUIRED + timedelta(days=1) if offset == 1 else _ACQUIRED - timedelta(days=3)
            )
            row["known_to"] = _ACQUIRED - timedelta(days=2) if offset == 2 else KNOWN_TO_MAX
            row["public_id"] = f"00000000-0000-4000-8000-{offset + 200:012d}"
            session.add(Candle(**row))
        await session.commit()
    before = await _stored(repository)
    attempted = [_row(offset) for offset in range(4)]
    attempted[0]["close"] = 10.5

    inserted = await repository.insert_missing_candles(attempted)

    assert [row["open_at"] for row in inserted] == [_OPEN + timedelta(minutes=3)]
    assert (await _stored(repository))[:3] == before


@pytest.mark.asyncio
async def test_caller_transaction_can_rollback_and_repeated_calls_are_true_noops(
    repository: SQLAlchemyRepository,
) -> None:
    """Caller-owned staging never commits and a committed natural key never versions."""
    async with repository.session() as session:
        assert len(await repository.insert_missing_candles([_row()], session=session)) == 1
        await session.rollback()
    assert await _stored(repository) == []
    assert len(await repository.insert_missing_candles([_row()])) == 1
    before = await _stored(repository)
    assert await repository.insert_missing_candles([_row()]) == []
    assert await _stored(repository) == before


@pytest.mark.asyncio
async def test_concurrent_live_insert_wins_without_catchup_versioning(
    repository: SQLAlchemyRepository,
) -> None:
    """Catchup waits for a real uncommitted writer and skips its committed natural key."""
    async with repository.session() as live:
        live_row = _row()
        live_row["close"] = 10.5
        await repository.upsert_candles([live_row], session=live)
        await live.flush()
        catchup = asyncio.create_task(repository.insert_missing_candles([_row()]))
        await asyncio.sleep(0.05)
        assert not catchup.done()
        await live.commit()
    assert await catchup == []
    async with repository.session() as session:
        stored = (await session.execute(select(Candle))).scalars().all()
        assert len(stored) == 1
        assert stored[0].close == 10.5
        assert stored[0].known_to == KNOWN_TO_MAX


@pytest.mark.asyncio
async def test_integrity_failure_rolls_back_the_whole_owned_batch(
    repository: SQLAlchemyRepository,
) -> None:
    """An unrelated unique-public-id failure cannot become a silently skipped candle."""
    first, second = _row(), _row(1)
    first["public_id"] = "00000000-0000-4000-8000-000000000300"
    second["public_id"] = first["public_id"]
    with pytest.raises(IntegrityError):
        await repository.insert_missing_candles([first, second])
    assert await _stored(repository) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"timeframe": "5m"},
        {"source": "calculated"},
        {"complete": False},
        {"open_at": _OPEN + timedelta(seconds=1)},
        {"open_at": _ACQUIRED},
        {"open_at": _OPEN.replace(tzinfo=None)},
        {"timestamp": _ACQUIRED.replace(tzinfo=None)},
        {"timestamp": datetime.now(UTC) + timedelta(days=1)},
        {"high": 9.5},
        {"low": 11.5},
        {"volume": -1.0},
        {"volume": float("nan")},
        {"volume": True},
        {"vwap": None},
        {"vwap": float("inf")},
        {"trades": None},
        {"trades": -1},
        {"trades": True},
        {"known_to": _ACQUIRED},
        {"instrument_public_id": ""},
        {"session_id": ""},
        {"sequence_id": 0},
        {"sequence_id": True},
    ],
)
async def test_invalid_native_input_rejects_whole_batch_before_any_insert(
    repository: SQLAlchemyRepository, changes: CandleUpsertRow
) -> None:
    """Invalid closure, precision or provenance never permits partial catchup."""
    invalid = _row(1)
    invalid.update(changes)
    with pytest.raises(ValueError):
        await repository.insert_missing_candles([_row(), invalid])
    assert await _stored(repository) == []


class _UnknownOffset(tzinfo):
    """Represent a datetime whose timezone refuses a concrete UTC offset."""

    def utcoffset(self, dt: datetime | None) -> timedelta | None:
        """Return no UTC offset rather than certifying an acquisition instant."""
        del dt
        return None

    def dst(self, dt: datetime | None) -> timedelta | None:
        """Return no daylight-saving adjustment for the unknown offset."""
        del dt
        return None

    def tzname(self, dt: datetime | None) -> str | None:
        """Name the deliberately unresolved timezone used by the refusal test."""
        del dt
        return "unresolved"


class _PostgresDialectRepository(SQLAlchemyRepository):
    """Exercise PostgreSQL command refusal branches without claiming a physical witness."""

    @property
    def dialect_name(self) -> str:
        """Select the PostgreSQL branch for an explicitly mocked session boundary."""
        return "postgresql"


class _UnsupportedDialectRepository(SQLAlchemyRepository):
    """Select an engine the missing-only protocol deliberately refuses."""

    @property
    def dialect_name(self) -> str:
        """Return an unsupported engine without making a network connection."""
        return "unsupported"


@pytest.mark.asyncio
async def test_normalizes_aware_offsets_preserves_nullable_basis_and_explicit_identity(
    repository: SQLAlchemyRepository,
) -> None:
    """Normalization retains explicit public identity and honest absent price-basis metadata."""
    row = _row()
    row["open_at"] = _OPEN.astimezone(timezone(timedelta(hours=2)))
    row["timestamp"] = _ACQUIRED.astimezone(timezone(timedelta(hours=2)))
    row["public_id"] = "00000000-0000-4000-8000-000000000400"
    row["known_to"] = KNOWN_TO_MAX
    del row["price_basis"]
    inserted = await repository.insert_missing_candles([row])
    assert inserted[0]["open_at"] == _OPEN
    assert inserted[0]["timestamp"] == _ACQUIRED
    assert inserted[0]["price_basis"] is None
    assert inserted[0]["public_id"] == row["public_id"]


@pytest.mark.asyncio
async def test_unknown_offset_and_untyped_extra_fields_are_refused(
    repository: SQLAlchemyRepository,
) -> None:
    """Ambiguous timezone and injected physical identifiers never reach the database."""
    row = _row()
    row["timestamp"] = _ACQUIRED.replace(tzinfo=_UnknownOffset())
    with pytest.raises(ValueError):
        await repository.insert_missing_candles([row])
    extra = cast(CandleUpsertRow, dict(_row(), id=1))
    with pytest.raises(ValueError):
        await repository.insert_missing_candles([extra])
    assert await _stored(repository) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("isolation", ["repeatable read", "serializable"])
async def test_postgres_stale_snapshot_is_refused_before_any_mutation(isolation: str) -> None:
    """A stale snapshot cannot authenticate the absence of concurrent committed history."""
    repo = _PostgresDialectRepository("sqlite+aiosqlite:///:memory:")
    session = AsyncMock(spec=AsyncSession)
    result = MagicMock()
    result.scalar_one.return_value = isolation
    session.execute.return_value = result
    try:
        with pytest.raises(ValueError, match="READ COMMITTED"):
            await repo.insert_missing_candles([_row()], session=cast(AsyncSession, session))
        assert session.execute.await_count == 1
        session.commit.assert_not_awaited()
    finally:
        await repo.engine.dispose()


@pytest.mark.asyncio
async def test_postgres_serialization_commands_precede_insert_receipt_without_committing() -> None:
    """The session boundary must establish its isolation and writer fence before an insert."""
    repo = _PostgresDialectRepository("sqlite+aiosqlite:///:memory:")
    session = AsyncMock(spec=AsyncSession)
    isolation = MagicMock()
    isolation.scalar_one.return_value = "read committed"
    inserted = MagicMock()
    inserted.scalar_one_or_none.return_value = "00000000-0000-4000-8000-000000000401"
    session.execute.side_effect = [isolation, MagicMock(), MagicMock(), inserted]
    try:
        assert (
            len(await repo.insert_missing_candles([_row()], session=cast(AsyncSession, session)))
            == 1
        )
        commands = [str(call.args[0]) for call in session.execute.await_args_list]
        assert commands[0] == "SHOW transaction_isolation"
        assert "current_setting('lock_timeout')" in commands[1]
        assert "SHARE ROW EXCLUSIVE" in commands[2]
        assert "INSERT INTO candles" in commands[3] and "NOT (EXISTS" in commands[3]
        assert "UPDATE " not in commands[3]
        session.commit.assert_not_awaited()
    finally:
        await repo.engine.dispose()


@pytest.mark.asyncio
async def test_unsupported_dialect_refuses_before_a_database_statement() -> None:
    """An unknown engine cannot silently select an unproven concurrency protocol."""
    repo = _UnsupportedDialectRepository("sqlite+aiosqlite:///:memory:")
    try:
        with pytest.raises(NotImplementedError):
            await repo.insert_missing_candles([_row()])
    finally:
        await repo.engine.dispose()


@pytest.mark.asyncio
async def test_queued_live_rows_skip_newer_current_authors_without_rewriting_payload() -> None:
    """A fresh active-author projection skips stale queued rows while preserving equal/new keys."""
    session = AsyncMock(spec=AsyncSession)
    result = MagicMock()
    result.all.return_value = [
        (_INSTRUMENT, "1m", _OPEN, _ACQUIRED + timedelta(days=1)),
        (_INSTRUMENT, "1m", _OPEN, _ACQUIRED - timedelta(days=1)),
        (_INSTRUMENT, "1m", _OPEN + timedelta(minutes=1), _ACQUIRED),
    ]
    session.execute.return_value = result
    rows = [_row(offset) for offset in range(3)]
    before = [dict(row) for row in rows]
    kept = await SQLAlchemyRepository._retain_nonstale_live_candle_rows(
        cast(AsyncSession, session), rows
    )
    assert kept == rows[1:]
    assert rows == before
    assert (
        await SQLAlchemyRepository._retain_nonstale_live_candle_rows(
            cast(AsyncSession, session), []
        )
        == []
    )


@pytest.mark.asyncio
async def test_empty_ordinary_live_batch_has_no_write_work(
    repository: SQLAlchemyRepository,
) -> None:
    """Empty ordinary writes retain their original no-op behavior."""
    assert await repository.upsert_candles([]) == 0


@pytest.mark.asyncio
async def test_caller_owned_postgres_live_write_fences_before_both_candle_lookups() -> None:
    """A queued caller-managed writer cannot take a tuple lock before the repair fence clears."""
    repo = _PostgresDialectRepository("sqlite+aiosqlite:///:memory:")
    session = AsyncMock(spec=AsyncSession)
    session.add = MagicMock()
    isolation = MagicMock()
    isolation.scalar_one.return_value = "read committed"
    active = MagicMock()
    active.all.return_value = []
    visible = MagicMock()
    visible.scalars.return_value.all.return_value = []
    session.execute.side_effect = [isolation, MagicMock(), MagicMock(), active, visible]
    try:
        assert await repo.upsert_candles([_row()], session=cast(AsyncSession, session)) == 1
        commands = [str(call.args[0]) for call in session.execute.await_args_list]
        assert "ROW EXCLUSIVE" in commands[2] and "SHARE ROW" not in commands[2]
        assert "FOR UPDATE" not in commands[3]
        assert "FOR UPDATE" in commands[4]
        session.add.assert_called_once()
        session.commit.assert_not_awaited()
    finally:
        await repo.engine.dispose()


@pytest.mark.asyncio
async def test_empty_duplicate_missing_field_and_over_budget_batches(
    repository: SQLAlchemyRepository,
) -> None:
    """No work stays empty and ambiguous or oversized batches fail before writes."""
    assert await repository.insert_missing_candles([]) == []
    with pytest.raises(ValueError):
        await repository.insert_missing_candles([_row(), _row()])
    invalid = _row()
    del invalid["vwap"]
    with pytest.raises(ValueError):
        await repository.insert_missing_candles([invalid])
    with pytest.raises(ValueError):
        await repository.insert_missing_candles([_row(offset) for offset in range(721)])
    assert await _stored(repository) == []
