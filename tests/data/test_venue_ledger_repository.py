"""Tests for append-only venue ledger persistence."""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.repository import SQLAlchemyRepository
from snapper.data.venue_ledger_store import VenueLedgerCursorWrite
from snapper.data.venue_ledger_store import VenueLedgerInsertRow
from snapper.data.venue_ledger_store import _count_ids
from snapper.data.venue_ledger_store import _load_ids
from snapper.data.venue_ledger_store import ledger_entry_insert

_WALLET = "00000000-0000-7000-8000-000000000101"
_SESSION = "00000000-0000-7000-8000-000000000201"
_OBSERVATION = "00000000-0000-7000-8000-000000000301"
_NOW = datetime(2026, 11, 23, 18, 0, tzinfo=UTC)


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create one migrated-by-metadata repository and dispose it afterward."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'ledger.db'}")
    await repo.create_all()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


def _entry(ledger_id: str, amount: str, observation_id: str = _OBSERVATION) -> VenueLedgerInsertRow:
    """Return one posting insert row."""
    return {
        "wallet_public_id": _WALLET,
        "exchange": "kraken",
        "mode": "live",
        "venue_ledger_id": ledger_id,
        "refid": "R1",
        "ledger_time_text": "1700000000.9",
        "ledger_type": "margin",
        "subtype": "",
        "aclass": "currency",
        "asset": "ZEUR",
        "amount_text": amount,
        "fee_text": "0.0100",
        "balance_text": "8.0000",
        "observed_at": _NOW,
        "observation_id": observation_id,
    }


def _cursor(
    *,
    second: int | None,
    as_of: datetime,
    session_id: str = _SESSION,
    ids: tuple[str, ...] = (),
    status: str | None = None,
) -> VenueLedgerCursorWrite:
    """Return one cursor write."""
    return VenueLedgerCursorWrite(
        wallet_public_id=_WALLET,
        exchange="kraken",
        mode="live",
        cursor_second=second,
        cursor_ids=ids,
        boundary_as_of=as_of,
        boundary_watermark=4,
        boundary_session_id=session_id,
        boundary_sequence_id=7,
        evidence_status=status,
        reported_count=1,
        ingested_count=1,
        pass_started_at=_NOW,
        observed_at=_NOW,
        observation_id=_OBSERVATION,
        trade_balance_json='{"eb":"1"}',
        trade_balance_observed_at=_NOW,
        api_points_used=5,
    )


@pytest.mark.asyncio
async def test_ledger_insert_is_idempotent_and_ordered(repository: SQLAlchemyRepository) -> None:
    """A repeated venue ledger id does not change the stored posting.

    Given two postings and a second pass that repeats the first id with new text,
    When both passes commit,
    Then the first text and observation stay, the new id is appended, and list
        order follows insertion.
    """
    first = await repository.commit_venue_ledger_pass(
        [_entry("L1", "1.2500"), _entry("L2", "2.0000")],
        _cursor(second=100, as_of=_NOW),
    )
    second = await repository.commit_venue_ledger_pass(
        [_entry("L1", "9.0000", observation_id="00000000-0000-7000-8000-000000000399")],
        _cursor(second=110, as_of=_NOW),
    )
    stored = await repository.list_venue_ledger_entries(_WALLET, "kraken", "live")
    assert first == 2
    assert second == 0
    assert [row.venue_ledger_id for row in stored] == ["L1", "L2"]
    assert stored[0].amount_text == "1.2500"
    assert stored[0].ledger_time_text == "1700000000.9"
    assert stored[0].observation_id == _OBSERVATION


@pytest.mark.asyncio
async def test_cursor_boundary_is_written_once(repository: SQLAlchemyRepository) -> None:
    """Later passes keep the reconciliation boundary from the first insert.

    Given a cursor saved at one boundary and a later pass naming another boundary,
    When the later pass commits,
    Then the cursor second moves and the original boundary stays.
    """
    later = _NOW + timedelta(hours=1)
    other_session = "00000000-0000-7000-8000-000000000299"
    await repository.commit_venue_ledger_pass([], _cursor(second=100, as_of=_NOW, ids=("A",)))
    await repository.commit_venue_ledger_pass(
        [],
        _cursor(second=200, as_of=later, session_id=other_session, ids=("B",)),
    )
    stored = await repository.get_venue_ledger_cursor(_WALLET, "kraken", "live")
    assert stored is not None
    assert stored.cursor_second == 200
    assert stored.cursor_ids == ("B",)
    assert stored.boundary_as_of == _NOW
    assert stored.boundary_session_id == _SESSION
    assert stored.boundary_watermark == 4
    assert stored.boundary_sequence_id == 7


@pytest.mark.asyncio
async def test_cursor_is_absent_until_the_first_pass(repository: SQLAlchemyRepository) -> None:
    """No cursor exists before the first ledger pass.

    Given an account with no ledger writes,
    When the cursor and postings are read,
    Then both are empty.
    """
    assert await repository.get_venue_ledger_cursor(_WALLET, "kraken", "live") is None
    assert await repository.list_venue_ledger_entries(_WALLET, "kraken", "live") == []


def test_postgresql_and_sqlite_inserts_ignore_identity_conflicts() -> None:
    """Both dialects compile an idempotent insert on the venue ledger identity.

    Given one posting,
    When the insert is compiled for PostgreSQL and SQLite,
    Then both statements do nothing on the identity conflict.
    """
    row = _entry("L1", "1.2500")
    postgres = str(ledger_entry_insert("postgresql", [row]).compile(dialect=postgresql.dialect()))
    sqlite_sql = str(ledger_entry_insert("sqlite", [row]).compile(dialect=sqlite.dialect()))
    assert "ON CONFLICT" in postgres
    assert "DO NOTHING" in postgres
    assert "ON CONFLICT" in sqlite_sql
    assert "DO NOTHING" in sqlite_sql


def test_malformed_cursor_ids_are_rejected() -> None:
    """Cursor ids must be a JSON array of strings.

    Given an object payload and an array containing a number,
    When the cursor id loader reads each payload,
    Then both raise ValueError.
    """
    with pytest.raises(ValueError, match="malformed"):
        _load_ids("{}")
    with pytest.raises(ValueError, match="malformed"):
        _load_ids("[1]")


@pytest.mark.asyncio
async def test_non_integer_ledger_count_is_rejected() -> None:
    """A non-integer id count cannot be used to measure new postings.

    Given a session whose count query returns None,
    When the insert counter reads it,
    Then RuntimeError is raised.
    """

    class _Session:
        async def scalar(self, stmt: object) -> object:
            del stmt
            return None

    with pytest.raises(RuntimeError, match="not an integer"):
        await _count_ids(cast(AsyncSession, _Session()), _entry("L1", "1"), ["L1"])
