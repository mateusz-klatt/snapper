"""Tests for the clock-safe truthful position projection DAL (Phase 2 S1).

Pins the D9 consensus contract: the upsert selects the CURRENT version
by the current-version sentinel filter (never a bus-time-scoped
lookup), clamps the effective timestamp to
``max(bus_time, existing.timestamp)`` so successors never travel back
in time, carries the predecessor's ``public_id``, retries a lost
first-insert unique race exactly once, and ``close_position_projection``
closes the active row WITHOUT a successor while SCD2 history stays
queryable.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

from sqlalchemy import select

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Position
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PositionProjectionUpsertRow

_INSTRUMENT = "00000000-0000-7000-8000-00000000000a"
_WALLET = "00000000-0000-7000-8000-000000000001"


def _row(bus_time: datetime, **overrides: Any) -> PositionProjectionUpsertRow:
    """Build a full-state projection upsert row with sane defaults.

    Args:
        bus_time: Bus time of the projecting event.
        overrides: Field overrides applied on top of the defaults.

    Returns:
        Complete upsert row for the test identity.
    """
    row: PositionProjectionUpsertRow = {
        "instrument_public_id": _INSTRUMENT,
        "mode": "paper",
        "wallet_public_id": _WALLET,
        "quantity": 0.5,
        "average_price": 50000.0,
        "unrealized_pnl": 25.0,
        "realized_pnl": 10.0,
        "mark_price": 50050.0,
        "marked_at": bus_time,
        "source_venue_event_id": 42,
        "session_id": "00000000-0000-7000-8000-0000000000aa",
        "sequence_id": 1,
        "bus_time": bus_time,
    }
    row.update(overrides)
    return row


async def _make_repo(tmp_path: Path) -> SQLAlchemyRepository:
    """Create a throwaway SQLite repository with the full schema.

    Args:
        tmp_path: Pytest-provided temporary directory.

    Returns:
        Repository bound to a fresh database.
    """
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'positions.db'}")
    await repo.create_all()
    return repo


async def _all_rows(repo: SQLAlchemyRepository) -> list[Position]:
    """Return every position row ordered by id.

    Args:
        repo: Repository under test.

    Returns:
        All rows, historical and active.
    """
    async with repo.session() as s:
        result = await s.execute(select(Position).order_by(Position.id))
        return list(result.scalars().all())


async def test_upsert_inserts_new_active_row(tmp_path: Path) -> None:
    """First upsert creates one active row carrying the full snapshot.

    Given: an empty database,
    When: upsert_position_projection runs once,
    Then: exactly one active row exists with every value including the
        mark trio and watermark, timestamp equals bus_time, and a
        public_id was generated.
    """
    repo = await _make_repo(tmp_path)
    now = datetime.now(UTC)
    new_id = await repo.upsert_position_projection(_row(now))
    assert new_id > 0
    rows = await _all_rows(repo)
    assert len(rows) == 1
    row = rows[0]
    assert row.known_to == KNOWN_TO_MAX
    assert row.timestamp == now
    assert row.public_id
    assert row.quantity == 0.5
    assert row.average_price == 50000.0
    assert row.unrealized_pnl == 25.0
    assert row.realized_pnl == 10.0
    assert row.mark_price == 50050.0
    assert row.marked_at == now
    assert row.source_venue_event_id == 42


async def test_upsert_closes_predecessor_and_carries_public_id(tmp_path: Path) -> None:
    """Second upsert closes the old version and reuses its public_id.

    Given: an existing active row,
    When: a later upsert runs for the same identity,
    Then: the predecessor is closed at the new bus_time, the successor
        carries the same public_id with updated values, and exactly one
        active row remains.
    """
    repo = await _make_repo(tmp_path)
    t1 = datetime.now(UTC)
    t2 = t1 + timedelta(seconds=5)
    first_id = await repo.upsert_position_projection(_row(t1))
    second_id = await repo.upsert_position_projection(
        _row(t2, quantity=1.5, realized_pnl=20.0, source_venue_event_id=57)
    )
    assert second_id != first_id
    rows = await _all_rows(repo)
    assert len(rows) == 2
    old, new = rows
    assert old.known_to == t2
    assert new.known_to == KNOWN_TO_MAX
    assert new.public_id == old.public_id
    assert new.timestamp == t2
    assert new.quantity == 1.5
    assert new.source_venue_event_id == 57


async def test_upsert_clamps_lagging_bus_clock(tmp_path: Path) -> None:
    """A bus_time older than the stored timestamp is clamped forward.

    Given: an active row stamped at t1,
    When: an upsert arrives with bus_time five seconds BEFORE t1
        (the Phase-0 clock-skew class),
    Then: the successor is stamped at t1 (never travelling back), the
        predecessor closes at t1, and exactly one row stays active.
    """
    repo = await _make_repo(tmp_path)
    t1 = datetime.now(UTC)
    skewed = t1 - timedelta(seconds=5)
    await repo.upsert_position_projection(_row(t1))
    await repo.upsert_position_projection(_row(skewed, quantity=0.0))
    rows = await _all_rows(repo)
    assert len(rows) == 2
    old, new = rows
    assert old.known_to == t1
    assert new.timestamp == t1
    assert new.known_to == KNOWN_TO_MAX
    active = [r for r in rows if r.known_to == KNOWN_TO_MAX]
    assert len(active) == 1
    assert active[0].quantity == 0.0


async def test_upsert_persists_honest_nulls(tmp_path: Path) -> None:
    """NULL valuation and mark fields survive the round trip.

    Given: an aggregate with opposing directions and no usable mark,
    When: the upsert stores NULL entry, unrealized, mark trio, and
        watermark,
    Then: every one of them reads back as None — zero is never
        substituted.
    """
    repo = await _make_repo(tmp_path)
    now = datetime.now(UTC)
    await repo.upsert_position_projection(
        _row(
            now,
            average_price=None,
            unrealized_pnl=None,
            mark_price=None,
            marked_at=None,
            source_venue_event_id=None,
        )
    )
    rows = await _all_rows(repo)
    assert rows[0].average_price is None
    assert rows[0].unrealized_pnl is None
    assert rows[0].mark_price is None
    assert rows[0].marked_at is None
    assert rows[0].source_venue_event_id is None


async def test_upsert_retries_lost_first_insert_race(tmp_path: Path) -> None:
    """A lost partial-unique first-insert race re-reads the winner.

    Given: a concurrent writer commits the winning active row while the
        first close-and-insert pass believed the identity was empty, so
        flushing a conflicting active row dies on the REAL partial
        unique index (exercising failed-transaction rollback and
        pending-state cleanup),
    When: upsert_position_projection retries,
    Then: the retry finds and closes the committed winner, the
        successor carries the WINNER's public_id, and exactly one
        active row remains.
    """
    repo = await _make_repo(tmp_path)
    rival = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'positions.db'}")
    t1 = datetime.now(UTC)
    t2 = t1 + timedelta(seconds=1)
    original = repo._position_projection_close_and_insert
    attempts: list[int] = []

    async def flaky(s: Any, row: PositionProjectionUpsertRow) -> int:
        attempts.append(1)
        if len(attempts) == 1:
            await rival.upsert_position_projection(_row(t1, quantity=9.9))
            s.add(
                Position(
                    instrument_public_id=_INSTRUMENT,
                    mode="paper",
                    wallet_public_id=_WALLET,
                    quantity=0.1,
                    realized_pnl=0.0,
                    session_id="00000000-0000-7000-8000-0000000000aa",
                    sequence_id=2,
                    timestamp=t1,
                    known_to=KNOWN_TO_MAX,
                )
            )
            await s.flush()
            raise AssertionError("partial unique index must have rejected the insert")
        return await original(s, row)

    with patch.object(repo, "_position_projection_close_and_insert", side_effect=flaky):
        new_id = await repo.upsert_position_projection(_row(t2, quantity=1.0))
    assert new_id > 0
    assert len(attempts) == 2
    rows = await _all_rows(repo)
    assert len(rows) == 2
    winner, successor = rows
    assert winner.quantity == 9.9
    assert winner.known_to == t2
    assert successor.known_to == KNOWN_TO_MAX
    assert successor.public_id == winner.public_id
    assert successor.quantity == 1.0


async def test_close_position_projection_closes_without_successor(tmp_path: Path) -> None:
    """Closing a proven-flat identity leaves history but no active row.

    Given: an active row stamped at t1,
    When: close_position_projection runs at t2,
    Then: True is returned, the row's known_to becomes t2, no successor
        exists, and the closed version stays queryable.
    """
    repo = await _make_repo(tmp_path)
    t1 = datetime.now(UTC)
    t2 = t1 + timedelta(seconds=3)
    await repo.upsert_position_projection(_row(t1))
    closed = await repo.close_position_projection(_INSTRUMENT, "paper", _WALLET, t2)
    assert closed is True
    rows = await _all_rows(repo)
    assert len(rows) == 1
    assert rows[0].known_to == t2
    assert rows[0].quantity == 0.5


async def test_close_position_projection_returns_false_when_absent(tmp_path: Path) -> None:
    """Closing a non-existent identity reports False.

    Given: an empty database,
    When: close_position_projection runs,
    Then: False is returned and nothing is written.
    """
    repo = await _make_repo(tmp_path)
    closed = await repo.close_position_projection(_INSTRUMENT, "paper", _WALLET, datetime.now(UTC))
    assert closed is False
    assert await _all_rows(repo) == []


async def test_close_position_projection_clamps_lagging_bus_clock(tmp_path: Path) -> None:
    """A close with a lagging bus clock never rewinds known_to.

    Given: an active row stamped at t1,
    When: close_position_projection runs with bus_time BEFORE t1,
    Then: the row closes at t1 (zero-width version, never negative).
    """
    repo = await _make_repo(tmp_path)
    t1 = datetime.now(UTC)
    await repo.upsert_position_projection(_row(t1))
    closed = await repo.close_position_projection(
        _INSTRUMENT, "paper", _WALLET, t1 - timedelta(seconds=7)
    )
    assert closed is True
    rows = await _all_rows(repo)
    assert rows[0].known_to == t1
