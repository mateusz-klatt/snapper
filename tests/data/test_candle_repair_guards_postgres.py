"""Opt-in live-PostgreSQL witness for the candle repair driver's guarded writers.

The SQLite module ``test_candle_repair_guards_repository.py`` proves the
calculated insert, the calculated correction and the expected-predecessor
check; this module repeats the load-bearing cases on ``postgresql+asyncpg``,
where the writers take real relation locks, and adds the driver's lock-free
reconciliation read.

Every test SKIPS unless the session database URL (``DB_URL``, the Makefile's
``TEST_DB_URL`` pass-through) is a PostgreSQL URL, so the default SQLite
``make test`` / ``make check-all`` runs are undisturbed. Opt in against a
migrated scratch database::

    make test TEST_DB_URL=postgresql+asyncpg://snapper:example@localhost:5432/snapper_repair_proof

Each test writes only candles of its own random instrument and deletes them
afterwards. Never point it at production.
"""

import asyncio
import os
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast
from uuid import uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy import insert
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.exc import IntegrityError

from snapper.application.updaters.historical.candle_repair import classify_failure
from snapper.application.updaters.historical.candle_repair import reconcile
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import CandlePredecessor
from snapper.data.repository_types import CandleUpsertRow

_NOW = datetime.now(UTC).replace(second=0, microsecond=0)
_OPEN = _NOW - timedelta(days=1, hours=3)
_AUTHOR = _NOW - timedelta(hours=1)


def _configured_backend_name() -> str:
    """Return the backend name of the session-configured database URL, or ''."""
    url = os.environ.get("DB_URL", "")
    if not url:
        return ""
    try:
        return make_url(url).get_backend_name()
    except ArgumentError:
        return ""


pytestmark = pytest.mark.skipif(
    _configured_backend_name() != "postgresql",
    reason=(
        "live-PostgreSQL witness: opt in via "
        "make test TEST_DB_URL=postgresql+asyncpg://... (see module docstring)"
    ),
)


@pytest.fixture
async def repository() -> AsyncIterator[SQLAlchemyRepository]:
    """Create one repository engine against the live PostgreSQL database."""
    result = SQLAlchemyRepository(os.environ["DB_URL"])
    try:
        yield result
    finally:
        await result.engine.dispose()


@pytest.fixture
async def instrument(repository: SQLAlchemyRepository) -> AsyncIterator[str]:
    """Provide a random instrument whose candles are removed after the test."""
    identity = str(uuid4())
    try:
        yield identity
    finally:
        async with repository.session() as session:
            await session.execute(delete(Candle).where(Candle.instrument_public_id == identity))
            await session.commit()


def _row(instrument: str, offset: int = 0, source: str = "calculated") -> CandleUpsertRow:
    """Build a closed minute with trades."""
    return {
        "instrument_public_id": instrument,
        "open_at": _OPEN + timedelta(minutes=offset),
        "timestamp": _AUTHOR,
        "timeframe": "1m",
        "open": 10.0,
        "high": 12.0,
        "low": 9.0,
        "close": 11.0,
        "volume": 3.0,
        "vwap": 10.75,
        "trades": 7,
        "source": source,
        "complete": True,
        "session_id": "00000000-0000-4000-8000-000000000b02",
        "sequence_id": offset + 1,
    }


def _zero(instrument: str, offset: int = 0) -> CandleUpsertRow:
    """Build an earlier flat synthesized zero."""
    return _row(instrument, offset) | {
        "timestamp": _AUTHOR - timedelta(hours=1),
        "source": "synthesized",
        "open": 10.0,
        "high": 10.0,
        "low": 10.0,
        "close": 10.0,
        "volume": 0.0,
        "vwap": 10.0,
        "trades": 0,
    }


async def _current(repository: SQLAlchemyRepository, instrument: str) -> list[dict[str, object]]:
    """Read the current versions of the instrument."""
    async with repository.session() as session:
        result = await session.execute(
            select(Candle.__table__)
            .where(Candle.instrument_public_id == instrument, Candle.known_to == KNOWN_TO_MAX)
            .order_by(Candle.open_at)
        )
        return [dict(row) for row in result.mappings().all()]


@pytest.mark.asyncio
async def test_calculated_insert_adds_only_absent_minutes_on_postgresql(
    repository: SQLAlchemyRepository, instrument: str
) -> None:
    """The calculated insert keeps the native repair's absence rule under ROW EXCLUSIVE.

    Given: One present native minute and one absent minute.
    When: Calculated rows for both are inserted.
    Then: Only the absent minute is inserted, as calculated.
    """
    await repository.upsert_candles([_row(instrument, 0, "native")])
    inserted = await repository.insert_missing_calculated_candles(
        [_row(instrument, 0), _row(instrument, 1)]
    )
    assert [row["open_at"] for row in inserted] == [_OPEN + timedelta(minutes=1)]
    assert [row["source"] for row in await _current(repository, instrument)] == [
        "native",
        "calculated",
    ]


@pytest.mark.asyncio
async def test_calculated_correction_and_voted_predecessor_on_postgresql(
    repository: SQLAlchemyRepository, instrument: str
) -> None:
    """A same-time replacement of the voted zero is skipped; the current zero is corrected.

    Given: A voted zero replaced by a zero with the same time and public ID.
    When: The correction runs with the voted zero, then with the successor.
    Then: The first is skipped under SHARE ROW EXCLUSIVE and the second corrects the
        successor, keeping the calculated source and the public ID.
    """
    await repository.upsert_candles([_zero(instrument)])
    voted = await _current(repository, instrument)
    async with repository.session() as session:
        await session.execute(
            update(Candle).where(Candle.id == voted[0]["id"]).values(known_to=voted[0]["timestamp"])
        )
        await session.execute(
            insert(Candle).values(**{key: value for key, value in voted[0].items() if key != "id"})
        )
        await session.commit()
    successor = (await _current(repository, instrument))[0]
    key = (instrument, "1m", _OPEN)

    def expected(version: dict[str, object]) -> dict[tuple[str, str, datetime], CandlePredecessor]:
        """Return a version as the expected predecessor of the key."""
        return {
            key: {
                "id": cast(int, version["id"]),
                "public_id": cast(str, version["public_id"]),
                "timestamp": cast(datetime, version["timestamp"]),
            }
        }

    skipped = await repository.correct_false_zero_candles(
        [_row(instrument)], source="calculated", expected_predecessors=expected(voted[0])
    )
    corrected = await repository.correct_false_zero_candles(
        [_row(instrument)], source="calculated", expected_predecessors=expected(successor)
    )

    assert skipped == []
    assert len(corrected) == 1
    current = await _current(repository, instrument)
    assert [(row["source"], row["public_id"]) for row in current] == [
        ("calculated", voted[0]["public_id"])
    ]


@pytest.mark.asyncio
async def test_reconciliation_reads_without_a_lock_on_postgresql(
    repository: SQLAlchemyRepository, instrument: str
) -> None:
    """Reconciliation sees committed rows only, never waits and never blocks a live write.

    Given: One committed run row, one superseded run row and one row staged in a
        transaction that is still open.
    When: The chunk is reconciled while that transaction holds its row lock and a live
        writer inserts another minute at the same time.
    Then: The rows read as applied, applied_superseded and uncertain, and both the read
        and the live write finish while the writer's transaction is still open.
    """
    rows = [_row(instrument, offset) for offset in range(3)]
    await repository.upsert_candles([rows[0], rows[1]])
    await repository.upsert_candles(
        [rows[1] | {"timestamp": _AUTHOR + timedelta(minutes=1), "close": 11.5}]
    )
    async with repository.session() as writer:
        await writer.execute(insert(Candle).values(**(rows[2] | {"public_id": str(uuid4())})))
        await writer.flush()
        outcomes, live = await asyncio.wait_for(
            asyncio.gather(
                reconcile(repository, rows, datetime.now(UTC)),
                repository.upsert_candles([_row(instrument, 5, "native")]),
            ),
            10,
        )
        await writer.rollback()
    assert live == 1
    assert outcomes == {
        (instrument, rows[0]["open_at"]): "applied",
        (instrument, rows[1]["open_at"]): "applied_superseded",
        (instrument, rows[2]["open_at"]): "uncertain",
    }


@pytest.mark.asyncio
async def test_unique_violations_are_classified_by_their_postgresql_index_names(
    repository: SQLAlchemyRepository, instrument: str
) -> None:
    """Real asyncpg violations of the natural key are conflicts; others stop the run.

    Given: Current rows in the leaf of the day before the run and in the leaf of the run.
    When: A second current row with the same natural key, and then a row reusing a
        current public ID, is inserted in each leaf.
    Then: The duplicate keys are natural-key conflicts and the reused public IDs are
        unrelated unique violations.
    """
    recent = _NOW - timedelta(hours=1)
    first = _row(instrument) | {"public_id": str(uuid4())}
    second = _row(instrument) | {"open_at": recent, "public_id": str(uuid4())}
    outcomes: list[str] = []
    for row in (first, second):
        async with repository.session() as session:
            await session.execute(insert(Candle).values(**row))
            await session.commit()
        for duplicate in (
            row | {"public_id": str(uuid4())},
            row | {"open_at": row["open_at"] + timedelta(minutes=1)},
        ):
            with pytest.raises(IntegrityError) as raised:
                async with repository.session() as session:
                    await session.execute(insert(Candle).values(**duplicate))
                    await session.commit()
            outcomes.append(classify_failure(raised.value))
    assert outcomes == [
        "natural_key_conflict",
        "unique_violation",
        "natural_key_conflict",
        "unique_violation",
    ]
