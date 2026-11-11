"""Repository tests for the record-once order terminal and the venue end time (item G)."""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import PropertyMock
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.models import VenueEvent
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import VenueEventInsertRow

_WALLET = "00000000-0000-7000-8000-000000000001"
_SESSION = "00000000-0000-7000-8000-0000000000a1"
_NOW = datetime(2026, 11, 11, 12, 0, tzinfo=UTC)


def _event(
    *,
    event_type: str = "order_terminal",
    client_order_id: str | None = "cid-1",
    status: str | None = "canceled",
    received_at: datetime = _NOW,
    sequence_id: int = 1,
) -> VenueEventInsertRow:
    """Build one complete venue event row.

    Args:
        event_type: Venue event type.
        client_order_id: Order the event belongs to.
        status: Event status as the executor writes it.
        received_at: Observation time.
        sequence_id: Provenance sequence of the row.

    Returns:
        The insert row.
    """
    return VenueEventInsertRow(
        event_type=event_type,
        shard_key="kraken.BTC-USD.live",
        exchange="kraken",
        instrument="BTC-USD",
        mode="live",
        received_at=received_at,
        session_id=_SESSION,
        sequence_id=sequence_id,
        timestamp=received_at,
        client_order_id=client_order_id,
        exchange_order_id="ex-1",
        status=status,
        wallet_public_id=_WALLET,
        liquidity_role="unknown",
    )


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Provide an empty SQLite repository owned by one test."""
    created = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'terminals.db'}")
    await created.create_all()
    try:
        yield created
    finally:
        await created.engine.dispose()


async def _terminal_count(repository: SQLAlchemyRepository, client_order_id: str = "cid-1") -> int:
    """Count the stored order_terminal rows of one order.

    Args:
        repository: Repository to read.
        client_order_id: Order whose terminals are counted.

    Returns:
        The number of order_terminal rows.
    """
    async with repository.session() as s:
        result = await s.execute(
            sa.select(sa.func.count())
            .select_from(VenueEvent)
            .where(
                VenueEvent.client_order_id == client_order_id,
                VenueEvent.event_type == "order_terminal",
            )
        )
        return int(result.scalar_one())


@pytest.mark.asyncio
async def test_second_call_finds_the_recorded_terminal(repository: SQLAlchemyRepository) -> None:
    """Record an order's terminal once, whoever calls again.

    Given: an order without a terminal row,
    When: the record-once insert runs twice for it,
    Then: the first call inserts, the second finds the row, and one row exists.
    """
    first = await repository.insert_order_terminal_once(_event())
    second = await repository.insert_order_terminal_once(_event(sequence_id=2))

    assert (first, second) == (True, False)
    assert await _terminal_count(repository) == 1


@pytest.mark.asyncio
async def test_independent_engines_race_on_one_file(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """Serialise two engines on one SQLite file through the write reservation.

    Given: two independent repositories on one database file, the first held
        inside its record-once transaction after BEGIN IMMEDIATE,
    When: the second calls the record-once insert for the same order,
    Then: it cannot finish while the first holds the reservation, it finds the
        row once the first commits, and one row exists.
    """
    other = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'terminals.db'}")
    entered = asyncio.Event()
    release = asyncio.Event()
    begin = repository._begin_order_terminal_transaction

    async def held_begin(session: AsyncSession, client_order_id: str) -> None:
        """Open the transaction, then wait inside it until released."""
        await begin(session, client_order_id)
        entered.set()
        await release.wait()

    try:
        with patch.object(repository, "_begin_order_terminal_transaction", side_effect=held_begin):
            first = asyncio.create_task(repository.insert_order_terminal_once(_event()))
            await entered.wait()
            second = asyncio.create_task(other.insert_order_terminal_once(_event(sequence_id=2)))
            await asyncio.sleep(0.2)
            assert not second.done()
            release.set()
            results = await asyncio.gather(first, second)
    finally:
        await other.engine.dispose()

    assert results == [True, False]
    assert await _terminal_count(repository) == 1


@pytest.mark.asyncio
async def test_commit_that_raises_afterwards_is_resolved_by_a_second_call(
    repository: SQLAlchemyRepository,
) -> None:
    """Resolve an ambiguous commit by calling the record-once insert again.

    Given: a commit that reaches the database and then raises,
    When: the caller calls the record-once insert a second time,
    Then: the second call finds the committed row and one row exists.
    """
    commit = AsyncSession.commit
    commits: list[int] = []

    async def commit_then_raise(session: AsyncSession) -> None:
        """Commit for real, then report a lost acknowledgement once."""
        await commit(session)
        commits.append(1)
        if len(commits) == 1:
            raise ConnectionError("commit reached the database, acknowledgement lost")

    with patch.object(AsyncSession, "commit", commit_then_raise):
        with pytest.raises(ConnectionError):
            await repository.insert_order_terminal_once(_event())
        again = await repository.insert_order_terminal_once(_event(sequence_id=2))

    assert again is False
    assert await _terminal_count(repository) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row",
    [_event(event_type="order_accepted"), _event(client_order_id=None), _event(client_order_id="")],
)
async def test_refuses_a_row_that_is_not_an_order_terminal(
    repository: SQLAlchemyRepository, row: VenueEventInsertRow
) -> None:
    """Refuse rows the record-once path cannot key.

    Given: a row of another event type, or one without a client order id,
    When: the record-once insert is called,
    Then: it raises before opening any transaction.
    """
    with pytest.raises(ValueError, match="order_terminal row with a client order id"):
        await repository.insert_order_terminal_once(row)


@pytest.mark.asyncio
async def test_postgresql_forces_read_committed_then_locks_the_order() -> None:
    """Open the PostgreSQL record-once transaction in the safe order.

    Given: a PostgreSQL repository and a fresh session,
    When: the record-once transaction is opened for an order,
    Then: READ COMMITTED is set first and the advisory lock is keyed by the
        full client order id.
    """
    repository = SQLAlchemyRepository.__new__(SQLAlchemyRepository)
    session = AsyncMock(spec=AsyncSession)
    with patch.object(
        SQLAlchemyRepository, "dialect_name", new_callable=PropertyMock, return_value="postgresql"
    ):
        await repository._begin_order_terminal_transaction(session, "cid-7")

    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert statements[0] == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
    assert "hashtextextended('order_terminal:' || :client_order_id, 0)" in statements[1]
    assert session.execute.await_args_list[1].args[1] == {"client_order_id": "cid-7"}


@pytest.mark.asyncio
async def test_unsupported_dialect_fails_closed() -> None:
    """Refuse a dialect the record-once transaction has no guarantee for.

    Given: a repository whose dialect is neither PostgreSQL nor SQLite,
    When: the record-once transaction is opened,
    Then: it raises without executing anything.
    """
    repository = SQLAlchemyRepository.__new__(SQLAlchemyRepository)
    session = AsyncMock(spec=AsyncSession)
    with (
        patch.object(
            SQLAlchemyRepository, "dialect_name", new_callable=PropertyMock, return_value="oracle"
        ),
        pytest.raises(NotImplementedError, match="dialect=oracle"),
    ):
        await repository._begin_order_terminal_transaction(session, "cid-7")
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_latest_received_at_filters_types_and_statuses(
    repository: SQLAlchemyRepository,
) -> None:
    """Return the newest qualifying observation time of an order's events.

    Given: an order with an accept, two fills and terminals in mixed-case
        statuses, and another order's later terminal,
    When: the newest received_at is read for several type and status filters,
    Then: each answer is the newest matching row of that order, and an
        unmatched filter answers None.
    """
    times = [_NOW + timedelta(minutes=minute) for minute in range(6)]
    rows = [
        _event(event_type="order_accepted", status=None, received_at=times[0], sequence_id=1),
        _event(event_type="fill_observed", status="partial", received_at=times[1], sequence_id=2),
        _event(event_type="fill_observed", status="filled", received_at=times[2], sequence_id=3),
        _event(event_type="order_terminal", status="CANCELED", received_at=times[3], sequence_id=4),
        _event(event_type="order_terminal", status="Expired", received_at=times[4], sequence_id=5),
        _event(
            event_type="order_terminal",
            client_order_id="cid-other",
            received_at=times[5],
            sequence_id=6,
        ),
    ]
    for row in rows:
        await repository.insert_venue_event(row)

    read = repository.get_latest_venue_event_received_at
    assert await read("cid-1", ("fill_observed",)) == times[2]
    assert await read("cid-1", ("order_terminal",), ("canceled", "cancelled")) == times[3]
    assert await read("cid-1", ("order_terminal",), ("EXPIRED",)) == times[4]
    assert await read("cid-1", ("order_terminal",)) == times[4]
    assert await read("cid-1", ("order_terminal",), ("filled", "closed")) is None
    assert await read("cid-missing", ("order_terminal",)) is None
