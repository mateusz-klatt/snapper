"""Opt-in live-PostgreSQL witness for the record-once order terminal (item G).

The SQLite tests show the write reservation serialising two engines. This
module shows the PostgreSQL guarantee on the production dialect, with genuinely
overlapping connections: a second writer parked on the per-order advisory lock,
running on an engine whose sessions inherit ``REPEATABLE READ``, still finds
the row the first writer committed, because the record-once transaction forces
``READ COMMITTED`` before the lock. One row exists afterwards.

Every test SKIPS unless the session database URL (``DB_URL``, the Makefile's
``TEST_DB_URL`` pass-through) is a PostgreSQL URL. It needs a migrated scratch
database, as the other live-PostgreSQL witnesses do, and writes only venue
events keyed by a random client order id. Never point it at production.
"""

import asyncio
import os
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

from snapper.data.models import VenueEvent
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import VenueEventInsertRow

_NOW = datetime(2026, 11, 11, 12, 0, tzinfo=UTC)
_RACE_TIMEOUT_SECONDS = 10.0


def _configured_backend_name() -> str:
    """Return the backend name of the session-configured database URL.

    An unset or unparseable ``DB_URL`` maps to the empty string, so the module
    skips instead of erroring under ad-hoc invocations.
    """
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
        "live-PostgreSQL witness module: opt in via "
        "make test TEST_DB_URL=postgresql+asyncpg://... (see module docstring)"
    ),
)


def _terminal(client_order_id: str, sequence_id: int) -> VenueEventInsertRow:
    """Build one order_terminal row for a random test order.

    Args:
        client_order_id: Order the terminal belongs to.
        sequence_id: Provenance sequence of the row.

    Returns:
        The insert row.
    """
    return VenueEventInsertRow(
        event_type="order_terminal",
        shard_key="kraken.BTC-USD.live",
        exchange="kraken",
        instrument="BTC-USD",
        mode="live",
        received_at=_NOW,
        session_id=str(uuid4()),
        sequence_id=sequence_id,
        timestamp=_NOW,
        client_order_id=client_order_id,
        exchange_order_id="ex-witness",
        status="canceled",
        wallet_public_id=str(uuid4()),
        liquidity_role="unknown",
    )


@pytest.fixture
async def repository() -> AsyncIterator[SQLAlchemyRepository]:
    """Create the first writer's engine against the live PostgreSQL database."""
    result = SQLAlchemyRepository(os.environ["DB_URL"])
    try:
        yield result
    finally:
        await result.engine.dispose()


@pytest.fixture
async def inherited_repeatable_read_repository() -> AsyncIterator[SQLAlchemyRepository]:
    """Create a second, one-connection engine whose sessions default to REPEATABLE READ.

    Rebinding the session factory to a ``REPEATABLE READ`` proxy reproduces an
    inherited ``default_transaction_isolation`` while the record-once insert
    runs unmodified. The pool holds exactly one connection, so the backend pid
    read before the race is the backend the second writer then uses, and the
    poll watches that writer alone.
    """
    result = SQLAlchemyRepository(os.environ["DB_URL"])
    single = create_async_engine(os.environ["DB_URL"], pool_size=1, max_overflow=0)
    result.session_factory = async_sessionmaker(
        single.execution_options(isolation_level="REPEATABLE READ"),
        expire_on_commit=False,
        class_=AsyncSession,
    )
    try:
        yield result
    finally:
        await single.dispose()
        await result.engine.dispose()


async def _backend_pid(repository: SQLAlchemyRepository) -> int:
    """Return the backend pid of the connection the repository's sessions use.

    Args:
        repository: Repository whose session factory has one pooled connection.

    Returns:
        The PostgreSQL backend pid.
    """
    async with repository.session() as s:
        pid = (await s.execute(text("SELECT pg_backend_pid()"))).scalar_one()
    return int(pid)


async def _wait_until_parked_on_the_advisory_lock(observer: SQLAlchemyRepository, pid: int) -> str:
    """Poll one backend until it waits on an advisory lock, bounded.

    Args:
        observer: Repository used for the read-only poll, on other connections.
        pid: Backend pid of the writer to watch.

    Returns:
        The statement that backend is running while it waits.

    Raises:
        TimeoutError: When the backend does not reach the advisory wait in time.
    """
    deadline = asyncio.get_running_loop().time() + _RACE_TIMEOUT_SECONDS
    while asyncio.get_running_loop().time() < deadline:
        async with observer.session() as s:
            row = (
                await s.execute(
                    text(
                        "SELECT wait_event_type, wait_event, query "
                        "FROM pg_stat_activity WHERE pid = :pid"
                    ),
                    {"pid": pid},
                )
            ).first()
        if row is not None and (row[0], row[1]) == ("Lock", "advisory"):
            return str(row[2])
        await asyncio.sleep(0.05)
    raise TimeoutError(f"backend {pid} did not reach the advisory lock wait in time")


async def _terminal_count(repository: SQLAlchemyRepository, client_order_id: str) -> int:
    """Count the stored order_terminal rows of one order.

    Args:
        repository: Repository to read.
        client_order_id: Order whose terminals are counted.

    Returns:
        The number of order_terminal rows.
    """
    async with repository.session() as s:
        count = (
            await s.execute(
                select(func.count())
                .select_from(VenueEvent)
                .where(
                    VenueEvent.client_order_id == client_order_id,
                    VenueEvent.event_type == "order_terminal",
                )
            )
        ).scalar_one()
    return int(count)


async def test_parked_repeatable_read_writer_finds_the_first_terminal(
    repository: SQLAlchemyRepository,
    inherited_repeatable_read_repository: SQLAlchemyRepository,
) -> None:
    """Serialise two engines on the per-order advisory lock without a duplicate.

    Given: a first writer held inside its record-once transaction after it
        took the advisory lock, and a second writer on an engine that
        inherits REPEATABLE READ,
    When: the second writer calls the record-once insert for the same order,
    Then: its own backend parks on the advisory lock while its current
        statement is still the lock, so its existence check has not run; it
        finds the first writer's row once that commits; and one row exists.
    """
    client_order_id = f"witness-{uuid4()}"
    entered = asyncio.Event()
    release = asyncio.Event()
    begin = repository._begin_order_terminal_transaction

    async def held_begin(session: AsyncSession, cid: str) -> None:
        """Take the lock, then wait inside the transaction until released."""
        await begin(session, cid)
        entered.set()
        await release.wait()

    second_pid = await _backend_pid(inherited_repeatable_read_repository)
    with patch.object(repository, "_begin_order_terminal_transaction", side_effect=held_begin):
        first = asyncio.create_task(
            repository.insert_order_terminal_once(_terminal(client_order_id, 1))
        )
        await asyncio.wait_for(entered.wait(), timeout=_RACE_TIMEOUT_SECONDS)
        second = asyncio.create_task(
            inherited_repeatable_read_repository.insert_order_terminal_once(
                _terminal(client_order_id, 2)
            )
        )
        statement = await _wait_until_parked_on_the_advisory_lock(repository, second_pid)
        assert "pg_advisory_xact_lock" in statement
        assert not second.done()
        release.set()
        results = await asyncio.wait_for(
            asyncio.gather(first, second), timeout=_RACE_TIMEOUT_SECONDS
        )

    assert results == [True, False]
    assert await _terminal_count(repository, client_order_id) == 1
