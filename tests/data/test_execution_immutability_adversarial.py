"""Adversarial proof that the executions ledger is physically append-only.

The Python-layer ``_guard_immutable_ledger`` refuses the six generic ORM
mutation primitives, but a target-parameter guard cannot reach raw
``text()`` SQL, an ORM-enabled ``update()`` / ``delete()`` issued straight
on a session, an aliased delete, or a ``quoted_name(quote=False)``
identifier injection — the exact spelling that once fooled name-based
guards. This module issues every one of those vectors RAW on the session
(so only the physical trigger can be doing the rejecting) against BOTH a
``create_all``-built database (the ``after_create`` event installs the
trigger at table creation) AND a migration-built database (migration 0030
installs it on the already-existing table), proving UPDATE and DELETE —
including a no-``WHERE`` mass delete — are refused through every path.

Liveness is proven alongside rejection: the fenced ``insert_execution``
still appends and mints contiguous counters, the counted-range proof still
reads COMPLETE over the appended prefix, and a mutation of a NON-ledger
table still succeeds, so the trigger neither impedes ingest nor leaks onto
other tables.

The PostgreSQL dialect (including its ``TRUNCATE`` trigger, which SQLite
lacks) is proven in the opt-in live-PostgreSQL companion module; SQLite
here always runs under the default ``make test`` / ``make check-all``.
"""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import column
from sqlalchemy import delete
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import table
from sqlalchemy import text
from sqlalchemy import update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import aliased
from sqlalchemy.sql import quoted_name
from sqlalchemy.sql.expression import Executable

from snapper.data.models import Execution
from snapper.data.models import Instrument
from snapper.data.models import Order
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 7, 17, 8, 0, tzinfo=UTC)
_WALLET = "0000face-0000-7000-8000-00000000c101"
_EXCHANGE = "walutomat"
_MODE = "live"
_SESSION = "00000000-0000-7000-8000-000000000301"
_INSTRUMENT = "00000000-0000-7000-8000-000000000501"
_ORDER = "00000000-0000-7000-8000-000000000601"
_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_SQLITE_TRIGGER_QUERY = (
    "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name='executions' ORDER BY name"
)


@pytest.fixture()
async def create_all_repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Build a repository whose schema comes from ``Base.metadata.create_all``.

    This is the path every test and in-memory database takes; the
    ``after_create`` event installs the triggers at table creation, so no
    migration is involved.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'create-all.db'}")
    await repository.create_all()
    try:
        yield repository
    finally:
        await repository.engine.dispose()


@pytest.fixture()
async def migration_repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Build a repository whose schema comes from Alembic migrations to head.

    This mirrors the production table: created and extended by migrations,
    with the trigger installed by migration 0030 on the already-existing
    table (never by ``create_all``).
    """
    db_path = tmp_path / "migrated.db"
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", f"sqlite:///{db_path}")
    command.upgrade(config, "head")
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    try:
        yield repository
    finally:
        await repository.engine.dispose()


async def _seed_execution_row(repository: SQLAlchemyRepository) -> int:
    """Insert one committed execution row directly (INSERT is permitted).

    A direct ``session.add`` of an ``Execution`` is a plain ``INSERT``, the
    only mutation the trigger allows, so it seeds a sealed row to attack
    without needing the full ingest lineage. Returns the row id.
    """
    async with repository.session() as s:
        row = Execution(
            order_public_id=_ORDER,
            wallet_public_id=_WALLET,
            exchange=_EXCHANGE,
            mode=_MODE,
            scope_sequence=1,
            timestamp=_NOW,
            side="buy",
            status="filled",
            price=4.25,
            size=100.0,
            fee=0.5,
            fee_asset="PLN",
            session_id=_SESSION,
            sequence_id=1,
        )
        s.add(row)
        await s.commit()
        return int((await s.execute(select(Execution.id))).scalar_one())


def _mutation_vectors(row_id: int) -> dict[str, Executable]:
    """Build every raw mutation vector that must be physically rejected.

    Each vector bypasses the six Python-guarded primitives — raw ``text()``
    SQL, an ORM-enabled ``update``/``delete`` on the session, an aliased
    delete, and a ``quoted_name(quote=False)`` identifier injection — so a
    rejection can only come from the physical trigger.
    """
    injected = table(quoted_name("executions", quote=False), column("id"), column("fee"))
    return {
        "raw text UPDATE": text("UPDATE executions SET fee = fee + 1 WHERE id = :id").bindparams(
            id=row_id
        ),
        "raw text DELETE": text("DELETE FROM executions WHERE id = :id").bindparams(id=row_id),
        "ORM update": update(Execution).where(Execution.id == row_id).values(fee=9.0),
        "ORM delete": delete(Execution).where(Execution.id == row_id),
        "aliased delete": delete(aliased(Execution)),
        "quoted_name delete": delete(injected).where(injected.c.id == row_id),
        "quoted_name update": update(injected).where(injected.c.id == row_id).values(fee=9.0),
        "no-WHERE mass delete": text("DELETE FROM executions"),
    }


async def _row_count(repository: SQLAlchemyRepository) -> int:
    """Return the committed executions row count."""
    async with repository.session() as s:
        return int((await s.execute(select(func.count()).select_from(Execution))).scalar_one())


async def _assert_every_mutation_is_rejected(repository: SQLAlchemyRepository) -> None:
    """Assert every raw mutation vector is refused and the sealed row survives."""
    row_id = await _seed_execution_row(repository)
    for statement in _mutation_vectors(row_id).values():
        async with repository.session() as s:
            with pytest.raises(DBAPIError, match="append-only"):
                await s.execute(statement)
                await s.commit()
        assert await _row_count(repository) == 1


@pytest.mark.asyncio
async def test_create_all_built_ledger_rejects_every_raw_mutation(
    create_all_repository: SQLAlchemyRepository,
) -> None:
    """Every UPDATE/DELETE vector is physically refused on a create_all database.

    Given: A ``create_all``-built ledger holding one sealed execution row.
    When: Each raw mutation vector — text SQL, ORM update/delete, aliased
        delete, ``quoted_name`` injection, and a no-``WHERE`` mass delete —
        is issued raw on the session.
    Then: Each raises an ``append-only`` ``DBAPIError`` and the sealed row
        survives untouched.
    """
    await _assert_every_mutation_is_rejected(create_all_repository)


@pytest.mark.asyncio
async def test_migration_built_ledger_rejects_every_raw_mutation(
    migration_repository: SQLAlchemyRepository,
) -> None:
    """Every UPDATE/DELETE vector is physically refused on a migration-built database.

    Given: A migration-built ledger (trigger installed by 0030 on the
        already-existing table) holding one sealed execution row.
    When: Each raw mutation vector is issued raw on the session.
    Then: Each raises an ``append-only`` ``DBAPIError`` and the sealed row
        survives — proving the migration install matches the event install.
    """
    await _assert_every_mutation_is_rejected(migration_repository)


@pytest.mark.asyncio
async def test_triggers_are_present_on_both_build_paths(
    create_all_repository: SQLAlchemyRepository,
    migration_repository: SQLAlchemyRepository,
) -> None:
    """Both build paths carry the two named SQLite triggers.

    Given: A ``create_all``-built and a migration-built ledger.
    When: Each database's trigger catalog is read.
    Then: Both carry exactly ``executions_reject_delete`` and
        ``executions_reject_update`` — the install is present everywhere the
        table exists.
    """
    for repository in (create_all_repository, migration_repository):
        async with repository.session() as s:
            names = [str(row[0]) for row in (await s.execute(text(_SQLITE_TRIGGER_QUERY)))]
        assert names == ["executions_reject_delete", "executions_reject_update"]


def _replace_vectors(row_id: int) -> dict[str, Executable]:
    """Build the two ``REPLACE`` spellings that delete a sealed row.

    SQLite resolves an ``INSERT OR REPLACE`` / ``REPLACE INTO`` unique- or
    primary-key conflict by physically DELETING the conflicting row and
    inserting the replacement. That delete fires a ``BEFORE DELETE`` trigger
    ONLY when ``PRAGMA recursive_triggers`` is ON (it defaults OFF), so
    without the connect-time pragma these vectors would silently substitute
    a different fill at the same ``scope_sequence`` without tripping
    ``executions_reject_delete``.

    Both vectors source the sealed row's columns via ``SELECT`` (avoiding
    datetime re-formatting) and rewrite ``price`` so a successful bypass
    would be visible. The first conflicts on ``uq_executions_scope_sequence``
    (a fresh id + fresh ``public_id`` but the same scope key — the true
    substitution attack); the second conflicts on the primary key ``id``.
    """
    return {
        "INSERT OR REPLACE scope-sequence substitution": text(
            "INSERT OR REPLACE INTO executions "
            "(public_id, session_id, sequence_id, timestamp, known_to, "
            "order_public_id, wallet_public_id, exchange, mode, scope_sequence, "
            "side, status, price, size, fee, fee_asset, liquidity_role) "
            "SELECT :new_pid, session_id, sequence_id, timestamp, known_to, "
            "order_public_id, wallet_public_id, exchange, mode, scope_sequence, "
            "side, status, 999.0, size, fee, fee_asset, liquidity_role "
            "FROM executions WHERE id = :id"
        ).bindparams(id=row_id, new_pid="00000000-0000-7000-8000-0000000009a1"),
        "REPLACE INTO primary-key substitution": text(
            "REPLACE INTO executions "
            "(id, public_id, session_id, sequence_id, timestamp, known_to, "
            "order_public_id, wallet_public_id, exchange, mode, scope_sequence, "
            "side, status, price, size, fee, fee_asset, liquidity_role) "
            "SELECT id, :new_pid, session_id, sequence_id, timestamp, known_to, "
            "order_public_id, wallet_public_id, exchange, mode, scope_sequence, "
            "side, status, 888.0, size, fee, fee_asset, liquidity_role "
            "FROM executions WHERE id = :id"
        ).bindparams(id=row_id, new_pid="00000000-0000-7000-8000-0000000009a2"),
    }


async def _assert_replace_bypass_is_rejected(repository: SQLAlchemyRepository) -> None:
    """Assert both REPLACE spellings are refused and the sealed row is untouched."""
    original_id = await _seed_execution_row(repository)
    for statement in _replace_vectors(original_id).values():
        async with repository.session() as s:
            with pytest.raises(DBAPIError, match="append-only"):
                await s.execute(statement)
                await s.commit()
        async with repository.session() as s:
            surviving = (await s.execute(select(Execution.id, Execution.price))).all()
        assert [(int(row[0]), float(row[1])) for row in surviving] == [(original_id, 4.25)]


@pytest.mark.asyncio
async def test_create_all_built_ledger_rejects_insert_or_replace(
    create_all_repository: SQLAlchemyRepository,
) -> None:
    """``INSERT OR REPLACE`` / ``REPLACE INTO`` are refused on a create_all database.

    Given: A ``create_all``-built ledger holding one sealed execution row,
        with ``PRAGMA recursive_triggers=ON`` applied on every connection.
    When: An ``INSERT OR REPLACE`` conflicting on ``uq_executions_scope_sequence``
        and a ``REPLACE INTO`` conflicting on the primary key are each issued
        raw — the delete-and-substitute vector that bypasses a ``BEFORE
        DELETE`` trigger when recursive triggers are OFF.
    Then: Each raises an ``append-only`` ``DBAPIError`` (the REPLACE-induced
        delete fires ``executions_reject_delete``) and the sealed row keeps
        its original id and price.
    """
    await _assert_replace_bypass_is_rejected(create_all_repository)


@pytest.mark.asyncio
async def test_migration_built_ledger_rejects_insert_or_replace(
    migration_repository: SQLAlchemyRepository,
) -> None:
    """``INSERT OR REPLACE`` / ``REPLACE INTO`` are refused on a migration database.

    Given: A migration-built ledger (trigger installed by 0030) holding one
        sealed execution row, with ``recursive_triggers=ON`` per connection.
    When: The same two REPLACE substitution vectors are issued raw.
    Then: Each raises an ``append-only`` ``DBAPIError`` and the sealed row
        survives with its original id and price — the pragma closes the
        REPLACE bypass on the migration build too.
    """
    await _assert_replace_bypass_is_rejected(migration_repository)


async def _seed_lineage(repository: SQLAlchemyRepository) -> None:
    """Seed the active Order -> Instrument lineage the fenced ingest resolves."""
    async with repository.session() as s:
        s.add_all(
            [
                Instrument(
                    public_id=_INSTRUMENT,
                    symbol_public_id=_INSTRUMENT,
                    exchange=_EXCHANGE,
                    timestamp=_NOW - timedelta(days=1),
                    session_id=_SESSION,
                    sequence_id=1,
                ),
                Order(
                    public_id=_ORDER,
                    instrument_public_id=_INSTRUMENT,
                    mode=_MODE,
                    wallet_public_id=_WALLET,
                    created_at=_NOW - timedelta(hours=2),
                    timestamp=_NOW - timedelta(hours=2),
                    side="buy",
                    order_type="limit",
                    price=4.25,
                    size=100.0,
                    status="filled",
                    session_id=_SESSION,
                    sequence_id=1,
                ),
            ]
        )
        await s.commit()


async def _ingest(repository: SQLAlchemyRepository, sequence_id: int) -> int:
    """Append one fill through the fenced production ingest path."""
    return await repository.insert_execution(
        order_public_id=_ORDER,
        wallet_public_id=_WALLET,
        timestamp=_NOW - timedelta(hours=1),
        side="buy",
        status="filled",
        price=4.25,
        size=100.0,
        fee=0.5,
        fee_asset="PLN",
        session_id=_SESSION,
        sequence_id=sequence_id,
    )


@pytest.mark.asyncio
async def test_fenced_ingest_still_appends_contiguously_under_the_trigger(
    create_all_repository: SQLAlchemyRepository,
) -> None:
    """The append-only trigger leaves the fenced INSERT ingest fully working.

    Given: A ``create_all``-built ledger with the trigger installed and one
        active order/instrument lineage.
    When: Two fills are ingested through ``insert_execution`` and the
        counted-range proof is evaluated over the appended prefix.
    Then: Both fills persist, they mint the contiguous counters 1 and 2,
        and the counted range ``(0, 2]`` reads COMPLETE — INSERT is
        untouched and gaps stay impossible.
    """
    await _seed_lineage(create_all_repository)
    await _ingest(create_all_repository, 1)
    await _ingest(create_all_repository, 2)
    async with create_all_repository.session() as s:
        counters = [
            int(value)
            for value in (
                await s.execute(
                    select(Execution.scope_sequence)
                    .where(
                        Execution.wallet_public_id == _WALLET,
                        Execution.exchange == _EXCHANGE,
                        Execution.mode == _MODE,
                    )
                    .order_by(Execution.scope_sequence)
                )
            ).scalars()
        ]
        counted = int(
            (
                await s.execute(
                    select(func.count())
                    .select_from(Execution)
                    .where(
                        Execution.wallet_public_id == _WALLET,
                        Execution.exchange == _EXCHANGE,
                        Execution.mode == _MODE,
                        Execution.scope_sequence > 0,
                        Execution.scope_sequence <= 2,
                    )
                )
            ).scalar_one()
        )
    assert counters == [1, 2]
    assert counted == 2


@pytest.mark.asyncio
async def test_trigger_does_not_leak_onto_non_ledger_tables(
    create_all_repository: SQLAlchemyRepository,
) -> None:
    """A mutation of a NON-ledger table still succeeds.

    Given: A ``create_all``-built database with the executions trigger
        installed and one seeded instrument row.
    When: A raw ORM ``update`` mutates the instrument (a non-ledger table).
    Then: The update commits and takes effect — the append-only trigger is
        scoped to ``executions`` and does not impede other tables.
    """
    await _seed_lineage(create_all_repository)
    async with create_all_repository.session() as s:
        await s.execute(
            update(Instrument).where(Instrument.public_id == _INSTRUMENT).values(exchange="kraken")
        )
        await s.commit()
    async with create_all_repository.session() as s:
        exchange = (
            await s.execute(select(Instrument.exchange).where(Instrument.public_id == _INSTRUMENT))
        ).scalar_one()
    assert exchange == "kraken"
