"""Opt-in live-PostgreSQL adversarial proof of the append-only trigger.

SQLite has no ``TRUNCATE`` and cannot exhibit the PostgreSQL trigger
shape (a plpgsql function fired by a ``BEFORE UPDATE OR DELETE`` row
trigger plus a ``BEFORE TRUNCATE`` statement trigger). This module
witnesses physical immutability on the production dialect: every raw
mutation vector — text SQL, ORM update/delete, aliased delete,
``quoted_name(quote=False)`` injection, a no-``WHERE`` mass delete, and
``TRUNCATE`` / ``TRUNCATE CASCADE`` (which a ``BEFORE DELETE`` row trigger
would NOT catch) — is refused, against BOTH a ``create_all``-built scratch
database (the ``after_create`` event installs the trigger) AND the
migration-built session database (migration 0030 installs it). Legit
fenced ingest still appends.

Every test SKIPS unless the session database URL (``DB_URL``, the
Makefile's ``TEST_DB_URL`` pass-through) is a PostgreSQL URL, so the
default SQLite ``make test`` / ``make check-all`` runs are undisturbed.
Opt in against a migrated scratch database exactly as the sealed-prefix
proof module documents.
"""

import os
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import column
from sqlalchemy import delete
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import table
from sqlalchemy import text
from sqlalchemy import update
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import aliased
from sqlalchemy.sql import quoted_name
from sqlalchemy.sql.expression import Executable

from snapper.data.models import Execution
from snapper.data.models import Instrument
from snapper.data.models import Order
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 7, 17, 8, 0, tzinfo=UTC)
_EXCHANGE = "walutomat"
_MODE = "live"


def _configured_backend_name() -> str:
    """Return the backend name of the session-configured database URL."""
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
        "live-PostgreSQL adversarial module: opt in via "
        "make test TEST_DB_URL=postgresql+asyncpg://... (see module docstring)"
    ),
)


async def _admin_connection(base_url: object) -> asyncpg.Connection:
    """Open an autocommit connection to the maintenance ``postgres`` database."""
    return await asyncpg.connect(
        user=base_url.username,
        password=base_url.password,
        host=base_url.host,
        port=base_url.port,
        database="postgres",
    )


@pytest.fixture()
async def migration_repository() -> AsyncIterator[SQLAlchemyRepository]:
    """Yield a repository over the migration-built session database.

    The session ``DB_URL`` scratch database is brought to head by
    ``db-init``, so migration 0030 installed the trigger on its
    already-existing ``executions`` table.
    """
    repository = SQLAlchemyRepository(os.environ["DB_URL"])
    try:
        yield repository
    finally:
        await repository.engine.dispose()


@pytest.fixture()
async def create_all_repository() -> AsyncIterator[SQLAlchemyRepository]:
    """Create a throwaway PostgreSQL database via ``create_all`` and reap it.

    The schema comes from ``Base.metadata.create_all`` (never Alembic), so
    the ``after_create`` event is the sole trigger installer — the same
    path the SQLite test and in-memory fixtures take, proven on the
    production dialect.
    """
    base_url = make_url(os.environ["DB_URL"])
    scratch_name = f"snapper_immut_createall_{uuid4().hex}"
    creator = await _admin_connection(base_url)
    try:
        await creator.execute(f'CREATE DATABASE "{scratch_name}"')
    finally:
        await creator.close()
    repository = SQLAlchemyRepository(
        base_url.set(database=scratch_name).render_as_string(hide_password=False)
    )
    await repository.create_all()
    try:
        yield repository
    finally:
        await repository.engine.dispose()
        dropper = await _admin_connection(base_url)
        try:
            await dropper.execute(f'DROP DATABASE IF EXISTS "{scratch_name}" WITH (FORCE)')
        finally:
            await dropper.close()


def _scope_identity() -> tuple[str, str, str, str]:
    """Return per-test random (wallet, order, instrument, session) identities."""
    return str(uuid4()), str(uuid4()), str(uuid4()), str(uuid4())


async def _seed_execution_row(
    repository: SQLAlchemyRepository,
    wallet: str,
    order_public_id: str,
    session_id: str,
) -> int:
    """Insert one sealed execution row directly (INSERT is permitted)."""
    async with repository.session() as s:
        row = Execution(
            order_public_id=order_public_id,
            wallet_public_id=wallet,
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
            session_id=session_id,
            sequence_id=1,
        )
        s.add(row)
        await s.commit()
        return int(
            (
                await s.execute(select(Execution.id).where(Execution.wallet_public_id == wallet))
            ).scalar_one()
        )


def _mutation_vectors(row_id: int) -> dict[str, Executable]:
    """Build every raw mutation vector, including PostgreSQL TRUNCATE."""
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
        "TRUNCATE": text("TRUNCATE executions"),
        "TRUNCATE CASCADE": text("TRUNCATE executions CASCADE"),
    }


async def _assert_every_mutation_is_rejected(
    repository: SQLAlchemyRepository, wallet: str, row_id: int
) -> None:
    """Assert every vector is refused and the sealed row for this scope survives."""
    for statement in _mutation_vectors(row_id).values():
        async with repository.session() as s:
            with pytest.raises(DBAPIError, match="append-only"):
                await s.execute(statement)
                await s.commit()
        async with repository.session() as s:
            survivors = int(
                (
                    await s.execute(
                        select(func.count())
                        .select_from(Execution)
                        .where(Execution.wallet_public_id == wallet)
                    )
                ).scalar_one()
            )
        assert survivors == 1


def _replica_role_vectors(row_id: int) -> dict[str, Executable]:
    """Build the DELETE/UPDATE/TRUNCATE vectors run under the replica role.

    A plain (origin) PostgreSQL trigger does NOT fire when the session sets
    ``session_replication_role = replica`` — a GUC the app's own connection
    can set — so without the ``ENABLE ALWAYS`` promotion these three
    mutations would silently succeed under replica mode with the
    append-only triggers disabled session-wide. Each must still raise
    because the triggers are installed ``ENABLE ALWAYS``.
    """
    return {
        "replica DELETE": text("DELETE FROM executions WHERE id = :id").bindparams(id=row_id),
        "replica UPDATE": text("UPDATE executions SET fee = fee + 1 WHERE id = :id").bindparams(
            id=row_id
        ),
        "replica TRUNCATE": text("TRUNCATE executions"),
    }


async def _assert_replica_role_bypass_is_rejected(
    repository: SQLAlchemyRepository, wallet: str, row_id: int
) -> None:
    """Assert each replica-role mutation is refused and the scope's row survives."""
    for statement in _replica_role_vectors(row_id).values():
        async with repository.session() as s:
            await s.execute(text("SET session_replication_role = replica"))
            with pytest.raises(DBAPIError, match="append-only"):
                await s.execute(statement)
                await s.commit()
        async with repository.session() as s:
            survivors = int(
                (
                    await s.execute(
                        select(func.count())
                        .select_from(Execution)
                        .where(Execution.wallet_public_id == wallet)
                    )
                ).scalar_one()
            )
        assert survivors == 1


@pytest.mark.asyncio
async def test_migration_built_postgres_rejects_replica_role_mutations(
    migration_repository: SQLAlchemyRepository,
) -> None:
    """Under ``session_replication_role = replica`` every mutation still raises (migration build).

    Given: The migration-built session database (triggers from 0030,
        installed ``ENABLE ALWAYS``) holding one sealed row under a random
        wallet.
    When: The session sets ``session_replication_role = replica`` and then
        issues DELETE, UPDATE, and TRUNCATE on ``executions``.
    Then: Each raises an ``append-only`` ``DBAPIError`` and the scope's
        sealed row survives — the ``ENABLE ALWAYS`` promotion closes the
        replica-role session-wide bypass.
    """
    wallet, order_public_id, _instrument, session_id = _scope_identity()
    row_id = await _seed_execution_row(migration_repository, wallet, order_public_id, session_id)
    await _assert_replica_role_bypass_is_rejected(migration_repository, wallet, row_id)


@pytest.mark.asyncio
async def test_create_all_built_postgres_rejects_replica_role_mutations(
    create_all_repository: SQLAlchemyRepository,
) -> None:
    """Under ``session_replication_role = replica`` every mutation still raises (create_all build).

    Given: A ``create_all``-built throwaway PostgreSQL database (triggers
        from the ``after_create`` event, installed ``ENABLE ALWAYS``) holding
        one sealed row under a random wallet.
    When: The session sets ``session_replication_role = replica`` and then
        issues DELETE, UPDATE, and TRUNCATE on ``executions``.
    Then: Each raises an ``append-only`` ``DBAPIError`` and the sealed row
        survives — the event install carries the same ``ENABLE ALWAYS``
        promotion as the migration install.
    """
    wallet, order_public_id, session_id = str(uuid4()), str(uuid4()), str(uuid4())
    row_id = await _seed_execution_row(create_all_repository, wallet, order_public_id, session_id)
    await _assert_replica_role_bypass_is_rejected(create_all_repository, wallet, row_id)


@pytest.mark.asyncio
async def test_migration_built_postgres_rejects_every_mutation_including_truncate(
    migration_repository: SQLAlchemyRepository,
) -> None:
    """On the migration-built PostgreSQL ledger every mutation vector is refused.

    Given: The migration-built session database (trigger from 0030) holding
        one sealed execution row under a random wallet.
    When: Each raw mutation vector — text SQL, ORM update/delete, aliased
        delete, ``quoted_name`` injection, mass delete, and ``TRUNCATE`` /
        ``TRUNCATE CASCADE`` — is issued raw.
    Then: Each raises an ``append-only`` ``DBAPIError`` and the scope's
        sealed row survives, including through TRUNCATE which a
        ``BEFORE DELETE`` row trigger alone would not catch.
    """
    wallet, order_public_id, _instrument, session_id = _scope_identity()
    row_id = await _seed_execution_row(migration_repository, wallet, order_public_id, session_id)
    await _assert_every_mutation_is_rejected(migration_repository, wallet, row_id)


@pytest.mark.asyncio
async def test_create_all_built_postgres_rejects_every_mutation_including_truncate(
    create_all_repository: SQLAlchemyRepository,
) -> None:
    """On a create_all-built PostgreSQL ledger every mutation vector is refused.

    Given: A ``create_all``-built throwaway PostgreSQL database (trigger
        from the ``after_create`` event) holding one sealed execution row.
    When: Each raw mutation vector, including ``TRUNCATE`` variants, is
        issued raw.
    Then: Each raises an ``append-only`` ``DBAPIError`` and the sealed row
        survives — the event install matches the migration install on the
        production dialect.
    """
    wallet, order_public_id, _instrument, session_id = _scope_identity()
    row_id = await _seed_execution_row(create_all_repository, wallet, order_public_id, session_id)
    await _assert_every_mutation_is_rejected(create_all_repository, wallet, row_id)


@pytest.mark.asyncio
async def test_fenced_ingest_still_appends_on_create_all_postgres(
    create_all_repository: SQLAlchemyRepository,
) -> None:
    """Legit fenced ingest still appends and mints contiguous counters under the trigger.

    Given: A ``create_all``-built PostgreSQL ledger with the trigger and one
        active order/instrument lineage under a random wallet.
    When: Two fills are ingested through ``insert_execution``.
    Then: Both persist and mint the contiguous counters 1 and 2 — the
        trigger leaves the INSERT ingest path untouched.
    """
    wallet, order_public_id, instrument_public_id, session_id = _scope_identity()
    async with create_all_repository.session() as s:
        s.add_all(
            [
                Instrument(
                    public_id=instrument_public_id,
                    symbol_public_id=instrument_public_id,
                    exchange=_EXCHANGE,
                    timestamp=_NOW - timedelta(days=1),
                    session_id=session_id,
                    sequence_id=1,
                ),
                Order(
                    public_id=order_public_id,
                    instrument_public_id=instrument_public_id,
                    mode=_MODE,
                    wallet_public_id=wallet,
                    created_at=_NOW - timedelta(hours=2),
                    timestamp=_NOW - timedelta(hours=2),
                    side="buy",
                    order_type="limit",
                    price=4.25,
                    size=100.0,
                    status="filled",
                    session_id=session_id,
                    sequence_id=1,
                ),
            ]
        )
        await s.commit()
    for sequence_id in (1, 2):
        await create_all_repository.insert_execution(
            order_public_id=order_public_id,
            wallet_public_id=wallet,
            timestamp=_NOW - timedelta(hours=1),
            side="buy",
            status="filled",
            price=4.25,
            size=100.0,
            fee=0.5,
            fee_asset="PLN",
            session_id=session_id,
            sequence_id=sequence_id,
        )
    async with create_all_repository.session() as s:
        counters = [
            int(value)
            for value in (
                await s.execute(
                    select(Execution.scope_sequence)
                    .where(Execution.wallet_public_id == wallet)
                    .order_by(Execution.scope_sequence)
                )
            ).scalars()
        ]
    assert counters == [1, 2]
