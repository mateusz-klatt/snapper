"""Adversarial proof that the annulment manifest is physically append-only.

A correction manifest that could be re-pointed, re-reasoned, or quietly deleted
would not be evidence: deleting one row silently restores a repudiated
execution to effective accounting history, and updating one silently changes
WHICH execution a committed correction repudiates. The Python-layer
``_guard_immutable_ledger`` now refuses generic primitives aimed at the table by
physical name, but a target-parameter guard cannot reach raw ``text()`` SQL, an
ORM-enabled ``update``/``delete`` issued straight on a session, an aliased
delete, or a ``quoted_name(quote=False)`` identifier injection. This module
issues every one of those vectors RAW on the session — so only the physical
trigger can be doing the rejecting — against BOTH a ``create_all``-built
database (the ``after_create`` event installs the triggers at table creation)
AND a migration-built database (migration 0037 installs them on the table it
just created), exactly as the executions ledger is proven.

Liveness and non-interference are proven alongside rejection: INSERT still
appends, and the executions ledger's own triggers still reject their own
mutation vectors on a database that carries BOTH tables, so adding the manifest
plane did not weaken the ledger it corrects.

The PostgreSQL dialect (including its ``TRUNCATE`` trigger, which SQLite lacks)
is pinned by the statement-protocol tests in ``test_ledger_triggers`` and
exercised by ``db-init`` against the live scratch database; SQLite here always
runs under the default ``make test`` / ``make check-all``.
"""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
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

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Execution
from snapper.data.models import ExecutionAnnulment
from snapper.data.models import ExecutionAnnulmentVisibility
from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 7, 25, 8, 0, tzinfo=UTC)
_WALLET = "0000face-0000-7000-8000-00000000c101"
_TARGET = "0000face-0000-7000-8000-00000000e101"
_USER = "0000face-0000-7000-8000-00000000d101"
_ORDER = "00000000-0000-7000-8000-000000000601"
_SESSION = "00000000-0000-7000-8000-000000000301"
_DIGEST = "b" * 64
_EVIDENCE = '{"diagnosis":"no fill_observed witness"}'
_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_SQLITE_TRIGGER_QUERY = (
    "SELECT name FROM sqlite_master WHERE type='trigger' "
    "AND tbl_name='execution_annulments' ORDER BY name"
)


@pytest.fixture
async def create_all_repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Build a repository whose schema comes from ``Base.metadata.create_all``.

    This is the path every test and in-memory database takes; the
    ``after_create`` event installs the manifest triggers at table creation, so
    no migration is involved.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'create-all.db'}")
    await repository.create_all()
    try:
        yield repository
    finally:
        await repository.engine.dispose()


@pytest.fixture
async def migration_repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Build a repository whose schema comes from Alembic migrations to head.

    This mirrors the production table: created and extended by migrations, with
    the manifest triggers installed by migration 0037 rather than by
    ``create_all``.
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


async def _seed_annulment_row(repository: SQLAlchemyRepository) -> int:
    """Insert one committed manifest row directly (INSERT is permitted).

    A direct ``session.add`` is a plain ``INSERT``, the only mutation the
    triggers allow, so it seeds a sealed correction to attack without going
    through the guarded writer's proof protocol. Returns the row id.
    """
    async with repository.session() as s:
        s.add(
            ExecutionAnnulment(
                target_execution_public_id=_TARGET,
                target_execution_digest=_DIGEST,
                wallet_public_id=_WALLET,
                exchange="kraken",
                mode="live",
                scope_sequence=1,
                annulled_by_user_public_id=_USER,
                correction_time=_NOW,
                reason="unwitnessed_phantom",
                evidence_json=_EVIDENCE,
                timestamp=_NOW,
                known_to=KNOWN_TO_MAX,
                session_id=_SESSION,
                sequence_id=1,
            )
        )
        await s.commit()
        return int((await s.execute(select(ExecutionAnnulment.id))).scalar_one())


def _mutation_vectors(row_id: int) -> dict[str, Executable]:
    """Build every raw mutation vector that must be physically rejected.

    Each vector bypasses the Python-guarded primitives — raw ``text()`` SQL, an
    ORM-enabled ``update``/``delete`` on the session, an aliased delete, and a
    ``quoted_name(quote=False)`` identifier injection — so a rejection can only
    come from the physical trigger.
    """
    injected = table(
        quoted_name("execution_annulments", quote=False),
        column("id"),
        column("reason"),
    )
    return {
        "raw text UPDATE": text(
            "UPDATE execution_annulments SET reason = 'unwitnessed_legacy_lineage' WHERE id = :id"
        ).bindparams(id=row_id),
        "raw text DELETE": text("DELETE FROM execution_annulments WHERE id = :id").bindparams(
            id=row_id
        ),
        "ORM update": update(ExecutionAnnulment)
        .where(ExecutionAnnulment.id == row_id)
        .values(target_execution_public_id="0000face-0000-7000-8000-00000000e999"),
        "ORM delete": delete(ExecutionAnnulment).where(ExecutionAnnulment.id == row_id),
        "aliased delete": delete(aliased(ExecutionAnnulment)),
        "quoted_name delete": delete(injected).where(injected.c.id == row_id),
        "quoted_name update": update(injected)
        .where(injected.c.id == row_id)
        .values(reason="unwitnessed_legacy_lineage"),
        "no-WHERE mass delete": text("DELETE FROM execution_annulments"),
        "known_to close": text(
            "UPDATE execution_annulments SET known_to = :closed WHERE id = :id"
        ).bindparams(closed="2026-07-25 09:00:00.000000", id=row_id),
    }


def _replace_vectors(row_id: int) -> dict[str, Executable]:
    """Build the two ``REPLACE`` spellings that delete a sealed manifest row.

    SQLite resolves an ``INSERT OR REPLACE`` / ``REPLACE INTO`` conflict by
    physically DELETING the conflicting row; that delete fires a ``BEFORE
    DELETE`` trigger only when ``PRAGMA recursive_triggers`` is ON, which the
    repository sets on every connection. The first vector conflicts on
    ``uq_execution_annulments_target`` (a fresh id and ``public_id`` but the same
    target — the true substitution attack, re-reasoning a committed correction);
    the second conflicts on the primary key.
    """
    columns = (
        "target_execution_public_id, target_execution_digest, wallet_public_id, exchange, "
        "mode, scope_sequence, annulled_by_user_public_id, correction_time, evidence_json, "
        "session_id, sequence_id, timestamp, known_to"
    )
    return {
        "INSERT OR REPLACE target substitution": text(
            f"INSERT OR REPLACE INTO execution_annulments (public_id, reason, {columns}) "
            f"SELECT :new_pid, 'unwitnessed_legacy_lineage', {columns} "
            "FROM execution_annulments WHERE id = :id"
        ).bindparams(id=row_id, new_pid="00000000-0000-7000-8000-0000000009a1"),
        "REPLACE INTO primary-key substitution": text(
            f"REPLACE INTO execution_annulments (id, public_id, reason, {columns}) "
            f"SELECT id, :new_pid, 'unwitnessed_legacy_lineage', {columns} "
            "FROM execution_annulments WHERE id = :id"
        ).bindparams(id=row_id, new_pid="00000000-0000-7000-8000-0000000009a2"),
    }


async def _surviving_row(repository: SQLAlchemyRepository) -> tuple[int, int, str]:
    """Return the committed row count plus the sealed row's id and reason."""
    async with repository.session() as s:
        total = int(
            (await s.execute(select(func.count()).select_from(ExecutionAnnulment))).scalar_one()
        )
        row = (await s.execute(select(ExecutionAnnulment.id, ExecutionAnnulment.reason))).one()
    return total, int(row[0]), str(row[1])


async def _assert_every_mutation_is_rejected(repository: SQLAlchemyRepository) -> None:
    """Assert every raw mutation vector is refused and the sealed row survives."""
    row_id = await _seed_annulment_row(repository)
    vectors = {**_mutation_vectors(row_id), **_replace_vectors(row_id)}
    for statement in vectors.values():
        async with repository.session() as s:
            with pytest.raises(DBAPIError, match="append-only"):
                await s.execute(statement)
                await s.commit()
        assert await _surviving_row(repository) == (1, row_id, "unwitnessed_phantom")


@pytest.mark.asyncio
async def test_create_all_built_manifest_rejects_every_raw_mutation(
    create_all_repository: SQLAlchemyRepository,
) -> None:
    """Every mutation vector is physically refused on a create_all database.

    Given: A ``create_all``-built manifest holding one sealed correction.
    When: Each raw vector — text SQL, ORM update/delete, aliased delete,
        ``quoted_name`` injection, a no-``WHERE`` mass delete, a ``known_to``
        close, and both ``REPLACE`` substitutions — is issued raw on the
        session.
    Then: Each raises an ``append-only`` ``DBAPIError`` and the sealed
        correction survives with its original id and reason.
    """
    await _assert_every_mutation_is_rejected(create_all_repository)


@pytest.mark.asyncio
async def test_migration_built_manifest_rejects_every_raw_mutation(
    migration_repository: SQLAlchemyRepository,
) -> None:
    """Every mutation vector is physically refused on a migration database.

    Given: A migration-built manifest (triggers installed by 0037) holding one
        sealed correction.
    When: Each raw mutation and ``REPLACE`` vector is issued raw on the session.
    Then: Each raises an ``append-only`` ``DBAPIError`` and the sealed
        correction survives — proving the migration install matches the
        ``after_create`` install.
    """
    await _assert_every_mutation_is_rejected(migration_repository)


@pytest.mark.asyncio
async def test_manifest_triggers_are_present_on_both_build_paths(
    create_all_repository: SQLAlchemyRepository,
    migration_repository: SQLAlchemyRepository,
) -> None:
    """Both build paths carry the two named SQLite manifest triggers.

    Given: A ``create_all``-built and a migration-built manifest.
    When: Each database's trigger catalog is read.
    Then: Both carry exactly ``execution_annulments_reject_delete`` and
        ``execution_annulments_reject_update`` — the install is present
        everywhere the table exists.
    """
    for repository in (create_all_repository, migration_repository):
        async with repository.session() as s:
            names = [str(row[0]) for row in (await s.execute(text(_SQLITE_TRIGGER_QUERY)))]
        assert names == [
            "execution_annulments_reject_delete",
            "execution_annulments_reject_update",
        ]


@pytest.mark.asyncio
async def test_executions_ledger_immutability_is_not_weakened_by_the_manifest(
    create_all_repository: SQLAlchemyRepository,
) -> None:
    """The executions ledger still refuses its own mutations beside the manifest.

    Given: A database carrying BOTH the executions ledger and the annulment
        manifest, with one sealed execution row.
    When: A raw UPDATE and a raw DELETE are issued against ``executions``.
    Then: Both are refused with an ``append-only`` error and the execution
        survives untouched — the correction plane is additive and takes nothing
        away from the ledger's own physical immutability.
    """
    async with create_all_repository.session() as s:
        s.add(
            Execution(
                order_public_id=_ORDER,
                wallet_public_id=_WALLET,
                exchange="kraken",
                mode="live",
                scope_sequence=1,
                timestamp=_NOW,
                side="buy",
                status="filled",
                price=4.25,
                size=100.0,
                fee=0.5,
                fee_asset="USD",
                session_id=_SESSION,
                sequence_id=1,
            )
        )
        await s.commit()
    for statement in (
        text("UPDATE executions SET fee = fee + 1"),
        text("DELETE FROM executions"),
    ):
        async with create_all_repository.session() as s:
            with pytest.raises(DBAPIError, match="append-only"):
                await s.execute(statement)
                await s.commit()
    async with create_all_repository.session() as s:
        assert (await s.execute(select(Execution.fee))).scalar_one() == 0.5


async def _seed_visibility_row(repository: SQLAlchemyRepository) -> int:
    """Insert one committed durability observation directly (INSERT is permitted).

    Seeds a sealed observation to attack without going through the writer's
    re-read protocol. Returns the row id.
    """
    async with repository.session() as s:
        s.add(
            ExecutionAnnulmentVisibility(
                annulment_public_id=_TARGET,
                annulment_id=1,
                observed_at=_NOW,
                wallet_public_id=_WALLET,
                exchange="kraken",
                mode="live",
                timestamp=_NOW,
                known_to=KNOWN_TO_MAX,
                session_id=_SESSION,
                sequence_id=1,
            )
        )
        await s.commit()
        return int((await s.execute(select(ExecutionAnnulmentVisibility.id))).scalar_one())


def _visibility_mutation_vectors(row_id: int) -> dict[str, Executable]:
    """Build every raw mutation vector the observation ledger must reject.

    The same bypasses the manifest suite uses — raw ``text()`` SQL, an
    ORM-enabled ``update``/``delete`` on the session, and a
    ``quoted_name(quote=False)`` identifier injection — so a rejection can only
    come from the physical trigger.
    """
    injected = table(
        quoted_name("execution_annulment_visibility", quote=False),
        column("id"),
        column("observed_at"),
    )
    backdated = "2020-01-01 00:00:00.000000"
    return {
        "raw text UPDATE": text(
            "UPDATE execution_annulment_visibility SET observed_at = :moved WHERE id = :id"
        ).bindparams(moved=backdated, id=row_id),
        "raw text DELETE": text(
            "DELETE FROM execution_annulment_visibility WHERE id = :id"
        ).bindparams(id=row_id),
        "ORM update": update(ExecutionAnnulmentVisibility)
        .where(ExecutionAnnulmentVisibility.id == row_id)
        .values(observed_at=datetime(2020, 1, 1, tzinfo=UTC)),
        "ORM delete": delete(ExecutionAnnulmentVisibility).where(
            ExecutionAnnulmentVisibility.id == row_id
        ),
        "aliased delete": delete(aliased(ExecutionAnnulmentVisibility)),
        "identifier injection": update(injected).values(observed_at=backdated),
    }


@pytest.mark.parametrize("vector", sorted(_visibility_mutation_vectors(1)))
async def test_create_all_visibility_rejects_every_raw_mutation_vector(
    create_all_repository: SQLAlchemyRepository,
    vector: str,
) -> None:
    """No raw vector can move or remove a durability observation.

    Given: One sealed observation in a ``create_all``-built database.
    When: Each raw mutation vector is issued straight on the session.
    Then: Every one is physically refused and the observed instant is unchanged.
        This is the sharpest refusal of the three planes: a movable
        ``observed_at`` would let a correction claim it was knowable before it
        was durable — forging the exact proof this ledger exists to make
        unforgeable — and a deletable one would retract a correction from a
        history already reported with it.
    """
    row_id = await _seed_visibility_row(create_all_repository)
    statement = _visibility_mutation_vectors(row_id)[vector]
    async with create_all_repository.session() as s:
        with pytest.raises(DBAPIError, match="append-only"):
            await s.execute(statement)
            await s.commit()
    async with create_all_repository.session() as s:
        assert (
            await s.execute(select(ExecutionAnnulmentVisibility.observed_at))
        ).scalar_one() == _NOW


@pytest.mark.parametrize("vector", sorted(_visibility_mutation_vectors(1)))
async def test_migrated_visibility_rejects_every_raw_mutation_vector(
    migration_repository: SQLAlchemyRepository,
    vector: str,
) -> None:
    """The migration-built observation ledger refuses exactly as the ORM-built one.

    Given: One sealed observation in a database built by Alembic to head, where
        migration 0039 installed the triggers rather than ``after_create``.
    When: Each raw mutation vector is issued straight on the session.
    Then: Every one is physically refused. Both install paths call the same
        shared installer, so this proves the production table carries the same
        physical refusal the test fixtures do.
    """
    row_id = await _seed_visibility_row(migration_repository)
    statement = _visibility_mutation_vectors(row_id)[vector]
    async with migration_repository.session() as s:
        with pytest.raises(DBAPIError, match="append-only"):
            await s.execute(statement)
            await s.commit()
    async with migration_repository.session() as s:
        assert (
            await s.execute(select(ExecutionAnnulmentVisibility.observed_at))
        ).scalar_one() == _NOW


async def test_visibility_insert_still_appends(
    create_all_repository: SQLAlchemyRepository,
) -> None:
    """Liveness: the observation ledger still accepts the one mutation it needs.

    Given: A database carrying the observation triggers.
    When: Two observations for different corrections are appended.
    Then: Both persist. The refusal is total for UPDATE and DELETE and absent
        for INSERT, which is what an append-only proof plane requires.
    """
    await _seed_visibility_row(create_all_repository)
    async with create_all_repository.session() as s:
        s.add(
            ExecutionAnnulmentVisibility(
                annulment_public_id="0000face-0000-7000-8000-00000000e102",
                annulment_id=2,
                observed_at=_NOW,
                wallet_public_id=_WALLET,
                exchange="kraken",
                mode="live",
                timestamp=_NOW,
                known_to=KNOWN_TO_MAX,
                session_id=_SESSION,
                sequence_id=2,
            )
        )
        await s.commit()
    async with create_all_repository.session() as s:
        count = await s.scalar(select(func.count()).select_from(ExecutionAnnulmentVisibility))
    assert count == 2
