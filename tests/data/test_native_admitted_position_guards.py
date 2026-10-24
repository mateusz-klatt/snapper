"""Actual SQLite physical guards for the strict native admitted-position graph and restore boundary."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from shutil import copy2
from typing import Literal

import pytest
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

from snapper.data.contract_accounting_guards import ContractAccountingIntegrityError
from snapper.data.contract_accounting_guards import PhysicalAccountingGuard
from snapper.data.contract_accounting_guards import require_contract_accounting_guards
from snapper.data.execution_admission_triggers import install_execution_admission_table_triggers
from snapper.data.execution_unit_snapshot_triggers import install_execution_unit_snapshot_triggers
from snapper.data.native_admitted_position_guards import NATIVE_ADMITTED_POSITION_EVIDENCE_TABLES
from snapper.data.native_admitted_position_guards import (
    acquire_native_admitted_position_restore_barrier,
)
from snapper.data.native_admitted_position_guards import require_native_admitted_position_guards
from snapper.data.native_contract_accounting_guards import require_native_contract_accounting_guards

_ALL_TABLES = (
    "accounting_hold_resolutions",
    "accounting_native_source_claims",
    "accounting_raw_receipts",
    "accounting_scope_holds",
    "contract_accounting_anchors",
    "contract_accounting_events",
    "execution_unit_snapshots",
    "financial_publication_deliveries",
    "financial_publication_intents",
    "native_account_authorities",
    "native_account_identity_proofs",
    "native_account_read_attempts",
    "native_account_read_outcomes",
    "private_source_ownership_receipts",
    "submission_unit_admissions",
)


@asynccontextmanager
async def _database(template: Path, directory: Path) -> AsyncIterator[AsyncEngine]:
    """Use an independently owned copy of the actual complete closed migration fixture."""
    target = directory / "native-admitted-guards.db"
    copy2(template, target)
    engine = create_async_engine(f"sqlite+aiosqlite:///{target}")
    try:
        yield engine
    finally:
        await engine.dispose()


async def _check(session: AsyncSession, restore: bool) -> tuple[PhysicalAccountingGuard, ...]:
    """Call the actual writer or exclusive restore API without altering physical verification."""
    if restore:
        return await acquire_native_admitted_position_restore_barrier(session)
    return await require_native_admitted_position_guards(session)


def _repair(connection: Connection, table: str) -> None:
    """Reinstall only the original actual migration helper for the damaged new dependency."""
    if table == "execution_unit_snapshots":
        install_execution_unit_snapshot_triggers(connection)
    else:
        assert table == "submission_unit_admissions"
        install_execution_admission_table_triggers(connection, "submission_unit_admissions")


@pytest.mark.parametrize("restore", (False, True))
async def test_actual_admitted_closure_adds_only_units_and_admissions_to_unchanged_cash_guards(
    db_template_path: Path, tmp_path: Path, restore: bool
) -> None:
    """Strict admitted-position protection extends physical closure without changing older APIs.

    Given: A private copy of the actual complete migrated SQLite catalog,
    When: Legacy, native cash and admitted-position boundaries inspect the same real transaction,
    Then: They return sixteen, twenty-six and thirty exact guards over the expected relations.
    """
    async with (
        _database(db_template_path, tmp_path) as engine,
        async_sessionmaker(engine)() as session,
    ):
        await session.begin()
        legacy = await require_contract_accounting_guards(session, include_publications=True)
        native = await require_native_contract_accounting_guards(session)
        admitted = await _check(session, restore)
        assert len(legacy) == 16 and len(native) == 26 and len(admitted) == 30
        assert tuple(dict.fromkeys(guard.table_name for guard in admitted)) == _ALL_TABLES
        assert set(native).issubset(set(admitted))
        assert {guard.table_name for guard in admitted} - {
            guard.table_name for guard in native
        } == set(NATIVE_ADMITTED_POSITION_EVIDENCE_TABLES)
        assert "execution_route_authorities" not in {guard.table_name for guard in admitted}
        assert all(len(guard.definition_digest) == 64 for guard in admitted)


@pytest.mark.parametrize("table", _ALL_TABLES)
@pytest.mark.parametrize("restore", (False, True))
async def test_actual_missing_guard_in_any_of_fifteen_relations_refuses_writer_and_restore(
    db_template_path: Path, tmp_path: Path, table: str, restore: bool
) -> None:
    """Every relation in the native admitted graph is required at both public boundaries.

    Given: The original fifteen-relation physical catalog in an independently owned database,
    When: One actual delete guard is dropped and committed,
    Then: Both writer and restore APIs identify the unprotected target and refuse their proof.
    """
    async with (
        _database(db_template_path, tmp_path) as engine,
        async_sessionmaker(engine)() as session,
    ):
        await session.execute(text(f"DROP TRIGGER {table}_reject_delete"))
        await session.commit()
        await session.begin()
        with pytest.raises(ContractAccountingIntegrityError, match=table + "_reject_delete"):
            await _check(session, restore)


@pytest.mark.parametrize("table", NATIVE_ADMITTED_POSITION_EVIDENCE_TABLES)
@pytest.mark.parametrize("operation", ("UPDATE", "DELETE"))
@pytest.mark.parametrize("damage", ("permissive", "foreign_table", "conditional"))
async def test_actual_new_dependency_guard_cannot_borrow_its_name_from_relaxed_sql(
    db_template_path: Path,
    tmp_path: Path,
    table: str,
    operation: str,
    damage: str,
) -> None:
    """Familiar trigger names cannot replace exact original unit or admission mutation semantics.

    Given: Older native cash guards remain intact in an actual private SQLite catalog,
    When: A new dependency guard becomes permissive, conditional or attached to another table,
    Then: Cash still verifies twenty-six guards while admitted-position verification refuses.
    """
    async with (
        _database(db_template_path, tmp_path) as engine,
        async_sessionmaker(engine)() as session,
    ):
        trigger = f"{table}_reject_{operation.lower()}"
        await session.execute(text(f"DROP TRIGGER {trigger}"))
        actual_table = table
        if damage == "foreign_table":
            actual_table = "unrelated_guard_fixture"
            await session.execute(text(f"CREATE TABLE {actual_table} (id INTEGER)"))
        condition = " WHEN OLD.id > 0" if damage == "conditional" else ""
        body = (
            f"SELECT RAISE(ABORT, '{table} is append-only: {operation} forbidden');"
            if damage == "conditional"
            else "SELECT 1;"
        )
        await session.execute(
            text(
                f"CREATE TRIGGER {trigger} BEFORE {operation} ON {actual_table}{condition} BEGIN {body} END"
            )
        )
        await session.commit()
        await session.begin()
        assert len(await require_native_contract_accounting_guards(session)) == 26
        with pytest.raises(ContractAccountingIntegrityError, match=trigger):
            await require_native_admitted_position_guards(session)


@pytest.mark.parametrize("table", NATIVE_ADMITTED_POSITION_EVIDENCE_TABLES)
@pytest.mark.parametrize("relation", ("TABLE", "VIEW"))
@pytest.mark.parametrize("restore", (False, True))
async def test_actual_temporary_unit_or_admission_relation_cannot_borrow_persistent_guards(
    db_template_path: Path, tmp_path: Path, table: str, relation: str, restore: bool
) -> None:
    """Connection-local table and view shadows cannot substitute for durable unit or admission rows.

    Given: Every persistent physical guard remains original and enabled,
    When: A real connection creates a temporary relation with a protected unit or admission name,
    Then: Writer and restore APIs refuse before returning persistent-looking source proof.
    """
    async with (
        _database(db_template_path, tmp_path) as engine,
        async_sessionmaker(engine)() as session,
    ):
        shape = "(id INTEGER)" if relation == "TABLE" else "AS SELECT 1 AS id"
        await session.execute(text(f"CREATE TEMP {relation} {table} {shape}"))
        assert len(await require_native_contract_accounting_guards(session)) == 26
        with pytest.raises(ContractAccountingIntegrityError, match="temporary_relation_shadow"):
            await _check(session, restore)


@pytest.mark.parametrize("table", NATIVE_ADMITTED_POSITION_EVIDENCE_TABLES)
@pytest.mark.parametrize("restore", (False, True))
async def test_actual_admitted_guard_reservation_blocks_other_connection_ddl_until_rollback(
    db_template_path: Path, tmp_path: Path, table: str, restore: bool
) -> None:
    """Physical evidence remains protected for the entire caller-owned transaction.

    Given: A real deferred SQLite caller has performed only a read,
    When: The admitted boundary reserves writes and another connection attempts guard removal,
    Then: Removal is blocked until the caller rolls back and releases its actual reservation.
    """
    async with _database(db_template_path, tmp_path) as engine:
        sessions = async_sessionmaker(engine)
        async with sessions() as protected:
            await protected.execute(text("SELECT 1"))
            assert len(await _check(protected, restore)) == 30
            async with sessions() as other:
                await other.execute(text("PRAGMA busy_timeout=100"))
                with pytest.raises(OperationalError, match="locked"):
                    await other.execute(text(f"DROP TRIGGER {table}_reject_update"))
                await other.rollback()
            await protected.rollback()
        async with sessions() as other:
            await other.execute(text(f"DROP TRIGGER {table}_reject_update"))
            await other.commit()


@pytest.mark.parametrize("restore", (False, True))
async def test_actual_admitted_boundary_requires_an_existing_caller_transaction(
    db_template_path: Path, tmp_path: Path, restore: bool
) -> None:
    """Physical verification cannot create its own transaction behind a caller's back.

    Given: A real SQLite AsyncSession with no active transaction,
    When: Its writer or restore boundary is invoked,
    Then: It refuses before autobegin and leaves the actual session without a transaction.
    """
    async with (
        _database(db_template_path, tmp_path) as engine,
        async_sessionmaker(engine)() as session,
    ):
        assert not session.in_transaction()
        with pytest.raises(RuntimeError, match="caller_transaction"):
            await _check(session, restore)
        assert not session.in_transaction()


@pytest.mark.parametrize("damage", ("guards", "shadow"))
@pytest.mark.parametrize("restore", (False, True))
async def test_untraversed_legacy_route_relation_is_not_added_to_strict_native_v3_closure(
    db_template_path: Path,
    tmp_path: Path,
    damage: Literal["guards", "shadow"],
    restore: bool,
) -> None:
    """Strict native position v3 does not borrow or require a legacy route-authority family.

    Given: All original native source, unit, admission and financial guards remain intact,
    When: Untraversed legacy route guards are removed or their table is locally shadowed,
    Then: Writer and restore still verify exactly the same thirty native admitted-position guards.
    """
    async with (
        _database(db_template_path, tmp_path) as engine,
        async_sessionmaker(engine)() as session,
    ):
        if damage == "guards":
            await session.execute(text("DROP TRIGGER execution_route_authorities_reject_update"))
            await session.execute(text("DROP TRIGGER execution_route_authorities_reject_delete"))
        else:
            await session.execute(
                text("CREATE TEMP TABLE execution_route_authorities (id INTEGER)")
            )
        assert len(await _check(session, restore)) == 30
        assert len(await require_native_contract_accounting_guards(session)) == 26


@pytest.mark.parametrize("table", NATIVE_ADMITTED_POSITION_EVIDENCE_TABLES)
async def test_actual_original_ddl_reinstallation_restores_physical_preconditions_only(
    db_template_path: Path, tmp_path: Path, table: str
) -> None:
    """Restore can recover physical protection without inventing a semantic-history audit result.

    Given: An actual unit or admission delete guard has been dropped,
    When: The public restore boundary refuses and the original migration helper reinstalls that guard,
    Then: The same transaction verifies thirty physical proofs without changing retained evidence.
    """
    async with (
        _database(db_template_path, tmp_path) as engine,
        async_sessionmaker(engine)() as session,
    ):
        await session.execute(text(f"DROP TRIGGER {table}_reject_delete"))
        with pytest.raises(ContractAccountingIntegrityError, match="physical_guard_invalid"):
            await acquire_native_admitted_position_restore_barrier(session)
        connection = await session.connection()
        await connection.run_sync(_repair, table)
        assert len(await acquire_native_admitted_position_restore_barrier(session)) == 30
