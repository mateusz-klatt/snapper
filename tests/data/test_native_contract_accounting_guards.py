"""Check actual native SQLite guard closure and PostgreSQL boundary command sequencing.

PostgreSQL metadata operands here are explicit mocks. A separate owned live
PostgreSQL witness must prove catalog, relation locking and barrier behavior.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from shutil import copy2
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

from snapper.data.contract_accounting_guards import ACCOUNTING_STARTUP_LOCK_DOMAIN
from snapper.data.contract_accounting_guards import ACCOUNTING_STARTUP_LOCK_NAMESPACE
from snapper.data.contract_accounting_guards import ContractAccountingIntegrityError
from snapper.data.contract_accounting_guards import PhysicalAccountingGuard
from snapper.data.contract_accounting_guards import require_contract_accounting_guards
from snapper.data.native_contract_accounting_guards import NATIVE_PROVENANCE_TABLE_NAMES
from snapper.data.native_contract_accounting_guards import _native_specifications
from snapper.data.native_contract_accounting_guards import (
    acquire_native_contract_accounting_restore_barrier,
)
from snapper.data.native_contract_accounting_guards import require_native_contract_accounting_guards


@asynccontextmanager
async def _database(template: Path, directory: Path) -> AsyncIterator[AsyncEngine]:
    """Copy the real closed complete schema to an independently owned file and close every pool."""
    target = directory / "native-guards.db"
    copy2(template, target)
    engine = create_async_engine(f"sqlite+aiosqlite:///{target}")
    try:
        yield engine
    finally:
        await engine.dispose()


async def _check(session: AsyncSession, restore: bool) -> tuple[PhysicalAccountingGuard, ...]:
    """Exercise only the two public native boundaries without altering their lock behavior."""
    if restore:
        return await acquire_native_contract_accounting_restore_barrier(session)
    return await require_native_contract_accounting_guards(session)


def _metadata() -> list[dict[str, object]]:
    """Supply all exact PostgreSQL catalog properties without claiming any live SQL execution."""
    return [
        {
            "table_name": specification.table_name,
            "trigger_name": f"{specification.table_name}_reject_{suffix}",
            "enabled": "A",
            "relation_kind": "r",
            "persistence": "p",
            "trigger_type": trigger_type,
            "internal": False,
            "deferrable": False,
            "conditional": False,
            "whole_table": True,
            "no_arguments": True,
            "function_name": specification.function_name,
            "function_body": specification.function_body,
            "function_in_table_schema": True,
            "function_arguments": 0,
            "returns_trigger": True,
            "language": "plpgsql",
            "security_definer": False,
        }
        for specification in _native_specifications()
        for suffix, trigger_type in (("row_mutation", 27), ("truncate", 34))
    ]


def _session(restore: bool, isolation: object = "read committed") -> tuple[AsyncSession, AsyncMock]:
    """Expose the strict SQLAlchemy metadata boundary with actual source-derived guard operands."""
    binding = MagicMock()
    binding.dialect.name = "postgresql"
    mode = MagicMock()
    mode.scalar_one.return_value = isolation
    result = MagicMock()
    result.mappings.return_value = _metadata()
    prefix = (
        [mode, MagicMock(), mode, MagicMock(), MagicMock()]
        if restore
        else [mode, MagicMock(), MagicMock()]
    )
    execute = AsyncMock(side_effect=[*prefix, result])
    session = MagicMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    session.get_bind.return_value = binding
    session.execute = execute
    return cast(AsyncSession, session), execute


@pytest.mark.parametrize("restore", (False, True))
async def test_actual_native_sqlite_closure_preserves_legacy_counts(
    db_template_path: Path, tmp_path: Path, restore: bool
) -> None:
    """Native proof dependencies extend the physical boundary without changing legacy semantics.

    Given: A private copy of the actual complete schema with all migration-owned guards,
    When: The native public writer or restore boundary verifies its catalog,
    Then: It returns thirteen tables and twenty-six guards while the legacy boundary remains sixteen.
    """
    async with (
        _database(db_template_path, tmp_path) as engine,
        async_sessionmaker(engine)() as session,
    ):
        await session.begin()
        legacy = await require_contract_accounting_guards(session, include_publications=True)
        guards = await _check(session, restore)
        assert len(legacy) == 16
        assert len(guards) == 26
        assert len({guard.table_name for guard in guards}) == 13
        assert {guard.table_name for guard in guards} - {
            guard.table_name for guard in legacy
        } == set(NATIVE_PROVENANCE_TABLE_NAMES)
        assert not {"futures_product_read_attempts", "futures_product_read_outcomes"} & {
            guard.table_name for guard in guards
        }
        assert all(len(guard.definition_digest) == 64 for guard in guards)


@pytest.mark.parametrize("table", NATIVE_PROVENANCE_TABLE_NAMES)
@pytest.mark.parametrize("damage", ("missing", "permissive", "foreign_table"))
async def test_actual_native_source_guard_damage_cannot_borrow_legacy_protection(
    db_template_path: Path, tmp_path: Path, table: str, damage: str
) -> None:
    """Every newly required provenance relation retains its own exact physical mutation rule.

    Given: All older financial guards remain valid in an actual private schema,
    When: A native source guard is removed, made permissive or attached to a different table,
    Then: Legacy checking remains valid while the native boundary refuses the missing protection.
    """
    async with (
        _database(db_template_path, tmp_path) as engine,
        async_sessionmaker(engine)() as session,
    ):
        trigger = f"{table}_reject_delete"
        await session.execute(text(f"DROP TRIGGER {trigger}"))
        if damage == "permissive":
            await session.execute(
                text(f"CREATE TRIGGER {trigger} BEFORE DELETE ON {table} BEGIN SELECT 1; END")
            )
        elif damage == "foreign_table":
            await session.execute(text("CREATE TABLE unrelated_restore_fixture (id INTEGER)"))
            await session.execute(
                text(
                    f"CREATE TRIGGER {trigger} BEFORE DELETE ON unrelated_restore_fixture "
                    "BEGIN SELECT 1; END"
                )
            )
        await session.commit()
        await session.begin()
        assert (
            len(await require_contract_accounting_guards(session, include_publications=True)) == 16
        )
        with pytest.raises(ContractAccountingIntegrityError, match="physical_guard_invalid"):
            await require_native_contract_accounting_guards(session)


@pytest.mark.parametrize("table", NATIVE_PROVENANCE_TABLE_NAMES)
async def test_actual_native_temporary_relation_cannot_shadow_persistent_guard_proof(
    db_template_path: Path, tmp_path: Path, table: str
) -> None:
    """Main-table guards cannot establish the identity of a connection-local shadow relation.

    Given: Every persistent native lineage guard remains intact,
    When: The actual connection creates a temporary relation with one protected source table name,
    Then: Native checking refuses that shadow before interpreting a financial source graph.
    """
    async with (
        _database(db_template_path, tmp_path) as engine,
        async_sessionmaker(engine)() as session,
    ):
        await session.execute(text(f"CREATE TEMP TABLE {table} (id INTEGER)"))
        with pytest.raises(ContractAccountingIntegrityError, match="temporary_relation_shadow"):
            await require_native_contract_accounting_guards(session)


@pytest.mark.parametrize("table", NATIVE_PROVENANCE_TABLE_NAMES)
async def test_actual_native_guard_check_holds_schema_reservation_until_rollback(
    db_template_path: Path, tmp_path: Path, table: str
) -> None:
    """A successful check remains protected from competing DDL until its caller completes.

    Given: A deferred SQLite transaction has only performed a read,
    When: The native boundary takes its real reservation and another connection drops a source guard,
    Then: The DDL is blocked until rollback releases the same actual write reservation.
    """
    async with _database(db_template_path, tmp_path) as engine:
        sessions = async_sessionmaker(engine)
        async with sessions() as protected:
            await protected.execute(text("SELECT 1"))
            assert len(await require_native_contract_accounting_guards(protected)) == 26
            async with sessions() as ddl:
                await ddl.execute(text("PRAGMA busy_timeout=100"))
                with pytest.raises(OperationalError, match="locked"):
                    await ddl.execute(text(f"DROP TRIGGER {table}_reject_delete"))
                await ddl.rollback()
            await protected.rollback()
        async with sessions() as ddl:
            await ddl.execute(text(f"DROP TRIGGER {table}_reject_delete"))
            await ddl.commit()


@pytest.mark.parametrize("restore", (False, True))
async def test_native_boundary_refuses_without_a_real_caller_transaction(restore: bool) -> None:
    """A caller cannot obtain physical evidence by allowing source reads to autobegin a transaction.

    Given: An otherwise valid session boundary reports no active caller transaction,
    When: Either native public entrypoint is invoked,
    Then: It refuses before executing any SQL.
    """
    session, execute = _session(restore)
    cast(MagicMock, session).in_transaction.return_value = False
    with pytest.raises(RuntimeError, match="caller_transaction"):
        await _check(session, restore)
    execute.assert_not_called()


@pytest.mark.parametrize("restore", (False, True))
async def test_postgres_native_boundary_uses_same_global_key_and_one_sorted_relation_set(
    restore: bool,
) -> None:
    """The native catalog extension retains global writer ordering and deterministic relation locks.

    Given: Complete strict mocked PostgreSQL metadata derived from the actual helper definitions,
    When: Either native public boundary performs its commands,
    Then: Actual isolation checking precedes the original global key and one sorted thirteen-table lock.
    """
    session, execute = _session(restore)
    guards = await _check(session, restore)
    assert len(guards) == 26
    commands = [str(call.args[0]) for call in execute.call_args_list]
    assert commands[0] == "SHOW transaction_isolation"
    assert ("pg_advisory_xact_lock(" if restore else "pg_advisory_xact_lock_shared(") in commands[1]
    assert execute.call_args_list[1].args[1] == {
        "namespace": ACCOUNTING_STARTUP_LOCK_NAMESPACE,
        "domain": ACCOUNTING_STARTUP_LOCK_DOMAIN,
    }
    locks = [command for command in commands if command.startswith("LOCK TABLE ")]
    tables = [item.table_name for item in _native_specifications()]
    assert tables == sorted(set(tables)) and len(tables) == 13
    assert locks == [
        "LOCK TABLE " + ", ".join(f'"{table}"' for table in tables) + " IN ROW EXCLUSIVE MODE"
    ]


@pytest.mark.parametrize("restore", (False, True))
@pytest.mark.parametrize("isolation", ("repeatable read", "serializable", "autocommit", None))
async def test_native_postgres_snapshot_precondition_precedes_any_barrier_wait(
    restore: bool, isolation: object
) -> None:
    """Adding source relations cannot bypass the proven PostgreSQL snapshot isolation boundary.

    Given: The actual SQLAlchemy metadata boundary reports unsupported or unknown isolation,
    When: Either native public entrypoint begins,
    Then: It refuses after SHOW and before advisory waits, relation locks or source interpretation.
    """
    session, execute = _session(restore, isolation)
    with pytest.raises(RuntimeError, match="READ COMMITTED"):
        await _check(session, restore)
    assert [str(call.args[0]) for call in execute.call_args_list] == ["SHOW transaction_isolation"]
