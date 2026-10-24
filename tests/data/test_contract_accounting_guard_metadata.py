"""Verify strict PostgreSQL catalog interpretation separately from actual live-PG witnesses."""

from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.contract_accounting_guards import ACCOUNTING_STARTUP_LOCK_DOMAIN
from snapper.data.contract_accounting_guards import ACCOUNTING_STARTUP_LOCK_NAMESPACE
from snapper.data.contract_accounting_guards import ContractAccountingIntegrityError
from snapper.data.contract_accounting_guards import _specifications
from snapper.data.contract_accounting_guards import acquire_contract_accounting_restore_barrier
from snapper.data.contract_accounting_guards import acquire_contract_accounting_write_barrier
from snapper.data.contract_accounting_guards import require_contract_accounting_guards


def _metadata(include_publications: bool = False) -> list[dict[str, object]]:
    """Provide complete typed catalog operands for the fixed migration-owned guard definitions."""
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
        for specification in _specifications(include_publications)
        for suffix, trigger_type in (("row_mutation", 27), ("truncate", 34))
    ]


def _session(
    rows: list[dict[str, object]],
    dialect: str = "postgresql",
    *,
    restore: bool = False,
    isolation: object = "read committed",
) -> tuple[AsyncSession, AsyncMock]:
    """Expose the SQLAlchemy metadata boundary without pretending to execute PostgreSQL SQL."""
    binding = MagicMock()
    binding.dialect.name = dialect
    result = MagicMock()
    result.mappings.return_value = rows
    mode = MagicMock()
    mode.scalar_one.return_value = isolation
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


@pytest.mark.parametrize("include_publications", [False, True])
async def test_postgres_metadata_check_retains_fixed_order_locks_and_exact_guard_evidence(
    include_publications: bool,
) -> None:
    """Inspect fixed PostgreSQL lock and metadata commands without claiming live SQL proof.

    Given: Catalog operands describe every actual expected persistent unconditional ALWAYS guard.
    When: The strict checker receives them through the SQLAlchemy boundary.
    Then: It returns exact guard evidence and requests shared startup then stable-order relation locks.
    """
    session, execute = _session(_metadata(include_publications))
    guards = await require_contract_accounting_guards(
        session, include_publications=include_publications
    )
    assert len(guards) == (16 if include_publications else 12)
    assert all(len(guard.definition_digest) == 64 for guard in guards)
    commands = [str(call.args[0]) for call in execute.call_args_list]
    assert commands[0] == "SHOW transaction_isolation"
    assert "pg_advisory_xact_lock_shared" in commands[1]
    assert commands[2].startswith("LOCK TABLE ")
    assert commands[2].endswith(" IN ROW EXCLUSIVE MODE")
    tables = [specification.table_name for specification in _specifications(include_publications)]
    assert commands[2] == (
        "LOCK TABLE "
        + ", ".join(f'"{table}"' for table in sorted(tables))
        + " IN ROW EXCLUSIVE MODE"
    )
    assert "to_regclass" in commands[3]
    assert "c.relpersistence" in commands[3]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("enabled", "O"),
        ("enabled", "D"),
        ("relation_kind", "v"),
        ("persistence", "t"),
        ("persistence", "u"),
        ("trigger_type", 25),
        ("internal", True),
        ("deferrable", True),
        ("conditional", True),
        ("whole_table", False),
        ("no_arguments", False),
        ("function_name", "permissive_function"),
        ("function_body", "BEGIN RETURN NEW; END;"),
        ("function_in_table_schema", False),
        ("function_arguments", 1),
        ("returns_trigger", False),
        ("language", "internal"),
        ("security_definer", True),
    ],
)
async def test_relaxed_postgres_guard_metadata_cannot_grant_financial_write_preconditions(
    field: str, value: object
) -> None:
    """Reject weakened catalog evidence independently of table names or caller assertions.

    Given: Every required PostgreSQL guard initially has strict expected metadata.
    When: One property changes to a temporary, disabled, conditional or permissive form.
    Then: The precondition refuses rather than accepting an incomplete immutability claim.
    """
    rows = _metadata()
    rows[0][field] = value
    session, _ = _session(rows)
    with pytest.raises(ContractAccountingIntegrityError, match="physical_guard_invalid"):
        await require_contract_accounting_guards(session)


@pytest.mark.parametrize("change", ["absent", "foreign_table"])
async def test_expected_guard_identity_must_belong_to_the_actual_target_relation(
    change: str,
) -> None:
    """Keep familiar trigger identities attached to their exact protected relation.

    Given: The expected native-accounting catalog inventory is complete.
    When: A required trigger is absent or is attached to a different relation.
    Then: The actual target guard remains unproven and financial admission refuses.
    """
    rows = _metadata()
    if change == "absent":
        rows.pop(0)
    else:
        rows[0]["table_name"] = "foreign_guarded_table"
    session, _ = _session(rows)
    with pytest.raises(ContractAccountingIntegrityError, match="physical_guard_invalid"):
        await require_contract_accounting_guards(session)


async def test_unknown_backend_cannot_skip_the_actual_write_barrier() -> None:
    """Refuse a backend whose physical startup and DDL boundary is unimplemented.

    Given: A caller has a logical session on an unsupported database dialect.
    When: It requests the shared write barrier.
    Then: The operation refuses without issuing a readiness-looking metadata query.
    """
    session, execute = _session([], "unsupported_backend")
    with pytest.raises(NotImplementedError, match="write barrier unsupported"):
        await acquire_contract_accounting_write_barrier(session)
    execute.assert_not_awaited()


async def test_exclusive_restore_phase_uses_the_exact_shared_writer_identity() -> None:
    """Inspect startup key identity independently of the separate actual PostgreSQL lock witness.

    Given: A complete catalog describes all guarded journal and outbox relations.
    When: The real thin startup phase requests its exclusive barrier and physical checks.
    Then: Exclusive and shared requests use the exact exported domain before metadata locks.
    """
    session, execute = _session(_metadata(True), restore=True)
    guards = await acquire_contract_accounting_restore_barrier(session)
    assert len(guards) == 16
    commands = [str(call.args[0]) for call in execute.call_args_list]
    assert commands[0] == "SHOW transaction_isolation"
    assert "pg_advisory_xact_lock(" in commands[1]
    assert commands[2] == "SHOW transaction_isolation"
    assert "pg_advisory_xact_lock_shared(" in commands[3]
    parameters = {
        "namespace": ACCOUNTING_STARTUP_LOCK_NAMESPACE,
        "domain": ACCOUNTING_STARTUP_LOCK_DOMAIN,
    }
    assert execute.call_args_list[1].args[1] == parameters
    assert execute.call_args_list[3].args[1] == parameters


@pytest.mark.parametrize("isolation", ["repeatable read", "serializable", "read uncommitted", None])
@pytest.mark.parametrize("operation", ["writer", "restore", "claim_guard"])
async def test_actual_isolation_is_checked_before_any_advisory_wait_or_catalog_read(
    isolation: object, operation: str
) -> None:
    """Refuse stale or unsupported transaction isolation before waiting for financial writers.

    Given: The public session boundary reports an isolation other than READ COMMITTED.
    When: A writer, startup audit or claim guard requests its actual barrier.
    Then: It refuses after one server mode read without switching snapshots or acquiring locks.
    """
    session, execute = _session([], restore=operation == "restore", isolation=isolation)
    with pytest.raises(RuntimeError, match="READ COMMITTED"):
        if operation == "writer":
            await acquire_contract_accounting_write_barrier(session)
        elif operation == "restore":
            await acquire_contract_accounting_restore_barrier(session)
        else:
            await require_contract_accounting_guards(session)
    assert execute.await_count == 1
    assert str(execute.call_args.args[0]) == "SHOW transaction_isolation"
