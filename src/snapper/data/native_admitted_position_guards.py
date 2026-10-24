"""Require physical immutability for the strict native admitted-position v3 evidence graph.

This boundary adds original execution-unit snapshots and submission admissions
only to the existing native thirteen-relation closure. Native v3 does not read
legacy execution-route authorities. Existing native cash guards stay unchanged;
physical proof remains separate from retained-graph validation and activation.
"""

from typing import Literal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.contract_accounting_guards import ACCOUNTING_STARTUP_LOCK_DOMAIN
from snapper.data.contract_accounting_guards import ACCOUNTING_STARTUP_LOCK_NAMESPACE
from snapper.data.contract_accounting_guards import PhysicalAccountingGuard
from snapper.data.contract_accounting_guards import _postgres_guards
from snapper.data.contract_accounting_guards import _require_postgres_read_committed
from snapper.data.contract_accounting_guards import _sqlite_guards
from snapper.data.contract_accounting_guards import _TableGuards
from snapper.data.contract_accounting_guards import acquire_contract_accounting_write_barrier
from snapper.data.execution_unit_snapshot_triggers import _PG_INSTALL as SNAPSHOT_POSTGRES_DDL
from snapper.data.execution_unit_snapshot_triggers import _SQLITE_INSTALL as SNAPSHOT_SQLITE_DDL
from snapper.data.native_contract_accounting_guards import _native_specifications

NATIVE_ADMITTED_POSITION_GUARD_POLICY = "native_admitted_position_v3"
NATIVE_ADMITTED_POSITION_EVIDENCE_TABLES = (
    "execution_unit_snapshots",
    "submission_unit_admissions",
)


_ADMISSION_TABLE: Literal["submission_unit_admissions"] = "submission_unit_admissions"


def _submission_admission_specification() -> _TableGuards:
    """Describe frozen admission guards without changing their canonical installer.

    These literals are verifier metadata, never an alternative installer. Actual
    SQLite and PostgreSQL migration-catalog tests must bind them to the unchanged
    canonical installer before this new verifier can be accepted.

    Returns:
        Expected guards for the one fixed original submission-admission table.
    """
    return _TableGuards(
        _ADMISSION_TABLE,
        "submission_unit_admissions_reject_mutation",
        (
            " BEGIN RAISE EXCEPTION "
            "'submission_unit_admissions is append-only: % forbidden', TG_OP "
            "USING ERRCODE = 'raise_exception', "
            "TABLE = 'submission_unit_admissions'; RETURN NULL; END; "
        ),
        tuple(
            "CREATE TRIGGER IF NOT EXISTS "
            f"{_ADMISSION_TABLE}_reject_{operation.lower()} "
            f"BEFORE {operation} ON {_ADMISSION_TABLE} BEGIN "
            f"SELECT RAISE(ABORT, '{_ADMISSION_TABLE} is append-only: "
            f"{operation} forbidden'); END"
            for operation in ("UPDATE", "DELETE")
        ),
    )


def _admitted_position_specifications() -> tuple[_TableGuards, ...]:
    """Add the exact actual migration DDL to the native closure in one sorted relation-lock order."""
    specifications = [
        *_native_specifications(),
        _TableGuards(
            "execution_unit_snapshots",
            "execution_unit_snapshots_reject_mutation",
            SNAPSHOT_POSTGRES_DDL[0].split("$$")[1],
            SNAPSHOT_SQLITE_DDL,
        ),
        _submission_admission_specification(),
    ]
    return tuple(sorted(specifications, key=lambda item: item.table_name))


async def require_native_admitted_position_guards(
    session: AsyncSession,
) -> tuple[PhysicalAccountingGuard, ...]:
    """Verify strict native v3 physical preconditions inside the actual caller transaction.

    Args:
        session: Real admitted-position transaction with its operation-specific wallet or scope fence.

    Returns:
        Thirty exact checked guards across fifteen persistent immutable relations.

    Raises:
        RuntimeError: If a caller transaction or actual PostgreSQL READ COMMITTED proof is absent.
        ValueError: If any required persistent relation or exact physical guard is unproven.
    """
    dialect = await acquire_contract_accounting_write_barrier(session)
    specifications = _admitted_position_specifications()
    if dialect == "sqlite":
        return await _sqlite_guards(session, specifications)
    return await _postgres_guards(session, specifications)


async def acquire_native_admitted_position_restore_barrier(
    session: AsyncSession,
) -> tuple[PhysicalAccountingGuard, ...]:
    """Drain the same financial writers before auditing the strict native admitted-position graph.

    Args:
        session: Actual startup or restore transaction before interpreting retained financial evidence.

    Returns:
        Thirty exact guard proofs while the existing global exclusive restore boundary remains held.

    Raises:
        RuntimeError: If a caller transaction or actual PostgreSQL READ COMMITTED proof is absent.
        ValueError: If any native source, unit, admission, journal or publication guard is unproven.
    """
    if not session.in_transaction():
        raise RuntimeError("native_admitted_position_restore_requires_caller_transaction")
    if session.get_bind().dialect.name == "postgresql":
        await _require_postgres_read_committed(session)
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:namespace), hashtext(:domain))"),
            {
                "namespace": ACCOUNTING_STARTUP_LOCK_NAMESPACE,
                "domain": ACCOUNTING_STARTUP_LOCK_DOMAIN,
            },
        )
    return await require_native_admitted_position_guards(session)
