"""Require the complete immutable native financial source graph's physical guard protocol.

The native v2 boundary adds original read attempts/outcomes, selected-key identity,
native authority and original private receive ownership to the existing financial
journal and publication set. It retains the exact legacy global barrier identity.
It does not include product-read tables or grant financial activation/readiness.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.contract_accounting_guards import ACCOUNTING_STARTUP_LOCK_DOMAIN
from snapper.data.contract_accounting_guards import ACCOUNTING_STARTUP_LOCK_NAMESPACE
from snapper.data.contract_accounting_guards import PhysicalAccountingGuard
from snapper.data.contract_accounting_guards import _postgres_guards
from snapper.data.contract_accounting_guards import _require_postgres_read_committed
from snapper.data.contract_accounting_guards import _specifications
from snapper.data.contract_accounting_guards import _sqlite_guards
from snapper.data.contract_accounting_guards import _TableGuards
from snapper.data.contract_accounting_guards import acquire_contract_accounting_write_barrier
from snapper.data.native_account_authority_triggers import NATIVE_ACCOUNT_AUTHORITY_TABLES
from snapper.data.native_account_authority_triggers import _postgres_install as authority_postgres
from snapper.data.native_account_authority_triggers import _sqlite_install as authority_sqlite
from snapper.data.native_account_identity_triggers import NATIVE_ACCOUNT_IDENTITY_TABLES
from snapper.data.native_account_identity_triggers import _postgres_install as identity_postgres
from snapper.data.native_account_identity_triggers import _sqlite_install as identity_sqlite
from snapper.data.native_account_read_triggers import NATIVE_ACCOUNT_READ_TABLES
from snapper.data.native_account_read_triggers import _postgres_install as read_postgres
from snapper.data.native_account_read_triggers import _sqlite_install as read_sqlite
from snapper.data.private_source_ownership_triggers import PRIVATE_SOURCE_OWNERSHIP_TABLES
from snapper.data.private_source_ownership_triggers import _postgres_install as ownership_postgres
from snapper.data.private_source_ownership_triggers import _sqlite_install as ownership_sqlite

NATIVE_ACCOUNTING_GUARD_POLICY = "native_contract_accounting_v2"
NATIVE_PROVENANCE_TABLE_NAMES = tuple(
    sorted(
        (
            *NATIVE_ACCOUNT_READ_TABLES,
            *NATIVE_ACCOUNT_IDENTITY_TABLES,
            *NATIVE_ACCOUNT_AUTHORITY_TABLES,
            *PRIVATE_SOURCE_OWNERSHIP_TABLES,
        )
    )
)


def _native_specifications() -> tuple[_TableGuards, ...]:
    """Combine actual helper definitions into one deterministic relation-lock order."""
    specifications = list(_specifications(include_publications=True))
    for tables, function_name, postgres_ddl, sqlite_ddl in (
        (
            NATIVE_ACCOUNT_READ_TABLES,
            "native_account_read_reject_mutation",
            read_postgres,
            read_sqlite,
        ),
        (
            NATIVE_ACCOUNT_IDENTITY_TABLES,
            "native_account_identity_reject_mutation",
            identity_postgres,
            identity_sqlite,
        ),
        (
            NATIVE_ACCOUNT_AUTHORITY_TABLES,
            "native_account_authority_reject_mutation",
            authority_postgres,
            authority_sqlite,
        ),
        (
            PRIVATE_SOURCE_OWNERSHIP_TABLES,
            "private_source_ownership_reject_mutation",
            ownership_postgres,
            ownership_sqlite,
        ),
    ):
        specifications.extend(
            _TableGuards(
                table, function_name, postgres_ddl(table)[0].split("$$")[1], sqlite_ddl(table)
            )
            for table in tables
        )
    return tuple(sorted(specifications, key=lambda item: item.table_name))


async def require_native_contract_accounting_guards(
    session: AsyncSession,
) -> tuple[PhysicalAccountingGuard, ...]:
    """Check native financial provenance and publication guards in the caller's real transaction.

    Args:
        session: Actual native financial or reserved-policy transaction with operation-specific locks.

    Returns:
        Twenty-six actual checked guard identities across thirteen immutable relations.

    Raises:
        RuntimeError: If the caller transaction or PostgreSQL READ COMMITTED precondition is absent.
        ValueError: If any required persistent relation or exact guard cannot be proven.
    """
    dialect = await acquire_contract_accounting_write_barrier(session)
    specifications = _native_specifications()
    if dialect == "sqlite":
        return await _sqlite_guards(session, specifications)
    return await _postgres_guards(session, specifications)


async def acquire_native_contract_accounting_restore_barrier(
    session: AsyncSession,
) -> tuple[PhysicalAccountingGuard, ...]:
    """Drain the same financial writers before auditing the complete native source graph.

    Args:
        session: Actual startup/restore transaction before reading retained financial facts.

    Returns:
        Twenty-six actual checked guards while the existing global restore barrier remains held.

    Raises:
        RuntimeError: If the caller transaction or actual PostgreSQL READ COMMITTED proof is absent.
        ValueError: If a native lineage, financial journal or publication guard is unproven.
    """
    if not session.in_transaction():
        raise RuntimeError("native_accounting_restore_requires_caller_transaction")
    if session.get_bind().dialect.name == "postgresql":
        await _require_postgres_read_committed(session)
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:namespace), hashtext(:domain))"),
            {
                "namespace": ACCOUNTING_STARTUP_LOCK_NAMESPACE,
                "domain": ACCOUNTING_STARTUP_LOCK_DOMAIN,
            },
        )
    return await require_native_contract_accounting_guards(session)
