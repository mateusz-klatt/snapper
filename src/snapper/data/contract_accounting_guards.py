"""Verify constant-size physical accounting preconditions without scanning financial history."""

import hashlib
import json
from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel
from pydantic import ConfigDict
from sqlalchemy import bindparam
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.accounting_native_source_triggers import ACCOUNTING_NATIVE_SOURCE_TABLE
from snapper.data.accounting_native_source_triggers import _postgres_install as native_postgres_ddl
from snapper.data.accounting_native_source_triggers import _sqlite_install as native_sqlite_ddl
from snapper.data.contract_accounting_triggers import ACCOUNTING_EVIDENCE_TABLES
from snapper.data.contract_accounting_triggers import _postgres_install as accounting_postgres_ddl
from snapper.data.contract_accounting_triggers import _sqlite_install as accounting_sqlite_ddl
from snapper.data.financial_publication_triggers import FINANCIAL_PUBLICATION_TABLES
from snapper.data.financial_publication_triggers import (
    _postgres_install as publication_postgres_ddl,
)
from snapper.data.financial_publication_triggers import _sqlite_install as publication_sqlite_ddl

ACCOUNTING_STARTUP_LOCK_NAMESPACE = "snapper_startup_integrity"
ACCOUNTING_STARTUP_LOCK_DOMAIN = "contract_accounting_v1"


class ContractAccountingIntegrityError(ValueError):
    """Required physical guards or restored financial lineage cannot establish safe accounting."""


@dataclass(frozen=True, slots=True)
class PhysicalAccountingGuard:
    """Identify one actually verified native accounting trigger without caching permission."""

    table_name: str
    trigger_name: str
    definition_digest: str


@dataclass(frozen=True, slots=True)
class _TableGuards:
    """Expected DDL from the actual online migration helpers for one fixed protected relation."""

    table_name: str
    function_name: str
    function_body: str
    sqlite_statements: tuple[str, ...]


class _SqliteGuard(BaseModel):
    """Typed metadata at the SQLite catalog boundary."""

    model_config = ConfigDict(extra="forbid", strict=True)

    table_name: str
    trigger_name: str
    definition: str


class _PostgresGuard(BaseModel):
    """Typed metadata at the PostgreSQL catalog boundary."""

    model_config = ConfigDict(extra="forbid", strict=True)

    table_name: str
    trigger_name: str
    enabled: str
    relation_kind: str
    persistence: str
    trigger_type: int
    internal: bool
    deferrable: bool
    conditional: bool
    whole_table: bool
    no_arguments: bool
    function_name: str
    function_body: str
    function_in_table_schema: bool
    function_arguments: int
    returns_trigger: bool
    language: str
    security_definer: bool


def _normalize_sql(value: str) -> str:
    """Normalize catalog whitespace and the semantically irrelevant trigger creation modifier."""
    return " ".join(value.split()).replace("CREATE TRIGGER IF NOT EXISTS ", "CREATE TRIGGER ", 1)


def _specifications(include_publications: bool) -> tuple[_TableGuards, ...]:
    """Use only statically owned identifiers and actual immutable migration helper definitions."""
    result = [
        _TableGuards(
            table,
            "accounting_evidence_reject_mutation",
            accounting_postgres_ddl(table)[0].split("$$")[1],
            accounting_sqlite_ddl(table),
        )
        for table in ACCOUNTING_EVIDENCE_TABLES
    ]
    result.append(
        _TableGuards(
            ACCOUNTING_NATIVE_SOURCE_TABLE,
            "accounting_native_source_reject_mutation",
            native_postgres_ddl(ACCOUNTING_NATIVE_SOURCE_TABLE)[0].split("$$")[1],
            native_sqlite_ddl(ACCOUNTING_NATIVE_SOURCE_TABLE),
        )
    )
    if include_publications:
        result.extend(
            _TableGuards(
                table,
                "financial_publication_reject_mutation",
                publication_postgres_ddl(table)[0].split("$$")[1],
                publication_sqlite_ddl(table),
            )
            for table in FINANCIAL_PUBLICATION_TABLES
        )
    return tuple(sorted(result, key=lambda item: item.table_name))


def _proof(table: str, trigger: str, definition: str) -> PhysicalAccountingGuard:
    """Return actual checked catalog evidence, never a remembered writer capability."""
    return PhysicalAccountingGuard(table, trigger, hashlib.sha256(definition.encode()).hexdigest())


async def _require_postgres_read_committed(session: AsyncSession) -> None:
    """Reject stale-snapshot callers before any startup advisory wait or financial write."""
    isolation: object = (await session.execute(text("SHOW transaction_isolation"))).scalar_one()
    if isolation != "read committed":
        raise RuntimeError("Contract accounting requires READ COMMITTED isolation")


async def _sqlite_guards(
    session: AsyncSession, specifications: tuple[_TableGuards, ...]
) -> tuple[PhysicalAccountingGuard, ...]:
    """Bind exact persistent trigger definitions and refuse connection-local table shadowing."""
    tables = [item.table_name for item in specifications]
    shadows = await session.execute(
        text(
            "SELECT name FROM temp.sqlite_schema "
            "WHERE type IN ('table', 'view') AND name IN :tables"
        ).bindparams(bindparam("tables", expanding=True)),
        {"tables": tables},
    )
    if shadows.first() is not None:
        raise ContractAccountingIntegrityError("accounting_guard_temporary_relation_shadow")
    rows = await session.execute(
        text(
            "SELECT tbl_name AS table_name, name AS trigger_name, sql AS definition "
            "FROM main.sqlite_schema WHERE type = 'trigger' AND tbl_name IN :tables"
        ).bindparams(bindparam("tables", expanding=True)),
        {"tables": tables},
    )
    actual = {
        item.trigger_name: item
        for row in rows.mappings()
        for item in (_SqliteGuard.model_validate(dict(row)),)
    }
    result: list[PhysicalAccountingGuard] = []
    for specification in specifications:
        for operation, definition in zip(
            ("update", "delete"), specification.sqlite_statements, strict=True
        ):
            name = f"{specification.table_name}_reject_{operation}"
            found = actual.get(name)
            if (
                found is None
                or found.table_name != specification.table_name
                or _normalize_sql(found.definition) != _normalize_sql(definition)
            ):
                raise ContractAccountingIntegrityError(f"accounting_physical_guard_invalid:{name}")
            result.append(_proof(found.table_name, name, _normalize_sql(found.definition)))
    return tuple(result)


def _postgres_matches(
    actual: _PostgresGuard, specification: _TableGuards, trigger_type: int
) -> bool:
    """Require the exact event mask, firing mode, relation function and unconditional trigger body."""
    return (
        actual.enabled == "A"
        and actual.relation_kind == "r"
        and actual.persistence == "p"
        and actual.trigger_type == trigger_type
        and not actual.internal
        and not actual.deferrable
        and not actual.conditional
        and actual.whole_table
        and actual.no_arguments
        and actual.function_name == specification.function_name
        and _normalize_sql(actual.function_body) == _normalize_sql(specification.function_body)
        and actual.function_in_table_schema
        and actual.function_arguments == 0
        and actual.returns_trigger
        and actual.language == "plpgsql"
        and not actual.security_definer
    )


async def _postgres_guards(
    session: AsyncSession, specifications: tuple[_TableGuards, ...]
) -> tuple[PhysicalAccountingGuard, ...]:
    """Retain stable-order relation locks while validating actual PostgreSQL trigger metadata."""
    relations = ", ".join(f'"{item.table_name}"' for item in specifications)
    await session.execute(text(f"LOCK TABLE {relations} IN ROW EXCLUSIVE MODE"))
    parameters = {f"table{number}": item.table_name for number, item in enumerate(specifications)}
    targets = ", ".join(f"to_regclass(:{name})" for name in parameters)
    rows = await session.execute(
        text(
            "SELECT c.relname::text AS table_name, t.tgname::text AS trigger_name, "
            "t.tgenabled::text AS enabled, c.relkind::text AS relation_kind, "
            "c.relpersistence::text AS persistence, t.tgtype::int AS trigger_type, "
            "t.tgisinternal AS internal, t.tgdeferrable AS deferrable, "
            "t.tgqual IS NOT NULL AS conditional, t.tgattr::text = '' AS whole_table, "
            "t.tgargs = ''::bytea AS no_arguments, p.proname::text AS function_name, "
            "p.prosrc AS function_body, p.pronamespace = c.relnamespace AS function_in_table_schema, "
            "p.pronargs::int AS function_arguments, p.prorettype = 'trigger'::regtype AS returns_trigger, "
            "l.lanname::text AS language, p.prosecdef AS security_definer "
            "FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
            "JOIN pg_proc p ON p.oid = t.tgfoid JOIN pg_language l ON l.oid = p.prolang "
            f"WHERE c.oid IN ({targets})"
        ),
        parameters,
    )
    actual = {
        (item.table_name, item.trigger_name): item
        for row in rows.mappings()
        for item in (_PostgresGuard.model_validate(dict(row)),)
    }
    result: list[PhysicalAccountingGuard] = []
    for specification in specifications:
        for suffix, trigger_type in (("row_mutation", 27), ("truncate", 34)):
            name = f"{specification.table_name}_reject_{suffix}"
            found = actual.get((specification.table_name, name))
            if found is None or not _postgres_matches(found, specification, trigger_type):
                raise ContractAccountingIntegrityError(f"accounting_physical_guard_invalid:{name}")
            definition = json.dumps(found.model_dump(), sort_keys=True, separators=(",", ":"))
            result.append(_proof(specification.table_name, name, definition))
    return tuple(result)


async def require_contract_accounting_guards(
    session: AsyncSession, *, include_publications: bool = False
) -> tuple[PhysicalAccountingGuard, ...]:
    """Require actual physical append-only accounting guards inside the caller's transaction.

    The native writer calls this constant-size metadata check before interpreting
    family-filtered history. SQLite callers must already hold the repository's
    BEGIN IMMEDIATE reservation. PostgreSQL retains ROW EXCLUSIVE locks in fixed
    order so concurrent writers remain compatible while trigger ALTER/DROP
    operations cannot pass the relation boundary. Restored semantic history
    still requires the separate full startup audit after guards are reinstalled.

    Args:
        session: Actual financial transaction already holding its required wallet fence.
        include_publications: Also verify the fixed outbox relations during global startup audit.

    Returns:
        Actual checked trigger identities, not a cached activation grant or financial readiness.
    """
    dialect = await acquire_contract_accounting_write_barrier(session)
    specifications = _specifications(include_publications)
    if dialect == "sqlite":
        return await _sqlite_guards(session, specifications)
    return await _postgres_guards(session, specifications)


async def acquire_contract_accounting_write_barrier(
    session: AsyncSession,
) -> Literal["sqlite", "postgresql"]:
    """Join the actual startup boundary before staging any native accounting anchor or event.

    Native cash and position writer entries acquire this shared PostgreSQL
    transaction lock before their first accounting-table write. The startup
    audit takes the same key exclusively, drains existing writers, then checks
    physical guards and all retained graphs. SQLite callers already own the
    actual repository BEGIN IMMEDIATE reservation. An explicit main-database
    zero-row write also acquires that physical reservation for an otherwise
    deferred caller before catalog checks. It changes no retained row and
    cannot fire row mutation guards. This lock is unrelated to current account
    capability and never resolves a financial hold. PostgreSQL callers must
    already use actual READ COMMITTED; the barrier never changes isolation
    after a caller may have fixed a snapshot.

    Args:
        session: Real outer financial transaction, before any accounting-table write.

    Returns:
        The actual supported dialect after joining its transaction-scoped startup boundary.
    """
    if not session.in_transaction():
        raise RuntimeError("accounting_guard_requires_caller_transaction")
    dialect = session.get_bind().dialect.name
    if dialect == "postgresql":
        await _require_postgres_read_committed(session)
        await session.execute(
            text("SELECT pg_advisory_xact_lock_shared(hashtext(:namespace), hashtext(:domain))"),
            {
                "namespace": ACCOUNTING_STARTUP_LOCK_NAMESPACE,
                "domain": ACCOUNTING_STARTUP_LOCK_DOMAIN,
            },
        )
        return "postgresql"
    if dialect == "sqlite":
        await session.execute(
            text("UPDATE main.accounting_native_source_claims SET id = id WHERE 0")
        )
        return "sqlite"
    raise NotImplementedError(f"Accounting write barrier unsupported for {dialect}")


async def acquire_contract_accounting_restore_barrier(
    session: AsyncSession,
) -> tuple[PhysicalAccountingGuard, ...]:
    """Drain guarded writers before the full semantic startup audit without taking wallet fences.

    This phase verifies only physical preconditions and transaction ordering.
    Financial activation must also complete the separate full retained-graph
    audit. PostgreSQL uses the exclusive form of the exact shared writer key.
    SQLite acquires an actual main-database write reservation through the same
    verified guard path. The resulting evidence never grants readiness.

    Args:
        session: Actual startup transaction, before interpreting retained financial history.

    Returns:
        Required journal and outbox physical guards held under the actual startup boundary.
    """
    if not session.in_transaction():
        raise RuntimeError("accounting_restore_requires_caller_transaction")
    if session.get_bind().dialect.name == "postgresql":
        await _require_postgres_read_committed(session)
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:namespace), hashtext(:domain))"),
            {
                "namespace": ACCOUNTING_STARTUP_LOCK_NAMESPACE,
                "domain": ACCOUNTING_STARTUP_LOCK_DOMAIN,
            },
        )
    return await require_contract_accounting_guards(session, include_publications=True)
