"""Physical append-only guards for raw accounting evidence and durable financial holds."""

from sqlalchemy import text
from sqlalchemy.engine import Connection

ACCOUNTING_EVIDENCE_TABLES = (
    "accounting_raw_receipts",
    "accounting_scope_holds",
    "accounting_hold_resolutions",
    "contract_accounting_anchors",
    "contract_accounting_events",
)


def _require_table(table_name: str) -> None:
    """Restrict DDL identifiers to the statically declared accounting evidence tables."""
    if table_name not in ACCOUNTING_EVIDENCE_TABLES:
        raise ValueError("Unknown accounting evidence table")


def _sqlite_install(table_name: str) -> tuple[str, ...]:
    """Create per-table row-mutation guards without rewriting retained evidence."""
    return tuple(
        f"CREATE TRIGGER IF NOT EXISTS {table_name}_reject_{operation.lower()} "
        f"BEFORE {operation} ON {table_name} BEGIN "
        f"SELECT RAISE(ABORT, '{table_name} is append-only: {operation} forbidden'); END"
        for operation in ("UPDATE", "DELETE")
    )


def _postgres_install(table_name: str) -> tuple[str, ...]:
    """Protect row updates, deletes and whole-table truncation including replica mode."""
    return (
        (
            "CREATE OR REPLACE FUNCTION accounting_evidence_reject_mutation() "
            "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
            "RAISE EXCEPTION 'accounting evidence is append-only: % forbidden', TG_OP "
            "USING ERRCODE = 'raise_exception', TABLE = TG_TABLE_NAME; RETURN NULL; END; $$"
        ),
        f"DROP TRIGGER IF EXISTS {table_name}_reject_row_mutation ON {table_name}",
        (
            f"CREATE TRIGGER {table_name}_reject_row_mutation BEFORE UPDATE OR DELETE "
            f"ON {table_name} FOR EACH ROW EXECUTE FUNCTION accounting_evidence_reject_mutation()"
        ),
        f"ALTER TABLE {table_name} ENABLE ALWAYS TRIGGER {table_name}_reject_row_mutation",
        f"DROP TRIGGER IF EXISTS {table_name}_reject_truncate ON {table_name}",
        (
            f"CREATE TRIGGER {table_name}_reject_truncate BEFORE TRUNCATE "
            f"ON {table_name} FOR EACH STATEMENT EXECUTE FUNCTION accounting_evidence_reject_mutation()"
        ),
        f"ALTER TABLE {table_name} ENABLE ALWAYS TRIGGER {table_name}_reject_truncate",
    )


def install_contract_accounting_table_triggers(connection: Connection, table_name: str) -> None:
    """Install identical migration/create-all guards for one existing evidence table.

    Args:
        connection: The caller's online SQLite or PostgreSQL DDL transaction.
        table_name: One statically declared evidence table, already created.
    """
    _require_table(table_name)
    dialect = connection.dialect.name
    if dialect == "sqlite":
        statements = _sqlite_install(table_name)
    elif dialect == "postgresql":
        statements = _postgres_install(table_name)
    else:
        raise NotImplementedError(f"Accounting evidence guards unsupported for dialect={dialect}")
    for statement in statements:
        connection.execute(text(statement))


def drop_contract_accounting_table_triggers(connection: Connection, table_name: str) -> None:
    """Remove table-local guards only after a migration proves every evidence table empty.

    Args:
        connection: The same caller-owned transaction retaining the downgrade write fence.
        table_name: The empty statically declared evidence table about to be dropped.
    """
    _require_table(table_name)
    dialect = connection.dialect.name
    if dialect == "sqlite":
        for operation in ("update", "delete"):
            connection.execute(text(f"DROP TRIGGER IF EXISTS {table_name}_reject_{operation}"))
        return
    if dialect != "postgresql":
        raise NotImplementedError(f"Accounting evidence guards unsupported for dialect={dialect}")
    for suffix in ("row_mutation", "truncate"):
        connection.execute(
            text(f"DROP TRIGGER IF EXISTS {table_name}_reject_{suffix} ON {table_name}")
        )
