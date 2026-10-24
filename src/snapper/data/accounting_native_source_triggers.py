"""Physical append-only guards for ownership of native account-log identities."""

from sqlalchemy import text
from sqlalchemy.engine import Connection

ACCOUNTING_NATIVE_SOURCE_TABLE = "accounting_native_source_claims"


def _require_table(table_name: str) -> None:
    """Restrict every interpolated DDL identifier to the declared native claim table."""
    if table_name != ACCOUNTING_NATIVE_SOURCE_TABLE:
        raise ValueError("Unknown native accounting source claim table")


def _sqlite_install(table_name: str) -> tuple[str, ...]:
    """Refuse row replacement and deletion through every SQLite connection."""
    return tuple(
        f"CREATE TRIGGER IF NOT EXISTS {table_name}_reject_{operation.lower()} "
        f"BEFORE {operation} ON {table_name} BEGIN "
        f"SELECT RAISE(ABORT, '{table_name} is append-only: {operation} forbidden'); END"
        for operation in ("UPDATE", "DELETE")
    )


def _postgres_install(table_name: str) -> tuple[str, ...]:
    """Protect claim updates, deletes and truncation even in replica sessions."""
    function = "accounting_native_source_reject_mutation"
    return (
        (
            f"CREATE OR REPLACE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ "
            "BEGIN RAISE EXCEPTION 'native accounting source is append-only: % forbidden', TG_OP "
            "USING ERRCODE = 'raise_exception', TABLE = TG_TABLE_NAME; RETURN NULL; END; $$"
        ),
        f"DROP TRIGGER IF EXISTS {table_name}_reject_row_mutation ON {table_name}",
        (
            f"CREATE TRIGGER {table_name}_reject_row_mutation BEFORE UPDATE OR DELETE "
            f"ON {table_name} FOR EACH ROW EXECUTE FUNCTION {function}()"
        ),
        f"ALTER TABLE {table_name} ENABLE ALWAYS TRIGGER {table_name}_reject_row_mutation",
        f"DROP TRIGGER IF EXISTS {table_name}_reject_truncate ON {table_name}",
        (
            f"CREATE TRIGGER {table_name}_reject_truncate BEFORE TRUNCATE "
            f"ON {table_name} FOR EACH STATEMENT EXECUTE FUNCTION {function}()"
        ),
        f"ALTER TABLE {table_name} ENABLE ALWAYS TRIGGER {table_name}_reject_truncate",
    )


def install_accounting_native_source_triggers(connection: Connection, table_name: str) -> None:
    """Install identical migration and create-all evidence retention guards.

    Args:
        connection: Caller-owned online SQLite or PostgreSQL DDL transaction.
        table_name: The statically declared existing native source claim table.
    """
    _require_table(table_name)
    dialect = connection.dialect.name
    if dialect == "sqlite":
        statements = _sqlite_install(table_name)
    elif dialect == "postgresql":
        statements = _postgres_install(table_name)
    else:
        raise NotImplementedError(f"Native accounting source guards unsupported for {dialect}")
    for statement in statements:
        connection.execute(text(statement))


def drop_accounting_native_source_triggers(connection: Connection, table_name: str) -> None:
    """Remove guards only while the caller retains the migration's empty-table fence.

    Args:
        connection: Caller-owned transaction retaining the physical downgrade lock.
        table_name: The statically declared native source claim table, proven empty.
    """
    _require_table(table_name)
    dialect = connection.dialect.name
    if dialect == "sqlite":
        for operation in ("update", "delete"):
            connection.execute(text(f"DROP TRIGGER IF EXISTS {table_name}_reject_{operation}"))
        return
    if dialect != "postgresql":
        raise NotImplementedError(f"Native accounting source guards unsupported for {dialect}")
    for suffix in ("row_mutation", "truncate"):
        connection.execute(
            text(f"DROP TRIGGER IF EXISTS {table_name}_reject_{suffix} ON {table_name}")
        )
