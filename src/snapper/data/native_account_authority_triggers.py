"""Physical append-only guards for source-bound native account authorities."""

from sqlalchemy import text
from sqlalchemy.engine import Connection

NATIVE_ACCOUNT_AUTHORITY_TABLES = ("native_account_authorities",)


def _require_table(table_name: str) -> None:
    """Restrict interpolated DDL identifiers to the statically declared native authority tables."""
    if table_name not in NATIVE_ACCOUNT_AUTHORITY_TABLES:
        raise ValueError("Unknown native account authority table")


def _sqlite_install(table_name: str) -> tuple[str, ...]:
    """Refuse updates and deletes through every SQLite connection."""
    return tuple(
        f"CREATE TRIGGER IF NOT EXISTS {table_name}_reject_{operation.lower()} "
        f"BEFORE {operation} ON {table_name} BEGIN "
        f"SELECT RAISE(ABORT, '{table_name} is append-only: {operation} forbidden'); END"
        for operation in ("UPDATE", "DELETE")
    )


def _postgres_install(table_name: str) -> tuple[str, ...]:
    """Protect row mutations and truncation, including sessions in replica mode."""
    function = "native_account_authority_reject_mutation"
    return (
        (
            f"CREATE OR REPLACE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ "
            "BEGIN RAISE EXCEPTION 'native account authority is append-only: % forbidden', TG_OP "
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


def install_native_account_authority_triggers(connection: Connection, table_name: str) -> None:
    """Install identical online migration and create-all evidence guards.

    Args:
        connection: Caller-owned SQLite or PostgreSQL DDL transaction.
        table_name: Existing statically declared native authority table.
    """
    _require_table(table_name)
    dialect = connection.dialect.name
    if dialect == "sqlite":
        statements = _sqlite_install(table_name)
    elif dialect == "postgresql":
        statements = _postgres_install(table_name)
    else:
        raise NotImplementedError(f"Native account authority guards unsupported for {dialect}")
    for statement in statements:
        connection.execute(text(statement))


def drop_native_account_authority_triggers(connection: Connection, table_name: str) -> None:
    """Remove guards only while the migration retains its proven-empty downgrade fence.

    Args:
        connection: Caller-owned DDL transaction still holding the write fence.
        table_name: Statically declared empty table about to be removed.
    """
    _require_table(table_name)
    dialect = connection.dialect.name
    if dialect == "sqlite":
        for operation in ("update", "delete"):
            connection.execute(text(f"DROP TRIGGER IF EXISTS {table_name}_reject_{operation}"))
        return
    if dialect != "postgresql":
        raise NotImplementedError(f"Native account authority guards unsupported for {dialect}")
    for suffix in ("row_mutation", "truncate"):
        connection.execute(
            text(f"DROP TRIGGER IF EXISTS {table_name}_reject_{suffix} ON {table_name}")
        )
