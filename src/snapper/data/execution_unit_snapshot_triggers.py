"""Shared physical immutability DDL for execution-unit snapshot evidence."""

from sqlalchemy import text
from sqlalchemy.engine import Connection

_SQLITE_INSTALL = (
    (
        "CREATE TRIGGER IF NOT EXISTS execution_unit_snapshots_reject_update "
        "BEFORE UPDATE ON execution_unit_snapshots BEGIN "
        "SELECT RAISE(ABORT, 'execution_unit_snapshots is append-only: UPDATE forbidden'); END"
    ),
    (
        "CREATE TRIGGER IF NOT EXISTS execution_unit_snapshots_reject_delete "
        "BEFORE DELETE ON execution_unit_snapshots BEGIN "
        "SELECT RAISE(ABORT, 'execution_unit_snapshots is append-only: DELETE forbidden'); END"
    ),
)
_PG_INSTALL = (
    (
        "CREATE OR REPLACE FUNCTION execution_unit_snapshots_reject_mutation() "
        "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
        "RAISE EXCEPTION 'execution_unit_snapshots is append-only: % forbidden', TG_OP "
        "USING ERRCODE = 'raise_exception', TABLE = 'execution_unit_snapshots'; RETURN NULL; END; $$"
    ),
    "DROP TRIGGER IF EXISTS execution_unit_snapshots_reject_row_mutation ON execution_unit_snapshots",
    (
        "CREATE TRIGGER execution_unit_snapshots_reject_row_mutation BEFORE UPDATE OR DELETE "
        "ON execution_unit_snapshots FOR EACH ROW EXECUTE FUNCTION execution_unit_snapshots_reject_mutation()"
    ),
    "ALTER TABLE execution_unit_snapshots ENABLE ALWAYS TRIGGER execution_unit_snapshots_reject_row_mutation",
    "DROP TRIGGER IF EXISTS execution_unit_snapshots_reject_truncate ON execution_unit_snapshots",
    (
        "CREATE TRIGGER execution_unit_snapshots_reject_truncate BEFORE TRUNCATE "
        "ON execution_unit_snapshots FOR EACH STATEMENT EXECUTE FUNCTION execution_unit_snapshots_reject_mutation()"
    ),
    "ALTER TABLE execution_unit_snapshots ENABLE ALWAYS TRIGGER execution_unit_snapshots_reject_truncate",
)


def install_execution_unit_snapshot_triggers(connection: Connection) -> None:
    """Install the same immutable-evidence guards for migration and create_all stores.

    Args:
        connection: Online PostgreSQL or SQLite connection containing the table.
    """
    dialect = connection.dialect.name
    if dialect == "sqlite":
        statements: tuple[str, ...] = _SQLITE_INSTALL
    elif dialect == "postgresql":
        statements = _PG_INSTALL
    else:
        raise NotImplementedError(f"Snapshot immutability unsupported for dialect={dialect}")
    for statement in statements:
        connection.execute(text(statement))


def drop_execution_unit_snapshot_triggers(connection: Connection) -> None:
    """Remove guards only after an empty-table downgrade has been proven safe.

    Args:
        connection: Online PostgreSQL or SQLite connection containing an empty table.
    """
    dialect = connection.dialect.name
    if dialect == "sqlite":
        names = ("execution_unit_snapshots_reject_update", "execution_unit_snapshots_reject_delete")
        for name in names:
            connection.execute(text(f"DROP TRIGGER IF EXISTS {name}"))
        return
    if dialect != "postgresql":
        raise NotImplementedError(f"Snapshot immutability unsupported for dialect={dialect}")
    for name in (
        "execution_unit_snapshots_reject_row_mutation",
        "execution_unit_snapshots_reject_truncate",
    ):
        connection.execute(text(f"DROP TRIGGER IF EXISTS {name} ON execution_unit_snapshots"))
    connection.execute(text("DROP FUNCTION IF EXISTS execution_unit_snapshots_reject_mutation()"))
