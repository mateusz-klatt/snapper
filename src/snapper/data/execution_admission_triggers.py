"""Physical append-only guards for shared account routes and v2 unit admissions."""

from typing import Literal

from sqlalchemy import text
from sqlalchemy.engine import Connection

type AdmissionEvidenceTable = Literal["execution_route_authorities", "submission_unit_admissions"]

ADMISSION_EVIDENCE_TABLES: tuple[AdmissionEvidenceTable, ...] = (
    "execution_route_authorities",
    "submission_unit_admissions",
)


def install_execution_admission_table_triggers(
    connection: Connection, table: AdmissionEvidenceTable
) -> None:
    """Install physical guards for one newly created immutable evidence table.

    Args:
        connection: Real online SQLite or PostgreSQL connection.
        table: One fixed internal evidence table, never an arbitrary SQL identifier.
    """
    if table not in ADMISSION_EVIDENCE_TABLES:
        raise ValueError("Unsupported execution admission evidence table")
    if connection.dialect.name == "sqlite":
        for operation in ("UPDATE", "DELETE"):
            connection.execute(
                text(
                    f"CREATE TRIGGER IF NOT EXISTS {table}_reject_{operation.lower()} "
                    f"BEFORE {operation} ON {table} BEGIN "
                    f"SELECT RAISE(ABORT, '{table} is append-only: {operation} forbidden'); END"
                )
            )
        return
    if connection.dialect.name != "postgresql":
        raise NotImplementedError("Execution admission guards support SQLite and PostgreSQL")
    connection.execute(
        text(
            f"CREATE OR REPLACE FUNCTION {table}_reject_mutation() "
            "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
            f"RAISE EXCEPTION '{table} is append-only: % forbidden', TG_OP "
            f"USING ERRCODE = 'raise_exception', TABLE = '{table}'; RETURN NULL; END; $$"
        )
    )
    for suffix, operations, level in (
        ("row_mutation", "UPDATE OR DELETE", "ROW"),
        ("truncate", "TRUNCATE", "STATEMENT"),
    ):
        name = f"{table}_reject_{suffix}"
        connection.execute(text(f"DROP TRIGGER IF EXISTS {name} ON {table}"))
        connection.execute(
            text(
                f"CREATE TRIGGER {name} BEFORE {operations} ON {table} "
                f"FOR EACH {level} EXECUTE FUNCTION {table}_reject_mutation()"
            )
        )
        connection.execute(text(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {name}"))


def install_execution_admission_triggers(connection: Connection) -> None:
    """Install the same guards used by both migration and create-all stores.

    Args:
        connection: Online connection after both evidence tables have been created.
    """
    for table in ADMISSION_EVIDENCE_TABLES:
        install_execution_admission_table_triggers(connection, table)


def drop_execution_admission_triggers(connection: Connection) -> None:
    """Remove guards only after a serialized empty-table downgrade proves safety.

    Args:
        connection: Online connection holding the downgrade's table/write fences.
    """
    dialect = connection.dialect.name
    if dialect not in {"sqlite", "postgresql"}:
        raise NotImplementedError("Execution admission guards support SQLite and PostgreSQL")
    for table in ADMISSION_EVIDENCE_TABLES:
        suffixes = ("update", "delete") if dialect == "sqlite" else ("row_mutation", "truncate")
        for suffix in suffixes:
            clause = "" if dialect == "sqlite" else f" ON {table}"
            connection.execute(text(f"DROP TRIGGER IF EXISTS {table}_reject_{suffix}{clause}"))
        if dialect == "postgresql":
            connection.execute(text(f"DROP FUNCTION IF EXISTS {table}_reject_mutation()"))
