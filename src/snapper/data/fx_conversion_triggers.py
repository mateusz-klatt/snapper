"""Canonical append-only trigger DDL for FX conversion artifacts."""

from sqlalchemy import text
from sqlalchemy.engine import Connection

_TABLES = ("fx_conversion_elections", "fx_conversion_proofs")


def _install_table(connection: Connection, table_name: str) -> None:
    """Install one table's dialect-specific immutable-ledger triggers."""
    if connection.dialect.name == "sqlite":
        for operation in ("UPDATE", "DELETE"):
            connection.execute(
                text(
                    f"CREATE TRIGGER IF NOT EXISTS {table_name}_reject_{operation.lower()} "
                    f"BEFORE {operation} ON {table_name} BEGIN "
                    f"SELECT RAISE(ABORT, '{table_name} is append-only: {operation} is "
                    "physically forbidden'); END"
                )
            )
        return
    function_name = f"{table_name}_reject_mutation"
    connection.execute(
        text(
            f"CREATE OR REPLACE FUNCTION {function_name}() RETURNS trigger LANGUAGE plpgsql "
            f"AS $$ BEGIN RAISE EXCEPTION '{table_name} is append-only: % is physically "
            f"forbidden', TG_OP USING ERRCODE = 'raise_exception', TABLE = '{table_name}'; "
            "RETURN NULL; END; $$"
        )
    )
    connection.execute(
        text(f"DROP TRIGGER IF EXISTS {table_name}_reject_row_mutation ON {table_name}")
    )
    connection.execute(
        text(
            f"CREATE TRIGGER {table_name}_reject_row_mutation BEFORE UPDATE OR DELETE ON "
            f"{table_name} FOR EACH ROW EXECUTE FUNCTION {function_name}()"
        )
    )
    connection.execute(
        text(f"ALTER TABLE {table_name} ENABLE ALWAYS TRIGGER {table_name}_reject_row_mutation")
    )
    connection.execute(text(f"DROP TRIGGER IF EXISTS {table_name}_reject_truncate ON {table_name}"))
    connection.execute(
        text(
            f"CREATE TRIGGER {table_name}_reject_truncate BEFORE TRUNCATE ON {table_name} "
            f"FOR EACH STATEMENT EXECUTE FUNCTION {function_name}()"
        )
    )
    connection.execute(
        text(f"ALTER TABLE {table_name} ENABLE ALWAYS TRIGGER {table_name}_reject_truncate")
    )


def install_fx_conversion_immutability_triggers(connection: Connection) -> None:
    """Install idempotent append-only triggers for both FX artifact tables."""
    for table_name in _TABLES:
        _install_table(connection, table_name)


def drop_fx_conversion_immutability_triggers(connection: Connection) -> None:
    """Drop both FX artifact trigger families before dropping their tables."""
    if connection.dialect.name == "sqlite":
        for table_name in _TABLES:
            for operation in ("update", "delete"):
                connection.execute(text(f"DROP TRIGGER IF EXISTS {table_name}_reject_{operation}"))
        return
    for table_name in _TABLES:
        connection.execute(
            text(f"DROP TRIGGER IF EXISTS {table_name}_reject_row_mutation ON {table_name}")
        )
        connection.execute(
            text(f"DROP TRIGGER IF EXISTS {table_name}_reject_truncate ON {table_name}")
        )
        connection.execute(text(f"DROP FUNCTION IF EXISTS {table_name}_reject_mutation()"))
