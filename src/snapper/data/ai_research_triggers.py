"""Canonical DDL for immutable AI-research artifact triggers.

``market_views`` and ``market_view_sources`` are submitted facts. They do not
use SCD2 because neither an authored view nor one of its citations has a valid
in-place correction lifecycle: a correction is a newly submitted artifact.
The triggers here enforce that doctrine below the repository layer on SQLite
and PostgreSQL, including raw SQL mutation paths.

The installer is shared by ORM table-creation listeners and migration 0036 so
both schema creation paths emit the same DDL. Installation and removal are
idempotent. PostgreSQL also rejects ``TRUNCATE`` and promotes its triggers to
``ENABLE ALWAYS`` so changing ``session_replication_role`` cannot bypass the
artifact boundary.
"""

from typing import Literal

from sqlalchemy import text
from sqlalchemy.engine import Connection

_ImmutableTable = Literal["market_views", "market_view_sources"]
_IMMUTABLE_TABLES: tuple[_ImmutableTable, ...] = (
    "market_views",
    "market_view_sources",
)
_PG_FUNCTION_NAME = "ai_research_reject_market_artifact_mutation"


def _selected_tables(only: _ImmutableTable | None) -> tuple[_ImmutableTable, ...]:
    """Resolve the full plane or one table selected by an ORM DDL event."""
    if only is None:
        return _IMMUTABLE_TABLES
    return (only,)


def install_ai_research_immutability_triggers(
    connection: Connection,
    *,
    only: _ImmutableTable | None = None,
) -> None:
    """Install insert-only triggers for AI-research artifact tables.

    Args:
        connection: Live SQLAlchemy connection used for DDL execution.
        only: Optional single table selected by an ORM ``after_create`` event.
            Migration callers omit it to install protection for both tables.
    """
    tables = _selected_tables(only)
    if connection.dialect.name == "sqlite":
        for table_name in tables:
            for operation in ("UPDATE", "DELETE"):
                trigger_name = f"{table_name}_reject_{operation.lower()}"
                connection.execute(
                    text(
                        f"CREATE TRIGGER IF NOT EXISTS {trigger_name} "
                        f"BEFORE {operation} ON {table_name} BEGIN "
                        f"SELECT RAISE(ABORT, '{table_name} is insert-only: "
                        f"{operation} is physically forbidden'); END"
                    )
                )
        return

    connection.execute(
        text(
            f"CREATE OR REPLACE FUNCTION {_PG_FUNCTION_NAME}() "
            "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
            "RAISE EXCEPTION '% is insert-only: % is physically forbidden', "
            "TG_TABLE_NAME, TG_OP USING ERRCODE = 'raise_exception'; "
            "RETURN NULL; END; $$"
        )
    )
    for table_name in tables:
        row_trigger = f"{table_name}_reject_row_mutation"
        truncate_trigger = f"{table_name}_reject_truncate"
        connection.execute(text(f"DROP TRIGGER IF EXISTS {row_trigger} ON {table_name}"))
        connection.execute(
            text(
                f"CREATE TRIGGER {row_trigger} BEFORE UPDATE OR DELETE ON {table_name} "
                f"FOR EACH ROW EXECUTE FUNCTION {_PG_FUNCTION_NAME}()"
            )
        )
        connection.execute(text(f"ALTER TABLE {table_name} ENABLE ALWAYS TRIGGER {row_trigger}"))
        connection.execute(text(f"DROP TRIGGER IF EXISTS {truncate_trigger} ON {table_name}"))
        connection.execute(
            text(
                f"CREATE TRIGGER {truncate_trigger} BEFORE TRUNCATE ON {table_name} "
                f"FOR EACH STATEMENT EXECUTE FUNCTION {_PG_FUNCTION_NAME}()"
            )
        )
        connection.execute(
            text(f"ALTER TABLE {table_name} ENABLE ALWAYS TRIGGER {truncate_trigger}")
        )


def drop_ai_research_immutability_triggers(
    connection: Connection,
) -> None:
    """Remove AI-research artifact immutability triggers idempotently.

    Args:
        connection: Live SQLAlchemy connection used for DDL execution.
    """
    if connection.dialect.name == "sqlite":
        for table_name in _IMMUTABLE_TABLES:
            connection.execute(text(f"DROP TRIGGER IF EXISTS {table_name}_reject_update"))
            connection.execute(text(f"DROP TRIGGER IF EXISTS {table_name}_reject_delete"))
        return

    for table_name in _IMMUTABLE_TABLES:
        connection.execute(
            text(f"DROP TRIGGER IF EXISTS {table_name}_reject_row_mutation ON {table_name}")
        )
        connection.execute(
            text(f"DROP TRIGGER IF EXISTS {table_name}_reject_truncate ON {table_name}")
        )
    connection.execute(text(f"DROP FUNCTION IF EXISTS {_PG_FUNCTION_NAME}()"))
