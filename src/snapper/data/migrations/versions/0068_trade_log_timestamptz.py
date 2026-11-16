"""Store the trade log tables' timestamps as ``timestamp with time zone``.

``0001_init`` created ``trade_commands``, ``venue_events`` and
``trade_projection_checkpoints`` with a bare ``sa.DateTime``, so on PostgreSQL
their 14 timestamp columns are ``timestamp without time zone`` while the ORM
declares :class:`~snapper.data.models.TZDateTime`, which is ``timestamptz``,
for every one of them. A naive column compares correctly with the
timezone-aware values the application binds only when the session
``TimeZone`` is UTC. The repository engine pins UTC; the production server
default is ``Europe/Warsaw``, and a session at that default finds no active
row by the aware ``known_to`` sentinel.

PostgreSQL converts ``timestamp`` to ``timestamptz`` without rewriting the
table when the session zone is UTC and no ``USING`` clause is given, and it
reads each stored value as the UTC instant it already is. Every stored value
of these tables was written by a UTC session. The migration pins ``TIME ZONE
'UTC'`` itself, because ``env.py`` pins no zone: under any other zone the same
ALTER rewrites the table and shifts every value, the active sentinel included.

Five partial unique indexes carry a naive ``known_to`` predicate, which
PostgreSQL refuses to convert, so they are dropped before the ALTER and
recreated after it from the ORM's own predicate text. Against an aware column
that text parses to the ``timestamptz`` literal every other table carries;
against a naive column it parses to the original naive literal. The downgrade
therefore recreates the exact original definitions with the same text. All
three tables are locked up front, in one fixed order, under bounded lock and
statement timeouts; ``SET LOCAL`` keeps these settings, and the UTC zone, until
the migration transaction ends.

SQLite already stores these columns as the ORM declares, so both directions
issue no statement there. Revises 0067.
"""

from collections.abc import Sequence
from dataclasses import dataclass

import sqlalchemy as sa
from alembic import op

revision: str = "0068"
down_revision: str | None = "0067"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_AWARE = "timestamp with time zone"
_NAIVE = "timestamp without time zone"
_ACTIVE_PREDICATE = "known_to = '9999-12-31T23:59:59+00:00'"
_SESSION_SETTINGS = (
    "SET LOCAL TIME ZONE 'UTC'",
    "SET LOCAL lock_timeout = '5s'",
    "SET LOCAL statement_timeout = '30s'",
)


@dataclass(frozen=True)
class _Table:
    """One trade log table and its legacy naive timestamp columns.

    Attributes:
        name: Table name.
        columns: The table's ``timestamp without time zone`` columns, in the
            order 0001 created them.
    """

    name: str
    columns: tuple[str, ...]


@dataclass(frozen=True)
class _PartialIndex:
    """One partial unique index whose predicate names ``known_to``.

    Attributes:
        name: Index name.
        table: Indexed table.
        column: The single indexed column.
        predicate: The ORM's ``postgresql_where`` text for the index.
    """

    name: str
    table: str
    column: str
    predicate: str


_TABLES: tuple[_Table, ...] = (
    _Table(
        "trade_commands",
        ("timestamp", "known_to", "created_at", "dispatched_at", "acked_at", "terminal_at"),
    ),
    _Table("venue_events", ("timestamp", "known_to", "venue_timestamp", "received_at")),
    _Table(
        "trade_projection_checkpoints",
        ("timestamp", "known_to", "last_venue_event_at", "checkpoint_at"),
    ),
)
_INDEXES: tuple[_PartialIndex, ...] = (
    _PartialIndex("ix_trade_commands_public_id", "trade_commands", "public_id", _ACTIVE_PREDICATE),
    _PartialIndex(
        "uq_trade_commands_idempotency",
        "trade_commands",
        "idempotency_key",
        f"idempotency_key IS NOT NULL AND {_ACTIVE_PREDICATE}",
    ),
    _PartialIndex("ix_venue_events_public_id", "venue_events", "public_id", _ACTIVE_PREDICATE),
    _PartialIndex(
        "ix_trade_checkpoints_public_id",
        "trade_projection_checkpoints",
        "public_id",
        _ACTIVE_PREDICATE,
    ),
    _PartialIndex(
        "uq_trade_checkpoints_shard_key",
        "trade_projection_checkpoints",
        "shard_key",
        _ACTIVE_PREDICATE,
    ),
)


def _quoted(identifier: str) -> str:
    """Quote one SQL identifier.

    Args:
        identifier: Table, column or index name.

    Returns:
        The identifier in double quotes, with embedded quotes doubled.
    """
    return '"' + identifier.replace('"', '""') + '"'


def _lock_statement() -> str:
    """Build the statement that locks all three tables in one fixed order.

    Returns:
        ``LOCK TABLE`` of the three tables in ``ACCESS EXCLUSIVE`` mode.
    """
    tables = ", ".join(_quoted(table.name) for table in _TABLES)
    return f"LOCK TABLE {tables} IN ACCESS EXCLUSIVE MODE"


def _alter_statement(table: _Table, column_type: str) -> str:
    """Build one table's single ``ALTER TABLE`` for all its columns.

    Args:
        table: Table and its columns.
        column_type: Target column type.

    Returns:
        ``ALTER TABLE`` with one ``ALTER COLUMN ... TYPE`` clause per column
        and no ``USING`` clause.
    """
    clauses = ", ".join(
        f"ALTER COLUMN {_quoted(column)} TYPE {column_type}" for column in table.columns
    )
    return f"ALTER TABLE {_quoted(table.name)} {clauses}"


def _create_statement(index: _PartialIndex) -> str:
    """Build the statement that recreates one partial unique index.

    Args:
        index: Index definition.

    Returns:
        ``CREATE UNIQUE INDEX`` with the ORM's predicate text.
    """
    return (
        f"CREATE UNIQUE INDEX {_quoted(index.name)} ON {_quoted(index.table)} "
        f"({_quoted(index.column)}) WHERE {index.predicate}"
    )


def _statements(column_type: str) -> tuple[str, ...]:
    """Build the whole PostgreSQL conversion to one column type, in order.

    Args:
        column_type: Target type of the 14 columns.

    Returns:
        The session settings, the lock, the index drops, one ALTER per table
        and the index recreations.
    """
    return (
        *_SESSION_SETTINGS,
        _lock_statement(),
        *(f"DROP INDEX {_quoted(index.name)}" for index in _INDEXES),
        *(_alter_statement(table, column_type) for table in _TABLES),
        *(_create_statement(index) for index in _INDEXES),
    )


def _convert(column_type: str) -> None:
    """Convert the 14 columns to one type on PostgreSQL; leave SQLite as it is.

    Args:
        column_type: Target type of the 14 columns.

    Raises:
        RuntimeError: If the database is neither PostgreSQL nor SQLite.
    """
    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        return
    if bind.dialect.name != "postgresql":
        raise RuntimeError("0068 supports only PostgreSQL and SQLite")
    for statement in _statements(column_type):
        bind.execute(sa.text(statement))


def upgrade() -> None:
    """Store the 14 trade log timestamps as ``timestamp with time zone``."""
    _convert(_AWARE)


def downgrade() -> None:
    """Restore the 14 trade log timestamps to ``timestamp without time zone``."""
    _convert(_NAIVE)
