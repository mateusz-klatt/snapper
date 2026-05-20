"""Migrate Integer (INT4) primary keys to BigInteger (INT8) on high-write tables.

PostgreSQL ``Integer`` columns are 4-byte signed (max 2_147_483_647).
At Snapper's observed production write rates the ``ticks`` table was
~13 days away from sequence overflow (2026-05-19 measurement: sequence
``last_value`` = 383_305_895 / INT4 max 2_147_483_647).

This migration:

* ``ANALYZE`` each table so the planner stats are fresh (helps with
  the rewrite plan + downtime estimation).
* ``ALTER TABLE ... ALTER COLUMN id TYPE BIGINT`` — PostgreSQL
  rewrites the whole table (column width 4→8 bytes). Holds
  ``ACCESS EXCLUSIVE`` for the duration. Time scales with table size.
* ``ALTER SEQUENCE ... AS BIGINT`` on the owned sequence (resolved
  via ``pg_get_serial_sequence`` rather than hardcoding
  ``{table}_id_seq`` — the SDK-owned sequence name is authoritative
  even if naming conventions drift).
* Post-rewrite ``ANALYZE`` so planner statistics reflect the new
  storage.

SQLite is a no-op — ``INTEGER PRIMARY KEY`` is already 64-bit rowid.
The model declares ``BigInteger().with_variant(Integer, "sqlite")``
so DDL emitted by ``create_all`` keeps ``INTEGER`` on SQLite.

Downgrade refuses to revert any table whose ``max(id)`` exceeds INT4
(silent data corruption otherwise). Operator must shed data manually
before downgrading.

See ``proprietary/plans/plan_2026_05_19_bigint_pk_high_write_tables.md``
for the full design.
"""

from collections.abc import Sequence
from typing import Any

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


HIGH_WRITE_TABLES: tuple[str, ...] = (
    "ticks",
    "trades",
    "candles",
    "market_snapshots",
    "executions",
    "orders",
    "telemetry",
)

_INT4_MAX: int = 2_147_483_647


def _resolve_owned_sequence(bind: Any, table: str) -> str | None:
    """Resolve the owned sequence name via ``pg_get_serial_sequence``.

    Hardcoded ``{table}_id_seq`` is brittle — sequence names can drift
    if the table was created through a non-standard path or renamed.
    ``pg_get_serial_sequence`` is the authoritative way to look up
    the sequence currently owned by ``<table>.id``. Returns ``None``
    when the column has no owned sequence (defensive — should never
    happen for ``autoincrement=True`` columns).
    """
    result = bind.execute(
        sa.text("SELECT pg_get_serial_sequence(:t, 'id')"),
        {"t": table},
    ).scalar()
    if result is None:
        return None
    return str(result)


def _upgrade_postgresql() -> None:
    """ALTER each PK to BigInt + dependent sequence to BIGINT."""
    bind = op.get_bind()
    for table in HIGH_WRITE_TABLES:
        op.execute(sa.text(f"ANALYZE {table}"))
    for table in HIGH_WRITE_TABLES:
        op.execute(sa.text(f"ALTER TABLE {table} ALTER COLUMN id TYPE BIGINT"))
        seq_name = _resolve_owned_sequence(bind, table)
        if seq_name is not None:
            op.execute(sa.text(f"ALTER SEQUENCE {seq_name} AS BIGINT"))
    for table in HIGH_WRITE_TABLES:
        op.execute(sa.text(f"ANALYZE {table}"))


def _upgrade_sqlite() -> None:
    """SQLite stores ``INTEGER PRIMARY KEY`` as 64-bit rowid; nothing to do."""
    return


def _downgrade_postgresql() -> None:
    """Revert BIGINT → INT4. Refuses if any row's id exceeds INT4.

    Without this guard the downgrade would either fail unpredictably
    or silently corrupt data on the next insert. The operator is
    responsible for shedding rows (e.g. archive + truncate) before
    invoking the downgrade if the sequence advanced past INT4 limits.
    """
    bind = op.get_bind()
    for table in HIGH_WRITE_TABLES:
        max_id = bind.execute(sa.text(f"SELECT max(id) FROM {table}")).scalar()
        if max_id is not None and max_id > _INT4_MAX:
            raise RuntimeError(
                f"Cannot downgrade {table}: max(id)={max_id} exceeds INT4 limit {_INT4_MAX}"
            )
    for table in HIGH_WRITE_TABLES:
        seq_name = _resolve_owned_sequence(bind, table)
        if seq_name is not None:
            op.execute(sa.text(f"ALTER SEQUENCE {seq_name} AS INTEGER"))
        op.execute(sa.text(f"ALTER TABLE {table} ALTER COLUMN id TYPE INTEGER"))


def _downgrade_sqlite() -> None:
    """SQLite no-op."""
    return


def upgrade() -> None:
    """Apply the migration for the active database dialect."""
    dialect_name = op.get_bind().dialect.name
    if dialect_name == "postgresql":
        _upgrade_postgresql()
    elif dialect_name == "sqlite":
        _upgrade_sqlite()


def downgrade() -> None:
    """Revert the migration for the active database dialect."""
    dialect_name = op.get_bind().dialect.name
    if dialect_name == "postgresql":
        _downgrade_postgresql()
    elif dialect_name == "sqlite":
        _downgrade_sqlite()
