"""Add ``ix_telemetry_timestamp`` for retention window + Cluster B archivable counts.

Cluster C ``EventArchiver.export(table='telemetry', day_start, day_end)``
filters on ``Telemetry.timestamp`` (the bus-time column inherited from
``TemporalMixin``); without an index this is a full table scan over a
high-volume audit table. Cluster B's per-table ``archivable`` counter
runs a half-open ``timestamp`` window query every sample interval, so
the same scan repeats once per cadence. Both call paths benefit from
this single B-tree index.

Postgres path uses ``CREATE INDEX CONCURRENTLY`` so a live deploy can
apply the migration without blocking writes. ``CREATE INDEX
CONCURRENTLY`` cannot run inside a transaction, and the project's
Alembic environment wraps every migration in
``context.begin_transaction()``
(``src/snapper/data/migrations/env.py:31`` and ``:48``); the migration
body therefore opens an ``op.get_context().autocommit_block()`` for the
PG branch. SQLite path runs the standard create/drop without the block
(no concurrency model to support).

Plan reference:
``proprietary/plans/plan_observability_cluster_b.md`` §4.1 + §11.7.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_INDEX_NAME = "ix_telemetry_timestamp"
_TABLE_NAME = "telemetry"
_COLUMN_NAME = "timestamp"


def upgrade() -> None:
    """Create ``ix_telemetry_timestamp``; PG uses CONCURRENTLY in autocommit block."""
    ctx = op.get_context()
    if ctx.dialect.name == "postgresql":
        with ctx.autocommit_block():
            op.create_index(
                _INDEX_NAME,
                _TABLE_NAME,
                [_COLUMN_NAME],
                postgresql_concurrently=True,
            )
        return
    op.create_index(_INDEX_NAME, _TABLE_NAME, [_COLUMN_NAME])


def downgrade() -> None:
    """Drop ``ix_telemetry_timestamp``; PG uses CONCURRENTLY in autocommit block."""
    ctx = op.get_context()
    if ctx.dialect.name == "postgresql":
        with ctx.autocommit_block():
            op.drop_index(
                _INDEX_NAME,
                table_name=_TABLE_NAME,
                postgresql_concurrently=True,
            )
        return
    op.drop_index(_INDEX_NAME, table_name=_TABLE_NAME)
