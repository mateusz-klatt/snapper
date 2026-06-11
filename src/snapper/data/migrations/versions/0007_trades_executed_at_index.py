"""Add a trades executed_at index for event-time range maintenance reads.

The Kraken Equities candle-fragmentation repair rebuilds 1m bars from raw
trades filtered by half-open ``executed_at`` windows. A production EXPLAIN on
the ~60M-row trades table planned each one-hour window as a parallel
sequential scan (cost ~1.59M) because only bus-time ``timestamp`` indexes
exist, so a chunked multi-week repair run would repeat that full-table scan
per chunk. This single-column event-time index turns each window into an
index range read. The index is created ``CONCURRENTLY`` on PostgreSQL inside
an autocommit block so the hot trades write path is never blocked during the
build; if a concurrent build is interrupted PostgreSQL can leave an INVALID
index behind, which must be dropped before rerunning the upgrade. SQLite
ignores the dialect-specific flag and builds the index normally. Revises
0006.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the trades executed_at index without blocking trade writes."""
    with op.get_context().autocommit_block():
        op.create_index(
            "ix_trades_executed_at",
            "trades",
            ["executed_at"],
            postgresql_concurrently=True,
        )


def downgrade() -> None:
    """Drop the trades executed_at event-time index."""
    with op.get_context().autocommit_block():
        op.drop_index(
            "ix_trades_executed_at",
            table_name="trades",
            postgresql_concurrently=True,
        )
