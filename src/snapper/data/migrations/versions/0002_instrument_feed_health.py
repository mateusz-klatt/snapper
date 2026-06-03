"""Add the current-state ``instrument_feed_health`` table.

Persists each publisher subprocess's in-memory subscription / feed-health
tracker so operators can query, after the fact, which symbols are dark,
when each last received data, and why. The table is CURRENT-STATE
(last-write-wins on the natural key), NOT bitemporal / SCD2: the periodic
publisher flush upserts on ``(coordinator, exchange, channel, symbol)``
so each row reflects only the latest observed state.

Revises 0001 (initial schema). Mirrors 0001's column / constraint /
index conventions: plain ``sa.DateTime(timezone=True)`` columns matching
the :class:`snapper.data.models.TZDateTime` decorator, named
``CheckConstraint``s, and a named ``UniqueConstraint`` for the upsert
key.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the ``instrument_feed_health`` table, constraints, and index."""
    op.create_table(
        "instrument_feed_health",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("coordinator", sa.String(32), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("channel", sa.String(64), nullable=False),
        sa.Column("symbol", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("confirmed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_seen_data_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("snapshot_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "coordinator",
            "exchange",
            "channel",
            "symbol",
            name="uq_instrument_feed_health_key",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'confirmed', 'failed')",
            name="ck_instrument_feed_health_status",
        ),
        sa.CheckConstraint(
            "retry_count >= 0",
            name="ck_instrument_feed_health_retry_count_nonneg",
        ),
        sa.CheckConstraint(
            "exchange = LOWER(exchange)",
            name="ck_instrument_feed_health_exchange_lower",
        ),
    )
    op.create_index(
        "ix_instrument_feed_health_exchange",
        "instrument_feed_health",
        ["exchange"],
    )


def downgrade() -> None:
    """Drop the ``instrument_feed_health`` table."""
    op.drop_index("ix_instrument_feed_health_exchange", table_name="instrument_feed_health")
    op.drop_table("instrument_feed_health")
