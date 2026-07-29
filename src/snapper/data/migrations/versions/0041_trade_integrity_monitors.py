"""Add durable work and cursor state for incremental trade integrity monitors.

Restore, import, and replay writers enqueue the identities they touch in the
worklog inside the trade transaction. Each monitor drains its own pending flag
while a separate durable cursor records bounded timestamp-and-id sweep
progress. The partial pending indexes keep draining proportional to
outstanding work rather than accumulated history. Revises 0040.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

revision: str = "0041"
down_revision: str | None = "0040"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_WORKLOG_TABLE = "trade_integrity_worklog"
_CURSOR_TABLE = "trade_integrity_monitor_cursors"


def _uuid_col() -> sa.types.TypeEngine[str]:
    """Build native PostgreSQL UUID storage with SQLite text fallback."""
    return sa.String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql")


def _worklog_id_col() -> sa.types.TypeEngine[int]:
    """Build a PostgreSQL BIGINT identity with SQLite rowid compatibility."""
    return sa.BigInteger().with_variant(sa.Integer(), "sqlite")


def upgrade() -> None:
    """Create the integrity worklog, pending indexes, and monitor cursors."""
    op.create_table(
        _WORKLOG_TABLE,
        sa.Column("id", _worklog_id_col(), autoincrement=True, nullable=False),
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("instrument_public_id", _uuid_col(), nullable=False),
        sa.Column("trade_id", sa.String(64), nullable=True),
        sa.Column("executed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "enqueued_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "m1_pending",
            sa.Boolean(),
            server_default=sa.true(),
            nullable=False,
        ),
        sa.Column(
            "m2_pending",
            sa.Boolean(),
            server_default=sa.true(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_trade_integrity_worklog_m1_pending",
        _WORKLOG_TABLE,
        ["id"],
        sqlite_where=text("m1_pending = 1"),
        postgresql_where=text("m1_pending IS TRUE"),
    )
    op.create_index(
        "ix_trade_integrity_worklog_m2_pending",
        _WORKLOG_TABLE,
        ["id"],
        sqlite_where=text("m2_pending = 1"),
        postgresql_where=text("m2_pending IS TRUE"),
    )
    op.create_table(
        _CURSOR_TABLE,
        sa.Column("monitor", sa.String(2), nullable=False),
        sa.Column("covered_through", sa.DateTime(timezone=True), nullable=False),
        sa.Column("scan_cursor_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("scan_cursor_id", sa.BigInteger(), nullable=False),
        sa.Column("scan_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "monitor IN ('m1', 'm2')",
            name="ck_trade_integrity_monitor_cursors_monitor",
        ),
        sa.CheckConstraint(
            "scan_cursor_id >= 0",
            name="ck_trade_integrity_monitor_cursors_scan_cursor_id",
        ),
        sa.PrimaryKeyConstraint("monitor"),
    )


def downgrade() -> None:
    """Drop monitor cursor state and then the integrity worklog."""
    op.drop_table(_CURSOR_TABLE)
    op.drop_table(_WORKLOG_TABLE)
