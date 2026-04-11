"""Add position_cycles table for Phase 2 Bracket Evaluator prerequisite.

A position cycle represents a single open->close lifetime of a position on a
given shard. Brackets (SL/TP) attach to a cycle, not to an order: if a
position closes and the user reopens, the new trades belong to a new cycle
even though instrument/wallet/mode are identical.

All columns are bitemporal via TemporalMixin (SCD2 known_to close-and-insert).
The DB-level unique partial index on shard_key where status='open' is a hard
guard against multi-open cycles per shard, on top of the in-memory cache and
pre-insert idempotency check in the trader fill sync path.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

revision: str = "0017"
down_revision: str = "0016"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"
_CK_EXCHANGE_LOWER = "exchange = LOWER(exchange)"


def upgrade() -> None:
    """Create position_cycles bitemporal table with SCD2 active guards."""
    op.create_table(
        "position_cycles",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False),
        sa.Column("shard_key", sa.String(128), nullable=False),
        sa.Column("wallet_public_id", sa.String(36), nullable=False),
        sa.Column("operator_public_id", sa.String(36), nullable=True),
        sa.Column("direction", sa.String(8), nullable=False),
        sa.Column("max_qty", sa.Float(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("opening_command_public_id", sa.String(36), nullable=True),
        sa.Column("closing_command_public_id", sa.String(36), nullable=True),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_pc_exchange_lower"),
        sa.CheckConstraint("mode IN ('live', 'paper')", name="ck_pc_mode"),
        sa.CheckConstraint(
            "direction IN ('long', 'short')",
            name="ck_pc_direction",
        ),
        sa.CheckConstraint(
            "status IN ('open', 'closed', 'liquidated')",
            name="ck_pc_status",
        ),
        sa.CheckConstraint("max_qty >= 0", name="ck_pc_max_qty_nonneg"),
    )
    op.create_index("ix_pc_instrument_public_id", "position_cycles", ["instrument_public_id"])
    op.create_index("ix_pc_exchange", "position_cycles", ["exchange"])
    op.create_index("ix_pc_shard_key", "position_cycles", ["shard_key"])
    op.create_index("ix_pc_wallet_public_id", "position_cycles", ["wallet_public_id"])
    op.create_index("ix_pc_status", "position_cycles", ["status"])
    op.create_index("ix_pc_shard_status", "position_cycles", ["shard_key", "status"])
    op.create_index(
        "ix_pc_instrument_status", "position_cycles", ["instrument_public_id", "status"]
    )
    op.create_index("ix_pc_wallet_status", "position_cycles", ["wallet_public_id", "status"])

    with op.get_context().autocommit_block():
        dialect = op.get_bind().dialect.name
        active_filter = _KNOWN_TO_ACTIVE_PG if dialect == "postgresql" else _KNOWN_TO_ACTIVE_SQLITE
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_pc_public_id "
                f"ON position_cycles (public_id) WHERE {active_filter}"
            )
        )
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_pc_shard_open_active "
                "ON position_cycles (shard_key) "
                f"WHERE status = 'open' AND {active_filter}"
            )
        )


def downgrade() -> None:
    """Drop position_cycles table and all indexes."""
    with op.get_context().autocommit_block():
        op.execute(text("DROP INDEX IF EXISTS uq_pc_shard_open_active"))
        op.execute(text("DROP INDEX IF EXISTS ix_pc_public_id"))
    op.drop_index("ix_pc_wallet_status", table_name="position_cycles")
    op.drop_index("ix_pc_instrument_status", table_name="position_cycles")
    op.drop_index("ix_pc_shard_status", table_name="position_cycles")
    op.drop_index("ix_pc_status", table_name="position_cycles")
    op.drop_index("ix_pc_wallet_public_id", table_name="position_cycles")
    op.drop_index("ix_pc_shard_key", table_name="position_cycles")
    op.drop_index("ix_pc_exchange", table_name="position_cycles")
    op.drop_index("ix_pc_instrument_public_id", table_name="position_cycles")
    op.drop_table("position_cycles")
