"""Add execution_plans, execution_plan_checkpoints, execution_plan_decisions tables.

Phase 1 Day 1 of the Execution Plans framework: the three core plan tables
plus plan_public_id back-reference columns on orders and trade_commands.

All new tables are bitemporal (TemporalMixin) per project policy.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

revision: str = "0016"
down_revision: str = "0015"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"
_CK_EXCHANGE_LOWER = "exchange = LOWER(exchange)"


def upgrade() -> None:
    """Create plan tables and add plan_public_id to orders + trade_commands."""
    op.create_table(
        "execution_plans",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("plan_type", sa.String(32), nullable=False),
        sa.Column("created_by_user_id", sa.String(36), nullable=True),
        sa.Column("created_by_strategy", sa.String(128), nullable=True),
        sa.Column("created_via", sa.String(16), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False),
        sa.Column("shard_key", sa.String(128), nullable=False),
        sa.Column("wallet_public_id", sa.String(36), nullable=False),
        sa.Column("operator_public_id", sa.String(36), nullable=True),
        sa.Column("total_quantity", sa.Float(), nullable=False),
        sa.Column("filled_quantity", sa.Float(), nullable=False, server_default="0"),
        sa.Column("side", sa.String(8), nullable=False),
        sa.Column("parent_plan_public_id", sa.String(36), nullable=True),
        sa.Column("position_cycle_public_id", sa.String(36), nullable=True),
        sa.Column("params", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancel_requested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_evaluated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.String(1024), nullable=True),
        sa.Column("idempotency_key", sa.String(64), nullable=True),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_ep_exchange_lower"),
        sa.CheckConstraint(
            "plan_type IN ('manual_once', 'bracket', 'trailing_stop', "
            "'passive_mm', 'peg', 'scheduler')",
            name="ck_ep_plan_type",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'armed', 'active', 'paused', 'completed', "
            "'cancel_requested', 'cancelled', 'failed', 'expired')",
            name="ck_ep_status",
        ),
        sa.CheckConstraint("side IN ('buy', 'sell')", name="ck_ep_side"),
        sa.CheckConstraint("mode IN ('live', 'paper')", name="ck_ep_mode"),
        sa.CheckConstraint(
            "created_via IN ('ui', 'api', 'cli', 'strategy')",
            name="ck_ep_created_via",
        ),
    )
    op.create_index("ix_ep_plan_type", "execution_plans", ["plan_type"])
    op.create_index("ix_ep_created_by_user_id", "execution_plans", ["created_by_user_id"])
    op.create_index("ix_ep_instrument_public_id", "execution_plans", ["instrument_public_id"])
    op.create_index("ix_ep_exchange", "execution_plans", ["exchange"])
    op.create_index("ix_ep_shard_key", "execution_plans", ["shard_key"])
    op.create_index("ix_ep_status", "execution_plans", ["status"])
    op.create_index("ix_ep_status_exchange_mode", "execution_plans", ["status", "exchange", "mode"])
    op.create_index(
        "ix_ep_instrument_status", "execution_plans", ["instrument_public_id", "status"]
    )
    op.create_index("ix_ep_shard_status", "execution_plans", ["shard_key", "status"])

    with op.get_context().autocommit_block():
        dialect = op.get_bind().dialect.name
        active_filter = _KNOWN_TO_ACTIVE_PG if dialect == "postgresql" else _KNOWN_TO_ACTIVE_SQLITE
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_ep_idempotency_key "
                "ON execution_plans (idempotency_key) "
                f"WHERE idempotency_key IS NOT NULL AND {active_filter}"
            )
        )
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_ep_public_id "
                f"ON execution_plans (public_id) WHERE {active_filter}"
            )
        )

    op.create_table(
        "execution_plan_checkpoints",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("plan_public_id", sa.String(36), nullable=False),
        sa.Column("state", sa.JSON(), nullable=False),
        sa.Column("last_venue_event_id", sa.Integer(), nullable=False),
        sa.Column("last_tick_timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("checkpoint_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_epc_plan_public_id", "execution_plan_checkpoints", ["plan_public_id"])

    with op.get_context().autocommit_block():
        dialect = op.get_bind().dialect.name
        active_filter = _KNOWN_TO_ACTIVE_PG if dialect == "postgresql" else _KNOWN_TO_ACTIVE_SQLITE
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_epc_public_id "
                f"ON execution_plan_checkpoints (public_id) WHERE {active_filter}"
            )
        )

    op.create_table(
        "execution_plan_decisions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("plan_public_id", sa.String(36), nullable=False),
        sa.Column("decision_type", sa.String(32), nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("trigger_type", sa.String(16), nullable=False),
        sa.Column("evidence", sa.JSON(), nullable=False),
        sa.Column("emitted_command_public_id", sa.String(36), nullable=True),
        sa.Column("new_status", sa.String(20), nullable=True),
        sa.Column("reason", sa.String(512), nullable=False),
        sa.Column("decision_importance", sa.String(16), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "decision_importance IN ('action', 'transition', 'routine')",
            name="ck_epd_importance",
        ),
    )
    op.create_index("ix_epd_plan_public_id", "execution_plan_decisions", ["plan_public_id"])
    op.create_index("ix_epd_decided_at", "execution_plan_decisions", ["decided_at"])

    with op.get_context().autocommit_block():
        dialect = op.get_bind().dialect.name
        active_filter = _KNOWN_TO_ACTIVE_PG if dialect == "postgresql" else _KNOWN_TO_ACTIVE_SQLITE
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_epd_public_id "
                f"ON execution_plan_decisions (public_id) WHERE {active_filter}"
            )
        )

    op.add_column("orders", sa.Column("plan_public_id", sa.String(36), nullable=True))
    op.create_index("ix_orders_plan_public_id", "orders", ["plan_public_id"])

    op.add_column("trade_commands", sa.Column("plan_public_id", sa.String(36), nullable=True))
    op.create_index("ix_trade_commands_plan_public_id", "trade_commands", ["plan_public_id"])


def downgrade() -> None:
    """Drop plan tables and plan_public_id columns."""
    op.drop_index("ix_trade_commands_plan_public_id", table_name="trade_commands")
    op.drop_column("trade_commands", "plan_public_id")
    op.drop_index("ix_orders_plan_public_id", table_name="orders")
    op.drop_column("orders", "plan_public_id")
    op.drop_table("execution_plan_decisions")
    op.drop_table("execution_plan_checkpoints")
    op.drop_table("execution_plans")
