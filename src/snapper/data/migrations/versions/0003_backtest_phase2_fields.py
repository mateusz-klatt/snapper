"""Add Phase 2 configuration fields to backtest_runs.

Adds execution_mode, fill_model, slippage_bps, commission_bps columns plus
matching CHECK constraints. Existing rows backfill via server defaults.
SQLite requires batch mode for ALTER TABLE ADD CONSTRAINT — both engines
go through op.batch_alter_table for portability.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add 4 columns and 4 CHECK constraints to backtest_runs."""
    with op.batch_alter_table("backtest_runs") as batch_op:
        batch_op.add_column(
            sa.Column(
                "execution_mode",
                sa.String(16),
                nullable=False,
                server_default="direct_db",
            )
        )
        batch_op.add_column(
            sa.Column(
                "fill_model",
                sa.String(32),
                nullable=False,
                server_default="market",
            )
        )
        batch_op.add_column(
            sa.Column(
                "slippage_bps",
                sa.Float(),
                nullable=False,
                server_default="0",
            )
        )
        batch_op.add_column(
            sa.Column(
                "commission_bps",
                sa.Float(),
                nullable=False,
                server_default="0",
            )
        )
        batch_op.create_check_constraint(
            "ck_br_execution_mode",
            "execution_mode IN ('direct_db', 'zmq_replay')",
        )
        batch_op.create_check_constraint(
            "ck_br_fill_model",
            "fill_model IN ('market')",
        )
        batch_op.create_check_constraint(
            "ck_br_slippage_bounds",
            "slippage_bps >= 0 AND slippage_bps <= 500",
        )
        batch_op.create_check_constraint(
            "ck_br_commission_bounds",
            "commission_bps >= 0 AND commission_bps <= 500",
        )


def downgrade() -> None:
    """Drop the 4 CHECK constraints and the 4 columns."""
    with op.batch_alter_table("backtest_runs") as batch_op:
        batch_op.drop_constraint("ck_br_commission_bounds", type_="check")
        batch_op.drop_constraint("ck_br_slippage_bounds", type_="check")
        batch_op.drop_constraint("ck_br_fill_model", type_="check")
        batch_op.drop_constraint("ck_br_execution_mode", type_="check")
        batch_op.drop_column("commission_bps")
        batch_op.drop_column("slippage_bps")
        batch_op.drop_column("fill_model")
        batch_op.drop_column("execution_mode")
