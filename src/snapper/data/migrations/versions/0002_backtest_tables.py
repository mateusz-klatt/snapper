"""Backtest schema migration.

Adds the six backtest tables that were missing from 0001_init:
backtest_runs, backtest_events, backtest_results, backtest_signals,
backtest_trades, backtest_equity_points. All tables use TemporalMixin
columns (id, public_id, session_id, sequence_id, timestamp, known_to)
and partial unique indexes on public_id where known_to is active.

Matches the ORM definitions in snapper.data.models (BacktestRun and
siblings). Tables are multi-tenant via wallet_public_id on the run row;
child tables reference by run_public_id.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"
_CK_SESSION_ID = "session_id != ''"
_CK_SEQUENCE_ID = "sequence_id > 0"

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the six backtest tables with their indexes."""
    op.create_table(
        "backtest_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("wallet_public_id", sa.String(36), nullable=False),
        sa.Column("operator_public_id", sa.String(36), nullable=True),
        sa.Column("strategy_name", sa.String(128), nullable=False),
        sa.Column("strategy_params", sa.JSON(), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False, server_default="paper"),
        sa.Column("timeframe", sa.String(16), nullable=False),
        sa.Column("start_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("end_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("initial_cash", sa.Float(), nullable=False, server_default="10000.0"),
        sa.Column("status", sa.String(24), nullable=False, server_default="pending"),
        sa.Column("created_by_user_id", sa.String(64), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("process_name", sa.String(128), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'completed', 'failed', "
            "'cancel_requested', 'cancelled')",
            name="ck_br_status",
        ),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_backtest_runs_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_backtest_runs_sequence_id"),
    )
    op.create_index(
        "ix_backtest_runs_wallet_status", "backtest_runs", ["wallet_public_id", "status"]
    )

    op.create_table(
        "backtest_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("run_public_id", sa.String(36), nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("detail", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_backtest_events_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_backtest_events_sequence_id"),
    )
    op.create_index("ix_be_run_ts", "backtest_events", ["run_public_id", "timestamp"])
    op.create_index("ix_backtest_events_run_public_id", "backtest_events", ["run_public_id"])

    op.create_table(
        "backtest_results",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("run_public_id", sa.String(36), nullable=False),
        sa.Column("total_trades", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("winning_trades", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("losing_trades", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_pnl", sa.Float(), nullable=False, server_default="0"),
        sa.Column("max_drawdown", sa.Float(), nullable=False, server_default="0"),
        sa.Column("sharpe_ratio", sa.Float(), nullable=True),
        sa.Column("win_rate", sa.Float(), nullable=True),
        sa.Column("profit_factor", sa.Float(), nullable=True),
        sa.Column("final_equity", sa.Float(), nullable=False, server_default="0"),
        sa.Column("max_equity", sa.Float(), nullable=False, server_default="0"),
        sa.Column("extra_metrics", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_backtest_results_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_backtest_results_sequence_id"),
    )
    op.create_index("ix_backtest_results_run_public_id", "backtest_results", ["run_public_id"])

    op.create_table(
        "backtest_signals",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("run_public_id", sa.String(36), nullable=False),
        sa.Column("signal_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("signal_type", sa.String(32), nullable=False),
        sa.Column("instrument", sa.String(64), nullable=False),
        sa.Column("price", sa.Float(), nullable=False),
        sa.Column("indicators", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_backtest_signals_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_backtest_signals_sequence_id"),
    )
    op.create_index("ix_bs_run_ts", "backtest_signals", ["run_public_id", "signal_time"])
    op.create_index("ix_backtest_signals_run_public_id", "backtest_signals", ["run_public_id"])

    op.create_table(
        "backtest_trades",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("run_public_id", sa.String(36), nullable=False),
        sa.Column("executed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("instrument", sa.String(64), nullable=False),
        sa.Column("side", sa.String(8), nullable=False),
        sa.Column("quantity", sa.Float(), nullable=False),
        sa.Column("price", sa.Float(), nullable=False),
        sa.Column("fee", sa.Float(), nullable=False, server_default="0"),
        sa.Column("pnl", sa.Float(), nullable=True),
        sa.Column("position_after", sa.Float(), nullable=False, server_default="0"),
        sa.Column("signal_public_id", sa.String(36), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_backtest_trades_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_backtest_trades_sequence_id"),
    )
    op.create_index("ix_bt_run_ts", "backtest_trades", ["run_public_id", "executed_at"])
    op.create_index("ix_backtest_trades_run_public_id", "backtest_trades", ["run_public_id"])

    op.create_table(
        "backtest_equity_points",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("run_public_id", sa.String(36), nullable=False),
        sa.Column("point_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("equity", sa.Float(), nullable=False),
        sa.Column("cash", sa.Float(), nullable=False),
        sa.Column("position_value", sa.Float(), nullable=False, server_default="0"),
        sa.Column("drawdown", sa.Float(), nullable=False, server_default="0"),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_backtest_equity_points_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_backtest_equity_points_sequence_id"),
    )
    op.create_index("ix_bep_run_ts", "backtest_equity_points", ["run_public_id", "point_time"])
    op.create_index(
        "ix_backtest_equity_points_run_public_id",
        "backtest_equity_points",
        ["run_public_id"],
    )

    dialect = op.get_bind().dialect.name
    active_filter = _KNOWN_TO_ACTIVE_PG if dialect == "postgresql" else _KNOWN_TO_ACTIVE_SQLITE
    with op.get_context().autocommit_block():
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_backtest_runs_public_id "
                f"ON backtest_runs (public_id) WHERE {active_filter}"
            )
        )
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_be_public_id "
                f"ON backtest_events (public_id) WHERE {active_filter}"
            )
        )
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_br_run_active "
                "ON backtest_results (run_public_id) "
                f"WHERE {active_filter}"
            )
        )
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_bres_public_id "
                f"ON backtest_results (public_id) WHERE {active_filter}"
            )
        )
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_bs_public_id "
                f"ON backtest_signals (public_id) WHERE {active_filter}"
            )
        )
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_bt_public_id "
                f"ON backtest_trades (public_id) WHERE {active_filter}"
            )
        )
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_bep_public_id "
                f"ON backtest_equity_points (public_id) WHERE {active_filter}"
            )
        )


def downgrade() -> None:
    """Drop the six backtest tables and their partial indexes."""
    with op.get_context().autocommit_block():
        op.execute(text("DROP INDEX IF EXISTS ix_bep_public_id"))
        op.execute(text("DROP INDEX IF EXISTS ix_bt_public_id"))
        op.execute(text("DROP INDEX IF EXISTS ix_bs_public_id"))
        op.execute(text("DROP INDEX IF EXISTS ix_bres_public_id"))
        op.execute(text("DROP INDEX IF EXISTS uq_br_run_active"))
        op.execute(text("DROP INDEX IF EXISTS ix_be_public_id"))
        op.execute(text("DROP INDEX IF EXISTS ix_backtest_runs_public_id"))
    op.drop_table("backtest_equity_points")
    op.drop_table("backtest_trades")
    op.drop_table("backtest_signals")
    op.drop_table("backtest_results")
    op.drop_table("backtest_events")
    op.drop_table("backtest_runs")
