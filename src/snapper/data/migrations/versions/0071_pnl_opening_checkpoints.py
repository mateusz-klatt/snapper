"""Add the certified P&L opening-checkpoint table.

Creates ``portfolio_pnl_opening_checkpoints`` keyed by the five correction
frontier primary-key columns without altering ``candles`` or any other table.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from snapper.data.models import TZDateTime
from snapper.data.models import UUIDColumn

revision: str = "0071"
down_revision: str | None = "0070"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create ``portfolio_pnl_opening_checkpoints`` with its primary key and CHECKs."""
    op.create_table(
        "portfolio_pnl_opening_checkpoints",
        sa.Column("wallet_public_id", UUIDColumn(), primary_key=True),
        sa.Column("mode", sa.String(8), primary_key=True),
        sa.Column("valuation_ccy", sa.String(16), primary_key=True),
        sa.Column("epoch_public_id", UUIDColumn(), primary_key=True),
        sa.Column("calc_version", sa.String(16), primary_key=True),
        sa.Column("timeline_calc_version", sa.String(16), nullable=False),
        sa.Column("fx_policy_version", sa.String(64), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("source_generation", sa.BigInteger(), nullable=False),
        sa.Column("checkpoint_minute", TZDateTime(), nullable=False),
        sa.Column("as_of", TZDateTime(), nullable=False),
        sa.Column("anchor_public_id", UUIDColumn(), nullable=False),
        sa.Column("anchor_digest", sa.String(64), nullable=False),
        sa.Column("cutoff_execution_watermarks_json", sa.Text(), nullable=False),
        sa.Column("execution_chain_digests_json", sa.Text(), nullable=False),
        sa.Column("cutoff_execution_counts_json", sa.Text(), nullable=False),
        sa.Column("applied_watermarks_json", sa.Text(), nullable=False),
        sa.Column("deferred_execution_sequences_json", sa.Text(), nullable=False),
        sa.Column("prefix_has_idless_executions", sa.Boolean(), nullable=False),
        sa.Column("execution_manifest_digest", sa.String(64), nullable=False),
        sa.Column("prefix_annulment_digest", sa.String(64), nullable=False),
        sa.Column("accrual_prefix_fingerprint_json", sa.Text(), nullable=False),
        sa.Column("prefix_lineage_digest", sa.String(64), nullable=False),
        sa.Column("prefix_fill_witness_digest", sa.String(64), nullable=False),
        sa.Column("prefix_venue_event_watermarks_json", sa.Text(), nullable=False),
        sa.Column("source_expected_venues_json", sa.Text(), nullable=False),
        sa.Column("source_venue_event_watermarks_json", sa.Text(), nullable=False),
        sa.Column("source_fill_witness_digest", sa.String(64), nullable=False),
        sa.Column("symbol_ref_digest", sa.String(64), nullable=False),
        sa.Column("consumed_fx_pairs_json", sa.Text(), nullable=False),
        sa.Column("consumed_fx_open_ats_json", sa.Text(), nullable=False),
        sa.Column("event_fx_exchanges_digest", sa.String(64), nullable=False),
        sa.Column("event_fx_candle_count", sa.BigInteger(), nullable=False),
        sa.Column("event_fx_candle_id_sum", sa.Text(), nullable=False),
        sa.Column("event_fx_candle_max_id", sa.BigInteger(), nullable=False),
        sa.Column("event_fx_candle_max_ts", TZDateTime(), nullable=True),
        sa.Column("state_json", sa.Text(), nullable=False),
        sa.Column("updated_at", TZDateTime(), nullable=False),
        sa.CheckConstraint("mode IN ('live', 'paper')", name="ck_pnl_opening_checkpoints_mode"),
        sa.CheckConstraint(
            "schema_version >= 1",
            name="ck_pnl_opening_checkpoints_schema_version",
        ),
        sa.CheckConstraint(
            "source_generation >= 1",
            name="ck_pnl_opening_checkpoints_source_generation",
        ),
        sa.CheckConstraint(
            "checkpoint_minute <= as_of",
            name="ck_pnl_opening_checkpoints_as_of",
        ),
        sa.CheckConstraint(
            "event_fx_candle_count >= 0",
            name="ck_pnl_opening_checkpoints_fx_candle_count",
        ),
        sa.CheckConstraint(
            "event_fx_candle_max_id >= 0",
            name="ck_pnl_opening_checkpoints_fx_candle_max_id",
        ),
    )


def downgrade() -> None:
    """Drop only ``portfolio_pnl_opening_checkpoints`` without touching other tables."""
    op.drop_table("portfolio_pnl_opening_checkpoints")
