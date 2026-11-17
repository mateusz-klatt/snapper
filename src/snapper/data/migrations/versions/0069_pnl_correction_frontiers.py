"""Add explicit correction continuation control without rewriting financial rows.

The table is dormant until a bounded snapshotter uses its atomic repository
contract. No existing sample, watermark or calculation version is changed.
Applying this source migration to production is a separate release decision.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from snapper.data.models import TZDateTime
from snapper.data.models import UUIDColumn

revision: str = "0069"
down_revision: str | None = "0068"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create a per-scope checkpoint with generation and revision CAS tokens."""
    op.create_table(
        "portfolio_pnl_correction_frontiers",
        sa.Column("wallet_public_id", UUIDColumn(), primary_key=True),
        sa.Column("mode", sa.String(8), primary_key=True),
        sa.Column("valuation_ccy", sa.String(16), primary_key=True),
        sa.Column("epoch_public_id", UUIDColumn(), primary_key=True),
        sa.Column("calc_version", sa.String(16), primary_key=True),
        sa.Column("generation", sa.BigInteger(), nullable=False),
        sa.Column("revision", sa.BigInteger(), nullable=False),
        sa.Column("window_start", TZDateTime(), nullable=False),
        sa.Column("next_minute", TZDateTime(), nullable=True),
        sa.Column("tip", TZDateTime(), nullable=False),
        sa.Column("as_of", TZDateTime(), nullable=False),
        sa.Column("baseline_watermarks_json", sa.Text(), nullable=False),
        sa.Column("pending_watermarks_json", sa.Text(), nullable=False),
        sa.Column("prior_peak", sa.Float(), nullable=True),
        sa.Column("repair_from", TZDateTime(), nullable=True),
        sa.CheckConstraint("mode IN ('live', 'paper')", name="ck_pnl_correction_mode"),
        sa.CheckConstraint("generation >= 1 AND revision >= 1", name="ck_pnl_correction_revision"),
        sa.CheckConstraint("window_start <= tip AND tip <= as_of", name="ck_pnl_correction_window"),
        sa.CheckConstraint(
            "next_minute IS NULL OR (window_start <= next_minute AND next_minute <= tip)",
            name="ck_pnl_correction_next",
        ),
        sa.CheckConstraint(
            "repair_from IS NULL OR (window_start <= repair_from AND repair_from <= tip)",
            name="ck_pnl_correction_repair",
        ),
    )


def downgrade() -> None:
    """Drop only continuation control; retained financial evidence is untouched."""
    op.drop_table("portfolio_pnl_correction_frontiers")
