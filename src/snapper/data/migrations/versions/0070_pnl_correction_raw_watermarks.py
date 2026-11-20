"""Persist the captured execution evidence in the correction checkpoint.

Nullable columns keep legacy control rows readable. An explicit empty JSON
object represents a generation that froze an empty execution prefix.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0070"
down_revision: str | None = "0069"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the captured replay evidence to correction control."""
    op.add_column(
        "portfolio_pnl_correction_frontiers",
        sa.Column("raw_execution_watermarks_json", sa.Text(), nullable=True),
    )
    op.add_column(
        "portfolio_pnl_correction_frontiers",
        sa.Column("execution_manifest_digest", sa.String(64), nullable=True),
    )
    op.add_column(
        "portfolio_pnl_correction_frontiers",
        sa.Column("has_fill_gap", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "portfolio_pnl_correction_frontiers",
        sa.Column("expected_venues_json", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    """Remove the captured replay evidence from correction control."""
    op.drop_column("portfolio_pnl_correction_frontiers", "expected_venues_json")
    op.drop_column("portfolio_pnl_correction_frontiers", "has_fill_gap")
    op.drop_column("portfolio_pnl_correction_frontiers", "execution_manifest_digest")
    op.drop_column("portfolio_pnl_correction_frontiers", "raw_execution_watermarks_json")
