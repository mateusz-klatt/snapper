"""Add seen_exec_ids column to trade_projection_checkpoints.

Persists the fill deduplication set so checkpoint recovery can
restore it without full VenueEvent replay.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add seen_exec_ids TEXT column with default empty JSON array."""
    with op.batch_alter_table("trade_projection_checkpoints", recreate="auto") as batch_op:
        batch_op.add_column(
            sa.Column("seen_exec_ids", sa.Text(), server_default="[]", nullable=False),
        )


def downgrade() -> None:
    """Remove seen_exec_ids from trade_projection_checkpoints."""
    with op.batch_alter_table("trade_projection_checkpoints", recreate="auto") as batch_op:
        batch_op.drop_column("seen_exec_ids")
