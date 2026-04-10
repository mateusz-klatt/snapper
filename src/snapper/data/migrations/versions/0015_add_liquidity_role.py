"""Add liquidity_role column to executions and venue_events.

Tracks whether each fill was a maker or taker. Exchange adapters
populate this from venue response fields (liquidity_ind, was_taker,
etc.). MM/peg evaluators use it to verify realized fee assumptions.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0015"
down_revision: str = "0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add liquidity_role to executions and venue_events."""
    with op.batch_alter_table("executions", recreate="auto") as batch_op:
        batch_op.add_column(
            sa.Column("liquidity_role", sa.String(16), nullable=False, server_default="unknown")
        )
    with op.batch_alter_table("venue_events", recreate="auto") as batch_op:
        batch_op.add_column(
            sa.Column("liquidity_role", sa.String(16), nullable=False, server_default="unknown")
        )


def downgrade() -> None:
    """Remove liquidity_role from executions and venue_events."""
    with op.batch_alter_table("venue_events", recreate="auto") as batch_op:
        batch_op.drop_column("liquidity_role")
    with op.batch_alter_table("executions", recreate="auto") as batch_op:
        batch_op.drop_column("liquidity_role")
