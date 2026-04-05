"""Add leverage and reduce_only columns to trade_commands.

Supports margin trading and position-closing orders for short selling.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006"
down_revision: str = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add leverage (nullable int) and reduce_only (bool, default False) to trade_commands."""
    op.add_column("trade_commands", sa.Column("leverage", sa.Integer(), nullable=True))
    op.add_column(
        "trade_commands",
        sa.Column("reduce_only", sa.Boolean(), nullable=False, server_default="0"),
    )


def downgrade() -> None:
    """Remove leverage and reduce_only from trade_commands."""
    op.drop_column("trade_commands", "reduce_only")
    op.drop_column("trade_commands", "leverage")
