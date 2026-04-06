"""Add leverage and reduce_only columns to orders.

Mirrors the trade_commands extension from migration 0006 so the order STATE
path (DB row, REST response, WS event) carries margin/reduce semantics for
short-selling visibility, not just the request path.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0007"
down_revision: str = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add leverage (nullable int) and reduce_only (bool, default False) to orders."""
    op.add_column("orders", sa.Column("leverage", sa.Integer(), nullable=True))
    op.add_column(
        "orders",
        sa.Column("reduce_only", sa.Boolean(), nullable=False, server_default="0"),
    )


def downgrade() -> None:
    """Remove leverage and reduce_only from orders."""
    op.drop_column("orders", "reduce_only")
    op.drop_column("orders", "leverage")
