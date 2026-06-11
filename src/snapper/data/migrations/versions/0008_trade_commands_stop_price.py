"""Add a nullable stop_price column to trade_commands (#156).

Stop and stop-limit orders carry a trigger price that the API accepted
and validated but only persisted inside ``execution_plan.params`` — the
durable command row had no column for it, so the outbox publish and the
executor reconstruction silently dropped the trigger. The column is
nullable and additive: market/limit rows never set it, no backfill is
needed (the live table was verified to contain zero stop-typed rows
before this migration), and SQLite test databases pick the column up
from the ORM model via ``create_all``. Revises 0007.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the nullable trade_commands.stop_price column."""
    op.add_column("trade_commands", sa.Column("stop_price", sa.Float(), nullable=True))


def downgrade() -> None:
    """Drop the trade_commands.stop_price column."""
    op.drop_column("trade_commands", "stop_price")
