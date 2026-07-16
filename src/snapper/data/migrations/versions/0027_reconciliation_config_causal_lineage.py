"""Add durable causal lineage to reconciliation method configuration.

The nullable predecessor observation identifier preserves classification-time
causality without assigning lineage to existing configuration rows.
Revises 0026.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0027"
down_revision: str | None = "0026"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the nullable classification predecessor observation identifier."""
    op.add_column(
        "portfolio_reconciliation_method_configs",
        sa.Column("classified_after_observation_id", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    """Remove the classification predecessor observation identifier."""
    op.drop_column(
        "portfolio_reconciliation_method_configs",
        "classified_after_observation_id",
    )
