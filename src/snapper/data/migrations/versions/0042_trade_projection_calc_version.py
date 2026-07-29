"""Version the deterministic fold stored in trade projection checkpoints.

Existing rows remain NULL so they are recognizable as legacy state and must
be rebuilt from the authoritative ledger. New writers always stamp the current
calculation version. The nullable, no-default column is portable between the
supported SQLite and PostgreSQL migration paths and never rewrites SCD2
history. Revises 0041.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0042"
down_revision: str | None = "0041"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "trade_projection_checkpoints"
_COLUMN = "projection_calc_version"


def upgrade() -> None:
    """Add the nullable projection calculation version."""
    op.add_column(_TABLE, sa.Column(_COLUMN, sa.Integer(), nullable=True))


def downgrade() -> None:
    """Drop the projection calculation version."""
    op.drop_column(_TABLE, _COLUMN)
