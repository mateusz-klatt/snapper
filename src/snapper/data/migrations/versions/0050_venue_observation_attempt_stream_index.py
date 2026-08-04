"""Index the bounded venue-account observation attempt stream.

The F6 activation evaluator reconstructs every minute in a bounded window from
one latest-at-window-start seed per exchange plus all later attempts. The index
matches both the grouped seed lookup and the ordered range scan without changing
the append-only observation contract. Revises 0049.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0050"
down_revision: str | None = "0049"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "venue_account_observations"
_INDEX = "ix_venue_account_observations_attempt_stream"
_COLUMNS = ("wallet_public_id", "exchange", "mode", "timestamp", "id")


def upgrade() -> None:
    """Create the composite attempt-stream index."""
    op.create_index(_INDEX, _TABLE, list(_COLUMNS))


def downgrade() -> None:
    """Drop the composite attempt-stream index."""
    op.drop_index(_INDEX, table_name=_TABLE)
