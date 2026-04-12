"""Add partial unique index for one-bracket-per-cycle enforcement.

Ensures only one active bracket plan can exist per position cycle at any time.
Active means: plan_type='bracket', status not in terminal set, and current SCD2
row (known_to = sentinel). Dialect-specific known_to literals match the storage
formats used by SQLite and PostgreSQL respectively.
"""

from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

revision: str = "0018"
down_revision: str = "0017"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"

_TERMINAL_STATUSES = "('completed', 'cancelled', 'failed', 'expired')"


def upgrade() -> None:
    """Create partial unique index for one-bracket-per-cycle guard."""
    dialect = op.get_bind().dialect.name
    active_filter = _KNOWN_TO_ACTIVE_PG if dialect == "postgresql" else _KNOWN_TO_ACTIVE_SQLITE
    with op.get_context().autocommit_block():
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_ep_active_bracket_per_cycle "
                "ON execution_plans (position_cycle_public_id) "
                "WHERE position_cycle_public_id IS NOT NULL "
                f"AND plan_type = 'bracket' "
                f"AND status NOT IN {_TERMINAL_STATUSES} "
                f"AND {active_filter}"
            )
        )


def downgrade() -> None:
    """Drop the one-bracket-per-cycle partial unique index."""
    with op.get_context().autocommit_block():
        op.execute(text("DROP INDEX IF EXISTS uq_ep_active_bracket_per_cycle"))
