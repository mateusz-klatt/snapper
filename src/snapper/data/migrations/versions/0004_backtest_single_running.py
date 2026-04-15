"""Enforce single active backtest_runs row via partial unique index.

At most one row with status='running' AND known_to=KNOWN_TO_MAX at any
bitemporal snapshot. A second runner trying to transition pending →
running receives ``IntegrityError`` at COMMIT; the runner catches the
unique violation (see ``snapper.data.backtest_conflict``) and transitions
to 'failed' rather than blocking forever.

Declarative DB guarantee — no application-layer check, no race window.
The same pattern is used ~20 times in 0001_init for other "at most one
active" invariants.
"""

from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"


def upgrade() -> None:
    """Create the partial unique index using a dialect-aware predicate."""
    dialect = op.get_bind().dialect.name
    active_filter = _KNOWN_TO_ACTIVE_PG if dialect == "postgresql" else _KNOWN_TO_ACTIVE_SQLITE
    op.execute(
        text(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_bt_single_running "
            "ON backtest_runs (status) "
            f"WHERE status = 'running' AND {active_filter}"
        )
    )


def downgrade() -> None:
    """Drop the partial unique index."""
    op.execute(text("DROP INDEX IF EXISTS uq_bt_single_running"))
