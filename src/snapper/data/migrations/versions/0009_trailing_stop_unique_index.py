"""Add partial unique index for one-trailing-stop-per-cycle.

Mirrors the `uq_ep_active_bracket_per_cycle` partial index created in
`0001_init.py` but for `plan_type = 'trailing_stop'`. The ORM declares
this index in `src/snapper/data/models.py:1643-1658`, so greenfield
databases that run `create_all()` already pick it up — but any
database that came through the 0001 init migration lacks the index,
so the §2.3 "409 on duplicate trailing stop per cycle" guarantee
from `plan_phase3_trailing_stop.md` only fired on greenfield installs
until this migration ships.

Uses the same dialect-aware `CREATE UNIQUE INDEX IF NOT EXISTS` shape
as the bracket index inside an ``autocommit_block``.
"""

from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"
_TERMINAL_STATUSES = "('completed', 'cancelled', 'failed', 'expired')"


def upgrade() -> None:
    """Create the trailing-stop partial unique index if it does not exist."""
    dialect = op.get_bind().dialect.name
    active_filter = _KNOWN_TO_ACTIVE_PG if dialect == "postgresql" else _KNOWN_TO_ACTIVE_SQLITE
    with op.get_context().autocommit_block():
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_ep_active_trailing_stop_per_cycle "
                "ON execution_plans (position_cycle_public_id) "
                "WHERE position_cycle_public_id IS NOT NULL "
                "AND plan_type = 'trailing_stop' "
                f"AND status NOT IN {_TERMINAL_STATUSES} "
                f"AND {active_filter}"
            )
        )


def downgrade() -> None:
    """Drop the trailing-stop partial unique index if present."""
    with op.get_context().autocommit_block():
        op.execute(text("DROP INDEX IF EXISTS uq_ep_active_trailing_stop_per_cycle"))
