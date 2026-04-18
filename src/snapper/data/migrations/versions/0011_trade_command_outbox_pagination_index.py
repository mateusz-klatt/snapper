"""Add composite index on trade_commands(status, created_at, id).

Phase 4 Day 3 wired OutboxDispatcher to paginate through the
``status='created'`` backlog with ``ORDER BY created_at, id`` and
``OFFSET`` (see plan §3.3). The ORM declares this composite index
in ``src/snapper/data/models.py:872-877`` after the final-review
recommendation, so greenfield databases that run ``create_all()``
pick it up — but any database that migrated through the 0001 init
lacks the covering index, and the pagination query has to do a
full index scan on ``ix_trade_commands_status`` followed by an
in-memory sort on ``(created_at, id)``.

Without the composite index, a backlog of N ``created`` rows costs
O(N log N) per poll cycle at the 50ms cadence. With it, the
database walks the sorted index directly, applies OFFSET+LIMIT,
and returns only the page-size rows touched.

Non-blocking at low traffic. Critical before a backlog accumulates
under load.
"""

from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

revision: str = "0011"
down_revision: str | None = "0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the composite outbox-pagination index if it does not exist."""
    with op.get_context().autocommit_block():
        op.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_trade_commands_outbox_pagination "
                "ON trade_commands (status, created_at, id)"
            )
        )


def downgrade() -> None:
    """Drop the composite outbox-pagination index if present."""
    with op.get_context().autocommit_block():
        op.execute(text("DROP INDEX IF EXISTS ix_trade_commands_outbox_pagination"))
