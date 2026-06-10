"""Add a paired_execution_groups (status, timestamp) index for completed-window recovery.

The startup recovery replays venue fills into RECENTLY COMPLETED
paired-execution groups (a late original fill that landed while the
coordinator was down must reopen the completed group, and the live reopen
trigger never fires for events already persisted). The replay is bounded by a
time window over the group successor ``timestamp`` (the completion time), so
the query filters ``status = 'completed' AND timestamp > cutoff``. Completed
current-active rows accumulate for the lifetime of a deployment, and the
existing single-column ``ix_peg_status`` index would leave the timestamp
filter scanning all of them; this composite index makes the window read an
index range scan. Revises 0005.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the paired_execution_groups status / timestamp window index."""
    op.create_index(
        "ix_peg_status_timestamp",
        "paired_execution_groups",
        ["status", "timestamp"],
    )


def downgrade() -> None:
    """Drop the paired_execution_groups status / timestamp window index."""
    op.drop_index("ix_peg_status_timestamp", table_name="paired_execution_groups")
