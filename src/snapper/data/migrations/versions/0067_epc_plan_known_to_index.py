"""Add a current-version lookup index for execution plan checkpoints.

Every reader of ``execution_plan_checkpoints`` asks for the version of one plan
that is open at an instant: ``plan_public_id = :plan AND timestamp <= :at AND
known_to > :at``. The only usable index was ``ix_epc_plan_public_id``, which
returns every version the plan ever had. A deployment with 6.8 million versions
and 15 current rows therefore visited several hundred thousand index entries
and heap rows per lookup to keep one, on a table that is read on every
checkpoint write. The composite ``(plan_public_id, known_to)`` B-tree turns the
same predicate into a range that holds only versions still open after ``:at``;
no query changes. The index is created ``CONCURRENTLY`` on PostgreSQL inside an
autocommit block so checkpoint writes are never blocked during the build; if a
concurrent build is interrupted PostgreSQL can leave an INVALID index behind,
which must be dropped before rerunning the upgrade. SQLite ignores the
dialect-specific flag and builds the index normally. Revises 0066.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0067"
down_revision: str | None = "0066"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "execution_plan_checkpoints"
_INDEX = "ix_epc_plan_known_to"
_COLUMNS = ("plan_public_id", "known_to")


def upgrade() -> None:
    """Create the open-version lookup index without blocking checkpoint writes."""
    with op.get_context().autocommit_block():
        op.create_index(
            _INDEX,
            _TABLE,
            list(_COLUMNS),
            postgresql_concurrently=True,
        )


def downgrade() -> None:
    """Drop the open-version lookup index."""
    with op.get_context().autocommit_block():
        op.drop_index(
            _INDEX,
            table_name=_TABLE,
            postgresql_concurrently=True,
        )
