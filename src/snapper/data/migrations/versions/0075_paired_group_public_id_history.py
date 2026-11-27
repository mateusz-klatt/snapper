"""Index all paired-execution group versions by public ID.

The outbox checks retained group history when deciding whether a command is
standalone. PostgreSQL builds and drops the index concurrently, and both steps
are idempotent. If a build is interrupted and leaves an invalid index, drop it
before retrying. Revises 0074.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0075"
down_revision: str | None = "0074"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "paired_execution_groups"
_INDEX = "ix_peg_public_id_history"


def upgrade() -> None:
    """Index the public ID of every retained group version."""
    with op.get_context().autocommit_block():
        op.create_index(
            _INDEX, _TABLE, ["public_id"], postgresql_concurrently=True, if_not_exists=True
        )


def downgrade() -> None:
    """Drop the history index."""
    with op.get_context().autocommit_block():
        op.drop_index(_INDEX, table_name=_TABLE, postgresql_concurrently=True, if_exists=True)
