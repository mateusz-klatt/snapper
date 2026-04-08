"""Add nullable ``operator_public_id`` column to executions + checkpoints.

Operator plumbing into the remaining recovery source tables.
``_recover_from_checkpoints`` and ``_recover_from_executions``
used to create engines with empty ``operator_public_id`` because
neither source row carried the operator attribution. An earlier
backfill in ``_recover_active_orders`` covers the case where an
active order follows the recovered engine, but engines whose only
source is a checkpoint or an execution row still end up with
empty operator until the next signal arrives.

This migration closes the remaining gap by adding the column to
both source tables. The column stays nullable because
strategy-emitted orders can legitimately have no operator, and
the existing ``Execution.wallet_public_id`` NOT NULL constraint
is sufficient for routing. Population happens on every insert
path:

- ``insert_execution`` accepts a new optional ``operator_public_id``
  parameter that the exchange client's ``_log_execution_to_db``
  reads from the pending order state.
- ``upsert_checkpoint`` accepts the field via the
  ``CheckpointUpsertRow`` TypedDict; the trader coordinator
  populates it from the engine's current ``operator_public_id``.

No index changes — operator is attribution metadata, not a
routing key. Lookups happen by ``wallet_public_id`` and
``shard_key`` as before.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0013"
down_revision: str = "0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add nullable ``operator_public_id`` to executions + checkpoints."""
    with op.batch_alter_table("executions", recreate="auto") as batch_op:
        batch_op.add_column(sa.Column("operator_public_id", sa.String(36), nullable=True))

    with op.batch_alter_table("trade_projection_checkpoints", recreate="auto") as batch_op:
        batch_op.add_column(sa.Column("operator_public_id", sa.String(36), nullable=True))


def downgrade() -> None:
    """Drop the operator_public_id column from both tables."""
    with op.batch_alter_table("trade_projection_checkpoints", recreate="auto") as batch_op:
        batch_op.drop_column("operator_public_id")

    with op.batch_alter_table("executions", recreate="auto") as batch_op:
        batch_op.drop_column("operator_public_id")
