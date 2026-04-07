"""Multi-tenant foundation: extend existing tables with wallet/operator columns.

Plan 0 / Phase 0a (multi-tenant foundation, step 2 of N).

Adds nullable ``wallet_public_id`` / ``operator_public_id`` / ``user_public_id``
columns to the 9 write-path tables and bumps ``shard_key`` from ``String(64)``
to ``String(256)`` on the 3 tables that carry it. Per Plan 0 D4 the pre-launch
DB is disposable, so this migration only ALTERs the existing schema; backfill
and tightening to NOT NULL happen later.

Why nullable now (vs. NOT NULL per Plan 0 Section 3.2):

- The default seed (User → Operator → Wallet → ScopeGrants) lands in step 4.
- Production callsites populate wallet_public_id once the per-wallet trader
  coordinator is wired up in step 4 / Phase 0c.
- Until both exist, NOT NULL would force every existing write-path test to
  invent a wallet UUID, exploding the blast radius of step 2.
- Step 5 of Phase 0a tightens these columns to NOT NULL after seed and
  callsites are in place. Index extensions (Position unique on
  wallet_public_id, AccrualLedger unique prepend) also move to step 5 — with
  NULL values present, both PostgreSQL and SQLite treat NULLs as distinct in
  unique indexes, which would silently weaken the dedup guarantee.

Tables touched (9):

- ``positions``  → +wallet_public_id
- ``orders``     → +wallet_public_id, +operator_public_id
- ``executions`` → +wallet_public_id
- ``signals``    → +wallet_public_id, +operator_public_id
- ``trade_commands`` → +wallet_public_id, +operator_public_id, +user_public_id,
  shard_key String(64)→String(256)
- ``venue_events`` → +wallet_public_id,
  shard_key String(64)→String(256)
- ``trade_projection_checkpoints`` → +wallet_public_id,
  shard_key String(64)→String(256)
- ``process_runs`` → +wallet_public_id (broker/feeds stay NULL)
- ``accrual_ledger`` → +wallet_public_id, +operator_public_id

Plan and rationale: ``proprietary/plans/plan_multi_tenant_foundation.md``
Sections 3.2, 3.3, 14.8.2, 14.8.3.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0010"
down_revision: str = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add nullable wallet/operator/user columns and bump shard_key lengths."""
    with op.batch_alter_table("positions", recreate="auto") as batch_op:
        batch_op.add_column(sa.Column("wallet_public_id", sa.String(36), nullable=True))

    with op.batch_alter_table("orders", recreate="auto") as batch_op:
        batch_op.add_column(sa.Column("wallet_public_id", sa.String(36), nullable=True))
        batch_op.add_column(sa.Column("operator_public_id", sa.String(36), nullable=True))

    with op.batch_alter_table("executions", recreate="auto") as batch_op:
        batch_op.add_column(sa.Column("wallet_public_id", sa.String(36), nullable=True))

    with op.batch_alter_table("signals", recreate="auto") as batch_op:
        batch_op.add_column(sa.Column("wallet_public_id", sa.String(36), nullable=True))
        batch_op.add_column(sa.Column("operator_public_id", sa.String(36), nullable=True))

    with op.batch_alter_table("trade_commands", recreate="auto") as batch_op:
        batch_op.add_column(sa.Column("wallet_public_id", sa.String(36), nullable=True))
        batch_op.add_column(sa.Column("operator_public_id", sa.String(36), nullable=True))
        batch_op.add_column(sa.Column("user_public_id", sa.String(36), nullable=True))
        batch_op.alter_column(
            "shard_key",
            existing_type=sa.String(64),
            type_=sa.String(256),
            existing_nullable=False,
        )

    with op.batch_alter_table("venue_events", recreate="auto") as batch_op:
        batch_op.add_column(sa.Column("wallet_public_id", sa.String(36), nullable=True))
        batch_op.alter_column(
            "shard_key",
            existing_type=sa.String(64),
            type_=sa.String(256),
            existing_nullable=False,
        )

    with op.batch_alter_table("trade_projection_checkpoints", recreate="auto") as batch_op:
        batch_op.add_column(sa.Column("wallet_public_id", sa.String(36), nullable=True))
        batch_op.alter_column(
            "shard_key",
            existing_type=sa.String(64),
            type_=sa.String(256),
            existing_nullable=False,
        )

    with op.batch_alter_table("process_runs", recreate="auto") as batch_op:
        batch_op.add_column(sa.Column("wallet_public_id", sa.String(36), nullable=True))

    with op.batch_alter_table("accrual_ledger", recreate="auto") as batch_op:
        batch_op.add_column(sa.Column("wallet_public_id", sa.String(36), nullable=True))
        batch_op.add_column(sa.Column("operator_public_id", sa.String(36), nullable=True))


def downgrade() -> None:
    """Drop the added columns and revert shard_key length bumps."""
    with op.batch_alter_table("accrual_ledger", recreate="auto") as batch_op:
        batch_op.drop_column("operator_public_id")
        batch_op.drop_column("wallet_public_id")

    with op.batch_alter_table("process_runs", recreate="auto") as batch_op:
        batch_op.drop_column("wallet_public_id")

    with op.batch_alter_table("trade_projection_checkpoints", recreate="auto") as batch_op:
        batch_op.alter_column(
            "shard_key",
            existing_type=sa.String(256),
            type_=sa.String(64),
            existing_nullable=False,
        )
        batch_op.drop_column("wallet_public_id")

    with op.batch_alter_table("venue_events", recreate="auto") as batch_op:
        batch_op.alter_column(
            "shard_key",
            existing_type=sa.String(256),
            type_=sa.String(64),
            existing_nullable=False,
        )
        batch_op.drop_column("wallet_public_id")

    with op.batch_alter_table("trade_commands", recreate="auto") as batch_op:
        batch_op.alter_column(
            "shard_key",
            existing_type=sa.String(256),
            type_=sa.String(64),
            existing_nullable=False,
        )
        batch_op.drop_column("user_public_id")
        batch_op.drop_column("operator_public_id")
        batch_op.drop_column("wallet_public_id")

    with op.batch_alter_table("signals", recreate="auto") as batch_op:
        batch_op.drop_column("operator_public_id")
        batch_op.drop_column("wallet_public_id")

    with op.batch_alter_table("executions", recreate="auto") as batch_op:
        batch_op.drop_column("wallet_public_id")

    with op.batch_alter_table("orders", recreate="auto") as batch_op:
        batch_op.drop_column("operator_public_id")
        batch_op.drop_column("wallet_public_id")

    with op.batch_alter_table("positions", recreate="auto") as batch_op:
        batch_op.drop_column("wallet_public_id")
