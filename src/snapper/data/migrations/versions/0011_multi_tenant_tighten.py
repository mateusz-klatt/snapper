"""Multi-tenant foundation: tighten wallet_public_id to NOT NULL on 8 write-path tables.

Migration 0010 added nullable ``wallet_public_id`` /
``operator_public_id`` / ``user_public_id`` columns to the 9
write-path tables so that the initial multi-tenant schema could
ship without breaking every existing callsite. A follow-up
refactor moved executors to per-wallet instances, wired the wallet
cache into the trader coordinator, and switched every production
insert path to populate ``wallet_public_id``. This migration
closes the loop:

1. ALTER ``wallet_public_id`` to ``NOT NULL`` on the 8 write-path
   tables that genuinely know their wallet at insert time.
   ``process_runs`` stays nullable because broker / market-data
   feeder processes are wallet-agnostic.

2. DROP + CREATE the ``positions`` unique index to include
   ``wallet_public_id``. Two wallets holding the same instrument
   in the same mode must coexist; the same wallet must still
   conflict.

3. DROP + CREATE the ``accrual_ledger`` unique + recovery indexes
   with ``wallet_public_id`` PREPENDED. Per-wallet accrual queries
   hit the prepended column for index sargability, and the unique
   key no longer bleeds across wallets.

Tables tightened (8):

- ``positions`` / ``orders`` / ``executions`` / ``signals`` /
  ``trade_commands`` / ``venue_events`` /
  ``trade_projection_checkpoints`` / ``accrual_ledger``

Tables NOT tightened:

- ``process_runs`` — broker/feeds stay wallet-agnostic.
- ``orders.operator_public_id`` / ``orders.user_public_id``,
  ``signals.operator_public_id``,
  ``trade_commands.{operator,user}_public_id``,
  ``accrual_ledger.operator_public_id`` — stay nullable because
  strategy-emitted rows carry NULL operator/user.

The pre-launch DB is disposable, so there is no backfill step:
the migration assumes every existing row already has a populated
wallet, which is true after the per-wallet executor refactor shipped
and ``make migrate-dev`` was re-run against a clean DB.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0011"
down_revision: str = "0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TIGHTENED_TABLES: tuple[str, ...] = (
    "positions",
    "orders",
    "executions",
    "signals",
    "trade_commands",
    "venue_events",
    "trade_projection_checkpoints",
    "accrual_ledger",
)

_KNOWN_TO_ACTIVE_SQLITE: str = "known_to = '9999-12-31 23:59:59.000000'"
_KNOWN_TO_ACTIVE_PG: str = "known_to = '9999-12-31T23:59:59+00:00'"


def upgrade() -> None:
    """Tighten wallet_public_id NOT NULL + rebuild positions/accrual_ledger indexes."""
    for table_name in _TIGHTENED_TABLES:
        with op.batch_alter_table(table_name, recreate="auto") as batch_op:
            batch_op.alter_column(
                "wallet_public_id",
                existing_type=sa.String(36),
                nullable=False,
            )

    with op.batch_alter_table("positions", recreate="auto") as batch_op:
        batch_op.drop_index("uq_positions_instrument_public_id")
        batch_op.create_index(
            "uq_positions_instrument_public_id",
            ["instrument_public_id", "mode", "wallet_public_id"],
            unique=True,
            sqlite_where=sa.text(_KNOWN_TO_ACTIVE_SQLITE),
            postgresql_where=sa.text(_KNOWN_TO_ACTIVE_PG),
        )

    with op.batch_alter_table("accrual_ledger", recreate="auto") as batch_op:
        batch_op.drop_index("ix_accrual_ledger_unique_active")
        batch_op.create_index(
            "ix_accrual_ledger_unique_active",
            [
                "wallet_public_id",
                "instrument_public_id",
                "mode",
                "accrual_type",
                "accrued_at",
            ],
            unique=True,
            sqlite_where=sa.text(_KNOWN_TO_ACTIVE_SQLITE),
            postgresql_where=sa.text(_KNOWN_TO_ACTIVE_PG),
        )
        batch_op.drop_index("ix_accrual_ledger_recovery")
        batch_op.create_index(
            "ix_accrual_ledger_recovery",
            [
                "wallet_public_id",
                "instrument_public_id",
                "mode",
                "accrued_at",
            ],
        )


def downgrade() -> None:
    """Revert NOT NULL + restore the original indexes."""
    with op.batch_alter_table("accrual_ledger", recreate="auto") as batch_op:
        batch_op.drop_index("ix_accrual_ledger_recovery")
        batch_op.create_index(
            "ix_accrual_ledger_recovery",
            ["instrument_public_id", "mode", "accrued_at"],
        )
        batch_op.drop_index("ix_accrual_ledger_unique_active")
        batch_op.create_index(
            "ix_accrual_ledger_unique_active",
            ["instrument_public_id", "mode", "accrual_type", "accrued_at"],
            unique=True,
            sqlite_where=sa.text(_KNOWN_TO_ACTIVE_SQLITE),
            postgresql_where=sa.text(_KNOWN_TO_ACTIVE_PG),
        )

    with op.batch_alter_table("positions", recreate="auto") as batch_op:
        batch_op.drop_index("uq_positions_instrument_public_id")
        batch_op.create_index(
            "uq_positions_instrument_public_id",
            ["instrument_public_id", "mode"],
            unique=True,
            sqlite_where=sa.text(_KNOWN_TO_ACTIVE_SQLITE),
            postgresql_where=sa.text(_KNOWN_TO_ACTIVE_PG),
        )

    for table_name in reversed(_TIGHTENED_TABLES):
        with op.batch_alter_table(table_name, recreate="auto") as batch_op:
            batch_op.alter_column(
                "wallet_public_id",
                existing_type=sa.String(36),
                nullable=True,
            )
