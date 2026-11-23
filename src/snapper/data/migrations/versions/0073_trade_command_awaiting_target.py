"""Widen ``trade_commands.status`` CHECK to admit ``awaiting_target``.

Held id-less cancel commands inserted during stranded-plan startup recovery
must not enter ``created``, because the outbox polls ``status = 'created'`` in
single-page batches and large held backlogs would starve dispatchable rows.
Adding ``awaiting_target`` to ``ck_trade_commands_status`` lets unresolved
cancels persist outside the outbox fetch until durable evidence supplies a
venue order id and CAS-hydrates them to ``created``.

On SQLite, widening or narrowing a CHECK requires ``batch_alter_table(...,
recreate="always")``. On PostgreSQL, the CHECK swap runs inside an
``autocommit_block`` with ``NOT VALID`` followed by ``VALIDATE CONSTRAINT`` so
no long ``ACCESS EXCLUSIVE`` table-scan lock blocks concurrent readers or
writers. Revises 0072.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0073"
down_revision: str | None = "0072"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "trade_commands"
_CONSTRAINT = "ck_trade_commands_status"
_CK_OLD = (
    "status IN ('created', 'dispatched', 'direct_dispatched', 'accepted', "
    "'filled', 'partially_filled', 'rejected', 'cancelled', 'expired', 'failed')"
)
_CK_NEW = (
    "status IN ('created', 'awaiting_target', 'dispatched', 'direct_dispatched', 'accepted', "
    "'filled', 'partially_filled', 'rejected', 'cancelled', 'expired', 'failed')"
)


def _is_sqlite() -> bool:
    """Return True when the bound connection is SQLite."""
    return op.get_bind().dialect.name == "sqlite"


def _swap_status_check(expr: str) -> None:
    """Replace ``ck_trade_commands_status`` with ``expr`` for the active dialect."""
    if _is_sqlite():
        with op.batch_alter_table(_TABLE, recreate="always") as batch:
            batch.drop_constraint(_CONSTRAINT, type_="check")
            batch.create_check_constraint(_CONSTRAINT, expr)
        return
    with op.get_context().autocommit_block():
        op.execute(
            f"ALTER TABLE {_TABLE} "
            f"DROP CONSTRAINT IF EXISTS {_CONSTRAINT}, "
            f"ADD CONSTRAINT {_CONSTRAINT} CHECK ({expr}) NOT VALID"
        )
        op.execute(f"ALTER TABLE {_TABLE} VALIDATE CONSTRAINT {_CONSTRAINT}")


def upgrade() -> None:
    """Widen ``ck_trade_commands_status`` to include ``awaiting_target``."""
    _swap_status_check(_CK_NEW)


def downgrade() -> None:
    """Restore pre-0073 ``ck_trade_commands_status`` when no ``awaiting_target`` rows exist.

    Raises:
        RuntimeError: If any ``trade_commands`` row has ``status = 'awaiting_target'``.
    """
    if not op.get_context().as_sql:
        awaiting = int(
            op.get_bind()
            .execute(
                sa.text("SELECT count(*) FROM trade_commands WHERE status = 'awaiting_target'")
            )
            .scalar_one()
        )
        if awaiting:
            raise RuntimeError(
                f"refused: {awaiting} trade_commands row(s) have status 'awaiting_target'; "
                "narrowing ck_trade_commands_status would violate held-cancel state"
            )
    _swap_status_check(_CK_OLD)
