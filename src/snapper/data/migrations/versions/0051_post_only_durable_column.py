"""Carry post-only through the durable order path.

``post_only`` was accepted at both entry surfaces and written only into the
execution plan's JSON params, so it never reached the executor: the command row
had no column, ``OrderRequestData`` had no field, and
``_exchange_order_request_from_core`` therefore had nothing to copy. A caller
asking for maker-only placement got a plain order that could take liquidity.

The flag is stored on ``trade_commands`` and ``orders`` beside ``leverage`` and
``reduce_only``, which already travel that path, so the three execution
modifiers now share one shape. Boolean NOT NULL defaulting false: every existing
row predates the feature and none of them requested maker-only placement, so the
backfill value is the truth rather than a guess. Revises 0050.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0051"
down_revision: str | None = "0050"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLES = ("trade_commands", "orders")
_COLUMN = "post_only"


def upgrade() -> None:
    """Add the ``post_only`` column to both durable order tables.

    Returns:
        None.
    """
    for table in _TABLES:
        op.add_column(
            table,
            sa.Column(
                _COLUMN,
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("false"),
            ),
        )


def downgrade() -> None:
    """Drop the ``post_only`` column from both durable order tables.

    Returns:
        None.
    """
    for table in _TABLES:
        op.drop_column(table, _COLUMN)
