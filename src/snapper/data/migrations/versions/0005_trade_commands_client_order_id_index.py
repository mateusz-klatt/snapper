"""Add a trade_commands (client_order_id) index for compensation-fill routing.

The paired-execution guard's compensation-fill projection routes a
reduce-only FLATTEN order's venue fill back to its leg by resolving the active
``trade_commands`` row whose ``client_order_id`` is the flatten order's fresh
id, then following ``supersedes_command_id`` to the leg's original command. The
live fill hook performs this lookup once per fill that matched no leg directly
(i.e. most fills while the guard is enabled), so without an index on
``client_order_id`` that lookup would seq-scan an append-only, long-lived
table. This single-column index makes it an index range scan; it also benefits
the pre-existing ``client_order_id`` plan / order resolvers. Revises 0004.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the trade_commands client-order lookup index."""
    op.create_index(
        "ix_trade_commands_client_order_id",
        "trade_commands",
        ["client_order_id"],
    )


def downgrade() -> None:
    """Drop the trade_commands client-order lookup index."""
    op.drop_index("ix_trade_commands_client_order_id", table_name="trade_commands")
