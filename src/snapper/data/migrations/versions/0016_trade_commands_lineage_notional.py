"""Add decision-lineage and admission-notional columns to trade_commands.

PnL Phase 1 (order truth & decision lineage): ``signal_public_id`` and
``ai_review_public_id`` link a durable command back to the signal that
fired it and the AI review that approved it (both nullable + indexed —
manual REST/MCP and plan-emitted commands carry no signal, cancels and
compensations carry neither). ``submitted_notional_usd`` snapshots the
USD notional the caps enforcer quoted at admission so market orders
(``price IS NULL``) finally count against the rolling 24h notional cap.
All three columns are additive and nullable: no backfill is possible
(the submit-time valuation for historical rows no longer exists) and
SQLite test databases pick the columns up from the ORM model via
``create_all``. Revises 0015.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0016"
down_revision: str | None = "0015"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _uuid_col() -> sa.types.TypeEngine[str]:
    """Build the per-dialect public identity column type.

    Returns:
        SQLAlchemy type using native PostgreSQL UUID and SQLite text storage.
    """
    return sa.String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql")


def upgrade() -> None:
    """Add the two lineage columns (+ indexes) and the notional column.

    Returns:
        None.
    """
    op.add_column("trade_commands", sa.Column("signal_public_id", _uuid_col(), nullable=True))
    op.add_column("trade_commands", sa.Column("ai_review_public_id", _uuid_col(), nullable=True))
    op.add_column(
        "trade_commands",
        sa.Column("submitted_notional_usd", sa.Numeric(18, 2), nullable=True),
    )
    op.create_index("ix_trade_commands_signal_public_id", "trade_commands", ["signal_public_id"])
    op.create_index(
        "ix_trade_commands_ai_review_public_id", "trade_commands", ["ai_review_public_id"]
    )


def downgrade() -> None:
    """Drop the lineage indexes and the three Phase 1 columns.

    Returns:
        None.
    """
    op.drop_index("ix_trade_commands_ai_review_public_id", table_name="trade_commands")
    op.drop_index("ix_trade_commands_signal_public_id", table_name="trade_commands")
    op.drop_column("trade_commands", "submitted_notional_usd")
    op.drop_column("trade_commands", "ai_review_public_id")
    op.drop_column("trade_commands", "signal_public_id")
