"""Add channel-specific market-data capabilities."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

revision: str = "0014"
down_revision: str | None = "0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"
_CK_EXCHANGE_LOWER = "exchange = LOWER(exchange)"
_CK_CHANNEL_LOWER_NON_EMPTY = "channel = LOWER(channel) AND LENGTH(channel) > 0"


def _uuid_col() -> sa.types.TypeEngine[str]:
    """Build the per-dialect public identity column type.

    Returns:
        SQLAlchemy type using native PostgreSQL UUID and SQLite text storage.
    """
    return sa.String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql")


def upgrade() -> None:
    """Create symbol_market_data_channel_capabilities and active indexes.

    Returns:
        None.
    """
    op.create_table(
        "symbol_market_data_channel_capabilities",
        sa.Column("symbol_public_id", _uuid_col(), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("channel", sa.String(64), nullable=False),
        sa.Column("can_market_data", sa.Boolean(), nullable=False),
        sa.Column("source", sa.String(64), nullable=True),
        sa.Column("reason", sa.String(1024), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("session_id", _uuid_col(), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_smdcc_exchange_lower"),
        sa.CheckConstraint(
            _CK_CHANNEL_LOWER_NON_EMPTY,
            name="ck_smdcc_channel_lower_non_empty",
        ),
    )
    op.create_index(
        "uq_smdcc_symbol_exchange_channel",
        "symbol_market_data_channel_capabilities",
        ["symbol_public_id", "exchange", "channel"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_symbol_market_data_channel_capabilities_public_id",
        "symbol_market_data_channel_capabilities",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_symbol_market_data_channel_capabilities_symbol_public_id",
        "symbol_market_data_channel_capabilities",
        ["symbol_public_id"],
    )
    op.create_index(
        "ix_smdcc_exchange_channel",
        "symbol_market_data_channel_capabilities",
        ["exchange", "channel"],
    )


def downgrade() -> None:
    """Drop the channel-capability table.

    Returns:
        None.
    """
    op.drop_table("symbol_market_data_channel_capabilities")
