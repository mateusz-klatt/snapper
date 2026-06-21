"""Add the shadow_candles table for trade-built candle A/B persistence.

The table stores candidate candles separately from the live ``candles`` plane.
Its natural key includes ``source`` so trade-built rows cannot collide with
venue-native rows, and no live read path queries this table.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

revision: str = "0013"
down_revision: str | None = "0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"
_CK_SHADOW_CANDLE_SOURCE = "source IN ('native', 'calculated', 'synthesized')"


def _uuid_col() -> sa.types.TypeEngine[str]:
    """Build the per-dialect public identity column type.

    Returns:
        SQLAlchemy type using native PostgreSQL UUID and SQLite text storage.
    """
    return sa.String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql")


def upgrade() -> None:
    """Create shadow_candles and its active-row uniqueness indexes.

    Returns:
        None.
    """
    op.create_table(
        "shadow_candles",
        sa.Column(
            "id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            autoincrement=True,
            nullable=False,
        ),
        sa.Column("instrument_public_id", _uuid_col(), nullable=False),
        sa.Column("open_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("timeframe", sa.String(8), nullable=False),
        sa.Column("open", sa.Float(), nullable=False),
        sa.Column("high", sa.Float(), nullable=False),
        sa.Column("low", sa.Float(), nullable=False),
        sa.Column("close", sa.Float(), nullable=False),
        sa.Column("volume", sa.Float(), nullable=False),
        sa.Column("vwap", sa.Float(), nullable=True),
        sa.Column("trades", sa.Integer(), nullable=True),
        sa.Column("source", sa.String(16), nullable=False, server_default="native"),
        sa.Column("complete", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("session_id", _uuid_col(), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SHADOW_CANDLE_SOURCE, name="ck_shadow_candle_source"),
    )
    op.create_index(
        "uq_shadow_candle_itf_open_source",
        "shadow_candles",
        ["instrument_public_id", "timeframe", "open_at", "source"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_shadow_candles_public_id",
        "shadow_candles",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_shadow_candle_instrument_open_source",
        "shadow_candles",
        ["instrument_public_id", "open_at", "source"],
    )
    op.create_index(
        "ix_shadow_candles_instrument_public_id",
        "shadow_candles",
        ["instrument_public_id"],
    )


def downgrade() -> None:
    """Drop the shadow_candles table.

    Returns:
        None.
    """
    op.drop_table("shadow_candles")
