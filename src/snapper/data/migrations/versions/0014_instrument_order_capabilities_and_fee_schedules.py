"""Add instrument_order_capabilities and venue_fee_schedules tables.

Phase 0 of the Execution Plans framework: the capability matrix and
fee schedule tables that evaluators consult to gate plan creation,
select order types, and estimate profitability.

Both tables are bitemporal (TemporalMixin) per project policy.
Volume is managed by the nightly archiver with per-table retention.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

revision: str = "0014"
down_revision: str = "0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"
_CK_EXCHANGE_LOWER = "exchange = LOWER(exchange)"


def upgrade() -> None:
    """Create instrument_order_capabilities and venue_fee_schedules tables."""
    op.create_table(
        "instrument_order_capabilities",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("supported_order_types", sa.JSON(), nullable=False),
        sa.Column("supports_post_only", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("supports_reduce_only", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("supports_amend_in_place", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("supports_native_stop_loss", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("supports_native_take_profit", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column(
            "supports_trailing_stop_client_side",
            sa.Boolean(),
            nullable=False,
            server_default="1",
        ),
        sa.Column("supports_market_making", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("supports_short_selling", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("supports_leverage", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("max_leverage_long", sa.Float(), nullable=False, server_default="1.0"),
        sa.Column("max_leverage_short", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("min_notional", sa.Float(), nullable=True),
        sa.Column("max_order_size", sa.Float(), nullable=True),
        sa.Column("top_of_book_quality", sa.String(16), nullable=False, server_default="unknown"),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_ioc_exchange_lower"),
        sa.CheckConstraint(
            "top_of_book_quality IN ('realtime', 'polled', 'thin', 'unknown')",
            name="ck_ioc_tob_quality",
        ),
    )
    op.create_index(
        "ix_ioc_instrument_exchange_active",
        "instrument_order_capabilities",
        ["instrument_public_id", "exchange"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_ioc_public_id",
        "instrument_order_capabilities",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_ioc_instrument_public_id",
        "instrument_order_capabilities",
        ["instrument_public_id"],
    )

    op.create_table(
        "venue_fee_schedules",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=True),
        sa.Column("fee_tier", sa.String(32), nullable=False),
        sa.Column("maker_bps", sa.Float(), nullable=False),
        sa.Column("taker_bps", sa.Float(), nullable=False),
        sa.Column("min_volume_30d", sa.Float(), nullable=True),
        sa.Column("currency", sa.String(8), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_vfs_exchange_lower"),
    )
    op.create_index(
        "ix_vfs_exchange_tier_active",
        "venue_fee_schedules",
        ["exchange", "fee_tier"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_vfs_public_id",
        "venue_fee_schedules",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_vfs_exchange",
        "venue_fee_schedules",
        ["exchange"],
    )


def downgrade() -> None:
    """Drop instrument_order_capabilities and venue_fee_schedules tables."""
    op.drop_table("venue_fee_schedules")
    op.drop_table("instrument_order_capabilities")
