"""Add the canonical P&L series plane (PnL Phase 5).

One storage-only SCD2 table, ``portfolio_pnl_points``, holds the epoch-relative
P&L series per (wallet, mode, valuation_ccy, point_time). v1 persists only the
``anchor`` row — the durable activation seed carrying the frozen opening basket,
legacy unattributed weights, and per-exchange scope watermark map — so on-demand
timeline reconstruction picks a stable t0 across reloads (accepted decision #6).
The continuous 1m ``sample`` writer, USD cash/position/equity valuation, and
drawdown are Phase-5B additions that reuse the same table (nullable columns, no
further migration). Purely additive: a brand-new table with no runtime writer,
observer, or public surface added here. Both dialects create the same
CHECK-constrained schema directly. Revises 0034.
"""

from collections.abc import Sequence
from datetime import datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

revision: str = "0035"
down_revision: str | None = "0034"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"
_CK_MODE_LIVE_PAPER = "mode IN ('live', 'paper')"
_CK_PNL_POINT_KIND = "point_kind IN ('anchor', 'sample')"
_CK_PNL_VALUATION_STATUS = "valuation_status IN ('complete', 'incomplete')"
_CK_PNL_ANCHOR_ZERO = (
    "point_kind != 'anchor' OR "
    "(realized_pnl = 0 AND fee_pnl = 0 AND accrual_pnl = 0 AND "
    "external_flow_adjustment = 0 AND opening_basket_json IS NOT NULL)"
)
_CK_PNL_VALUATION_COMPLETE = (
    "(valuation_status = 'complete' AND unrealized_pnl IS NOT NULL AND "
    "mark_source IS NOT NULL AND mark_time IS NOT NULL) OR "
    "(valuation_status = 'incomplete' AND unrealized_pnl IS NULL)"
)


def _uuid_col() -> sa.types.TypeEngine[str]:
    """Build the per-dialect public identity column type.

    Returns:
        SQLAlchemy type using native PostgreSQL UUID and SQLite text storage.
    """
    return sa.String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql")


def _temporal_columns() -> tuple[
    sa.Column[int],
    sa.Column[str],
    sa.Column[str],
    sa.Column[int],
    sa.Column[datetime],
    sa.Column[datetime],
]:
    """Build the standard temporal columns shared by SCD2 tables.

    Returns:
        Fresh SQLAlchemy column objects for the table.
    """
    return (
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("session_id", _uuid_col(), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
    )


def upgrade() -> None:
    """Create the ``portfolio_pnl_points`` table and its indexes.

    Returns:
        None.
    """
    op.create_table(
        "portfolio_pnl_points",
        sa.Column("wallet_public_id", _uuid_col(), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False, server_default="live"),
        sa.Column("valuation_ccy", sa.String(16), nullable=False),
        sa.Column("point_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("point_kind", sa.String(16), nullable=False),
        sa.Column("epoch_public_id", _uuid_col(), nullable=False),
        sa.Column("calc_version", sa.String(16), nullable=False),
        sa.Column("valuation_status", sa.String(16), nullable=False),
        sa.Column("realized_pnl", sa.Float(), nullable=False),
        sa.Column("fee_pnl", sa.Float(), nullable=False),
        sa.Column("accrual_pnl", sa.Float(), nullable=False),
        sa.Column("unrealized_pnl", sa.Float(), nullable=True),
        sa.Column("external_flow_adjustment", sa.Float(), nullable=False),
        sa.Column("cash_usd", sa.Float(), nullable=True),
        sa.Column("position_value_usd", sa.Float(), nullable=True),
        sa.Column("drawdown", sa.Float(), nullable=True),
        sa.Column("mark_source", sa.String(32), nullable=True),
        sa.Column("mark_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("watermarks_json", sa.Text(), nullable=True),
        sa.Column("opening_basket_json", sa.Text(), nullable=True),
        sa.Column("contributions_json", sa.Text(), nullable=True),
        *_temporal_columns(),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_MODE_LIVE_PAPER, name="ck_portfolio_pnl_points_mode"),
        sa.CheckConstraint(_CK_PNL_POINT_KIND, name="ck_portfolio_pnl_points_kind"),
        sa.CheckConstraint(
            _CK_PNL_VALUATION_STATUS, name="ck_portfolio_pnl_points_valuation_status"
        ),
        sa.CheckConstraint(_CK_PNL_ANCHOR_ZERO, name="ck_portfolio_pnl_points_anchor_zero"),
        sa.CheckConstraint(
            _CK_PNL_VALUATION_COMPLETE, name="ck_portfolio_pnl_points_valuation_complete"
        ),
    )
    op.create_index(
        "uq_portfolio_pnl_points_identity",
        "portfolio_pnl_points",
        ["wallet_public_id", "mode", "valuation_ccy", "point_time"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_portfolio_pnl_points_public_id",
        "portfolio_pnl_points",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_portfolio_pnl_points_series",
        "portfolio_pnl_points",
        ["wallet_public_id", "mode", "epoch_public_id", "point_time"],
    )


def downgrade() -> None:
    """Drop the ``portfolio_pnl_points`` table.

    Returns:
        None.
    """
    op.drop_table("portfolio_pnl_points")
