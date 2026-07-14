"""Add immutable spot replay anchors and raw execution decimal evidence.

The anchor plane stores the exact bootstrap inventory as canonical decimal
strings inside a text JSON document. PostgreSQL and SQLite therefore receive
the same bytes without SQLite NUMERIC affinity converting them through a
binary float. Execution raw strings remain nullable for pre-migration rows.
Revises 0024.
"""

from collections.abc import Sequence
from datetime import datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

revision: str = "0025"
down_revision: str | None = "0024"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"
_CK_EXECUTION_PROVENANCE = (
    "numeric_provenance IS NULL OR numeric_provenance IN ('venue_raw', 'legacy_float')"
)
_CK_EXECUTION_RAW = (
    "(price_decimal IS NULL OR LENGTH(TRIM(price_decimal)) > 0) AND "
    "(size_decimal IS NULL OR LENGTH(TRIM(size_decimal)) > 0) AND "
    "(fee_decimal IS NULL OR LENGTH(TRIM(fee_decimal)) > 0) AND "
    "((price_decimal IS NULL AND size_decimal IS NULL AND fee_decimal IS NULL) OR "
    "numeric_provenance IS NOT NULL)"
)


def _uuid_col() -> sa.types.TypeEngine[str]:
    """Build native PostgreSQL UUID with SQLite text fallback."""
    return sa.String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql")


def _watermark_col() -> sa.types.TypeEngine[int]:
    """Build PostgreSQL BIGINT with SQLite integer-compatible storage."""
    return sa.BigInteger().with_variant(sa.Integer(), "sqlite")


def _temporal_columns() -> tuple[
    sa.Column[int],
    sa.Column[str],
    sa.Column[str],
    sa.Column[int],
    sa.Column[datetime],
    sa.Column[datetime],
]:
    """Build the standard temporal envelope for one anchor table."""
    return (
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("session_id", _uuid_col(), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
    )


def _execution_columns() -> tuple[sa.Column[str], sa.Column[str], sa.Column[str], sa.Column[str]]:
    """Build fresh nullable execution evidence columns."""
    return (
        sa.Column("price_decimal", sa.Text(), nullable=True),
        sa.Column("size_decimal", sa.Text(), nullable=True),
        sa.Column("fee_decimal", sa.Text(), nullable=True),
        sa.Column("numeric_provenance", sa.String(16), nullable=True),
    )


def _is_sqlite() -> bool:
    """Return whether the migration is executing against SQLite."""
    return op.get_bind().dialect.name == "sqlite"


def upgrade() -> None:
    """Create the anchor plane and extend execution numeric evidence."""
    op.create_table(
        "portfolio_spot_reconciliation_anchors",
        sa.Column("wallet_public_id", _uuid_col(), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False, server_default="live"),
        sa.Column("venue_account_state_public_id", _uuid_col(), nullable=False),
        sa.Column("balance_observation_id", sa.Integer(), nullable=False),
        sa.Column("source_watermark_kind", sa.String(32), nullable=False),
        sa.Column("source_watermark", _watermark_col(), nullable=False),
        sa.Column("balances_json", sa.Text(), nullable=False),
        sa.Column("first_request_started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("first_request_completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("second_request_started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("second_request_completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("boundary_status", sa.String(24), nullable=False),
        sa.Column("inventory_status", sa.String(24), nullable=False),
        sa.Column("margin_status", sa.String(24), nullable=False),
        sa.Column("provenance", sa.String(128), nullable=False),
        *_temporal_columns(),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "exchange = LOWER(exchange)", name="ck_portfolio_spot_anchor_exchange_lower"
        ),
        sa.CheckConstraint("mode = 'live'", name="ck_portfolio_spot_anchor_mode"),
        sa.CheckConstraint(
            "source_watermark_kind = 'execution_id' AND source_watermark >= 0",
            name="ck_portfolio_spot_anchor_watermark",
        ),
        sa.CheckConstraint(
            "LENGTH(TRIM(balances_json)) > 0 AND LENGTH(TRIM(provenance)) > 0",
            name="ck_portfolio_spot_anchor_evidence_text",
        ),
        sa.CheckConstraint(
            "balance_observation_id > 0", name="ck_portfolio_spot_anchor_observation"
        ),
        sa.CheckConstraint(
            "first_request_completed_at >= first_request_started_at AND "
            "second_request_started_at >= first_request_completed_at AND "
            "second_request_completed_at >= second_request_started_at AND "
            "timestamp >= second_request_completed_at",
            name="ck_portfolio_spot_anchor_timestamp_order",
        ),
        sa.CheckConstraint(
            "boundary_status IN ('cursor_certified', 'double_read_equal', 'uncertified')",
            name="ck_portfolio_spot_anchor_boundary_status",
        ),
        sa.CheckConstraint(
            "inventory_status IN ('certified_full', 'uncertified', 'suspect_partial')",
            name="ck_portfolio_spot_anchor_inventory_status",
        ),
        sa.CheckConstraint(
            "margin_status IN ('cash', 'unsupported_margin', 'unknown')",
            name="ck_portfolio_spot_anchor_margin_status",
        ),
    )
    op.create_index(
        "uq_portfolio_spot_reconciliation_anchors_identity",
        "portfolio_spot_reconciliation_anchors",
        ["wallet_public_id", "exchange", "mode"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_portfolio_spot_reconciliation_anchors_public_id",
        "portfolio_spot_reconciliation_anchors",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    if _is_sqlite():
        with op.batch_alter_table("executions", recreate="always") as batch:
            for column in _execution_columns():
                batch.add_column(column)
            batch.create_check_constraint(
                "ck_executions_numeric_provenance", _CK_EXECUTION_PROVENANCE
            )
            batch.create_check_constraint("ck_executions_raw_decimals", _CK_EXECUTION_RAW)
        return
    for column in _execution_columns():
        op.add_column("executions", column)
    op.create_check_constraint(
        "ck_executions_numeric_provenance", "executions", _CK_EXECUTION_PROVENANCE
    )
    op.create_check_constraint("ck_executions_raw_decimals", "executions", _CK_EXECUTION_RAW)


def downgrade() -> None:
    """Remove execution raw evidence and the immutable anchor plane."""
    column_names = ("numeric_provenance", "fee_decimal", "size_decimal", "price_decimal")
    if _is_sqlite():
        with op.batch_alter_table("executions", recreate="always") as batch:
            batch.drop_constraint("ck_executions_raw_decimals", type_="check")
            batch.drop_constraint("ck_executions_numeric_provenance", type_="check")
            for column_name in column_names:
                batch.drop_column(column_name)
    else:
        op.drop_constraint("ck_executions_raw_decimals", "executions", type_="check")
        op.drop_constraint("ck_executions_numeric_provenance", "executions", type_="check")
        for column_name in column_names:
            op.drop_column("executions", column_name)
    op.drop_table("portfolio_spot_reconciliation_anchors")
