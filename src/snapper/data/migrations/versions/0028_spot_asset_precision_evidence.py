"""Add durable per-asset spot precision evidence.

The SCD2 table preserves venue-scoped balance and fee decimal observations
without certifying missing precision fields. Reconciliation applies freshness
and completeness checks when it consumes these rows. Revises 0027.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

revision: str = "0028"
down_revision: str | None = "0027"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"


def _uuid_col() -> sa.types.TypeEngine[str]:
    """Build native PostgreSQL UUID with SQLite text fallback."""
    return sa.String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql")


def upgrade() -> None:
    """Create the per-exchange asset precision evidence plane."""
    op.create_table(
        "spot_asset_precision_evidence",
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("asset", sa.String(16), nullable=False),
        sa.Column("balance_decimals", sa.Integer(), nullable=True),
        sa.Column("balance_decimals_max", sa.Integer(), nullable=True),
        sa.Column("balance_max_source", sa.String(128), nullable=True),
        sa.Column("balance_max_version", sa.String(96), nullable=True),
        sa.Column("balance_max_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("balance_source", sa.String(128), nullable=True),
        sa.Column("balance_version", sa.String(96), nullable=True),
        sa.Column("balance_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("fee_decimals", sa.Integer(), nullable=True),
        sa.Column("fee_source", sa.String(128), nullable=True),
        sa.Column("fee_version", sa.String(96), nullable=True),
        sa.Column("fee_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("session_id", _uuid_col(), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "exchange = LOWER(exchange)",
            name="ck_spot_asset_precision_evidence_exchange_lower",
        ),
        sa.CheckConstraint(
            "LENGTH(TRIM(asset)) > 0 AND asset = TRIM(asset) AND "
            "(balance_source IS NULL OR (LENGTH(TRIM(balance_source)) > 0 AND "
            "balance_source = TRIM(balance_source))) AND "
            "(balance_version IS NULL OR (LENGTH(TRIM(balance_version)) > 0 AND "
            "balance_version = TRIM(balance_version))) AND "
            "(balance_max_source IS NULL OR (LENGTH(TRIM(balance_max_source)) > 0 AND "
            "balance_max_source = TRIM(balance_max_source))) AND "
            "(balance_max_version IS NULL OR (LENGTH(TRIM(balance_max_version)) > 0 AND "
            "balance_max_version = TRIM(balance_max_version))) AND "
            "(fee_source IS NULL OR (LENGTH(TRIM(fee_source)) > 0 AND "
            "fee_source = TRIM(fee_source))) AND "
            "(fee_version IS NULL OR (LENGTH(TRIM(fee_version)) > 0 AND "
            "fee_version = TRIM(fee_version)))",
            name="ck_spot_asset_precision_evidence_text",
        ),
        sa.CheckConstraint(
            "(balance_source IS NULL AND balance_version IS NULL AND "
            "balance_observed_at IS NULL AND balance_decimals IS NULL) OR "
            "(balance_source IS NOT NULL AND balance_version IS NOT NULL AND "
            "balance_observed_at IS NOT NULL)",
            name="ck_spot_asset_precision_evidence_balance_provenance",
        ),
        sa.CheckConstraint(
            "(balance_decimals_max IS NULL AND balance_max_source IS NULL AND "
            "balance_max_version IS NULL AND balance_max_observed_at IS NULL) OR "
            "(balance_decimals_max IS NOT NULL AND balance_max_source IS NOT NULL AND "
            "balance_max_version IS NOT NULL AND balance_max_observed_at IS NOT NULL)",
            name="ck_spot_asset_precision_evidence_balance_max_provenance",
        ),
        sa.CheckConstraint(
            "(fee_source IS NULL AND fee_version IS NULL AND fee_observed_at IS NULL AND "
            "fee_decimals IS NULL) OR (fee_source IS NOT NULL AND fee_version IS NOT NULL "
            "AND fee_observed_at IS NOT NULL)",
            name="ck_spot_asset_precision_evidence_fee_provenance",
        ),
        sa.CheckConstraint(
            "balance_observed_at IS NOT NULL OR fee_observed_at IS NOT NULL",
            name="ck_spot_asset_precision_evidence_observed_plane",
        ),
        sa.CheckConstraint(
            "(balance_decimals IS NULL OR balance_decimals BETWEEN 0 AND 256) AND "
            "(balance_decimals_max IS NULL OR balance_decimals_max BETWEEN 0 AND 256) AND "
            "(fee_decimals IS NULL OR fee_decimals BETWEEN 0 AND 256)",
            name="ck_spot_asset_precision_evidence_decimals",
        ),
        sa.CheckConstraint(
            "balance_decimals IS NULL OR (balance_decimals_max IS NOT NULL AND "
            "balance_decimals_max >= balance_decimals)",
            name="ck_spot_asset_precision_evidence_balance_ratchet",
        ),
    )
    op.create_index(
        "uq_spot_asset_precision_evidence_exchange_asset",
        "spot_asset_precision_evidence",
        ["exchange", "asset"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_spot_asset_precision_evidence_public_id",
        "spot_asset_precision_evidence",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_spot_asset_precision_evidence_temporal",
        "spot_asset_precision_evidence",
        ["exchange", "asset", "known_to", "timestamp"],
    )


def downgrade() -> None:
    """Remove the per-asset spot precision evidence plane."""
    op.drop_table("spot_asset_precision_evidence")
