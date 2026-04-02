"""Add underlying_assets and instrument_underlying_mappings tables.

Introduces the UnderlyingAsset model (canonical asset identity) and the
InstrumentUnderlyingMapping table (temporal link from instruments to their
underlying asset). Tables start empty — populated by the underlying updater
from YAML definitions.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"
_CK_SESSION_ID = "session_id != ''"
_CK_SEQUENCE_ID = "sequence_id > 0"

revision: str = "0002"
down_revision: str = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create underlying_assets and instrument_underlying_mappings tables."""
    op.create_table(
        "underlying_assets",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("name", sa.String(64), nullable=False),
        sa.Column("ticker", sa.String(16), nullable=False),
        sa.Column("asset_class", sa.String(16), nullable=False),
        sa.Column("sector", sa.String(32), nullable=True),
        sa.Column("description", sa.String(256), nullable=True),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "asset_class IN ('crypto', 'forex', 'equity', 'index', 'commodity', 'yield')",
            name="ck_underlying_asset_class",
        ),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_underlying_assets_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_underlying_assets_sequence_id"),
    )
    op.create_index(
        "uq_underlying_assets_active_ticker",
        "underlying_assets",
        ["ticker"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "uq_underlying_assets_active_name",
        "underlying_assets",
        ["name"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_underlying_assets_public_id",
        "underlying_assets",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )

    op.create_table(
        "instrument_underlying_mappings",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=False),
        sa.Column("underlying_public_id", sa.String(36), nullable=False),
        sa.Column("relationship_type", sa.String(16), nullable=False),
        sa.Column("contract_family", sa.String(16), nullable=True),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "relationship_type IN ('exact', 'derivative', 'proxy')",
            name="ck_ium_relationship_type",
        ),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_ium_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_ium_sequence_id"),
    )
    op.create_index(
        "uq_ium_active_instrument",
        "instrument_underlying_mappings",
        ["instrument_public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_ium_underlying",
        "instrument_underlying_mappings",
        ["underlying_public_id"],
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_ium_family",
        "instrument_underlying_mappings",
        ["underlying_public_id", "contract_family"],
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_ium_instrument_public_id",
        "instrument_underlying_mappings",
        ["instrument_public_id"],
    )
    op.create_index(
        "ix_ium_underlying_public_id",
        "instrument_underlying_mappings",
        ["underlying_public_id"],
    )


def downgrade() -> None:
    """Drop underlying_assets and instrument_underlying_mappings tables."""
    op.drop_table("instrument_underlying_mappings")
    op.drop_table("underlying_assets")
