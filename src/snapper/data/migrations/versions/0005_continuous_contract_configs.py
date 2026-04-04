"""Add continuous_contract_configs table.

Stores saved configuration presets for continuous contract series
computation. The series data is computed on-demand, not stored.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"

revision: str = "0005"
down_revision: str = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create continuous_contract_configs table."""
    op.create_table(
        "continuous_contract_configs",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(64), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "known_to",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default="9999-12-31 23:59:59.000000",
        ),
        sa.Column("underlying_public_id", sa.String(36), nullable=False),
        sa.Column("exchange", sa.String(20), nullable=False),
        sa.Column("contract_family", sa.String(16), nullable=False),
        sa.Column("method", sa.String(16), nullable=False),
        sa.Column("rollover_days_before", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("label", sa.String(64), nullable=True),
        sa.CheckConstraint(
            "method IN ('unadjusted', 'ratio', 'panama')",
            name="ck_ccc_method",
        ),
    )
    op.create_index(
        "uq_ccc_active_key",
        "continuous_contract_configs",
        [
            "underlying_public_id",
            "exchange",
            "contract_family",
            "method",
            "rollover_days_before",
        ],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_ccc_public_id",
        "continuous_contract_configs",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )


def downgrade() -> None:
    """Drop continuous_contract_configs table."""
    op.drop_index("ix_ccc_public_id", table_name="continuous_contract_configs")
    op.drop_index("uq_ccc_active_key", table_name="continuous_contract_configs")
    op.drop_table("continuous_contract_configs")
