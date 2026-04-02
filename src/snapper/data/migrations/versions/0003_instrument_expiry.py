"""Add expiry_at and instrument_kind columns to instrument_specs.

Extends InstrumentSpec with contract expiry timestamp and product type
for front-month rollover and continuous contract support.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"
_CK_KIND = (
    "instrument_kind IN ('spot', 'perpetual', 'future', 'etf', 'option') "
    "OR instrument_kind IS NULL"
)

revision: str = "0003"
down_revision: str = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add expiry_at and instrument_kind to instrument_specs."""
    with op.batch_alter_table("instrument_specs", recreate="auto") as batch_op:
        batch_op.add_column(
            sa.Column("expiry_at", sa.DateTime(timezone=True), nullable=True),
        )
        batch_op.add_column(
            sa.Column("instrument_kind", sa.String(16), nullable=True),
        )
        batch_op.create_check_constraint(
            "ck_instrument_specs_kind",
            _CK_KIND,
        )
    op.create_index(
        "ix_instrument_specs_expiry",
        "instrument_specs",
        ["expiry_at"],
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )


def downgrade() -> None:
    """Remove expiry_at and instrument_kind from instrument_specs."""
    op.drop_index("ix_instrument_specs_expiry", table_name="instrument_specs")
    with op.batch_alter_table("instrument_specs", recreate="auto") as batch_op:
        batch_op.drop_constraint("ck_instrument_specs_kind", type_="check")
        batch_op.drop_column("instrument_kind")
        batch_op.drop_column("expiry_at")
