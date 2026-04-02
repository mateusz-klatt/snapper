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

revision: str = "0003"
down_revision: str = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add expiry_at and instrument_kind to instrument_specs."""
    op.add_column(
        "instrument_specs",
        sa.Column("expiry_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "instrument_specs",
        sa.Column("instrument_kind", sa.String(16), nullable=True),
    )
    op.create_check_constraint(
        "ck_instrument_specs_kind",
        "instrument_specs",
        "instrument_kind IN ('spot', 'perpetual', 'future', 'etf', 'option') OR instrument_kind IS NULL",
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
    op.drop_constraint("ck_instrument_specs_kind", "instrument_specs", type_="check")
    op.drop_column("instrument_specs", "instrument_kind")
    op.drop_column("instrument_specs", "expiry_at")
