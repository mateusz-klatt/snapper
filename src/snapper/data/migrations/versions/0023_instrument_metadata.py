"""Add venue-sourced unit and provenance evidence to instrument specifications.

The nullable metadata columns preserve honest absence for existing and unsupported
instruments. Certification is permanently default-false and database constraints
require positive contract size, contract-count units, and complete provenance before
the evidence flag may be stored as true. SQLite recreates the table through Alembic's
batch path while PostgreSQL applies the same columns and checks directly. Revises 0022.
"""

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal

import sqlalchemy as sa
from alembic import op

revision: str = "0023"
down_revision: str | None = "0022"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CK_CONTRACT_SIZE = "contract_size IS NULL OR CAST(contract_size AS NUMERIC) > 0"
_CK_QUANTITY_UNIT = "quantity_unit IS NULL OR quantity_unit IN ('base_asset', 'contract_count')"
_CK_PROVENANCE = (
    "(spec_source IS NULL AND spec_version IS NULL AND spec_observed_at IS NULL) OR "
    "(spec_source IS NOT NULL AND spec_version IS NOT NULL AND spec_observed_at IS NOT NULL)"
)
_CK_UNIT_CERTIFIED = (
    "unit_certified = false OR (contract_size IS NOT NULL AND "
    "CAST(contract_size AS NUMERIC) > 0 AND "
    "quantity_unit IS NOT NULL AND quantity_unit = 'contract_count' AND "
    "spec_source IS NOT NULL AND "
    "spec_version IS NOT NULL AND spec_observed_at IS NOT NULL)"
)
_CONSTRAINTS = (
    ("ck_instrument_specs_contract_size_positive", _CK_CONTRACT_SIZE),
    ("ck_instrument_specs_quantity_unit", _CK_QUANTITY_UNIT),
    ("ck_instrument_specs_provenance", _CK_PROVENANCE),
    ("ck_instrument_specs_unit_certified", _CK_UNIT_CERTIFIED),
)


def _is_sqlite() -> bool:
    """Return whether the migration is executing against SQLite."""
    return op.get_bind().dialect.name == "sqlite"


def _columns() -> tuple[
    sa.Column[Decimal],
    sa.Column[str],
    sa.Column[str],
    sa.Column[str],
    sa.Column[datetime],
    sa.Column[bool],
]:
    """Build fresh metadata column objects for one migration path."""
    return (
        sa.Column("contract_size", sa.Numeric(38, 18), nullable=True),
        sa.Column("quantity_unit", sa.String(32), nullable=True),
        sa.Column("spec_source", sa.String(64), nullable=True),
        sa.Column("spec_version", sa.String(96), nullable=True),
        sa.Column("spec_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "unit_certified",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )


def upgrade() -> None:
    """Add instrument metadata columns and fail-closed checks."""
    if _is_sqlite():
        with op.batch_alter_table("instrument_specs", recreate="always") as batch:
            for column in _columns():
                batch.add_column(column)
            for name, condition in _CONSTRAINTS:
                batch.create_check_constraint(name, condition)
        return
    for column in _columns():
        op.add_column("instrument_specs", column)
    for name, condition in _CONSTRAINTS:
        op.create_check_constraint(name, "instrument_specs", condition)


def downgrade() -> None:
    """Remove instrument metadata checks and columns."""
    column_names = (
        "unit_certified",
        "spec_observed_at",
        "spec_version",
        "spec_source",
        "quantity_unit",
        "contract_size",
    )
    if _is_sqlite():
        with op.batch_alter_table("instrument_specs", recreate="always") as batch:
            for name, _condition in reversed(_CONSTRAINTS):
                batch.drop_constraint(name, type_="check")
            for column_name in column_names:
                batch.drop_column(column_name)
        return
    for name, _condition in reversed(_CONSTRAINTS):
        op.drop_constraint(name, "instrument_specs", type_="check")
    for column_name in column_names:
        op.drop_column("instrument_specs", column_name)
