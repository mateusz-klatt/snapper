"""Persist explicit derivative payoff and native order units without guessing history."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0055"
down_revision: str | None = "0054"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CONSTRAINTS = (
    (
        "ck_instrument_specs_native_order_quantity_unit",
        (
            "native_order_quantity_unit IS NULL OR "
            "native_order_quantity_unit IN ('base_asset', 'contract_count')"
        ),
    ),
    (
        "ck_instrument_specs_payoff_model",
        "contract_payoff_model IS NULL OR contract_payoff_model IN ('linear', 'inverse')",
    ),
    (
        "ck_instrument_specs_payoff_evidence",
        (
            "(contract_payoff_model IS NULL AND contract_value_currency IS NULL "
            "AND settlement_currency IS NULL) OR "
            "(contract_payoff_model IS NOT NULL AND contract_value_currency IS NOT NULL "
            "AND settlement_currency IS NOT NULL "
            "AND length(trim(contract_value_currency)) > 0 "
            "AND length(trim(settlement_currency)) > 0)"
        ),
    ),
)


def _columns() -> tuple[sa.Column[str], ...]:
    """Build fresh nullable payoff and native-unit columns for either database dialect."""
    return (
        sa.Column("contract_payoff_model", sa.String(16), nullable=True),
        sa.Column("contract_value_currency", sa.String(32), nullable=True),
        sa.Column("settlement_currency", sa.String(32), nullable=True),
        sa.Column("native_order_quantity_unit", sa.String(32), nullable=True),
    )


def upgrade() -> None:
    """Add explicit derivative interpretations while leaving historical rows unknown."""
    if op.get_bind().dialect.name == "sqlite":
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


def _serialize_evidence_downgrade() -> None:
    """Reserve writes before reading evidence without committing the caller transaction."""
    connection = op.get_bind()
    dialect = connection.dialect.name
    if dialect == "postgresql":
        if connection.get_isolation_level() != "READ COMMITTED":
            raise RuntimeError("Evidence downgrade requires READ COMMITTED isolation")
        connection.execute(sa.text("LOCK TABLE instrument_specs IN ACCESS EXCLUSIVE MODE"))
        return
    if dialect != "sqlite":
        raise NotImplementedError("Evidence downgrade supports only SQLite and PostgreSQL")
    driver = connection.connection.driver_connection
    if getattr(driver, "autocommit", None) is True:
        raise RuntimeError("Evidence downgrade requires caller-controlled transaction completion")
    active = getattr(driver, "in_transaction", None)
    if not isinstance(active, bool):
        raise RuntimeError("Evidence downgrade requires the SQLite driver transaction state")
    if active:
        connection.exec_driver_sql("UPDATE instrument_specs SET id = id WHERE 0")
    else:
        connection.exec_driver_sql("BEGIN IMMEDIATE")
    if getattr(driver, "in_transaction", None) is not True:
        raise RuntimeError("Evidence downgrade did not retain its SQLite write transaction")


def downgrade() -> None:
    """Refuse deleting recorded derivative interpretation from instrument history."""
    _serialize_evidence_downgrade()
    evidence_count = op.get_bind().scalar(
        sa.text(
            "SELECT COUNT(*) FROM instrument_specs WHERE contract_payoff_model IS NOT NULL "
            "OR contract_value_currency IS NOT NULL OR settlement_currency IS NOT NULL "
            "OR native_order_quantity_unit IS NOT NULL"
        )
    )
    if evidence_count:
        raise RuntimeError("Derivative interpretation evidence prevents a lossless downgrade")
    column_names = tuple(column.name for column in reversed(_columns()))
    if op.get_bind().dialect.name == "sqlite":
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
