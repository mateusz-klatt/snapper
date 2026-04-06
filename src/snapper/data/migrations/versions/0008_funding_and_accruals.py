"""Add funding rate model and accrual ledger.

Stream B (funding fee model) session 1: schema + plumbing.

- Adds five funding metadata columns to ``instrument_specs`` plus a
  CHECK constraint on ``funding_type``.
- Adds ``position_opened_at`` to ``trade_projection_checkpoints`` so the
  funding accrual loop can clamp catch-up boundaries to the current
  open cycle across trader restarts.
- Creates the ``funding_rates`` table (bitemporal) with a partial
  unique index over
  ``(instrument_public_id, exchange, rate_type, direction, effective_from)``.
- Creates the ``accrual_ledger`` table (bitemporal) with a partial
  unique index over
  ``(instrument_public_id, mode, accrual_type, accrued_at)``.

The plan and rationale live in
``proprietary/plans/plan_funding_fee_model.md``.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

revision: str = "0008"
down_revision: str = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"
_CK_EXCHANGE_LOWER = "exchange = LOWER(exchange)"
_CK_SESSION_ID = "session_id != ''"
_CK_SEQUENCE_ID = "sequence_id > 0"


def upgrade() -> None:
    """Add funding metadata, position_opened_at, and the new tables."""
    with op.batch_alter_table("instrument_specs", recreate="auto") as batch_op:
        batch_op.add_column(sa.Column("funding_type", sa.String(32), nullable=True))
        batch_op.add_column(
            sa.Column("funding_frequency_hours", sa.Integer(), nullable=True),
        )
        batch_op.add_column(sa.Column("rollover_rate_long", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("rollover_rate_short", sa.Float(), nullable=True))
        batch_op.add_column(sa.Column("max_funding_rate", sa.Float(), nullable=True))
        batch_op.create_check_constraint(
            "ck_instrument_specs_funding_type",
            "funding_type IS NULL OR funding_type IN "
            "('spot_margin_rollover', 'perpetual_funding')",
        )

    with op.batch_alter_table("trade_projection_checkpoints", recreate="auto") as batch_op:
        batch_op.add_column(
            sa.Column("position_opened_at", sa.DateTime(timezone=True), nullable=True),
        )

    op.create_table(
        "funding_rates",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("rate_type", sa.String(32), nullable=False),
        sa.Column("direction", sa.String(8), nullable=False),
        sa.Column("rate", sa.Float(), nullable=False),
        sa.Column("notional_asset", sa.String(16), nullable=False),
        sa.Column("effective_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_funding_rates_exchange_lower"),
        sa.CheckConstraint(
            "rate_type IN ('spot_margin_rollover', 'perpetual_funding')",
            name="ck_funding_rates_rate_type",
        ),
        sa.CheckConstraint(
            "direction IN ('long', 'short', 'both')",
            name="ck_funding_rates_direction",
        ),
        sa.CheckConstraint(
            "source IN ('exchange_api', 'exchange_docs', 'manual', 'derived')",
            name="ck_funding_rates_source",
        ),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_funding_rates_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_funding_rates_sequence_id"),
    )
    op.create_index(
        "ix_funding_rates_unique_active",
        "funding_rates",
        [
            "instrument_public_id",
            "exchange",
            "rate_type",
            "direction",
            "effective_from",
        ],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_funding_rates_public_id",
        "funding_rates",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_funding_rates_instrument_public_id",
        "funding_rates",
        ["instrument_public_id"],
    )

    op.create_table(
        "accrual_ledger",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False),
        sa.Column("accrual_type", sa.String(16), nullable=False),
        sa.Column("accrued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("amount", sa.Float(), nullable=False),
        sa.Column("amount_asset", sa.String(16), nullable=False),
        sa.Column("rate", sa.Float(), nullable=False),
        sa.Column("notional", sa.Float(), nullable=False),
        sa.Column("position_quantity_at_accrual", sa.Float(), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_accrual_ledger_exchange_lower"),
        sa.CheckConstraint(
            "mode IN ('live', 'paper', 'backtest')",
            name="ck_accrual_ledger_mode",
        ),
        sa.CheckConstraint(
            "accrual_type IN ('funding', 'rollover', 'borrow')",
            name="ck_accrual_ledger_type",
        ),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_accrual_ledger_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_accrual_ledger_sequence_id"),
    )
    op.create_index(
        "ix_accrual_ledger_unique_active",
        "accrual_ledger",
        ["instrument_public_id", "mode", "accrual_type", "accrued_at"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_accrual_ledger_public_id",
        "accrual_ledger",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_accrual_ledger_recovery",
        "accrual_ledger",
        ["instrument_public_id", "mode", "accrued_at"],
    )
    op.create_index(
        "ix_accrual_ledger_accrued_at",
        "accrual_ledger",
        ["accrued_at"],
    )
    op.create_index(
        "ix_accrual_ledger_instrument_public_id",
        "accrual_ledger",
        ["instrument_public_id"],
    )


def downgrade() -> None:
    """Drop the new tables and revert column additions."""
    op.drop_index("ix_accrual_ledger_instrument_public_id", table_name="accrual_ledger")
    op.drop_index("ix_accrual_ledger_accrued_at", table_name="accrual_ledger")
    op.drop_index("ix_accrual_ledger_recovery", table_name="accrual_ledger")
    op.drop_index("ix_accrual_ledger_public_id", table_name="accrual_ledger")
    op.drop_index("ix_accrual_ledger_unique_active", table_name="accrual_ledger")
    op.drop_table("accrual_ledger")

    op.drop_index("ix_funding_rates_instrument_public_id", table_name="funding_rates")
    op.drop_index("ix_funding_rates_public_id", table_name="funding_rates")
    op.drop_index("ix_funding_rates_unique_active", table_name="funding_rates")
    op.drop_table("funding_rates")

    with op.batch_alter_table("trade_projection_checkpoints", recreate="auto") as batch_op:
        batch_op.drop_column("position_opened_at")

    with op.batch_alter_table("instrument_specs", recreate="auto") as batch_op:
        batch_op.drop_constraint("ck_instrument_specs_funding_type", type_="check")
        batch_op.drop_column("max_funding_rate")
        batch_op.drop_column("rollover_rate_short")
        batch_op.drop_column("rollover_rate_long")
        batch_op.drop_column("funding_frequency_hours")
        batch_op.drop_column("funding_type")
