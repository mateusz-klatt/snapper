"""Schema hygiene: enum CHECKs, executions index, drop a stray default.

Adds the enum-vocabulary CHECK constraints that orders / trade_commands /
executions never had (value sets derived from every persistence writer,
not from the enums alone — orders.status carries WIRE spellings, the
order_type set is dual-era and varchar(16)-bounded, command_type allows
the intentional create/submit/cancel/replace vocabulary), the missing
``ix_executions_wallet_ts`` composite index, the ``ck_bc_pairing_mode``
CHECK (auto/manual; ``ck_bc_runs_distinct`` already exists), and drops the
stray ``server_default`` on ``continuous_contract_configs.known_to`` so it
matches every other SCD2 table (Python-side KNOWN_TO_MAX default).

CHECK adds and the default drop require table recreation on SQLite (no
ALTER-ADD-CHECK / ALTER-DROP-DEFAULT support there), so those run inside
``op.batch_alter_table(recreate="always")`` on SQLite; on PostgreSQL they
run as direct DDL (the affected trading tables are empty in production, so
the CHECK validation scan is metadata-cheap). The index add works directly
on both dialects. Revises 0009.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CK_MODE_LIVE_PAPER = "mode IN ('live', 'paper')"
_CK_SIDE_BUY_SELL = "side IN ('buy', 'sell')"
_CK_ORDER_TYPE_VALUES = (
    "order_type IN ('market', 'limit', 'stop', 'stop_limit', 'stop-loss', "
    "'stop-loss-limit', 'take-profit', 'trailing-stop', 'iceberg', 'settle-position')"
)
_CK_ORDERS_STATUS_WIRE = (
    "status IN ('pending', 'open', 'closed', 'canceled', 'expired', "
    "'pending_new', 'new', 'partially_filled', 'filled')"
)
_CK_EXECUTIONS_STATUS = "status IN ('filled', 'partial')"
_CK_TRADE_COMMAND_TYPE = "command_type IN ('create', 'submit', 'cancel', 'replace')"
_CK_TRADE_COMMAND_STATUS = (
    "status IN ('created', 'dispatched', 'direct_dispatched', 'accepted', "
    "'filled', 'partially_filled', 'rejected', 'cancelled', 'expired', 'failed')"
)
_CK_PAIRING_MODE = "pairing_mode IN ('auto', 'manual')"

_CHECKS: tuple[tuple[str, str, str], ...] = (
    ("orders", "ck_orders_mode", _CK_MODE_LIVE_PAPER),
    ("orders", "ck_orders_side", _CK_SIDE_BUY_SELL),
    ("orders", "ck_orders_order_type", _CK_ORDER_TYPE_VALUES),
    ("orders", "ck_orders_status", _CK_ORDERS_STATUS_WIRE),
    ("executions", "ck_executions_side", _CK_SIDE_BUY_SELL),
    ("executions", "ck_executions_status", _CK_EXECUTIONS_STATUS),
    ("trade_commands", "ck_trade_commands_command_type", _CK_TRADE_COMMAND_TYPE),
    ("trade_commands", "ck_trade_commands_mode", _CK_MODE_LIVE_PAPER),
    ("trade_commands", "ck_trade_commands_side", _CK_SIDE_BUY_SELL),
    ("trade_commands", "ck_trade_commands_order_type", _CK_ORDER_TYPE_VALUES),
    ("trade_commands", "ck_trade_commands_status", _CK_TRADE_COMMAND_STATUS),
    ("backtest_comparisons", "ck_bc_pairing_mode", _CK_PAIRING_MODE),
)


def _is_sqlite() -> bool:
    """Return True when the bound connection is SQLite (test fixture)."""
    return op.get_bind().dialect.name == "sqlite"


def upgrade() -> None:
    """Add hygiene CHECKs + executions index; drop the known_to default."""
    op.create_index(
        "ix_executions_wallet_ts",
        "executions",
        ["wallet_public_id", "timestamp"],
        sqlite_where=sa.text("known_to = '9999-12-31 23:59:59.000000'"),
        postgresql_where=sa.text("known_to = '9999-12-31T23:59:59+00:00'"),
    )
    if _is_sqlite():
        by_table: dict[str, list[tuple[str, str]]] = {}
        for table, name, expr in _CHECKS:
            by_table.setdefault(table, []).append((name, expr))
        for table, checks in by_table.items():
            with op.batch_alter_table(table, recreate="always") as batch:
                for name, expr in checks:
                    batch.create_check_constraint(name, expr)
        with op.batch_alter_table("continuous_contract_configs", recreate="always") as batch:
            batch.alter_column("known_to", server_default=None)
        return
    for table, name, expr in _CHECKS:
        op.create_check_constraint(name, table, expr)
    op.alter_column("continuous_contract_configs", "known_to", server_default=None)


def downgrade() -> None:
    """Drop hygiene CHECKs + executions index; restore the known_to default."""
    if _is_sqlite():
        by_table: dict[str, list[str]] = {}
        for table, name, _expr in _CHECKS:
            by_table.setdefault(table, []).append(name)
        for table, names in by_table.items():
            with op.batch_alter_table(table, recreate="always") as batch:
                for name in names:
                    batch.drop_constraint(name, type_="check")
        with op.batch_alter_table("continuous_contract_configs", recreate="always") as batch:
            batch.alter_column("known_to", server_default="9999-12-31 23:59:59.000000")
    else:
        for table, name, _expr in _CHECKS:
            op.drop_constraint(name, table, type_="check")
        op.alter_column(
            "continuous_contract_configs",
            "known_to",
            server_default="9999-12-31 23:59:59.000000",
        )
    op.drop_index("ix_executions_wallet_ts", table_name="executions")
