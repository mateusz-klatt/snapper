"""Add postgresql partial-index counterparts for four sqlite-only partial indexes.

Four indexes were declared with ``sqlite_where=...`` but no matching
``postgresql_where=...``. On PostgreSQL this silently created **full**
indexes (covering every row) instead of the intended partial indexes,
wasting disk and RAM. This migration drops the PG full indexes and
recreates them as proper partial indexes mirroring the SQLite predicate
in PostgreSQL syntax (``can_trade = true`` not ``can_trade = 1``).

SQLite is unaffected — its partial indexes were already correct and
remain in place.

Indexes touched:

- ``ix_sec_exchange_trade``      (symbol_exchange_capabilities, partial: can_trade)
- ``ix_sec_exchange_md``         (symbol_exchange_capabilities, partial: can_market_data)
- ``uq_executions_order_exec``   (executions, partial: exec_id IS NOT NULL)
- ``uq_executions_order_trade``  (executions, partial: trade_id IS NOT NULL)
"""

from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _upgrade_postgresql() -> None:
    """Replace four full PG indexes with partial indexes that mirror the SQLite predicate."""
    op.drop_index("ix_sec_exchange_trade", table_name="symbol_exchange_capabilities")
    op.create_index(
        "ix_sec_exchange_trade",
        "symbol_exchange_capabilities",
        ["exchange", "can_trade"],
        postgresql_where=text("can_trade = true"),
    )
    op.drop_index("ix_sec_exchange_md", table_name="symbol_exchange_capabilities")
    op.create_index(
        "ix_sec_exchange_md",
        "symbol_exchange_capabilities",
        ["exchange", "can_market_data"],
        postgresql_where=text("can_market_data = true"),
    )
    op.drop_index("uq_executions_order_exec", table_name="executions")
    op.create_index(
        "uq_executions_order_exec",
        "executions",
        ["order_public_id", "exec_id"],
        unique=True,
        postgresql_where=text("exec_id IS NOT NULL"),
    )
    op.drop_index("uq_executions_order_trade", table_name="executions")
    op.create_index(
        "uq_executions_order_trade",
        "executions",
        ["order_public_id", "trade_id"],
        unique=True,
        postgresql_where=text("trade_id IS NOT NULL"),
    )


def _downgrade_postgresql() -> None:
    """Restore the original full PG indexes (no WHERE clause)."""
    op.drop_index("ix_sec_exchange_trade", table_name="symbol_exchange_capabilities")
    op.create_index(
        "ix_sec_exchange_trade",
        "symbol_exchange_capabilities",
        ["exchange", "can_trade"],
    )
    op.drop_index("ix_sec_exchange_md", table_name="symbol_exchange_capabilities")
    op.create_index(
        "ix_sec_exchange_md",
        "symbol_exchange_capabilities",
        ["exchange", "can_market_data"],
    )
    op.drop_index("uq_executions_order_exec", table_name="executions")
    op.create_index(
        "uq_executions_order_exec",
        "executions",
        ["order_public_id", "exec_id"],
        unique=True,
    )
    op.drop_index("uq_executions_order_trade", table_name="executions")
    op.create_index(
        "uq_executions_order_trade",
        "executions",
        ["order_public_id", "trade_id"],
        unique=True,
    )


def upgrade() -> None:
    """Apply the migration for the active database dialect."""
    dialect_name = op.get_bind().dialect.name
    if dialect_name == "postgresql":
        _upgrade_postgresql()


def downgrade() -> None:
    """Revert the migration for the active database dialect."""
    dialect_name = op.get_bind().dialect.name
    if dialect_name == "postgresql":
        _downgrade_postgresql()
