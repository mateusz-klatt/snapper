"""Retain atomic financial publication intents and immutable local delivery receipts."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.schema import SchemaItem

from snapper.data.financial_publication_schema import financial_delivery_constraints
from snapper.data.financial_publication_schema import financial_intent_constraints
from snapper.data.financial_publication_triggers import FINANCIAL_PUBLICATION_TABLES
from snapper.data.financial_publication_triggers import drop_financial_publication_triggers
from snapper.data.financial_publication_triggers import install_financial_publication_triggers

revision: str = "0059"
down_revision: str | None = "0058"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _require_online() -> None:
    """Require an actual supported database transaction for physical evidence retention."""
    if op.get_context().as_sql:
        raise RuntimeError("migration 0059 requires an online connection")
    if op.get_bind().dialect.name not in {"sqlite", "postgresql"}:
        raise NotImplementedError("migration 0059 supports only SQLite and PostgreSQL")


def _uuid_type() -> sa.String:
    """Keep native PostgreSQL UUID storage and compatible isolated SQLite fixtures."""
    return sa.String(36).with_variant(UUID(as_uuid=False), "postgresql")


def _scope_columns() -> tuple[SchemaItem, ...]:
    """Create fresh immutable provenance and actual account scope for each outbox table."""
    return (
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), primary_key=True),
        sa.Column("public_id", _uuid_type(), nullable=False),
        sa.Column("session_id", _uuid_type(), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("wallet_public_id", _uuid_type(), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False),
        sa.Column("environment", sa.String(16), nullable=False),
        sa.Column("margin_account", sa.String(128), nullable=False),
    )


def _create_intents() -> None:
    """Create empty source-deduplicated notification intents without publishing old executions."""
    op.create_table(
        "financial_publication_intents",
        *_scope_columns(),
        sa.Column("account_scope_digest", sa.String(64), nullable=False),
        sa.Column("source_execution_id", sa.String(256), nullable=False),
        sa.Column("execution_public_id", _uuid_type(), nullable=False),
        sa.Column("position_event_public_id", _uuid_type(), nullable=False),
        sa.Column("topic", sa.String(256), nullable=False),
        sa.Column("content_digest", sa.String(64), nullable=False),
        sa.Column("canonical_payload", sa.Text(), nullable=False),
        *financial_intent_constraints(),
    )
    op.create_index(
        "ix_financial_intent_scope",
        "financial_publication_intents",
        ["account_scope_digest", "id"],
    )


def _create_deliveries() -> None:
    """Retain local publisher-return evidence without fabricating consumer acknowledgements."""
    op.create_table(
        "financial_publication_deliveries",
        *_scope_columns(),
        sa.Column("publication_public_id", _uuid_type(), nullable=False),
        sa.Column("publication_digest", sa.String(64), nullable=False),
        sa.Column("delivery_semantics", sa.String(32), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        *financial_delivery_constraints(),
    )


def upgrade() -> None:
    """Install empty immutable stores; no order or historical publication is synthesized."""
    _require_online()
    _create_intents()
    _create_deliveries()
    for table_name in FINANCIAL_PUBLICATION_TABLES:
        install_financial_publication_triggers(op.get_bind(), table_name)


def _fence_downgrade() -> None:
    """Retain a write fence until the caller commits or rolls back the downgrade."""
    connection = op.get_bind()
    dialect = connection.dialect.name
    if dialect == "postgresql":
        if connection.get_isolation_level() != "READ COMMITTED":
            raise RuntimeError("Financial publication downgrade requires READ COMMITTED")
        tables = ", ".join(FINANCIAL_PUBLICATION_TABLES)
        connection.execute(sa.text(f"LOCK TABLE {tables} IN ACCESS EXCLUSIVE MODE"))
        return
    if dialect != "sqlite":
        raise NotImplementedError("Financial publication downgrade supports SQLite/PostgreSQL")
    driver = connection.connection.driver_connection
    if getattr(driver, "autocommit", None) is True:
        raise RuntimeError("Financial publication downgrade requires caller-controlled completion")
    active = getattr(driver, "in_transaction", None)
    if not isinstance(active, bool):
        raise RuntimeError("Financial publication downgrade requires the SQLite driver state")
    if active:
        connection.exec_driver_sql("UPDATE financial_publication_intents SET id = id WHERE 0")
    else:
        connection.exec_driver_sql("BEGIN IMMEDIATE")
    if getattr(driver, "in_transaction", None) is not True:
        raise RuntimeError("Financial publication downgrade did not retain its transaction")


def downgrade() -> None:
    """Refuse deletion of any publication or delivery evidence, including already sent intents."""
    _require_online()
    _fence_downgrade()
    connection = op.get_bind()
    for table_name in FINANCIAL_PUBLICATION_TABLES:
        if connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table_name}")):
            raise RuntimeError("Financial publication evidence prevents a lossless downgrade")
    for table_name in reversed(FINANCIAL_PUBLICATION_TABLES):
        drop_financial_publication_triggers(connection, table_name)
        op.drop_table(table_name)
    if connection.dialect.name == "postgresql":
        connection.execute(
            sa.text("DROP FUNCTION IF EXISTS financial_publication_reject_mutation()")
        )
