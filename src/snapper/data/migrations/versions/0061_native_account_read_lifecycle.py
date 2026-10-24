"""Retain native account READ attempts before I/O and immutable historical outcomes."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.schema import SchemaItem

from snapper.data.native_account_read_schema import native_account_read_attempt_constraints
from snapper.data.native_account_read_schema import native_account_read_outcome_constraints
from snapper.data.native_account_read_triggers import NATIVE_ACCOUNT_READ_TABLES
from snapper.data.native_account_read_triggers import drop_native_account_read_triggers
from snapper.data.native_account_read_triggers import install_native_account_read_triggers

revision: str = "0061"
down_revision: str | None = "0060"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _require_online() -> None:
    """Require the caller's actual supported transaction for native read-history DDL."""
    if op.get_context().as_sql:
        raise RuntimeError("migration 0061 requires an online connection")
    if op.get_bind().dialect.name not in {"sqlite", "postgresql"}:
        raise NotImplementedError("migration 0061 supports only SQLite and PostgreSQL")


def _uuid_type() -> sa.String:
    """Keep native PostgreSQL UUIDs and the established SQLite representation."""
    return sa.String(36).with_variant(UUID(as_uuid=False), "postgresql")


def _family_columns() -> tuple[SchemaItem, ...]:
    """Keep read lifecycle ownership independent of any margin-account classification."""
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
        sa.Column("margin_account", sa.String(128), nullable=True),
        sa.Column("scope_digest", sa.String(64), nullable=False),
        sa.Column("family_sequence", sa.BigInteger(), nullable=False),
        sa.Column("content_digest", sa.String(64), nullable=False),
        sa.Column("canonical_payload", sa.Text(), nullable=False),
    )


def upgrade() -> None:
    """Create empty histories without inventing prior requests, responses or authority."""
    _require_online()
    op.create_table(
        "native_account_read_attempts",
        *_family_columns(),
        sa.Column("client_selection_digest", sa.String(64), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        *native_account_read_attempt_constraints(),
    )
    op.create_table(
        "native_account_read_outcomes",
        *_family_columns(),
        sa.Column("attempt_public_id", _uuid_type(), nullable=False),
        sa.Column("attempt_content_digest", sa.String(64), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("result_kind", sa.String(16), nullable=False),
        sa.Column("raw_receipt_public_id", _uuid_type(), nullable=True),
        *native_account_read_outcome_constraints(),
    )
    for table in NATIVE_ACCOUNT_READ_TABLES:
        suffix = "attempt" if table.endswith("attempts") else "outcome"
        op.create_index(
            f"ix_native_read_{suffix}_family", table, ["scope_digest", "family_sequence"]
        )
        install_native_account_read_triggers(op.get_bind(), table)


def _fence_downgrade() -> None:
    """Retain both lifecycle tables' writer fences until the caller completes its transaction."""
    connection = op.get_bind()
    dialect = connection.dialect.name
    if dialect == "postgresql":
        if connection.get_isolation_level() != "READ COMMITTED":
            raise RuntimeError("Native account read downgrade requires READ COMMITTED")
        tables = ", ".join(NATIVE_ACCOUNT_READ_TABLES)
        connection.execute(sa.text(f"LOCK TABLE {tables} IN ACCESS EXCLUSIVE MODE"))
        return
    if dialect != "sqlite":
        raise NotImplementedError("Native account read downgrade supports SQLite/PostgreSQL")
    driver = connection.connection.driver_connection
    if getattr(driver, "autocommit", None) is True:
        raise RuntimeError("Native account read downgrade requires caller-controlled completion")
    active = getattr(driver, "in_transaction", None)
    if not isinstance(active, bool):
        raise RuntimeError("Native account read downgrade requires the SQLite driver state")
    if active:
        connection.exec_driver_sql("UPDATE native_account_read_attempts SET id = id WHERE 0")
    else:
        connection.exec_driver_sql("BEGIN IMMEDIATE")
    if getattr(driver, "in_transaction", None) is not True:
        raise RuntimeError("Native account read downgrade did not retain its transaction")


def downgrade() -> None:
    """Refuse destruction of any pending request, successful response or failed outcome."""
    _require_online()
    _fence_downgrade()
    connection = op.get_bind()
    for table in NATIVE_ACCOUNT_READ_TABLES:
        if connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table}")):
            raise RuntimeError("Native account read evidence prevents a lossless downgrade")
    for table in reversed(NATIVE_ACCOUNT_READ_TABLES):
        drop_native_account_read_triggers(connection, table)
        op.drop_table(table)
    if connection.dialect.name == "postgresql":
        connection.execute(sa.text("DROP FUNCTION IF EXISTS native_account_read_reject_mutation()"))
