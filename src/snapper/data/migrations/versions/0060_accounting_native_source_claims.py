"""Retain one immutable owner for each genuine native account-log identity."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

from snapper.data.accounting_native_source_schema import accounting_native_source_constraints
from snapper.data.accounting_native_source_triggers import ACCOUNTING_NATIVE_SOURCE_TABLE
from snapper.data.accounting_native_source_triggers import drop_accounting_native_source_triggers
from snapper.data.accounting_native_source_triggers import install_accounting_native_source_triggers

revision: str = "0060"
down_revision: str | None = "0059"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _require_online() -> None:
    """Require the caller's actual supported transaction for native identity ownership."""
    if op.get_context().as_sql:
        raise RuntimeError("migration 0060 requires an online connection")
    if op.get_bind().dialect.name not in {"sqlite", "postgresql"}:
        raise NotImplementedError("migration 0060 supports only SQLite and PostgreSQL")


def _uuid_type() -> sa.String:
    """Keep native PostgreSQL UUID storage and the established SQLite representation."""
    return sa.String(36).with_variant(UUID(as_uuid=False), "postgresql")


def upgrade() -> None:
    """Create empty claims without asserting ownership of historical accounting events."""
    _require_online()
    op.create_table(
        ACCOUNTING_NATIVE_SOURCE_TABLE,
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
        sa.Column("scope_digest", sa.String(64), nullable=False),
        sa.Column("account_scope_digest", sa.String(64), nullable=False),
        sa.Column("source_domain", sa.Text(), nullable=False),
        sa.Column("source_log_id", sa.Text(), nullable=False),
        sa.Column("source_identity_digest", sa.String(64), nullable=False),
        sa.Column("semantic_digest", sa.String(64), nullable=False),
        sa.Column("raw_receipt_public_id", _uuid_type(), nullable=False),
        sa.Column("accounting_event_public_id", _uuid_type(), nullable=False),
        sa.Column("route_authority_public_id", _uuid_type(), nullable=False),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("booking_uid", sa.Text(), nullable=False),
        sa.Column("content_digest", sa.String(64), nullable=False),
        sa.Column("canonical_payload", sa.Text(), nullable=False),
        *accounting_native_source_constraints(),
    )
    op.create_index(
        "ix_acct_native_claim_family", ACCOUNTING_NATIVE_SOURCE_TABLE, ["scope_digest", "id"]
    )
    install_accounting_native_source_triggers(op.get_bind(), ACCOUNTING_NATIVE_SOURCE_TABLE)


def _fence_downgrade() -> None:
    """Retain the caller's write fence before evaluating whether claim history is empty."""
    connection = op.get_bind()
    dialect = connection.dialect.name
    if dialect == "postgresql":
        if connection.get_isolation_level() != "READ COMMITTED":
            raise RuntimeError("Native accounting source downgrade requires READ COMMITTED")
        connection.execute(
            sa.text(f"LOCK TABLE {ACCOUNTING_NATIVE_SOURCE_TABLE} IN ACCESS EXCLUSIVE MODE")
        )
        return
    if dialect != "sqlite":
        raise NotImplementedError("Native accounting source downgrade supports SQLite/PostgreSQL")
    driver = connection.connection.driver_connection
    if getattr(driver, "autocommit", None) is True:
        raise RuntimeError(
            "Native accounting source downgrade requires caller-controlled completion"
        )
    active = getattr(driver, "in_transaction", None)
    if not isinstance(active, bool):
        raise RuntimeError("Native accounting source downgrade requires the SQLite driver state")
    if active:
        connection.exec_driver_sql(f"UPDATE {ACCOUNTING_NATIVE_SOURCE_TABLE} SET id = id WHERE 0")
    else:
        connection.exec_driver_sql("BEGIN IMMEDIATE")
    if getattr(driver, "in_transaction", None) is not True:
        raise RuntimeError("Native accounting source downgrade did not retain its transaction")


def downgrade() -> None:
    """Refuse destruction of any ownership claim, even when its financial event was published."""
    _require_online()
    _fence_downgrade()
    connection = op.get_bind()
    if connection.scalar(sa.text(f"SELECT COUNT(*) FROM {ACCOUNTING_NATIVE_SOURCE_TABLE}")):
        raise RuntimeError("Native accounting source evidence prevents a lossless downgrade")
    drop_accounting_native_source_triggers(connection, ACCOUNTING_NATIVE_SOURCE_TABLE)
    op.drop_table(ACCOUNTING_NATIVE_SOURCE_TABLE)
    if connection.dialect.name == "postgresql":
        connection.execute(
            sa.text("DROP FUNCTION IF EXISTS accounting_native_source_reject_mutation()")
        )
