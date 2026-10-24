"""Retain immutable source-bound native account authority without renewing an old read."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.schema import SchemaItem

from snapper.data.native_account_authority_schema import native_account_authority_constraints
from snapper.data.native_account_authority_triggers import drop_native_account_authority_triggers
from snapper.data.native_account_authority_triggers import install_native_account_authority_triggers

revision: str = "0063"
down_revision: str | None = "0062"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
_TABLE = "native_account_authorities"


def _require_online() -> None:
    """Require an actual supported caller transaction before native-authority DDL."""
    if op.get_context().as_sql:
        raise RuntimeError("migration 0063 requires an online connection")
    if op.get_bind().dialect.name not in {"sqlite", "postgresql"}:
        raise NotImplementedError("migration 0063 supports only SQLite and PostgreSQL")


def _uuid_type() -> sa.String:
    """Preserve native PostgreSQL UUIDs and the established SQLite representation."""
    return sa.String(36).with_variant(UUID(as_uuid=False), "postgresql")


def _authority_columns() -> tuple[SchemaItem, ...]:
    """Retain exact selected native scope, actual source graph and original issuance interval."""
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
        sa.Column("account_scope_digest", sa.String(64), nullable=False),
        sa.Column("credential_public_id", _uuid_type(), nullable=False),
        sa.Column("credential_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("public_api_origin", sa.String(256), nullable=False),
        sa.Column("venue_account_id", _uuid_type(), nullable=False),
        sa.Column("owner_proof_public_id", _uuid_type(), nullable=False),
        sa.Column("read_attempt_public_id", _uuid_type(), nullable=False),
        sa.Column("read_outcome_public_id", _uuid_type(), nullable=False),
        sa.Column("raw_receipt_public_id", _uuid_type(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("valid_until", sa.DateTime(timezone=True), nullable=False),
        sa.Column("route_revision", sa.String(64), nullable=False),
        sa.Column("content_digest", sa.String(64), nullable=False),
        sa.Column("canonical_payload", sa.Text(), nullable=False),
    )


def upgrade() -> None:
    """Create empty source-bound authority storage without certifying old balances or profiles."""
    _require_online()
    op.create_table(_TABLE, *_authority_columns(), *native_account_authority_constraints())
    op.create_index("ix_native_authority_scope", _TABLE, ["account_scope_digest", "timestamp"])
    install_native_account_authority_triggers(op.get_bind(), _TABLE)


def _fence_downgrade() -> None:
    """Retain the authority table's actual writer fence until caller commit or rollback."""
    connection = op.get_bind()
    dialect = connection.dialect.name
    if dialect == "postgresql":
        if connection.get_isolation_level() != "READ COMMITTED":
            raise RuntimeError("Native account authority downgrade requires READ COMMITTED")
        connection.execute(
            sa.text("LOCK TABLE native_account_authorities IN ACCESS EXCLUSIVE MODE")
        )
        return
    if dialect != "sqlite":
        raise NotImplementedError("Native account authority downgrade supports SQLite/PostgreSQL")
    driver = connection.connection.driver_connection
    if getattr(driver, "autocommit", None) is True:
        raise RuntimeError(
            "Native account authority downgrade requires caller-controlled completion"
        )
    active = getattr(driver, "in_transaction", None)
    if not isinstance(active, bool):
        raise RuntimeError("Native account authority downgrade requires the SQLite driver state")
    if active:
        connection.exec_driver_sql("UPDATE native_account_authorities SET id = id WHERE 0")
    else:
        connection.exec_driver_sql("BEGIN IMMEDIATE")
    if getattr(driver, "in_transaction", None) is not True:
        raise RuntimeError("Native account authority downgrade did not retain its transaction")


def downgrade() -> None:
    """Refuse destruction of any retained native source authority or its original validity."""
    _require_online()
    _fence_downgrade()
    connection = op.get_bind()
    if connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_authorities")):
        raise RuntimeError("Native account authority evidence prevents a lossless downgrade")
    drop_native_account_authority_triggers(connection, _TABLE)
    op.drop_table(_TABLE)
    if connection.dialect.name == "postgresql":
        connection.execute(
            sa.text("DROP FUNCTION IF EXISTS native_account_authority_reject_mutation()")
        )
