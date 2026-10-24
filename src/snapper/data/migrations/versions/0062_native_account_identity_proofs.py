"""Retain redacted selected-key identity without replacing an original account owner."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.schema import SchemaItem

from snapper.data.native_account_identity_schema import NATIVE_ACCOUNT_IDENTITY_INITIAL_ROOT_KEYS
from snapper.data.native_account_identity_schema import (
    NATIVE_ACCOUNT_IDENTITY_INITIAL_ROOT_PREDICATE,
)
from snapper.data.native_account_identity_schema import native_account_identity_constraints
from snapper.data.native_account_identity_triggers import drop_native_account_identity_triggers
from snapper.data.native_account_identity_triggers import install_native_account_identity_triggers

revision: str = "0062"
down_revision: str | None = "0061"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
_TABLE = "native_account_identity_proofs"


def _require_online() -> None:
    """Require an actual supported caller transaction before identity-proof DDL."""
    if op.get_context().as_sql:
        raise RuntimeError("migration 0062 requires an online connection")
    if op.get_bind().dialect.name not in {"sqlite", "postgresql"}:
        raise NotImplementedError("migration 0062 supports only SQLite and PostgreSQL")


def _uuid_type() -> sa.String:
    """Preserve native PostgreSQL UUIDs and the established SQLite representation."""
    return sa.String(36).with_variant(UUID(as_uuid=False), "postgresql")


def _proof_columns() -> tuple[SchemaItem, ...]:
    """Store only whitelisted public identity and the complete actual attempt/root coordinate."""
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
        sa.Column("client_selection_digest", sa.String(64), nullable=False),
        sa.Column("attempt_public_id", _uuid_type(), nullable=False),
        sa.Column("attempt_content_digest", sa.String(64), nullable=False),
        sa.Column("owner_root_public_id", _uuid_type(), nullable=True),
        sa.Column("owner_root_content_digest", sa.String(64), nullable=True),
        sa.Column("owner_root_account_uid", _uuid_type(), nullable=True),
        sa.Column("source_policy", sa.String(64), nullable=False),
        sa.Column("account_uid", _uuid_type(), nullable=False),
        sa.Column("general_access", sa.String(16), nullable=False),
        sa.Column("transfer_access", sa.String(16), nullable=False),
        sa.Column("request_started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("content_digest", sa.String(64), nullable=False),
        sa.Column("canonical_payload", sa.Text(), nullable=False),
    )


def upgrade() -> None:
    """Create empty identity history without certifying older source material or API permissions."""
    _require_online()
    op.create_table(_TABLE, *_proof_columns(), *native_account_identity_constraints())
    for name, columns in NATIVE_ACCOUNT_IDENTITY_INITIAL_ROOT_KEYS:
        op.create_index(
            name,
            _TABLE,
            list(columns),
            unique=True,
            sqlite_where=sa.text(NATIVE_ACCOUNT_IDENTITY_INITIAL_ROOT_PREDICATE),
            postgresql_where=sa.text(NATIVE_ACCOUNT_IDENTITY_INITIAL_ROOT_PREDICATE),
        )
    op.create_index("ix_native_identity_family", _TABLE, ["scope_digest", "family_sequence"])
    install_native_account_identity_triggers(op.get_bind(), _TABLE)


def _fence_downgrade() -> None:
    """Retain the identity table's actual writer fence until caller commit or rollback."""
    connection = op.get_bind()
    dialect = connection.dialect.name
    if dialect == "postgresql":
        if connection.get_isolation_level() != "READ COMMITTED":
            raise RuntimeError("Native account identity downgrade requires READ COMMITTED")
        connection.execute(
            sa.text("LOCK TABLE native_account_identity_proofs IN ACCESS EXCLUSIVE MODE")
        )
        return
    if dialect != "sqlite":
        raise NotImplementedError("Native account identity downgrade supports SQLite/PostgreSQL")
    driver = connection.connection.driver_connection
    if getattr(driver, "autocommit", None) is True:
        raise RuntimeError(
            "Native account identity downgrade requires caller-controlled completion"
        )
    active = getattr(driver, "in_transaction", None)
    if not isinstance(active, bool):
        raise RuntimeError("Native account identity downgrade requires the SQLite driver state")
    if active:
        connection.exec_driver_sql("UPDATE native_account_identity_proofs SET id = id WHERE 0")
    else:
        connection.exec_driver_sql("BEGIN IMMEDIATE")
    if getattr(driver, "in_transaction", None) is not True:
        raise RuntimeError("Native account identity downgrade did not retain its transaction")


def downgrade() -> None:
    """Refuse loss of any first owner, credential rotation proof or retained account conflict."""
    _require_online()
    _fence_downgrade()
    connection = op.get_bind()
    if connection.scalar(sa.text("SELECT COUNT(*) FROM native_account_identity_proofs")):
        raise RuntimeError("Native account identity evidence prevents a lossless downgrade")
    drop_native_account_identity_triggers(connection, _TABLE)
    op.drop_table(_TABLE)
    if connection.dialect.name == "postgresql":
        connection.execute(
            sa.text("DROP FUNCTION IF EXISTS native_account_identity_reject_mutation()")
        )
