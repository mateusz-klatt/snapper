"""Private source ownership 0064 private source ownership migration; no historical transport backfill."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.schema import SchemaItem

from snapper.data.private_source_ownership_schema import private_source_ownership_constraints
from snapper.data.private_source_ownership_triggers import drop_private_source_ownership_triggers
from snapper.data.private_source_ownership_triggers import install_private_source_ownership_triggers

revision: str = "0064"
down_revision: str | None = "0063"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
_TABLE = "private_source_ownership_receipts"


def _require_online() -> None:
    """Require an actual supported caller transaction before private source ownership DDL."""
    if op.get_context().as_sql:
        raise RuntimeError("migration 0064 requires an online connection")
    if op.get_bind().dialect.name not in {"sqlite", "postgresql"}:
        raise NotImplementedError("migration 0064 supports only SQLite and PostgreSQL")


def _uuid_type() -> sa.String:
    """Preserve native PostgreSQL UUIDs and the established SQLite representation."""
    return sa.String(36).with_variant(UUID(as_uuid=False), "postgresql")


def _ownership_columns() -> tuple[SchemaItem, ...]:
    """Retain selected public client, direct original source and original transport clocks."""
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
        sa.Column("account_family_digest", sa.String(64), nullable=False),
        sa.Column("client_selection_digest", sa.String(64), nullable=False),
        sa.Column("credential_public_id", _uuid_type(), nullable=False),
        sa.Column("credential_timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("public_api_origin", sa.String(256), nullable=False),
        sa.Column("raw_receipt_public_id", _uuid_type(), nullable=False),
        sa.Column("raw_content_digest", sa.String(64), nullable=False),
        sa.Column("target_json_pointer", sa.Text(), nullable=True),
        sa.Column("frame_basis_digest", sa.String(64), nullable=False),
        sa.Column("key_proof_public_id", _uuid_type(), nullable=False),
        sa.Column("key_proof_content_digest", sa.String(64), nullable=False),
        sa.Column("key_proof_recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("key_owner_root_public_id", _uuid_type(), nullable=False),
        sa.Column("key_owner_root_content_digest", sa.String(64), nullable=False),
        sa.Column("client_capture_public_id", _uuid_type(), nullable=False),
        sa.Column("connection_epoch_public_id", _uuid_type(), nullable=False),
        sa.Column("subscription_dispatch_public_id", _uuid_type(), nullable=False),
        sa.Column("frame_receive_public_id", _uuid_type(), nullable=False),
        sa.Column("subscription_feed", sa.String(32), nullable=False),
        sa.Column("raw_feed", sa.String(32), nullable=False),
        sa.Column("auth_encoded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ack_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("frame_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_recorded_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("family", sa.String(32), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("source_policy", sa.String(64), nullable=False),
        sa.Column("binding_kind", sa.String(24), nullable=False),
        sa.Column("origin_sidecar_public_id", _uuid_type(), nullable=True),
        sa.Column("origin_sidecar_content_digest", sa.String(64), nullable=True),
        sa.Column("content_digest", sa.String(64), nullable=False),
        sa.Column("canonical_payload", sa.Text(), nullable=False),
    )


def upgrade() -> None:
    """Create empty transport ownership history without certifying previous raw frames."""
    _require_online()
    op.create_table(_TABLE, *_ownership_columns(), *private_source_ownership_constraints())
    op.create_index("ix_private_owner_family", _TABLE, ["account_family_digest", "timestamp"])
    op.create_index(
        "ix_private_owner_receive", _TABLE, ["frame_receive_public_id", "target_json_pointer"]
    )
    install_private_source_ownership_triggers(op.get_bind(), _TABLE)


def _fence_downgrade() -> None:
    """Retain this table's actual writer fence until caller commit or rollback."""
    connection = op.get_bind()
    dialect = connection.dialect.name
    if dialect == "postgresql":
        if connection.get_isolation_level() != "READ COMMITTED":
            raise RuntimeError("Private source ownership downgrade requires READ COMMITTED")
        connection.execute(
            sa.text("LOCK TABLE private_source_ownership_receipts IN ACCESS EXCLUSIVE MODE")
        )
        return
    if dialect != "sqlite":
        raise NotImplementedError("Private source ownership downgrade supports SQLite/PostgreSQL")
    driver = connection.connection.driver_connection
    if getattr(driver, "autocommit", None) is True:
        raise RuntimeError(
            "Private source ownership downgrade requires caller-controlled completion"
        )
    active = getattr(driver, "in_transaction", None)
    if not isinstance(active, bool):
        raise RuntimeError("Private source ownership downgrade requires the SQLite driver state")
    if active:
        connection.exec_driver_sql("UPDATE private_source_ownership_receipts SET id = id WHERE 0")
    else:
        connection.exec_driver_sql("BEGIN IMMEDIATE")
    if getattr(driver, "in_transaction", None) is not True:
        raise RuntimeError("Private source ownership downgrade did not retain its transaction")


def downgrade() -> None:
    """Refuse loss of any original receive proof or direct historical recovery projection."""
    _require_online()
    _fence_downgrade()
    connection = op.get_bind()
    if connection.scalar(sa.text("SELECT COUNT(*) FROM private_source_ownership_receipts")):
        raise RuntimeError("Private source ownership evidence prevents a lossless downgrade")
    drop_private_source_ownership_triggers(connection, _TABLE)
    op.drop_table(_TABLE)
    if connection.dialect.name == "postgresql":
        connection.execute(
            sa.text("DROP FUNCTION IF EXISTS private_source_ownership_reject_mutation()")
        )
