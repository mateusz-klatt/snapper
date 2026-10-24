"""Add immutable unit evidence for proven intents and fills without historical backfill."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

from snapper.data.execution_unit_snapshot_triggers import drop_execution_unit_snapshot_triggers
from snapper.data.execution_unit_snapshot_triggers import install_execution_unit_snapshot_triggers

revision: str = "0056"
down_revision: str | None = "0055"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _require_online() -> None:
    """Require a real dialect bind before creating physical immutability triggers."""
    if op.get_context().as_sql:
        raise RuntimeError("migration 0056 requires an online connection for immutability triggers")
    if op.get_bind().dialect.name not in {"sqlite", "postgresql"}:
        raise NotImplementedError("migration 0056 supports only SQLite and PostgreSQL")


def upgrade() -> None:
    """Create an empty append-only snapshot store without altering the execution ledger."""
    _require_online()
    uuid_type = sa.String(36).with_variant(UUID(as_uuid=False), "postgresql")
    timestamp_type = sa.DateTime(timezone=True)
    op.create_table(
        "execution_unit_snapshots",
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), primary_key=True),
        sa.Column("public_id", uuid_type, nullable=False),
        sa.Column("session_id", uuid_type, nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", timestamp_type, nullable=False),
        sa.Column("known_to", timestamp_type, nullable=False),
        sa.Column("phase", sa.String(16), nullable=False),
        sa.Column("wallet_public_id", uuid_type, nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False),
        sa.Column("instrument_public_id", uuid_type, nullable=False),
        sa.Column("command_public_id", uuid_type, nullable=True),
        sa.Column("execution_public_id", uuid_type, nullable=True),
        sa.Column("spec_public_id", uuid_type, nullable=False),
        sa.Column("spec_timestamp", timestamp_type, nullable=False),
        sa.Column("spec_source", sa.String(64), nullable=False),
        sa.Column("spec_version", sa.String(96), nullable=False),
        sa.Column("content_digest", sa.String(64), nullable=False),
        sa.Column("canonical_payload", sa.Text(), nullable=False),
        sa.CheckConstraint("phase IN ('submission', 'fill')", name="ck_unit_snapshot_phase"),
        sa.CheckConstraint("mode IN ('live', 'paper')", name="ck_unit_snapshot_mode"),
        sa.CheckConstraint(
            "exchange = LOWER(exchange) AND length(trim(exchange)) > 0",
            name="ck_unit_snapshot_exchange",
        ),
        sa.CheckConstraint(
            "(phase = 'submission' AND command_public_id IS NOT NULL AND execution_public_id IS NULL) "
            "OR (phase = 'fill' AND command_public_id IS NULL AND execution_public_id IS NOT NULL)",
            name="ck_unit_snapshot_target",
        ),
        sa.CheckConstraint("length(content_digest) = 64", name="ck_unit_snapshot_digest"),
        sa.CheckConstraint("length(canonical_payload) > 0", name="ck_unit_snapshot_payload"),
        sa.CheckConstraint(
            (
                "known_to = TIMESTAMPTZ '9999-12-31 23:59:59+00:00'"
                if op.get_bind().dialect.name == "postgresql"
                else "known_to = '9999-12-31 23:59:59.000000'"
            ),
            name="ck_unit_snapshot_known_to_max",
        ),
        sa.UniqueConstraint("public_id", name="uq_unit_snapshot_public_id"),
    )
    _create_indexes()
    install_execution_unit_snapshot_triggers(op.get_bind())


def _create_indexes() -> None:
    """Use target identities alone, never target plus digest, for immutable idempotency."""
    for column, suffix in (("command_public_id", "command"), ("execution_public_id", "execution")):
        predicate = sa.text(f"{column} IS NOT NULL")
        op.create_index(
            f"uq_unit_snapshot_{suffix}",
            "execution_unit_snapshots",
            [column],
            unique=True,
            sqlite_where=predicate,
            postgresql_where=predicate,
        )
    op.create_index(
        "ix_unit_snapshot_scope",
        "execution_unit_snapshots",
        ["wallet_public_id", "exchange", "mode", "instrument_public_id"],
    )


def _serialize_evidence_downgrade() -> None:
    """Reserve writes before reading evidence without committing the caller transaction."""
    connection = op.get_bind()
    dialect = connection.dialect.name
    if dialect == "postgresql":
        if connection.get_isolation_level() != "READ COMMITTED":
            raise RuntimeError("Evidence downgrade requires READ COMMITTED isolation")
        connection.execute(sa.text("LOCK TABLE execution_unit_snapshots IN ACCESS EXCLUSIVE MODE"))
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
        connection.exec_driver_sql("UPDATE execution_unit_snapshots SET id = id WHERE 0")
    else:
        connection.exec_driver_sql("BEGIN IMMEDIATE")
    if getattr(driver, "in_transaction", None) is not True:
        raise RuntimeError("Evidence downgrade did not retain its SQLite write transaction")


def downgrade() -> None:
    """Refuse removal of populated evidence and leave historical execution bytes untouched."""
    _require_online()
    _serialize_evidence_downgrade()
    count = op.get_bind().scalar(sa.text("SELECT COUNT(*) FROM execution_unit_snapshots"))
    if count:
        raise RuntimeError("Execution unit evidence prevents a lossless downgrade")
    drop_execution_unit_snapshot_triggers(op.get_bind())
    op.drop_table("execution_unit_snapshots")
