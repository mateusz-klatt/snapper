"""Add immutable shared account-route and current submission admission evidence."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

from snapper.data.execution_admission_triggers import drop_execution_admission_triggers
from snapper.data.execution_admission_triggers import install_execution_admission_triggers

revision: str = "0057"
down_revision: str | None = "0056"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _require_online() -> None:
    """Require a real supported dialect before creating physical evidence guards."""
    if op.get_context().as_sql:
        raise RuntimeError("migration0057 requires an online connection for evidence guards")
    if op.get_bind().dialect.name not in {"sqlite", "postgresql"}:
        raise NotImplementedError("migration0057 supports SQLite and PostgreSQL")


def upgrade() -> None:
    """Create empty route and admission witnesses without guessing historical authority."""
    _require_online()
    _create_route_authorities()
    _create_unit_admissions()
    install_execution_admission_triggers(op.get_bind())


def _create_route_authorities() -> None:
    """Create execution_route_authorities with immutable identities and indexed public scope."""
    uuid_type = sa.String(36).with_variant(UUID(as_uuid=False), "postgresql")
    timestamp_type = sa.DateTime(timezone=True)
    op.create_table(
        "execution_route_authorities",
        sa.Column(
            "id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
            primary_key=True,
        ),
        sa.Column("public_id", uuid_type, nullable=False),
        sa.Column("session_id", uuid_type, nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", timestamp_type, nullable=False),
        sa.Column("known_to", timestamp_type, nullable=False),
        sa.Column("wallet_public_id", uuid_type, nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False),
        sa.Column("environment", sa.String(16), nullable=False),
        sa.Column("margin_account", sa.String(128), nullable=False),
        sa.Column("account_scope_digest", sa.String(64), nullable=False),
        sa.Column("credential_public_id", uuid_type, nullable=True),
        sa.Column("credential_timestamp", timestamp_type, nullable=True),
        sa.Column("venue_account_id", sa.String(256), nullable=True),
        sa.Column("account_observation_public_id", uuid_type, nullable=False),
        sa.Column("account_observation_timestamp", timestamp_type, nullable=False),
        sa.Column("observed_at", timestamp_type, nullable=False),
        sa.Column("valid_until", timestamp_type, nullable=False),
        sa.Column("route_revision", sa.String(64), nullable=False),
        sa.Column("content_digest", sa.String(64), nullable=False),
        sa.Column("canonical_payload", sa.Text(), nullable=False),
        sa.UniqueConstraint("public_id", name="uq_execution_route_authority_public_id"),
        sa.UniqueConstraint("content_digest", name="uq_execution_route_authority_content"),
        sa.CheckConstraint(
            "exchange = LOWER(exchange) AND length(trim(exchange)) > 0",
            name="ck_execution_route_authority_exchange",
        ),
        sa.CheckConstraint("mode IN ('live', 'paper')", name="ck_execution_route_authority_mode"),
        sa.CheckConstraint(
            "environment IN ('production', 'demo', 'paper')",
            name="ck_execution_route_authority_environment",
        ),
        sa.CheckConstraint(
            "(exchange = 'paper' AND mode = 'paper' AND environment = 'paper') OR (exchange != 'paper' AND mode = 'live' AND environment IN ('production', 'demo'))",
            name="ck_execution_route_authority_scope",
        ),
        sa.CheckConstraint(
            "length(trim(margin_account)) > 0", name="ck_execution_route_authority_margin_account"
        ),
        sa.CheckConstraint(
            "(credential_public_id IS NULL AND credential_timestamp IS NULL) OR (credential_public_id IS NOT NULL AND credential_timestamp IS NOT NULL)",
            name="ck_execution_route_authority_credential_pair",
        ),
        sa.CheckConstraint(
            "environment = 'paper' OR credential_public_id IS NOT NULL",
            name="ck_execution_route_authority_credential_required",
        ),
        sa.CheckConstraint(
            "length(account_scope_digest) = 64 AND length(route_revision) = 64 AND length(content_digest) = 64",
            name="ck_execution_route_authority_digests",
        ),
        sa.CheckConstraint(
            "length(canonical_payload) > 0", name="ck_execution_route_authority_payload"
        ),
        sa.CheckConstraint(
            "observed_at < valid_until", name="ck_execution_route_authority_validity"
        ),
        sa.CheckConstraint(
            (
                "known_to = TIMESTAMPTZ '9999-12-31 23:59:59+00:00'"
                if op.get_bind().dialect.name == "postgresql"
                else "known_to = '9999-12-31 23:59:59.000000'"
            ),
            name="ck_execution_route_authority_known_to_max",
        ),
    )
    op.create_index(
        "ix_execution_route_authority_scope",
        "execution_route_authorities",
        ["account_scope_digest", "observed_at"],
    )
    op.create_index(
        "ix_execution_route_authority_wallet",
        "execution_route_authorities",
        ["wallet_public_id", "exchange", "mode"],
    )


def _create_unit_admissions() -> None:
    """Create submission_unit_admissions with immutable identities and indexed public scope."""
    uuid_type = sa.String(36).with_variant(UUID(as_uuid=False), "postgresql")
    timestamp_type = sa.DateTime(timezone=True)
    op.create_table(
        "submission_unit_admissions",
        sa.Column(
            "id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            nullable=False,
            primary_key=True,
        ),
        sa.Column("public_id", uuid_type, nullable=False),
        sa.Column("session_id", uuid_type, nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", timestamp_type, nullable=False),
        sa.Column("known_to", timestamp_type, nullable=False),
        sa.Column("attempt_public_id", uuid_type, nullable=False),
        sa.Column("wallet_public_id", uuid_type, nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False),
        sa.Column("instrument_public_id", uuid_type, nullable=False),
        sa.Column("command_public_id", uuid_type, nullable=False),
        sa.Column("client_order_id", sa.String(128), nullable=False),
        sa.Column("authority_public_id", uuid_type, nullable=False),
        sa.Column("authority_content_digest", sa.String(64), nullable=False),
        sa.Column("account_scope_digest", sa.String(64), nullable=False),
        sa.Column("command_snapshot_public_id", uuid_type, nullable=False),
        sa.Column("command_snapshot_content_digest", sa.String(64), nullable=False),
        sa.Column("source_exchange", sa.String(32), nullable=False),
        sa.Column("source_instrument_public_id", uuid_type, nullable=False),
        sa.Column("current_spec_public_id", uuid_type, nullable=False),
        sa.Column("current_spec_timestamp", timestamp_type, nullable=False),
        sa.Column("source_spec_public_id", uuid_type, nullable=False),
        sa.Column("source_spec_timestamp", timestamp_type, nullable=False),
        sa.Column("economic_context_digest", sa.String(64), nullable=False),
        sa.Column("request_digest", sa.String(64), nullable=False),
        sa.Column("interpreted_at", timestamp_type, nullable=False),
        sa.Column("content_digest", sa.String(64), nullable=False),
        sa.Column("canonical_payload", sa.Text(), nullable=False),
        sa.UniqueConstraint("public_id", name="uq_submission_unit_admission_public_id"),
        sa.UniqueConstraint("attempt_public_id", name="uq_submission_unit_admission_attempt"),
        sa.CheckConstraint(
            "exchange = LOWER(exchange) AND length(trim(exchange)) > 0",
            name="ck_submission_unit_admission_exchange",
        ),
        sa.CheckConstraint(
            "source_exchange = LOWER(source_exchange) AND length(trim(source_exchange)) > 0",
            name="ck_submission_unit_admission_source_exchange",
        ),
        sa.CheckConstraint("mode IN ('live', 'paper')", name="ck_submission_unit_admission_mode"),
        sa.CheckConstraint(
            "length(account_scope_digest) = 64 AND length(authority_content_digest) = 64 AND length(command_snapshot_content_digest) = 64 AND length(economic_context_digest) = 64 AND length(request_digest) = 64 AND length(content_digest) = 64",
            name="ck_submission_unit_admission_digests",
        ),
        sa.CheckConstraint(
            "length(canonical_payload) > 0", name="ck_submission_unit_admission_payload"
        ),
        sa.CheckConstraint(
            (
                "known_to = TIMESTAMPTZ '9999-12-31 23:59:59+00:00'"
                if op.get_bind().dialect.name == "postgresql"
                else "known_to = '9999-12-31 23:59:59.000000'"
            ),
            name="ck_submission_unit_admission_known_to_max",
        ),
    )
    op.create_index(
        "ix_submission_unit_admission_command",
        "submission_unit_admissions",
        ["command_public_id", "interpreted_at"],
    )
    op.create_index(
        "ix_submission_unit_admission_account",
        "submission_unit_admissions",
        ["account_scope_digest", "interpreted_at"],
    )
    op.create_index(
        "ix_submission_unit_admission_source",
        "submission_unit_admissions",
        ["source_exchange", "source_instrument_public_id"],
    )


def _serialize_evidence_downgrade() -> None:
    """Fence both tables before checking evidence, retaining the caller transaction."""
    connection = op.get_bind()
    if connection.dialect.name == "postgresql":
        if connection.get_isolation_level() != "READ COMMITTED":
            raise RuntimeError("Admission evidence downgrade requires READ COMMITTED isolation")
        connection.execute(
            sa.text(
                "LOCK TABLE execution_route_authorities, submission_unit_admissions IN ACCESS EXCLUSIVE MODE"
            )
        )
        return
    if connection.dialect.name != "sqlite":
        raise NotImplementedError("Admission evidence downgrade supports SQLite and PostgreSQL")
    driver = connection.connection.driver_connection
    if getattr(driver, "autocommit", None) is True:
        raise RuntimeError("Admission evidence downgrade requires caller transaction completion")
    active = getattr(driver, "in_transaction", None)
    if not isinstance(active, bool):
        raise RuntimeError("Admission evidence downgrade requires SQLite transaction state")
    if active:
        connection.exec_driver_sql("UPDATE execution_route_authorities SET id = id WHERE 0")
    else:
        connection.exec_driver_sql("BEGIN IMMEDIATE")
    if getattr(driver, "in_transaction", None) is not True:
        raise RuntimeError("Admission evidence downgrade lost its SQLite write transaction")


def downgrade() -> None:
    """Refuse to remove populated route or admission evidence, including concurrent writers."""
    _require_online()
    _serialize_evidence_downgrade()
    connection = op.get_bind()
    for table in ("execution_route_authorities", "submission_unit_admissions"):
        if connection.scalar(sa.text(f"SELECT EXISTS(SELECT 1 FROM {table})")):
            raise RuntimeError("Execution route/admission evidence prevents a lossless downgrade")
    drop_execution_admission_triggers(connection)
    op.drop_table("submission_unit_admissions")
    op.drop_table("execution_route_authorities")
