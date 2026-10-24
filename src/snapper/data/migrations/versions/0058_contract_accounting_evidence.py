"""Add immutable raw accounting receipts, account holds, anchors and distinct ledger legs."""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.schema import SchemaItem

from snapper.data.contract_accounting_schema import accounting_evidence_constraints
from snapper.data.contract_accounting_triggers import ACCOUNTING_EVIDENCE_TABLES
from snapper.data.contract_accounting_triggers import drop_contract_accounting_table_triggers
from snapper.data.contract_accounting_triggers import install_contract_accounting_table_triggers

revision: str = "0058"
down_revision: str | None = "0057"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _require_online() -> None:
    """Require the caller's real supported transaction for physical evidence guards."""
    if op.get_context().as_sql:
        raise RuntimeError("migration 0058 requires an online connection")
    if op.get_bind().dialect.name not in {"sqlite", "postgresql"}:
        raise NotImplementedError("migration 0058 supports only SQLite and PostgreSQL")


def _uuid_type() -> sa.String:
    """Retain native PostgreSQL UUID storage without changing SQLite portability."""
    return sa.String(36).with_variant(UUID(as_uuid=False), "postgresql")


def _scope_columns(*, margin_required: bool = False) -> tuple[SchemaItem, ...]:
    """Create fresh provenance and actual financial-scope columns for one table."""
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
        sa.Column("margin_account", sa.String(128), nullable=not margin_required),
    )


def _payload_columns() -> tuple[SchemaItem, ...]:
    """Keep canonical immutable evidence separate from indexed compatibility copies."""
    return (
        sa.Column("content_digest", sa.String(64), nullable=False),
        sa.Column("canonical_payload", sa.Text(), nullable=False),
    )


def _create_raw_receipts() -> None:
    """Retain every observation, including changed-content retries and unknown authority."""
    op.create_table(
        "accounting_raw_receipts",
        *_scope_columns(),
        sa.Column("claimed_public_id", _uuid_type(), nullable=False),
        sa.Column("scope_digest", sa.String(64), nullable=False),
        sa.Column("route_authority_public_id", _uuid_type(), nullable=True),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        *_payload_columns(),
        *accounting_evidence_constraints("acct_raw"),
        sa.CheckConstraint("length(scope_digest) = 64", name="ck_acct_raw_scope_digest"),
    )


def _create_holds() -> None:
    """Keep account-family unavailability independent of mutable projection watermarks."""
    op.create_table(
        "accounting_scope_holds",
        *_scope_columns(),
        sa.Column("scope_digest", sa.String(64), nullable=False),
        sa.Column("raw_receipt_public_id", _uuid_type(), nullable=False),
        sa.Column("reason", sa.String(64), nullable=False),
        *accounting_evidence_constraints("acct_hold", has_payload=False),
        sa.UniqueConstraint("raw_receipt_public_id", "reason", name="uq_acct_hold_receipt_reason"),
        sa.CheckConstraint("length(scope_digest) = 64", name="ck_acct_hold_scope_digest"),
    )
    op.create_table(
        "accounting_hold_resolutions",
        *_scope_columns(),
        sa.Column("hold_public_id", _uuid_type(), nullable=False),
        sa.Column("accounting_event_public_id", _uuid_type(), nullable=False),
        sa.Column("route_authority_public_id", _uuid_type(), nullable=False),
        sa.Column("resolution_kind", sa.String(32), nullable=False),
        *_payload_columns(),
        *accounting_evidence_constraints("acct_resolution"),
        sa.UniqueConstraint("hold_public_id", name="uq_acct_resolution_hold"),
    )


def _create_anchors() -> None:
    """Require explicit account sources instead of manufacturing a starting balance."""
    op.create_table(
        "contract_accounting_anchors",
        *_scope_columns(margin_required=True),
        sa.Column("account_scope_digest", sa.String(64), nullable=False),
        sa.Column("route_authority_public_id", _uuid_type(), nullable=False),
        sa.Column("source_receipt_public_id", _uuid_type(), nullable=False),
        sa.Column("anchor_kind", sa.String(32), nullable=False),
        sa.Column("source_execution_prefix_status", sa.String(16), nullable=False),
        sa.Column("source_execution_scope_sequence", sa.BigInteger(), nullable=True),
        *_payload_columns(),
        *accounting_evidence_constraints("contract_anchor"),
        sa.UniqueConstraint(
            "source_receipt_public_id", "account_scope_digest", name="uq_contract_anchor_source"
        ),
        sa.CheckConstraint("length(account_scope_digest) = 64", name="ck_contract_anchor_scope"),
        sa.CheckConstraint(
            "anchor_kind IN ('native_cash_old_balance', 'venue_account_snapshot', 'position_source_seed')",
            name="ck_contract_anchor_kind",
        ),
        sa.CheckConstraint(
            "(source_execution_prefix_status = 'unavailable' AND source_execution_scope_sequence IS NULL) OR "
            "(source_execution_prefix_status = 'sealed' AND source_execution_scope_sequence IS NOT NULL AND source_execution_scope_sequence >= 0)",
            name="ck_contract_anchor_prefix",
        ),
    )


def _create_ledger_events() -> None:
    """Separate actual native-cash legs from position economics and execution deduplication."""
    op.create_table(
        "contract_accounting_events",
        *_scope_columns(margin_required=True),
        sa.Column("account_scope_digest", sa.String(64), nullable=False),
        sa.Column("route_authority_public_id", _uuid_type(), nullable=False),
        sa.Column("anchor_public_id", _uuid_type(), nullable=False),
        sa.Column("raw_receipt_public_id", _uuid_type(), nullable=False),
        sa.Column("ledger_sequence", sa.BigInteger(), nullable=False),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("source_identity", sa.String(256), nullable=False),
        sa.Column("instrument_public_id", _uuid_type(), nullable=True),
        sa.Column("execution_public_id", _uuid_type(), nullable=True),
        sa.Column("unit_snapshot_public_id", _uuid_type(), nullable=True),
        *_payload_columns(),
        *accounting_evidence_constraints("contract_event"),
        sa.UniqueConstraint(
            "account_scope_digest", "ledger_sequence", name="uq_contract_event_sequence"
        ),
        sa.UniqueConstraint(
            "account_scope_digest", "kind", "source_identity", name="uq_contract_event_source"
        ),
        sa.CheckConstraint("ledger_sequence > 0", name="ck_contract_event_sequence"),
        sa.CheckConstraint("length(account_scope_digest) = 64", name="ck_contract_event_scope"),
    )


def _create_indexes() -> None:
    """Index account identities without temporal filters that could hide unresolved evidence."""
    indexes: tuple[tuple[str, str, list[str]], ...] = (
        ("ix_acct_raw_scope", "accounting_raw_receipts", ["scope_digest", "id"]),
        ("ix_acct_raw_claimed", "accounting_raw_receipts", ["claimed_public_id"]),
        ("ix_acct_hold_scope", "accounting_scope_holds", ["scope_digest", "margin_account", "id"]),
        ("ix_contract_anchor_scope", "contract_accounting_anchors", ["account_scope_digest", "id"]),
        (
            "ix_contract_event_scope",
            "contract_accounting_events",
            ["account_scope_digest", "ledger_sequence"],
        ),
    )
    for name, table, columns in indexes:
        op.create_index(name, table, columns)
    predicate = sa.text("execution_public_id IS NOT NULL")
    op.create_index(
        "uq_contract_event_execution",
        "contract_accounting_events",
        ["execution_public_id"],
        unique=True,
        sqlite_where=predicate,
        postgresql_where=predicate,
    )


def upgrade() -> None:
    """Create empty append-only stores without historical economic backfills."""
    _require_online()
    _create_raw_receipts()
    _create_holds()
    _create_anchors()
    _create_ledger_events()
    _create_indexes()
    for table_name in ACCOUNTING_EVIDENCE_TABLES:
        install_contract_accounting_table_triggers(op.get_bind(), table_name)


def _fence_downgrade() -> None:
    """Reserve writes before checking evidence and retain the caller's transaction fence."""
    connection = op.get_bind()
    dialect = connection.dialect.name
    if dialect == "postgresql":
        if connection.get_isolation_level() != "READ COMMITTED":
            raise RuntimeError("Accounting evidence downgrade requires READ COMMITTED")
        tables = ", ".join(ACCOUNTING_EVIDENCE_TABLES)
        connection.execute(sa.text(f"LOCK TABLE {tables} IN ACCESS EXCLUSIVE MODE"))
        return
    if dialect != "sqlite":
        raise NotImplementedError("Accounting evidence downgrade supports only SQLite/PostgreSQL")
    driver = connection.connection.driver_connection
    if getattr(driver, "autocommit", None) is True:
        raise RuntimeError("Accounting evidence downgrade requires caller-controlled completion")
    active = getattr(driver, "in_transaction", None)
    if not isinstance(active, bool):
        raise RuntimeError("Accounting evidence downgrade requires the SQLite driver state")
    if active:
        connection.exec_driver_sql("UPDATE accounting_raw_receipts SET id = id WHERE 0")
    else:
        connection.exec_driver_sql("BEGIN IMMEDIATE")
    if getattr(driver, "in_transaction", None) is not True:
        raise RuntimeError("Accounting evidence downgrade did not retain its write transaction")


def downgrade() -> None:
    """Refuse removal of any retained accounting evidence, even after a hold is resolved."""
    _require_online()
    _fence_downgrade()
    connection = op.get_bind()
    for table_name in ACCOUNTING_EVIDENCE_TABLES:
        if connection.scalar(sa.text(f"SELECT COUNT(*) FROM {table_name}")):
            raise RuntimeError("Accounting evidence prevents a lossless downgrade")
    for table_name in reversed(ACCOUNTING_EVIDENCE_TABLES):
        drop_contract_accounting_table_triggers(connection, table_name)
        op.drop_table(table_name)
    if connection.dialect.name == "postgresql":
        connection.execute(sa.text("DROP FUNCTION IF EXISTS accounting_evidence_reject_mutation()"))
