"""Add the append-only ``execution_annulments`` correction manifest.

``executions`` is an event ledger whose committed rows can never be closed or
revised, so a fill booked by a defect cannot be taken back in place. This
migration adds the plane where the ledger's own answer — corrections enter as
new appended events — is recorded: one row uniquely targets one execution by
immutable public id plus canonical row digest, restates the target's
certification coordinates, and carries the acting user, correction knowledge
time, a CHECK-constrained reason and a JSON evidence envelope.

Purely additive: a brand-new table with no runtime writer or public surface
reachable from this migration. Both TOTAL unique indexes mirror the executions
doctrine (no ``known_to`` predicate), and the dual-dialect immutability triggers
are installed from the same shared installer the ORM ``after_create`` event
uses, so the migration-built and ``create_all``-built schemas are byte-identical
by construction. Revises 0036.
"""

from collections.abc import Sequence
from datetime import datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from snapper.data.ledger_triggers import drop_execution_annulment_immutability_triggers
from snapper.data.ledger_triggers import install_execution_annulment_immutability_triggers

revision: str = "0037"
down_revision: str | None = "0036"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "execution_annulments"
_CK_MODE_LIVE_PAPER = "mode IN ('live', 'paper')"
_CK_REASON = "reason IN ('unwitnessed_phantom', 'unwitnessed_legacy_lineage')"
_CK_KNOWN_TO_OPEN = "known_to >= '9999-12-31 00:00:00+00'"


def _uuid_col() -> sa.types.TypeEngine[str]:
    """Build the per-dialect public identity column type.

    Returns:
        SQLAlchemy type using native PostgreSQL UUID and SQLite text storage.
    """
    return sa.String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql")


def _big_int_col() -> sa.types.TypeEngine[int]:
    """Build the per-dialect wide counter column type.

    Returns:
        SQLAlchemy type using PostgreSQL BIGINT and SQLite INTEGER storage.
    """
    return sa.BigInteger().with_variant(sa.Integer(), "sqlite")


def _temporal_columns() -> tuple[
    sa.Column[str],
    sa.Column[str],
    sa.Column[int],
    sa.Column[datetime],
    sa.Column[datetime],
]:
    """Build the standard temporal provenance columns for the manifest.

    Returns:
        Fresh SQLAlchemy column objects for the table.
    """
    return (
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("session_id", _uuid_col(), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
    )


def _require_online_bind() -> None:
    """Refuse offline SQL rendering before dialect-specific trigger DDL."""
    if op.get_context().as_sql:
        raise RuntimeError(
            "migration 0037 requires an online connection: it installs "
            "dialect-specific execution annulment immutability triggers"
        )


def upgrade() -> None:
    """Create the manifest table, its indexes, and its immutability triggers.

    Returns:
        None.
    """
    _require_online_bind()
    op.create_table(
        _TABLE,
        sa.Column("id", _big_int_col(), autoincrement=True, nullable=False),
        sa.Column("target_execution_public_id", _uuid_col(), nullable=False),
        sa.Column("target_execution_digest", sa.String(64), nullable=False),
        sa.Column("wallet_public_id", _uuid_col(), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False),
        sa.Column("scope_sequence", _big_int_col(), nullable=False),
        sa.Column("annulled_by_user_public_id", _uuid_col(), nullable=False),
        sa.Column("correction_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sa.String(32), nullable=False),
        sa.Column("evidence_json", sa.Text(), nullable=False),
        *_temporal_columns(),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "exchange = LOWER(exchange) AND LENGTH(TRIM(exchange)) > 0",
            name="ck_execution_annulments_exchange_lower",
        ),
        sa.CheckConstraint(_CK_MODE_LIVE_PAPER, name="ck_execution_annulments_mode"),
        sa.CheckConstraint("scope_sequence >= 1", name="ck_execution_annulments_scope_sequence"),
        sa.CheckConstraint(_CK_REASON, name="ck_execution_annulments_reason"),
        sa.CheckConstraint(
            "LENGTH(target_execution_digest) = 64 AND "
            "target_execution_digest = LOWER(target_execution_digest)",
            name="ck_execution_annulments_digest",
        ),
        sa.CheckConstraint(
            "LENGTH(TRIM(evidence_json)) > 0",
            name="ck_execution_annulments_evidence",
        ),
        sa.CheckConstraint(_CK_KNOWN_TO_OPEN, name="ck_execution_annulments_known_to_open"),
    )
    op.create_index(
        "uq_execution_annulments_target",
        _TABLE,
        ["target_execution_public_id"],
        unique=True,
    )
    op.create_index(
        "uq_execution_annulments_scope",
        _TABLE,
        ["wallet_public_id", "exchange", "mode", "scope_sequence"],
        unique=True,
    )
    op.create_index(
        "ix_execution_annulments_public_id",
        _TABLE,
        ["public_id"],
        unique=True,
    )
    op.create_index(
        "ix_execution_annulments_manifest",
        _TABLE,
        ["wallet_public_id", "mode", "correction_time"],
    )
    install_execution_annulment_immutability_triggers(op.get_bind())


def downgrade() -> None:
    """Drop the manifest's triggers and then the table itself.

    Returns:
        None.
    """
    _require_online_bind()
    drop_execution_annulment_immutability_triggers(op.get_bind())
    op.drop_table(_TABLE)
