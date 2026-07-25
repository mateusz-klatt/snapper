"""Add the append-only ``execution_annulment_visibility`` observation ledger.

A correction's knowledge instant decides which historical answers may fold it,
so it must be a fact rather than a claim. The manifest cannot supply one: its
``timestamp`` is stamped just before the insert, and no supported dialect
assigns a commit-time value to an ordinary column, so a stalled writer could
commit a row carrying an instant from before it existed. This migration adds
the plane that closes that class: after the manifest transaction commits, a
second transaction re-reads the correction — seeing a committed row IS the
durability proof — and appends one observation stamped at that moment, giving
``annulment_durable_at <= observed_at`` by construction.

Purely additive: a brand-new table with no runtime writer reachable from this
migration. Both TOTAL unique indexes mirror the manifest doctrine (no
``known_to`` predicate), so one correction has exactly one observation under
either spelling of its identity, and the dual-dialect immutability triggers are
installed from the same shared installer the ORM ``after_create`` event uses,
so the migration-built and ``create_all``-built schemas are byte-identical by
construction. Revises 0038.
"""

from collections.abc import Sequence
from datetime import datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from snapper.data.ledger_triggers import (
    drop_execution_annulment_visibility_immutability_triggers as _drop_visibility_triggers,
)
from snapper.data.ledger_triggers import (
    install_execution_annulment_visibility_immutability_triggers as _install_visibility_triggers,
)

revision: str = "0039"
down_revision: str | None = "0038"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "execution_annulment_visibility"
_CK_MODE_LIVE_PAPER = "mode IN ('live', 'paper')"
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
    """Build the standard temporal provenance columns for the observation ledger.

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
            "migration 0039 requires an online connection: it installs "
            "dialect-specific execution annulment visibility immutability triggers"
        )


def upgrade() -> None:
    """Create the observation table, its indexes, and its immutability triggers.

    Returns:
        None.
    """
    _require_online_bind()
    op.create_table(
        _TABLE,
        sa.Column("id", _big_int_col(), autoincrement=True, nullable=False),
        sa.Column("annulment_public_id", _uuid_col(), nullable=False),
        sa.Column("annulment_id", _big_int_col(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("wallet_public_id", _uuid_col(), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False),
        *_temporal_columns(),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "exchange = LOWER(exchange) AND LENGTH(TRIM(exchange)) > 0",
            name="ck_execution_annulment_visibility_exchange_lower",
        ),
        sa.CheckConstraint(_CK_MODE_LIVE_PAPER, name="ck_execution_annulment_visibility_mode"),
        sa.CheckConstraint(
            "annulment_id >= 1",
            name="ck_execution_annulment_visibility_annulment_id",
        ),
        sa.CheckConstraint(
            _CK_KNOWN_TO_OPEN,
            name="ck_execution_annulment_visibility_known_to_open",
        ),
    )
    op.create_index(
        "uq_execution_annulment_visibility_annulment",
        _TABLE,
        ["annulment_public_id"],
        unique=True,
    )
    op.create_index(
        "uq_execution_annulment_visibility_annulment_id",
        _TABLE,
        ["annulment_id"],
        unique=True,
    )
    op.create_index(
        "ix_execution_annulment_visibility_public_id",
        _TABLE,
        ["public_id"],
        unique=True,
    )
    op.create_index(
        "ix_execution_annulment_visibility_scope",
        _TABLE,
        ["wallet_public_id", "mode", "observed_at"],
    )
    _install_visibility_triggers(op.get_bind())


def downgrade() -> None:
    """Drop the observation ledger's triggers and then the table itself.

    Returns:
        None.
    """
    _require_online_bind()
    _drop_visibility_triggers(op.get_bind())
    op.drop_table(_TABLE)
