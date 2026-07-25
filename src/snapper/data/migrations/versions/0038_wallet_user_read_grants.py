"""Add the ``wallet_user_read_grants`` read-visibility plane.

Wallet visibility has only ever had the TRADE plane to lean on, and that plane
cannot express per-user read access. Every ``wallet_operator_scope_grants`` row
is instrument-exclusive on its wallet — both partial unique indexes omit
``operator_public_id`` — so an instrument or underlying on a wallet is covered
by exactly ONE operator's grant globally, and two people who must both see the
same wallet would have to share a single operator.

This migration adds the plane where the USER is in the key: one bitemporal
table whose active-unique index spans ``(user_public_id, wallet_public_id)``, so
any number of distinct users may hold a live read grant on one wallet while each
pair still admits at most one. ``granted_by_user_public_id`` is nullable because
a seed-provisioned grant has no human granter. Purely additive: a brand-new
table with no runtime writer or public surface reachable from this migration,
and both dialects build the same schema directly. Revises 0037.
"""

from collections.abc import Sequence
from datetime import datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

revision: str = "0038"
down_revision: str | None = "0037"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "wallet_user_read_grants"
_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"


def _uuid_col() -> sa.types.TypeEngine[str]:
    """Build the per-dialect public identity column type.

    Returns:
        SQLAlchemy type using native PostgreSQL UUID and SQLite text storage.
    """
    return sa.String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql")


def _temporal_columns() -> tuple[
    sa.Column[int],
    sa.Column[str],
    sa.Column[str],
    sa.Column[int],
    sa.Column[datetime],
    sa.Column[datetime],
]:
    """Build the standard temporal columns shared by SCD2 tables.

    Returns:
        Fresh SQLAlchemy column objects for the table.
    """
    return (
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("session_id", _uuid_col(), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
    )


def upgrade() -> None:
    """Create the ``wallet_user_read_grants`` table and its indexes.

    Returns:
        None.
    """
    op.create_table(
        _TABLE,
        sa.Column("user_public_id", _uuid_col(), nullable=False),
        sa.Column("wallet_public_id", _uuid_col(), nullable=False),
        sa.Column("granted_by_user_public_id", _uuid_col(), nullable=True),
        sa.Column("note", sa.String(512), nullable=True),
        *_temporal_columns(),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_wallet_user_read_grants_public_id",
        _TABLE,
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_wallet_user_read_grants_unique_active",
        _TABLE,
        ["user_public_id", "wallet_public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_wallet_user_read_grants_user",
        _TABLE,
        ["user_public_id"],
    )
    op.create_index(
        "ix_wallet_user_read_grants_wallet",
        _TABLE,
        ["wallet_public_id"],
    )


def downgrade() -> None:
    """Drop the ``wallet_user_read_grants`` table.

    Returns:
        None.
    """
    op.drop_table(_TABLE)
