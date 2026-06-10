"""Add paired-execution guard SCD2 tables and signal/venue group columns.

Creates the three Slowly-Changing-Dimension Type 2 tables that back the
multi-leg paired-execution guard: ``paired_execution_groups`` (the
bounded-compensation group FSM), ``paired_execution_legs`` (the
authoritative per-leg map with signed fill / compensation accounting),
and ``paired_execution_halts`` (the strategy-pair halt projection). Also adds a single
denormalised, nullable ``paired_group_id`` column to ``signals`` and
``venue_events`` for guard provenance / handler fast-paths.

Revises 0002. Mirrors 0001's conventions: ``_uuid_col()`` for UUID
columns (native ``UUID`` on Postgres, ``VARCHAR(36)`` on SQLite),
``sa.DateTime(timezone=True)`` matching the :class:`TZDateTime`
decorator, named ``CheckConstraint``s, and partial unique indexes
restricted to the SCD2-active rows (``known_to = KNOWN_TO_MAX``) with
dialect-specific literals. The status columns are deliberately NOT
constrained by a CHECK so the guard FSM can extend without
a constraint-widening migration; only the immutable ``policy`` / ``side``
/ ``mode`` enums are pinned.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CK_SEQUENCE_ID = "sequence_id > 0"
_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"
_PEL_COMMAND_ACTIVE_PG = "command_public_id IS NOT NULL AND known_to = '9999-12-31T23:59:59+00:00'"
_PEL_COMMAND_ACTIVE_SQLITE = (
    "command_public_id IS NOT NULL AND known_to = '9999-12-31 23:59:59.000000'"
)
_PAIRED_GROUP_ID_NOT_NULL = "paired_group_id IS NOT NULL"


def _uuid_col() -> sa.types.TypeEngine[str]:
    """Per-dialect UUID column: native ``UUID`` on Postgres, ``VARCHAR(36)`` on SQLite.

    Built fresh for each column site so SQLAlchemy doesn't cache a
    single instance across unrelated columns. Mirrors the
    :class:`snapper.data.models.UUIDColumn` decorator.
    """
    return sa.String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql")


def upgrade() -> None:
    """Create the paired-execution tables and add the group provenance columns."""
    op.create_table(
        "paired_execution_groups",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("wallet_public_id", _uuid_col(), nullable=False),
        sa.Column("operator_public_id", _uuid_col(), nullable=True),
        sa.Column("strategy_id", sa.String(64), nullable=False),
        sa.Column("policy", sa.String(24), nullable=False),
        sa.Column("expected_leg_count", sa.Integer(), nullable=False),
        sa.Column("group_key", sa.String(512), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("assembly_deadline", sa.DateTime(timezone=True), nullable=False),
        sa.Column("fill_deadline", sa.DateTime(timezone=True), nullable=False),
        sa.Column("failure_reason", sa.String(512), nullable=True),
        sa.Column("halted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("session_id", _uuid_col(), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_paired_execution_groups_sequence_id"),
        sa.CheckConstraint(
            "policy IN ('simultaneous', 'sequential_handoff')",
            name="ck_peg_policy",
        ),
    )
    op.create_index(
        "ix_peg_public_id",
        "paired_execution_groups",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index("ix_peg_status", "paired_execution_groups", ["status"])
    op.create_index("ix_peg_group_key", "paired_execution_groups", ["group_key"])

    op.create_table(
        "paired_execution_legs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("group_public_id", _uuid_col(), nullable=False),
        sa.Column("leg_index", sa.Integer(), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False),
        sa.Column("instrument", sa.String(64), nullable=False),
        sa.Column("shard_key", sa.String(256), nullable=False),
        sa.Column("side", sa.String(4), nullable=False),
        sa.Column("target_qty", sa.Float(), nullable=False),
        sa.Column("signal_public_id", _uuid_col(), nullable=False),
        sa.Column("command_public_id", _uuid_col(), nullable=True),
        sa.Column("client_order_id", sa.String(64), nullable=True),
        sa.Column("exchange_order_id", sa.String(64), nullable=True),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("filled_signed_qty", sa.Float(), nullable=False, server_default="0"),
        sa.Column("compensated_signed_qty", sa.Float(), nullable=False, server_default="0"),
        sa.Column("compensation_seq", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_venue_event_id", sa.Integer(), nullable=True),
        sa.Column("wallet_public_id", _uuid_col(), nullable=False),
        sa.Column("operator_public_id", _uuid_col(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("session_id", _uuid_col(), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_paired_execution_legs_sequence_id"),
        sa.CheckConstraint("side IN ('buy', 'sell')", name="ck_pel_side"),
        sa.CheckConstraint("mode IN ('live', 'paper')", name="ck_pel_mode"),
    )
    op.create_index(
        "ix_pel_public_id",
        "paired_execution_legs",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "uq_pel_group_leg",
        "paired_execution_legs",
        ["group_public_id", "leg_index"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "uq_pel_command",
        "paired_execution_legs",
        ["command_public_id"],
        unique=True,
        sqlite_where=text(_PEL_COMMAND_ACTIVE_SQLITE),
        postgresql_where=text(_PEL_COMMAND_ACTIVE_PG),
    )
    op.create_index("ix_pel_group", "paired_execution_legs", ["group_public_id"])
    op.create_index("ix_pel_client_order_id", "paired_execution_legs", ["client_order_id"])
    op.create_index("ix_pel_exchange_order_id", "paired_execution_legs", ["exchange_order_id"])
    op.create_index("ix_pel_shard_status", "paired_execution_legs", ["shard_key", "status"])

    op.create_table(
        "paired_execution_halts",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("wallet_public_id", _uuid_col(), nullable=False),
        sa.Column("operator_public_id", _uuid_col(), nullable=True),
        sa.Column("strategy_id", sa.String(64), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False),
        sa.Column("group_key", sa.String(512), nullable=False),
        sa.Column("group_public_id", _uuid_col(), nullable=False),
        sa.Column("reason", sa.String(512), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("session_id", _uuid_col(), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_paired_execution_halts_sequence_id"),
        sa.CheckConstraint("mode IN ('live', 'paper')", name="ck_peh_mode"),
    )
    op.create_index(
        "ix_peh_public_id",
        "paired_execution_halts",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "uq_peh_scope",
        "paired_execution_halts",
        ["wallet_public_id", "strategy_id", "group_key"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index("ix_peh_group_key", "paired_execution_halts", ["group_key"])

    op.add_column("signals", sa.Column("paired_group_id", _uuid_col(), nullable=True))
    op.create_index(
        "ix_signals_paired_group_id",
        "signals",
        ["paired_group_id"],
        sqlite_where=text(_PAIRED_GROUP_ID_NOT_NULL),
        postgresql_where=text(_PAIRED_GROUP_ID_NOT_NULL),
    )
    op.add_column("venue_events", sa.Column("paired_group_id", _uuid_col(), nullable=True))
    op.create_index(
        "ix_venue_events_paired_group_id",
        "venue_events",
        ["paired_group_id"],
        sqlite_where=text(_PAIRED_GROUP_ID_NOT_NULL),
        postgresql_where=text(_PAIRED_GROUP_ID_NOT_NULL),
    )


def downgrade() -> None:
    """Drop the group provenance columns and the paired-execution tables."""
    op.drop_index("ix_venue_events_paired_group_id", table_name="venue_events")
    op.drop_column("venue_events", "paired_group_id")
    op.drop_index("ix_signals_paired_group_id", table_name="signals")
    op.drop_column("signals", "paired_group_id")
    op.drop_table("paired_execution_halts")
    op.drop_table("paired_execution_legs")
    op.drop_table("paired_execution_groups")
