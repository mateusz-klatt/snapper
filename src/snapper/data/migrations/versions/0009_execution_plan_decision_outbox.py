"""Add durable outbox for execution-plan decision publishes.

Plan-decision ZMQ frames used to be best-effort after the
``execution_plan_decisions`` insert. This migration adds an SCD2
outbox so the decision row and publish intent commit together, then
the plan executor can retry missed ``plans.decisions.*`` frames after
broker outages. Revises 0008.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

revision: str = "0009"
down_revision: str | None = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"
_PENDING_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00' AND status = 'pending'"
_PENDING_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000' AND status = 'pending'"


def _uuid_col() -> sa.types.TypeEngine[str]:
    """Per-dialect UUID column matching ``UUIDColumn``."""
    return sa.String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql")


def upgrade() -> None:
    """Create the execution-plan decision outbox table and indexes."""
    op.create_table(
        "execution_plan_decision_outbox",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("decision_public_id", _uuid_col(), nullable=False),
        sa.Column("plan_public_id", _uuid_col(), nullable=False),
        sa.Column("topic", sa.String(256), nullable=False),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_reason", sa.String(512), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("session_id", _uuid_col(), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "status IN ('pending', 'sent', 'failed')",
            name="ck_epd_outbox_status",
        ),
    )
    op.create_index(
        "ix_epd_outbox_public_id",
        "execution_plan_decision_outbox",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_epd_outbox_decision_public_id",
        "execution_plan_decision_outbox",
        ["decision_public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_epd_outbox_ready",
        "execution_plan_decision_outbox",
        ["next_attempt_at", "created_at"],
        sqlite_where=text(_PENDING_ACTIVE_SQLITE),
        postgresql_where=text(_PENDING_ACTIVE_PG),
    )
    op.create_index(
        "ix_epd_outbox_plan_public_id",
        "execution_plan_decision_outbox",
        ["plan_public_id"],
    )


def downgrade() -> None:
    """Drop the execution-plan decision outbox table."""
    op.drop_index("ix_epd_outbox_plan_public_id", table_name="execution_plan_decision_outbox")
    op.drop_index("ix_epd_outbox_ready", table_name="execution_plan_decision_outbox")
    op.drop_index("ix_epd_outbox_decision_public_id", table_name="execution_plan_decision_outbox")
    op.drop_index("ix_epd_outbox_public_id", table_name="execution_plan_decision_outbox")
    op.drop_table("execution_plan_decision_outbox")
