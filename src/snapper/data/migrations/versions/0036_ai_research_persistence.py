"""Add the AI-research rounds, market views, and source citations.

The research plane is separate from the CONSULT ``ai_reviews`` state machine.
Rounds are mutable lifecycle rows with one global pending slot, while market
views and their sources are immutable submitted facts. Each view stores both
the authored ``as_of`` and server-assigned ``submitted_at`` clocks required
for causally correct replay eligibility. Revises 0035.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

revision: str = "0036"
down_revision: str | None = "0035"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _uuid_col() -> sa.types.TypeEngine[str]:
    """Build native PostgreSQL UUID with SQLite text fallback."""
    return sa.String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql")


def upgrade() -> None:
    """Create the three AI-research persistence tables."""
    op.create_table(
        "ai_research_rounds",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("trigger", sa.String(64), nullable=False),
        sa.Column("status", sa.String(24), server_default="pending", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("public_id", name="uq_ai_research_rounds_public_id"),
        sa.CheckConstraint(
            "status IN ('pending', 'completed', 'superseded', 'expired')",
            name="ck_ai_research_rounds_status_enum",
        ),
        sa.CheckConstraint(
            "(status = 'pending' AND resolved_at IS NULL) OR "
            "(status IN ('completed', 'superseded', 'expired') AND resolved_at IS NOT NULL)",
            name="ck_ai_research_rounds_status_consistency",
        ),
        sa.CheckConstraint(
            "length(trim(trigger)) > 0",
            name="ck_ai_research_rounds_trigger_nonempty",
        ),
    )
    op.create_index(
        "uq_ai_research_rounds_one_pending",
        "ai_research_rounds",
        ["status"],
        unique=True,
        sqlite_where=text("status = 'pending'"),
        postgresql_where=text("status = 'pending'"),
    )
    op.create_index(
        "ix_ai_research_rounds_created_at",
        "ai_research_rounds",
        ["created_at"],
    )

    op.create_table(
        "market_views",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("research_round_public_id", _uuid_col(), nullable=False),
        sa.Column("trigger", sa.String(64), nullable=False),
        sa.Column("status", sa.String(24), nullable=False),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "submitted_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("valid_until", sa.DateTime(timezone=True), nullable=False),
        sa.Column("regime", sa.String(16), nullable=False),
        sa.Column("bias", sa.String(20), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("horizon_hours", sa.Integer(), nullable=False),
        sa.Column("key_risks", sa.JSON(), nullable=False),
        sa.Column("next_events", sa.JSON(), nullable=False),
        sa.Column("rationale", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("public_id", name="uq_market_views_public_id"),
        sa.UniqueConstraint(
            "research_round_public_id",
            name="uq_market_views_research_round_public_id",
        ),
        sa.CheckConstraint(
            "regime IN ('risk_on', 'neutral', 'risk_off', 'event_window')",
            name="ck_market_views_regime_enum",
        ),
        sa.CheckConstraint(
            "bias IN ('longs_ok', 'neutral', 'avoid_new_longs', 'avoid_all')",
            name="ck_market_views_bias_enum",
        ),
        sa.CheckConstraint(
            "confidence >= 0 AND confidence <= 1",
            name="ck_market_views_confidence_range",
        ),
        sa.CheckConstraint(
            "length(trim(trigger)) > 0",
            name="ck_market_views_trigger_nonempty",
        ),
        sa.CheckConstraint(
            "length(trim(status)) > 0",
            name="ck_market_views_status_nonempty",
        ),
    )
    op.create_index(
        "ix_market_views_replay_eligibility",
        "market_views",
        ["submitted_at", "as_of", "valid_until"],
    )

    op.create_table(
        "market_view_sources",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("market_view_public_id", _uuid_col(), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("url", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("retrieved_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("public_id", name="uq_market_view_sources_public_id"),
        sa.UniqueConstraint(
            "market_view_public_id",
            "ordinal",
            name="uq_market_view_sources_view_ordinal",
        ),
        sa.CheckConstraint(
            "ordinal >= 0",
            name="ck_market_view_sources_ordinal_nonnegative",
        ),
    )


def downgrade() -> None:
    """Drop the AI-research persistence tables in dependency order."""
    op.drop_table("market_view_sources")
    op.drop_table("market_views")
    op.drop_table("ai_research_rounds")
