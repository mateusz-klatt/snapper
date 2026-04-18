"""AI Integration Phase A — schema additions (plan §4 Day 1 #6).

Adds the persistence surface required by the MCP endpoint +
``TradingCapsEnforcer`` + kill-switch token inventory + audit
``source_surface`` column:

1. ``user_trading_caps`` — temporal table with per-user safety caps
   (max order quantity per instrument, max open orders, max daily
   USD notional, max cancels per minute). Enforced pre-insert by
   ``TradingCapsEnforcer.guard()``.

2. ``user_active_tokens`` — NON-temporal token inventory with
   ``jti`` + ``token_hash`` so ``TokenManager.revoke_user_sessions``
   can (a) push JTIs into the in-memory fast-path blacklist and
   (b) enable DB-backed ``verify_token()`` lookup by token hash.

3. ``trade_commands.source_surface`` — new VARCHAR(20) column with
   server_default ``'rest'`` so existing rows backfill cleanly.
   Populated by all 7 insert call sites per plan §3.9.

4. ``execution_plan_decisions.source_surface`` — same pattern with
   server_default ``'strategy'`` (decisions originate from the
   strategy hot path).

5. ``users.created_by_user_public_id`` — nullable FK-style column
   for the AI-delegate ownership chain (plan §3.13). Indexed so
   ``COUNT(*) WHERE created_by_user_public_id = <owner>`` is cheap
   for the per-owner delegate-proliferation cap.

See ``plan_ai_integration_phase_a.md`` §4 Day 1 #6. Default feature
flag is off — no runtime consequences until the endpoint is mounted
in Day 2.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

revision: str = "0012"
down_revision: str | None = "0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CK_SESSION_ID = "length(session_id) > 0"
_CK_SEQUENCE_ID = "sequence_id >= 0"
_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"


def upgrade() -> None:
    """Apply Phase A schema additions."""
    op.create_table(
        "user_trading_caps",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("user_public_id", sa.String(36), nullable=False),
        sa.Column("max_order_quantity_per_instrument", sa.JSON(), nullable=True),
        sa.Column("max_open_orders", sa.Integer(), nullable=True),
        sa.Column("max_daily_notional_usd", sa.Numeric(18, 2), nullable=True),
        sa.Column("max_cancels_per_minute", sa.Integer(), nullable=True),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_user_trading_caps_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_user_trading_caps_sequence_id"),
    )
    op.create_index(
        "ix_user_trading_caps_public_id",
        "user_trading_caps",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_user_trading_caps_active",
        "user_trading_caps",
        ["user_public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )

    op.create_table(
        "user_active_tokens",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("user_public_id", sa.String(36), nullable=False),
        sa.Column("jti", sa.String(64), nullable=False),
        sa.Column("token_hash", sa.String(64), nullable=False),
        sa.Column("token_type", sa.String(10), nullable=False),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("public_id", name="uq_user_active_tokens_public_id"),
        sa.UniqueConstraint("jti", name="uq_user_active_tokens_jti"),
        sa.UniqueConstraint("token_hash", name="uq_user_active_tokens_token_hash"),
    )
    op.create_index(
        "ix_user_active_tokens_user_public_id",
        "user_active_tokens",
        ["user_public_id"],
    )
    op.create_index(
        "ix_user_active_tokens_user_revoked",
        "user_active_tokens",
        ["user_public_id", "revoked_at"],
    )

    op.add_column(
        "trade_commands",
        sa.Column(
            "source_surface",
            sa.String(20),
            nullable=False,
            server_default="rest",
        ),
    )
    op.add_column(
        "execution_plan_decisions",
        sa.Column(
            "source_surface",
            sa.String(20),
            nullable=False,
            server_default="strategy",
        ),
    )
    op.add_column(
        "users",
        sa.Column("created_by_user_public_id", sa.String(36), nullable=True),
    )
    op.create_index(
        "ix_users_created_by_user_public_id",
        "users",
        ["created_by_user_public_id"],
    )


def downgrade() -> None:
    """Roll back Phase A schema additions."""
    op.drop_index("ix_users_created_by_user_public_id", table_name="users")
    op.drop_column("users", "created_by_user_public_id")
    op.drop_column("execution_plan_decisions", "source_surface")
    op.drop_column("trade_commands", "source_surface")

    op.drop_index("ix_user_active_tokens_user_revoked", table_name="user_active_tokens")
    op.drop_index("ix_user_active_tokens_user_public_id", table_name="user_active_tokens")
    op.drop_table("user_active_tokens")

    op.drop_index("ix_user_trading_caps_active", table_name="user_trading_caps")
    op.drop_index("ix_user_trading_caps_public_id", table_name="user_trading_caps")
    op.drop_table("user_trading_caps")
