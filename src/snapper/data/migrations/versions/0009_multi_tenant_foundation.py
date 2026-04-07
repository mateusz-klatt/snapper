"""Multi-tenant foundation: wallets, operators, scope grants.

Plan 0 / Phase 0a (multi-tenant foundation, step 1 of N).

Adds the 5 new tables that form the multi-tenant 3-entity model:

- ``wallets`` — logical container for credentials and positions on one or
  more exchanges. Multiple operators may share one wallet (trading desk).
- ``wallet_credentials`` — per-exchange encrypted credential for a wallet.
  Reuses Snapper's existing setting-encryption infrastructure. Pull-on-startup
  only, never broadcast on the system.settings ZMQ topic.
- ``operators`` — trading identity, distinct from User (login identity).
- ``user_operator_memberships`` — M:N which users may act AS which operators.
  At most one primary operator per user.
- ``wallet_operator_scope_grants`` — M:N which operators may trade which
  scopes on which wallets. All grants are instrument-exclusive per Plan 0 D2:
  at most one operator per (wallet, instrument) tuple at any time. The CHECK
  constraint enforces scope_kind XOR (exactly one of underlying_public_id /
  instrument_public_id is non-NULL).

This migration only adds the 5 new tables. The follow-up migration will add
``wallet_public_id`` / ``operator_public_id`` columns to the existing
write-path tables (positions, orders, executions, signals, trade_commands,
venue_events, trade_projection_checkpoints, process_runs, accrual_ledger),
bump shard_key column lengths from String(64) to String(256), and extend the
accrual_ledger unique index to include wallet_public_id.

Plan and rationale: ``proprietary/plans/plan_multi_tenant_foundation.md``
Sections 3.1, 14.6, 14.7, 14.8.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

revision: str = "0009"
down_revision: str = "0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"
_CK_EXCHANGE_LOWER = "exchange = LOWER(exchange)"
_CK_SESSION_ID = "session_id != ''"
_CK_SEQUENCE_ID = "sequence_id > 0"


def upgrade() -> None:
    """Create the 5 new multi-tenant tables and their indexes."""
    op.create_table(
        "wallets",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("label", sa.String(128), nullable=False),
        sa.Column("description", sa.String(512), nullable=True),
        sa.Column(
            "is_paper",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_wallets_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_wallets_sequence_id"),
    )
    op.create_index(
        "ix_wallets_public_id",
        "wallets",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_wallets_label_active",
        "wallets",
        ["label"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )

    op.create_table(
        "wallet_credentials",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("wallet_public_id", sa.String(36), nullable=False),
        sa.Column("exchange", sa.String(20), nullable=False),
        sa.Column("credential_type", sa.String(32), nullable=False),
        sa.Column("encrypted_payload", sa.Text(), nullable=False),
        sa.Column("encryption_key_id", sa.String(64), nullable=False),
        sa.Column("label", sa.String(128), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_wallet_credentials_exchange_lower"),
        sa.CheckConstraint(
            "credential_type IN ('api_key_secret', 'rsa_pem', 'oauth', 'paper')",
            name="ck_wallet_credentials_type",
        ),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_wallet_credentials_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_wallet_credentials_sequence_id"),
    )
    op.create_index(
        "ix_wallet_credentials_public_id",
        "wallet_credentials",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_wallet_credentials_wallet_exchange_active",
        "wallet_credentials",
        ["wallet_public_id", "exchange"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_wallet_credentials_exchange",
        "wallet_credentials",
        ["exchange"],
    )

    op.create_table(
        "operators",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("label", sa.String(128), nullable=False),
        sa.Column("description", sa.String(512), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_operators_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_operators_sequence_id"),
    )
    op.create_index(
        "ix_operators_public_id",
        "operators",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_operators_label_active",
        "operators",
        ["label"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )

    op.create_table(
        "user_operator_memberships",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("user_public_id", sa.String(36), nullable=False),
        sa.Column("operator_public_id", sa.String(36), nullable=False),
        sa.Column(
            "is_primary",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_user_operator_memberships_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_user_operator_memberships_sequence_id"),
    )
    op.create_index(
        "ix_user_operator_memberships_public_id",
        "user_operator_memberships",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_user_operator_memberships_unique_active",
        "user_operator_memberships",
        ["user_public_id", "operator_public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_user_operator_memberships_primary_unique_active",
        "user_operator_memberships",
        ["user_public_id"],
        unique=True,
        sqlite_where=text("is_primary = 1 AND known_to = '9999-12-31 23:59:59.000000'"),
        postgresql_where=text("is_primary = TRUE AND known_to = '9999-12-31T23:59:59+00:00'"),
    )
    op.create_index(
        "ix_user_operator_memberships_user",
        "user_operator_memberships",
        ["user_public_id"],
    )
    op.create_index(
        "ix_user_operator_memberships_operator",
        "user_operator_memberships",
        ["operator_public_id"],
    )

    op.create_table(
        "wallet_operator_scope_grants",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("operator_public_id", sa.String(36), nullable=False),
        sa.Column("wallet_public_id", sa.String(36), nullable=False),
        sa.Column("granted_by_user_public_id", sa.String(36), nullable=False),
        sa.Column("scope_kind", sa.String(16), nullable=False),
        sa.Column("underlying_public_id", sa.String(36), nullable=True),
        sa.Column("instrument_public_id", sa.String(36), nullable=True),
        sa.Column("note", sa.String(512), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "scope_kind IN ('underlying', 'instrument')",
            name="ck_scope_grants_scope_kind",
        ),
        sa.CheckConstraint(
            "(scope_kind = 'underlying' AND underlying_public_id IS NOT NULL "
            "AND instrument_public_id IS NULL) "
            "OR (scope_kind = 'instrument' AND instrument_public_id IS NOT NULL "
            "AND underlying_public_id IS NULL)",
            name="ck_scope_grants_scope_kind_xor",
        ),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_scope_grants_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_scope_grants_sequence_id"),
    )
    op.create_index(
        "ix_scope_grants_public_id",
        "wallet_operator_scope_grants",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_scope_grants_instrument_exclusive_active",
        "wallet_operator_scope_grants",
        ["wallet_public_id", "instrument_public_id"],
        unique=True,
        sqlite_where=text(
            "instrument_public_id IS NOT NULL AND known_to = '9999-12-31 23:59:59.000000'"
        ),
        postgresql_where=text(
            "instrument_public_id IS NOT NULL AND known_to = '9999-12-31T23:59:59+00:00'"
        ),
    )
    op.create_index(
        "ix_scope_grants_underlying_exclusive_active",
        "wallet_operator_scope_grants",
        ["wallet_public_id", "underlying_public_id"],
        unique=True,
        sqlite_where=text(
            "underlying_public_id IS NOT NULL AND known_to = '9999-12-31 23:59:59.000000'"
        ),
        postgresql_where=text(
            "underlying_public_id IS NOT NULL AND known_to = '9999-12-31T23:59:59+00:00'"
        ),
    )
    op.create_index(
        "ix_scope_grants_operator",
        "wallet_operator_scope_grants",
        ["operator_public_id"],
    )
    op.create_index(
        "ix_scope_grants_wallet",
        "wallet_operator_scope_grants",
        ["wallet_public_id"],
    )


def downgrade() -> None:
    """Drop the 5 new multi-tenant tables and their indexes."""
    op.drop_index("ix_scope_grants_wallet", table_name="wallet_operator_scope_grants")
    op.drop_index("ix_scope_grants_operator", table_name="wallet_operator_scope_grants")
    op.drop_index(
        "ix_scope_grants_underlying_exclusive_active",
        table_name="wallet_operator_scope_grants",
    )
    op.drop_index(
        "ix_scope_grants_instrument_exclusive_active",
        table_name="wallet_operator_scope_grants",
    )
    op.drop_index("ix_scope_grants_public_id", table_name="wallet_operator_scope_grants")
    op.drop_table("wallet_operator_scope_grants")

    op.drop_index("ix_user_operator_memberships_operator", table_name="user_operator_memberships")
    op.drop_index("ix_user_operator_memberships_user", table_name="user_operator_memberships")
    op.drop_index(
        "ix_user_operator_memberships_primary_unique_active",
        table_name="user_operator_memberships",
    )
    op.drop_index(
        "ix_user_operator_memberships_unique_active",
        table_name="user_operator_memberships",
    )
    op.drop_index("ix_user_operator_memberships_public_id", table_name="user_operator_memberships")
    op.drop_table("user_operator_memberships")

    op.drop_index("ix_operators_label_active", table_name="operators")
    op.drop_index("ix_operators_public_id", table_name="operators")
    op.drop_table("operators")

    op.drop_index("ix_wallet_credentials_exchange", table_name="wallet_credentials")
    op.drop_index("ix_wallet_credentials_wallet_exchange_active", table_name="wallet_credentials")
    op.drop_index("ix_wallet_credentials_public_id", table_name="wallet_credentials")
    op.drop_table("wallet_credentials")

    op.drop_index("ix_wallets_label_active", table_name="wallets")
    op.drop_index("ix_wallets_public_id", table_name="wallets")
    op.drop_table("wallets")
