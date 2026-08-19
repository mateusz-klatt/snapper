"""Add persistence for the MCP OAuth authorization-code flow.

The four tables keep client registration, grants, one-time authorization
codes, and rotating opaque refresh tokens separate from the legacy JWT token
inventory. Codes and refresh credentials are stored only as SHA-256 hashes.
Lifecycle rows are deliberately non-temporal: their explicit consumed, used,
replaced, and revoked timestamps are the protocol audit trail. Revises 0051.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql

revision: str = "0052"
down_revision: str | None = "0051"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _uuid_col() -> sa.types.TypeEngine[str]:
    """Build native PostgreSQL UUID with SQLite text fallback.

    Returns:
        Dialect-aware UUID column type.
    """
    return sa.String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql")


def upgrade() -> None:
    """Create the MCP OAuth persistence tables and indexes.

    Returns:
        None.
    """
    op.create_table(
        "oauth_clients",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("client_id", sa.String(128), nullable=False),
        sa.Column("client_secret_hash", sa.String(255), nullable=False),
        sa.Column("client_name", sa.String(200), nullable=False),
        sa.Column("redirect_uris", sa.JSON(), nullable=False),
        sa.Column("token_endpoint_auth_method", sa.String(32), nullable=False),
        sa.Column("allowed_scopes", sa.JSON(), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default="true", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("public_id", name="uq_oauth_clients_public_id"),
        sa.UniqueConstraint("client_id", name="uq_oauth_clients_client_id"),
        sa.CheckConstraint(
            "token_endpoint_auth_method IN ('client_secret_basic', 'client_secret_post')",
            name="ck_oauth_clients_token_auth_method",
        ),
    )
    op.create_index("ix_oauth_clients_active", "oauth_clients", ["is_active"])

    op.create_table(
        "oauth_grants",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("owner_user_public_id", _uuid_col(), nullable=False),
        sa.Column("delegate_user_public_id", _uuid_col(), nullable=False),
        sa.Column("client_id", sa.String(128), nullable=False),
        sa.Column("resource", sa.String(2048), nullable=False),
        sa.Column("scopes", sa.JSON(), nullable=False),
        sa.Column("operator_public_id", _uuid_col(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("public_id", name="uq_oauth_grants_public_id"),
        sa.CheckConstraint(
            "length(trim(resource)) > 0",
            name="ck_oauth_grants_resource_nonempty",
        ),
    )
    op.create_index(
        "uq_oauth_grants_active_binding",
        "oauth_grants",
        ["owner_user_public_id", "operator_public_id", "client_id"],
        unique=True,
        sqlite_where=text("revoked_at IS NULL"),
        postgresql_where=text("revoked_at IS NULL"),
    )
    op.create_index(
        "ix_oauth_grants_delegate",
        "oauth_grants",
        ["delegate_user_public_id"],
    )
    op.create_index("ix_oauth_grants_client", "oauth_grants", ["client_id"])

    op.create_table(
        "oauth_authorization_requests",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("client_id", sa.String(128), nullable=False),
        sa.Column("redirect_uri", sa.Text(), nullable=False),
        sa.Column("redirect_uri_provided_explicitly", sa.Boolean(), nullable=False),
        sa.Column("state", sa.Text(), nullable=True),
        sa.Column("scopes", sa.JSON(), nullable=False),
        sa.Column("code_challenge", sa.String(128), nullable=False),
        sa.Column("resource", sa.String(2048), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("decision", sa.String(8), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("public_id", name="uq_oauth_authorization_requests_public_id"),
        sa.UniqueConstraint("request_hash", name="uq_oauth_authorization_requests_hash"),
        sa.CheckConstraint(
            "length(request_hash) = 64",
            name="ck_oauth_authorization_requests_hash_length",
        ),
        sa.CheckConstraint(
            "decision IS NULL OR decision IN ('approved', 'denied')",
            name="ck_oauth_authorization_requests_decision",
        ),
        sa.CheckConstraint(
            "(decision IS NULL AND resolved_at IS NULL) OR "
            "(decision IS NOT NULL AND resolved_at IS NOT NULL)",
            name="ck_oauth_authorization_requests_resolution",
        ),
    )
    op.create_index(
        "ix_oauth_authorization_requests_expires",
        "oauth_authorization_requests",
        ["expires_at"],
    )
    op.create_index(
        "ix_oauth_authorization_requests_client",
        "oauth_authorization_requests",
        ["client_id"],
    )

    op.create_table(
        "oauth_authorization_codes",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("code_hash", sa.String(64), nullable=False),
        sa.Column("grant_public_id", _uuid_col(), nullable=False),
        sa.Column("redirect_uri", sa.Text(), nullable=False),
        sa.Column("code_challenge", sa.String(128), nullable=False),
        sa.Column("code_challenge_method", sa.String(8), nullable=False),
        sa.Column("redirect_uri_provided_explicitly", sa.Boolean(), nullable=False),
        sa.Column("scopes", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("public_id", name="uq_oauth_authorization_codes_public_id"),
        sa.UniqueConstraint("code_hash", name="uq_oauth_authorization_codes_code_hash"),
        sa.CheckConstraint(
            "length(code_hash) = 64",
            name="ck_oauth_authorization_codes_hash_length",
        ),
        sa.CheckConstraint(
            "code_challenge_method = 'S256'",
            name="ck_oauth_authorization_codes_pkce_method",
        ),
    )
    op.create_index(
        "ix_oauth_authorization_codes_grant",
        "oauth_authorization_codes",
        ["grant_public_id"],
    )
    op.create_index(
        "ix_oauth_authorization_codes_expires",
        "oauth_authorization_codes",
        ["expires_at"],
    )

    op.create_table(
        "oauth_refresh_tokens",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", _uuid_col(), nullable=False),
        sa.Column("token_hash", sa.String(64), nullable=False),
        sa.Column("family_public_id", _uuid_col(), nullable=False),
        sa.Column("grant_public_id", _uuid_col(), nullable=False),
        sa.Column("scopes", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("replaced_by_public_id", _uuid_col(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("public_id", name="uq_oauth_refresh_tokens_public_id"),
        sa.UniqueConstraint("token_hash", name="uq_oauth_refresh_tokens_token_hash"),
        sa.CheckConstraint(
            "length(token_hash) = 64",
            name="ck_oauth_refresh_tokens_hash_length",
        ),
        sa.CheckConstraint(
            "replaced_by_public_id IS NULL OR used_at IS NOT NULL",
            name="ck_oauth_refresh_tokens_replacement_used",
        ),
    )
    op.create_index(
        "ix_oauth_refresh_tokens_family",
        "oauth_refresh_tokens",
        ["family_public_id"],
    )
    op.create_index(
        "ix_oauth_refresh_tokens_grant",
        "oauth_refresh_tokens",
        ["grant_public_id"],
    )
    op.create_index(
        "ix_oauth_refresh_tokens_expiry",
        "oauth_refresh_tokens",
        ["expires_at", "revoked_at"],
    )


def downgrade() -> None:
    """Drop the MCP OAuth persistence tables in dependency order.

    Returns:
        None.
    """
    op.drop_table("oauth_refresh_tokens")
    op.drop_table("oauth_authorization_codes")
    op.drop_table("oauth_authorization_requests")
    op.drop_table("oauth_grants")
    op.drop_table("oauth_clients")
