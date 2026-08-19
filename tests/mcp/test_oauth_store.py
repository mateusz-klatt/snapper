"""Integration tests for durable MCP OAuth protocol state."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import select

from snapper.auth.tokens import TokenManager
from snapper.auth.tokens import hash_token
from snapper.data.models import OAuthAuthorizationCode
from snapper.data.models import OAuthAuthorizationRequest
from snapper.data.models import OAuthClient
from snapper.data.models import OAuthGrant
from snapper.data.models import OAuthRefreshToken
from snapper.data.repository import SQLAlchemyRepository
from snapper.mcp.oauth.store import MCPOAuthStore
from snapper.mcp.oauth.store import OAuthRefreshRotationOutcome

_CLIENT_ID = "chatgpt-client"
_GRANT_ID = "00000000-0000-7000-8000-000000000101"
_FAMILY_ID = "00000000-0000-7000-8000-000000000102"


async def _store(tmp_path: Path) -> tuple[SQLAlchemyRepository, MCPOAuthStore]:
    """Create one isolated repository and OAuth store."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'oauth-store.db'}")
    await repository.create_all()
    return repository, MCPOAuthStore(repository)


def _client(now: datetime) -> OAuthClient:
    """Build one pre-registered active client row."""
    return OAuthClient(
        public_id="00000000-0000-7000-8000-000000000100",
        client_id=_CLIENT_ID,
        client_secret_hash="hashed-secret",
        client_name="ChatGPT",
        redirect_uris=["https://chatgpt.com/connector/oauth/callback"],
        token_endpoint_auth_method="client_secret_basic",
        allowed_scopes=["snapper.read", "offline_access"],
        is_active=True,
        created_at=now,
    )


def _grant(now: datetime) -> OAuthGrant:
    """Build one active read-only OAuth grant row."""
    return OAuthGrant(
        public_id=_GRANT_ID,
        owner_user_public_id="00000000-0000-7000-8000-000000000110",
        delegate_user_public_id="00000000-0000-7000-8000-000000000111",
        client_id=_CLIENT_ID,
        resource="https://snapper.ch/api/mcp",
        scopes=["snapper.read", "offline_access"],
        operator_public_id="00000000-0000-7000-8000-000000000112",
        created_at=now,
    )


def _refresh_row(
    raw_token: str,
    *,
    public_id: str,
    now: datetime,
    scopes: list[str] | None = None,
) -> OAuthRefreshToken:
    """Build one persisted refresh-token hash in the shared family."""
    return OAuthRefreshToken(
        public_id=public_id,
        token_hash=hash_token(raw_token),
        family_public_id=_FAMILY_ID,
        grant_public_id=_GRANT_ID,
        scopes=scopes or ["snapper.read", "offline_access"],
        created_at=now,
        expires_at=now + timedelta(days=90),
    )


@pytest.mark.asyncio
async def test_authorization_code_is_hashed_expiring_and_one_time(tmp_path: Path) -> None:
    """Verify only the correct client can consume a live code once.

    Given a hashed live authorization code bound to one active client grant,
    When a wrong client and then the correct client attempt exchanges twice,
    Then only the correct client's first exchange consumes the credential.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    raw_code = TokenManager.mint_oauth_refresh_token().token
    await store.add_client(_client(now))
    await store.add_grant(_grant(now))
    await store.add_authorization_code(
        OAuthAuthorizationCode(
            public_id="00000000-0000-7000-8000-000000000120",
            code_hash=hash_token(raw_code),
            grant_public_id=_GRANT_ID,
            redirect_uri="https://chatgpt.com/connector/oauth/callback",
            code_challenge="challenge",
            code_challenge_method="S256",
            redirect_uri_provided_explicitly=True,
            scopes=["snapper.read", "offline_access"],
            created_at=now,
            expires_at=now + timedelta(minutes=2),
        )
    )

    assert (
        await store.consume_authorization_code(
            raw_code,
            client_id="wrong-client",
            consumed_at=now,
        )
        is None
    )
    consumed = await store.consume_authorization_code(
        raw_code,
        client_id=_CLIENT_ID,
        consumed_at=now,
    )
    assert consumed is not None
    assert consumed.code_hash == hash_token(raw_code)
    assert consumed.code_hash != raw_code
    assert (
        await store.consume_authorization_code(
            raw_code,
            client_id=_CLIENT_ID,
            consumed_at=now,
        )
        is None
    )
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_pending_authorization_request_resolves_once_with_hashed_ids(
    tmp_path: Path,
) -> None:
    """Verify consent request and code remain opaque and one-time.

    Given one active client and a pending hashed browser request,
    When consent approves it with a separately hashed authorization code,
    Then the request resolves once, stores no raw IDs, and cannot be denied later.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    raw_request_id = TokenManager.mint_oauth_refresh_token().token
    raw_code = TokenManager.mint_oauth_refresh_token().token
    await store.add_client(_client(now))
    await store.add_grant(_grant(now))
    await store.add_authorization_request(
        OAuthAuthorizationRequest(
            public_id="00000000-0000-7000-8000-000000000121",
            request_hash=hash_token(raw_request_id),
            client_id=_CLIENT_ID,
            redirect_uri="https://chatgpt.com/connector/oauth/callback",
            redirect_uri_provided_explicitly=True,
            state="client-state",
            scopes=["snapper.read", "offline_access"],
            code_challenge="challenge",
            resource="https://snapper.ch/api/mcp",
            created_at=now,
            expires_at=now + timedelta(minutes=5),
        )
    )
    pending = await store.get_pending_authorization_request(raw_request_id, now=now)
    assert pending is not None
    assert pending.request_hash == hash_token(raw_request_id)
    approved = await store.approve_authorization_request(
        raw_request_id,
        code=OAuthAuthorizationCode(
            public_id="00000000-0000-7000-8000-000000000122",
            code_hash=hash_token(raw_code),
            grant_public_id=_GRANT_ID,
            redirect_uri=pending.redirect_uri,
            code_challenge=pending.code_challenge,
            code_challenge_method="S256",
            redirect_uri_provided_explicitly=True,
            scopes=pending.scopes,
            created_at=now,
            expires_at=now + timedelta(minutes=2),
        ),
        resolved_at=now,
    )
    assert approved is not None
    assert approved.decision == "approved"
    assert await store.get_pending_authorization_request(raw_request_id, now=now) is None
    assert await store.deny_authorization_request(raw_request_id, resolved_at=now) is None
    consumed = await store.consume_authorization_code(
        raw_code,
        client_id=_CLIENT_ID,
        consumed_at=now,
    )
    assert consumed is not None
    assert consumed.code_hash != raw_code
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_refresh_rotation_reuse_revokes_entire_family(tmp_path: Path) -> None:
    """Verify first rotation succeeds and predecessor reuse burns the family.

    Given one active opaque refresh token and a narrower successor,
    When the predecessor is rotated and then presented again,
    Then the first rotation succeeds and replay revokes every family member.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    predecessor = TokenManager.mint_oauth_refresh_token()
    successor = TokenManager.mint_oauth_refresh_token()
    await store.add_client(_client(now))
    await store.add_grant(_grant(now))
    await store.add_refresh_token(
        _refresh_row(
            predecessor.token,
            public_id="00000000-0000-7000-8000-000000000130",
            now=now,
        )
    )
    successor_row = _refresh_row(
        successor.token,
        public_id="00000000-0000-7000-8000-000000000131",
        now=now,
        scopes=["snapper.read"],
    )

    outcome = await store.rotate_refresh_token(
        predecessor.token,
        client_id=_CLIENT_ID,
        successor=successor_row,
        rotated_at=now + timedelta(seconds=1),
    )
    assert outcome is OAuthRefreshRotationOutcome.ROTATED
    assert (
        await store.load_refresh_token(
            successor.token,
            client_id=_CLIENT_ID,
            now=now + timedelta(seconds=2),
        )
        is not None
    )

    replay = await store.rotate_refresh_token(
        predecessor.token,
        client_id=_CLIENT_ID,
        successor=_refresh_row(
            "unused-successor",
            public_id="00000000-0000-7000-8000-000000000132",
            now=now,
        ),
        rotated_at=now + timedelta(seconds=3),
    )
    assert replay is OAuthRefreshRotationOutcome.REUSE_DETECTED
    assert (
        await store.load_refresh_token(
            successor.token,
            client_id=_CLIENT_ID,
            now=now + timedelta(seconds=4),
        )
        is None
    )
    async with repository.session() as session:
        rows = (
            await session.execute(
                select(OAuthRefreshToken).where(OAuthRefreshToken.family_public_id == _FAMILY_ID)
            )
        ).scalars()
        assert all(row.revoked_at is not None for row in rows)
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_refresh_rotation_rejects_invalid_or_widening_successor(tmp_path: Path) -> None:
    """Verify wrong clients, expired tokens, and scope widening fail closed.

    Given expired and live refresh tokens bound to one client and scope,
    When the wrong client, expired credential, or widening successor is used,
    Then no rotation occurs and widening raises before persistence.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    raw = TokenManager.mint_oauth_refresh_token().token
    await store.add_client(_client(now))
    await store.add_grant(_grant(now))
    row = _refresh_row(
        raw,
        public_id="00000000-0000-7000-8000-000000000140",
        now=now - timedelta(days=91),
    )
    row.expires_at = now - timedelta(days=1)
    await store.add_refresh_token(row)
    successor = _refresh_row(
        "successor",
        public_id="00000000-0000-7000-8000-000000000141",
        now=now,
    )
    assert (
        await store.rotate_refresh_token(
            raw,
            client_id="wrong-client",
            successor=successor,
            rotated_at=now,
        )
        is OAuthRefreshRotationOutcome.INVALID
    )
    assert (
        await store.rotate_refresh_token(
            raw,
            client_id=_CLIENT_ID,
            successor=successor,
            rotated_at=now,
        )
        is OAuthRefreshRotationOutcome.INVALID
    )

    live_raw = TokenManager.mint_oauth_refresh_token().token
    await store.add_refresh_token(
        _refresh_row(
            live_raw,
            public_id="00000000-0000-7000-8000-000000000142",
            now=now,
            scopes=["snapper.read"],
        )
    )
    widening = _refresh_row(
        "widening",
        public_id="00000000-0000-7000-8000-000000000143",
        now=now,
    )
    with pytest.raises(ValueError, match="family or scope"):
        await store.rotate_refresh_token(
            live_raw,
            client_id=_CLIENT_ID,
            successor=widening,
            rotated_at=now,
        )
    await repository.engine.dispose()
