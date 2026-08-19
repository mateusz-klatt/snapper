"""Integration tests for durable MCP OAuth protocol state."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

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


def _code_row(
    raw_code: str,
    *,
    public_id: str,
    now: datetime,
    expires_at: datetime | None = None,
) -> OAuthAuthorizationCode:
    """Build one hashed authorization-code row bound to the shared grant."""
    return OAuthAuthorizationCode(
        public_id=public_id,
        code_hash=hash_token(raw_code),
        grant_public_id=_GRANT_ID,
        redirect_uri="https://chatgpt.com/connector/oauth/callback",
        code_challenge="challenge",
        code_challenge_method="S256",
        redirect_uri_provided_explicitly=True,
        scopes=["snapper.read", "offline_access"],
        created_at=now,
        expires_at=expires_at or now + timedelta(minutes=2),
    )


def _request_row(
    raw_request_id: str,
    *,
    public_id: str,
    now: datetime,
    client_id: str = _CLIENT_ID,
    expires_at: datetime | None = None,
) -> OAuthAuthorizationRequest:
    """Build one hashed pending browser authorization request."""
    return OAuthAuthorizationRequest(
        public_id=public_id,
        request_hash=hash_token(raw_request_id),
        client_id=client_id,
        redirect_uri="https://chatgpt.com/connector/oauth/callback",
        redirect_uri_provided_explicitly=True,
        state="client-state",
        scopes=["snapper.read", "offline_access"],
        code_challenge="challenge",
        resource="https://snapper.ch/api/mcp",
        created_at=now,
        expires_at=expires_at or now + timedelta(minutes=5),
    )


def _execute_result(*, scalar: object | None = None, rowcount: int | None = 1) -> MagicMock:
    """Build one SQLAlchemy execute result with a scalar row and rowcount."""
    result = MagicMock()
    result.scalar_one_or_none.return_value = scalar
    result.rowcount = rowcount
    return result


def _async_session(*execute_results: MagicMock) -> AsyncMock:
    """Build one async session whose execute() returns the given results."""
    session = AsyncMock(add=MagicMock())
    session.execute = AsyncMock(side_effect=list(execute_results))
    return session


def _store_with_sessions(*sessions: AsyncMock) -> MCPOAuthStore:
    """Build a store that yields the provided sessions in order."""
    repository = MagicMock()
    queue = list(sessions)

    @asynccontextmanager
    async def _session() -> AsyncIterator[AsyncMock]:
        yield queue.pop(0)

    repository.session = _session
    return MCPOAuthStore(repository)


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


@pytest.mark.asyncio
async def test_get_client_returns_only_active_rows(tmp_path: Path) -> None:
    """Verify client lookup refuses inactive and unknown identifiers.

    Given one active and one deactivated pre-registered client,
    When each identifier and a missing identifier are loaded,
    Then only the active client row is returned.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    await store.add_client(_client(now))
    inactive = _client(now)
    inactive.public_id = "00000000-0000-7000-8000-000000000150"
    inactive.client_id = "chatgpt-inactive"
    inactive.is_active = False
    await store.add_client(inactive)

    loaded = await store.get_client(_CLIENT_ID)
    assert loaded is not None
    assert loaded.client_id == _CLIENT_ID
    assert await store.get_client("chatgpt-inactive") is None
    assert await store.get_client("missing-client") is None
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_get_active_grant_hides_revoked_identity(tmp_path: Path) -> None:
    """Verify grant lookup returns only the live unrevoked row.

    Given one persisted grant that is later revoked,
    When the public identity is loaded before and after revocation,
    Then only the unrevoked grant is visible.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    await store.add_client(_client(now))
    await store.add_grant(_grant(now))

    loaded = await store.get_active_grant(_GRANT_ID)
    assert loaded is not None
    assert loaded.public_id == _GRANT_ID
    assert await store.revoke_grant(_GRANT_ID, now + timedelta(seconds=1)) is True
    assert await store.get_active_grant(_GRANT_ID) is None
    assert await store.get_active_grant("00000000-0000-7000-8000-000000000151") is None
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_load_authorization_code_exposes_expiry_not_consumption(
    tmp_path: Path,
) -> None:
    """Verify load keeps expired codes visible and hides spent or foreign ones.

    Given an expired unconsumed code and a live code bound to one grant,
    When load and consume run for matching, expired, consumed, and foreign clients,
    Then expiry remains visible, consume refuses the expired credential, and
    consumed or foreign identities return None.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    expired_raw = TokenManager.mint_oauth_refresh_token().token
    live_raw = TokenManager.mint_oauth_refresh_token().token
    refresh_raw = TokenManager.mint_oauth_refresh_token().token
    await store.add_client(_client(now))
    await store.add_grant(_grant(now))
    await store.add_authorization_code(
        _code_row(
            expired_raw,
            public_id="00000000-0000-7000-8000-000000000152",
            now=now,
            expires_at=now - timedelta(seconds=1),
        )
    )
    await store.add_authorization_code(
        _code_row(
            live_raw,
            public_id="00000000-0000-7000-8000-000000000153",
            now=now,
        )
    )

    expired = await store.load_authorization_code(expired_raw, client_id=_CLIENT_ID)
    assert expired is not None
    assert expired[0].expires_at <= now
    assert expired[1].public_id == _GRANT_ID
    assert (
        await store.consume_authorization_code(
            expired_raw,
            client_id=_CLIENT_ID,
            consumed_at=now,
        )
        is None
    )
    assert await store.load_authorization_code(live_raw, client_id="wrong-client") is None
    live = await store.load_authorization_code(live_raw, client_id=_CLIENT_ID)
    assert live is not None
    consumed = await store.consume_authorization_code(
        live_raw,
        client_id=_CLIENT_ID,
        consumed_at=now,
        refresh_token=_refresh_row(
            refresh_raw,
            public_id="00000000-0000-7000-8000-000000000154",
            now=now,
        ),
    )
    assert consumed is not None
    assert await store.load_authorization_code(live_raw, client_id=_CLIENT_ID) is None
    assert await store.load_refresh_token(refresh_raw, client_id=_CLIENT_ID, now=now) is not None
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_pending_request_hidden_when_expired_or_client_inactive(
    tmp_path: Path,
) -> None:
    """Verify pending lookup requires a live request and an active client.

    Given an expired request for an active client and a live request for a
    deactivated client,
    When pending lookup, approve, and deny run,
    Then every resolution path returns None.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    expired_raw = TokenManager.mint_oauth_refresh_token().token
    inactive_raw = TokenManager.mint_oauth_refresh_token().token
    await store.add_client(_client(now))
    inactive = _client(now)
    inactive.public_id = "00000000-0000-7000-8000-000000000155"
    inactive.client_id = "chatgpt-inactive"
    inactive.is_active = False
    await store.add_client(inactive)
    await store.add_grant(_grant(now))
    await store.add_authorization_request(
        _request_row(
            expired_raw,
            public_id="00000000-0000-7000-8000-000000000156",
            now=now,
            expires_at=now - timedelta(seconds=1),
        )
    )
    await store.add_authorization_request(
        _request_row(
            inactive_raw,
            public_id="00000000-0000-7000-8000-000000000157",
            now=now,
            client_id="chatgpt-inactive",
        )
    )

    assert await store.get_pending_authorization_request(expired_raw, now=now) is None
    assert await store.get_pending_authorization_request(inactive_raw, now=now) is None
    assert (
        await store.approve_authorization_request(
            expired_raw,
            code=_code_row(
                TokenManager.mint_oauth_refresh_token().token,
                public_id="00000000-0000-7000-8000-000000000158",
                now=now,
            ),
            resolved_at=now,
        )
        is None
    )
    assert await store.deny_authorization_request(expired_raw, resolved_at=now) is None
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_deny_authorization_request_resolves_once(tmp_path: Path) -> None:
    """Verify a live pending request can be denied exactly once.

    Given one hashed pending browser request for an active client,
    When deny runs twice,
    Then the first call persists the denial and the second call is a no-op.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    raw_request_id = TokenManager.mint_oauth_refresh_token().token
    await store.add_client(_client(now))
    await store.add_grant(_grant(now))
    await store.add_authorization_request(
        _request_row(
            raw_request_id,
            public_id="00000000-0000-7000-8000-000000000159",
            now=now,
        )
    )

    denied = await store.deny_authorization_request(raw_request_id, resolved_at=now)
    assert denied is not None
    assert denied.decision == "denied"
    assert denied.resolved_at == now
    assert await store.get_pending_authorization_request(raw_request_id, now=now) is None
    assert await store.deny_authorization_request(raw_request_id, resolved_at=now) is None
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_revoke_refresh_family_and_grant_are_idempotent(tmp_path: Path) -> None:
    """Verify family and grant revocation burn live tokens and then no-op.

    Given one live refresh token bound to an active grant,
    When the family is revoked and the grant is revoked twice,
    Then the first family revoke burns the token, grant lookup dies on the
    first grant revoke, and the second grant revoke reports no transition.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    raw = TokenManager.mint_oauth_refresh_token().token
    await store.add_client(_client(now))
    await store.add_grant(_grant(now))
    await store.add_refresh_token(
        _refresh_row(
            raw,
            public_id="00000000-0000-7000-8000-000000000160",
            now=now,
        )
    )

    assert await store.revoke_refresh_family(_FAMILY_ID, now) == 1
    assert await store.load_refresh_token(raw, client_id=_CLIENT_ID, now=now) is None
    assert await store.revoke_refresh_family(_FAMILY_ID, now + timedelta(seconds=1)) == 0
    assert await store.get_active_grant(_GRANT_ID) is not None
    assert await store.revoke_grant(_GRANT_ID, now + timedelta(seconds=2)) is True
    leftover = TokenManager.mint_oauth_refresh_token().token
    await store.add_refresh_token(
        _refresh_row(
            leftover,
            public_id="00000000-0000-7000-8000-000000000161",
            now=now,
        )
    )
    assert await store.revoke_grant(_GRANT_ID, now + timedelta(seconds=3)) is False
    async with repository.session() as session:
        leftover_row = (
            await session.execute(
                select(OAuthRefreshToken).where(
                    OAuthRefreshToken.public_id == "00000000-0000-7000-8000-000000000161"
                )
            )
        ).scalar_one()
        assert leftover_row.revoked_at is not None
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_load_refresh_token_rejects_expired_revoked_and_unknown(
    tmp_path: Path,
) -> None:
    """Verify refresh load refuses expired, grant-revoked, and missing tokens.

    Given an expired token, a live token whose grant is later revoked, and
    an unknown credential,
    When each is loaded,
    Then only a live unrevoked token bound to an active grant would succeed,
    and every fixture here returns None.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    expired_raw = TokenManager.mint_oauth_refresh_token().token
    live_raw = TokenManager.mint_oauth_refresh_token().token
    await store.add_client(_client(now))
    await store.add_grant(_grant(now))
    expired = _refresh_row(
        expired_raw,
        public_id="00000000-0000-7000-8000-000000000162",
        now=now,
    )
    expired.expires_at = now - timedelta(seconds=1)
    await store.add_refresh_token(expired)
    await store.add_refresh_token(
        _refresh_row(
            live_raw,
            public_id="00000000-0000-7000-8000-000000000163",
            now=now,
        )
    )

    assert await store.load_refresh_token(expired_raw, client_id=_CLIENT_ID, now=now) is None
    assert await store.load_refresh_token("unknown-token", client_id=_CLIENT_ID, now=now) is None
    assert await store.load_refresh_token(live_raw, client_id="wrong-client", now=now) is None
    assert await store.revoke_grant(_GRANT_ID, now) is True
    assert await store.load_refresh_token(live_raw, client_id=_CLIENT_ID, now=now) is None
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_refresh_rotation_rejects_revoked_or_foreign_successor(
    tmp_path: Path,
) -> None:
    """Verify revoked predecessors and identity-breaking successors fail closed.

    Given a live refresh token and successors that change family or grant,
    When rotation uses a revoked predecessor or a foreign successor,
    Then revoked credentials are invalid and identity breaks raise.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    raw = TokenManager.mint_oauth_refresh_token().token
    await store.add_client(_client(now))
    await store.add_grant(_grant(now))
    await store.add_refresh_token(
        _refresh_row(
            raw,
            public_id="00000000-0000-7000-8000-000000000164",
            now=now,
        )
    )
    successor = _refresh_row(
        "successor",
        public_id="00000000-0000-7000-8000-000000000165",
        now=now,
    )
    assert (
        await store.rotate_refresh_token(
            "unknown-token",
            client_id=_CLIENT_ID,
            successor=successor,
            rotated_at=now,
        )
        is OAuthRefreshRotationOutcome.INVALID
    )
    await store.revoke_refresh_family(_FAMILY_ID, now)
    assert (
        await store.rotate_refresh_token(
            raw,
            client_id=_CLIENT_ID,
            successor=successor,
            rotated_at=now + timedelta(seconds=1),
        )
        is OAuthRefreshRotationOutcome.INVALID
    )

    live_raw = TokenManager.mint_oauth_refresh_token().token
    await store.add_refresh_token(
        _refresh_row(
            live_raw,
            public_id="00000000-0000-7000-8000-000000000166",
            now=now,
        )
    )
    foreign_family = _refresh_row(
        "foreign-family",
        public_id="00000000-0000-7000-8000-000000000167",
        now=now,
    )
    foreign_family.family_public_id = "00000000-0000-7000-8000-000000000168"
    with pytest.raises(ValueError, match="family or scope"):
        await store.rotate_refresh_token(
            live_raw,
            client_id=_CLIENT_ID,
            successor=foreign_family,
            rotated_at=now,
        )
    foreign_grant = _refresh_row(
        "foreign-grant",
        public_id="00000000-0000-7000-8000-000000000169",
        now=now,
    )
    foreign_grant.grant_public_id = "00000000-0000-7000-8000-000000000170"
    with pytest.raises(ValueError, match="family or scope"):
        await store.rotate_refresh_token(
            live_raw,
            client_id=_CLIENT_ID,
            successor=foreign_grant,
            rotated_at=now,
        )
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_consume_authorization_code_lost_cas_rolls_back() -> None:
    """Verify a lost code-consumption CAS rolls back and stores no refresh.

    Given a selected unconsumed code whose UPDATE reports no claimed row,
    When consume runs with a successor refresh token,
    Then the store rolls back, skips persistence, and returns None.
    """
    now = datetime.now(UTC)
    code = _code_row(
        "raw-code",
        public_id="00000000-0000-7000-8000-000000000171",
        now=now,
    )
    code.id = 4
    session = _async_session(_execute_result(scalar=code), _execute_result(rowcount=None))
    store = _store_with_sessions(session)
    refresh = _refresh_row(
        "raw-refresh",
        public_id="00000000-0000-7000-8000-000000000172",
        now=now,
    )

    assert (
        await store.consume_authorization_code(
            "raw-code",
            client_id=_CLIENT_ID,
            consumed_at=now,
            refresh_token=refresh,
        )
        is None
    )
    session.rollback.assert_awaited_once()
    session.commit.assert_not_called()
    session.add.assert_not_called()


@pytest.mark.asyncio
async def test_approve_authorization_request_lost_cas_rolls_back() -> None:
    """Verify a lost consent CAS refuses the request and does not store a code.

    Given a selected pending request whose UPDATE reports no claimed row,
    When approve runs,
    Then the store rolls back, skips the authorization code, and returns None.
    """
    now = datetime.now(UTC)
    request = _request_row(
        "raw-request",
        public_id="00000000-0000-7000-8000-000000000173",
        now=now,
    )
    request.id = 8
    session = _async_session(_execute_result(scalar=request), _execute_result(rowcount=0))
    store = _store_with_sessions(session)

    assert (
        await store.approve_authorization_request(
            "raw-request",
            code=_code_row(
                "raw-code",
                public_id="00000000-0000-7000-8000-000000000174",
                now=now,
            ),
            resolved_at=now,
        )
        is None
    )
    session.rollback.assert_awaited_once()
    session.commit.assert_not_called()
    session.add.assert_not_called()


@pytest.mark.asyncio
async def test_refresh_rotation_lost_cas_revokes_family_when_already_used() -> None:
    """Verify a lost rotation CAS that finds a used predecessor burns the family.

    Given a live predecessor whose used_at UPDATE reports no claimed row,
    When recovery reloads the same hash with used_at already set,
    Then the family is revoked and the outcome is reuse detection.
    """
    now = datetime.now(UTC)
    predecessor = _refresh_row(
        "raw-token",
        public_id="00000000-0000-7000-8000-000000000175",
        now=now,
    )
    predecessor.id = 11
    used = _refresh_row(
        "raw-token",
        public_id="00000000-0000-7000-8000-000000000175",
        now=now,
    )
    used.id = 11
    used.used_at = now
    rotate_session = _async_session(
        _execute_result(scalar=predecessor), _execute_result(rowcount=0)
    )
    recovery_session = _async_session(_execute_result(scalar=used), _execute_result(rowcount=2))
    store = _store_with_sessions(rotate_session, recovery_session)

    outcome = await store.rotate_refresh_token(
        "raw-token",
        client_id=_CLIENT_ID,
        successor=_refresh_row(
            "successor",
            public_id="00000000-0000-7000-8000-000000000176",
            now=now,
        ),
        rotated_at=now,
    )
    assert outcome is OAuthRefreshRotationOutcome.REUSE_DETECTED
    rotate_session.rollback.assert_awaited_once()
    rotate_session.commit.assert_not_called()
    rotate_session.add.assert_not_called()
    recovery_session.commit.assert_awaited_once()
    recovery_session.add.assert_not_called()


@pytest.mark.asyncio
async def test_refresh_rotation_lost_cas_is_invalid_when_predecessor_unused() -> None:
    """Verify a lost rotation CAS is invalid if nobody else consumed the token.

    Given a live predecessor whose used_at UPDATE reports no claimed row,
    When recovery reloads the hash still unused, or missing entirely,
    Then both recoveries are invalid and no family revoke is committed.
    """
    now = datetime.now(UTC)
    predecessor = _refresh_row(
        "raw-token",
        public_id="00000000-0000-7000-8000-000000000177",
        now=now,
    )
    predecessor.id = 12
    successor = _refresh_row(
        "successor",
        public_id="00000000-0000-7000-8000-000000000178",
        now=now,
    )

    unused_rotate = _async_session(
        _execute_result(scalar=predecessor),
        _execute_result(rowcount=0),
    )
    unused_recovery = _async_session(_execute_result(scalar=predecessor))
    unused_store = _store_with_sessions(unused_rotate, unused_recovery)
    assert (
        await unused_store.rotate_refresh_token(
            "raw-token",
            client_id=_CLIENT_ID,
            successor=successor,
            rotated_at=now,
        )
        is OAuthRefreshRotationOutcome.INVALID
    )
    unused_rotate.rollback.assert_awaited_once()
    unused_recovery.commit.assert_not_called()

    missing_rotate = _async_session(
        _execute_result(scalar=predecessor),
        _execute_result(rowcount=0),
    )
    missing_recovery = _async_session(_execute_result(scalar=None))
    missing_store = _store_with_sessions(missing_rotate, missing_recovery)
    assert (
        await missing_store.rotate_refresh_token(
            "raw-token",
            client_id=_CLIENT_ID,
            successor=successor,
            rotated_at=now,
        )
        is OAuthRefreshRotationOutcome.INVALID
    )
    missing_recovery.commit.assert_not_called()
