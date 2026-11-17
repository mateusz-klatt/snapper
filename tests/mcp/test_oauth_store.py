"""Integration tests for durable MCP OAuth protocol state and bound primitives."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Executable

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
from snapper.mcp.oauth.transaction import OAuthTransactionConflictError

_CLIENT_ID = "chatgpt-client"
_OTHER_CLIENT_ID = "claude-client"
_GRANT_ID = "00000000-0000-7000-8000-000000000101"
_OTHER_GRANT_ID = "00000000-0000-7000-8000-000000000191"
_FAMILY_ID = "00000000-0000-7000-8000-000000000102"
_SIBLING_FAMILY_ID = "00000000-0000-7000-8000-000000000192"
_OTHER_FAMILY_ID = "00000000-0000-7000-8000-000000000193"


async def _store(tmp_path: Path) -> tuple[SQLAlchemyRepository, MCPOAuthStore]:
    """Create one isolated repository and OAuth store."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'oauth-store.db'}")
    await repository.create_all()
    return repository, MCPOAuthStore(repository)


def _client(
    now: datetime,
    *,
    public_id: str = "00000000-0000-7000-8000-000000000100",
    client_id: str = _CLIENT_ID,
    is_active: bool = True,
) -> OAuthClient:
    """Build one pre-registered client row."""
    return OAuthClient(
        public_id=public_id,
        client_id=client_id,
        client_secret_hash="hashed-secret",
        client_name="ChatGPT",
        redirect_uris=["https://chatgpt.com/connector/oauth/callback"],
        token_endpoint_auth_method="client_secret_basic",
        allowed_scopes=["snapper.read", "offline_access"],
        is_active=is_active,
        created_at=now,
    )


def _grant(
    now: datetime,
    *,
    public_id: str = _GRANT_ID,
    client_id: str = _CLIENT_ID,
    owner_user_public_id: str = "00000000-0000-7000-8000-000000000110",
    delegate_user_public_id: str = "00000000-0000-7000-8000-000000000111",
) -> OAuthGrant:
    """Build one active read-only OAuth grant row."""
    return OAuthGrant(
        public_id=public_id,
        owner_user_public_id=owner_user_public_id,
        delegate_user_public_id=delegate_user_public_id,
        client_id=client_id,
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
    family_public_id: str = _FAMILY_ID,
    grant_public_id: str = _GRANT_ID,
) -> OAuthRefreshToken:
    """Build one persisted refresh-token hash with full default scopes."""
    return OAuthRefreshToken(
        public_id=public_id,
        token_hash=hash_token(raw_token),
        family_public_id=family_public_id,
        grant_public_id=grant_public_id,
        scopes=["snapper.read", "offline_access"],
        created_at=now,
        expires_at=now + timedelta(days=90),
    )


def _scoped_refresh_row(
    raw_token: str,
    *,
    public_id: str,
    now: datetime,
    scopes: list[str],
) -> OAuthRefreshToken:
    """Build one persisted refresh-token hash with explicit scopes."""
    row = _refresh_row(raw_token, public_id=public_id, now=now)
    row.scopes = scopes
    return row


def _code_row(
    raw_code: str,
    *,
    public_id: str,
    now: datetime,
    grant_public_id: str = _GRANT_ID,
    expires_at: datetime | None = None,
) -> OAuthAuthorizationCode:
    """Build one hashed authorization-code row."""
    return OAuthAuthorizationCode(
        public_id=public_id,
        code_hash=hash_token(raw_code),
        grant_public_id=grant_public_id,
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


@pytest.mark.asyncio
async def test_authorization_code_is_hashed_expiring_and_one_time(tmp_path: Path) -> None:
    """Verify only the correct client can consume a live code once.

    Given a hash-only authorization code and active client binding.
    When valid, repeated and expired consumption are attempted.
    Then only one valid consumption succeeds.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    raw_code = TokenManager.mint_oauth_refresh_token().token
    await store.add_client(_client(now))
    await store.add_grant(_grant(now))
    await store.add_authorization_code(
        _code_row(raw_code, public_id="00000000-0000-7000-8000-000000000120", now=now)
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

    Given an opaque pending request and matching grant/code metadata.
    When approval and competing resolution are attempted.
    Then the request resolves once and only hashed credentials are stored.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    raw_request_id = TokenManager.mint_oauth_refresh_token().token
    raw_code = TokenManager.mint_oauth_refresh_token().token
    await store.add_client(_client(now))
    await store.add_grant(_grant(now))
    await store.add_authorization_request(
        _request_row(raw_request_id, public_id="00000000-0000-7000-8000-000000000121", now=now)
    )
    pending = await store.get_pending_authorization_request(raw_request_id, now=now)
    assert pending is not None
    assert pending.request_hash == hash_token(raw_request_id)
    approved = await store.approve_authorization_request(
        raw_request_id,
        code=_code_row(raw_code, public_id="00000000-0000-7000-8000-000000000122", now=now),
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
async def test_confirmed_refresh_reuse_revokes_affected_grant_and_all_families_isolating_unrelated_grant(
    tmp_path: Path,
) -> None:
    """Verify predecessor reuse revokes the affected grant and all its families only.

    Given an affected grant with two refresh families and an unrelated grant with one family,
    When one predecessor on the affected grant is rotated and then replayed,
    Then the affected grant and both of its families are revoked and committed,
    while the unrelated grant and its refresh family remain active.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    predecessor = TokenManager.mint_oauth_refresh_token()
    successor = TokenManager.mint_oauth_refresh_token()
    sibling_token = TokenManager.mint_oauth_refresh_token()
    unrelated_token = TokenManager.mint_oauth_refresh_token()

    await store.add_client(_client(now))
    await store.add_client(
        _client(
            now,
            public_id="00000000-0000-7000-8000-000000000190",
            client_id=_OTHER_CLIENT_ID,
        )
    )
    await store.add_grant(_grant(now))
    await store.add_grant(
        _grant(
            now,
            public_id=_OTHER_GRANT_ID,
            client_id=_OTHER_CLIENT_ID,
            delegate_user_public_id="00000000-0000-7000-8000-000000000194",
        )
    )
    await store.add_refresh_token(
        _refresh_row(predecessor.token, public_id="00000000-0000-7000-8000-000000000130", now=now)
    )
    await store.add_refresh_token(
        _refresh_row(
            sibling_token.token,
            public_id="00000000-0000-7000-8000-000000000133",
            now=now,
            family_public_id=_SIBLING_FAMILY_ID,
        )
    )
    await store.add_refresh_token(
        _refresh_row(
            unrelated_token.token,
            public_id="00000000-0000-7000-8000-000000000134",
            now=now,
            family_public_id=_OTHER_FAMILY_ID,
            grant_public_id=_OTHER_GRANT_ID,
        )
    )

    outcome = await store.rotate_refresh_token(
        predecessor.token,
        client_id=_CLIENT_ID,
        successor=_scoped_refresh_row(
            successor.token,
            public_id="00000000-0000-7000-8000-000000000131",
            now=now,
            scopes=["snapper.read"],
        ),
        rotated_at=now + timedelta(seconds=1),
    )
    assert outcome is OAuthRefreshRotationOutcome.ROTATED

    wrong_client_attempt = await store.rotate_refresh_token(
        predecessor.token,
        client_id=_OTHER_CLIENT_ID,
        successor=_refresh_row(
            "wrong-client-successor",
            public_id="00000000-0000-7000-8000-000000000135",
            now=now,
            family_public_id=_OTHER_FAMILY_ID,
            grant_public_id=_OTHER_GRANT_ID,
        ),
        rotated_at=now + timedelta(seconds=2),
    )
    assert wrong_client_attempt is OAuthRefreshRotationOutcome.INVALID
    assert await store.get_active_grant(_GRANT_ID) is not None

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
    assert await store.get_active_grant(_GRANT_ID) is None
    assert (
        await store.load_refresh_token(
            successor.token,
            client_id=_CLIENT_ID,
            now=now + timedelta(seconds=4),
        )
        is None
    )
    assert (
        await store.load_refresh_token(
            sibling_token.token,
            client_id=_CLIENT_ID,
            now=now + timedelta(seconds=4),
        )
        is None
    )
    assert await store.get_active_grant(_OTHER_GRANT_ID) is not None
    assert (
        await store.load_refresh_token(
            unrelated_token.token,
            client_id=_OTHER_CLIENT_ID,
            now=now + timedelta(seconds=4),
        )
        is not None
    )
    async with repository.session() as session:
        affected_rows = list(
            (
                await session.execute(
                    select(OAuthRefreshToken).where(OAuthRefreshToken.grant_public_id == _GRANT_ID)
                )
            ).scalars()
        )
        assert len(affected_rows) == 3
        assert all(row.revoked_at is not None for row in affected_rows)
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_refresh_rotation_rejects_invalid_or_widening_successor(tmp_path: Path) -> None:
    """Verify wrong clients, expired tokens, and scope widening fail closed.

    Given an unused refresh token.
    When a successor changes its family, grant or permitted scopes.
    Then rotation rejects the successor without consuming its predecessor.
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
        _scoped_refresh_row(
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

    Given active, inactive and absent registered client identities.
    When the store loads each client.
    Then only the active registered client is returned.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    await store.add_client(_client(now))
    await store.add_client(
        _client(
            now,
            public_id="00000000-0000-7000-8000-000000000150",
            client_id="chatgpt-inactive",
            is_active=False,
        )
    )

    loaded = await store.get_client(_CLIENT_ID)
    assert loaded is not None
    assert loaded.client_id == _CLIENT_ID
    assert await store.get_client("chatgpt-inactive") is None
    assert await store.get_client("missing-client") is None
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_get_active_grant_hides_revoked_identity(tmp_path: Path) -> None:
    """Verify grant lookup returns only the live unrevoked row.

    Given an active client-bound grant.
    When revocation is committed and the grant is loaded again.
    Then the revoked identity is unavailable.
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

    Given valid, consumed and expired authorization code rows.
    When the store performs non-consuming lookups.
    Then load visibility follows the documented code state contract.
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
async def test_pending_request_approve_and_deny_refuse_inactive_client_and_invalid_grant(
    tmp_path: Path,
) -> None:
    """Verify approve and deny refuse inactive clients and revoked or foreign grants.

    Given pending consent with an inactive client or invalid grant linkage.
    When approval and denial primitives run.
    Then invalid state cannot resolve the request.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    expired_raw = TokenManager.mint_oauth_refresh_token().token
    inactive_raw = TokenManager.mint_oauth_refresh_token().token
    foreign_grant_req = TokenManager.mint_oauth_refresh_token().token
    revoked_grant_req = TokenManager.mint_oauth_refresh_token().token

    await store.add_client(_client(now))
    await store.add_client(
        _client(
            now,
            public_id="00000000-0000-7000-8000-000000000155",
            client_id="chatgpt-inactive",
            is_active=False,
        )
    )
    await store.add_client(
        _client(
            now,
            public_id="00000000-0000-7000-8000-000000000195",
            client_id=_OTHER_CLIENT_ID,
        )
    )
    await store.add_grant(_grant(now))
    await store.add_grant(
        _grant(
            now,
            public_id=_OTHER_GRANT_ID,
            client_id=_OTHER_CLIENT_ID,
            delegate_user_public_id="00000000-0000-7000-8000-000000000196",
        )
    )
    revoked_grant = _grant(
        now,
        public_id="00000000-0000-7000-8000-000000000197",
        owner_user_public_id="00000000-0000-7000-8000-000000000198",
        delegate_user_public_id="00000000-0000-7000-8000-000000000199",
    )
    revoked_grant.revoked_at = now
    await store.add_grant(revoked_grant)

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
    await store.add_authorization_request(
        _request_row(
            foreign_grant_req,
            public_id="00000000-0000-7000-8000-000000000181",
            now=now,
        )
    )
    await store.add_authorization_request(
        _request_row(
            revoked_grant_req,
            public_id="00000000-0000-7000-8000-000000000182",
            now=now,
        )
    )

    assert await store.get_pending_authorization_request(expired_raw, now=now) is None
    assert await store.get_pending_authorization_request(inactive_raw, now=now) is None
    assert (
        await store.approve_authorization_request(
            inactive_raw,
            code=_code_row(
                "inactive-code", public_id="00000000-0000-7000-8000-000000000183", now=now
            ),
            resolved_at=now,
        )
        is None
    )
    assert await store.deny_authorization_request(inactive_raw, resolved_at=now) is None
    assert (
        await store.approve_authorization_request(
            foreign_grant_req,
            code=_code_row(
                "foreign-code",
                public_id="00000000-0000-7000-8000-000000000184",
                now=now,
                grant_public_id=_OTHER_GRANT_ID,
            ),
            resolved_at=now,
        )
        is None
    )
    assert await store.get_pending_authorization_request(foreign_grant_req, now=now) is not None
    assert (
        await store.approve_authorization_request(
            revoked_grant_req,
            code=_code_row(
                "revoked-code",
                public_id="00000000-0000-7000-8000-000000000185",
                now=now,
                grant_public_id=revoked_grant.public_id,
            ),
            resolved_at=now,
        )
        is None
    )
    assert await store.get_pending_authorization_request(revoked_grant_req, now=now) is not None
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_deny_authorization_request_resolves_once(tmp_path: Path) -> None:
    """Verify a live pending request can be denied exactly once.

    Given a pending authorization request.
    When denial is repeated.
    Then exactly one terminal denial is committed.
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
async def test_revoke_refresh_family_preserves_family_only_semantics_and_grant_revoke_is_idempotent(
    tmp_path: Path,
) -> None:
    """Verify family revoke leaves grant and sibling families intact while grant revoke burns all.

    Given multiple refresh families and an active grant.
    When family-only revocation is followed by repeated grant revocation.
    Then the first operation stays scoped and grant revocation retires all remaining families once.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    raw = TokenManager.mint_oauth_refresh_token().token
    sibling = TokenManager.mint_oauth_refresh_token().token
    await store.add_client(_client(now))
    await store.add_grant(_grant(now))
    await store.add_refresh_token(
        _refresh_row(raw, public_id="00000000-0000-7000-8000-000000000160", now=now)
    )
    await store.add_refresh_token(
        _refresh_row(
            sibling,
            public_id="00000000-0000-7000-8000-000000000186",
            now=now,
            family_public_id=_SIBLING_FAMILY_ID,
        )
    )

    assert await store.revoke_refresh_family(_FAMILY_ID, now) == 1
    assert await store.load_refresh_token(raw, client_id=_CLIENT_ID, now=now) is None
    assert await store.load_refresh_token(sibling, client_id=_CLIENT_ID, now=now) is not None
    assert await store.revoke_refresh_family(_FAMILY_ID, now + timedelta(seconds=1)) == 0
    assert await store.get_active_grant(_GRANT_ID) is not None

    assert await store.revoke_grant(_GRANT_ID, now + timedelta(seconds=2)) is True
    assert await store.load_refresh_token(sibling, client_id=_CLIENT_ID, now=now) is None
    leftover = TokenManager.mint_oauth_refresh_token().token
    await store.add_refresh_token(
        _refresh_row(leftover, public_id="00000000-0000-7000-8000-000000000161", now=now)
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

    Given expired, revoked and unknown refresh identities.
    When the store loads each token for its client.
    Then no unusable credential is returned.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    expired_raw = TokenManager.mint_oauth_refresh_token().token
    live_raw = TokenManager.mint_oauth_refresh_token().token
    await store.add_client(_client(now))
    await store.add_grant(_grant(now))
    expired = _refresh_row(expired_raw, public_id="00000000-0000-7000-8000-000000000162", now=now)
    expired.expires_at = now - timedelta(seconds=1)
    await store.add_refresh_token(expired)
    await store.add_refresh_token(
        _refresh_row(live_raw, public_id="00000000-0000-7000-8000-000000000163", now=now)
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

    Given a predecessor that is revoked or a successor from another binding.
    When rotation is attempted.
    Then invalid tokens cannot create a successor.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    raw = TokenManager.mint_oauth_refresh_token().token
    await store.add_client(_client(now))
    await store.add_grant(_grant(now))
    await store.add_refresh_token(
        _refresh_row(raw, public_id="00000000-0000-7000-8000-000000000164", now=now)
    )
    successor = _refresh_row("successor", public_id="00000000-0000-7000-8000-000000000165", now=now)
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
        _refresh_row(live_raw, public_id="00000000-0000-7000-8000-000000000166", now=now)
    )
    foreign_family = _refresh_row(
        "foreign-family",
        public_id="00000000-0000-7000-8000-000000000167",
        now=now,
        family_public_id="00000000-0000-7000-8000-000000000168",
    )
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
        grant_public_id="00000000-0000-7000-8000-000000000170",
    )
    with pytest.raises(ValueError, match="family or scope"):
        await store.rotate_refresh_token(
            live_raw,
            client_id=_CLIENT_ID,
            successor=foreign_grant,
            rotated_at=now,
        )
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_lost_cas_in_legacy_wrappers_restarts_cleanly_and_revokes_affected_grant_on_reuse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify lost CAS rolls back whole failed TX and classifies in a fresh TX.

    Given a real SQLite repository where concurrent completion wins between SELECT and UPDATE,
    When consume_authorization_code, approve_authorization_request, and rotate_refresh_token
    lose their first CAS claim,
    Then code/request wrappers roll back without partial rows, and refresh rotation restarts
    in a fresh transaction that revokes the affected grant and all its families when used_at
    was set concurrently, or returns INVALID when revoked/unused.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    await store.add_client(_client(now))
    await store.add_grant(_grant(now))
    raw_token = TokenManager.mint_oauth_refresh_token().token
    sibling_token = TokenManager.mint_oauth_refresh_token().token
    await store.add_refresh_token(
        _refresh_row(raw_token, public_id="00000000-0000-7000-8000-000000000175", now=now)
    )
    await store.add_refresh_token(
        _refresh_row(
            sibling_token,
            public_id="00000000-0000-7000-8000-000000000187",
            now=now,
            family_public_id=_SIBLING_FAMILY_ID,
        )
    )

    original_session = repository.session
    intercepted = False

    @asynccontextmanager
    async def _racing_session() -> AsyncIterator[AsyncSession]:
        """Commit an independent winner only after the losing writer has rolled back."""
        nonlocal intercepted
        try:
            async with original_session() as session:
                original_execute = session.execute

                async def _execute_with_concurrent_winner(statement: Executable) -> object:
                    """Return a genuine zero-row claim without fabricating a same-TX winner."""
                    nonlocal intercepted
                    sql = str(statement)
                    if (
                        not intercepted
                        and "UPDATE oauth_refresh_tokens" in sql
                        and "used_at" in sql
                    ):
                        intercepted = True
                        return await original_execute(
                            update(OAuthRefreshToken)
                            .where(OAuthRefreshToken.id == -1)
                            .values(used_at=now)
                        )
                    return await original_execute(statement)

                monkeypatch.setattr(session, "execute", _execute_with_concurrent_winner)
                yield session
        except OAuthTransactionConflictError:
            async with original_session() as winner:
                await winner.execute(
                    update(OAuthRefreshToken)
                    .where(OAuthRefreshToken.token_hash == hash_token(raw_token))
                    .values(used_at=now)
                )
                await winner.commit()
            raise

    monkeypatch.setattr(repository, "session", _racing_session)
    outcome = await store.rotate_refresh_token(
        raw_token,
        client_id=_CLIENT_ID,
        successor=_refresh_row(
            "lost-cas-successor",
            public_id="00000000-0000-7000-8000-000000000176",
            now=now,
        ),
        rotated_at=now + timedelta(seconds=1),
    )
    monkeypatch.setattr(repository, "session", original_session)

    assert intercepted is True
    assert outcome is OAuthRefreshRotationOutcome.REUSE_DETECTED
    assert await store.get_active_grant(_GRANT_ID) is None
    async with repository.session() as check_session:
        inserted_successor = (
            await check_session.execute(
                select(OAuthRefreshToken).where(
                    OAuthRefreshToken.public_id == "00000000-0000-7000-8000-000000000176"
                )
            )
        ).scalar_one_or_none()
        sibling_row = (
            await check_session.execute(
                select(OAuthRefreshToken).where(
                    OAuthRefreshToken.public_id == "00000000-0000-7000-8000-000000000187"
                )
            )
        ).scalar_one()
    assert inserted_successor is None
    assert sibling_row.revoked_at is not None
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_session_bound_store_primitives_never_commit_and_honor_caller_rollback(
    tmp_path: Path,
) -> None:
    """Verify session-bound store primitives let the outer transaction owner roll back.

    Given a caller-owned transaction around the in-transaction store primitives.
    When the caller rolls back after successful mutations.
    Then all mutations disappear and no helper has committed.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    raw_request_id = TokenManager.mint_oauth_refresh_token().token
    raw_code = TokenManager.mint_oauth_refresh_token().token
    await store.add_client(_client(now))
    await store.add_grant(_grant(now))
    await store.add_authorization_request(
        _request_row(raw_request_id, public_id="00000000-0000-7000-8000-000000000188", now=now)
    )

    async with store.begin_write_transaction() as session:
        claimed = await store.approve_authorization_request_in_tx(
            session,
            raw_request_id,
            code=_code_row(raw_code, public_id="00000000-0000-7000-8000-000000000189", now=now),
            resolved_at=now,
        )
        assert claimed is not None
        await session.rollback()

    assert await store.get_pending_authorization_request(raw_request_id, now=now) is not None
    assert await store.load_authorization_code(raw_code, client_id=_CLIENT_ID) is None
    await repository.engine.dispose()
