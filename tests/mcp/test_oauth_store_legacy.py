"""Integration tests for durable MCP OAuth protocol state."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.engine import Result
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Executable
from sqlalchemy.sql.dml import Update

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
async def test_consume_authorization_code_lost_cas_rolls_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A real zero-row code claim rolls back and stores no refresh successor.

    Given a real SQLite code-consumption transaction.
    When its compare-and-swap update claims zero rows.
    Then the code and optional refresh insertion both roll back.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    await store.add_client(_client(now))
    await store.add_grant(_grant(now))
    await store.add_authorization_code(
        _code_row("raw-code", public_id="00000000-0000-7000-8000-000000000171", now=now)
    )
    _lose_one_claim(repository, monkeypatch, "oauth_authorization_codes")
    refresh = _refresh_row("raw-refresh", public_id="00000000-0000-7000-8000-000000000172", now=now)
    assert (
        await store.consume_authorization_code(
            "raw-code", client_id=_CLIENT_ID, consumed_at=now, refresh_token=refresh
        )
        is None
    )
    assert await store.load_authorization_code("raw-code", client_id=_CLIENT_ID) is not None
    assert await store.load_refresh_token("raw-refresh", client_id=_CLIENT_ID, now=now) is None
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_approve_authorization_request_lost_cas_rolls_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A real zero-row consent claim leaves the request pending and stores no code.

    Given a real SQLite consent-approval transaction.
    When its request compare-and-swap update claims zero rows.
    Then neither approval nor code insertion persists.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    await store.add_client(_client(now))
    await store.add_grant(_grant(now))
    await store.add_authorization_request(
        _request_row("raw-request", public_id="00000000-0000-7000-8000-000000000173", now=now)
    )
    _lose_one_claim(repository, monkeypatch, "oauth_authorization_requests")
    assert (
        await store.approve_authorization_request(
            "raw-request",
            code=_code_row("raw-code", public_id="00000000-0000-7000-8000-000000000174", now=now),
            resolved_at=now,
        )
        is None
    )
    assert await store.get_pending_authorization_request("raw-request", now=now) is not None
    assert await store.load_authorization_code("raw-code", client_id=_CLIENT_ID) is None
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_refresh_rotation_lost_cas_revokes_family_when_already_used(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fresh recovery sees a separately committed winner and revokes the grant's family.

    Given a loser rolled back before a separate winner commits predecessor use.
    When fresh recovery inspects the committed winner.
    Then strict replay revocation commits in the new transaction.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    await store.add_client(_client(now))
    await store.add_grant(_grant(now))
    await store.add_refresh_token(
        _refresh_row("raw-token", public_id="00000000-0000-7000-8000-000000000175", now=now)
    )
    _lose_one_claim(repository, monkeypatch, "oauth_refresh_tokens", winner_used_at=now)
    assert (
        await store.rotate_refresh_token(
            "raw-token",
            client_id=_CLIENT_ID,
            successor=_refresh_row(
                "successor", public_id="00000000-0000-7000-8000-000000000176", now=now
            ),
            rotated_at=now,
        )
        is OAuthRefreshRotationOutcome.REUSE_DETECTED
    )
    assert await store.get_active_grant(_GRANT_ID) is None
    assert await store.load_refresh_token("successor", client_id=_CLIENT_ID, now=now) is None
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_refresh_rotation_lost_cas_is_invalid_when_predecessor_unused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A lost claim with no committed winner stays invalid without revoking the grant.

    Given a refresh claim that loses without a committed winner.
    When fresh recovery still finds an unused predecessor.
    Then the operation is invalid and leaves the valid family unchanged.
    """
    repository, store = await _store(tmp_path)
    now = datetime.now(UTC)
    await store.add_client(_client(now))
    await store.add_grant(_grant(now))
    await store.add_refresh_token(
        _refresh_row("raw-token", public_id="00000000-0000-7000-8000-000000000177", now=now)
    )
    _lose_one_claim(repository, monkeypatch, "oauth_refresh_tokens")
    assert (
        await store.rotate_refresh_token(
            "raw-token",
            client_id=_CLIENT_ID,
            successor=_refresh_row(
                "successor", public_id="00000000-0000-7000-8000-000000000178", now=now
            ),
            rotated_at=now,
        )
        is OAuthRefreshRotationOutcome.INVALID
    )
    assert await store.get_active_grant(_GRANT_ID) is not None
    assert await store.load_refresh_token("raw-token", client_id=_CLIENT_ID, now=now) is not None
    await repository.engine.dispose()


def _lose_one_claim(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
    table: str,
    *,
    winner_used_at: datetime | None = None,
) -> None:
    """Model a zero-row claim; any winner commits only after full loser rollback."""
    original_session = repository.session
    intercepted = False

    @asynccontextmanager
    async def racing_session() -> AsyncIterator[AsyncSession]:
        """Keep real SQLite begin, rollback, and classification boundaries."""
        nonlocal intercepted
        try:
            async with original_session() as session:
                execute = session.execute

                async def losing_execute(statement: Executable) -> Result[tuple[object, ...]]:
                    """Add an impossible predicate to exactly one real claim update."""
                    nonlocal intercepted
                    if (
                        not intercepted
                        and isinstance(statement, Update)
                        and getattr(statement.table, "name", None) == table
                    ):
                        intercepted = True
                        statement = statement.where(statement.table.c.id == -1)
                    return cast(Result[tuple[object, ...]], await execute(statement))

                monkeypatch.setattr(session, "execute", losing_execute)
                yield session
        except OAuthTransactionConflictError:
            if winner_used_at is not None:
                async with original_session() as winner:
                    await winner.execute(
                        update(OAuthRefreshToken)
                        .where(OAuthRefreshToken.token_hash == hash_token("raw-token"))
                        .values(used_at=winner_used_at)
                    )
                    await winner.commit()
            raise

    monkeypatch.setattr(repository, "session", racing_session)
