"""Slice 2 service boundaries: refresh narrowing, revocation and the consent view.

The coordinator's policy (Iris ``ms_8a498d7a877b488474bece0493489eed``) makes
``offline_access`` the only authority to issue a refresh token. A valid refresh
that narrows to ``snapper.read`` consumes its predecessor and returns an access
token only. Replay detection and grant-wide revocation still hold after that
terminal exchange. An omitted refresh scope keeps the predecessor's scopes.
"""

from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from uuid import uuid7

import pytest
import pytest_asyncio
from freezegun import freeze_time
from sqlalchemy import select
from sqlalchemy import update

from snapper.data.models import OAuthAuthorizationRequest
from snapper.data.models import OAuthClient
from snapper.data.models import OAuthGrant
from snapper.data.models import OAuthRefreshToken
from snapper.mcp.oauth.contracts import MCPOAuthError
from snapper.mcp.oauth.contracts import OAuthConsentApprovalCommand
from snapper.mcp.oauth.contracts import OAuthRefreshReuseDetectedError
from snapper.mcp.oauth.contracts import OAuthRevocationCommand
from snapper.mcp.oauth.scopes import OFFLINE_ACCESS
from snapper.mcp.oauth.scopes import SNAPPER_READ
from tests.mcp.test_oauth_service import _CLIENT_ID
from tests.mcp.test_oauth_service import _CLIENT_SECRET
from tests.mcp.test_oauth_service import _OTHER_CLIENT_ID
from tests.mcp.test_oauth_service import _OTHER_CLIENT_SECRET
from tests.mcp.test_oauth_service import _RESOURCE
from tests.mcp.test_oauth_service import _build_harness
from tests.mcp.test_oauth_service import _principal
from tests.mcp.test_oauth_service import _seed_user_with_membership
from tests.mcp.test_oauth_service_boundaries import OAuthCase
from tests.mcp.test_oauth_service_boundaries import _refresh
from tests.mcp.test_oauth_service_boundaries import _request
from tests.mcp.test_oauth_service_boundaries import _tokens


@pytest_asyncio.fixture
async def oauth_case(tmp_path: Path) -> AsyncIterator[OAuthCase]:
    """Seed one owner with current membership on an isolated SQLite service."""
    harness = await _build_harness(tmp_path)
    owner_id = str(uuid7())
    membership = await _seed_user_with_membership(harness.repository, user_public_id=owner_id)
    try:
        yield OAuthCase(harness, _principal(owner_id, membership_public_id=membership))
    finally:
        await harness.repository.engine.dispose()


async def _refresh_rows(case: OAuthCase) -> list[OAuthRefreshToken]:
    """Return every persisted refresh row in creation order."""
    async with case.harness.repository.session() as session:
        statement = select(OAuthRefreshToken).order_by(OAuthRefreshToken.id)
        return list((await session.execute(statement)).scalars())


async def _grant_revoked(case: OAuthCase) -> bool:
    """Report whether the single grant of this case is revoked."""
    async with case.harness.repository.session() as session:
        grant = (await session.execute(select(OAuthGrant))).scalar_one()
        return grant.revoked_at is not None


@pytest.mark.asyncio
async def test_refresh_narrowed_without_offline_access_is_terminal_and_access_only(
    oauth_case: OAuthCase,
) -> None:
    """Narrowing away offline_access consumes the predecessor and issues no successor.

    Given a read/offline refresh family.
    When the client refreshes with only snapper.read.
    Then it receives a live read access token, no refresh token, and no successor row.
    """
    tokens = await _tokens(oauth_case)
    assert tokens.refresh_token is not None
    narrowed = await oauth_case.harness.service.exchange_refresh_token(
        replace(_refresh(tokens.refresh_token), scopes=(SNAPPER_READ,))
    )
    assert narrowed.refresh_token is None
    assert narrowed.scopes == (SNAPPER_READ,)
    verified = await oauth_case.harness.service.verify_access_token(narrowed.access_token)
    assert verified.scopes == (SNAPPER_READ,)
    rows = await _refresh_rows(oauth_case)
    assert len(rows) == 1
    assert rows[0].used_at is not None
    assert rows[0].replaced_by_public_id is None
    assert rows[0].revoked_at is None


@pytest.mark.asyncio
async def test_replay_after_terminal_access_only_exchange_revokes_the_grant(
    oauth_case: OAuthCase,
) -> None:
    """A consumed terminal predecessor is still a replay signal.

    Given a refresh token already consumed by an access-only narrowing.
    When the same refresh token is presented again.
    Then reuse is reported, the grant and its families are revoked, and live access ends.
    """
    tokens = await _tokens(oauth_case)
    assert tokens.refresh_token is not None
    narrowed = await oauth_case.harness.service.exchange_refresh_token(
        replace(_refresh(tokens.refresh_token), scopes=(SNAPPER_READ,))
    )
    with pytest.raises(OAuthRefreshReuseDetectedError):
        await oauth_case.harness.service.exchange_refresh_token(_refresh(tokens.refresh_token))
    assert await _grant_revoked(oauth_case)
    assert all(row.revoked_at is not None for row in await _refresh_rows(oauth_case))
    with pytest.raises(MCPOAuthError):
        await oauth_case.harness.service.verify_access_token(narrowed.access_token)


@pytest.mark.asyncio
async def test_omitted_refresh_scope_keeps_offline_access_and_rotates(
    oauth_case: OAuthCase,
) -> None:
    """An omitted refresh scope preserves the original authorized scope set.

    Given a read/offline refresh family.
    When the client refreshes without a scope parameter.
    Then the family rotates and the successor keeps both scopes.
    """
    tokens = await _tokens(oauth_case)
    assert tokens.refresh_token is not None
    rotated = await oauth_case.harness.service.exchange_refresh_token(
        _refresh(tokens.refresh_token)
    )
    assert rotated.refresh_token is not None
    assert rotated.scopes == (SNAPPER_READ, OFFLINE_ACCESS)
    rows = await _refresh_rows(oauth_case)
    assert len(rows) == 2
    assert rows[0].replaced_by_public_id == rows[1].public_id
    assert rows[1].scopes == [SNAPPER_READ, OFFLINE_ACCESS]


@pytest.mark.parametrize(
    "scopes",
    [(), (OFFLINE_ACCESS,), ("snapper.trade",), (SNAPPER_READ, "snapper.account.read")],
    ids=["empty", "offline-only", "widened-trade", "widened-account"],
)
@pytest.mark.asyncio
async def test_invalid_refresh_scope_is_refused_before_any_mutation(
    oauth_case: OAuthCase, scopes: tuple[str, ...]
) -> None:
    """Empty, resource-free and widened refresh scopes mutate nothing.

    Given a read/offline refresh family.
    When the client refreshes with an empty, offline-only or widened scope set.
    Then invalid_scope is raised and the predecessor remains unused and unrevoked.
    """
    tokens = await _tokens(oauth_case)
    assert tokens.refresh_token is not None
    with pytest.raises(MCPOAuthError, match="invalid_scope"):
        await oauth_case.harness.service.exchange_refresh_token(
            replace(_refresh(tokens.refresh_token), scopes=scopes)
        )
    rows = await _refresh_rows(oauth_case)
    assert len(rows) == 1
    assert rows[0].used_at is None
    assert rows[0].revoked_at is None
    assert not await _grant_revoked(oauth_case)


def _revocation(
    raw: str, *, client_id: str = _CLIENT_ID, secret: str = _CLIENT_SECRET
) -> OAuthRevocationCommand:
    """Build one client-authenticated RFC 7009 revocation."""
    method = "client_secret_basic" if client_id == _CLIENT_ID else "client_secret_post"
    return OAuthRevocationCommand(client_id, secret, method, raw)


@pytest.mark.parametrize("credential", ["refresh", "access"])
@pytest.mark.asyncio
async def test_revocation_by_either_credential_revokes_the_whole_owned_grant(
    oauth_case: OAuthCase, credential: str
) -> None:
    """Revoking an access or refresh credential ends the grant and every family.

    Given an issued access token and refresh family for the authenticated client.
    When the client revokes either credential.
    Then the grant and its refresh rows are revoked and both credentials stop working.
    """
    tokens = await _tokens(oauth_case)
    assert tokens.refresh_token is not None
    raw = tokens.refresh_token if credential == "refresh" else tokens.access_token
    await oauth_case.harness.service.revoke_token(_revocation(raw))
    assert await _grant_revoked(oauth_case)
    assert all(row.revoked_at is not None for row in await _refresh_rows(oauth_case))
    with pytest.raises(MCPOAuthError):
        await oauth_case.harness.service.verify_access_token(tokens.access_token)
    with pytest.raises(MCPOAuthError):
        await oauth_case.harness.service.exchange_refresh_token(_refresh(tokens.refresh_token))
    await oauth_case.harness.service.revoke_token(_revocation(raw))


@pytest.mark.parametrize(
    "token", ["unknown-opaque-token", "not.a.jwt", ""], ids=["unknown", "malformed", "empty"]
)
@pytest.mark.asyncio
async def test_revocation_of_an_unknown_or_invalid_token_succeeds_without_mutation(
    oauth_case: OAuthCase, token: str
) -> None:
    """RFC 7009: an invalid token is accepted and changes nothing.

    Given a live grant.
    When the client revokes an unknown, malformed or empty token.
    Then no error is raised and the grant stays active.
    """
    tokens = await _tokens(oauth_case)
    await oauth_case.harness.service.revoke_token(_revocation(token))
    assert not await _grant_revoked(oauth_case)
    assert await oauth_case.harness.service.verify_access_token(tokens.access_token)


@pytest.mark.parametrize("credential", ["refresh", "access"])
@pytest.mark.asyncio
async def test_another_client_cannot_revoke_a_foreign_grant(
    oauth_case: OAuthCase, credential: str
) -> None:
    """A valid client authenticating with someone else's token mutates nothing.

    Given a grant owned by the first client.
    When the second, separately authenticated client revokes its credential.
    Then the call succeeds as a no-op and the first client's grant stays live.
    """
    tokens = await _tokens(oauth_case)
    assert tokens.refresh_token is not None
    raw = tokens.refresh_token if credential == "refresh" else tokens.access_token
    await oauth_case.harness.service.revoke_token(
        _revocation(raw, client_id=_OTHER_CLIENT_ID, secret=_OTHER_CLIENT_SECRET)
    )
    assert not await _grant_revoked(oauth_case)
    assert await oauth_case.harness.service.verify_access_token(tokens.access_token)


@pytest.mark.asyncio
async def test_revocation_requires_client_authentication(oauth_case: OAuthCase) -> None:
    """A wrong client secret is invalid_client and mutates nothing.

    Given a live grant.
    When revocation is attempted with a wrong secret for the owning client.
    Then invalid_client is raised and the grant stays active.
    """
    tokens = await _tokens(oauth_case)
    with pytest.raises(MCPOAuthError, match="invalid_client"):
        await oauth_case.harness.service.revoke_token(
            _revocation(tokens.access_token, secret="wrong-secret")
        )
    assert not await _grant_revoked(oauth_case)


@pytest.mark.asyncio
async def test_describe_pending_request_is_mutation_free_and_secret_free(
    oauth_case: OAuthCase,
) -> None:
    """The consent view exposes client, scopes, resource and lifetime only.

    Given a fresh pending authorization request.
    When the consent page describes it twice.
    Then both reads agree, the request stays unresolved, and approval still works.
    """
    pending = await oauth_case.harness.service.create_authorization_request(_request())
    first = await oauth_case.harness.service.describe_authorization_request(pending.raw_request_id)
    second = await oauth_case.harness.service.describe_authorization_request(pending.raw_request_id)
    assert first.client_name == "ChatGPT Connector"
    assert first.scopes == (SNAPPER_READ, OFFLINE_ACCESS)
    assert first.resource == _RESOURCE
    assert 0 < first.expires_in <= 300
    assert second.scopes == first.scopes
    assert not hasattr(first, "state") and not hasattr(first, "redirect_uri")
    approved = await oauth_case.harness.service.approve_authorization_request(
        OAuthConsentApprovalCommand(pending.raw_request_id, oauth_case.owner)
    )
    assert approved.raw_code


@pytest.mark.parametrize("case", ["unknown", "resolved", "expired", "client_inactive"])
@pytest.mark.asyncio
async def test_unavailable_pending_requests_are_indistinguishable(
    oauth_case: OAuthCase, case: str
) -> None:
    """Unknown, resolved, expired and orphaned requests all report invalid_request.

    Given a request in one unavailable state.
    When the consent page describes it.
    Then the same invalid_request error is raised, revealing nothing about existence.
    """
    pending = await oauth_case.harness.service.create_authorization_request(_request())
    raw = pending.raw_request_id
    if case == "unknown":
        raw = "never-issued-request-id"
    elif case == "resolved":
        await oauth_case.harness.service.approve_authorization_request(
            OAuthConsentApprovalCommand(raw, oauth_case.owner)
        )
    else:
        async with oauth_case.harness.repository.session() as session:
            if case == "expired":
                await session.execute(
                    update(OAuthAuthorizationRequest).values(
                        expires_at=datetime.now(UTC) - timedelta(seconds=1)
                    )
                )
            else:
                await session.execute(update(OAuthClient).values(is_active=False))
            await session.commit()
    with pytest.raises(MCPOAuthError, match="^invalid_request$"):
        await oauth_case.harness.service.describe_authorization_request(raw)


@pytest.mark.asyncio
async def test_a_legacy_narrowed_refresh_row_cannot_re_expand(oauth_case: OAuthCase) -> None:
    """A pre-existing read-only refresh row still cannot widen back to offline access.

    Given a refresh row persisted with only snapper.read, as the earlier code could write.
    When the client asks to refresh it with snapper.read and offline_access.
    Then invalid_scope is raised before any mutation.
    """
    tokens = await _tokens(oauth_case)
    assert tokens.refresh_token is not None
    async with oauth_case.harness.repository.session() as session:
        await session.execute(update(OAuthRefreshToken).values(scopes=[SNAPPER_READ]))
        await session.commit()
    with pytest.raises(MCPOAuthError, match="invalid_scope"):
        await oauth_case.harness.service.exchange_refresh_token(
            replace(_refresh(tokens.refresh_token), scopes=(SNAPPER_READ, OFFLINE_ACCESS))
        )
    rows = await _refresh_rows(oauth_case)
    assert rows[0].used_at is None and rows[0].revoked_at is None


@pytest.mark.asyncio
async def test_revocation_with_an_expired_access_token_is_a_no_op(
    oauth_case: OAuthCase,
) -> None:
    """An expired access JWT cannot be verified, so it revokes nothing.

    Given a live grant and its access token, observed after the token expired.
    When the client revokes that expired access token.
    Then the call succeeds and the grant stays active.
    """
    tokens = await _tokens(oauth_case)
    later = datetime.now(UTC) + timedelta(seconds=3600)
    with freeze_time(later):
        await oauth_case.harness.service.revoke_token(_revocation(tokens.access_token))
    assert not await _grant_revoked(oauth_case)


@pytest.mark.parametrize("change", ["expired", "family_revoked"])
@pytest.mark.asyncio
async def test_revocation_with_a_dead_refresh_token_is_a_no_op(
    oauth_case: OAuthCase, change: str
) -> None:
    """Expired or individually revoked refresh rows revoke nothing (matrix row 4).

    Given a live grant whose refresh row has expired or was revoked on its own.
    When its own client revokes that refresh token.
    Then the call succeeds and the grant and its access token stay live.
    """
    tokens = await _tokens(oauth_case)
    assert tokens.refresh_token is not None
    values = (
        {"expires_at": datetime.now(UTC) - timedelta(seconds=1)}
        if change == "expired"
        else {"revoked_at": datetime.now(UTC)}
    )
    async with oauth_case.harness.repository.session() as session:
        await session.execute(update(OAuthRefreshToken).values(**values))
        await session.commit()
    await oauth_case.harness.service.revoke_token(_revocation(tokens.refresh_token))
    assert not await _grant_revoked(oauth_case)
    assert await oauth_case.harness.service.verify_access_token(tokens.access_token)


@pytest.mark.asyncio
async def test_revocation_with_a_used_own_refresh_token_revokes_its_grant(
    oauth_case: OAuthCase,
) -> None:
    """A consumed, unexpired predecessor still lets its own client revoke the grant.

    Given a refresh token already rotated once.
    When its own client revokes the consumed predecessor.
    Then the grant and every family are revoked, including the live successor.
    """
    tokens = await _tokens(oauth_case)
    assert tokens.refresh_token is not None
    rotated = await oauth_case.harness.service.exchange_refresh_token(
        _refresh(tokens.refresh_token)
    )
    await oauth_case.harness.service.revoke_token(_revocation(tokens.refresh_token))
    assert await _grant_revoked(oauth_case)
    with pytest.raises(MCPOAuthError):
        await oauth_case.harness.service.verify_access_token(rotated.access_token)


@pytest.mark.parametrize(("offset", "revokes"), [(-1, True), (0, False)], ids=["before", "at"])
@pytest.mark.asyncio
async def test_refresh_revocation_expiry_boundary_is_exclusive(
    oauth_case: OAuthCase, offset: int, revokes: bool
) -> None:
    """A refresh token is live strictly before its expiry instant.

    Given a refresh row and a clock one second before, or exactly at, its expiry.
    When its own client revokes it.
    Then the grant is revoked only while the token is still live.
    """
    tokens = await _tokens(oauth_case)
    assert tokens.refresh_token is not None
    expires_at = (await _refresh_rows(oauth_case))[0].expires_at
    with freeze_time(expires_at + timedelta(seconds=offset)):
        await oauth_case.harness.service.revoke_token(_revocation(tokens.refresh_token))
    assert await _grant_revoked(oauth_case) is revokes


@pytest.mark.asyncio
async def test_revoking_one_grant_leaves_another_owners_grant_usable(
    oauth_case: OAuthCase,
) -> None:
    """Revocation reaches exactly the presented credential's grant.

    Given two owners, each with a live grant for the same client.
    When the first owner's used predecessor is revoked.
    Then only the first grant is revoked and the second still refreshes and verifies.
    """
    first = await _tokens(oauth_case)
    assert first.refresh_token is not None
    other_id = str(uuid7())
    membership = await _seed_user_with_membership(
        oauth_case.harness.repository, user_public_id=other_id
    )
    other_owner = OAuthCase(
        oauth_case.harness, _principal(other_id, membership_public_id=membership)
    )
    second = await _tokens(other_owner)
    assert second.refresh_token is not None
    await oauth_case.harness.service.exchange_refresh_token(_refresh(first.refresh_token))
    await oauth_case.harness.service.revoke_token(_revocation(first.refresh_token))
    rotated = await oauth_case.harness.service.exchange_refresh_token(
        _refresh(second.refresh_token)
    )
    assert await oauth_case.harness.service.verify_access_token(rotated.access_token)
    async with oauth_case.harness.repository.session() as session:
        grants = (await session.execute(select(OAuthGrant))).scalars().all()
        states = {grant.owner_user_public_id: grant.revoked_at is not None for grant in grants}
    assert states == {oauth_case.owner.user_public_id: True, other_id: False}
