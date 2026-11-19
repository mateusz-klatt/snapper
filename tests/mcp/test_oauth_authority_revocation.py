"""Exercise durable grant revocation through actual user and desk mutations."""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from uuid import uuid7

import pytest
import pytest_asyncio
from sqlalchemy import ColumnElement
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.tokens import TokenPair
from snapper.auth.tokens import TOKEN_TYPE_ACCESS
from snapper.auth.tokens import TOKEN_TYPE_REFRESH
from snapper.auth.user_service import UserService
from snapper.data.models import OAuthGrant
from snapper.data.models import OAuthRefreshToken
from snapper.data.models import Operator
from snapper.data.models import UserOperatorMembership
from snapper.data.oauth_grant_revocation import revoke_oauth_grants_in_tx
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import DeskMembershipAttach
from snapper.data.repository_types import DeskMembershipDetach
from snapper.mcp.oauth.contracts import MCPOAuthError
from tests.mcp.test_oauth_service import _OPERATOR_A
from tests.mcp.test_oauth_service import _OPERATOR_B
from tests.mcp.test_oauth_service import _build_harness
from tests.mcp.test_oauth_service import _principal
from tests.mcp.test_oauth_service import _seed_user_with_membership
from tests.mcp.test_oauth_service_boundaries import OAuthCase
from tests.mcp.test_oauth_service_boundaries import _refresh
from tests.mcp.test_oauth_service_boundaries import _tokens


@pytest_asyncio.fixture
async def user_oauth_case(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[tuple[OAuthCase, UserService]]:
    """Use a disposable SQLite database and the real legacy token manager."""
    harness = await _build_harness(tmp_path)
    owner_id = str(uuid7())
    membership_id = await _seed_user_with_membership(harness.repository, user_public_id=owner_id)
    monkeypatch.setattr(UserService, "_instance", None)
    monkeypatch.setattr(UserService, "_initialized", False)
    monkeypatch.setattr("snapper.auth.user_service.get_repository", lambda _url: harness.repository)
    try:
        yield OAuthCase(
            harness, _principal(owner_id, membership_public_id=membership_id)
        ), UserService()
    finally:
        await harness.repository.engine.dispose()


async def _persisted_revocation(
    repository: SQLAlchemyRepository, grant_public_id: str
) -> tuple[datetime | None, list[datetime | None]]:
    """Read the committed grant and refresh-family revocation timestamps."""
    async with repository.session() as session:
        grant = await session.scalar(
            select(OAuthGrant).where(OAuthGrant.public_id == grant_public_id)
        )
        assert grant is not None
        refresh = await session.scalars(
            select(OAuthRefreshToken.revoked_at).where(
                OAuthRefreshToken.grant_public_id == grant_public_id
            )
        )
        return grant.revoked_at, list(refresh)


@pytest.mark.parametrize("path", ["deactivate", "update", "delete", "delegate"])
@pytest.mark.asyncio
async def test_account_authority_reduction_retires_grant_permanently(
    user_oauth_case: tuple[OAuthCase, UserService], path: str
) -> None:
    """Cover every supported account deactivation and subsequent reactivation.

    Given a live grant bound to an active owner and delegate.
    When either account is deactivated and then reactivated.
    Then the revoked grant and its old credentials remain invalid.
    """
    case, users = user_oauth_case
    oauth = case.harness.service
    tokens = await _tokens(case)
    verified = await oauth.verify_access_token(tokens.access_token)
    user = verified.principal if path == "delegate" else case.owner
    if path in {"deactivate", "delegate"}:
        assert await users.deactivate_user(user.user_public_id, reason=None)
    elif path == "update":
        assert (await users.update_user(user.username, is_active=False)) is not None
    else:
        assert await users.delete_user(user.username)
    grant_revoked_at, refresh_stamps = await _persisted_revocation(
        case.harness.repository, verified.grant_public_id
    )
    assert grant_revoked_at is not None and refresh_stamps == [grant_revoked_at]
    restored = await users.update_user(user.username, is_active=True)
    assert restored is not None and restored.is_active
    with pytest.raises(MCPOAuthError):
        await oauth.verify_access_token(tokens.access_token)
    assert tokens.refresh_token is not None
    with pytest.raises(MCPOAuthError):
        await oauth.exchange_refresh_token(_refresh(tokens.refresh_token))


@pytest.mark.asyncio
async def test_sqlite_deactivation_retires_login_between_credential_barriers(
    user_oauth_case: tuple[OAuthCase, UserService], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A login after the initial sweep cannot keep active token inventory.

    Given a user deactivation after its initial legacy session revocation.
    When that still-active account logs in before the final writer transaction.
    Then both late login tokens are durably revoked and stay unusable after reactivation.
    """
    case, users = user_oauth_case
    repository = case.harness.repository
    manager = case.harness.token_manager
    pairs: list[TokenPair] = []
    initial_sweep = users._sqlite_revoke_legacy_sessions

    async def login_after_initial_sweep(user_public_id: str) -> bool:
        """Perform the real login operations while the account is still active."""
        found = await initial_sweep(user_public_id)
        profile = await users.authenticate_user(case.owner.username, "fixture-password")
        assert profile is not None
        principal = await users.build_auth_principal(profile)
        pairs.append(manager.create_tokens(principal))
        await manager.persist_tokens(pairs[0], user_public_id, repository)
        assert len(await repository.list_active_user_token_jtis(user_public_id)) == 2
        return found

    monkeypatch.setattr(users, "_sqlite_revoke_legacy_sessions", login_after_initial_sweep)
    assert await users.deactivate_user(case.owner.user_public_id, reason=None)
    assert await repository.list_active_user_token_jtis(case.owner.user_public_id) == []
    restored = await users.update_user(case.owner.username, is_active=True)
    assert restored is not None and restored.is_active
    assert (
        await manager.verify_token_with_db(
            pairs[0].access_token, repository, expected_token_type=TOKEN_TYPE_ACCESS
        )
        is None
    )
    assert (
        await manager.verify_token_with_db(
            pairs[0].refresh_token, repository, expected_token_type=TOKEN_TYPE_REFRESH
        )
        is None
    )


async def _fail_after_revoke(
    session: AsyncSession, revoked_at: datetime, *grant_filters: ColumnElement[bool]
) -> None:
    """Simulate a failure after both grant and refresh writes, before commit."""
    await revoke_oauth_grants_in_tx(session, revoked_at, *grant_filters)
    raise RuntimeError("injected revocation failure")


@pytest.mark.asyncio
async def test_owner_disable_rolls_back_oauth_and_user_together(
    user_oauth_case: tuple[OAuthCase, UserService], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failure after writing OAuth state leaves original authority unchanged.

    Given an active owner and a usable grant with a refresh token.
    When deactivation fails after writing the OAuth revocation rows.
    Then the original authority and its credentials remain usable after rollback.
    """
    case, users = user_oauth_case
    oauth = case.harness.service
    tokens = await _tokens(case)
    verified = await oauth.verify_access_token(tokens.access_token)
    monkeypatch.setattr("snapper.auth.user_service.revoke_oauth_grants_in_tx", _fail_after_revoke)
    with pytest.raises(RuntimeError, match="injected revocation failure"):
        await users.update_user(case.owner.username, is_active=False)
    assert await _persisted_revocation(case.harness.repository, verified.grant_public_id) == (
        None,
        [None],
    )
    assert (
        await oauth.verify_access_token(tokens.access_token)
    ).grant_public_id == verified.grant_public_id


async def _install_desks(case: OAuthCase) -> OAuthCase:
    """Leave the selected owner in two active desks for grant-isolation checks."""
    now = datetime.now(UTC) - timedelta(minutes=1)
    membership_id = str(uuid7())
    async with case.harness.repository.session() as session:
        session.add_all(
            [
                Operator(
                    public_id=_OPERATOR_A,
                    label="desk-a",
                    timestamp=now,
                    session_id="fixtures",
                    sequence_id=1,
                ),
                Operator(
                    public_id=_OPERATOR_B,
                    label="desk-b",
                    timestamp=now,
                    session_id="fixtures",
                    sequence_id=2,
                ),
                UserOperatorMembership(
                    public_id=membership_id,
                    user_public_id=case.owner.user_public_id,
                    operator_public_id=_OPERATOR_B,
                    is_primary=False,
                    timestamp=now,
                    session_id="fixtures",
                    sequence_id=3,
                ),
            ]
        )
        await session.commit()
    return OAuthCase(
        case.harness,
        _principal(
            case.owner.user_public_id,
            operator_public_id=_OPERATOR_B,
            membership_public_id=membership_id,
        ),
    )


async def _no_legacy_revoke(_user_public_id: str) -> None:
    """Accept the legacy credential callback separately from OAuth mutation."""


@pytest.mark.parametrize("rollback", [False, True])
@pytest.mark.asyncio
async def test_desk_detach_retires_only_its_grant_in_the_membership_transaction(
    user_oauth_case: tuple[OAuthCase, UserService], monkeypatch: pytest.MonkeyPatch, rollback: bool
) -> None:
    """Detach and reattach with two grants, testing rollback and exact operator scope.

    Given one owner with separate grants for two operators.
    When the selected membership is detached or its transaction fails.
    Then only a committed detachment permanently revokes the selected grant.
    """
    case, users = user_oauth_case
    sibling = await _install_desks(case)
    repository = case.harness.repository
    oauth = case.harness.service
    old_tokens = await _tokens(case)
    sibling_tokens = await _tokens(sibling)
    selected = await oauth.verify_access_token(old_tokens.access_token)
    unaffected = await oauth.verify_access_token(sibling_tokens.access_token)
    username = case.owner.username
    assert await users.update_user(username, role=UserRole.VIEWER)
    detach = DeskMembershipDetach(username, _OPERATOR_A, datetime.now(UTC), "fixtures", 4)
    if rollback:
        monkeypatch.setattr("snapper.data.repository.revoke_oauth_grants_in_tx", _fail_after_revoke)
        with pytest.raises(RuntimeError, match="injected revocation failure"):
            await repository.detach_viewer_from_desk(detach, _no_legacy_revoke)
    else:
        assert await repository.detach_viewer_from_desk(detach, _no_legacy_revoke)
        reattach = DeskMembershipAttach(username, _OPERATOR_A, datetime.now(UTC), "fixtures", 5)
        await repository.attach_viewer_to_desk(reattach)
    assert await users.update_user(username, role=UserRole.OPERATOR)
    assert await _persisted_revocation(repository, unaffected.grant_public_id) == (None, [None])
    assert (
        await oauth.verify_access_token(sibling_tokens.access_token)
    ).grant_public_id == unaffected.grant_public_id
    grant_revoked_at, refresh_stamps = await _persisted_revocation(
        repository, selected.grant_public_id
    )
    if rollback:
        assert grant_revoked_at is None and refresh_stamps == [None]
        assert (
            await oauth.verify_access_token(old_tokens.access_token)
        ).grant_public_id == selected.grant_public_id
        return
    assert grant_revoked_at is not None and refresh_stamps == [grant_revoked_at]
    with pytest.raises(MCPOAuthError):
        await oauth.verify_access_token(old_tokens.access_token)
    assert old_tokens.refresh_token is not None
    with pytest.raises(MCPOAuthError):
        await oauth.exchange_refresh_token(_refresh(old_tokens.refresh_token))
