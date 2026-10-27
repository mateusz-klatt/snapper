"""Desk-directory observation checks preserve live authority and mutation isolation."""

from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.routes import _apply_wallet_hint
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.requests import RefreshTokenPayload
from snapper.auth.user_service import DeskMembershipAuthorizationError
from snapper.auth.user_service import UserService

_WALLET = "01910000-0000-7000-8000-000000000001"


def _principal() -> AuthPrincipal:
    """Return a current auditor with no operator membership or execution authority."""
    return AuthPrincipal(
        username="auditor",
        role=UserRole.AUDITOR,
        user_public_id="auditor-user",
        permission_scope_version=4,
    )


def _service(live_role: str | None = "auditor") -> tuple[UserService, MagicMock]:
    """Build an isolated service with a controlled live identity lookup."""
    session = AsyncMock()
    session.scalar.return_value = live_role
    repo = MagicMock()
    repo.session.return_value.__aenter__.return_value = session
    repo.list_human_desk_members = AsyncMock(return_value=[])
    repo.get_user_operator_memberships = AsyncMock(return_value=[])
    service = object.__new__(UserService)
    service.repository = repo
    return service, repo


@pytest.mark.asyncio
async def test_auditor_reads_foreign_desk_without_operator_memberships() -> None:
    """Global observation does not require or invent execution memberships.

    Given: An active auditor without operator memberships.
    When: The directory of an arbitrary desk is read.
    Then: The read succeeds without consulting memberships or mutating the repository.
    """
    service, repo = _service()
    principal = _principal()
    assert await service.list_desk_members(principal, "foreign-desk") == []
    assert principal.operator_public_ids == []
    repo.list_human_desk_members.assert_awaited_once()
    repo.get_user_operator_memberships.assert_not_awaited()


@pytest.mark.parametrize("live_role", [None, "unknown-role", "viewer"])
@pytest.mark.asyncio
async def test_auditor_desk_read_rechecks_current_identity(live_role: str | None) -> None:
    """Stale auditor tokens do not retain a revoked live directory capability.

    Given: A token whose live user is absent, malformed or downgraded to viewer.
    When: The caller requests a desk directory.
    Then: Authorization fails before any directory data is loaded.
    """
    service, repo = _service(live_role)
    with pytest.raises(DeskMembershipAuthorizationError):
        await service.list_desk_members(_principal(), "foreign-desk")
    repo.list_human_desk_members.assert_not_awaited()
    repo.get_user_operator_memberships.assert_not_awaited()


@pytest.mark.asyncio
async def test_auditor_desk_read_requires_stable_user_identity() -> None:
    """A globally readable token still requires an active identity.

    Given: An auditor token without its durable user identifier.
    When: A desk read is requested.
    Then: The service rejects it before reading the database.
    """
    service, repo = _service()
    principal = _principal().model_copy(update={"user_public_id": ""})
    with pytest.raises(DeskMembershipAuthorizationError, match="Active caller identity"):
        await service.list_desk_members(principal, "desk")
    assert repo.mock_calls == []


@pytest.mark.asyncio
async def test_live_role_must_still_grant_global_desk_observation() -> None:
    """A current desk operator cannot retain former global auditor visibility.

    Given: An auditor token whose live role is now a desk-scoped operator.
    When: A directory outside the token's memberships is requested.
    Then: The old global read capability cannot bypass the live scope.
    """
    service, repo = _service("operator")
    with pytest.raises(DeskMembershipAuthorizationError, match="token scope"):
        await service.list_desk_members(_principal(), "foreign-desk")
    repo.list_human_desk_members.assert_not_awaited()


@pytest.mark.parametrize("live_membership", [False, True])
@pytest.mark.asyncio
async def test_narrowed_desk_reader_needs_current_membership(live_membership: bool) -> None:
    """Removing global observation leaves both token and live membership checks intact.

    Given: An auditor token narrowed to directory read and one named desk.
    When: That desk is read with or without a current database membership.
    Then: Only the current membership permits the directory read.
    """
    service, repo = _service()
    principal = _principal().model_copy(
        update={
            "permissions": [Permission.READ_DESK_MEMBERSHIPS.value],
            "operator_public_ids": ["desk"],
        }
    )
    repo.get_user_operator_memberships.return_value = (
        [{"operator_public_id": "desk"}] if live_membership else []
    )
    if live_membership:
        assert await service.list_desk_members(principal, "desk") == []
        repo.list_human_desk_members.assert_awaited_once()
    else:
        with pytest.raises(DeskMembershipAuthorizationError, match="Current membership"):
            await service.list_desk_members(principal, "desk")
        repo.list_human_desk_members.assert_not_awaited()


@pytest.mark.asyncio
async def test_narrowed_auditor_cannot_read_without_directory_permission() -> None:
    """Global observation alone does not replace the directory content permission.

    Given: An auditor token narrowed to global scope without directory read.
    When: The service receives a direct directory request.
    Then: Neither the read path nor the existing management path authorizes it.
    """
    service, repo = _service()
    principal = _principal().model_copy(update={"permissions": [Permission.READ_ALL_SCOPES.value]})
    with pytest.raises(DeskMembershipAuthorizationError, match="permission"):
        await service.list_desk_members(principal, "desk")
    assert repo.mock_calls == []


@pytest.mark.asyncio
async def test_auditor_cannot_attach_or_detach_members_through_service() -> None:
    """The new read authorization is absent from both membership mutation paths.

    Given: A fully authorized auditor reader.
    When: A direct caller attempts membership attachment and detachment.
    Then: Both fail before any repository operation.
    """
    service, repo = _service()
    with pytest.raises(DeskMembershipAuthorizationError, match="permission"):
        await service.attach_viewer_to_desk(_principal(), "desk", "viewer")
    with pytest.raises(DeskMembershipAuthorizationError, match="permission"):
        await service.detach_viewer_from_desk(_principal(), "desk", "viewer")
    assert repo.mock_calls == []


@pytest.mark.asyncio
async def test_auditor_refresh_selects_wallet_without_execution_membership() -> None:
    """Wallet selection grants a display context without manufacturing trading scope.

    Given: An auditor with no operator memberships and one active wallet.
    When: Refresh applies the selected wallet hint.
    Then: Global read visibility accepts the wallet and operator memberships stay empty.
    """
    repo = MagicMock()
    repo.list_active_wallets = AsyncMock(return_value=[{"public_id": _WALLET}])
    repo.list_readable_wallets_for_user = AsyncMock()
    result = await _apply_wallet_hint(
        RefreshTokenPayload(active_wallet_public_id=_WALLET), _principal(), repo
    )
    assert result.active_wallet_public_id == _WALLET
    assert result.operator_public_ids == []
    repo.list_active_wallets.assert_awaited_once()
    repo.list_readable_wallets_for_user.assert_not_awaited()
