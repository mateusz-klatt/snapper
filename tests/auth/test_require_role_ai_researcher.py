"""AI_RESEARCHER role hierarchy, permission, and token-ceiling tests."""

from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import require_role
from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.permissions import get_effective_permissions
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.websocket_auth import WebSocketAuthManager


def _principal(
    role: UserRole,
    permissions: list[str] | None = None,
) -> AuthPrincipal:
    """Build a minimal principal for researcher authorization tests.

    Args:
        role: Role assigned to the principal.
        permissions: Optional permission strings carried by the token.

    Returns:
        Principal with the requested role and token permission claim.
    """
    return AuthPrincipal(
        username="researcher-probe",
        role=role,
        permissions=permissions,
        is_active=True,
    )


def test_ai_researcher_has_exact_permission_set() -> None:
    """Verify the researcher role exposes only research-plane permissions.

    Given: The canonical role-permission mapping.
    When: The AI_RESEARCHER grant is inspected.
    Then: It contains exactly market data, market-view read, and market-view submit.
    """
    assert ROLE_PERMISSIONS[UserRole.AI_RESEARCHER] == {
        Permission.READ_MARKET_DATA,
        Permission.READ_MARKET_VIEWS,
        Permission.SUBMIT_MARKET_VIEW,
    }


def test_ai_researcher_lacks_trading_intent_and_status_permissions() -> None:
    """Verify hostile research context has no trading-intent authority.

    Given: Permissions that expose or act on trading intent and system status.
    When: They are compared with the AI_RESEARCHER role grant.
    Then: Every forbidden permission is absent.
    """
    forbidden = {
        Permission.READ_ORDERS,
        Permission.CREATE_ORDERS,
        Permission.CANCEL_ORDERS,
        Permission.READ_POSITIONS,
        Permission.MANAGE_POSITIONS,
        Permission.READ_SIGNALS,
        Permission.READ_SYSTEM_STATUS,
    }
    assert forbidden.isdisjoint(ROLE_PERMISSIONS[UserRole.AI_RESEARCHER])


def test_ai_researcher_narrow_token_retains_selected_permission() -> None:
    """Verify a narrowed researcher token remains usable for its retained action.

    Given: An AI_RESEARCHER token retaining only SUBMIT_MARKET_VIEW.
    When: The REST permission dependency evaluates the token.
    Then: The researcher principal is admitted for that retained permission.
    """
    principal = _principal(
        UserRole.AI_RESEARCHER,
        permissions=[Permission.SUBMIT_MARKET_VIEW.value],
    )
    checker = require_permission(Permission.SUBMIT_MARKET_VIEW)

    assert checker(current_user=principal) is principal


def test_ai_researcher_token_cannot_expand_beyond_role_ceiling() -> None:
    """Verify token claims cannot grant trading permissions missing from the role.

    Given: A researcher token claiming one valid permission plus order and signal grants.
    When: Effective permissions are derived from the role ceiling.
    Then: Only the valid researcher permission survives the intersection.
    """
    effective = get_effective_permissions(
        UserRole.AI_RESEARCHER,
        [
            Permission.READ_MARKET_VIEWS.value,
            Permission.CREATE_ORDERS.value,
            Permission.READ_SIGNALS.value,
        ],
    )

    assert effective == {Permission.READ_MARKET_VIEWS}


def test_require_role_handles_ai_researcher_without_elevation() -> None:
    """Verify the HTTP hierarchy recognizes and confines the researcher role.

    Given: An AI_RESEARCHER principal and role guards for researcher and delegate.
    When: Both hierarchy comparisons are evaluated.
    Then: Its own guard passes while the AI_DELEGATE guard returns HTTP 403.
    """
    principal = _principal(UserRole.AI_RESEARCHER)

    assert require_role(UserRole.AI_RESEARCHER)(current_user=principal) is principal
    with pytest.raises(HTTPException) as exc_info:
        require_role(UserRole.AI_DELEGATE)(current_user=principal)
    assert exc_info.value.status_code == 403


def test_websocket_hierarchy_handles_ai_researcher_without_elevation() -> None:
    """Verify the WebSocket hierarchy recognizes and confines the researcher role.

    Given: An authenticated AI_RESEARCHER WebSocket connection.
    When: Researcher, delegate, and viewer role levels are checked.
    Then: Only the researcher's own level is satisfied and no KeyError occurs.
    """
    WebSocketAuthManager.clear_instance()
    manager = WebSocketAuthManager()
    websocket = MagicMock()
    manager.authenticated_connections[websocket] = _principal(UserRole.AI_RESEARCHER)
    try:
        assert manager.has_permission(websocket, UserRole.AI_RESEARCHER) is True
        assert manager.has_permission(websocket, UserRole.AI_DELEGATE) is False
        assert manager.has_permission(websocket, UserRole.VIEWER) is False
    finally:
        WebSocketAuthManager.clear_instance()
