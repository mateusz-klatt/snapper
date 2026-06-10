"""AI_DELEGATE role hierarchy + permission binding tests.

Covers the canonical guarantees:

    - ``UserRole.AI_DELEGATE`` is ordinally **below** VIEWER in BOTH
      ``role_hierarchy`` dicts (``auth/dependencies.py`` and
      ``auth/websocket_auth.py``) so any ``require_role(>= VIEWER)``
      guard rejects AI_DELEGATE by numeric comparison.
    - ``ROLE_PERMISSIONS[AI_DELEGATE]`` grants the narrow set of
      read + create/cancel-order + signal permissions required by
      the MCP surface — no strategy lifecycle, no admin, no
      wallet/user management.
    - ``require_permission(perm)`` is the authoritative gate for
      AI_DELEGATE access (hierarchy gate can only reject).
    - Adding ``AI_DELEGATE`` does not regress existing
      VIEWER/OPERATOR/ADMIN ordinal comparisons.

Gate: ``make check-all``. A future PR that drops AI_DELEGATE from
either ``role_hierarchy`` dict or flips it to ordinal ``>= VIEWER``
fails this file immediately.
"""

from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import require_role
from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.websocket_auth import WebSocketAuthManager


def _principal(role: UserRole) -> AuthPrincipal:
    """Return a minimal AuthPrincipal for role-gate testing."""
    return AuthPrincipal(username="probe", role=role, is_active=True)


def test_ai_delegate_rejected_by_require_role_viewer() -> None:
    """AI_DELEGATE must fail ``require_role(VIEWER)`` by ordinal comparison.

    Given: an AuthPrincipal with role AI_DELEGATE,
    When: the ``require_role(VIEWER)`` FastAPI dependency is invoked,
    Then: it raises HTTP 403 — AI_DELEGATE sits at ordinal ``-1`` in
        the role_hierarchy dict and VIEWER at ``0``, so the numeric
        guard rejects before any permission check runs.
    """
    checker = require_role(UserRole.VIEWER)
    with pytest.raises(HTTPException) as exc:
        checker(current_user=_principal(UserRole.AI_DELEGATE))
    assert exc.value.status_code == 403


def test_ai_delegate_rejected_by_require_role_operator() -> None:
    """AI_DELEGATE must fail ``require_role(OPERATOR)`` (stricter gate).

    Given: an AI_DELEGATE principal,
    When: ``require_role(OPERATOR)`` runs,
    Then: 403 is raised — confirms the rejection is not a VIEWER-only
        edge case but uniform across the hierarchy.
    """
    checker = require_role(UserRole.OPERATOR)
    with pytest.raises(HTTPException) as exc:
        checker(current_user=_principal(UserRole.AI_DELEGATE))
    assert exc.value.status_code == 403


def test_ai_delegate_rejected_by_require_role_admin() -> None:
    """AI_DELEGATE must fail ``require_role(ADMIN)`` at the top of the hierarchy.

    Given: an AI_DELEGATE principal,
    When: ``require_role(ADMIN)`` runs,
    Then: 403 is raised — confirms the dict lookup covers all three
        existing tiers.
    """
    checker = require_role(UserRole.ADMIN)
    with pytest.raises(HTTPException) as exc:
        checker(current_user=_principal(UserRole.AI_DELEGATE))
    assert exc.value.status_code == 403


def test_viewer_still_passes_require_role_viewer() -> None:
    """Regression: adding AI_DELEGATE must not break VIEWER's own gate.

    Given: a VIEWER principal,
    When: ``require_role(VIEWER)`` runs,
    Then: the principal is returned unchanged — the hierarchy
        comparison stays non-regressive for existing tiers.
    """
    checker = require_role(UserRole.VIEWER)
    assert checker(current_user=_principal(UserRole.VIEWER)).role == UserRole.VIEWER


def test_operator_still_passes_require_role_viewer() -> None:
    """Regression: OPERATOR still satisfies the VIEWER gate.

    Given: an OPERATOR principal,
    When: ``require_role(VIEWER)`` runs,
    Then: the principal passes — confirms the numeric comparison
        still recognizes higher-tier roles as satisfying a lower
        minimum.
    """
    checker = require_role(UserRole.VIEWER)
    assert checker(current_user=_principal(UserRole.OPERATOR)).role == UserRole.OPERATOR


def test_admin_still_passes_require_role_admin() -> None:
    """Regression: ADMIN still satisfies its own gate.

    Given: an ADMIN principal,
    When: ``require_role(ADMIN)`` runs,
    Then: the principal passes — confirms the highest tier is
        unaffected by the AI_DELEGATE insertion at the bottom.
    """
    checker = require_role(UserRole.ADMIN)
    assert checker(current_user=_principal(UserRole.ADMIN)).role == UserRole.ADMIN


def test_ai_delegate_has_canonical_permission_set() -> None:
    """``ROLE_PERMISSIONS[AI_DELEGATE]`` matches the canonical inclusion list.

    Given: the ROLE_PERMISSIONS mapping,
    When: the AI_DELEGATE entry is inspected,
    Then: it contains exactly the canonical permissions —
        read-only observability plus create/cancel-orders plus
        READ_SIGNALS — and excludes strategy lifecycle + admin.
    """
    expected: set[Permission] = {
        Permission.READ_MARKET_DATA,
        Permission.READ_ORDERS,
        Permission.CREATE_ORDERS,
        Permission.CANCEL_ORDERS,
        Permission.READ_POSITIONS,
        Permission.MANAGE_POSITIONS,
        Permission.READ_STRATEGIES,
        Permission.READ_SIGNALS,
        Permission.READ_SYSTEM_STATUS,
        Permission.READ_BACKTESTS,
    }
    assert ROLE_PERMISSIONS[UserRole.AI_DELEGATE] == expected


def test_ai_delegate_lacks_strategy_lifecycle_permissions() -> None:
    """AI_DELEGATE must NOT hold strategy lifecycle permissions.

    Given: the AI_DELEGATE permission set,
    When: START/STOP/CONFIGURE_STRATEGIES + MANAGE_PROCESSES are
        checked,
    Then: none of them are present — the AI surface observes
        signals, it does not run strategies.
    """
    forbidden = {
        Permission.START_STRATEGIES,
        Permission.STOP_STRATEGIES,
        Permission.CONFIGURE_STRATEGIES,
        Permission.MANAGE_PROCESSES,
    }
    assert forbidden.isdisjoint(ROLE_PERMISSIONS[UserRole.AI_DELEGATE])


def test_ai_delegate_lacks_admin_permissions() -> None:
    """AI_DELEGATE must NOT hold user/wallet/system/admin permissions.

    Given: the AI_DELEGATE permission set,
    When: MANAGE_USERS / CONFIGURE_SYSTEM / credential / scope /
        impersonation permissions are checked,
    Then: none of them are present — the AI surface is narrow by
        design.
    """
    forbidden = {
        Permission.CONFIGURE_SYSTEM,
        Permission.MANAGE_USERS,
        Permission.READ_WALLET_CREDENTIALS,
        Permission.MANAGE_WALLET_CREDENTIALS,
        Permission.MANAGE_SCOPE_GRANTS,
        Permission.IMPERSONATE_OPERATOR,
        Permission.MANAGE_BACKTESTS,
    }
    assert forbidden.isdisjoint(ROLE_PERMISSIONS[UserRole.AI_DELEGATE])


def test_ai_delegate_passes_require_permission_for_create_orders() -> None:
    """``require_permission`` is the authoritative gate for AI_DELEGATE.

    Given: an AI_DELEGATE principal and the CREATE_ORDERS permission,
    When: ``require_permission(CREATE_ORDERS)`` runs,
    Then: the principal is returned — the hierarchy gate cannot grant
        access but the permission gate can (and does for the
        narrow whitelisted set).
    """
    checker = require_permission(Permission.CREATE_ORDERS)
    result = checker(current_user=_principal(UserRole.AI_DELEGATE))
    assert result.role == UserRole.AI_DELEGATE


def test_ai_delegate_fails_require_permission_for_manage_users() -> None:
    """AI_DELEGATE is denied permissions outside the whitelist.

    Given: an AI_DELEGATE principal,
    When: ``require_permission(MANAGE_USERS)`` runs,
    Then: 403 is raised — the permission check consults
        ROLE_PERMISSIONS[AI_DELEGATE] (not the hierarchy dict) and
        finds MANAGE_USERS absent.
    """
    checker = require_permission(Permission.MANAGE_USERS)
    with pytest.raises(HTTPException) as exc:
        checker(current_user=_principal(UserRole.AI_DELEGATE))
    assert exc.value.status_code == 403


def test_ws_has_permission_rejects_ai_delegate_for_viewer() -> None:
    """``WebSocketAuthManager.has_permission()`` rejects AI_DELEGATE.

    Given: a registered AI_DELEGATE WebSocket connection using the
        WebSocket role hierarchy,
    When: ``has_permission(ws, VIEWER)`` runs,
    Then: it returns False — the dict must be updated in lockstep
        with ``dependencies.py`` or a ``KeyError`` would fire at
        runtime. This test guards against future drift between
        the two dicts.
    """
    WebSocketAuthManager.clear_instance()
    mgr = WebSocketAuthManager()
    ws = MagicMock()
    mgr.authenticated_connections[ws] = _principal(UserRole.AI_DELEGATE)
    try:
        assert mgr.has_permission(ws, UserRole.VIEWER) is False
        assert mgr.has_permission(ws, UserRole.OPERATOR) is False
    finally:
        WebSocketAuthManager.clear_instance()


def test_ws_has_permission_accepts_operator_for_viewer_regression() -> None:
    """Regression: OPERATOR still clears the WS VIEWER gate post-change.

    Given: a registered OPERATOR connection,
    When: ``has_permission(ws, VIEWER)`` runs,
    Then: True is returned — ensures the AI_DELEGATE insertion did
        not shift existing ordinals (OPERATOR stays above VIEWER).
    """
    WebSocketAuthManager.clear_instance()
    mgr = WebSocketAuthManager()
    ws = MagicMock()
    mgr.authenticated_connections[ws] = _principal(UserRole.OPERATOR)
    try:
        assert mgr.has_permission(ws, UserRole.VIEWER) is True
    finally:
        WebSocketAuthManager.clear_instance()
