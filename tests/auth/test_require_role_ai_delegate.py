"""AI_DELEGATE permission-set and token-binding tests.

Covers the canonical guarantees:

    - ``ROLE_PERMISSIONS[AI_DELEGATE]`` grants the narrow set of
      read + create/cancel-order + signal permissions required by
      the MCP surface — no strategy lifecycle, no admin, no
      wallet/user management.
    - ``require_permission(perm)`` is the authoritative gate for
      AI_DELEGATE access, including token-scope narrowing.
"""

import pytest
from fastapi import HTTPException

from snapper.auth.dependencies import require_permission
from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal


def _principal(
    role: UserRole,
    permissions: list[str] | None = None,
) -> AuthPrincipal:
    """Return a minimal principal with an optional token permission scope."""
    return AuthPrincipal(
        username="probe",
        role=role,
        permissions=permissions,
        is_active=True,
    )


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
        Permission.READ_MARKET_VIEWS,
        Permission.READ_ORDERS,
        Permission.CREATE_ORDERS,
        Permission.CANCEL_ORDERS,
        Permission.READ_POSITIONS,
        Permission.MANAGE_POSITIONS,
        Permission.READ_STRATEGIES,
        Permission.READ_SIGNALS,
        Permission.READ_SYSTEM_STATUS,
        Permission.MANAGE_RUNTIME_DIAGNOSTICS,
        Permission.READ_BACKTESTS,
        Permission.CREATE_BACKTEST_COMPARISONS,
        Permission.SUBMIT_AI_REVIEW_DECISION,
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
    Then: the principal is returned because its named permission set
        contains the required capability.
    """
    checker = require_permission(Permission.CREATE_ORDERS)
    result = checker(current_user=_principal(UserRole.AI_DELEGATE))
    assert result.role == UserRole.AI_DELEGATE


def test_ai_delegate_fails_require_permission_for_manage_users() -> None:
    """AI_DELEGATE is denied permissions outside the whitelist.

    Given: an AI_DELEGATE principal,
    When: ``require_permission(MANAGE_USERS)`` runs,
    Then: 403 is raised — the permission check consults
        ROLE_PERMISSIONS[AI_DELEGATE] and finds MANAGE_USERS absent.
    """
    checker = require_permission(Permission.MANAGE_USERS)
    delegate_principal = _principal(UserRole.AI_DELEGATE)
    with pytest.raises(HTTPException) as exc:
        checker(current_user=delegate_principal)
    assert exc.value.status_code == 403


def test_ai_delegate_narrow_token_cannot_restore_omitted_order_permission() -> None:
    """An explicit token scope can narrow the AI delegate permission set.

    Given: An AI_DELEGATE token retaining only READ_MARKET_DATA.
    When: The REST dependency requires CREATE_ORDERS.
    Then: The omitted mutation permission remains denied with HTTP 403.
    """
    checker = require_permission(Permission.CREATE_ORDERS)
    principal = _principal(
        UserRole.AI_DELEGATE,
        [Permission.READ_MARKET_DATA.value],
    )

    with pytest.raises(HTTPException) as exc:
        checker(current_user=principal)

    assert exc.value.status_code == 403
