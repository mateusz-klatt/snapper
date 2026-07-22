"""Review-only authorization role, compatibility, and token-scope tests."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from unittest.mock import MagicMock

import jwt
import pytest
from fastapi import HTTPException

from snapper.auth.dependencies import require_role
from snapper.auth.domain.permissions import CATEGORY_PERMISSIONS
from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.permissions import get_effective_permissions
from snapper.auth.domain.permissions import is_ai_review_decision_capable
from snapper.auth.domain.roles import AI_REVIEW_PRINCIPAL_ROLES
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.tokens import PERMISSION_SCOPE_VERSION
from snapper.auth.tokens import TokenManager
from snapper.auth.websocket_auth import WebSocketAuthManager


def test_ai_reviewer_role_and_grant_are_exact() -> None:
    """Verify the review-only identity and its complete grant.

    Given: The canonical role enum, review-principal set, and role grants.
    When: The AI_REVIEWER definitions are inspected.
    Then: The role value, operational role set, and decision-capable read grant are exact.
    """
    assert UserRole.AI_REVIEWER.value == "ai_reviewer"
    assert {
        UserRole.AI_REVIEWER,
        UserRole.AI_DELEGATE,
    } == AI_REVIEW_PRINCIPAL_ROLES
    assert ROLE_PERMISSIONS[UserRole.AI_REVIEWER] == {
        Permission.READ_MARKET_DATA,
        Permission.READ_MARKET_VIEWS,
        Permission.READ_ORDERS,
        Permission.READ_POSITIONS,
        Permission.READ_STRATEGIES,
        Permission.READ_SIGNALS,
        Permission.READ_SYSTEM_STATUS,
        Permission.READ_BACKTESTS,
        Permission.SUBMIT_AI_REVIEW_DECISION,
    }


def test_ai_reviewer_lacks_forbidden_mutation_permissions() -> None:
    """Verify the review-only role cannot research or trade.

    Given: The four mutation permissions forbidden to AI_REVIEWER.
    When: They are compared with the role's effective full grant.
    Then: Submit-market-view, order creation/cancellation, and position management are absent.
    """
    forbidden = {
        Permission.SUBMIT_MARKET_VIEW,
        Permission.CREATE_ORDERS,
        Permission.CANCEL_ORDERS,
        Permission.MANAGE_POSITIONS,
    }

    assert forbidden.isdisjoint(ROLE_PERMISSIONS[UserRole.AI_REVIEWER])


def test_ai_reviews_websocket_category_keeps_live_create_orders_gate() -> None:
    """Verify D4a does not cut over the live WebSocket category gate.

    Given: The category permission map used by current WebSocket subscriptions.
    When: The ai_reviews category requirements are inspected.
    Then: READ_SIGNALS and CREATE_ORDERS remain the exact live requirements.
    """
    assert CATEGORY_PERMISSIONS["ai_reviews"] == {
        Permission.READ_SIGNALS,
        Permission.CREATE_ORDERS,
    }


def test_ai_delegate_holds_ai_review_decision_permission() -> None:
    """Verify elevated roles retain review-decision authority.

    Given: The canonical AI_DELEGATE and ADMIN permission grants.
    When: Their decision permission is queried.
    Then: SUBMIT_AI_REVIEW_DECISION is present in both elevated profiles.
    """
    assert Permission.SUBMIT_AI_REVIEW_DECISION in ROLE_PERMISSIONS[UserRole.AI_DELEGATE]
    assert Permission.SUBMIT_AI_REVIEW_DECISION in ROLE_PERMISSIONS[UserRole.ADMIN]


def test_http_role_hierarchy_recognizes_ai_reviewer_without_elevation() -> None:
    """Verify the HTTP role hierarchy handles the review-only role safely.

    Given: An AI_REVIEWER principal and reviewer and viewer minimum-role guards.
    When: Both role comparisons are evaluated.
    Then: The reviewer tier passes itself and remains below VIEWER.
    """
    principal = AuthPrincipal(username="reviewer-http", role=UserRole.AI_REVIEWER)

    assert require_role(UserRole.AI_REVIEWER)(current_user=principal) is principal
    with pytest.raises(HTTPException) as exc_info:
        require_role(UserRole.VIEWER)(current_user=principal)
    assert exc_info.value.status_code == 403


def test_websocket_role_hierarchy_recognizes_ai_reviewer_without_elevation() -> None:
    """Verify the WebSocket role hierarchy handles the review-only role safely.

    Given: An authenticated AI_REVIEWER WebSocket connection.
    When: Reviewer and viewer role levels are checked.
    Then: The reviewer tier passes itself and remains below VIEWER without a lookup error.
    """
    WebSocketAuthManager.clear_instance()
    manager = WebSocketAuthManager()
    websocket = MagicMock()
    manager.authenticated_connections[websocket] = AuthPrincipal(
        username="reviewer-ws",
        role=UserRole.AI_REVIEWER,
    )
    try:
        assert manager.has_permission(websocket, UserRole.AI_REVIEWER) is True
        assert manager.has_permission(websocket, UserRole.VIEWER) is False
    finally:
        WebSocketAuthManager.clear_instance()


@pytest.mark.parametrize(
    ("role", "token_permissions", "permission_scope_version", "expected"),
    [
        pytest.param(
            UserRole.AI_REVIEWER,
            [Permission.SUBMIT_AI_REVIEW_DECISION.value],
            2,
            True,
            id="v2-reviewer",
        ),
        pytest.param(
            UserRole.AI_DELEGATE,
            [Permission.SUBMIT_AI_REVIEW_DECISION.value],
            2,
            True,
            id="v2-delegate",
        ),
        pytest.param(
            UserRole.AI_DELEGATE,
            [Permission.CREATE_ORDERS.value],
            1,
            True,
            id="v1-historical-delegate",
        ),
        pytest.param(
            UserRole.AI_DELEGATE,
            [Permission.CREATE_ORDERS.value],
            2,
            False,
            id="v2-create-only",
        ),
        pytest.param(
            UserRole.AI_DELEGATE,
            [Permission.READ_MARKET_DATA.value],
            1,
            False,
            id="v1-without-create",
        ),
        pytest.param(
            UserRole.AI_RESEARCHER,
            None,
            2,
            False,
            id="researcher",
        ),
        pytest.param(
            UserRole.VIEWER,
            None,
            2,
            False,
            id="viewer",
        ),
    ],
)
def test_ai_review_decision_capability_projection(
    role: UserRole,
    token_permissions: list[str] | None,
    permission_scope_version: int | None,
    expected: bool,
) -> None:
    """Verify every new and compatibility decision-capability branch.

    Given: A role-bounded token scope from the v2 or historical v1 model.
    When: The centralized AI-review decision projector evaluates it.
    Then: Only the new grant or the exact v1 delegate CREATE_ORDERS rule admits it.
    """
    assert (
        is_ai_review_decision_capable(
            role,
            token_permissions,
            permission_scope_version,
        )
        is expected
    )


def test_new_tokens_stamp_literal_v2_permission_scope() -> None:
    """Verify every token minting shape emits scope version two.

    Given: An AI_REVIEWER principal and the rotating and long-lived mint paths.
    When: Access, refresh, and delegate-style long-lived tokens are decoded.
    Then: Each token carries the literal permission scope version two.
    """
    token_manager = TokenManager()
    principal = AuthPrincipal(
        username="reviewer-v2",
        role=UserRole.AI_REVIEWER,
        user_public_id="reviewer-v2-user",
    )

    token_pair = token_manager.create_tokens(principal)
    long_lived = token_manager.create_delegate_access_token(
        principal,
        datetime.now(UTC),
    )
    access_claims = token_manager.decode_fresh_token(token_pair.access_token)
    refresh_claims = token_manager.decode_fresh_token(token_pair.refresh_token)
    long_lived_claims = token_manager.decode_fresh_token(long_lived.access_token)

    assert PERMISSION_SCOPE_VERSION == 2
    assert access_claims.permission_scope_version == 2
    assert refresh_claims.permission_scope_version == 2
    assert long_lived_claims.permission_scope_version == 2


def test_claimless_ai_delegate_token_resolves_full_current_role() -> None:
    """Verify claim-less historical delegate tokens retain full-role semantics.

    Given: A valid historical AI_DELEGATE JWT without permission or scope-version claims.
    When: The token is verified and its effective permissions are projected.
    Then: The claims remain absent and the complete current AI_DELEGATE grant is restored.
    """
    token_manager = TokenManager()
    now = datetime.now(UTC)
    payload: dict[str, str | int] = {
        "sub": "historical-delegate",
        "username": "historical-delegate",
        "role": UserRole.AI_DELEGATE.value,
        "sid": "historical-session",
        "exp": int((now + timedelta(hours=1)).timestamp()),
        "iat": int(now.timestamp()),
        "jti": "historical-delegate-jti",
    }
    token = jwt.encode(
        payload,
        token_manager.settings.auth_secret_key,
        algorithm=token_manager.settings.auth_algorithm,
    )

    claims = token_manager.verify_token(token)

    assert claims is not None
    assert claims.permissions is None
    assert claims.permission_scope_version is None
    assert (
        get_effective_permissions(claims.role, claims.permissions)
        == ROLE_PERMISSIONS[UserRole.AI_DELEGATE]
    )
