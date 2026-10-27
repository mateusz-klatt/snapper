"""HTTP proofs that administrative observation grants cannot perform writes."""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel
from pydantic import ValidationError

from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.routes import router as auth_router
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.requests import LoginRequest
from snapper.auth.schemas.requests import RefreshTokenRequest
from snapper.auth.schemas.user import UserProfile
from snapper.config.settings_routes import router as settings_router
from snapper.core.json_types import JsonObject
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.alert_default_routes import router as defaults_router
from snapper.server.android_device_routes import router as android_router
from snapper.server.dependencies import get_repository_dependency
from snapper.server.device_routes import router as devices_router
from snapper.server.position_cycle_routes import router as cycles_router

type MutationCase = tuple[str, str, str, JsonObject]

_USER_ID = "01910000-0000-7000-8000-000000000001"
_NOW = datetime(2026, 10, 26, tzinfo=UTC)
_SELF_MUTATIONS: list[MutationCase] = [
    ("POST", "/auth/me/update", "update_auth_me_request", {"default_language": "pl"}),
    (
        "POST",
        "/auth/users/auditor/change-password",
        "change_password_request",
        {"current_password": "old-test-password", "new_password": "new-test-password"},
    ),
    (
        "POST",
        "/devices",
        "register_device_command",
        {"device_token": "a" * 64, "device_id": "synthetic-device", "env": "sandbox"},
    ),
    ("DELETE", "/devices/synthetic-device", "", {}),
    (
        "PATCH",
        "/devices/synthetic-device/prefs",
        "update_device_pref_command",
        {"alert_type": "order_fill_full"},
    ),
    (
        "POST",
        "/devices/synthetic-device/prefs/synthetic-pref/revoke",
        "revoke_device_pref_command",
        {},
    ),
    (
        "POST",
        "/devices/android",
        "register_android_device_command",
        {
            "protocol": "fcm_fid",
            "configuration_id": _USER_ID,
            "package_name": "ch.snapper.android",
            "installation_id": _USER_ID,
            "binding_id": _USER_ID,
            "expected_revision": "0" * 64,
            "firebase_installation_id": "synthetic-FID",
            "expected_user_public_id": _USER_ID,
        },
    ),
    (
        "PATCH",
        "/alert_defaults",
        "update_user_alert_default_command",
        {"alert_type": "order_fill_full"},
    ),
]
_ADMIN_MUTATIONS: list[MutationCase] = [
    (
        "POST",
        "/auth/users",
        "create_user_request",
        {"username": "new-user", "password": "test-password", "role": "admin"},
    ),
    ("POST", "/auth/users/auditor/update", "update_user_request", {"role": "admin"}),
    ("POST", "/auth/users/auditor/deactivate", "deactivate_user_request", {}),
    (
        "POST",
        "/auth/users/auditor/admin-reset-password",
        "admin_reset_password_request",
        {"new_password": "test-password"},
    ),
    ("POST", "/auth/desks/desk/members/viewer", "", {}),
    ("DELETE", "/auth/desks/desk/members/viewer", "", {}),
    ("POST", "/settings/example/set", "setting_update", {"value": "changed"}),
    ("POST", "/settings/example/remove", "remove_setting_request", {}),
    (
        "POST",
        "/settings/push-beta/users",
        "update_push_beta_users_command",
        {"enabled": True, "user_public_ids": []},
    ),
    ("POST", "/position-cycles/close-orphan?cycle_public_id=cycle", "", {}),
    ("POST", "/position-cycles/sweep-orphans", "", {}),
]


def _envelope(type_name: str, payload: JsonObject) -> JsonObject:
    """Build a valid command envelope for direct HTTP requests."""
    return {
        "type": type_name,
        "public_id": _USER_ID,
        "session_id": _USER_ID,
        "sequence_id": 1,
        "timestamp": _NOW.isoformat(),
        "payload": payload,
    }


def _app(principal: AuthPrincipal, repo: MagicMock) -> FastAPI:
    """Mount real routes and authorization with explicit synthetic identity."""
    app = FastAPI()
    app.state.rest_tracker = SequenceTracker()
    for router in (
        auth_router,
        settings_router,
        cycles_router,
        android_router,
        devices_router,
        defaults_router,
    ):
        app.include_router(router)
    app.dependency_overrides[require_authentication] = lambda: principal
    app.dependency_overrides[validate_csrf_token] = lambda: None
    app.dependency_overrides[get_repository_dependency] = lambda: repo
    return app


@pytest.mark.parametrize("case", _SELF_MUTATIONS + _ADMIN_MUTATIONS)
@pytest.mark.parametrize("inject_write_claims", [False, True])
def test_auditor_direct_http_writes_are_denied_before_side_effects(
    case: MutationCase, inject_write_claims: bool
) -> None:
    """An auditor cannot bypass read-only access by constructing HTTP requests.

    Given: An auditor and valid request envelopes, optionally claiming every permission.
    When: Administrative or personal-account mutations are submitted directly.
    Then: The role ceiling rejects the request before service or repository activity.
    """
    principal = AuthPrincipal(
        username="auditor",
        user_public_id=_USER_ID,
        role=UserRole.AUDITOR,
        permissions=(
            [permission.value for permission in Permission] if inject_write_claims else None
        ),
        permission_scope_version=4,
    )
    repo = MagicMock()
    user_service = MagicMock()
    settings_service = AsyncMock()
    method, path, type_name, payload = case
    with (
        patch("snapper.auth.routes.get_user_service", return_value=user_service),
        patch("snapper.config.settings_routes.get_repository", return_value=repo),
        patch("snapper.config.settings_routes.get_settings_service", settings_service),
        TestClient(_app(principal, repo)) as client,
    ):
        response = client.request(method, path, json=_envelope(type_name, payload))
    assert response.status_code == 403, response.text
    assert repo.mock_calls == []
    assert user_service.mock_calls == []
    settings_service.assert_not_awaited()


@pytest.mark.parametrize(
    ("path", "legacy_permission"),
    [
        ("/auth/users", Permission.MANAGE_USERS),
        ("/auth/desks/desk/members", Permission.MANAGE_DESK_MEMBERSHIPS),
        ("/settings", Permission.CONFIGURE_SYSTEM),
        ("/settings/categories", Permission.CONFIGURE_SYSTEM),
        ("/settings/push-beta/users", Permission.CONFIGURE_SYSTEM),
        ("/position-cycles/open", Permission.MANAGE_USERS),
    ],
)
@pytest.mark.parametrize("legacy_admin", [False, True])
def test_administrative_reads_admit_auditor_and_legacy_admin(
    path: str, legacy_permission: Permission, legacy_admin: bool
) -> None:
    """Read permission separation preserves previously downscoped admin reads.

    Given: A full auditor or an old admin token carrying its original manage permission.
    When: An administrative directory or configuration read is requested.
    Then: The route returns its normal response without granting write capabilities.
    """
    principal = AuthPrincipal(
        username="reader",
        user_public_id=_USER_ID,
        role=UserRole.ADMIN if legacy_admin else UserRole.AUDITOR,
        permissions=[legacy_permission.value] if legacy_admin else None,
        permission_scope_version=3 if legacy_admin else 4,
    )
    repo = MagicMock()
    repo.get_settings = AsyncMock(return_value=[])
    repo.get_setting_categories = AsyncMock(return_value=[])
    repo.get_all_open_position_cycles = AsyncMock(return_value=[])
    users = MagicMock()
    users.get_all_users = AsyncMock(return_value=[])
    users.list_desk_members = AsyncMock(return_value=[])
    settings = MagicMock()
    settings.get_setting.return_value = None
    with (
        patch("snapper.auth.routes.get_user_service", return_value=users),
        patch("snapper.config.settings_routes.get_repository", return_value=repo),
        patch(
            "snapper.config.settings_routes.get_settings_service",
            AsyncMock(return_value=settings),
        ),
        TestClient(_app(principal, repo)) as client,
    ):
        response = client.get(path)
    assert response.status_code == 200, response.text


@pytest.mark.parametrize(
    "role",
    [
        UserRole.AI_RESEARCHER,
        UserRole.AI_REVIEWER,
        UserRole.AI_DELEGATE,
        UserRole.VIEWER,
        UserRole.OPERATOR,
        UserRole.ADMIN,
    ],
)
@pytest.mark.parametrize("scope_version", [3, 4])
def test_existing_roles_retain_self_service(role: UserRole, scope_version: int) -> None:
    """Existing role self-service remains available across the scope migration.

    Given: A historically narrowed token or a current token with self-service authority.
    When: Its owner changes their language preference.
    Then: The existing profile update succeeds and writes only the caller's preference.
    """
    permissions = [] if scope_version == 3 else [Permission.MANAGE_SELF_ACCOUNT.value]
    principal = AuthPrincipal(
        username="existing",
        user_public_id=_USER_ID,
        role=role,
        permissions=permissions,
        permission_scope_version=scope_version,
    )
    service = MagicMock()
    service.update_self_preferences = AsyncMock(
        return_value=UserProfile(
            public_id=_USER_ID,
            timestamp=_NOW,
            session_id=_USER_ID,
            sequence_id=1,
            username="existing",
            role=role,
            created_at=_NOW,
            default_language="pl",
        )
    )
    with (
        patch("snapper.auth.routes.get_user_service", return_value=service),
        TestClient(_app(principal, MagicMock())) as client,
    ):
        response = client.post(
            "/auth/me/update", json=_envelope("update_auth_me_request", {"default_language": "pl"})
        )
    assert response.status_code == 200, response.text
    service.update_self_preferences.assert_awaited_once_with(
        user_id="existing", default_language="pl"
    )


@pytest.mark.parametrize(
    ("schema", "type_name", "payload"),
    [
        (
            LoginRequest,
            "login_request",
            {"username": "auditor", "password": "test-password", "default_language": "pl"},
        ),
        (RefreshTokenRequest, "refresh_token_request", {"default_language": "pl"}),
    ],
)
def test_auth_lifecycle_payloads_cannot_change_account_preferences(
    schema: type[BaseModel], type_name: str, payload: JsonObject
) -> None:
    """Permitted authentication operations do not accept persistent preference changes.

    Given: A login or refresh command carrying an additional language preference.
    When: The route's actual request schema validates the body.
    Then: The extra field is rejected before authentication or rotation can execute.
    """
    with pytest.raises(ValidationError, match="extra_forbidden"):
        schema.model_validate(_envelope(type_name, payload))
