"""Legacy iOS response presentation must preserve canonical auditor authority."""

from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import Mock

import bcrypt
import pytest
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from fastapi import WebSocket
from httpx import ASGITransport
from httpx import AsyncClient
from pydantic import ValidationError
from sqlalchemy import select

from snapper.api.auth.schemas.ws_token import WsTokenPayload
from snapper.auth import client_compatibility
from snapper.auth.dependencies import require_authentication
from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.routes import router
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.responses import LoginResponse
from snapper.auth.schemas.responses import RefreshResponse
from snapper.auth.schemas.responses import UserListResponse
from snapper.auth.schemas.responses import UserResponse
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.schemas.user import UserProfile
from snapper.auth.tokens import TOKEN_TYPE_ACCESS
from snapper.auth.tokens import TOKEN_TYPE_REFRESH
from snapper.auth.tokens import TokenManager
from snapper.auth.user_service import UserService
from snapper.auth.websocket_auth import WebSocketAuthManager
from snapper.core.json_types import JsonObject
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import User
from snapper.data.repository import SQLAlchemyRepository
from snapper.interface.websocket.connection_manager import WebSocketConnectionManager
from snapper.interface.websocket.dispatcher import send_auth_complete
from snapper.interface.websocket.helpers import get_allowed_topics_for_role
from snapper.interface.websocket.schemas import WSAuthCompleteResponse
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency

_SYNTHETIC_LEGACY_UA = "Snapper/30 CFNetwork/1.2.3 Darwin/1.2.3"
_OBSERVED_LEGACY_UA = "Snapper/30 CFNetwork/3896.200.61 Darwin/27.2.0"
_CHROME_UA = "Mozilla/5.0 (X11; Linux x86_64) Chrome/140.0.0.0 Safari/537.36"
_USER_ID = "01948f94-0001-7a00-8000-000000000001"
_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_PASSWORD = "synthetic-test-password"


def _profile(role: UserRole = UserRole.AUDITOR) -> UserProfile:
    """Create a canonical profile with no fabricated execution identity."""
    return UserProfile(
        session_id=_USER_ID,
        sequence_id=1,
        public_id=_USER_ID,
        timestamp=_NOW,
        username="auditor",
        role=role,
        created_at=_NOW,
        effective_permissions=sorted(ROLE_PERMISSIONS[role], key=lambda item: item.value),
    )


def _request(user_agent: str) -> Request:
    """Build a native request with one explicitly supplied presentation hint."""
    return Request({"type": "http", "headers": [(b"user-agent", user_agent.encode())]})


def _envelope(type_name: str, payload: JsonObject) -> JsonObject:
    """Build a complete request envelope with synthetic provenance."""
    return {
        "type": type_name,
        "session_id": _USER_ID,
        "sequence_id": 1,
        "public_id": _USER_ID,
        "timestamp": _NOW.isoformat(),
        "payload": payload,
    }


@pytest.fixture
async def repository(monkeypatch: pytest.MonkeyPatch) -> AsyncGenerator[SQLAlchemyRepository]:
    """Use the actual account, token inventory, and user service in private SQLite."""
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await repo.create_all()
    password_hash = bcrypt.hashpw(_PASSWORD.encode(), bcrypt.gensalt(rounds=4)).decode()
    async with repo.session() as session:
        session.add(
            User(
                public_id=_USER_ID,
                session_id=_USER_ID,
                sequence_id=1,
                timestamp=_NOW,
                known_to=KNOWN_TO_MAX,
                username="auditor",
                password_hash=password_hash,
                role=UserRole.AUDITOR.value,
                is_active=True,
                created_at=_NOW,
            )
        )
        await session.commit()
    monkeypatch.setattr("snapper.auth.user_service.get_repository", Mock(return_value=repo))
    UserService.clear_instance()
    yield repo
    await repo.engine.dispose()


def _app(repository: SQLAlchemyRepository) -> FastAPI:
    """Bind the actual authentication pipeline to the private account database."""
    app = FastAPI()
    app.state.settings = SimpleNamespace(session_secure=False, session_same_site="lax")
    app.state.rest_tracker = SequenceTracker()
    app.include_router(router)
    app.dependency_overrides[get_repository_dependency] = lambda: repository
    return app


def _assert_auditor_claims(claims: TokenClaims | None) -> None:
    """Assert the signed identity retains all reads and no execution authority."""
    assert claims is not None
    assert claims.role is UserRole.AUDITOR
    assert claims.user_public_id == _USER_ID
    assert set(claims.permissions or []) == {
        permission.value for permission in ROLE_PERMISSIONS[UserRole.AUDITOR]
    }
    assert len(claims.permissions or []) == 20
    assert all(permission.startswith("read:") for permission in claims.permissions or [])
    assert Permission.READ_ALL_SCOPES.value in (claims.permissions or [])
    assert claims.operator_public_ids == []
    assert claims.operator_membership_public_ids == {}
    assert claims.primary_operator_public_id == ""
    assert claims.active_wallet_public_id is None


def _assert_profile(profile: UserProfile, expected_role: UserRole) -> None:
    """Assert response presentation changes no account state or capabilities."""
    assert profile.role is expected_role
    assert profile.public_id == _USER_ID
    assert set(profile.effective_permissions) == ROLE_PERMISSIONS[UserRole.AUDITOR]
    assert profile.operator_public_ids == []
    assert profile.primary_operator_public_id is None
    assert profile.active_wallet_public_id is None
    assert profile.delegate_public_id is None


@pytest.mark.parametrize("user_agent", [_OBSERVED_LEGACY_UA, _SYNTHETIC_LEGACY_UA])
def test_confirmed_build_remains_compatible_across_numeric_os_versions(user_agent: str) -> None:
    """The observed legacy build remains compatible when its OS version changes.

    Given: The observed installed build or an explicitly synthetic OS-version variant.
    When: Its canonical auditor profile is presented to the released decoder.
    Then: Only the wire role becomes viewer and canonical identity remains explicit.
    """
    profile = _profile()
    response = Response()
    result = client_compatibility.session_response_profile(_request(user_agent), response, profile)
    assert result is not profile
    assert result.role is UserRole.VIEWER
    assert profile.role is UserRole.AUDITOR
    assert response.headers["x-snapper-canonical-role"] == "auditor"
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["vary"] == "User-Agent"


@pytest.mark.parametrize("role", list(UserRole))
def test_verified_legacy_projection_changes_only_auditor_presentation(role: UserRole) -> None:
    """A recognized old client gets an independent profile copy only for auditor.

    Given: Every canonical role and a confirmed legacy client build.
    When: Response presentation adapts the user's profile.
    Then: Only auditor receives a viewer copy and the canonical input stays intact.
    """
    profile = _profile(role)
    response = Response(headers={"Vary": "Origin"})
    result = client_compatibility.session_response_profile(
        _request(_SYNTHETIC_LEGACY_UA), response, profile
    )
    assert profile.role is role
    assert response.headers["x-snapper-canonical-role"] == role.value
    assert response.headers.getlist("vary") == ["Origin", "User-Agent"]
    expected_role = UserRole.VIEWER if role is UserRole.AUDITOR else role
    assert result.model_dump() == profile.model_copy(update={"role": expected_role}).model_dump()
    assert (result is profile) is (role is not UserRole.AUDITOR)


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"user-agent": _CHROME_UA},
        {"user-agent": "Snapper/31 CFNetwork/1.2.3 Darwin/1.2.3"},
        {"user-agent": "Snapper/30 CFNetwork/1.2.3"},
        {"user-agent": "Snapper/30 CFNetwork/1.2.3 Darwin/1.2.3 extra"},
        {"user-agent": "Other/30 CFNetwork/1.2.3 Darwin/1.2.3"},
        {"user-agent": "Snapper/030 CFNetwork/1.2.3 Darwin/1.2.3"},
        {"user-agent": "Snapper/30 CFNetwork/1..3 Darwin/1.2.3"},
        {"user-agent": "Snapper/30 CFNetwork/1.2.3 Darwin/1.2.3\n"},
        {"user-agent": "Snapper/3.1.0.30 CFNetwork/1.2.3 Darwin/1.2.3"},
        {"user-agent": _CHROME_UA, "x-snapper-client-version": "3.1.0.30"},
        {"x-snapper-client-version": "3.1.0.30", "x-snapper-platform": "ios"},
    ],
)
def test_unknown_missing_and_spoofed_client_versions_remain_canonical(
    headers: dict[str, str],
) -> None:
    """Only a verified complete native signature selects an old response contract.

    Given: Missing, malformed, browser, future-build, or spoofed version hints.
    When: A canonical auditor profile is prepared for the caller.
    Then: Its role remains auditor without assuming a legacy native contract.
    """
    request = Request(
        {
            "type": "http",
            "headers": [(key.encode(), value.encode()) for key, value in headers.items()],
        }
    )
    profile = _profile()
    result = client_compatibility.session_response_profile(request, Response(), profile)
    assert result is profile


@pytest.mark.parametrize("legacy", [False, True])
async def test_login_me_and_refresh_preserve_canonical_database_and_token_authority(
    repository: SQLAlchemyRepository, legacy: bool, tmp_path: Path
) -> None:
    """Real login, access verification, and rotation keep auditor authority intact.

    Given: An actual auditor account without execution memberships or wallet state.
    When: A legacy or modern client logs in, reads its profile, and rotates tokens.
    Then: Responses decode consistently while database and signed authority stay auditor.
    """
    expected_role = UserRole.VIEWER if legacy else UserRole.AUDITOR
    user_agent = _OBSERVED_LEGACY_UA if legacy else _CHROME_UA
    manager = TokenManager.get_instance()
    async with AsyncClient(
        transport=ASGITransport(app=_app(repository)),
        base_url="http://localhost",
        headers={"user-agent": user_agent},
    ) as client:
        login = await client.post(
            "/auth/login?return_tokens=true",
            json=_envelope("login_request", {"username": "auditor", "password": _PASSWORD}),
        )
        assert login.status_code == 200, login.text
        payload = LoginResponse.model_validate_json(login.text).payload
        _assert_profile(payload.user, expected_role)
        assert payload.access_token is not None
        assert payload.refresh_token is not None
        _assert_auditor_claims(manager.verify_token(payload.access_token))
        _assert_auditor_claims(manager.verify_token(payload.refresh_token))
        me = await client.get(
            "/auth/me", headers={"authorization": f"Bearer {payload.access_token}"}
        )
        assert me.status_code == 200, me.text
        _assert_profile(UserResponse.model_validate_json(me.text).payload, expected_role)
        refresh = await client.post(
            "/auth/refresh?return_tokens=true",
            headers={"authorization": f"Bearer {payload.refresh_token}"},
        )
        assert refresh.status_code == 200, refresh.text
        rotated = RefreshResponse.model_validate_json(refresh.text).payload
        _assert_profile(rotated.user, expected_role)
        assert rotated.access_token is not None
        assert rotated.refresh_token is not None
        for token, token_type in (
            (rotated.access_token, TOKEN_TYPE_ACCESS),
            (rotated.refresh_token, TOKEN_TYPE_REFRESH),
        ):
            _assert_auditor_claims(
                await manager.verify_token_with_db(
                    token, repository, expected_token_type=token_type
                )
            )
        denied = await client.post(
            "/auth/me/update",
            headers={"authorization": f"Bearer {rotated.access_token}"},
            json=_envelope("update_auth_me_request", {"default_language": "pl"}),
        )
        assert denied.status_code == 403, denied.text
    for name, response in (("login", login), ("me", me), ("refresh", refresh)):
        assert response.headers["x-snapper-canonical-role"] == "auditor"
        assert response.headers["cache-control"] == "no-store"
        assert "User-Agent" in response.headers["vary"]
        (tmp_path / f"{name}.json").write_text(response.text, encoding="utf-8")
    async with repository.session() as session:
        user = (await session.execute(select(User))).scalar_one()
        assert user.role == UserRole.AUDITOR.value
        assert user.username == "auditor"
        assert user.default_language is None
        assert bcrypt.checkpw(_PASSWORD.encode(), user.password_hash.encode())
    assert await repository.get_user_operator_memberships(_USER_ID, datetime.now(UTC)) == []


def test_unknown_future_roles_fail_validation_without_a_privileged_fallback() -> None:
    """An unknown role never silently gains an existing role's capabilities.

    Given: A future role outside the canonical server enumeration.
    When: A profile attempts to validate that unsupported role.
    Then: Validation fails before response presentation or authorization can infer a grant.
    """
    unsupported = _profile().model_dump_json().replace('"role":"auditor"', '"role":"future"')
    with pytest.raises(ValidationError, match="role"):
        UserProfile.model_validate_json(unsupported)


@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("path", ["/auth/users", "/auth/desks/desk/members"])
async def test_readable_profile_lists_apply_the_same_legacy_contract(
    monkeypatch: pytest.MonkeyPatch, legacy: bool, path: str
) -> None:
    """Readable directories cannot reintroduce the enum rejected at login.

    Given: An auditor authorized to read both directories and mixed canonical profiles.
    When: A legacy or modern client requests a profile list.
    Then: Only auditor entries adapt while the service receives the canonical principal.
    """
    principal = AuthPrincipal(username="auditor", role=UserRole.AUDITOR)
    profiles = [_profile(role) for role in UserRole]
    service = Mock()
    service.get_all_users = AsyncMock(return_value=profiles)
    service.list_desk_members = AsyncMock(return_value=profiles)
    monkeypatch.setattr("snapper.auth.routes.get_user_service", Mock(return_value=service))
    app = _app(Mock())
    app.dependency_overrides[require_authentication] = lambda: principal
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://localhost") as client:
        response = await client.get(
            path, headers={"user-agent": _OBSERVED_LEGACY_UA if legacy else _CHROME_UA}
        )
    assert response.status_code == 200, response.text
    projected = UserListResponse.model_validate_json(response.text).payload
    expected = [
        UserRole.VIEWER if legacy and role is UserRole.AUDITOR else role for role in UserRole
    ]
    assert [profile.role for profile in projected] == expected
    assert [profile.role for profile in profiles] == list(UserRole)
    assert response.headers["x-snapper-canonical-role"] == "auditor"
    if path.endswith("/members"):
        assert service.list_desk_members.call_args.kwargs["principal"] is principal


@pytest.mark.parametrize("legacy", [False, True])
async def test_websocket_auth_complete_adapts_only_the_presented_role(
    legacy: bool, tmp_path: Path
) -> None:
    """The legacy WebSocket decoder receives full expiry and canonical topic grants.

    Given: A canonical auditor principal and a verified native or modern handshake.
    When: Authentication completion is emitted after authority has been resolved.
    Then: Only the wire role adapts while the principal, topics, and expiry stay intact.
    """
    websocket = Mock(spec=WebSocket)
    websocket.headers = {"user-agent": _OBSERVED_LEGACY_UA if legacy else _CHROME_UA}
    websocket.send_text = AsyncMock()
    principal = AuthPrincipal(username="auditor", role=UserRole.AUDITOR)
    manager = WebSocketConnectionManager()
    auth_manager = Mock(spec=WebSocketAuthManager)
    auth_manager.get_connection_expiration.return_value = _NOW
    payload = WsTokenPayload(
        purpose="ws_auth",
        sub="auditor",
        sid_hash="synthetic",
        iat=int(_NOW.timestamp()),
        exp=2_000_000_000,
        jti="synthetic",
    )
    await send_auth_complete(websocket, manager, principal, payload, auth_manager)
    frame = websocket.send_text.call_args_list[1].args[0]
    complete = WSAuthCompleteResponse.model_validate_json(frame)
    assert complete.user_role is (UserRole.VIEWER if legacy else UserRole.AUDITOR)
    assert complete.available_topics == get_allowed_topics_for_role(UserRole.AUDITOR)
    assert complete.ws_token_exp == datetime.fromtimestamp(payload.exp, UTC)
    assert complete.session_expires_at == _NOW
    assert principal.role is UserRole.AUDITOR
    (tmp_path / "auth_complete.json").write_text(frame, encoding="utf-8")
