"""The purpose gate must not break the credentials already in the field.

Tightening token acceptance is only safe if the tightening is keyed on
something every EXISTING credential already carries. The long-lived AI
delegate token running in production predates purpose entirely: it was
signed with a BARE UUID ``jti``, no purpose claim, and no
``permission_scope_version``, months ago, and it expires months from
now. It cannot be re-minted without breaking the integration it drives,
and a previous change that keyed a compatibility branch on
``permission_scope_version == 1`` missed it silently — the MCP decision
tool simply vanished from ``tools/list`` and three consults expired with
no alert.

So the gate is keyed on the ``user_active_tokens`` row instead: the
issuer has written ``token_type`` there since before that token was
minted, and the row is bound to the token by hash. These tests exercise
that claim end to end against a REAL database and the REAL auth paths —
the REST dependency chain, the MCP bearer middleware, and the WebSocket
upgrade — rather than by decoding a JWT in isolation. A test that only
decoded would not have caught the failure we are guarding against.

Read together the cases assert both halves of the contract:

    - the markerless live-shaped delegate credential still
      authenticates everywhere it did before, and still sees
      ``submit_ai_review_decision`` in the MCP catalogue;
    - a refresh credential — which authenticated everywhere before this
      change, because nothing asked what a token WAS — is now refused
      at every bearer surface while still rotating at ``/auth/refresh``.
"""

import uuid
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from types import SimpleNamespace
from typing import Any
from typing import Final
from unittest.mock import MagicMock
from unittest.mock import Mock

import jwt
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient as StarletteTestClient
from starlette.websockets import WebSocket

from snapper.application.services.settings import SettingsService
from snapper.auth import routes
from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.schemas.user import UserProfile
from snapper.auth.tokens import TOKEN_TYPE_ACCESS
from snapper.auth.tokens import TOKEN_TYPE_REFRESH
from snapper.auth.tokens import TokenManager
from snapper.auth.tokens import hash_token
from snapper.auth.websocket_auth import WebSocketAuthManager
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AiDelegate
from snapper.data.models import User
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import UserActiveTokenInsertRow
from snapper.mcp.server import BearerAuthMiddleware
from snapper.mcp.server import FeatureFlagMiddleware
from snapper.mcp.server import PermissionAwareMCPServer
from snapper.mcp.server import get_current_claims
from snapper.mcp.tools import register_mcp_tools
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency

_BCRYPT_FAKE_DIGEST: Final[str] = "$2b$12$" + "x" * 53
"""Structurally-valid bcrypt digest; no password is ever checked here."""

_DELEGATE_USER_ID: Final[str] = "user-live-delegate"
"""Owner of the live-shaped credential under test."""

_DELEGATE_USERNAME: Final[str] = "ai-delegate-live"
"""Username the live-shaped credential authenticates as."""

_DECISION_TOOL: Final[str] = "submit_ai_review_decision"
"""The MCP tool that silently vanished the last time compatibility broke."""


@pytest.fixture
async def repo() -> SQLAlchemyRepository:
    """In-memory database carrying the real ``user_active_tokens`` schema."""
    repository = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await repository.create_all()
    await _seed_delegate(repository)
    return repository


async def _seed_delegate(repository: SQLAlchemyRepository) -> None:
    """Insert the SCD2-active delegate user and its ``ai_delegates`` row."""
    seed_time = datetime(2026, 1, 1, tzinfo=UTC)
    async with repository.session() as session:
        session.add(
            User(
                session_id="seed",
                sequence_id=1,
                timestamp=seed_time,
                known_to=KNOWN_TO_MAX,
                public_id=_DELEGATE_USER_ID,
                username=_DELEGATE_USERNAME,
                email="delegate@example.com",
                password_hash=_BCRYPT_FAKE_DIGEST,
                role=UserRole.AI_DELEGATE.value,
                is_active=True,
                created_at=seed_time,
            )
        )
        session.add(
            AiDelegate(
                public_id="delegate-public-1",
                user_public_id=_DELEGATE_USER_ID,
                last_seen_at=None,
                active_reviews_count=0,
                created_at=seed_time,
                updated_at=seed_time,
            )
        )
        await session.commit()


def _fresh_manager() -> TokenManager:
    """Clean-state TokenManager so no cached verdict leaks between cases."""
    TokenManager._initialized = False
    manager = TokenManager()
    manager._blacklisted_tokens.clear()
    manager._blacklist_cleanup_heap.clear()
    manager._next_blacklist_cleanup_ts = float("inf")
    manager._verify_cache.clear()
    manager._user_cache_generations.clear()
    return manager


def _encode(manager: TokenManager, payload: dict[str, Any]) -> str:
    """Sign one hand-built claim set with the deployment's real signing key."""
    return jwt.encode(
        payload,
        manager.settings.auth_secret_key,
        algorithm=manager.settings.auth_algorithm,
    )


def _live_shaped_claims(jti: str, *, months_old: int = 3) -> dict[str, Any]:
    """Build the exact claim shape the production delegate token carries.

    No purpose claim. No ``permission_scope_version``. A bare UUID
    ``jti`` with no marker of any kind. An ``iat`` months in the past
    and an ``exp`` months in the future, because that is what a
    long-lived credential looks like by the time a tightening ships.

    Args:
        jti: The bare UUID to sign as the credential's id.
        months_old: Roughly how many 30-day months ago it was minted.

    Returns:
        A JWT claim mapping ready to sign.
    """
    issued = datetime.now(UTC) - timedelta(days=30 * months_old)
    expires = datetime.now(UTC) + timedelta(days=69)
    return {
        "sub": _DELEGATE_USERNAME,
        "username": _DELEGATE_USERNAME,
        "role": UserRole.AI_DELEGATE.value,
        "permissions": sorted(
            permission.value for permission in ROLE_PERMISSIONS[UserRole.AI_DELEGATE]
        ),
        "exp": int(expires.timestamp()),
        "iat": int(issued.timestamp()),
        "jti": jti,
        "sid": "live-delegate-session",
        "user_public_id": _DELEGATE_USER_ID,
        "operator_public_ids": ["op-live"],
        "primary_operator_public_id": "op-live",
    }


async def _persist_row(
    repository: SQLAlchemyRepository,
    token: str,
    jti: str,
    token_type: str,
) -> None:
    """Record one inventory row binding ``token``'s hash to its purpose."""
    now = datetime.now(UTC)
    await repository.insert_user_active_tokens(
        [
            UserActiveTokenInsertRow(
                public_id=str(uuid.uuid7()),
                user_public_id=_DELEGATE_USER_ID,
                jti=jti,
                token_hash=hash_token(token),
                token_type=token_type,
                issued_at=now - timedelta(days=90),
                expires_at=now + timedelta(days=69),
            )
        ]
    )


async def _mint_live_delegate_token(
    manager: TokenManager,
    repository: SQLAlchemyRepository,
) -> str:
    """Mint and inventory a markerless long-lived delegate access credential."""
    jti = str(uuid.uuid4())
    token = _encode(manager, _live_shaped_claims(jti))
    await _persist_row(repository, token, jti, TOKEN_TYPE_ACCESS)
    return token


async def _mint_legacy_refresh_token(
    manager: TokenManager,
    repository: SQLAlchemyRepository,
) -> str:
    """Mint and inventory a legacy-shaped refresh credential.

    Same markerless shape as the delegate credential — no
    ``permission_scope_version`` — but carrying the ``refresh_`` ``jti``
    prefix the issuer has always stamped on refresh JWTs.
    """
    jti = f"refresh_{uuid.uuid4()}"
    token = _encode(manager, _live_shaped_claims(jti))
    await _persist_row(repository, token, jti, TOKEN_TYPE_REFRESH)
    return token


class StubUserService:
    """Minimal account lookup so the refresh route can complete."""

    def __init__(self, profile: UserProfile) -> None:
        """Hold the single profile every lookup resolves to."""
        self.profile = profile

    async def get_user_by_id(self, user_id: str) -> UserProfile:
        """Resolve the refresh token's subject."""
        return self.profile

    async def build_auth_principal(self, user: UserProfile) -> AuthPrincipal:
        """Project the profile onto the principal the route re-mints from."""
        return AuthPrincipal(
            username=user.username,
            role=user.role,
            user_public_id=_DELEGATE_USER_ID,
            operator_public_ids=["op-live"],
            primary_operator_public_id="op-live",
        )


def _delegate_profile() -> UserProfile:
    """Account profile matching the seeded delegate user."""
    return UserProfile(
        session_id="live-sid",
        sequence_id=1,
        public_id=_DELEGATE_USER_ID,
        timestamp=datetime.now(UTC),
        username=_DELEGATE_USERNAME,
        email="delegate@example.com",
        role=UserRole.AI_DELEGATE,
        is_active=True,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


def _rest_client(repository: SQLAlchemyRepository) -> TestClient:
    """Mount the real auth router with the real dependency chain.

    Nothing in the authentication path is overridden — ``get_current_user``,
    ``verify_token_with_db`` and the inventory read all run for real. Only
    the repository binding is redirected at the test database.
    """
    app = FastAPI()
    app.state.settings = SimpleNamespace(session_secure=False, session_same_site="lax")
    app.state.rest_tracker = SequenceTracker()
    app.include_router(routes.router)
    app.dependency_overrides[get_repository_dependency] = lambda: repository
    return TestClient(app)


def _mcp_client(repository: SQLAlchemyRepository) -> StarletteTestClient:
    """Compose the production MCP middleware stack over a catalogue echo.

    The downstream handler answers with the tool names the authenticated
    claims may see, so one request proves BOTH that the bearer passed
    the middleware and that the catalogue it drives is intact.
    """
    server = PermissionAwareMCPServer("purpose-compatibility")
    register_mcp_tools(
        server,
        repository_getter=lambda: None,
        caps_enforcer_getter=lambda: None,
        claims_getter=get_current_claims,
    )

    async def _echo_catalogue(_request: Request) -> JSONResponse:
        """Return the tool names visible to the authenticated claims."""
        claims = get_current_claims()
        tools = await server.list_tools()
        return JSONResponse({"username": claims.username, "tools": [tool.name for tool in tools]})

    settings_service = Mock(spec=SettingsService)
    settings_service.get_setting.return_value = True
    app = Starlette(routes=[Route("/mcp", _echo_catalogue, methods=["POST"])])
    app.add_middleware(BearerAuthMiddleware, repository_getter=lambda: repository)
    app.add_middleware(FeatureFlagMiddleware, settings_service_getter=lambda: settings_service)
    return StarletteTestClient(app)


async def _ws_upgrade(
    repository: SQLAlchemyRepository,
    token: str,
) -> tuple[AuthPrincipal, TokenClaims] | None:
    """Run the real WebSocket upgrade auth with a bearer header."""
    websocket = MagicMock(spec=WebSocket)
    websocket.headers = {"authorization": f"Bearer {token}"}
    websocket.cookies = {}
    return await WebSocketAuthManager().verify_session_cookie(websocket, repository)


@pytest.fixture
def refresh_route_app(
    repo: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[TestClient]:
    """Real auth router wired to a real repo, with account lookup stubbed.

    The verifier, the inventory read and ``rotate_tokens`` are all real —
    only ``UserService``, which this change does not touch, is stubbed.
    """
    monkeypatch.setattr(routes, "get_user_service", lambda: StubUserService(_delegate_profile()))
    yield _rest_client(repo)


class TestLiveShapedDelegateCredentialStillWorks:
    """The credential that cannot be re-minted must survive the tightening."""

    @pytest.mark.asyncio
    async def test_it_authenticates_at_rest(self, repo: SQLAlchemyRepository) -> None:
        """A markerless long-lived bearer still passes the REST chain.

        Given: a delegate JWT with no purpose claim, no
            ``permission_scope_version``, a bare UUID ``jti``, a
            months-old ``iat`` and a months-away ``exp``, whose hash is
            recorded in the inventory as an ``access`` row,
        When: it is presented as ``Authorization: Bearer`` to a route
            behind the real ``require_authentication`` chain,
        Then: the request is authenticated.
        """
        manager = _fresh_manager()
        token = await _mint_live_delegate_token(manager, repo)
        with _rest_client(repo) as client:
            response = client.post("/auth/ws_token", headers={"Authorization": f"Bearer {token}"})
        assert response.status_code == 200
        assert response.json()["payload"]["ws_token"]

    @pytest.mark.asyncio
    async def test_it_authenticates_at_mcp_and_keeps_the_decision_tool(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """The same credential passes MCP auth AND still sees the decision tool.

        Given: the markerless live-shaped delegate credential,
        When: it drives one POST through the production MCP bearer
            middleware and the handler lists tools under the claims the
            middleware published,
        Then: the request is authenticated and
            ``submit_ai_review_decision`` is in the catalogue — the exact
            symptom that went unnoticed the last time a compatibility
            branch missed this token.
        """
        manager = _fresh_manager()
        token = await _mint_live_delegate_token(manager, repo)
        with _mcp_client(repo) as client:
            response = client.post("/mcp", headers={"Authorization": f"Bearer {token}"}, json={})
        assert response.status_code == 200
        body = response.json()
        assert body["username"] == _DELEGATE_USERNAME
        assert _DECISION_TOOL in body["tools"]

    @pytest.mark.asyncio
    async def test_it_authenticates_at_the_websocket_upgrade(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """The same credential still completes the WebSocket upgrade.

        Given: the markerless live-shaped delegate credential presented
            on the upgrade ``Authorization`` header,
        When: the real upgrade auth runs against the real inventory,
        Then: a principal and its claims come back.
        """
        manager = _fresh_manager()
        token = await _mint_live_delegate_token(manager, repo)
        result = await _ws_upgrade(repo, token)
        assert result is not None
        principal, claims = result
        assert principal.username == _DELEGATE_USERNAME
        assert claims.permission_scope_version is None


class TestRefreshCredentialIsRefusedAsABearer:
    """A refresh credential authenticated everywhere before this change."""

    @pytest.mark.asyncio
    async def test_it_is_refused_at_rest_and_at_ws_token(self, repo: SQLAlchemyRepository) -> None:
        """The REST chain — and therefore ``/auth/ws_token`` — refuses it.

        Given: a live, unrevoked, unexpired ``refresh`` inventory row and
            its matching refresh JWT,
        When: the refresh JWT is presented as a REST bearer,
        Then: the request is rejected with 401 and no ws_token is minted.
        """
        manager = _fresh_manager()
        token = await _mint_legacy_refresh_token(manager, repo)
        with _rest_client(repo) as client:
            response = client.post("/auth/ws_token", headers={"Authorization": f"Bearer {token}"})
        assert response.status_code == 401

    @pytest.mark.asyncio
    async def test_it_is_refused_at_mcp(self, repo: SQLAlchemyRepository) -> None:
        """The MCP bearer middleware refuses it.

        Given: the same live refresh credential,
        When: it is presented to the MCP transport,
        Then: the middleware answers 401 ``invalid_bearer_token`` and the
            tool handler never runs.
        """
        manager = _fresh_manager()
        token = await _mint_legacy_refresh_token(manager, repo)
        with _mcp_client(repo) as client:
            response = client.post("/mcp", headers={"Authorization": f"Bearer {token}"}, json={})
        assert response.status_code == 401
        assert response.json()["error_code"] == "invalid_bearer_token"

    @pytest.mark.asyncio
    async def test_it_is_refused_at_the_websocket_upgrade(self, repo: SQLAlchemyRepository) -> None:
        """The WebSocket upgrade refuses it.

        Given: the same live refresh credential on the upgrade header,
        When: the real upgrade auth runs,
        Then: no principal is produced.
        """
        manager = _fresh_manager()
        token = await _mint_legacy_refresh_token(manager, repo)
        assert await _ws_upgrade(repo, token) is None


class TestRefreshRotationStillWorks:
    """Refusing refresh credentials as bearers must not break rotation."""

    @pytest.mark.asyncio
    async def test_a_legacy_refresh_token_still_rotates(
        self,
        repo: SQLAlchemyRepository,
        refresh_route_app: TestClient,
    ) -> None:
        """``/auth/refresh`` still redeems a markerless legacy refresh JWT.

        Given: a legacy-shaped refresh JWT — no
            ``permission_scope_version`` — with a live ``refresh``
            inventory row,
        When: it is presented to the real refresh route,
        Then: rotation succeeds, a new pair is returned, and the redeemed
            row is revoked in the real inventory.
        """
        manager = _fresh_manager()
        token = await _mint_legacy_refresh_token(manager, repo)
        response = refresh_route_app.post(
            "/auth/refresh?return_tokens=true",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 200
        payload = response.json()["payload"]
        assert payload["access_token"]
        assert payload["refresh_token"] != token
        redeemed = await repo.get_active_token_by_hash(hash_token(token))
        assert redeemed is not None
        assert redeemed["revoked_at"] is not None

    @pytest.mark.asyncio
    async def test_the_freshly_minted_access_token_is_a_bearer_again(
        self,
        repo: SQLAlchemyRepository,
        refresh_route_app: TestClient,
    ) -> None:
        """Rotation's output is usable where its input was not.

        Given: the access token minted by a successful rotation,
        When: it is presented as a REST bearer,
        Then: it authenticates — proving the inventory rows the rotation
            wrote carry the purposes the gate expects, not merely that
            the old credential was refused.
        """
        manager = _fresh_manager()
        token = await _mint_legacy_refresh_token(manager, repo)
        rotated = refresh_route_app.post(
            "/auth/refresh?return_tokens=true",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert rotated.status_code == 200
        new_access = rotated.json()["payload"]["access_token"]
        response = refresh_route_app.post(
            "/auth/ws_token", headers={"Authorization": f"Bearer {new_access}"}
        )
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_an_access_token_cannot_be_redeemed_for_a_new_pair(
        self,
        repo: SQLAlchemyRepository,
        refresh_route_app: TestClient,
    ) -> None:
        """The gate closes the reverse direction too.

        Given: the live-shaped delegate ACCESS credential,
        When: it is presented to ``/auth/refresh``,
        Then: rotation is refused — an access bearer must not be able to
            mint itself a fresh long-lived pair.
        """
        manager = _fresh_manager()
        token = await _mint_live_delegate_token(manager, repo)
        response = refresh_route_app.post(
            "/auth/refresh",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 401
