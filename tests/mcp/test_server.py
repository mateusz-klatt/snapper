"""Day 2b tests for the MCP sub-app factory + middleware stack.

Verifies the three-layer composition from
:func:`snapper.mcp.server.build_mcp_app`:

1. :class:`FeatureFlagMiddleware` — 503 when ``ai_integration_enabled``
   is False (default) or when the settings service hasn't initialized
   yet; pass-through when True.
2. :class:`BearerAuthMiddleware` — 401 on missing / malformed /
   unverifiable Bearer; populates ``request.state.token_claims`` on
   success.
3. FastMCP downstream handler receives the request only when both
   gates admit.

The tests use Starlette's :class:`TestClient` against the sub-app
directly (not mounted), so framework-level behavior is exercised
without relying on FastAPI lifespan wiring.
"""

from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import Mock
from unittest.mock import patch

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from snapper.application.services.settings import SettingsService
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.tokens import REJECTION_REASON_INVALID
from snapper.auth.tokens import VerifyOutcome
from snapper.mcp.server import BearerAuthMiddleware
from snapper.mcp.server import FeatureFlagMiddleware
from snapper.mcp.server import build_mcp_app

_TEST_TOKEN_PLACEHOLDER = "dummy.value.used.by.tests.only"


def _make_settings_service(*, enabled: bool) -> Mock:
    """Build a :class:`SettingsService`-spec mock with the flag preset."""
    svc = Mock(spec=SettingsService)
    svc.get_setting.return_value = enabled
    return svc


def _make_token_claims() -> TokenClaims:
    """Return a representative :class:`TokenClaims` for a delegate."""
    now = int(datetime.now(UTC).timestamp())
    return TokenClaims(
        sub="user-ai-1",
        username="ai-delegate-1",
        role=UserRole.AI_DELEGATE,
        permissions=["read:market_data", "create:orders"],
        exp=now + 3600,
        iat=now,
        jti="jti-mcp",
        sid="sid-mcp",
    )


class TestFeatureFlagMiddleware:
    """503 responses when the AI integration flag is off."""

    def test_flag_off_returns_503_feature_disabled(self) -> None:
        """Given ``ai_integration_enabled=False``, Then 503.

        Given: MCP sub-app wired to a settings service whose
            ``ai_integration_enabled`` flag returns ``False``,
        When: any POST reaches the mount point,
        Then: the response is HTTP 503 with
            ``error_code="feature_disabled"`` — per plan §3.12
            always-mounted-but-gated semantics.
        """
        svc = _make_settings_service(enabled=False)
        app = build_mcp_app(settings_service_getter=lambda: svc, repository_getter=lambda: Mock())
        client = TestClient(app)
        response = client.post("/mcp", json={})
        assert response.status_code == 503
        body = response.json()
        assert body["error_code"] == "feature_disabled"

    def test_settings_service_missing_returns_503(self) -> None:
        """Lifespan-not-ready → 503, not an unhandled 500.

        Given: MCP sub-app whose getter returns ``None`` (FastAPI
            lifespan hasn't finished initializing settings_service),
        When: a request arrives,
        Then: HTTP 503 is returned with
            ``error_code="feature_disabled"`` rather than raising an
            AttributeError.
        """
        app = build_mcp_app(settings_service_getter=lambda: None, repository_getter=lambda: Mock())
        client = TestClient(app)
        response = client.post("/mcp", json={})
        assert response.status_code == 503
        assert response.json()["error_code"] == "feature_disabled"


class TestBearerAuthMiddleware:
    """Bearer-header requirements when the flag is on."""

    def test_missing_bearer_header_returns_401(self) -> None:
        """No Authorization header → 401 ``missing_bearer_token``.

        Given: the feature flag is on so the request reaches auth,
        When: no ``Authorization: Bearer`` header is set,
        Then: HTTP 401 with
            ``error_code="missing_bearer_token"`` is returned, and
            FastMCP downstream is never invoked.
        """
        svc = _make_settings_service(enabled=True)
        app = build_mcp_app(settings_service_getter=lambda: svc, repository_getter=lambda: Mock())
        client = TestClient(app)
        response = client.post("/mcp", json={})
        assert response.status_code == 401
        body = response.json()
        assert body["error_code"] == "missing_bearer_token"

    def test_non_bearer_scheme_returns_401(self) -> None:
        """``Authorization: Basic ...`` → 401 ``missing_bearer_token``.

        Given: the flag is on and the request carries a non-Bearer
            Authorization header,
        When: the middleware inspects the header,
        Then: HTTP 401 ``missing_bearer_token`` — the MCP transport
            requires Bearer specifically; the extractor returns
            ``None`` for any other scheme.
        """
        svc = _make_settings_service(enabled=True)
        app = build_mcp_app(settings_service_getter=lambda: svc, repository_getter=lambda: Mock())
        client = TestClient(app)
        response = client.post(
            "/mcp",
            headers={"Authorization": "Basic dXNlcjpwYXNz"},
            json={},
        )
        assert response.status_code == 401
        assert response.json()["error_code"] == "missing_bearer_token"

    def test_repository_getter_returns_none_yields_503(self) -> None:
        """Lifespan-not-ready repo → 503 ``mcp_unavailable`` (plan §3.6.3).

        Given: the feature flag is on and a Bearer token is present
            but the repository_getter returns ``None`` (e.g., FastAPI
            lifespan hasn't finished wiring the DB singleton),
        When: the bearer middleware processes the request,
        Then: HTTP 503 with ``error_code="mcp_unavailable"`` is
            returned BEFORE any TokenManager call. The alternative —
            silently skipping the DB-backed check — would let
            revoked JWTs through during startup, defeating the Day
            3d-B contract.
        """
        svc = _make_settings_service(enabled=True)
        app = build_mcp_app(settings_service_getter=lambda: svc, repository_getter=lambda: None)
        client = TestClient(app)
        response = client.post(
            "/mcp",
            headers={"Authorization": f"Bearer {_TEST_TOKEN_PLACEHOLDER}"},
            json={},
        )
        assert response.status_code == 503
        assert response.json()["error_code"] == "mcp_unavailable"

    def test_rejection_with_unexpected_none_reason_falls_back_to_invalid(self) -> None:
        """Defensive: ``None`` rejection_reason maps to ``invalid_bearer_token``.

        Day 3d-D R2 (Copilot MINOR NEW FINDING): pin the defensive
        default in ``_build_rejection_response`` so a future
        ``VerifyOutcome`` with a ``None`` reason (which should never
        happen in production but could emerge from a refactor bug)
        falls through to ``invalid_bearer_token`` instead of
        accidentally leaking ``user_deactivated``.
        """
        svc = _make_settings_service(enabled=True)
        app = build_mcp_app(settings_service_getter=lambda: svc, repository_getter=lambda: Mock())
        client = TestClient(app)
        with patch("snapper.mcp.server.get_token_manager") as mock_get:
            token_manager = Mock()
            token_manager.verify_token_with_reason = AsyncMock(
                return_value=VerifyOutcome(claims=None, rejection_reason=None)
            )
            mock_get.return_value = token_manager
            response = client.post(
                "/mcp",
                headers={"Authorization": f"Bearer {_TEST_TOKEN_PLACEHOLDER}"},
                json={},
            )
        assert response.status_code == 401
        assert response.json()["error_code"] == "invalid_bearer_token"

    def test_deactivated_user_token_returns_401_user_deactivated(self) -> None:
        """Deactivated user's token → 401 ``user_deactivated`` (plan §2 item 6).

        Given: the flag is on, the bearer token passes JWT signature
            + expiry, but the Day 3d-B DB-backed verify rejects it
            because ``users.is_active=False`` — ``verify_token_with_reason``
            returns a ``VerifyOutcome`` with
            ``rejection_reason=REJECTION_REASON_USER_DEACTIVATED``.
        When: the middleware processes the request,
        Then: HTTP 401 is returned with
            ``error_code="user_deactivated"`` — distinct from
            ``invalid_bearer_token`` so MCP clients surface a
            re-login prompt instead of attempting a refresh that
            would fail with the same verdict.
        """
        from snapper.auth.tokens import REJECTION_REASON_USER_DEACTIVATED
        from snapper.auth.tokens import VerifyOutcome

        svc = _make_settings_service(enabled=True)
        app = build_mcp_app(settings_service_getter=lambda: svc, repository_getter=lambda: Mock())
        client = TestClient(app)
        with patch("snapper.mcp.server.get_token_manager") as mock_get:
            token_manager = Mock()
            token_manager.verify_token_with_reason = AsyncMock(
                return_value=VerifyOutcome(
                    claims=None, rejection_reason=REJECTION_REASON_USER_DEACTIVATED
                )
            )
            mock_get.return_value = token_manager
            response = client.post(
                "/mcp",
                headers={"Authorization": f"Bearer {_TEST_TOKEN_PLACEHOLDER}"},
                json={},
            )
        assert response.status_code == 401
        assert response.json()["error_code"] == "user_deactivated"

    def test_invalid_bearer_token_returns_401_invalid_bearer(self) -> None:
        """Unverifiable JWT → 401 ``invalid_bearer_token``.

        Given: the flag is on and a Bearer token is present but the
            :class:`TokenManager` returns ``None`` for verification,
        When: the middleware processes the request,
        Then: HTTP 401 with
            ``error_code="invalid_bearer_token"`` — distinct from
            missing-header so MCP clients know to refresh rather
            than obtain a new token from scratch.
        """
        svc = _make_settings_service(enabled=True)
        app = build_mcp_app(settings_service_getter=lambda: svc, repository_getter=lambda: Mock())
        client = TestClient(app)
        with patch("snapper.mcp.server.get_token_manager") as mock_get:
            token_manager = Mock()
            token_manager.verify_token_with_db = AsyncMock(return_value=None)
            token_manager.verify_token_with_reason = AsyncMock(
                return_value=VerifyOutcome(
                    claims=None,
                    rejection_reason=REJECTION_REASON_INVALID,
                )
            )
            token_manager._verify_cache = {}
            mock_get.return_value = token_manager
            response = client.post(
                "/mcp",
                headers={"Authorization": "Bearer tampered.jwt"},
                json={},
            )
        assert response.status_code == 401
        assert response.json()["error_code"] == "invalid_bearer_token"

    def test_valid_bearer_stashes_claims_on_request_state(self) -> None:
        """Valid JWT → middleware stack admits and exposes claims downstream.

        Given: the full composed middleware chain (feature flag ON +
            bearer auth) wrapping a minimal echo downstream app that
            reflects ``request.state.token_claims`` as JSON,
        When: a Bearer-bearing request reaches the downstream,
        Then: the echo handler sees the verified :class:`TokenClaims`
            on ``request.state`` — proving the middleware populates
            the attribute per plan §3.2 tool-dispatch contract. The
            downstream status is 200 (FastMCP is not invoked here;
            the echo stub replaces it).
        """

        def _echo_claims(request: Request) -> JSONResponse:
            claims = getattr(request.state, "token_claims", None)
            if claims is None:
                return JSONResponse({"seen": None}, status_code=200)
            return JSONResponse({"seen": claims.username}, status_code=200)

        downstream = Starlette(routes=[Route("/mcp", _echo_claims, methods=["POST"])])
        svc = _make_settings_service(enabled=True)
        downstream.add_middleware(BearerAuthMiddleware, repository_getter=lambda: Mock())
        downstream.add_middleware(
            FeatureFlagMiddleware,
            settings_service_getter=lambda: svc,
        )
        claims = _make_token_claims()
        client = TestClient(downstream)
        with patch("snapper.mcp.server.get_token_manager") as mock_get:
            token_manager = Mock()
            token_manager.verify_token_with_db = AsyncMock(return_value=claims)
            token_manager.verify_token_with_reason = AsyncMock(
                return_value=VerifyOutcome(
                    claims=claims,
                    rejection_reason=None,
                )
            )
            mock_get.return_value = token_manager
            response = client.post(
                "/mcp",
                headers={"Authorization": f"Bearer {_TEST_TOKEN_PLACEHOLDER}"},
                json={},
            )
        assert response.status_code == 200
        assert response.json()["seen"] == "ai-delegate-1"


class TestMCPAppComposition:
    """Structural assertions on the sub-app factory output."""

    def test_build_mcp_app_returns_starlette_instance(self) -> None:
        """Factory returns a Starlette app ready to mount.

        Given: a configured settings_service_getter,
        When: :func:`build_mcp_app` is invoked,
        Then: a Starlette instance is returned (concrete type may
            be a Starlette subclass from the MCP SDK; duck-typed by
            ``routes`` attribute presence).
        """
        svc = _make_settings_service(enabled=True)
        app = build_mcp_app(settings_service_getter=lambda: svc, repository_getter=lambda: Mock())
        assert hasattr(app, "routes")
        assert hasattr(app, "user_middleware")

    def test_settings_service_getter_invoked_per_request(self) -> None:
        """Getter is called lazily at request time, not at build time.

        Given: a callable getter backed by a list the test controls,
        When: two requests hit the sub-app,
        Then: the getter is invoked twice — demonstrating lifespan
            ordering is safe (construction happens before settings
            is ready, requests happen after).
        """
        call_count: dict[str, int] = {"n": 0}
        svc = _make_settings_service(enabled=False)

        def _getter() -> Any:
            call_count["n"] += 1
            return svc

        app = build_mcp_app(settings_service_getter=_getter, repository_getter=lambda: Mock())
        client = TestClient(app)
        client.post("/mcp", json={})
        client.post("/mcp", json={})
        assert call_count["n"] == 2
