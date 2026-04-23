"""Vendor-neutral contract tests for the MCP endpoint.

The MCP surface MUST stay callable by any MCP-compatible client, not
just the Anthropic / OpenAI / Cursor wrappers we happen to ship first.
These tests exercise the observable HTTP + JSON-RPC contract the
specification guarantees, using only:

    - raw :mod:`httpx`-style ASGI calls through :class:`TestClient`,
    - the standard :class:`Authorization: Bearer <jwt>` header,
    - JSON bodies with no vendor-specific extension fields,
    - the structured :class:`error_code` envelope documented in
      ``docs/ai-integration.md``.

If a future change leaks a vendor-specific header, cookie, or
redirect into the MCP mount, one of these assertions will fail.
The tests pair with the ``make check-vendor-neutral`` grep gate so
the wire-level contract and the source-text contract guard the
same boundary from two directions.
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
from snapper.auth.tokens import REJECTION_REASON_USER_DEACTIVATED
from snapper.auth.tokens import VerifyOutcome
from snapper.mcp.server import BearerAuthMiddleware
from snapper.mcp.server import FeatureFlagMiddleware
from snapper.mcp.server import get_current_claims

_MCP_PATH = "/mcp"
_PLACEHOLDER_TOKEN = "contract.bearer.placeholder"
_JSONRPC_INITIALIZE: dict[str, Any] = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2024-11-05",
        "capabilities": {},
        "clientInfo": {"name": "vendor-neutral-contract-client", "version": "0.0.1"},
    },
}


def _valid_claims() -> TokenClaims:
    """Build a :class:`TokenClaims` representing a live delegate session."""
    now = int(datetime.now(UTC).timestamp())
    return TokenClaims(
        sub="user-contract",
        username="delegate-contract",
        role=UserRole.AI_DELEGATE,
        permissions=[],
        exp=now + 3600,
        iat=now,
        jti="jti-contract",
        sid="sid-contract",
        user_public_id="user-contract",
        primary_operator_public_id="op-contract",
    )


def _build_contract_app(settings_service: Mock) -> Starlette:
    """Compose the production middleware stack around an echo handler.

    Mirrors :func:`snapper.mcp.server.build_mcp_app` but substitutes a
    deterministic echo route for the downstream :mod:`FastMCP`
    dispatcher so the tests can assert on the exact response body
    without wrestling with the SDK's task-group lifecycle.

    Args:
        settings_service: Mock exposing ``get_setting`` — the feature
            flag middleware reads it at request time.

    Returns:
        A :class:`Starlette` app with the real
        :class:`FeatureFlagMiddleware` + :class:`BearerAuthMiddleware`
        composed in production order.
    """

    def _echo(_request: Request) -> JSONResponse:
        try:
            claims = get_current_claims()
        except RuntimeError:
            return JSONResponse({"error": "no_claims"}, status_code=500)
        return JSONResponse(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "result": {
                    "serverInfo": {"name": "snapper", "version": "0.1.0"},
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}},
                    "user": claims.username,
                },
            },
            status_code=200,
        )

    app = Starlette(routes=[Route(_MCP_PATH, _echo, methods=["POST"])])
    app.add_middleware(BearerAuthMiddleware, repository_getter=lambda: Mock())
    app.add_middleware(FeatureFlagMiddleware, settings_service_getter=lambda: settings_service)
    return app


def _flag_on_service() -> Mock:
    """Return a :class:`SettingsService` mock with the AI flag enabled."""
    svc = Mock(spec=SettingsService)
    svc.get_setting.return_value = True
    return svc


def _flag_off_service() -> Mock:
    """Return a :class:`SettingsService` mock with the AI flag disabled."""
    svc = Mock(spec=SettingsService)
    svc.get_setting.return_value = False
    return svc


class TestMcpFeatureFlagContract:
    """The feature flag is the outermost gate — cheapest reject path."""

    def test_flag_off_returns_503_feature_disabled(self) -> None:
        """Flag off → HTTP 503 with ``error_code='feature_disabled'``.

        Given: the ``ai_integration_enabled`` setting is ``False``,
        When: any MCP-compatible client POSTs to the endpoint,
        Then: the response is HTTP 503 with a structured
            ``error_code`` body clients can branch on without parsing
            free-text ``detail`` strings.
        """
        app = _build_contract_app(_flag_off_service())
        client = TestClient(app)

        response = client.post(
            _MCP_PATH,
            headers={"Authorization": f"Bearer {_PLACEHOLDER_TOKEN}"},
            json=_JSONRPC_INITIALIZE,
        )

        assert response.status_code == 503
        body = response.json()
        assert body["error_code"] == "feature_disabled"

    def test_settings_not_ready_returns_503_feature_disabled(self) -> None:
        """Pre-lifespan settings service → HTTP 503 with the same code.

        Given: the settings service resolver returns ``None`` (lifespan
            has not initialized the singleton yet),
        When: an MCP request arrives,
        Then: the endpoint treats the unresolved state as flag-off for
            safety and emits the same ``feature_disabled`` code — the
            wire contract is identical regardless of WHICH side of the
            startup race the caller lands on.
        """

        def _echo(_request: Request) -> JSONResponse:
            return JSONResponse({"ok": True}, status_code=200)

        app = Starlette(routes=[Route(_MCP_PATH, _echo, methods=["POST"])])
        app.add_middleware(FeatureFlagMiddleware, settings_service_getter=lambda: None)
        client = TestClient(app)

        response = client.post(
            _MCP_PATH,
            headers={"Authorization": f"Bearer {_PLACEHOLDER_TOKEN}"},
            json=_JSONRPC_INITIALIZE,
        )

        assert response.status_code == 503
        assert response.json()["error_code"] == "feature_disabled"


class TestMcpBearerAuthContract:
    """Bearer header is the exclusive auth transport for MCP."""

    def test_missing_bearer_header_returns_401_missing(self) -> None:
        """No Authorization header → ``missing_bearer_token`` 401.

        Given: a feature-flag-on app,
        When: a client POSTs with no ``Authorization`` header
            (and no cookie),
        Then: the response is 401 with
            ``error_code='missing_bearer_token'`` — cookie auth is
            intentionally NOT accepted on the MCP mount.
        """
        app = _build_contract_app(_flag_on_service())
        client = TestClient(app)

        response = client.post(_MCP_PATH, json=_JSONRPC_INITIALIZE)

        assert response.status_code == 401
        assert response.json()["error_code"] == "missing_bearer_token"

    def test_malformed_bearer_returns_401_missing(self) -> None:
        """Non-Bearer scheme → still ``missing_bearer_token``.

        Given: an ``Authorization: Basic …`` header,
        When: the client calls MCP,
        Then: the middleware treats the header as absent — MCP
            accepts ONLY the Bearer scheme, so any other scheme maps
            to the same missing-token code.
        """
        app = _build_contract_app(_flag_on_service())
        client = TestClient(app)

        response = client.post(
            _MCP_PATH,
            headers={"Authorization": "Basic dXNlcjpwYXNz"},
            json=_JSONRPC_INITIALIZE,
        )

        assert response.status_code == 401
        assert response.json()["error_code"] == "missing_bearer_token"

    def test_invalid_bearer_returns_401_invalid(self) -> None:
        """Signature / blacklist failure → ``invalid_bearer_token``.

        Given: a Bearer token whose verification returns
            ``VerifyOutcome(claims=None, rejection_reason=REJECTION_REASON_INVALID)``,
        When: the client calls MCP,
        Then: the middleware returns 401 with
            ``error_code='invalid_bearer_token'`` so clients know a
            refresh attempt is worthwhile.
        """
        app = _build_contract_app(_flag_on_service())
        with patch("snapper.mcp.server.get_token_manager") as mock_get:
            tm = Mock()
            tm.verify_token_with_reason = AsyncMock(
                return_value=VerifyOutcome(
                    claims=None,
                    rejection_reason=REJECTION_REASON_INVALID,
                )
            )
            mock_get.return_value = tm
            client = TestClient(app)
            response = client.post(
                _MCP_PATH,
                headers={"Authorization": f"Bearer {_PLACEHOLDER_TOKEN}"},
                json=_JSONRPC_INITIALIZE,
            )

        assert response.status_code == 401
        assert response.json()["error_code"] == "invalid_bearer_token"

    def test_deactivated_user_returns_401_user_deactivated(self) -> None:
        """Deactivated owner → ``user_deactivated`` (distinct from invalid).

        Given: a verification outcome signalling the owner account is
            deactivated,
        When: the client calls MCP,
        Then: the 401 body carries ``error_code='user_deactivated'`` —
            clients should re-login instead of auto-refreshing, the
            one client-observable state that distinguishes these two
            401 branches.
        """
        app = _build_contract_app(_flag_on_service())
        with patch("snapper.mcp.server.get_token_manager") as mock_get:
            tm = Mock()
            tm.verify_token_with_reason = AsyncMock(
                return_value=VerifyOutcome(
                    claims=None,
                    rejection_reason=REJECTION_REASON_USER_DEACTIVATED,
                )
            )
            mock_get.return_value = tm
            client = TestClient(app)
            response = client.post(
                _MCP_PATH,
                headers={"Authorization": f"Bearer {_PLACEHOLDER_TOKEN}"},
                json=_JSONRPC_INITIALIZE,
            )

        assert response.status_code == 401
        assert response.json()["error_code"] == "user_deactivated"

    def test_valid_bearer_reaches_handler_with_claims(self) -> None:
        """Valid Bearer + flag on → handler receives the authenticated claims.

        Given: a verification outcome returning fresh claims,
        When: the client sends the standard JSON-RPC ``initialize``,
        Then: the handler echoes the claims username, proving the
            middleware pipeline makes the authenticated principal
            available via :data:`TOKEN_CLAIMS_CTX` — the contract
            downstream tools rely on.
        """
        app = _build_contract_app(_flag_on_service())
        claims = _valid_claims()
        with patch("snapper.mcp.server.get_token_manager") as mock_get:
            tm = Mock()
            tm.verify_token_with_reason = AsyncMock(
                return_value=VerifyOutcome(claims=claims, rejection_reason=None)
            )
            mock_get.return_value = tm
            client = TestClient(app)
            response = client.post(
                _MCP_PATH,
                headers={"Authorization": f"Bearer {_PLACEHOLDER_TOKEN}"},
                json=_JSONRPC_INITIALIZE,
            )

        assert response.status_code == 200
        body = response.json()
        assert body["jsonrpc"] == "2.0"
        assert body["result"]["serverInfo"]["name"] == "snapper"
        assert body["result"]["user"] == "delegate-contract"

    def test_repository_getter_none_returns_503_mcp_unavailable(self) -> None:
        """Repository resolver returning ``None`` → ``mcp_unavailable``.

        Given: the lifespan has not yet populated the repository
            singleton so the middleware's getter returns ``None``,
        When: an authenticated request arrives,
        Then: the response is 503 with
            ``error_code='mcp_unavailable'`` — surfaces as a distinct
            transient outage, separate from the deliberate
            ``feature_disabled`` state.
        """

        def _echo(_request: Request) -> JSONResponse:
            return JSONResponse({"ok": True}, status_code=200)

        app = Starlette(routes=[Route(_MCP_PATH, _echo, methods=["POST"])])
        app.add_middleware(BearerAuthMiddleware, repository_getter=lambda: None)
        app.add_middleware(
            FeatureFlagMiddleware,
            settings_service_getter=lambda: _flag_on_service(),
        )
        client = TestClient(app)

        response = client.post(
            _MCP_PATH,
            headers={"Authorization": f"Bearer {_PLACEHOLDER_TOKEN}"},
            json=_JSONRPC_INITIALIZE,
        )

        assert response.status_code == 503
        assert response.json()["error_code"] == "mcp_unavailable"
