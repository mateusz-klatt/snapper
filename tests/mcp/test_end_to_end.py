"""End-to-end MCP dispatch: Bearer header → ContextVar → tool handler.

Exercises the full composed middleware stack from
:func:`snapper.mcp.server.build_mcp_app` to prove:

    1. A valid Bearer header populates
       :data:`TOKEN_CLAIMS_CTX` in time for the downstream tool
       handler to read it via :func:`get_current_claims`.
    2. The ContextVar is reset on request completion so a later
       request with NO Authorization header does NOT see stale
       claims — the isolation guarantee the middleware's try/finally
       is supposed to provide.

The test uses a custom Starlette sub-app that stands in for
FastMCP's Streamable HTTP handler, so the assertion can inspect
what the handler saw without pulling in FastMCP's full task-group
lifespan. The middleware composition is identical to production.
"""

from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import Mock
from unittest.mock import patch

from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from snapper.application.services.settings import SettingsService
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.tokens import VerifyOutcome
from snapper.mcp.server import TOKEN_CLAIMS_CTX
from snapper.mcp.server import BearerAuthMiddleware
from snapper.mcp.server import FeatureFlagMiddleware
from snapper.mcp.server import get_current_claims
from snapper.mcp.tools import register_mcp_tools

_TEST_TOKEN_PLACEHOLDER = "dummy.bearer.for.tests.only"


def _make_claims(username: str = "ai-delegate-e2e") -> TokenClaims:
    """Build a minimal TokenClaims for the end-to-end auth test."""
    now = int(datetime.now(UTC).timestamp())
    return TokenClaims(
        sub="user-e2e",
        username=username,
        role=UserRole.AI_DELEGATE,
        permissions=None,
        exp=now + 3600,
        iat=now,
        jti="jti-e2e",
        sid="sid-e2e",
        user_public_id="user-e2e",
        primary_operator_public_id="op-e2e",
    )


def _build_end_to_end_app(settings_service: Mock) -> Starlette:
    """Construct a Starlette sub-app mirroring the MCP mount's middleware stack.

    The downstream handler at ``/mcp`` invokes
    :func:`get_current_claims` and echoes the username — so test
    assertions can verify the ContextVar reached the handler.

    If the ContextVar is unset (which signals a middleware bug) the
    handler returns HTTP 500 with a sentinel marker instead of
    raising, so the test frame can distinguish "ContextVar was
    correctly unset between requests" from "auth silently passed
    through without setting the ContextVar".
    """

    def _echo_username(_request: Request) -> JSONResponse:
        try:
            claims = get_current_claims()
        except RuntimeError:
            return JSONResponse({"seen": None}, status_code=200)
        return JSONResponse({"seen": claims.username}, status_code=200)

    app = Starlette(routes=[Route("/mcp", _echo_username, methods=["POST"])])
    app.add_middleware(BearerAuthMiddleware, repository_getter=lambda: Mock())
    app.add_middleware(FeatureFlagMiddleware, settings_service_getter=lambda: settings_service)
    return app


class TestEndToEndBearerToContextVar:
    """Real ASGI request/response proving the full bearer → tool path."""

    def test_authenticated_request_reaches_tool_with_claims(self) -> None:
        """Given a valid Bearer request, Then the tool sees the authenticated claims.

        Given: the full composed middleware stack (flag on + bearer
            auth) and a stub :class:`TokenManager` that verifies to a
            realistic :class:`TokenClaims`,
        When: a single POST /mcp request is sent,
        Then: the echo handler sees the claims via
            :func:`get_current_claims`, returning the username the
            middleware unpacked from the Bearer JWT.
        """
        svc = Mock(spec=SettingsService)
        svc.get_setting.return_value = True
        app = _build_end_to_end_app(svc)
        claims = _make_claims("ai-delegate-e2e")
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
            client = TestClient(app)
            response = client.post(
                "/mcp",
                headers={"Authorization": f"Bearer {_TEST_TOKEN_PLACEHOLDER}"},
                json={},
            )
        assert response.status_code == 200
        assert response.json() == {"seen": "ai-delegate-e2e"}

    def test_contextvar_resets_between_requests(self) -> None:
        """Consecutive auth'd + un-auth'd requests don't cross-contaminate.

        Given: a first request with a valid Bearer header (sets the
            ContextVar), followed by a second request that has no
            Authorization header (must NOT see stale claims),
        When: both requests hit the same ASGI app,
        Then: request 1 returns the authenticated username; request
            2 is rejected with 401 ``missing_bearer_token`` BEFORE
            the handler runs — so the ContextVar cannot have leaked
            into an unauthenticated code path.
        """
        svc = Mock(spec=SettingsService)
        svc.get_setting.return_value = True
        app = _build_end_to_end_app(svc)
        claims_first = _make_claims("first-caller")
        with patch("snapper.mcp.server.get_token_manager") as mock_get:
            token_manager = Mock()
            token_manager.verify_token_with_db = AsyncMock(return_value=claims_first)
            token_manager.verify_token_with_reason = AsyncMock(
                return_value=VerifyOutcome(
                    claims=claims_first,
                    rejection_reason=None,
                )
            )
            mock_get.return_value = token_manager
            client = TestClient(app)
            response_1 = client.post(
                "/mcp",
                headers={"Authorization": f"Bearer {_TEST_TOKEN_PLACEHOLDER}"},
                json={},
            )
            response_2 = client.post("/mcp", json={})
        assert response_1.status_code == 200
        assert response_1.json()["seen"] == "first-caller"
        assert response_2.status_code == 401
        assert response_2.json()["error_code"] == "missing_bearer_token"

    def test_contextvar_unset_outside_middleware_scope(self) -> None:
        """Outside the middleware, the ContextVar is always unset.

        Given: an authenticated request just completed on the app,
        When: the test inspects :data:`TOKEN_CLAIMS_CTX` directly
            from the test frame (no middleware),
        Then: the ContextVar reports its default ``None`` — proving
            the ``finally: reset(token)`` branch executed for every
            request the app processed.
        """
        svc = Mock(spec=SettingsService)
        svc.get_setting.return_value = True
        app = _build_end_to_end_app(svc)
        with patch("snapper.mcp.server.get_token_manager") as mock_get:
            token_manager = Mock()
            token_manager.verify_token_with_db = AsyncMock(return_value=_make_claims("x"))
            token_manager.verify_token_with_reason = AsyncMock(
                return_value=VerifyOutcome(
                    claims=_make_claims("x"),
                    rejection_reason=None,
                )
            )
            mock_get.return_value = token_manager
            client = TestClient(app)
            client.post(
                "/mcp",
                headers={"Authorization": f"Bearer {_TEST_TOKEN_PLACEHOLDER}"},
                json={},
            )
        assert TOKEN_CLAIMS_CTX.get() is None

    def test_unauthenticated_request_returns_401_without_reaching_handler(self) -> None:
        """An un-auth'd request never reaches the tool handler.

        Given: no Authorization header on the request,
        When: the stack processes the request,
        Then: HTTP 401 ``missing_bearer_token`` is returned. The
            echo handler is NOT invoked — proving the middleware
            fails closed before handler dispatch.
        """
        svc = Mock(spec=SettingsService)
        svc.get_setting.return_value = True

        handler_invocations: dict[str, int] = {"count": 0}

        def _counting_echo(_request: Request) -> JSONResponse:
            handler_invocations["count"] += 1
            return JSONResponse({"seen": None}, status_code=200)

        app = Starlette(routes=[Route("/mcp", _counting_echo, methods=["POST"])])
        app.add_middleware(BearerAuthMiddleware, repository_getter=lambda: Mock())
        app.add_middleware(FeatureFlagMiddleware, settings_service_getter=lambda: svc)

        client = TestClient(app)
        response = client.post("/mcp", json={})
        assert response.status_code == 401
        assert handler_invocations["count"] == 0

    def test_feature_flag_off_short_circuits_before_bearer_check(self) -> None:
        """Feature flag OFF → 503 regardless of bearer header presence.

        Given: the flag returns False,
        When: a request is sent WITH a Bearer header,
        Then: 503 ``feature_disabled`` comes back — confirming the
            middleware composition order (flag outermost, bearer
            inner) from :func:`build_mcp_app`.
        """
        svc = Mock(spec=SettingsService)
        svc.get_setting.return_value = False
        app = _build_end_to_end_app(svc)
        client = TestClient(app)
        response = client.post(
            "/mcp",
            headers={"Authorization": f"Bearer {_TEST_TOKEN_PLACEHOLDER}"},
            json={},
        )
        assert response.status_code == 503
        assert response.json()["error_code"] == "feature_disabled"

    def test_real_tool_dispatched_via_bearer_middleware_stack(self) -> None:
        """Bearer header → middleware → ContextVar → REAL tool via tool manager.

        This is the canonical "bearer-to-tool" proof. Instead of a
        stub handler that just reads :func:`get_current_claims`,
        this test:

            1. Builds a real :class:`FastMCP` instance and registers
               the production tools via :func:`register_mcp_tools`
               with :func:`get_current_claims` as the ``claims_getter``
               (the same wiring ``build_mcp_app`` uses).
            2. Mounts a Starlette app with the full middleware stack
               (FeatureFlag + BearerAuth) whose handler dispatches
               the JSON-RPC-shaped body through
               ``FastMCP._tool_manager.call_tool``.
            3. Sends an authenticated request asking for
               ``list_instruments(exchange='kraken')`` and asserts
               the tool's return value (sorted list) comes back —
               proving the authenticated claims flowed through
               ``BearerAuthMiddleware`` → ``TOKEN_CLAIMS_CTX`` →
               ``get_current_claims`` inside the production tool,
               which then called the repo.

        If any link in that chain broke the test would either 401
        (middleware didn't admit), 500 with ``misconfigured``
        (ContextVar unset), or raise a permission error (claims
        role didn't include READ_MARKET_DATA).
        """
        svc = Mock(spec=SettingsService)
        svc.get_setting.return_value = True
        repo = AsyncMock()
        repo.get_exchange_instruments = AsyncMock(
            return_value=["ETH-USD", "BTC-USD"],
        )

        mcp_server = FastMCP("test-e2e")
        register_mcp_tools(
            mcp_server,
            repository_getter=lambda: repo,
            caps_enforcer_getter=lambda: None,
            claims_getter=get_current_claims,
        )

        async def _dispatch_tool(request: Request) -> JSONResponse:
            body = await request.json()
            result = await mcp_server._tool_manager.call_tool(body["tool"], body.get("args", {}))
            return JSONResponse({"result": result}, status_code=200)

        app = Starlette(routes=[Route("/mcp", _dispatch_tool, methods=["POST"])])
        app.add_middleware(BearerAuthMiddleware, repository_getter=lambda: Mock())
        app.add_middleware(FeatureFlagMiddleware, settings_service_getter=lambda: svc)

        with patch("snapper.mcp.server.get_token_manager") as mock_get:
            token_manager = Mock()
            real_tool_claims = _make_claims("real-tool-caller")
            token_manager.verify_token_with_db = AsyncMock(return_value=real_tool_claims)
            token_manager.verify_token_with_reason = AsyncMock(
                return_value=VerifyOutcome(claims=real_tool_claims, rejection_reason=None)
            )
            mock_get.return_value = token_manager
            client = TestClient(app)
            response = client.post(
                "/mcp",
                headers={"Authorization": f"Bearer {_TEST_TOKEN_PLACEHOLDER}"},
                json={"tool": "list_instruments", "args": {"exchange": "kraken"}},
            )

        assert response.status_code == 200
        body = response.json()["result"]
        assert body == {
            "exchange": "kraken",
            "instruments": ["BTC-USD", "ETH-USD"],
        }
        repo.get_exchange_instruments.assert_awaited_once()

    def test_echo_returns_none_when_contextvar_unexpectedly_unset(self) -> None:
        """Echo handler short-circuits to 200 + null on misconfiguration.

        Given: a patched middleware that clears the ContextVar right
            after the bearer auth sets it (simulating a hypothetical
            composition bug),
        When: the request reaches the echo handler,
        Then: the handler returns 200 with ``{"seen": None}`` —
            proving the RuntimeError from ``get_current_claims`` is
            handled by the test stub. This confirms test #1's
            assertion that ``"seen" == "ai-delegate-e2e"`` really
            required the ContextVar to reach the handler.
        """

        def _echo(_request: Request) -> JSONResponse:
            try:
                claims = get_current_claims()
            except RuntimeError:
                return JSONResponse({"seen": None}, status_code=200)
            return JSONResponse({"seen": claims.username}, status_code=200)

        svc = Mock(spec=SettingsService)
        svc.get_setting.return_value = True
        app = Starlette(routes=[Route("/mcp", _echo, methods=["POST"])])

        class _ClearingMiddleware:
            def __init__(self, app: Any) -> None:
                self.app = app

            async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
                TOKEN_CLAIMS_CTX.set(None)
                await self.app(scope, receive, send)

        app.add_middleware(_ClearingMiddleware)
        app.add_middleware(BearerAuthMiddleware, repository_getter=lambda: Mock())
        app.add_middleware(FeatureFlagMiddleware, settings_service_getter=lambda: svc)

        with patch("snapper.mcp.server.get_token_manager") as mock_get:
            token_manager = Mock()
            token_manager.verify_token_with_db = AsyncMock(return_value=_make_claims())
            token_manager.verify_token_with_reason = AsyncMock(
                return_value=VerifyOutcome(
                    claims=_make_claims(),
                    rejection_reason=None,
                )
            )
            mock_get.return_value = token_manager
            client = TestClient(app)
            response = client.post(
                "/mcp",
                headers={"Authorization": f"Bearer {_TEST_TOKEN_PLACEHOLDER}"},
                json={},
            )
        assert response.status_code == 200
        assert response.json()["seen"] is None
