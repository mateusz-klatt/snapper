"""MCP sub-application factory (plan §3.2 + §3.12).

Constructs the Starlette sub-app mounted under ``/api/mcp``. The
sub-app is ALWAYS mounted; the :class:`FeatureFlagMiddleware`
returns HTTP 503 with ``{"error_code": "feature_disabled"}`` when
the ``ai_integration_enabled`` DB setting is ``False`` (default).
This lets operators flip the flag at runtime without restarting
the API server.

The bearer-header auth extension shipped in Day 2a
(:func:`snapper.auth.dependencies.get_current_user`) provides the
transport; this module's auth middleware translates its absence /
invalidity into MCP-compatible JSON error responses. Per-tool
fine-grained authorization is handled by the individual tool
wrappers (Day 2c scope).
"""

from collections.abc import Callable
from typing import Any

from loguru import logger
from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp

from snapper.application.services.settings import SettingsService
from snapper.auth.dependencies import _extract_bearer_token
from snapper.auth.tokens import get_token_manager

_MCP_SERVER_NAME = "snapper"
_MCP_SERVER_VERSION = "0.1.0"
_FEATURE_FLAG_KEY = "ai_integration_enabled"


class FeatureFlagMiddleware(BaseHTTPMiddleware):
    """Reject every request with 503 when the AI integration flag is off.

    Always-mounted endpoint per plan §3.12: the sub-app is installed
    at app startup unconditionally; toggling the DB setting flips
    availability without a restart. A 503 response with
    ``error_code="feature_disabled"`` lets MCP clients distinguish a
    deliberately-disabled endpoint from a networking outage.
    """

    def __init__(
        self,
        app: ASGIApp,
        settings_service_getter: Callable[[], SettingsService | None],
    ) -> None:
        """Initialize with a lazy :class:`SettingsService` resolver.

        The resolver is invoked at request time, not construction
        time, because the sub-app is built inside ``create_app()``
        (before the FastAPI lifespan has initialized the settings
        singleton) and mounted on the parent FastAPI app. At request
        time the lifespan has already run, so ``app.state.settings_service``
        is populated.

        Args:
            app: Downstream ASGI app this middleware wraps.
            settings_service_getter: Zero-arg callable returning the
                initialized :class:`SettingsService`, or ``None`` if
                the service hasn't been initialized yet (misconfigured
                lifespan — treated as flag-off for safety).
        """
        super().__init__(app)
        self._settings_service_getter = settings_service_getter

    async def dispatch(self, request: Request, call_next: Any) -> Any:
        """Short-circuit when the flag is off, pass through when on.

        Args:
            request: Incoming Starlette request.
            call_next: Downstream ASGI app callable.

        Returns:
            Either a 503 :class:`JSONResponse` or the downstream
            response, depending on the flag value.
        """
        settings_service = self._settings_service_getter()
        if settings_service is None:
            return JSONResponse(
                status_code=503,
                content={
                    "error_code": "feature_disabled",
                    "detail": (
                        "Settings service not yet initialized. MCP "
                        "endpoint is unavailable until the API "
                        "lifespan completes startup."
                    ),
                },
            )
        enabled = settings_service.get_setting(_FEATURE_FLAG_KEY, default=False)
        if not enabled:
            return JSONResponse(
                status_code=503,
                content={
                    "error_code": "feature_disabled",
                    "detail": (
                        "AI integration is disabled. Enable the "
                        f"'{_FEATURE_FLAG_KEY}' setting to activate "
                        "the /api/mcp endpoint."
                    ),
                },
            )
        return await call_next(request)


class BearerAuthMiddleware(BaseHTTPMiddleware):
    """Require a valid ``Authorization: Bearer <jwt>`` on every MCP call.

    Rejects with 401 when the header is absent, malformed, or carries
    an unverifiable JWT. The verified :class:`TokenClaims` is stashed
    on ``request.state.token_claims`` for downstream tool dispatch
    (Day 2c wires per-tool permission checks off this state).

    The MCP transport (Streamable HTTP per plan §3.2) has no cookie
    semantics — clients exclusively present the bearer token they
    obtained via ``POST /api/auth/login?return_tokens=true`` (Day 2a).
    """

    async def dispatch(self, request: Request, call_next: Any) -> Any:
        """Verify the Bearer token before dispatching to the MCP app.

        Args:
            request: Incoming Starlette request.
            call_next: Downstream ASGI app callable.

        Returns:
            Either a 401 :class:`JSONResponse` or the downstream
            response when auth succeeds.
        """
        token = _extract_bearer_token(request)
        if token is None:
            return JSONResponse(
                status_code=401,
                content={
                    "error_code": "missing_bearer_token",
                    "detail": (
                        "MCP requires an Authorization: Bearer <jwt> "
                        "header. Obtain tokens via "
                        "POST /api/auth/login?return_tokens=true."
                    ),
                },
            )
        token_manager = get_token_manager()
        claims = token_manager.verify_token(token)
        if claims is None:
            return JSONResponse(
                status_code=401,
                content={
                    "error_code": "invalid_bearer_token",
                    "detail": "Bearer token failed verification. Refresh via POST /api/auth/refresh.",
                },
            )
        request.state.token_claims = claims
        return await call_next(request)


def build_mcp_app(
    settings_service_getter: Callable[[], SettingsService | None],
) -> Starlette:
    """Return the Starlette sub-app to be mounted under ``/api/mcp``.

    Composition order matters (outermost middleware runs first):

        1. :class:`FeatureFlagMiddleware` — cheapest reject path;
           short-circuits when the flag is off so disabled
           deployments don't even verify JWTs.
        2. :class:`BearerAuthMiddleware` — auth gate; populates
           ``request.state.token_claims`` before tool dispatch.
        3. Downstream FastMCP Streamable HTTP app serving the JSON-
           RPC protocol.

    Args:
        settings_service_getter: Zero-arg callable returning the
            :class:`SettingsService` singleton at request time. This
            must be a getter (not the service itself) because the
            sub-app is constructed in ``create_app()`` BEFORE the
            FastAPI lifespan has initialized the settings service.
            The typical wiring is
            ``build_mcp_app(lambda: getattr(app.state, "settings_service", None))``.

    Returns:
        A Starlette sub-app ready for ``FastAPI.mount("/api/mcp", ...)``.
    """
    mcp_server = FastMCP(
        _MCP_SERVER_NAME,
        instructions=f"Snapper MCP endpoint (v{_MCP_SERVER_VERSION}) — plan §3.2",
        stateless_http=True,
    )
    downstream = mcp_server.streamable_http_app()
    downstream.add_middleware(BearerAuthMiddleware)
    downstream.add_middleware(
        FeatureFlagMiddleware,
        settings_service_getter=settings_service_getter,
    )
    logger.info(
        "MCP sub-app built ({}, v{}) — mount path: /api/mcp",
        _MCP_SERVER_NAME,
        _MCP_SERVER_VERSION,
    )
    return downstream
