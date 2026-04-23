"""MCP sub-application factory.

Constructs the Starlette sub-app mounted under ``/api/mcp``. The
sub-app is ALWAYS mounted; the :class:`FeatureFlagMiddleware`
returns HTTP 503 with ``{"error_code": "feature_disabled"}`` when
the ``ai_integration_enabled`` DB setting is ``False`` (default).
This lets operators flip the flag at runtime without restarting
the API server.
The bearer-header auth extension shipped in
(:func:`snapper.auth.dependencies.get_current_user`) provides the
transport; this module's auth middleware translates its absence /
invalidity into MCP-compatible JSON error responses. Per-tool
fine-grained authorization is handled by the individual tool
wrappers.
"""

from collections.abc import Callable
from contextvars import ContextVar
from typing import Any

from loguru import logger
from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp

from snapper.application.services.settings import SettingsService
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.auth.dependencies import _extract_bearer_token
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.tokens import REJECTION_REASON_USER_DEACTIVATED
from snapper.auth.tokens import get_token_manager
from snapper.data.repository import Repository
from snapper.mcp.rate_limiting import PrincipalRateLimitMiddleware
from snapper.mcp.tools import register_mcp_tools

_MCP_SERVER_NAME = "snapper"
_MCP_SERVER_VERSION = "0.1.0"
_FEATURE_FLAG_KEY = "ai_integration_enabled"

TOKEN_CLAIMS_CTX: ContextVar[TokenClaims | None] = ContextVar("mcp_token_claims", default=None)
"""ContextVar carrying the authenticated :class:`TokenClaims` to tool handlers.

:class:`BearerAuthMiddleware` sets this after verifying the Bearer JWT;
individual tool handlers read it via :func:`get_current_claims` to check
per-tool permissions without needing to plumb the HTTP request through
the FastMCP dispatch layer. ContextVars propagate through the asyncio
task that serves a single MCP call, so the authenticated claims and the
tool handler always see the same context.
"""


def get_current_claims() -> TokenClaims:
    """Return the authenticated :class:`TokenClaims` for the current MCP call.

    Intended to be called from within a FastMCP tool handler. Raises if
    the ContextVar hasn't been set — which should never happen under
    normal middleware wiring because :class:`BearerAuthMiddleware` is
    composed above every tool dispatch and rejects before calling
    downstream.

    Returns:
        The :class:`TokenClaims` stashed by the bearer auth middleware.

    Raises:
        RuntimeError: when the ContextVar is unset — signals a
            middleware-chain misconfiguration.
    """
    claims = TOKEN_CLAIMS_CTX.get()
    if claims is None:
        raise RuntimeError(
            "MCP tool dispatch ran without an authenticated token_claims "
            "ContextVar — BearerAuthMiddleware is misconfigured."
        )
    return claims


class FeatureFlagMiddleware(BaseHTTPMiddleware):
    """Reject every request with 503 when the AI integration flag is off.

    Always-mounted endpoint : the sub-app is installed
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


def _build_rejection_response(rejection_reason: str | None) -> JSONResponse:
    """Return a 401 JSONResponse whose ``error_code`` matches ``rejection_reason``.

     +
    the reason comes straight from
    meth:`TokenManager.verify_token_with_reason` so the classifier
    cannot be fooled by a stale cache entry left over from an
    earlier request. Success is
    never routed here; only rejection reasons land in this
    function.

    Args:
        rejection_reason: One of :data:`REJECTION_REASON_USER_DEACTIVATED`
            (from ``TokenManager``) / :data:`REJECTION_REASON_INVALID` /
            ``None``. ``None`` is treated as ``REJECTION_REASON_INVALID``
            defensively so an unforeseen outcome shape cannot
            accidentally leak a ``user_deactivated`` verdict.

    Returns:
        401 :class:`JSONResponse` with the specific ``error_code``.
    """
    if rejection_reason == REJECTION_REASON_USER_DEACTIVATED:
        return JSONResponse(
            status_code=401,
            content={
                "error_code": "user_deactivated",
                "detail": (
                    "Account has been deactivated. Contact an administrator "
                    "or obtain new credentials via POST /api/auth/login."
                ),
            },
        )
    return JSONResponse(
        status_code=401,
        content={
            "error_code": "invalid_bearer_token",
            "detail": "Bearer token failed verification. Refresh via POST /api/auth/refresh.",
        },
    )


class BearerAuthMiddleware(BaseHTTPMiddleware):
    """Require a valid ``Authorization: Bearer <jwt>`` on every MCP call.

    Rejects with 401 when the header is absent, malformed, or carries
    an unverifiable JWT. The verified :class:`TokenClaims` is stashed
    on ``request.state.token_claims`` for downstream tool dispatch
    The MCP transport has no cookie
    semantics — clients exclusively present the bearer token they
    obtained via ``POST /api/auth/login?return_tokens=true``.
    Verification routes through
    meth:`TokenManager.verify_token_with_db` so each MCP call
    checks the ``user_active_tokens`` inventory + SCD2-active
    ``users.is_active`` via the 30-second LRU cache. Kill-switch
    propagation
        **Same-instance** — immediate. The JTI blacklist seeded
          by :meth:`TokenManager.revoke_user_sessions` is
          consulted inside the sync ``verify_token`` layer BEFORE
          the LRU, so revoked tokens cannot serve from cache.
        **Cross-instance** — bounded by the 30-second LRU TTL
          until the admin-bus subscriber calls
          meth:`TokenManager.invalidate_user_cache` on
          ``admin.user_deactivated``, collapsing latency to one
          bus round-trip.
    The effective ceiling drops from the 15-minute access-token
    TTL to 30 s.
    """

    def __init__(
        self,
        app: Any,
        repository_getter: Callable[[], Repository | None],
    ) -> None:
        """Initialize the middleware with a Repository getter.

        The getter is lazy because :func:`build_mcp_app` runs during
        ``create_app`` — BEFORE the FastAPI lifespan has set up the
        repository singleton. Resolving per-request instead of at
        construction time keeps the sub-app mountable in any order.

        Args:
            app: Downstream ASGI app.
            repository_getter: Zero-arg callable returning the
                shared :class:`Repository` singleton at request
                time. ``None`` at call time surfaces as a 503 so
                MCP requests fail fast during a broken lifespan
                rather than silently skipping the DB-backed check.
        """
        super().__init__(app)
        self._repository_getter = repository_getter

    async def dispatch(self, request: Request, call_next: Any) -> Any:
        """Verify the Bearer token before dispatching to the MCP app.

        The verified :class:`TokenClaims` is made available via two
        mechanisms:

            - ``request.state.token_claims`` — for any downstream
              middleware / Starlette handler that wants to read it.
            - :data:`TOKEN_CLAIMS_CTX` ContextVar — for FastMCP tool
              handlers (which don't see the HTTP request object by
              default). The ContextVar is reset on exit so the claims
              don't leak across unrelated asyncio tasks.

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
        repository = self._repository_getter()
        if repository is None:
            return JSONResponse(
                status_code=503,
                content={
                    "error_code": "mcp_unavailable",
                    "detail": (
                        "MCP repository not initialized — retry after server "
                        "lifespan startup completes."
                    ),
                },
            )
        token_manager = get_token_manager()
        outcome = await token_manager.verify_token_with_reason(token, repository)
        if outcome.claims is None:
            return _build_rejection_response(outcome.rejection_reason)
        claims = outcome.claims
        request.state.token_claims = claims
        ctx_token = TOKEN_CLAIMS_CTX.set(claims)
        try:
            return await call_next(request)
        finally:
            TOKEN_CLAIMS_CTX.reset(ctx_token)


def build_mcp_app(
    settings_service_getter: Callable[[], SettingsService | None],
    repository_getter: Callable[[], Repository | None] | None = None,
    caps_enforcer_getter: Callable[[], TradingCapsEnforcer | None] | None = None,
) -> Starlette:
    """Return the Starlette sub-app to be mounted under ``/api/mcp``.

    Composition order matters (outermost middleware runs first)
        1. :class:`FeatureFlagMiddleware` — cheapest reject path
           short-circuits when the flag is off so disabled
           deployments don't even verify JWTs.
        2. :class:`BearerAuthMiddleware` — auth gate; populates
           ``request.state.token_claims`` before tool dispatch.
        3. :class:`PrincipalRateLimitMiddleware` — per-principal
           throttle keyed off the claims set by (2). +
        4. Downstream FastMCP Streamable HTTP app with tools
           registered via :func:`register_mcp_tools`.

    Args:
        settings_service_getter: Zero-arg callable returning the
            class:`SettingsService` singleton at request time. This
            must be a getter (not the service itself) because the
            sub-app is constructed in ``create_app()`` BEFORE the
            FastAPI lifespan has initialized the settings service.
            The typical wiring is
            ``build_mcp_app(lambda: getattr(app.state, "settings_service", None))``.
        repository_getter: Zero-arg callable returning the shared
            class:`Repository` singleton. Tools read-only methods
            (``list_instruments``, ``list_positions``, etc.) call
            through this. ``None`` at construction time is supported
            and treated as "tools unavailable" at request time — so
            the sub-app can still be mounted before lifespan startup
            completes.
        caps_enforcer_getter: Zero-arg callable returning the shared
            class:`TradingCapsEnforcer` singleton. Write tools
            (``submit_manual_order``, ``cancel_order``) wrap inserts
            in ``guard(submission)`` against this enforcer so
            per-user caps apply to MCP-initiated writes identically
            to REST-initiated writes.

    Returns:
        A Starlette sub-app ready for ``FastAPI.mount("/api/mcp",...)``.
    """
    mcp_server = FastMCP(
        _MCP_SERVER_NAME,
        instructions=f"Snapper MCP endpoint (v{_MCP_SERVER_VERSION}).",
        stateless_http=True,
        streamable_http_path="/",
    )
    register_mcp_tools(
        mcp_server,
        repository_getter=repository_getter or (lambda: None),
        caps_enforcer_getter=caps_enforcer_getter or (lambda: None),
        claims_getter=get_current_claims,
    )
    downstream = mcp_server.streamable_http_app()
    downstream.add_middleware(PrincipalRateLimitMiddleware)
    downstream.add_middleware(
        BearerAuthMiddleware,
        repository_getter=repository_getter or (lambda: None),
    )
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
