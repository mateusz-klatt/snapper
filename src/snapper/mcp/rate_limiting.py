"""Per-principal rate limiter for the MCP sub-app.

Parent-app slowapi middleware does not propagate into Starlette
sub-apps mounted via ``app.mount`` (the same reason
:class:`snapper.mcp.server.BearerAuthMiddleware` is re-applied
here). Without a dedicated limiter, an automated AI client in a
retry loop could pound ``/api/mcp`` faster than any other surface
in Snapper. This module closes that gap by wiring a
Starlette middleware that consumes one
:class:`~limits.limits.RateLimitItem` per request, keyed by the
authenticated principal.
Design choices
    **Keying** — we prefer ``user_public_id`` from the
      :class:`~snapper.auth.schemas.tokens.TokenClaims` stashed on
      ``request.state`` by :class:`BearerAuthMiddleware`. If the
      claims never landed (misordered middleware), the limiter
      falls back to the client IP so a misconfigured deployment
      still has some throttling rather than none.
    **Storage** — the slowapi :class:`Limiter` singleton
      exported by :mod:`snapper.server.rate_limiting` carries the
      same in-memory backend the REST routes use, so there is no
      separate metrics surface to maintain. Keys are namespaced
      with the ``"mcp:"`` prefix so they do not collide with REST
      or login counters.
    **Error shape** — we emit a 429 JSON body with
      ``error_code='rate_limit_exceeded'`` matching the
      vendor-neutral envelope ``docs/ai-integration.md`` documents
      and the contract tests pin.
The concrete cap (:data:`MCP_RATE_LIMIT`) is intentionally
conservative for a default. Operators can raise it later
via configuration; the middleware does not try to be clever about
per-tool budgets yet.
"""

from typing import Any
from typing import Final

from limits import parse
from limits.limits import RateLimitItem
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp

from snapper.auth.schemas.tokens import TokenClaims
from snapper.server.rate_limiting import limiter

MCP_RATE_LIMIT: Final[str] = "60/minute"
"""Default per-principal MCP quota.

60 requests per minute per authenticated delegate is enough for
interactive usage from a desktop MCP client with a human in the
loop, but bounds a runaway retry loop to roughly one request per
second. Raising the cap is a deployment-time choice; hard-coding a
higher default risks letting abusive traffic through on fresh
installs.
"""

_MCP_RATE_LIMIT_ITEM: Final[RateLimitItem] = parse(MCP_RATE_LIMIT)

_MCP_KEY_NAMESPACE: Final[str] = "mcp:"
"""Key prefix so MCP counters don't collide with REST login counters."""


def _principal_key(request: Request) -> str:
    """Return the limiter key for the current MCP request.

    Prefers the authenticated ``user_public_id`` so two clients
    sharing an outbound NAT IP cannot starve each other. Falls back
    to the client host when claims are absent so a partially-failed
    middleware stack still throttles.

    Args:
        request: Inbound Starlette request.

    Returns:
        Namespaced limiter key. Always non-empty so the underlying
        storage engine never receives a blank identifier.
    """
    claims: TokenClaims | None = getattr(request.state, "token_claims", None)
    if claims is not None and claims.user_public_id:
        return f"{_MCP_KEY_NAMESPACE}user:{claims.user_public_id}"
    if claims is not None and claims.sub:
        return f"{_MCP_KEY_NAMESPACE}sub:{claims.sub}"
    client = request.client
    if client is not None and client.host:
        return f"{_MCP_KEY_NAMESPACE}ip:{client.host}"
    return f"{_MCP_KEY_NAMESPACE}anonymous"


class PrincipalRateLimitMiddleware(BaseHTTPMiddleware):
    """Throttle MCP requests by authenticated principal.

    Sits BELOW :class:`BearerAuthMiddleware` in the composition
    order so ``request.state.token_claims`` is already populated
    when the limiter key is derived. The middleware short-circuits
    the downstream dispatch with a 429 JSON response when the
    per-principal window is exhausted; successful calls consume
    one slot via :meth:`limiter.limiter.hit` AFTER
    :meth:`limiter.limiter.test` passes.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        rate_limit_item: RateLimitItem = _MCP_RATE_LIMIT_ITEM,
    ) -> None:
        """Initialise with the shared slowapi backend.

        Args:
            app: Downstream ASGI app.
            rate_limit_item: Parsed limit (default
                :data:`_MCP_RATE_LIMIT_ITEM`); tests override to
                exercise the 429 branch with a small burst.
        """
        super().__init__(app)
        self._rate_limit_item = rate_limit_item

    async def dispatch(self, request: Request, call_next: Any) -> Any:
        """Test + hit the limiter; dispatch or reject.

        Args:
            request: Incoming Starlette request.
            call_next: Downstream ASGI app callable.

        Returns:
            Either a 429 :class:`JSONResponse` when the principal's
            window is exhausted, or the downstream response when
            admitted. The ``Retry-After`` header is set on 429 so
            clients with an HTTP-compliant backoff strategy see the
            window reset.
        """
        if not limiter.enabled:
            return await call_next(request)
        key = _principal_key(request)
        if not limiter.limiter.test(self._rate_limit_item, key):
            return JSONResponse(
                status_code=429,
                content={
                    "error_code": "rate_limit_exceeded",
                    "detail": (
                        "Per-principal MCP rate limit exceeded. Retry once the "
                        "sliding window resets."
                    ),
                },
                headers={"Retry-After": "60"},
            )
        limiter.limiter.hit(self._rate_limit_item, key)
        return await call_next(request)
