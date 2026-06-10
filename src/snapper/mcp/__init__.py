"""MCP (Model Context Protocol) sub-application.

 Exposes ``/api/mcp`` as a Starlette sub-application mounted
under the main FastAPI app via :func:`build_mcp_app`. Because
Starlette does not propagate parent-app ``BaseHTTPMiddleware``
stacks (SlowAPIMiddleware, ClientProvenanceMiddleware) into
mounted sub-apps, the sub-app re-applies what it needs inside the
mount.
Composed layers inside :func:`build_mcp_app` (outermost first)
    1. :class:`FeatureFlagMiddleware` — 503 when
       ``ai_integration_enabled`` is off.
    2. :class:`BearerAuthMiddleware` — populates
       ``request.state.token_claims`` via the DB-backed
       verify + admin-bus-driven cache eviction.
    3. :class:`PrincipalRateLimitMiddleware` — per-principal
       throttle reading ``request.state.token_claims`` set by the
       middleware above.
    4. Downstream FastMCP Streamable HTTP dispatcher with tools
       registered via :func:`register_mcp_tools`.
CORS is NOT re-applied: :class:`~fastapi.middleware.cors.CORSMiddleware`
on the parent FastAPI app IS reached by sub-app requests because
it operates at the raw-ASGI layer below ``BaseHTTPMiddleware``.
Output sanitization runs inside each individual tool handler via
:func:`~snapper.mcp.output_sanitizer.sanitize_output` rather than
as a middleware layer.
"""
