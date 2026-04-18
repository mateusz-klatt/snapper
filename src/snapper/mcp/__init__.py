"""MCP (Model Context Protocol) sub-application.

Phase A exposes ``/api/mcp`` as a Starlette sub-application mounted
under the main FastAPI app via :func:`build_mcp_app`. Parent
middleware (CORS, SlowAPIMiddleware, ClientProvenanceMiddleware) does
NOT propagate into mounted sub-apps in Starlette, so this package
explicitly re-applies the necessary middleware + auth + rate
limiting + output sanitization inside the sub-app itself.

See ``proprietary/plans/plan_ai_integration_phase_a.md`` §3.2.
"""
