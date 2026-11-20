"""Shared segment-aware path matchers for OAuth credential and surface endpoints.

Used by provenance control-table suppression, uvicorn access-log query
redaction, and HTTP response no-store/no-referrer enforcement so every
HTTP-edge protection agrees on the exact protected surface.
"""

from typing import Final

OAUTH_CREDENTIAL_PREFIXES: Final[tuple[str, ...]] = (
    "/api/mcp/token",
    "/api/mcp/revoke",
    "/api/mcp/register",
    "/api/mcp/authorize",
    "/api/mcp-oauth/consent/approve",
    "/api/mcp-oauth/consent/deny",
)
"""OAuth endpoints whose request payloads carry client or grant credentials."""

OAUTH_SURFACE_PREFIXES: Final[tuple[str, ...]] = (
    "/api/mcp",
    "/api/mcp-oauth",
    "/.well-known/oauth-authorization-server",
    "/.well-known/oauth-protected-resource",
    "/.well-known/openid-configuration",
)
"""Every path prefix belonging to the MCP OAuth and MCP resource surface."""


def _matches_prefix(path: str, prefixes: tuple[str, ...]) -> bool:
    """Return True when ``path`` matches any prefix on a segment boundary.

    Args:
        path: Request URL path without query string.
        prefixes: Normalized path prefixes without trailing slashes.

    Returns:
        True for an exact match or a segment-prefixed subpath.
    """
    normalized = path.split("?", 1)[0].rstrip("/")
    return any(normalized == prefix or normalized.startswith(f"{prefix}/") for prefix in prefixes)


def is_oauth_credential_path(path: str) -> bool:
    """Return True when ``path`` is an OAuth credential-bearing endpoint.

    Args:
        path: Request URL path without query string.

    Returns:
        True when mutation bodies on ``path`` must never be parsed or persisted.
    """
    return _matches_prefix(path, OAUTH_CREDENTIAL_PREFIXES)


def is_oauth_surface_path(path: str) -> bool:
    """Return True when ``path`` is part of the OAuth or MCP HTTP surface.

    Args:
        path: Request URL path without query string.

    Returns:
        True when query strings must be redacted and no-store/no-referrer
        headers must be applied to responses.
    """
    return _matches_prefix(path, OAUTH_SURFACE_PREFIXES)
