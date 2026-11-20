"""Access-log redaction for OAuth and MCP request lines.

OAuth and MCP routes can carry ``state``, PKCE challenges, codes, or pending
request identifiers in query strings or unexpected client requests. uvicorn's
access log records the full request line, so the query of every OAuth surface
path is replaced with ``[REDACTED]`` while method, path, protocol, and status
stay readable for operations.
"""

import logging
from urllib.parse import urlsplit

from snapper.server.oauth_paths import OAUTH_SURFACE_PREFIXES
from snapper.server.oauth_paths import is_oauth_surface_path

ACCESS_LOGGER = "uvicorn.access"
"""The logger uvicorn writes request lines to."""

REDACTED_QUERY_PATHS = frozenset(OAUTH_SURFACE_PREFIXES)
"""Segment-aware OAuth surface prefixes whose query strings never reach the access log."""

_PATH_ARG = 2
_REDACTED = "[REDACTED]"


def redact_request_target(target: str) -> str:
    """Replace the query of any OAuth surface path, with or without a trailing slash.

    Args:
        target: The request target uvicorn logs, path plus optional query.

    Returns:
        The target with its query replaced when the path matches
        :data:`REDACTED_QUERY_PATHS` via
        :func:`snapper.server.oauth_paths.is_oauth_surface_path` and a query is
        present, otherwise the unchanged target.
    """
    parts = urlsplit(target)
    if not parts.query or not is_oauth_surface_path(parts.path):
        return target
    return f"{parts.path}?{_REDACTED}"


class OAuthQueryRedactionFilter(logging.Filter):
    """Rewrite uvicorn access records before any handler formats them."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Redact the logged request target in place; never drop the record.

        Args:
            record: One access record, ``(client, method, target, version, status)``.

        Returns:
            Always True, so the redacted line is still logged.
        """
        args = record.args
        if isinstance(args, tuple) and len(args) > _PATH_ARG:
            target = args[_PATH_ARG]
            if isinstance(target, str):
                record.args = (*args[:_PATH_ARG], redact_request_target(target), *args[3:])
        return True


def install_access_log_redaction() -> None:
    """Attach the redaction filter to uvicorn's access logger exactly once."""
    access = logging.getLogger(ACCESS_LOGGER)
    if not any(isinstance(existing, OAuthQueryRedactionFilter) for existing in access.filters):
        access.addFilter(OAuthQueryRedactionFilter())
