"""Access-log redaction for credential-bearing OAuth request lines.

``GET /api/mcp/authorize`` carries the client's ``state`` and PKCE challenge in
its query string, as OAuth requires. uvicorn's access log records the full
request line, so the query of that one path is replaced with ``[REDACTED]``
while method, path, protocol and status stay readable for operations.
"""

import logging
from urllib.parse import urlsplit

ACCESS_LOGGER = "uvicorn.access"
"""The logger uvicorn writes request lines to."""

REDACTED_QUERY_PATHS = frozenset({"/api/mcp/authorize"})
"""Paths whose query string never reaches the access log."""

_PATH_ARG = 2
_REDACTED = "[REDACTED]"


def redact_request_target(target: str) -> str:
    """Replace the query of a credential-bearing path, keeping every other target.

    Args:
        target: The request target uvicorn logs, path plus optional query.

    Returns:
        The target with its query replaced when the path is in
        :data:`REDACTED_QUERY_PATHS`, otherwise the unchanged target.
    """
    parts = urlsplit(target)
    if parts.path not in REDACTED_QUERY_PATHS or not parts.query:
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
