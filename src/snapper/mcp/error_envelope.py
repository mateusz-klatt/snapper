"""Canonical envelope shape for MCP tool results.

This module owns :func:`to_call_tool_result`, the single helper that
wraps any structured tool envelope (success boolean, optional
error_code, human-readable message, opaque details) into the
``mcp.types.CallToolResult`` shape FastMCP serialises across the wire.

Why a dedicated helper:

- Idempotent retries (e.g. ``decision_already_recorded``) MUST surface
  as ``success=True, error_code='decision_already_recorded'`` so the
  bridge logs the no-op for audit while the strategy treats it as a
  success. The naive shape ``success = error_code is None`` would
  collapse the two cases into ``isError=True`` and break the
  contract.
- The explicit ``success`` parameter is locked from day one so the
  helper signature does not need to be retroactively widened when
  read-mode tools (also envelope-based) start consuming it.

Server-side return shape:

.. code-block:: text

    CallToolResult(
        content=[TextContent(type="text", text=<JSON envelope>)],
        isError=(not success),
    )

The JSON envelope inside ``TextContent`` carries the four contract
fields:

.. code-block:: json

    {
        "success": <bool>,
        "error_code": <str | null>,
        "message": <str>,
        "details": <object>
    }

Consumers (the bridge + delegate clients) parse the JSON and branch
on ``success``; ``isError`` mirrors ``not success`` so MCP-aware
clients that bypass the JSON content also see the right top-level
flag.
"""

import json

from mcp.types import CallToolResult
from mcp.types import TextContent

from snapper.core.json_types import JsonObject

__all__ = ["to_call_tool_result"]


def to_call_tool_result(
    *,
    success: bool,
    error_code: str | None,
    message: str,
    details: JsonObject | None = None,
) -> CallToolResult:
    """Wrap a structured tool envelope as a FastMCP :class:`CallToolResult`.

    The four fields map 1:1 to the canonical envelope semantics —
    ``error_code`` is the load-bearing discriminator that the bridge
    forwards to delegate clients, ``message`` is the human-readable
    summary FastMCP exposes verbatim, and ``details`` is the opaque
    JSON payload (status, resolution_mode, dispatch_version, etc.)
    consumers parse for follow-up logic.

    Args:
        success: Explicit-success flag. ``True`` for first
            valid decision AND idempotent retry; ``False`` for hard
            errors (review_not_found, not_authorized, peer_resolved,
            review_id_expired). The MCP layer flips
            :attr:`CallToolResult.isError` to ``not success`` so
            MCP-aware clients without a JSON-envelope parser still
            observe the correct top-level error flag.
        error_code: Optional stable-classifier string. May
            be set even when ``success=True`` (idempotent retry
            ``decision_already_recorded``); ``None`` only on the
            first-valid-decision happy path.
        message: Human-readable summary of the outcome. Forwarded
            verbatim through FastMCP.
        details: Optional JSON-serialisable payload with envelope
            extras (status, resolution_mode, dispatch_version, ...).
            ``None`` is normalised to an empty object so consumers can
            unconditionally read ``details[...]``.

    Returns:
        A :class:`mcp.types.CallToolResult` whose single content entry
        is the JSON envelope and whose ``isError`` flag equals
        ``not success``.
    """
    envelope = {
        "success": success,
        "error_code": error_code,
        "message": message,
        "details": details if details is not None else {},
    }
    return CallToolResult(
        content=[TextContent(type="text", text=json.dumps(envelope))],
        isError=not success,
    )
