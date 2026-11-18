"""Mirror per-tool OAuth security schemes onto the top-level MCP tool field.

Hosted MCP connector clients read a tool's OAuth requirement both from
``_meta.securitySchemes`` and from a top-level ``securitySchemes`` tool field.
The MCP SDK serializes every spec-method result through its per-version
protocol surface, whose ``Tool`` model ignores unknown fields, so a top-level
``securitySchemes`` cannot survive any typed handler result: a ``Tool``
subclass, a ``ListToolsResult`` subclass and a replaced list handler are all
stripped on the wire. ``_meta`` survives because the surface declares it.

The SDK's context-tier ``ServerMiddleware`` receives the already-shaped wire
mapping from ``call_next`` and the runner only copies what the middleware
returns. :func:`mirror_security_schemes` uses that seam for exactly one
method. It copies an existing per-tool ``_meta.securitySchemes`` value to the
top level after the permission-filtered catalog was produced and sieved. It
never adds, removes or reorders tools, never invents a scheme for a tool
without one, and returns every other method, error and result shape unchanged.

The SDK documents ``Server.middleware`` as provisional within the 2.x line, so
raw-wire regression tests pin this behaviour for every supported protocol era.
"""

from collections.abc import Mapping
from typing import Any

from mcp.server.context import CallNext
from mcp.server.context import HandlerResult
from mcp.server.context import ServerRequestContext

TOOLS_LIST_METHOD = "tools/list"
SECURITY_SCHEMES_FIELD = "securitySchemes"
_META_FIELD = "_meta"
_TOOLS_FIELD = "tools"


def mirror_tool_security_schemes(tool: object) -> object:
    """Return one wire tool with its ``_meta`` security schemes mirrored.

    Args:
        tool: One already-serialized entry of a ``tools/list`` result.

    Returns:
        A new mapping carrying the top-level field when the entry declares
        ``_meta.securitySchemes``; otherwise the original entry unchanged.
    """
    if not isinstance(tool, Mapping):
        return tool
    meta = tool.get(_META_FIELD)
    if not isinstance(meta, Mapping):
        return tool
    schemes = meta.get(SECURITY_SCHEMES_FIELD)
    if schemes is None:
        return tool
    return {**tool, SECURITY_SCHEMES_FIELD: schemes}


def mirror_list_tools_result(method: str, result: HandlerResult) -> HandlerResult:
    """Mirror security schemes on a ``tools/list`` wire result only.

    Args:
        method: JSON-RPC method of the request that produced ``result``.
        result: Wire-shaped result returned by the remaining middleware chain.

    Returns:
        A new result whose tools carry the top-level field, or ``result``
        itself for any other method or for a result without a tool list.
    """
    if method != TOOLS_LIST_METHOD or not isinstance(result, Mapping):
        return result
    tools = result.get(_TOOLS_FIELD)
    if not isinstance(tools, list):
        return result
    return {**result, _TOOLS_FIELD: [mirror_tool_security_schemes(tool) for tool in tools]}


async def mirror_security_schemes(
    ctx: ServerRequestContext[Any, Any],
    call_next: CallNext,
) -> HandlerResult:
    """SDK server middleware that mirrors ``tools/list`` security schemes.

    Errors raised by the rest of the chain propagate untouched.

    Args:
        ctx: SDK request context; only its method name is read.
        call_next: Remaining SDK middleware chain and handler.

    Returns:
        The downstream result, with security schemes mirrored for ``tools/list``.
    """
    return mirror_list_tools_result(ctx.method, await call_next(ctx))
