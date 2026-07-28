"""MCP test dispatch support."""

from typing import Any

from mcp.server import MCPServer
from mcp.server.mcpserver.context import Context


async def call_raw_tool(
    server: MCPServer[None],
    name: str,
    arguments: dict[str, Any],
) -> Any:
    """Dispatch a tool without converting its Python return value.

    Args:
        server: MCP server containing the registered tool.
        name: Registered tool name.
        arguments: Tool arguments to validate and dispatch.

    Returns:
        The tool handler's raw Python return value.

    Raises:
        ToolError: When validation or tool execution fails.
    """
    context = Context(mcp_server=server)
    return await server._tool_manager.call_tool(name, arguments, context)
