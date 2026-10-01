"""Preserve manual-order scalar inputs at the MCP SDK boundary."""

from typing import Any

from mcp.server import MCPServer
from mcp.server.mcpserver.utilities.func_metadata import FuncMetadata

_MANUAL_ORDER_NUMERIC_FIELDS = ("quantity", "price", "stop_price", "leverage")


class ManualOrderMetadata(FuncMetadata):
    """Retain raw numeric values before existing strict argument validation."""

    def pre_parse_json(self, data: dict[str, Any]) -> dict[str, Any]:
        """Preserve numeric inputs while keeping SDK parsing of other arguments.

        Args:
            data: Raw arguments supplied to the registered tool.

        Returns:
            SDK-preparsed arguments with the supplied numeric inputs restored.
        """
        parsed = super().pre_parse_json(data)
        for name in _MANUAL_ORDER_NUMERIC_FIELDS:
            if name in data:
                parsed[name] = data[name]
        return parsed


def preserve_manual_order_numeric_inputs(server: MCPServer) -> None:
    """Install raw numeric handling on only the already-registered manual tool.

    Args:
        server: MCP server after submit_manual_order registration.

    Raises:
        RuntimeError: When registration ordering omitted the manual-order tool.
    """
    tool = server._tool_manager.get_tool("submit_manual_order")
    if tool is None:
        raise RuntimeError("Register submit_manual_order before preserving its numeric inputs")
    metadata = tool.fn_metadata
    tool.fn_metadata = ManualOrderMetadata(
        arg_model=metadata.arg_model,
        output_schema=metadata.output_schema,
        output_model=metadata.output_model,
        wrap_output=metadata.wrap_output,
    )
