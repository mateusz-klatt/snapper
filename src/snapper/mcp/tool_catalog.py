"""Permission policy for the authenticated MCP tool catalog."""

from collections.abc import Callable

from mcp.types import Tool as MCPTool

from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.permissions import get_effective_permissions
from snapper.auth.domain.permissions import is_ai_review_decision_capable
from snapper.auth.schemas.tokens import TokenClaims

type ToolVisibilityPredicate = Callable[[TokenClaims], bool]
type ToolVisibilityRule = Permission | ToolVisibilityPredicate


def _is_ai_review_decision_visible(claims: TokenClaims) -> bool:
    """Project current and v1-compatible AI review decision capability."""
    return is_ai_review_decision_capable(
        claims.role,
        claims.permissions,
        claims.permission_scope_version,
    )


MCP_TOOL_VISIBILITY_POLICY: dict[str, ToolVisibilityRule] = {
    "list_instruments": Permission.READ_MARKET_DATA,
    "submit_manual_order": Permission.CREATE_ORDERS,
    "submit_ai_review_decision": _is_ai_review_decision_visible,
    "submit_market_view": Permission.SUBMIT_MARKET_VIEW,
    "get_latest_research": Permission.READ_MARKET_VIEWS,
    "list_orders": Permission.READ_ORDERS,
    "get_order_status": Permission.READ_ORDERS,
    "list_positions": Permission.READ_POSITIONS,
    "list_venue_account_states": Permission.READ_ACCOUNT_STATE,
    "get_position_cycle": Permission.READ_POSITIONS,
    "get_ai_review_aftermath": Permission.READ_SIGNALS,
    "cancel_order": Permission.CANCEL_ORDERS,
    "get_ohlcv": Permission.READ_MARKET_DATA,
    "list_recent_signals": Permission.READ_SIGNALS,
}
"""Authoritative visibility rule for every registered MCP tool."""


def is_mcp_tool_visible(tool_name: str, claims: TokenClaims) -> bool:
    """Return whether authenticated claims may see one MCP tool.

    Args:
        tool_name: Registered MCP tool name.
        claims: Authenticated token claims for the current request.

    Returns:
        ``True`` when the central policy admits the tool. Tools missing
        from the policy are hidden from authenticated catalogs.
    """
    rule = MCP_TOOL_VISIBILITY_POLICY.get(tool_name)
    if rule is None:
        return False
    if isinstance(rule, Permission):
        effective_permissions = get_effective_permissions(claims.role, claims.permissions)
        return rule in effective_permissions
    return rule(claims)


def filter_mcp_tools(tools: list[MCPTool], claims: TokenClaims) -> list[MCPTool]:
    """Filter registered tools through the authenticated visibility policy.

    Args:
        tools: Registered tools in FastMCP catalog order.
        claims: Authenticated token claims for the current request.

    Returns:
        Tools admitted by :data:`MCP_TOOL_VISIBILITY_POLICY`, preserving
        their registration order.
    """
    return [tool for tool in tools if is_mcp_tool_visible(tool.name, claims)]
