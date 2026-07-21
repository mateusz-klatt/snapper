"""MCP authorization boundary tests for the AI researcher principal."""

import json
from datetime import UTC
from datetime import datetime
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import CallToolResult
from mcp.types import TextContent

from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.tokens import TokenClaims
from snapper.data.repository import Repository
from snapper.mcp.tools import register_mcp_tools

_RAISING_TRADING_CALLS: tuple[tuple[str, dict[str, object], Permission], ...] = (
    (
        "submit_manual_order",
        {
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "instrument_public_id": "instrument-1",
            "side": "buy",
            "order_type": "market",
            "quantity": 1.0,
            "idempotency_key": "researcher-order",
        },
        Permission.CREATE_ORDERS,
    ),
    (
        "submit_ai_review_decision",
        {"review_id": "review-1", "decision": "approve"},
        Permission.CREATE_ORDERS,
    ),
)

_ENVELOPE_DENIED_CALLS: tuple[tuple[str, dict[str, object]], ...] = (
    ("cancel_order", {"plan_public_id": "plan-1", "idempotency_key": "researcher-cancel"}),
    ("list_orders", {}),
    ("get_order_status", {"command_public_id": "command-1"}),
    ("list_positions", {}),
    ("list_venue_account_states", {}),
    ("get_position_cycle", {"cycle_public_id": "cycle-1"}),
    ("get_ai_review_aftermath", {"review_public_id": "review-1"}),
    ("list_recent_signals", {"since": "2026-07-21T00:00:00Z"}),
)


def _researcher_claims(permissions: list[str]) -> TokenClaims:
    """Build AI researcher claims carrying the supplied token scope.

    Args:
        permissions: Permission strings to place in the token claim.

    Returns:
        Authenticated claims for an AI researcher principal.
    """
    now = int(datetime.now(UTC).timestamp())
    return TokenClaims(
        sub="researcher-user-1",
        username="researcher-1",
        role=UserRole.AI_RESEARCHER,
        permissions=permissions,
        exp=now + 3600,
        iat=now,
        jti="researcher-jti",
        sid="researcher-sid",
        user_public_id="researcher-user-1",
        operator_public_ids=[],
        primary_operator_public_id="",
    )


def _build_server(
    claims: TokenClaims,
    repository: Repository | None = None,
) -> FastMCP:
    """Register the production MCP tool set against controlled dependencies.

    Args:
        claims: Claims returned for every tool invocation.
        repository: Optional repository used by an admitted tool.

    Returns:
        FastMCP server with every production tool registered.
    """
    server = FastMCP("ai-researcher-boundary")
    register_mcp_tools(
        server,
        repository_getter=lambda: repository,
        caps_enforcer_getter=lambda: None,
        claims_getter=lambda: claims,
    )
    return server


def _decode_envelope(result: object) -> dict[str, object]:
    """Decode one canonical MCP tool result envelope.

    Args:
        result: Raw value returned by FastMCP tool dispatch.

    Returns:
        Parsed canonical response envelope.
    """
    assert isinstance(result, CallToolResult)
    first = result.content[0]
    assert isinstance(first, TextContent)
    return cast(dict[str, object], json.loads(first.text))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool_name", "arguments", "required_permission"),
    _RAISING_TRADING_CALLS,
)
async def test_forged_researcher_scope_cannot_invoke_raising_trading_tools(
    tool_name: str,
    arguments: dict[str, object],
    required_permission: Permission,
) -> None:
    """A token cannot expand the researcher role into trading authority.

    Given: AI researcher claims forged to contain every known permission.
    When: A trading tool with a raising permission gate is dispatched.
    Then: FastMCP returns a permission error before any dependency is used.
    """
    claims = _researcher_claims([permission.value for permission in Permission])
    server = _build_server(claims)

    with pytest.raises(ToolError) as exc:
        await server._tool_manager.call_tool(tool_name, arguments)

    assert required_permission.value in str(exc.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(("tool_name", "arguments"), _ENVELOPE_DENIED_CALLS)
async def test_forged_researcher_scope_cannot_read_or_act_on_trading_intent(
    tool_name: str,
    arguments: dict[str, object],
) -> None:
    """All envelope-based intent tools remain outside the role ceiling.

    Given: AI researcher claims forged to contain every known permission.
    When: An order, position, account, signal, review, or cancel tool is dispatched.
    Then: The tool returns permission_denied before repository access is possible.
    """
    claims = _researcher_claims([permission.value for permission in Permission])
    server = _build_server(claims)

    result = await server._tool_manager.call_tool(tool_name, arguments)
    envelope = _decode_envelope(result)

    assert envelope["success"] is False
    assert envelope["error_code"] == "permission_denied"


@pytest.mark.asyncio
async def test_narrow_researcher_scope_can_still_read_market_instruments() -> None:
    """A legitimately narrowed researcher token retains its selected grant.

    Given: An AI researcher token scoped only to READ_MARKET_DATA.
    When: The researcher calls the market-only list_instruments tool.
    Then: The call succeeds and returns sorted repository data.
    """
    repository_mock = MagicMock(spec=Repository)
    repository_mock.get_exchange_instruments = AsyncMock(return_value=["ETH-USD", "BTC-USD"])
    repository = cast(Repository, repository_mock)
    claims = _researcher_claims([Permission.READ_MARKET_DATA.value])
    server = _build_server(claims, repository)

    result = await server._tool_manager.call_tool("list_instruments", {"exchange": "kraken"})

    assert result == {
        "exchange": "kraken",
        "instruments": ["BTC-USD", "ETH-USD"],
    }
    repository_mock.get_exchange_instruments.assert_awaited_once()
