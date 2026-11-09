"""MCP manual-order keys obey the same storage limit as HTTP requests."""

from unittest.mock import MagicMock

import pytest
from mcp.server import MCPServer
from mcp.server.mcpserver.exceptions import ToolError

from snapper.core.json_types import JsonObject
from snapper.mcp.tools import _ManualOrderInput
from snapper.mcp.tools import _prepare_manual_order
from snapper.mcp.tools import register_mcp_tools
from snapper.messaging.infrastructure.publisher import SequenceTracker
from tests.mcp.raw_dispatch import call_raw_tool
from tests.mcp.test_tools import _make_claims


@pytest.mark.asyncio
async def test_oversized_mcp_idempotency_key_before_repository() -> None:
    """Reject an oversized key before any MCP repository access.

    Given: a registered tool receiving a key one character above its maximum.
    When: MCP validates and dispatches the submission.
    Then: the transport raises a tool error without accessing persistence.
    """
    repository_getter = MagicMock(side_effect=AssertionError("repository accessed"))
    server: MCPServer[None] = MCPServer("idempotency-test")
    register_mcp_tools(
        server,
        repository_getter=repository_getter,
        caps_enforcer_getter=lambda: None,
        claims_getter=_make_claims,
    )
    arguments: JsonObject = {
        "exchange": "kraken",
        "instrument": "BTC-USD",
        "instrument_public_id": "instrument-1",
        "side": "buy",
        "order_type": "limit",
        "quantity": 1,
        "price": 2,
        "idempotency_key": "x" * 65,
    }
    with pytest.raises(ToolError, match="idempotency_key"):
        await call_raw_tool(server, "submit_manual_order", arguments)
    repository_getter.assert_not_called()


@pytest.mark.asyncio
async def test_oversized_direct_mcp_idempotency_key_before_repository() -> None:
    """Defend internal MCP callers independently of transport validation.

    Given: an internal manual-order input with a 65-character key.
    When: order preparation bypasses the MCP transport validator.
    Then: validation raises before the repository getter can run.
    """
    order = _ManualOrderInput(
        exchange="kraken",
        instrument="BTC-USD",
        instrument_public_id="instrument-1",
        side="buy",
        order_type="limit",
        quantity=1,
        price=2,
        stop_price=None,
        wallet_public_id=None,
        idempotency_key="x" * 65,
        operator_public_id=None,
        ai_review_public_id=None,
        leverage=None,
        post_only=False,
        reduce_only=False,
    )
    repository_getter = MagicMock(side_effect=AssertionError("repository accessed"))
    with pytest.raises(ValueError, match="at most 64 characters"):
        await _prepare_manual_order(
            repository_getter=repository_getter,
            caps_enforcer_getter=lambda: None,
            claims_getter=_make_claims,
            order=order,
            tracker=SequenceTracker(),
        )
    repository_getter.assert_not_called()
