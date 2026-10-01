"""Reject unsafe MCP manual-order numbers before obtaining repositories."""

from unittest.mock import MagicMock

import pytest
from mcp.server import MCPServer
from mcp.server.mcpserver.exceptions import ToolError

from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper.mcp.tools import _ManualOrderInput
from snapper.mcp.tools import _prepare_manual_order
from snapper.mcp.tools import register_mcp_tools
from snapper.messaging.infrastructure.publisher import SequenceTracker
from tests.mcp.raw_dispatch import call_raw_tool
from tests.mcp.test_tools import _make_claims


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("quantity", float("inf")),
        ("quantity", 0),
        ("quantity", True),
        ("quantity", -1),
        ("quantity", float("nan")),
        ("quantity", 10**1000),
        ("price", float("inf")),
        ("price", 0),
        ("price", True),
        ("stop_price", -float("inf")),
        ("stop_price", 0),
        ("stop_price", True),
        ("leverage", 0),
        ("leverage", -1),
        ("leverage", True),
        ("leverage", 1.5),
        ("leverage", 1.0),
        ("leverage", 2147483648),
    ],
)
async def test_mcp_invalid_numbers_before_repository(field: str, value: JsonValue) -> None:
    """Wire coercion cannot turn unsafe input into an order or database read.

    Given: registered MCP tools and arguments containing an unsafe number.
    When: the manual-order tool dispatches through MCP validation.
    Then: a tool error is raised before the repository getter is called.
    """
    repository_getter = MagicMock(side_effect=AssertionError("repository accessed"))
    server: MCPServer[None] = MCPServer("numeric-test")
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
        "order_type": "stop_limit",
        "quantity": 1,
        "price": 2,
        "stop_price": 3,
        "idempotency_key": "numeric-test",
    }
    arguments[field] = value
    with pytest.raises(ToolError):
        await call_raw_tool(server, "submit_manual_order", arguments)
    repository_getter.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("quantity", float("inf")),
        ("quantity", None),
        ("quantity", True),
        ("price", float("nan")),
        ("stop_price", -1),
        ("leverage", True),
        ("leverage", 1.5),
        ("leverage", 2147483648),
    ],
)
async def test_direct_mcp_numbers_before_repository(field: str, value: JsonValue) -> None:
    """Defend internal callers independently of MCP's transport validator.

    Given: an internal manual-order input carrying an unsafe number.
    When: order preparation runs without MCP transport validation.
    Then: validation raises ValueError before the repository getter is called.
    """
    order = _ManualOrderInput(
        exchange="kraken",
        instrument="BTC-USD",
        instrument_public_id="instrument-1",
        side="buy",
        order_type="stop_limit",
        quantity=1,
        price=2,
        stop_price=3,
        wallet_public_id=None,
        idempotency_key="numeric-test",
        operator_public_id=None,
        ai_review_public_id=None,
        leverage=None,
        post_only=False,
        reduce_only=False,
    )
    object.__setattr__(order, field, value)
    repository_getter = MagicMock(side_effect=AssertionError("repository accessed"))
    tracker = SequenceTracker()
    with pytest.raises(ValueError):
        await _prepare_manual_order(
            repository_getter=repository_getter,
            caps_enforcer_getter=lambda: None,
            claims_getter=_make_claims,
            order=order,
            tracker=tracker,
        )
    repository_getter.assert_not_called()
