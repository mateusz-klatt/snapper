"""Preserve strict manual-order numeric types across actual MCP SDK preprocessing."""

import json
from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest
from mcp.server import MCPServer
from mcp.server.context import ServerRequestContext
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.mcpserver.tools.base import Tool
from mcp.server.mcpserver.utilities.func_metadata import FuncMetadata
from mcp.types import CallToolRequestParams
from mcp.types import CallToolResult
from mcp.types import TextContent
from pydantic import BaseModel
from pydantic import Field
from pydantic import ValidationError

from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper.mcp import tools as mcp_tools
from snapper.mcp._manual_order_arguments import ManualOrderMetadata
from snapper.mcp._manual_order_arguments import preserve_manual_order_numeric_inputs
from snapper.mcp.tools import register_mcp_tools
from tests.mcp.raw_dispatch import call_raw_tool
from tests.mcp.test_tools import _make_claims


@dataclass
class _Boundary:
    """Registered server with poisoned domain dependencies."""

    server: MCPServer[None]
    repository: MagicMock
    caps: MagicMock


def _registered_boundary() -> _Boundary:
    """Register actual tools while refusing all repository and caps access."""
    repository = MagicMock(side_effect=AssertionError("repository boundary reached"))
    caps = MagicMock(side_effect=AssertionError("caps boundary reached"))
    server: MCPServer[None] = MCPServer("raw-manual-order-test")
    register_mcp_tools(
        server,
        repository_getter=repository,
        caps_enforcer_getter=caps,
        claims_getter=_make_claims,
    )
    return _Boundary(server, repository, caps)


@pytest.fixture
def boundary() -> _Boundary:
    """Provide independently registered tools and domain traps for each case."""
    return _registered_boundary()


def _manual_tool(server: MCPServer[None]) -> Tool:
    """Return the real manual tool, failing clearly if registration regresses."""
    tool = server._tool_manager.get_tool("submit_manual_order")
    assert tool is not None
    return tool


def _arguments() -> JsonObject:
    """Provide an otherwise valid market order with absent optional numbers."""
    return {
        "exchange": "kraken",
        "instrument": "BTC-USD",
        "instrument_public_id": "instrument-1",
        "side": "buy",
        "order_type": "market",
        "quantity": 1,
        "idempotency_key": "raw-numeric-test",
    }


def _validation_cause(error: BaseException) -> ValidationError | None:
    """Find the strict validator failure without accepting arbitrary tool errors."""
    current: BaseException | None = error
    while current is not None:
        if isinstance(current, ValidationError):
            return current
        current = current.__cause__
    return None


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["price", "stop_price", "leverage"])
@pytest.mark.parametrize("value", ["null", " null ", "\t null\n"])
async def test_string_null_rejected_before_domain_access(
    boundary: _Boundary, field: str, value: str
) -> None:
    """String null remains a string when strict numeric arguments are validated.

    Given a registered manual-order tool and poisoned domain dependencies,
    When an optional numeric field receives JSON null text,
    Then SDK dispatch rejects that field before repository or caps access.
    """
    arguments = _arguments()
    arguments[field] = value
    with pytest.raises(ToolError) as caught:
        await call_raw_tool(boundary.server, "submit_manual_order", arguments)
    boundary.repository.assert_not_called()
    boundary.caps.assert_not_called()
    cause = _validation_cause(caught.value)
    assert cause is not None
    assert any(error["loc"] == (field,) for error in cause.errors())


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["price", "stop_price", "leverage"])
async def test_protocol_handler_reports_json_safe_field_validation(
    boundary: _Boundary, field: str
) -> None:
    """The actual tools/call handler serializes strict raw-field rejection safely.

    Given a JSON-RPC-shaped request with string null in a numeric field,
    When the SDK handler validates and serializes its response,
    Then a field-specific validation error survives the JSON roundtrip without domain access.
    """
    arguments = _arguments()
    arguments[field] = " null "
    params = CallToolRequestParams(name="submit_manual_order", arguments=arguments)
    request: JsonObject = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": params.model_dump(mode="json", by_alias=True),
    }
    wire_request = json.dumps(request, allow_nan=False)
    decoded = json.loads(wire_request)
    context = ServerRequestContext(
        session=MagicMock(),
        lifespan_context=None,
        protocol_version="2025-11-25",
        method="tools/call",
        params=decoded["params"],
        request_id=decoded["id"],
    )
    result = await boundary.server._handle_call_tool(
        context, CallToolRequestParams.model_validate(decoded["params"])
    )
    assert isinstance(result, CallToolResult)
    wire_result = json.dumps(
        {"jsonrpc": "2.0", "id": 1, "result": result.model_dump(mode="json", by_alias=True)},
        allow_nan=False,
    )
    assert json.loads(wire_result)["result"]["isError"] is True
    text = "\n".join(item.text for item in result.content if isinstance(item, TextContent))
    assert field in text
    assert "validation error" in text.lower()
    assert "repository boundary reached" not in text
    boundary.repository.assert_not_called()
    boundary.caps.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["price", "stop_price", "leverage"])
async def test_actual_null_preserves_optional_semantics(boundary: _Boundary, field: str) -> None:
    """Genuine null still reaches ordinary manual-order preparation.

    Given a valid market order and a genuinely null optional numeric field,
    When actual manager dispatch validates the arguments,
    Then it reaches the poisoned repository instead of reporting a numeric validation error.
    """
    arguments = _arguments()
    arguments[field] = None
    with pytest.raises(ToolError) as caught:
        await call_raw_tool(boundary.server, "submit_manual_order", arguments)
    assert _validation_cause(caught.value) is None
    assert isinstance(caught.value.__cause__, AssertionError)
    boundary.repository.assert_called_once_with()
    boundary.caps.assert_not_called()


@pytest.mark.asyncio
async def test_omitted_numeric_fields_remain_absent_until_defaults(boundary: _Boundary) -> None:
    """Preservation does not manufacture explicit nulls for omitted arguments.

    Given a market order omitting every optional numeric field,
    When preprocessing and then validation run,
    Then preprocessing preserves omission and validation applies the existing defaults.
    """
    arguments = _arguments()
    metadata = _manual_tool(boundary.server).fn_metadata
    parsed = metadata.pre_parse_json(arguments)
    assert parsed == arguments
    for field in ("price", "stop_price", "leverage"):
        assert field not in parsed
    validated = metadata.validate_arguments(arguments)
    assert validated["price"] is None
    assert validated["stop_price"] is None
    assert validated["leverage"] is None
    assert validated["post_only"] is False
    assert validated["reduce_only"] is False
    with pytest.raises(ToolError):
        await call_raw_tool(boundary.server, "submit_manual_order", arguments)
    boundary.repository.assert_called_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize("quantity", [1, 1.25])
async def test_valid_stop_limit_numbers_reach_preparation(
    boundary: _Boundary, quantity: int | float
) -> None:
    """Valid numeric values retain existing domain preparation behavior.

    Given positive integer or floating amounts with integer leverage,
    When a stop-limit order dispatches through the actual manager,
    Then numeric validation succeeds and the repository trap is reached once.
    """
    arguments = _arguments()
    arguments.update(
        order_type="stop_limit", quantity=quantity, price=2, stop_price=3.5, leverage=2
    )
    with pytest.raises(ToolError) as caught:
        await call_raw_tool(boundary.server, "submit_manual_order", arguments)
    assert _validation_cause(caught.value) is None
    boundary.repository.assert_called_once_with()
    boundary.caps.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["quantity", "price", "stop_price", "leverage"])
@pytest.mark.parametrize("value", ["1", "true", True, "[]", "{}"])
async def test_other_non_numeric_raw_values_remain_rejected(
    boundary: _Boundary, field: str, value: JsonValue
) -> None:
    """Numeric preservation does not weaken strict scalar validation.

    Given a numeric field holding a string, boolean or encoded container,
    When the registered tool dispatches,
    Then its strict validator rejects the identified field before domain access.
    """
    arguments = _arguments()
    arguments[field] = value
    with pytest.raises(ToolError) as caught:
        await call_raw_tool(boundary.server, "submit_manual_order", arguments)
    cause = _validation_cause(caught.value)
    assert cause is not None
    assert any(error["loc"] == (field,) for error in cause.errors())
    boundary.repository.assert_not_called()
    boundary.caps.assert_not_called()


def test_missing_manual_tool_refuses_metadata_installation() -> None:
    """Registration order mistakes fail explicitly at the compatibility boundary.

    Given a server without the manual-order tool,
    When raw numeric metadata installation is requested,
    Then it raises an explanatory registration error.
    """
    server: MCPServer[None] = MCPServer("missing-manual-order")
    with pytest.raises(RuntimeError, match="Register submit_manual_order"):
        preserve_manual_order_numeric_inputs(server)


@pytest.mark.asyncio
async def test_complete_registered_schema_and_original_metadata_are_preserved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Installing the boundary does not change the published tool contract.

    Given the actual tools registered with original SDK metadata,
    When only manual-order raw numeric preservation is installed,
    Then the entire tool catalog and every public metadata attribute stay equal.
    """
    monkeypatch.setattr(mcp_tools, "preserve_manual_order_numeric_inputs", lambda _server: None)
    boundary = _registered_boundary()
    tool = _manual_tool(boundary.server)
    original = tool.fn_metadata
    before = [
        item.model_dump(mode="json", by_alias=True) for item in await boundary.server.list_tools()
    ]
    preserve_manual_order_numeric_inputs(boundary.server)
    after = [
        item.model_dump(mode="json", by_alias=True) for item in await boundary.server.list_tools()
    ]
    assert after == before
    assert isinstance(tool.fn_metadata, ManualOrderMetadata)
    assert tool.fn_metadata.model_dump() == original.model_dump()
    assert tool.fn_metadata.arg_model is original.arg_model
    result: JsonObject = {"status": "accepted", "quantity": 1.25}
    assert tool.fn_metadata.convert_result(result) == original.convert_result(result)


@pytest.mark.parametrize("field", ["wallet_public_id", "operator_public_id", "ai_review_public_id"])
def test_optional_identifier_preprocessing_is_unchanged(boundary: _Boundary, field: str) -> None:
    """Non-numeric nullable identifiers retain SDK JSON-string preprocessing.

    Given a manual-order optional identifier containing null text,
    When preprocessing runs with the numeric boundary installed,
    Then the identifier still becomes genuine None while numeric strings remain raw.
    """
    arguments = _arguments()
    arguments[field] = "null"
    arguments["price"] = "null"
    snapshot = arguments.copy()
    parsed = _manual_tool(boundary.server).fn_metadata.pre_parse_json(arguments)
    assert parsed[field] is None
    assert parsed["price"] == "null"
    assert arguments == snapshot


class _Output(BaseModel):
    """Structured output exercises alias-preserving result conversion."""

    order_id: str = Field(alias="orderId")


def _structured_tool(quantity: int) -> _Output:
    """Return a model with a serialized alias."""
    return _Output(orderId=str(quantity))


def _wrapped_tool(quantity: int) -> list[int]:
    """Return a list that requires the SDK output wrapper."""
    return [quantity]


def _unstructured_tool(quantity: int) -> object:
    """Return an unstructured result matching manual-order metadata today."""
    return {"quantity": quantity}


@pytest.mark.asyncio
@pytest.mark.parametrize("shape", ["structured", "wrapped", "unstructured"])
async def test_output_schema_and_conversion_survive_metadata_replacement(shape: str) -> None:
    """Metadata adaptation preserves structured, wrapped and unstructured output contracts.

    Given registered SDK metadata with a particular output conversion shape,
    When raw numeric preservation replaces that metadata,
    Then complete catalog schema, output types and converted wire results remain identical.
    """
    server: MCPServer[None] = MCPServer("output-contract")
    result: object
    if shape == "structured":
        server.add_tool(_structured_tool, name="submit_manual_order")
        result = _Output(orderId="1")
    elif shape == "wrapped":
        server.add_tool(_wrapped_tool, name="submit_manual_order")
        result = [1]
    else:
        server.add_tool(_unstructured_tool, name="submit_manual_order")
        result = {"quantity": 1}
    tool = _manual_tool(server)
    original = tool.fn_metadata
    schema_before = [
        item.model_dump(mode="json", by_alias=True) for item in await server.list_tools()
    ]
    converted_before = original.convert_result(result).model_dump(mode="json", by_alias=True)
    preserve_manual_order_numeric_inputs(server)
    replacement = tool.fn_metadata
    assert replacement.arg_model is original.arg_model
    assert replacement.output_model is original.output_model
    assert replacement.output_schema == original.output_schema
    assert replacement.wrap_output is original.wrap_output
    assert replacement.model_dump() == original.model_dump()
    assert [
        item.model_dump(mode="json", by_alias=True) for item in await server.list_tools()
    ] == schema_before
    assert (
        replacement.convert_result(result).model_dump(mode="json", by_alias=True)
        == converted_before
    )


def _container_tool(items: list[int], optional_id: str | None = None) -> JsonObject:
    """Expose ordinary SDK encoded-container and nullable-identifier preprocessing."""
    return {"items": list(items), "optional_id": optional_id}


@pytest.mark.asyncio
async def test_unrelated_tool_keeps_sdk_preprocessing(boundary: _Boundary) -> None:
    """Installing manual-order protection cannot alter another tool's metadata.

    Given an unrelated tool accepting encoded container and optional identifier arguments,
    When manual-order metadata is installed and that unrelated tool dispatches,
    Then its original SDK metadata and decoded argument behavior remain intact.
    """
    boundary.server.add_tool(_container_tool, name="container_probe")
    tool = boundary.server._tool_manager.get_tool("container_probe")
    assert tool is not None
    original: FuncMetadata = tool.fn_metadata
    preserve_manual_order_numeric_inputs(boundary.server)
    assert tool.fn_metadata is original
    assert type(original) is FuncMetadata
    result = await call_raw_tool(
        boundary.server, "container_probe", {"items": "[1, 2]", "optional_id": "null"}
    )
    assert result == {"items": [1, 2], "optional_id": None}
    boundary.repository.assert_not_called()
    boundary.caps.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, "null"])
async def test_required_quantity_null_still_refused(boundary: _Boundary, value: JsonValue) -> None:
    """Required quantity never acquires nullable semantics.

    Given either JSON null or null text as quantity,
    When actual manual-order dispatch validates raw arguments,
    Then quantity fails validation without touching domain dependencies.
    """
    arguments = _arguments()
    arguments["quantity"] = value
    with pytest.raises(ToolError) as caught:
        await call_raw_tool(boundary.server, "submit_manual_order", arguments)
    cause = _validation_cause(caught.value)
    assert cause is not None
    assert any(error["loc"] == ("quantity",) for error in cause.errors())
    boundary.repository.assert_not_called()
    boundary.caps.assert_not_called()


@pytest.mark.asyncio
async def test_valid_market_price_keeps_existing_domain_error_order(boundary: _Boundary) -> None:
    """Numeric acceptance does not bypass the existing market-price presence rule.

    Given a positive price on a market order,
    When actual tool dispatch accepts its numeric type,
    Then domain validation refuses the price before repository or caps access.
    """
    arguments = _arguments()
    arguments["price"] = 2
    with pytest.raises(ToolError) as caught:
        await call_raw_tool(boundary.server, "submit_manual_order", arguments)
    assert _validation_cause(caught.value) is None
    assert isinstance(caught.value.__cause__, ToolError)
    assert isinstance(caught.value.__cause__.__cause__, ValueError)
    assert "price" in str(caught.value).lower()
    boundary.repository.assert_not_called()
    boundary.caps.assert_not_called()


def test_missing_quantity_remains_missing_before_required_validation(boundary: _Boundary) -> None:
    """Raw preservation never fabricates a value for absent required quantity.

    Given otherwise valid arguments omitting quantity,
    When preprocessing runs before required-field validation,
    Then absence remains visible and the normal required error identifies quantity.
    """
    arguments = _arguments()
    arguments.pop("quantity")
    metadata = _manual_tool(boundary.server).fn_metadata
    assert "quantity" not in metadata.pre_parse_json(arguments)
    with pytest.raises(ValidationError) as caught:
        metadata.validate_arguments(arguments)
    assert any(
        error["loc"] == ("quantity",) and error["type"] == "missing"
        for error in caught.value.errors()
    )
