"""Tests for the typed stateless Snapper MCP bridge."""

import json
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from contextlib import asynccontextmanager
from types import TracebackType
from unittest.mock import MagicMock

import pytest
from mcp import types

import snapper_delegate.mcp_bridge as mcp_bridge
from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper_delegate.mcp_bridge import MCPBridge
from snapper_delegate.mcp_bridge import MCPErrorKind
from snapper_delegate.mcp_bridge import MCPOutcome
from snapper_delegate.mcp_bridge import MCPSession
from snapper_delegate.mcp_bridge import MCPToolCallFailure
from snapper_delegate.mcp_bridge import MCPToolCallSuccess
from snapper_delegate.mcp_bridge import MCPToolCatalogFailure
from snapper_delegate.mcp_bridge import MCPToolCatalogSuccess
from snapper_delegate.mcp_bridge import sanitize_openai_schema

type RawCallResult = types.CallToolResult | types.InputRequiredResult | types.Result

_GEMINI_FORBIDDEN_SCHEMA_KEYS = frozenset(
    {"$defs", "$ref", "additionalProperties", "anyOf", "default", "title"}
)
_REAL_TOOL_SCHEMAS: tuple[tuple[str, JsonObject], ...] = (
    (
        "list_instruments",
        {
            "properties": {"exchange": {"title": "Exchange", "type": "string"}},
            "required": ["exchange"],
            "title": "list_instrumentsArguments",
            "type": "object",
        },
    ),
    (
        "get_ohlcv",
        {
            "properties": {
                "exchange": {"title": "Exchange", "type": "string"},
                "instrument": {"title": "Instrument", "type": "string"},
                "limit": {"default": 200, "title": "Limit", "type": "integer"},
                "since": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "default": None,
                    "title": "Since",
                },
                "timeframe": {"title": "Timeframe", "type": "string"},
                "until": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "default": None,
                    "title": "Until",
                },
            },
            "required": ["exchange", "instrument", "timeframe"],
            "title": "get_ohlcvArguments",
            "type": "object",
        },
    ),
    (
        "submit_ai_review_decision",
        {
            "properties": {
                "decision": {"title": "Decision", "type": "string"},
                "rationale": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "default": None,
                    "title": "Rationale",
                },
                "review_id": {"title": "Review Id", "type": "string"},
            },
            "required": ["review_id", "decision"],
            "title": "submit_ai_review_decisionArguments",
            "type": "object",
        },
    ),
    (
        "submit_market_view",
        {
            "$defs": {
                "JsonObject": {
                    "additionalProperties": {"$ref": "#/$defs/JsonValue"},
                    "type": "object",
                },
                "JsonPrimitive": {
                    "anyOf": [
                        {"type": "string"},
                        {"type": "integer"},
                        {"type": "number"},
                        {"type": "boolean"},
                        {"type": "null"},
                    ]
                },
                "JsonValue": {
                    "anyOf": [
                        {"$ref": "#/$defs/JsonPrimitive"},
                        {
                            "items": {"$ref": "#/$defs/JsonValue"},
                            "type": "array",
                        },
                        {
                            "additionalProperties": {"$ref": "#/$defs/JsonValue"},
                            "type": "object",
                        },
                    ]
                },
            },
            "properties": {
                "payload": {"$ref": "#/$defs/JsonObject"},
                "research_round_public_id": {
                    "title": "Research Round Public Id",
                    "type": "string",
                },
            },
            "required": ["research_round_public_id", "payload"],
            "title": "submit_market_viewArguments",
            "type": "object",
        },
    ),
    (
        "get_latest_research",
        {
            "properties": {},
            "title": "get_latest_researchArguments",
            "type": "object",
        },
    ),
    (
        "list_orders",
        {
            "properties": {
                "exchange": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "default": None,
                    "title": "Exchange",
                },
                "instrument": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "default": None,
                    "title": "Instrument",
                },
                "limit": {"default": 50, "title": "Limit", "type": "integer"},
                "offset": {"default": 0, "title": "Offset", "type": "integer"},
                "status": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "default": None,
                    "title": "Status",
                },
                "wallet_public_id": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "default": None,
                    "title": "Wallet Public Id",
                },
            },
            "title": "list_ordersArguments",
            "type": "object",
        },
    ),
    (
        "get_order_status",
        {
            "properties": {
                "command_public_id": {
                    "title": "Command Public Id",
                    "type": "string",
                }
            },
            "required": ["command_public_id"],
            "title": "get_order_statusArguments",
            "type": "object",
        },
    ),
    (
        "list_positions",
        {
            "properties": {
                "exchange": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "default": None,
                    "title": "Exchange",
                },
                "instrument": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "default": None,
                    "title": "Instrument",
                },
                "wallet_public_id": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "default": None,
                    "title": "Wallet Public Id",
                },
            },
            "title": "list_positionsArguments",
            "type": "object",
        },
    ),
    (
        "list_venue_account_states",
        {
            "properties": {
                "exchange": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "default": None,
                    "title": "Exchange",
                },
                "wallet_public_id": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "default": None,
                    "title": "Wallet Public Id",
                },
            },
            "title": "list_venue_account_statesArguments",
            "type": "object",
        },
    ),
    (
        "get_position_cycle",
        {
            "properties": {
                "cycle_public_id": {
                    "title": "Cycle Public Id",
                    "type": "string",
                }
            },
            "required": ["cycle_public_id"],
            "title": "get_position_cycleArguments",
            "type": "object",
        },
    ),
    (
        "get_ai_review_aftermath",
        {
            "properties": {
                "review_public_id": {
                    "title": "Review Public Id",
                    "type": "string",
                }
            },
            "required": ["review_public_id"],
            "title": "get_ai_review_aftermathArguments",
            "type": "object",
        },
    ),
    (
        "cancel_order",
        {
            "properties": {
                "idempotency_key": {
                    "title": "Idempotency Key",
                    "type": "string",
                },
                "plan_public_id": {
                    "title": "Plan Public Id",
                    "type": "string",
                },
            },
            "required": ["plan_public_id", "idempotency_key"],
            "title": "cancel_orderArguments",
            "type": "object",
        },
    ),
    (
        "list_recent_signals",
        {
            "properties": {
                "exchange": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "default": None,
                    "title": "Exchange",
                },
                "instrument": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "default": None,
                    "title": "Instrument",
                },
                "limit": {"default": 50, "title": "Limit", "type": "integer"},
                "since": {"title": "Since", "type": "string"},
                "strategy": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "default": None,
                    "title": "Strategy",
                },
                "wallet_public_id": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "default": None,
                    "title": "Wallet Public Id",
                },
            },
            "required": ["since"],
            "title": "list_recent_signalsArguments",
            "type": "object",
        },
    ),
    (
        "submit_manual_order",
        {
            "properties": {
                "ai_review_public_id": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "default": None,
                    "title": "Ai Review Public Id",
                },
                "exchange": {"title": "Exchange", "type": "string"},
                "idempotency_key": {
                    "title": "Idempotency Key",
                    "type": "string",
                },
                "instrument": {"title": "Instrument", "type": "string"},
                "instrument_public_id": {
                    "title": "Instrument Public Id",
                    "type": "string",
                },
                "operator_public_id": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "default": None,
                    "title": "Operator Public Id",
                },
                "order_type": {"title": "Order Type", "type": "string"},
                "price": {
                    "anyOf": [{"type": "number"}, {"type": "null"}],
                    "default": None,
                    "title": "Price",
                },
                "quantity": {"title": "Quantity", "type": "number"},
                "side": {"title": "Side", "type": "string"},
                "stop_price": {
                    "anyOf": [{"type": "number"}, {"type": "null"}],
                    "default": None,
                    "title": "Stop Price",
                },
                "wallet_public_id": {
                    "anyOf": [{"type": "string"}, {"type": "null"}],
                    "default": None,
                    "title": "Wallet Public Id",
                },
            },
            "required": [
                "exchange",
                "instrument",
                "instrument_public_id",
                "side",
                "order_type",
                "quantity",
                "idempotency_key",
            ],
            "title": "submit_manual_orderArguments",
            "type": "object",
        },
    ),
)


class FakeSession:
    """Record calls while returning injected MCP SDK result models."""

    def __init__(
        self,
        catalog: types.ListToolsResult | None = None,
        call_result: RawCallResult | None = None,
        fail: bool = False,
    ) -> None:
        """Initialize a controllable session double."""
        self.catalog = catalog or types.ListToolsResult(tools=[])
        self.call_result = call_result or types.Result()
        self.fail = fail
        self.initialize_count = 0
        self.calls: list[tuple[str, JsonObject | None]] = []

    async def initialize(self) -> object:
        """Record protocol initialization or raise an injected session failure."""
        self.initialize_count += 1
        if self.fail:
            raise OSError("offline")
        return object()

    async def list_tools(self) -> types.ListToolsResult:
        """Return the injected tool catalog."""
        return self.catalog

    async def call_tool(
        self,
        name: str,
        arguments: JsonObject | None = None,
    ) -> RawCallResult:
        """Record the invocation and return the injected tool result."""
        self.calls.append((name, arguments))
        return self.call_result


@asynccontextmanager
async def _session_context(session: MCPSession) -> AsyncIterator[MCPSession]:
    """Yield an injected MCP session."""
    yield session


class RecordingSessionFactory:
    """Record endpoint and credential inputs to the injectable session seam."""

    def __init__(self, session: MCPSession) -> None:
        """Retain the session yielded for every factory call."""
        self.session = session
        self.calls: list[tuple[str, str]] = []

    def __call__(
        self,
        endpoint_url: str,
        access_token: str,
    ) -> AbstractAsyncContextManager[MCPSession]:
        """Record the input and return a session context manager."""
        self.calls.append((endpoint_url, access_token))
        return _session_context(self.session)


def _text_result(payload: JsonObject, is_error: bool = False) -> types.CallToolResult:
    """Build one valid single-text MCP call result."""
    return types.CallToolResult(
        content=[types.TextContent(text=json.dumps(payload, separators=(",", ":")))],
        is_error=is_error,
    )


def _assert_gemini_schema_subset(value: JsonValue) -> None:
    """Assert recursively that a sanitized schema uses the supported subset."""
    if isinstance(value, dict):
        assert _GEMINI_FORBIDDEN_SCHEMA_KEYS.isdisjoint(value)
        for nested in value.values():
            _assert_gemini_schema_subset(nested)
        return
    if isinstance(value, list):
        for nested in value:
            _assert_gemini_schema_subset(nested)


def test_real_tool_schema_fixture_covers_current_catalog() -> None:
    """The golden catalog names pin every current Snapper MCP tool schema."""
    assert {name for name, schema in _REAL_TOOL_SCHEMAS if schema} == {
        "cancel_order",
        "get_ai_review_aftermath",
        "get_latest_research",
        "get_ohlcv",
        "get_order_status",
        "get_position_cycle",
        "list_instruments",
        "list_orders",
        "list_positions",
        "list_recent_signals",
        "list_venue_account_states",
        "submit_ai_review_decision",
        "submit_manual_order",
        "submit_market_view",
    }


@pytest.mark.parametrize(
    "schema",
    [pytest.param(schema, id=name) for name, schema in _REAL_TOOL_SCHEMAS],
)
def test_real_tool_schemas_sanitize_to_gemini_subset(schema: JsonObject) -> None:
    """Every current golden MCP input schema remains Gemini-compatible."""
    _assert_gemini_schema_subset(sanitize_openai_schema(schema))


def test_schema_sanitizer_flattens_nullable_and_strips_metadata() -> None:
    """OpenAI function schemas omit metadata at every nested schema level.

    Given: A FastMCP schema with nullable fields, arrays, and retained unions,
    When: The schema is sanitized for a chat-completions tool definition,
    Then: Nullable pairs flatten and title and default keys disappear recursively.
    """
    schema: JsonObject = {
        "title": "SubmitDecision",
        "type": "object",
        "properties": {
            "rationale": {
                "anyOf": [
                    {"type": "string", "title": "Rationale", "maxLength": 4096},
                    {"type": "null"},
                ],
                "default": None,
                "title": "Optional rationale",
                "description": "Short rationale",
            },
            "tags": {
                "type": "array",
                "items": {"type": "string", "title": "Tag", "default": "ignored"},
            },
            "choice": {
                "anyOf": [{"type": "string"}, {"type": "integer"}],
            },
        },
    }

    sanitized = sanitize_openai_schema(schema)

    assert sanitized == {
        "type": "object",
        "properties": {
            "rationale": {
                "type": "string",
                "maxLength": 4096,
                "description": "Short rationale",
            },
            "tags": {"type": "array", "items": {"type": "string"}},
            "choice": {
                "anyOf": [{"type": "string"}, {"type": "integer"}],
            },
        },
    }
    assert schema["title"] == "SubmitDecision"


def test_schema_sanitizer_inlines_definitions_and_preserves_ref_siblings() -> None:
    """Local definition references inline after recursive sanitization."""
    schema: JsonObject = {
        "$defs": {
            "Filter": {
                "additionalProperties": False,
                "properties": {
                    "symbol": {"title": "Symbol", "type": "string"},
                },
                "required": ["symbol"],
                "title": "Filter",
                "type": "object",
            }
        },
        "properties": {
            "filter": {
                "$ref": "#/$defs/Filter",
                "description": "Selected instrument",
            }
        },
        "type": "object",
    }

    assert sanitize_openai_schema(schema) == {
        "properties": {
            "filter": {
                "properties": {"symbol": {"type": "string"}},
                "required": ["symbol"],
                "type": "object",
                "description": "Selected instrument",
            }
        },
        "type": "object",
    }


def test_schema_sanitizer_drops_additional_properties_at_every_level() -> None:
    """Gemini-incompatible additional-properties declarations are omitted."""
    schema: JsonObject = {
        "additionalProperties": False,
        "properties": {
            "metadata": {
                "additionalProperties": {"type": "string"},
                "type": "object",
            }
        },
        "type": "object",
    }

    assert sanitize_openai_schema(schema) == {
        "properties": {"metadata": {"type": "object"}},
        "type": "object",
    }


def test_schema_sanitizer_inlines_reference_inside_array_items() -> None:
    """Definition references nested in array items inline recursively."""
    schema: JsonObject = {
        "$defs": {
            "Amount": {
                "properties": {"value": {"default": 0, "title": "Value", "type": "number"}},
                "type": "object",
            }
        },
        "items": {"$ref": "#/$defs/Amount"},
        "type": "array",
    }

    assert sanitize_openai_schema(schema) == {
        "items": {
            "properties": {"value": {"type": "number"}},
            "type": "object",
        },
        "type": "array",
    }


def test_schema_sanitizer_leaves_cycles_and_logs_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cyclic references remain visible and emit one bounded warning."""
    warning = MagicMock()
    monkeypatch.setattr("snapper_delegate.mcp_bridge.logger.warning", warning)
    schema: JsonObject = {
        "$defs": {
            "Node": {
                "properties": {
                    "left": {"$ref": "#/$defs/Node"},
                    "right": {"$ref": "#/$defs/Node"},
                },
                "title": "Node",
                "type": "object",
            }
        },
        "$ref": "#/$defs/Node",
    }

    assert sanitize_openai_schema(schema) == {
        "properties": {
            "left": {"$ref": "#/$defs/Node"},
            "right": {"$ref": "#/$defs/Node"},
        },
        "type": "object",
    }
    warning.assert_called_once_with(
        "MCP tool schema contains cyclic or excessively deep local references"
    )


def test_schema_sanitizer_bounds_long_reference_chains(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Acyclic local reference expansion stops at the fixed depth bound."""
    warning = MagicMock()
    monkeypatch.setattr("snapper_delegate.mcp_bridge.logger.warning", warning)
    definitions: JsonObject = {}
    for index in range(mcp_bridge._MAX_REFERENCE_DEPTH):
        definitions[f"Node{index}"] = {
            "$ref": f"#/$defs/Node{index + 1}",
        }
    definitions[f"Node{mcp_bridge._MAX_REFERENCE_DEPTH}"] = {"type": "string"}
    schema: JsonObject = {
        "$defs": definitions,
        "$ref": "#/$defs/Node0",
    }

    assert sanitize_openai_schema(schema) == {
        "$ref": f"#/$defs/Node{mcp_bridge._MAX_REFERENCE_DEPTH}"
    }
    warning.assert_called_once_with(
        "MCP tool schema contains cyclic or excessively deep local references"
    )


@pytest.mark.parametrize(
    ("schema", "expected"),
    [
        (
            {"$ref": "#/components/schemas/Foreign", "title": "Foreign"},
            {"$ref": "#/components/schemas/Foreign"},
        ),
        (
            {
                "$defs": {"Known": {"type": "string"}},
                "$ref": "#/$defs/Missing",
            },
            {"$ref": "#/$defs/Missing"},
        ),
    ],
)
def test_schema_sanitizer_leaves_foreign_and_missing_references(
    schema: JsonObject,
    expected: JsonObject,
) -> None:
    """Only resolvable local top-level definitions are eligible for inlining."""
    assert sanitize_openai_schema(schema) == expected


@pytest.mark.parametrize(
    "schema",
    [
        {"anyOf": "not-an-array"},
        {"anyOf": []},
        {"anyOf": [{"type": "string"}, "null"]},
        {"anyOf": [{"type": "null"}, {"type": "null"}]},
        {
            "anyOf": [
                {"type": "string"},
                {"type": "integer"},
                {"type": "boolean"},
            ]
        },
    ],
)
def test_schema_sanitizer_preserves_non_nullable_any_of(schema: JsonObject) -> None:
    """Shapes other than one base plus one null option remain unions.

    Given: A value that is not the FastMCP two-object nullable pattern,
    When: The schema sanitizer visits it,
    Then: It preserves the union value after recursively sanitizing it.
    """
    assert sanitize_openai_schema(schema) == schema


@pytest.mark.asyncio
async def test_catalog_is_initialized_sanitized_and_typed() -> None:
    """A valid MCP catalog becomes ready-to-send chat tools.

    Given: An injected stateless session exposing a FastMCP nullable schema,
    When: The bridge lists tools with a rotated bearer token,
    Then: It initializes the session and returns a sanitized function definition.
    """
    catalog = types.ListToolsResult(
        tools=[
            types.Tool(
                name="submit_ai_review_decision",
                description="Submit the review decision",
                input_schema={
                    "title": "Input",
                    "type": "object",
                    "properties": {
                        "rationale": {
                            "anyOf": [{"type": "null"}, {"type": "string"}],
                            "default": None,
                        }
                    },
                },
            )
        ]
    )
    session = FakeSession(catalog=catalog)
    factory = RecordingSessionFactory(session)
    bridge = MCPBridge("http://snapper.invalid/", session_factory=factory)

    result = await bridge.list_tools("rotated-token")

    assert repr(bridge) == "MCPBridge()"
    assert isinstance(result, MCPToolCatalogSuccess)
    assert result.outcome is MCPOutcome.SUCCESS
    assert session.initialize_count == 1
    assert factory.calls == [("http://snapper.invalid/api/mcp", "rotated-token")]
    assert result.tools[0].model_dump() == {
        "type": "function",
        "function": {
            "name": "submit_ai_review_decision",
            "description": "Submit the review decision",
            "parameters": {
                "type": "object",
                "properties": {"rationale": {"type": "string"}},
            },
        },
    }


@pytest.mark.asyncio
async def test_malformed_catalog_returns_typed_failure() -> None:
    """Invalid JSON schema content cannot escape the bridge.

    Given: An SDK tool whose external schema includes a non-JSON set,
    When: The bridge validates the catalog,
    Then: It reports a malformed catalog without raising.
    """
    catalog = types.ListToolsResult(
        tools=[types.Tool(name="invalid", input_schema={"enum": {"not-json"}})]
    )
    bridge = MCPBridge(
        "http://snapper.invalid",
        session_factory=RecordingSessionFactory(FakeSession(catalog=catalog)),
    )

    result = await bridge.list_tools("token")

    assert isinstance(result, MCPToolCatalogFailure)
    assert result.outcome is MCPOutcome.ERROR
    assert result.error_kind is MCPErrorKind.MALFORMED_CATALOG


@pytest.mark.asyncio
async def test_catalog_session_failure_returns_typed_failure() -> None:
    """Session setup failures are recoverable catalog outcomes.

    Given: A session that cannot initialize,
    When: The bridge attempts to list tools,
    Then: It reports the session category without exposing the exception.
    """
    bridge = MCPBridge(
        "http://snapper.invalid",
        session_factory=RecordingSessionFactory(FakeSession(fail=True)),
    )

    result = await bridge.list_tools("token")

    assert isinstance(result, MCPToolCatalogFailure)
    assert result.error_kind is MCPErrorKind.SESSION


@pytest.mark.asyncio
async def test_successful_tool_call_preserves_replay_content() -> None:
    """Successful decision results are typed even for idempotent replays.

    Given: A success envelope carrying decision_already_recorded,
    When: The bridge calls the decision tool,
    Then: It preserves the text and reports success for immediate loop termination.
    """
    payload: JsonObject = {
        "success": True,
        "error_code": "decision_already_recorded",
        "message": "Decision already recorded",
        "details": {"review_public_id": "review-1"},
    }
    session = FakeSession(call_result=_text_result(payload))
    factory = RecordingSessionFactory(session)
    bridge = MCPBridge("http://snapper.invalid", session_factory=factory)
    arguments: JsonObject = {
        "review_id": "review-1",
        "decision": "approve",
        "rationale": None,
    }

    result = await bridge.call_tool("fresh-token", "submit_ai_review_decision", arguments)

    assert isinstance(result, MCPToolCallSuccess)
    assert result.success is True
    assert result.outcome is MCPOutcome.SUCCESS
    assert result.error_code == "decision_already_recorded"
    assert result.message == "Decision already recorded"
    assert result.details == {"review_public_id": "review-1"}
    assert json.loads(result.content) == payload
    assert session.initialize_count == 1
    assert session.calls == [("submit_ai_review_decision", arguments)]
    assert factory.calls == [("http://snapper.invalid/api/mcp", "fresh-token")]


@pytest.mark.asyncio
async def test_remote_tool_rejection_is_replayable_failure() -> None:
    """A valid server rejection remains distinct from malformed transport data.

    Given: A failure envelope whose MCP isError flag agrees with success false,
    When: The bridge parses the tool result,
    Then: It returns the remote error fields and original replay content.
    """
    payload: JsonObject = {
        "success": False,
        "error_code": "not_selected_before_fanout",
        "message": "Delegate is not selected yet",
        "details": None,
    }
    bridge = MCPBridge(
        "http://snapper.invalid",
        session_factory=RecordingSessionFactory(
            FakeSession(call_result=_text_result(payload, is_error=True))
        ),
    )

    result = await bridge.call_tool("token", "submit_ai_review_decision", {})

    assert isinstance(result, MCPToolCallFailure)
    assert result.success is False
    assert result.outcome is MCPOutcome.ERROR
    assert result.error_kind is MCPErrorKind.REMOTE
    assert result.error_code == "not_selected_before_fanout"
    assert result.message == "Delegate is not selected yet"
    assert result.details is None
    assert json.loads(result.content) == payload


@pytest.mark.parametrize(
    "raw_result",
    [
        types.Result(),
        types.CallToolResult(content=[]),
        types.CallToolResult(content=[types.ImageContent(data="aW1hZ2U=", mime_type="image/png")]),
        types.CallToolResult(content=[types.TextContent(text="not-json")]),
        types.CallToolResult(content=[types.TextContent(text='{"message":"missing success"}')]),
        _text_result({"success": True}, is_error=True),
    ],
)
@pytest.mark.asyncio
async def test_malformed_tool_results_return_safe_failure(raw_result: RawCallResult) -> None:
    """Every violation of the single-text envelope contract fails closed.

    Given: A non-call result, wrong content shape, invalid JSON, or flag mismatch,
    When: The bridge parses the external SDK result,
    Then: It returns one generic safe failure for replay instead of raising.
    """
    bridge = MCPBridge(
        "http://snapper.invalid",
        session_factory=RecordingSessionFactory(FakeSession(call_result=raw_result)),
    )

    result = await bridge.call_tool("token", "tool", {})

    assert isinstance(result, MCPToolCallFailure)
    assert result.error_kind is MCPErrorKind.MALFORMED_RESULT
    assert result.error_code == "mcp_bridge_error"
    assert result.message == "MCP tool call failed"
    assert result.details is None
    assert json.loads(result.content) == {
        "success": False,
        "error_code": "mcp_bridge_error",
        "message": "MCP tool call failed",
        "details": None,
    }


@pytest.mark.asyncio
async def test_tool_call_session_failure_returns_safe_failure() -> None:
    """Tool session failures stay inside the typed bridge boundary.

    Given: A session that fails during initialization,
    When: The bridge attempts a tool call,
    Then: It returns a safe session failure suitable for chat replay.
    """
    bridge = MCPBridge(
        "http://snapper.invalid",
        session_factory=RecordingSessionFactory(FakeSession(fail=True)),
    )

    result = await bridge.call_tool("token", "tool", {})

    assert isinstance(result, MCPToolCallFailure)
    assert result.error_kind is MCPErrorKind.SESSION


class FakeHttpClient:
    """Record the default factory's HTTP client lifetime."""

    def __init__(self, events: list[str]) -> None:
        """Retain a shared event ledger."""
        self.events = events

    async def __aenter__(self) -> FakeHttpClient:
        """Record HTTP client entry."""
        self.events.append("http_enter")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Record HTTP client exit."""
        self.events.append("http_exit")


class FakeSDKSession:
    """Exercise the concrete SDK session context used by the default seam."""

    def __init__(self, events: list[str], read_stream: object, write_stream: object) -> None:
        """Retain streams and a shared event ledger."""
        self.events = events
        self.read_stream = read_stream
        self.write_stream = write_stream

    async def __aenter__(self) -> FakeSDKSession:
        """Record SDK session entry."""
        self.events.append("session_enter")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Record SDK session exit."""
        self.events.append("session_exit")

    async def initialize(self) -> object:
        """Record SDK protocol initialization."""
        self.events.append("initialize")
        return object()

    async def list_tools(self) -> types.ListToolsResult:
        """Return an empty but valid catalog."""
        self.events.append("list_tools")
        return types.ListToolsResult(tools=[])


@pytest.mark.asyncio
async def test_default_factory_uses_sdk_two_stream_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The production seam follows the MCP SDK 2.x HTTP and stream contracts.

    Given: Patched SDK context managers and a bearer credential,
    When: A bridge without an injected factory lists tools,
    Then: It passes an owned authenticated httpx2 client through the two-stream API.
    """
    events: list[str] = []
    captured_headers: list[dict[str, str] | None] = []
    captured_endpoint: list[str] = []
    read_stream = object()
    write_stream = object()

    def _create_http_client(headers: dict[str, str] | None = None) -> FakeHttpClient:
        captured_headers.append(headers)
        events.append("http_create")
        return FakeHttpClient(events)

    @asynccontextmanager
    async def _streamable_http_client(
        endpoint_url: str,
        *,
        http_client: FakeHttpClient,
    ) -> AsyncIterator[tuple[object, object]]:
        captured_endpoint.append(endpoint_url)
        assert http_client.events is events
        events.append("stream_enter")
        try:
            yield read_stream, write_stream
        finally:
            events.append("stream_exit")

    def _client_session(read: object, write: object) -> FakeSDKSession:
        assert read is read_stream
        assert write is write_stream
        return FakeSDKSession(events, read, write)

    monkeypatch.setattr(mcp_bridge, "create_mcp_http_client", _create_http_client)
    monkeypatch.setattr(mcp_bridge, "streamable_http_client", _streamable_http_client)
    monkeypatch.setattr(mcp_bridge, "ClientSession", _client_session)
    bridge = MCPBridge("https://snapper.invalid")

    result = await bridge.list_tools("secret-token")

    assert isinstance(result, MCPToolCatalogSuccess)
    assert result.tools == []
    assert captured_headers == [{"Authorization": "Bearer secret-token"}]
    assert captured_endpoint == ["https://snapper.invalid/api/mcp"]
    assert events == [
        "http_create",
        "http_enter",
        "stream_enter",
        "session_enter",
        "initialize",
        "list_tools",
        "session_exit",
        "stream_exit",
        "http_exit",
    ]
