"""Typed stateless bridge from delegate consults to Snapper MCP tools."""

from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from contextlib import asynccontextmanager
from dataclasses import dataclass
from dataclasses import field
from enum import StrEnum
from typing import Literal
from typing import Protocol

from loguru import logger
from mcp import ClientSession
from mcp import types
from mcp.client.streamable_http import streamable_http_client
from mcp.shared._httpx_utils import create_mcp_http_client
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import TypeAdapter
from pydantic import ValidationError

from snapper_delegate.chat_completions import ChatFunctionDefinition
from snapper_delegate.chat_completions import ChatTool
from snapper_delegate.json_types import JsonObject
from snapper_delegate.json_types import JsonValue

_CHAT_TOOL_SCHEMA_KEYS_TO_STRIP = frozenset({"$defs", "additionalProperties", "default", "title"})
_LOCAL_REFERENCE_PREFIX = "#/$defs/"
_MAX_REFERENCE_DEPTH = 32
_JSON_OBJECT_ADAPTER: TypeAdapter[JsonObject] = TypeAdapter(JsonObject)
_TOOL_FAILURE_CONTENT = (
    '{"success":false,"error_code":"mcp_bridge_error",'
    '"message":"MCP tool call failed","details":null}'
)
_MODEL_CONFIG = ConfigDict(extra="allow", frozen=True, strict=True, validate_default=True)


class MCPOutcome(StrEnum):
    """Terminal result categories exposed by the bridge."""

    SUCCESS = "success"
    ERROR = "error"


class MCPErrorKind(StrEnum):
    """Failure categories that callers may handle without inspecting exceptions."""

    SESSION = "session"
    MALFORMED_CATALOG = "malformed_catalog"
    MALFORMED_RESULT = "malformed_result"
    REMOTE = "remote"


class MCPSession(Protocol):
    """Narrow subset of an initialized MCP client session used by the bridge."""

    async def initialize(self) -> object:
        """Negotiate MCP protocol capabilities.

        Returns:
            The MCP initialization result supplied by the session.
        """
        ...

    async def list_tools(self) -> types.ListToolsResult:
        """Return the current MCP tool catalog.

        Returns:
            The tools advertised by the initialized MCP session.
        """
        ...

    async def call_tool(
        self,
        name: str,
        arguments: JsonObject | None = None,
    ) -> types.CallToolResult | types.InputRequiredResult | types.Result:
        """Invoke one MCP tool.

        Args:
            name: Exact advertised tool name.
            arguments: Validated JSON arguments, or no arguments when omitted.

        Returns:
            The raw typed result returned by the MCP session.
        """
        ...


class MCPSessionFactory(Protocol):
    """Create an asynchronous MCP session context for one bearer token."""

    def __call__(
        self,
        endpoint_url: str,
        access_token: str,
    ) -> AbstractAsyncContextManager[MCPSession]:
        """Return an inert context manager for one stateless MCP session."""
        ...


class _ToolEnvelope(BaseModel):
    """Validate the JSON envelope returned by Snapper MCP tools."""

    model_config = _MODEL_CONFIG

    success: bool
    error_code: str | None = None
    message: str | None = None
    details: JsonValue = None


@dataclass(frozen=True, slots=True)
class MCPToolCatalogSuccess:
    """Carry a sanitized catalog ready for a chat-completions request."""

    tools: list[ChatTool]
    outcome: Literal[MCPOutcome.SUCCESS] = field(default=MCPOutcome.SUCCESS, init=False)


@dataclass(frozen=True, slots=True)
class MCPToolCatalogFailure:
    """Report a catalog failure without retaining transport details."""

    error_kind: MCPErrorKind
    outcome: Literal[MCPOutcome.ERROR] = field(default=MCPOutcome.ERROR, init=False)


type MCPToolCatalogResult = MCPToolCatalogSuccess | MCPToolCatalogFailure


@dataclass(frozen=True, slots=True)
class MCPToolCallSuccess:
    """Carry a successful MCP tool envelope and its replayable JSON text."""

    content: str
    error_code: str | None
    message: str | None
    details: JsonValue
    success: Literal[True] = field(default=True, init=False)
    outcome: Literal[MCPOutcome.SUCCESS] = field(default=MCPOutcome.SUCCESS, init=False)


@dataclass(frozen=True, slots=True)
class MCPToolCallFailure:
    """Carry a recoverable MCP call failure and safe replayable content."""

    error_kind: MCPErrorKind
    content: str
    error_code: str | None
    message: str | None
    details: JsonValue
    success: Literal[False] = field(default=False, init=False)
    outcome: Literal[MCPOutcome.ERROR] = field(default=MCPOutcome.ERROR, init=False)


type MCPToolCallResult = MCPToolCallSuccess | MCPToolCallFailure


class MCPBridgeClient(Protocol):
    """Structural interface accepted by bounded consult orchestration."""

    async def list_tools(self, access_token: str) -> MCPToolCatalogResult:
        """Return a sanitized tool catalog for the current credential.

        Args:
            access_token: Bearer credential used for this stateless session.

        Returns:
            A sanitized catalog or a typed recoverable failure.
        """
        ...

    async def call_tool(
        self,
        access_token: str,
        name: str,
        arguments: JsonObject,
    ) -> MCPToolCallResult:
        """Invoke one tool and return replayable typed content.

        Args:
            access_token: Bearer credential used for this stateless session.
            name: Exact MCP tool name to invoke.
            arguments: Validated JSON object supplied to the tool.

        Returns:
            A replayable tool result or a typed recoverable failure.
        """
        ...


def _nullable_base(options: JsonValue) -> JsonObject | None:
    """Return the non-null branch of a two-option nullable schema."""
    if not isinstance(options, list) or len(options) != 2:
        return None
    object_options: list[JsonObject] = []
    for option in options:
        if not isinstance(option, dict):
            return None
        object_options.append(option)
    null_options = [option for option in object_options if option.get("type") == "null"]
    base_options = [option for option in object_options if option.get("type") != "null"]
    if len(null_options) != 1:
        return None
    return base_options[0]


def _local_reference_name(value: JsonValue) -> str | None:
    """Return the definition name for a local top-level definition reference."""
    if isinstance(value, str) and value.startswith(_LOCAL_REFERENCE_PREFIX):
        return value[len(_LOCAL_REFERENCE_PREFIX) :]
    return None


@dataclass(slots=True)
class _SchemaSanitizer:
    """Sanitize a schema against one immutable top-level definition map."""

    definitions: JsonObject
    warned_about_reference_recursion: bool = False

    def sanitize_value(
        self,
        value: JsonValue,
        active_references: frozenset[str],
    ) -> JsonValue:
        """Recursively sanitize one JSON schema value."""
        if isinstance(value, dict):
            return self.sanitize_object(value, active_references)
        if isinstance(value, list):
            return [self.sanitize_value(item, active_references) for item in value]
        return value

    def sanitize_object(
        self,
        schema: JsonObject,
        active_references: frozenset[str],
    ) -> JsonObject:
        """Sanitize one schema object, resolving an eligible local reference."""
        reference_name = _local_reference_name(schema.get("$ref"))
        if reference_name is not None:
            resolved = self._resolve_reference(schema, reference_name, active_references)
            if resolved is not None:
                return resolved
        return self._sanitize_plain_object(schema, active_references)

    def _resolve_reference(
        self,
        schema: JsonObject,
        reference_name: str,
        active_references: frozenset[str],
    ) -> JsonObject | None:
        """Inline one known acyclic reference or decline it fail closed."""
        definition = self.definitions.get(reference_name)
        if not isinstance(definition, dict):
            return None
        if reference_name in active_references or len(active_references) >= _MAX_REFERENCE_DEPTH:
            self._warn_about_reference_recursion()
            return None
        nested_references = active_references.union((reference_name,))
        sanitized_definition = self.sanitize_object(definition, nested_references)
        siblings = {key: value for key, value in schema.items() if key != "$ref"}
        sanitized_siblings = self._sanitize_plain_object(siblings, active_references)
        return {**sanitized_definition, **sanitized_siblings}

    def _sanitize_plain_object(
        self,
        schema: JsonObject,
        active_references: frozenset[str],
    ) -> JsonObject:
        """Strip unsupported keys and flatten one nullable schema object."""
        sanitized = {
            key: self.sanitize_value(value, active_references)
            for key, value in schema.items()
            if key not in _CHAT_TOOL_SCHEMA_KEYS_TO_STRIP
        }
        nullable_base = _nullable_base(schema.get("anyOf"))
        if nullable_base is None:
            return sanitized
        sanitized.pop("anyOf")
        return {
            **self.sanitize_object(nullable_base, active_references),
            **sanitized,
        }

    def _warn_about_reference_recursion(self) -> None:
        """Log one warning when local reference expansion cannot remain bounded."""
        if self.warned_about_reference_recursion:
            return
        self.warned_about_reference_recursion = True
        logger.warning("MCP tool schema contains cyclic or excessively deep local references")


def sanitize_chat_tool_schema(schema: JsonObject) -> JsonObject:
    """Inline definitions and remove unsupported chat-tool schema features.

    Args:
        schema: JSON schema emitted by the MCP tool catalog.

    Returns:
        A recursively sanitized schema accepted by chat completions.
    """
    raw_definitions = schema.get("$defs")
    definitions = raw_definitions if isinstance(raw_definitions, dict) else {}
    sanitizer = _SchemaSanitizer(definitions)
    return sanitizer.sanitize_object(schema, frozenset())


@asynccontextmanager
async def _sdk_session_factory(
    endpoint_url: str,
    access_token: str,
) -> AsyncIterator[MCPSession]:
    """Create one MCP SDK 2.x streamable-HTTP client session."""
    http_client = create_mcp_http_client(
        headers={"Authorization": f"Bearer {access_token}"},
    )
    async with (
        http_client,
        streamable_http_client(
            endpoint_url,
            http_client=http_client,
        ) as (read_stream, write_stream),
        ClientSession(
            read_stream,
            write_stream,
        ) as session,
    ):
        yield session


def _catalog_from_result(result: types.ListToolsResult) -> MCPToolCatalogResult:
    """Convert an MCP catalog to validated chat function definitions."""
    try:
        tools = [
            ChatTool(
                type="function",
                function=ChatFunctionDefinition(
                    name=tool.name,
                    description=tool.description,
                    parameters=sanitize_chat_tool_schema(
                        _JSON_OBJECT_ADAPTER.validate_python(tool.input_schema, strict=True)
                    ),
                ),
            )
            for tool in result.tools
        ]
    except ValidationError:
        return MCPToolCatalogFailure(error_kind=MCPErrorKind.MALFORMED_CATALOG)
    return MCPToolCatalogSuccess(tools=tools)


def _call_failure(error_kind: MCPErrorKind) -> MCPToolCallFailure:
    """Build a generic failure that is safe to replay to the model."""
    return MCPToolCallFailure(
        error_kind=error_kind,
        content=_TOOL_FAILURE_CONTENT,
        error_code="mcp_bridge_error",
        message="MCP tool call failed",
        details=None,
    )


def _call_from_result(
    result: types.CallToolResult | types.InputRequiredResult | types.Result,
) -> MCPToolCallResult:
    """Validate the single-text-content Snapper MCP result contract."""
    if not isinstance(result, types.CallToolResult) or len(result.content) != 1:
        return _call_failure(MCPErrorKind.MALFORMED_RESULT)
    content = result.content[0]
    if not isinstance(content, types.TextContent):
        return _call_failure(MCPErrorKind.MALFORMED_RESULT)
    try:
        envelope = _ToolEnvelope.model_validate_json(content.text)
    except ValidationError:
        return _call_failure(MCPErrorKind.MALFORMED_RESULT)
    if result.is_error == envelope.success:
        return _call_failure(MCPErrorKind.MALFORMED_RESULT)
    if not envelope.success:
        return MCPToolCallFailure(
            error_kind=MCPErrorKind.REMOTE,
            content=content.text,
            error_code=envelope.error_code,
            message=envelope.message,
            details=envelope.details,
        )
    return MCPToolCallSuccess(
        content=content.text,
        error_code=envelope.error_code,
        message=envelope.message,
        details=envelope.details,
    )


class MCPBridge:
    """Expose Snapper MCP operations through short-lived stateless sessions."""

    def __init__(
        self,
        base_url: str,
        *,
        session_factory: MCPSessionFactory | None = None,
    ) -> None:
        """Initialize the bridge without opening a network connection."""
        self._endpoint_url = f"{base_url.rstrip('/')}/api/mcp"
        self._session_factory = session_factory or _sdk_session_factory

    def __repr__(self) -> str:
        """Return a representation that omits endpoint and credential details."""
        return "MCPBridge()"

    async def list_tools(self, access_token: str) -> MCPToolCatalogResult:
        """Initialize a session and return its sanitized tool catalog.

        Args:
            access_token: Bearer credential used for this stateless session.

        Returns:
            A sanitized catalog or a typed session or validation failure.
        """
        try:
            async with self._session_factory(self._endpoint_url, access_token) as session:
                await session.initialize()
                result = await session.list_tools()
        except Exception:
            return MCPToolCatalogFailure(error_kind=MCPErrorKind.SESSION)
        return _catalog_from_result(result)

    async def call_tool(
        self,
        access_token: str,
        name: str,
        arguments: JsonObject,
    ) -> MCPToolCallResult:
        """Initialize a session, invoke one tool, and parse its result envelope.

        Args:
            access_token: Bearer credential used for this stateless session.
            name: Exact MCP tool name to invoke.
            arguments: Validated JSON object supplied to the tool.

        Returns:
            The replayable tool result or a typed contained failure.
        """
        try:
            async with self._session_factory(self._endpoint_url, access_token) as session:
                await session.initialize()
                result = await session.call_tool(name, arguments)
        except Exception:
            return _call_failure(MCPErrorKind.SESSION)
        return _call_from_result(result)
