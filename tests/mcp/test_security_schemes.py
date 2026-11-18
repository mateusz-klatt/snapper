"""Raw-wire regression tests for the ``tools/list`` security-scheme mirror.

These tests drive the installed MCP SDK's real Streamable HTTP transport with
JSON-RPC bodies for every supported protocol era, so they observe exactly what
the SDK's per-version result surface serializes, not a Pydantic model dump.
"""

from collections.abc import Callable
from typing import Any

import httpx
import pytest
from mcp.server import MCPServer
from mcp.server.context import CallNext
from mcp.server.context import HandlerResult
from mcp.server.context import ServerRequestContext
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ListToolsResult
from mcp.types import Tool
from mcp.types import ToolAnnotations
from mcp_types import methods as protocol_methods
from pydantic import BaseModel

from snapper.core.json_types import JsonArray
from snapper.core.json_types import JsonObject
from snapper.mcp.security_schemes import SECURITY_SCHEMES_FIELD
from snapper.mcp.security_schemes import TOOLS_LIST_METHOD
from snapper.mcp.security_schemes import mirror_list_tools_result
from snapper.mcp.security_schemes import mirror_security_schemes
from snapper.mcp.security_schemes import mirror_tool_security_schemes

HANDSHAKE_ERAS = ("2025-06-18", "2025-11-25")
MODERN_ERA = "2026-07-28"
ALL_ERAS = (*HANDSHAKE_ERAS, MODERN_ERA)
READ_SCHEMES: JsonArray = [{"type": "oauth2", "scopes": ["snapper.read"]}]
TRADE_SCHEMES: JsonArray = [{"type": "oauth2", "scopes": ["snapper.read", "snapper.trade"]}]
READ_ONLY = ToolAnnotations(
    read_only_hint=True, destructive_hint=False, idempotent_hint=True, open_world_hint=False
)
DESTRUCTIVE = ToolAnnotations(
    read_only_hint=False, destructive_hint=True, idempotent_hint=False, open_world_hint=True
)


class FilteredServer(MCPServer[None]):
    """Synthetic principal filter in the same ``list_tools`` hook Snapper uses."""

    def __init__(self, visible: frozenset[str], *, mirror: bool) -> None:
        """Build a server whose catalog exposes only ``visible`` tool names.

        Args:
            visible: Tool names the synthetic principal may list.
            mirror: Whether to install the security-scheme middleware.
        """
        super().__init__(
            "security-schemes-probe", middleware=[mirror_security_schemes] if mirror else []
        )
        self.visible = visible

    async def list_tools(self) -> list[Tool]:
        """Return the permission-filtered catalog.

        Returns:
            Registered tools whose names the synthetic principal may see.
        """
        return [tool for tool in await super().list_tools() if tool.name in self.visible]


def _register(server: MCPServer[None]) -> None:
    """Register a read tool, a write tool and a tool without security metadata.

    Args:
        server: Server receiving the synthetic catalog.
    """

    def read_tool() -> str:
        """Return a read result.

        Returns:
            Constant read marker.
        """
        return "read"

    def write_tool() -> str:
        """Return a write result.

        Returns:
            Constant write marker.
        """
        return "write"

    def plain_tool() -> str:
        """Return a result for a tool without OAuth metadata.

        Returns:
            Constant plain marker.
        """
        return "plain"

    server.add_tool(
        read_tool,
        name="read_tool",
        annotations=READ_ONLY,
        meta={SECURITY_SCHEMES_FIELD: READ_SCHEMES},
    )
    server.add_tool(
        write_tool,
        name="write_tool",
        annotations=DESTRUCTIVE,
        meta={SECURITY_SCHEMES_FIELD: TRADE_SCHEMES},
    )
    server.add_tool(plain_tool, name="plain_tool", meta={"vendor/other": "kept"})


def _build(visible: frozenset[str], *, mirror: bool = True) -> FilteredServer:
    """Build one registered synthetic server.

    Args:
        visible: Tool names the synthetic principal may list.
        mirror: Whether to install the security-scheme middleware.

    Returns:
        Server with the synthetic catalog registered.
    """
    server = FilteredServer(visible, mirror=mirror)
    _register(server)
    return server


def _request_params(version: str) -> JsonObject:
    """Return request params carrying modern-era per-request client metadata.

    Args:
        version: Negotiated protocol version.

    Returns:
        Empty params for handshake eras, or the 2026 per-request ``_meta``.
    """
    if version != MODERN_ERA:
        return {}
    return {
        "_meta": {
            "io.modelcontextprotocol/protocolVersion": version,
            "io.modelcontextprotocol/clientInfo": {"name": "probe", "version": "0"},
            "io.modelcontextprotocol/clientCapabilities": {},
        }
    }


async def _rpc(
    server: MCPServer[None], version: str, method: str, params: JsonObject
) -> JsonObject:
    """Send one JSON-RPC request through the SDK's stateless HTTP transport.

    Handshake eras first complete ``initialize`` as a real client would.

    Args:
        server: Server under test.
        version: Protocol version sent in headers and the handshake.
        method: JSON-RPC method name.
        params: JSON-RPC params, merged with modern-era request metadata.

    Returns:
        Decoded JSON-RPC response envelope.
    """
    app = server.streamable_http_app(
        streamable_http_path="/",
        stateless_http=True,
        json_response=True,
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
    )
    headers = {"Accept": "application/json, text/event-stream", "MCP-Protocol-Version": version}
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://mcp.test") as client:
            if version != MODERN_ERA:
                handshake = await client.post(
                    "/",
                    headers=headers,
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "initialize",
                        "params": {
                            "protocolVersion": version,
                            "capabilities": {},
                            "clientInfo": {"name": "probe", "version": "0"},
                        },
                    },
                )
                assert handshake.status_code == 200
            routing = {"Mcp-Method": method}
            if isinstance(params.get("name"), str):
                routing["Mcp-Name"] = str(params["name"])
            response = await client.post(
                "/",
                headers={**headers, **routing},
                json={
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": method,
                    "params": {**params, **_request_params(version)},
                },
            )
    assert response.status_code == 200
    body: JsonObject = response.json()
    return body


async def _list_tools(server: MCPServer[None], version: str) -> list[JsonObject]:
    """Return the raw wire tool entries of one ``tools/list`` call.

    Args:
        server: Server under test.
        version: Protocol version for the call.

    Returns:
        Serialized tool objects exactly as the client receives them.
    """
    body = await _rpc(server, version, TOOLS_LIST_METHOD, {})
    result = body["result"]
    assert isinstance(result, dict)
    tools = result["tools"]
    assert isinstance(tools, list)
    return [tool for tool in tools if isinstance(tool, dict)]


@pytest.mark.parametrize("version", ALL_ERAS)
async def test_read_principal_wire_carries_both_forms(version: str) -> None:
    """A filtered read catalog carries both scheme forms and read annotations.

    Given a mirrored server whose principal may list only the read tool.
    When a raw tools/list request is sent in each protocol era.
    Then the tool carries equal top-level and _meta schemes plus read-only annotations.
    """
    tools = await _list_tools(_build(frozenset({"read_tool"})), version)

    assert [tool["name"] for tool in tools] == ["read_tool"]
    tool = tools[0]
    assert tool[SECURITY_SCHEMES_FIELD] == READ_SCHEMES
    assert tool["_meta"] == {SECURITY_SCHEMES_FIELD: READ_SCHEMES}
    assert tool["annotations"] == {
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }


@pytest.mark.parametrize("version", ALL_ERAS)
async def test_mirror_preserves_filtering_order_and_unannotated_tools(version: str) -> None:
    """Mirroring keeps the filtered set and order and invents no scheme.

    Given identical mirrored and unmirrored catalogs with read, write and plain tools.
    When both are listed on the raw wire in each protocol era.
    Then only the declared schemes are added and every other byte is identical.
    """
    visible = frozenset({"read_tool", "write_tool", "plain_tool"})
    mirrored = await _list_tools(_build(visible), version)
    baseline = await _list_tools(_build(visible, mirror=False), version)

    assert [tool["name"] for tool in mirrored] == [tool["name"] for tool in baseline]
    by_name = {str(tool["name"]): tool for tool in mirrored}
    assert by_name["write_tool"][SECURITY_SCHEMES_FIELD] == TRADE_SCHEMES
    assert by_name["write_tool"]["annotations"] == {
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": True,
    }
    assert by_name["plain_tool"] == next(tool for tool in baseline if tool["name"] == "plain_tool")
    for tool, expected in zip(mirrored, baseline, strict=True):
        assert {
            key: value for key, value in tool.items() if key != SECURITY_SCHEMES_FIELD
        } == expected


@pytest.mark.parametrize("version", ALL_ERAS)
async def test_sdk_strips_top_level_field_without_middleware(version: str) -> None:
    """Regression guard: the SDK alone emits only the _meta form.

    Given a server without the mirror middleware.
    When the read tool is listed on the raw wire.
    Then the top-level field is absent while _meta keeps the schemes.
    """
    tools = await _list_tools(_build(frozenset({"read_tool"}), mirror=False), version)

    assert SECURITY_SCHEMES_FIELD not in tools[0]
    assert tools[0]["_meta"] == {SECURITY_SCHEMES_FIELD: READ_SCHEMES}


@pytest.mark.parametrize("version", ALL_ERAS)
def test_protocol_surface_drops_top_level_field(version: str) -> None:
    """The per-version protocol surface drops the top-level field.

    Given a dumped tools/list result with a top-level securitySchemes entry.
    When the SDK serializes it through the protocol surface for each era.
    Then the top-level field is removed, so no typed result can carry it.
    """
    result = ListToolsResult(
        tools=[
            Tool(
                name="read_tool",
                input_schema={"type": "object"},
                meta={SECURITY_SCHEMES_FIELD: READ_SCHEMES},
            )
        ]
    ).model_dump(by_alias=True, mode="json", exclude_none=True)
    tools = result["tools"]
    tools[0][SECURITY_SCHEMES_FIELD] = READ_SCHEMES

    sieved = protocol_methods.serialize_server_result(TOOLS_LIST_METHOD, version, result)

    assert SECURITY_SCHEMES_FIELD not in sieved["tools"][0]


async def test_modern_era_marks_filtered_catalog_private() -> None:
    """The 2026 era keeps a per-principal catalog out of shared caches.

    Given a mirrored filtered server.
    When tools/list is sent in the 2026-07-28 era.
    Then the result is marked cacheScope private with a zero TTL.
    """
    body = await _rpc(_build(frozenset({"read_tool"})), MODERN_ERA, TOOLS_LIST_METHOD, {})
    result = body["result"]
    assert isinstance(result, dict)

    assert result["cacheScope"] == "private"
    assert result["ttlMs"] == 0


@pytest.mark.parametrize("version", HANDSHAKE_ERAS)
async def test_handshake_eras_have_no_cache_fields(version: str) -> None:
    """Handshake-era surfaces carry no cache hints on the wire.

    Given a mirrored filtered server.
    When tools/list is sent in a handshake era.
    Then neither cacheScope nor ttlMs appears in the result.
    """
    body = await _rpc(_build(frozenset({"read_tool"})), version, TOOLS_LIST_METHOD, {})
    result = body["result"]
    assert isinstance(result, dict)

    assert "cacheScope" not in result
    assert "ttlMs" not in result


@pytest.mark.parametrize("version", ALL_ERAS)
async def test_tool_call_and_errors_are_unchanged(version: str) -> None:
    """Calls and JSON-RPC errors are identical with and without the mirror.

    Given mirrored and unmirrored servers with the same catalog.
    When a known and an unknown tool are called in each era.
    Then both responses are equal.
    """
    visible = frozenset({"read_tool"})
    call: JsonObject = {"name": "read_tool", "arguments": {}}
    unknown: JsonObject = {"name": "missing_tool", "arguments": {}}

    for params in (call, unknown):
        mirrored = await _rpc(_build(visible), version, "tools/call", params)
        baseline = await _rpc(_build(visible, mirror=False), version, "tools/call", params)
        assert mirrored == baseline


class _Model(BaseModel):
    """Typed result returned by a short-circuiting middleware."""

    tools: list[str]


@pytest.mark.parametrize(
    ("method", "result"),
    [
        ("tools/call", {"tools": [{"_meta": {SECURITY_SCHEMES_FIELD: READ_SCHEMES}}]}),
        (TOOLS_LIST_METHOD, None),
        (TOOLS_LIST_METHOD, _Model(tools=["read_tool"])),
        (TOOLS_LIST_METHOD, {"tools": "not-a-list"}),
        (TOOLS_LIST_METHOD, {"nextCursor": "c"}),
    ],
)
def test_non_list_results_pass_through_identically(method: str, result: HandlerResult) -> None:
    """Non-list methods and non-list result shapes are returned as the same object.

    Given a downstream result that is not a tools/list mapping with a tool list.
    When the result is mirrored.
    Then the identical object is returned.
    """
    assert mirror_list_tools_result(method, result) is result


@pytest.mark.parametrize(
    "tool",
    [
        "not-a-mapping",
        {"name": "no_meta"},
        {"name": "meta_not_mapping", "_meta": ["x"]},
        {"name": "meta_without_schemes", "_meta": {"vendor/other": "kept"}},
        {"name": "null_schemes", "_meta": {SECURITY_SCHEMES_FIELD: None}},
    ],
)
def test_tools_without_schemes_are_returned_identically(tool: object) -> None:
    """Entries without a declared scheme are never rewritten.

    Given a serialized tool entry without usable _meta.securitySchemes.
    When the entry is mirrored.
    Then the identical entry is returned.
    """
    assert mirror_tool_security_schemes(tool) is tool


def test_mirroring_copies_without_mutating_input() -> None:
    """Mirroring returns new mappings and leaves the downstream result untouched.

    Given a tools/list result with one scheme-bearing tool and a cursor.
    When the result is mirrored.
    Then a new result carries the field and the input objects are unchanged.
    """
    tool: JsonObject = {"name": "read_tool", "_meta": {SECURITY_SCHEMES_FIELD: READ_SCHEMES}}
    result: JsonObject = {"tools": [tool], "nextCursor": "c"}

    mirrored = mirror_list_tools_result(TOOLS_LIST_METHOD, result)

    assert mirrored == {
        "tools": [{**tool, SECURITY_SCHEMES_FIELD: READ_SCHEMES}],
        "nextCursor": "c",
    }
    assert mirrored is not result
    assert result == {"tools": [tool], "nextCursor": "c"}
    assert SECURITY_SCHEMES_FIELD not in tool


async def test_middleware_reads_method_and_awaits_chain_once() -> None:
    """The middleware forwards the context once and shapes its result.

    Given a recording downstream chain returning a tools/list result.
    When the middleware runs with a real SDK request context.
    Then the chain is awaited once and the schemes are mirrored.
    """
    calls: list[str] = []
    tool: JsonObject = {"name": "t", "_meta": {SECURITY_SCHEMES_FIELD: READ_SCHEMES}}
    downstream: JsonObject = {"tools": [tool]}

    async def call_next(ctx: ServerRequestContext[Any, Any]) -> HandlerResult:
        """Record the forwarded method and return a wire result.

        Args:
            ctx: Context forwarded by the middleware.

        Returns:
            A synthetic ``tools/list`` wire result.
        """
        calls.append(ctx.method)
        return downstream

    ctx = _context(TOOLS_LIST_METHOD)
    chain: CallNext = call_next
    result = await mirror_security_schemes(ctx, chain)

    assert calls == [TOOLS_LIST_METHOD]
    assert result == {"tools": [{**tool, SECURITY_SCHEMES_FIELD: READ_SCHEMES}]}


async def test_middleware_propagates_downstream_errors() -> None:
    """A raising chain is not swallowed or converted.

    Given a downstream chain that raises.
    When the middleware runs.
    Then the same exception propagates.
    """

    async def call_next(ctx: ServerRequestContext[Any, Any]) -> HandlerResult:
        """Fail like a handler error.

        Args:
            ctx: Context forwarded by the middleware.

        Raises:
            RuntimeError: Always.
        """
        raise RuntimeError(ctx.method)

    with pytest.raises(RuntimeError, match=TOOLS_LIST_METHOD):
        await mirror_security_schemes(_context(TOOLS_LIST_METHOD), call_next)


def _context(method: str) -> ServerRequestContext[Any, Any]:
    """Build a minimal real SDK request context.

    Args:
        method: Request method name.

    Returns:
        SDK context carrying the method name.
    """
    factory: Callable[..., ServerRequestContext[Any, Any]] = ServerRequestContext
    return factory(
        session=None,
        lifespan_context=None,
        protocol_version=MODERN_ERA,
        method=method,
        params=None,
        request_id=1,
    )
