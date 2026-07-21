"""MCP contract tests for AI-research market-view tools."""

import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import CallToolResult
from mcp.types import TextContent

import snapper.mcp.tools as mcp_tools
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.tokens import TokenClaims
from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper.data.repository import Repository
from snapper.data.repository_types import MarketViewInsertRow
from snapper.data.repository_types import MarketViewRow
from snapper.data.repository_types import MarketViewSourceInsertRow
from snapper.mcp.tools import register_mcp_tools

_AS_OF = datetime(2026, 7, 21, 8, 0, tzinfo=UTC)
_SERVER_NOW = datetime(2026, 7, 21, 8, 5, tzinfo=UTC)


def _payload(**overrides: JsonValue) -> JsonObject:
    """Build one valid market-view JSON payload."""
    payload: JsonObject = {
        "as_of": _AS_OF.isoformat(),
        "valid_until": (_AS_OF + timedelta(hours=2)).isoformat(),
        "regime": "neutral",
        "bias": "longs_ok",
        "confidence": 0.75,
        "horizon_hours": 2,
        "key_risks": ["Unexpected inflation surprise"],
        "next_events": [
            {
                "when_utc": (_AS_OF + timedelta(hours=1)).isoformat(),
                "name": "CPI release",
                "severity": "high",
            }
        ],
        "sources": [
            {
                "url": "https://example.com/research",
                "title": "Market briefing",
                "retrieved_at": (_AS_OF - timedelta(minutes=5)).isoformat(),
            }
        ],
        "rationale": "Balanced conditions with event risk ahead.",
    }
    payload.update(overrides)
    return payload


def _claims(
    *,
    role: UserRole = UserRole.AI_RESEARCHER,
    permissions: list[str] | None = None,
) -> TokenClaims:
    """Build authenticated claims for a focused MCP invocation."""
    issued_at = int(_SERVER_NOW.timestamp())
    return TokenClaims(
        sub="research-user-1",
        username="researcher",
        role=role,
        permissions=permissions,
        exp=issued_at + 3600,
        iat=issued_at,
        jti="research-jti",
        sid="research-sid",
        user_public_id="research-user-1",
        operator_public_ids=[],
        primary_operator_public_id="",
    )


def _repository(
    *,
    insert_result: str = "market-view-1",
    insert_error: ValueError | None = None,
    latest: MarketViewRow | None = None,
) -> MagicMock:
    """Build a repository double for both research tools."""
    repository = MagicMock(spec=Repository)
    repository.insert_market_view = AsyncMock(
        return_value=insert_result,
        side_effect=insert_error,
    )
    repository.get_latest_market_view = AsyncMock(return_value=latest)
    return repository


def _market_view() -> MarketViewRow:
    """Build a complete persisted market-view projection."""
    return {
        "public_id": "market-view-1",
        "research_round_public_id": "round-1",
        "trigger": "periodic",
        "status": "completed",
        "as_of": _AS_OF,
        "submitted_at": _SERVER_NOW,
        "valid_until": _AS_OF + timedelta(hours=2),
        "regime": "neutral",
        "bias": "longs_ok",
        "confidence": 0.75,
        "horizon_hours": 2,
        "key_risks": ["Unexpected inflation surprise"],
        "next_events": [
            {
                "when_utc": _AS_OF + timedelta(hours=1),
                "name": "CPI release",
                "severity": "high",
            }
        ],
        "rationale": "Balanced conditions with event risk ahead.",
        "sources": [
            {
                "public_id": "source-1",
                "market_view_public_id": "market-view-1",
                "ordinal": 0,
                "url": "https://example.com/research",
                "title": "Market briefing",
                "retrieved_at": _AS_OF - timedelta(minutes=5),
            }
        ],
    }


def _server(repository: MagicMock | None, claims: TokenClaims) -> FastMCP:
    """Register the production MCP surface with focused dependencies."""
    server = FastMCP("ai-research-tools-test")
    register_mcp_tools(
        server,
        repository_getter=lambda: cast(Repository | None, repository),
        caps_enforcer_getter=lambda: None,
        claims_getter=lambda: claims,
    )
    return server


def _freeze_server_clock(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replace the MCP module clock with one deterministic instant."""
    server_clock = MagicMock(wraps=datetime)
    server_clock.now.return_value = _SERVER_NOW
    monkeypatch.setattr(mcp_tools, "datetime", server_clock)
    return server_clock


def _decode_result(result: object) -> JsonObject:
    """Decode one canonical MCP result envelope."""
    assert isinstance(result, CallToolResult)
    first = result.content[0]
    assert isinstance(first, TextContent)
    decoded = cast(JsonValue, json.loads(first.text))
    assert isinstance(decoded, dict)
    return decoded


def _object(value: JsonValue) -> JsonObject:
    """Narrow one decoded JSON value to an object."""
    assert isinstance(value, dict)
    return value


@pytest.mark.asyncio
async def test_submit_market_view_uses_inline_sources_and_server_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A valid authored payload persists inline sources at server time."""
    repository = _repository()
    server = _server(repository, _claims())
    server_clock = _freeze_server_clock(monkeypatch)

    result = await server._tool_manager.call_tool(
        "submit_market_view",
        {
            "research_round_public_id": "round-1",
            "payload": _payload(),
        },
    )

    envelope = _decode_result(result)
    details = _object(envelope["details"])
    assert envelope["success"] is True
    assert envelope["error_code"] is None
    assert details["market_view_public_id"] == "market-view-1"
    expected_row: MarketViewInsertRow = {
        "research_round_public_id": "round-1",
        "as_of": _AS_OF,
        "valid_until": _AS_OF + timedelta(hours=2),
        "regime": "neutral",
        "bias": "longs_ok",
        "confidence": 0.75,
        "horizon_hours": 2,
        "key_risks": ["Unexpected inflation surprise"],
        "next_events": [
            {
                "when_utc": _AS_OF + timedelta(hours=1),
                "name": "CPI release",
                "severity": "high",
            }
        ],
        "rationale": "Balanced conditions with event risk ahead.",
    }
    expected_sources: list[MarketViewSourceInsertRow] = [
        {
            "url": "https://example.com/research",
            "title": "Market briefing",
            "retrieved_at": _AS_OF - timedelta(minutes=5),
        }
    ]
    repository.insert_market_view.assert_awaited_once_with(
        expected_row,
        expected_sources,
        submitted_at=_SERVER_NOW,
    )
    server_clock.now.assert_called_once_with(UTC)


@pytest.mark.asyncio
async def test_submit_market_view_rejects_forged_submitted_at() -> None:
    """Strict payload validation rejects a caller-authored submission clock."""
    repository = _repository()
    server = _server(repository, _claims())

    result = await server._tool_manager.call_tool(
        "submit_market_view",
        {
            "research_round_public_id": "round-1",
            "payload": _payload(submitted_at=_SERVER_NOW.isoformat()),
        },
    )

    envelope = _decode_result(result)
    details = _object(envelope["details"])
    assert envelope["success"] is False
    assert envelope["error_code"] == "invalid_market_view"
    assert "submitted_at" in str(details["validation_error"])
    repository.insert_market_view.assert_not_awaited()


@pytest.mark.asyncio
async def test_submit_market_view_denies_role_without_permission() -> None:
    """A role without SUBMIT_MARKET_VIEW receives a permission envelope."""
    repository = _repository()
    server = _server(repository, _claims(role=UserRole.AI_DELEGATE))

    result = await server._tool_manager.call_tool(
        "submit_market_view",
        {
            "research_round_public_id": "round-1",
            "payload": _payload(),
        },
    )

    envelope = _decode_result(result)
    assert envelope["success"] is False
    assert envelope["error_code"] == "permission_denied"
    repository.insert_market_view.assert_not_awaited()


@pytest.mark.asyncio
async def test_submit_market_view_reports_uninitialized_repository() -> None:
    """A pre-lifespan repository returns service_unavailable."""
    server = _server(None, _claims())

    result = await server._tool_manager.call_tool(
        "submit_market_view",
        {
            "research_round_public_id": "round-1",
            "payload": _payload(),
        },
    )

    envelope = _decode_result(result)
    assert envelope["success"] is False
    assert envelope["error_code"] == "service_unavailable"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("repository_message", "expected_error_code"),
    [
        pytest.param(
            "market view requires at least one source",
            "invalid_market_view",
            id="sources",
        ),
        pytest.param(
            "market view rationale must not exceed 2048 UTF-8 bytes",
            "invalid_market_view",
            id="rationale",
        ),
        pytest.param(
            "market view requires a pending AI-research round",
            "research_round_not_pending",
            id="pending_round",
        ),
    ],
)
async def test_submit_market_view_maps_known_repository_rejections(
    repository_message: str,
    expected_error_code: str,
) -> None:
    """Every repository validation outcome remains a clean envelope."""
    repository = _repository(insert_error=ValueError(repository_message))
    server = _server(repository, _claims())

    result = await server._tool_manager.call_tool(
        "submit_market_view",
        {
            "research_round_public_id": "round-1",
            "payload": _payload(),
        },
    )

    envelope = _decode_result(result)
    details = _object(envelope["details"])
    assert envelope["success"] is False
    assert envelope["error_code"] == expected_error_code
    assert details["reason"] == repository_message


@pytest.mark.asyncio
async def test_submit_market_view_does_not_mask_unknown_repository_error() -> None:
    """An unknown repository ValueError remains observable through FastMCP."""
    repository = _repository(insert_error=ValueError("unexpected market view failure"))
    server = _server(repository, _claims())

    with pytest.raises(ToolError, match="unexpected market view failure"):
        await server._tool_manager.call_tool(
            "submit_market_view",
            {
                "research_round_public_id": "round-1",
                "payload": _payload(),
            },
        )


@pytest.mark.asyncio
async def test_get_latest_research_returns_full_artifact_at_server_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The latest read returns every artifact and source field."""
    repository = _repository(latest=_market_view())
    server = _server(repository, _claims())
    server_clock = _freeze_server_clock(monkeypatch)

    result = await server._tool_manager.call_tool("get_latest_research", {})

    envelope = _decode_result(result)
    details = _object(envelope["details"])
    market_view = _object(details["market_view"])
    assert envelope["success"] is True
    assert envelope["error_code"] is None
    assert market_view == {
        "public_id": "market-view-1",
        "research_round_public_id": "round-1",
        "trigger": "periodic",
        "status": "completed",
        "as_of": _AS_OF.isoformat(),
        "submitted_at": _SERVER_NOW.isoformat(),
        "valid_until": (_AS_OF + timedelta(hours=2)).isoformat(),
        "regime": "neutral",
        "bias": "longs_ok",
        "confidence": 0.75,
        "horizon_hours": 2,
        "key_risks": ["Unexpected inflation surprise"],
        "next_events": [
            {
                "when_utc": (_AS_OF + timedelta(hours=1)).isoformat(),
                "name": "CPI release",
                "severity": "high",
            }
        ],
        "rationale": "Balanced conditions with event risk ahead.",
        "sources": [
            {
                "public_id": "source-1",
                "market_view_public_id": "market-view-1",
                "ordinal": 0,
                "url": "https://example.com/research",
                "title": "Market briefing",
                "retrieved_at": (_AS_OF - timedelta(minutes=5)).isoformat(),
            }
        ],
    }
    repository.get_latest_market_view.assert_awaited_once_with(replay_at=_SERVER_NOW)
    server_clock.now.assert_called_once_with(UTC)


@pytest.mark.asyncio
async def test_get_latest_research_returns_none_cleanly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No eligible view is a successful read with a null artifact."""
    repository = _repository(latest=None)
    server = _server(repository, _claims())
    _freeze_server_clock(monkeypatch)

    result = await server._tool_manager.call_tool("get_latest_research", {})

    envelope = _decode_result(result)
    details = _object(envelope["details"])
    assert envelope["success"] is True
    assert envelope["error_code"] is None
    assert details["market_view"] is None
    repository.get_latest_market_view.assert_awaited_once_with(replay_at=_SERVER_NOW)


@pytest.mark.asyncio
async def test_get_latest_research_denies_narrow_token_without_permission() -> None:
    """A token without READ_MARKET_VIEWS receives a permission envelope."""
    repository = _repository(latest=_market_view())
    claims = _claims(permissions=[Permission.READ_MARKET_DATA.value])
    server = _server(repository, claims)

    result = await server._tool_manager.call_tool("get_latest_research", {})

    envelope = _decode_result(result)
    assert envelope["success"] is False
    assert envelope["error_code"] == "permission_denied"
    repository.get_latest_market_view.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_latest_research_reports_uninitialized_repository() -> None:
    """A pre-lifespan repository returns service_unavailable."""
    server = _server(None, _claims())

    result = await server._tool_manager.call_tool("get_latest_research", {})

    envelope = _decode_result(result)
    assert envelope["success"] is False
    assert envelope["error_code"] == "service_unavailable"
