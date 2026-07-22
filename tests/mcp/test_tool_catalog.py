"""Permission-aware MCP tool-catalog regression tests."""

from datetime import UTC
from datetime import datetime

import pytest
from mcp.types import ListToolsRequest
from mcp.types import ListToolsResult

from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.tokens import TokenClaims
from snapper.mcp.server import TOKEN_CLAIMS_CTX
from snapper.mcp.server import PermissionAwareFastMCP
from snapper.mcp.server import get_current_claims
from snapper.mcp.tool_catalog import MCP_TOOL_VISIBILITY_POLICY
from snapper.mcp.tool_catalog import is_mcp_tool_visible
from snapper.mcp.tools import register_mcp_tools


def _make_claims(
    role: UserRole,
    permissions: list[str],
    permission_scope_version: int = 2,
) -> TokenClaims:
    """Build authenticated claims for one catalog principal.

    Args:
        role: Role ceiling applied to the token grant.
        permissions: Explicit permission strings embedded in the token.
        permission_scope_version: Version of the explicit token scope.

    Returns:
        Token claims ready for the MCP request context.
    """
    now = int(datetime.now(UTC).timestamp())
    return TokenClaims(
        sub=f"{role.value}-user",
        username=f"{role.value}-catalog-test",
        role=role,
        permissions=permissions,
        permission_scope_version=permission_scope_version,
        exp=now + 3600,
        iat=now,
        jti=f"{role.value}-catalog-jti",
        sid=f"{role.value}-catalog-sid",
        user_public_id=f"{role.value}-user",
    )


def _build_catalog_server() -> PermissionAwareFastMCP:
    """Build the production tool registration on the filtered server.

    Returns:
        Permission-aware FastMCP server with all production tools registered.
    """
    server = PermissionAwareFastMCP("tool-catalog-test")
    register_mcp_tools(
        server,
        repository_getter=lambda: None,
        caps_enforcer_getter=lambda: None,
        claims_getter=get_current_claims,
    )
    return server


async def _listed_tool_names(
    server: PermissionAwareFastMCP,
    claims: TokenClaims | None,
) -> set[str]:
    """Return catalog names under an optional claims context.

    Args:
        server: Permission-aware FastMCP server to query.
        claims: Claims to bind, or ``None`` for the compatibility path.

    Returns:
        Names returned by the server's registered protocol list handler.
    """
    context_token = TOKEN_CLAIMS_CTX.set(claims) if claims is not None else None
    try:
        handler = server._mcp_server.request_handlers[ListToolsRequest]
        server_result = await handler(ListToolsRequest())
        list_result = server_result.root
        assert isinstance(list_result, ListToolsResult)
        return {tool.name for tool in list_result.tools}
    finally:
        if context_token is not None:
            TOKEN_CLAIMS_CTX.reset(context_token)


@pytest.mark.asyncio
async def test_policy_covers_every_registered_tool_and_hides_unknown_tools() -> None:
    """The central policy is exhaustive and authenticated catalogs fail closed.

    Given: The complete production MCP registration and an authenticated reviewer.
    When: Policy keys are compared with registrations and an unknown tool is checked.
    Then: Every registered tool has one policy entry and the unknown tool is hidden.
    """
    server = _build_catalog_server()
    registered_names = await _listed_tool_names(server, None)
    claims = _make_claims(
        UserRole.AI_REVIEWER,
        sorted(permission.value for permission in ROLE_PERMISSIONS[UserRole.AI_REVIEWER]),
    )

    assert set(MCP_TOOL_VISIBILITY_POLICY) == registered_names
    assert is_mcp_tool_visible("future_unmapped_tool", claims) is False


@pytest.mark.asyncio
async def test_absent_claims_preserve_the_complete_catalog() -> None:
    """Context-free list paths retain the historical unfiltered behavior.

    Given: The complete production MCP registration with no claims in the ContextVar.
    When: The permission-aware list handler is invoked directly.
    Then: All policy-covered tools are returned without an authentication failure.
    """
    server = _build_catalog_server()

    visible_names = await _listed_tool_names(server, None)

    assert visible_names == set(MCP_TOOL_VISIBILITY_POLICY)


@pytest.mark.asyncio
async def test_ai_reviewer_sees_review_tool_without_research_or_trading_mutations() -> None:
    """The review-only principal sees decisions but no research or trading writes.

    Given: An AI_REVIEWER token carrying its complete v2 role grant.
    When: The permission-aware FastMCP list handler builds its catalog.
    Then: The decision tool is visible while web tools and research or trading writes are absent.
    """
    server = _build_catalog_server()
    claims = _make_claims(
        UserRole.AI_REVIEWER,
        sorted(permission.value for permission in ROLE_PERMISSIONS[UserRole.AI_REVIEWER]),
    )

    visible_names = await _listed_tool_names(server, claims)

    assert visible_names == {
        "list_instruments",
        "submit_ai_review_decision",
        "get_latest_research",
        "list_orders",
        "get_order_status",
        "list_positions",
        "get_position_cycle",
        "get_ai_review_aftermath",
        "get_ohlcv",
        "list_recent_signals",
    }
    assert {
        "submit_market_view",
        "web_search",
        "read_source",
        "submit_manual_order",
        "cancel_order",
    }.isdisjoint(visible_names)


@pytest.mark.asyncio
async def test_ai_researcher_sees_research_tools_without_review_or_trading_tools() -> None:
    """The research principal sees market research but no review or trading tools.

    Given: An AI_RESEARCHER token carrying its complete v2 role grant.
    When: The permission-aware FastMCP list handler builds its catalog.
    Then: Research submit/read tools appear and decision plus trading tools stay hidden.
    """
    server = _build_catalog_server()
    claims = _make_claims(
        UserRole.AI_RESEARCHER,
        sorted(permission.value for permission in ROLE_PERMISSIONS[UserRole.AI_RESEARCHER]),
    )

    visible_names = await _listed_tool_names(server, claims)

    assert visible_names == {
        "list_instruments",
        "submit_market_view",
        "get_latest_research",
        "get_ohlcv",
    }
    assert {
        "submit_ai_review_decision",
        "submit_manual_order",
        "cancel_order",
    }.isdisjoint(visible_names)
