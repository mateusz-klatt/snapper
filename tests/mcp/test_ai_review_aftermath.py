"""MCP contract tests for the read-only AI-review aftermath tool."""

import json
from collections.abc import Generator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock

import pytest
from mcp.server.fastmcp import FastMCP
from mcp.types import CallToolResult
from mcp.types import TextContent

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.scope_grant_service import ScopeGrantService
from snapper.auth.scope_grant_service import get_scope_grant_service
from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper.data.repository import Repository
from snapper.data.repository_types import AiReviewAftermathRow
from snapper.data.repository_types import AiReviewRow
from snapper.mcp.tools import register_mcp_tools


def _claims(role: UserRole = UserRole.AI_DELEGATE) -> TokenClaims:
    """Build authenticated claims for one MCP tool invocation.

    Args:
        role: Role whose static permission set the tool should evaluate.

    Returns:
        Non-expired token claims with one operator membership.
    """
    now = int(datetime.now(UTC).timestamp())
    return TokenClaims(
        sub="user-1",
        username="delegate",
        role=role,
        permissions=None,
        exp=now + 3600,
        iat=now,
        jti="jti-1",
        sid="sid-1",
        user_public_id="user-1",
        operator_public_ids=["operator-1"],
        primary_operator_public_id="operator-1",
    )


def _review(status: str = "timeout") -> AiReviewRow:
    """Build a complete review row for MCP lifecycle checks.

    Args:
        status: Lifecycle state returned by the repository double.

    Returns:
        Fully populated review row.
    """
    created_at = datetime(2026, 7, 20, 10, 0, tzinfo=UTC)
    terminal = status not in {"pending", "fanout_dispatched"}
    return {
        "public_id": "review-1",
        "session_id": "session-1",
        "sequence_id": 1,
        "user_public_id": "strategy-user-1",
        "operator_public_id": "operator-1",
        "wallet_public_id": "wallet-1",
        "instrument_public_id": "instrument-1",
        "strategy_public_id": "strategy-1",
        "selected_delegate_public_id": "delegate-1",
        "responding_delegate_public_id": None,
        "resolution_mode": "timeout_no_response" if terminal else None,
        "status": status,
        "signal_envelope": {"side": "buy", "thesis": "breakout"},
        "signal_snapshot_hash": "a" * 64,
        "instrument_metadata": {"spread_bps": 8.0},
        "deadline": created_at + timedelta(seconds=5),
        "fanout_after": created_at + timedelta(seconds=2),
        "decision": None,
        "rationale": None,
        "dispatch_version": 1,
        "counter_decremented_at": created_at + timedelta(seconds=6) if terminal else None,
        "created_at": created_at,
        "updated_at": created_at + timedelta(seconds=6) if terminal else created_at,
        "resolved_at": created_at + timedelta(seconds=6) if terminal else None,
    }


def _aftermath(review: AiReviewRow) -> AiReviewAftermathRow:
    """Build a terminal projection with no subsequent trading activity.

    Args:
        review: Review row carried by the projection.

    Returns:
        Empty-activity and no-position aftermath fixture.
    """
    return {
        "review": review,
        "window_started_at": review["created_at"],
        "as_of": review["created_at"] + timedelta(minutes=1),
        "orders": [],
        "executions": [],
        "position_cycle_transitions": [],
        "current_positions": [],
    }


def _repository(
    *,
    delegate_registered: bool = True,
    review: AiReviewRow | None = None,
    scope_ok: bool = True,
    aftermath: AiReviewAftermathRow | None = None,
) -> AsyncMock:
    """Configure the repository reads used by the MCP handler.

    Args:
        delegate_registered: Whether user identity resolves to a delegate row.
        review: Review returned by the public-id lookup.
        scope_ok: Active delegate grant result.
        aftermath: Aggregate projection returned after policy checks.

    Returns:
        Async repository double.
    """
    repo = AsyncMock(spec=Repository)
    delegate = (
        {
            "public_id": "delegate-1",
            "user_public_id": "user-1",
            "last_seen_at": None,
            "active_reviews_count": 0,
            "created_at": datetime(2026, 7, 1, tzinfo=UTC),
            "updated_at": datetime(2026, 7, 1, tzinfo=UTC),
        }
        if delegate_registered
        else None
    )
    repo.get_ai_delegate_by_user_public_id = AsyncMock(return_value=delegate)
    repo.get_ai_review = AsyncMock(return_value=review)
    repo.has_grant_for_delegate = AsyncMock(return_value=scope_ok)
    repo.get_ai_review_aftermath = AsyncMock(return_value=aftermath)
    get_scope_grant_service().repository = cast(Repository, repo)
    return repo


@pytest.fixture(autouse=True)
def _clear_scope_grant_service() -> Generator[None]:
    """Reset the scope-policy singleton around each MCP case."""
    ScopeGrantService.clear_instance()
    yield
    ScopeGrantService.clear_instance()


def _server(repository: AsyncMock | None, claims: TokenClaims) -> FastMCP:
    """Register the production tool set against focused test dependencies.

    Args:
        repository: Optional repository double returned by the getter.
        claims: Authenticated claims returned by the getter.

    Returns:
        FastMCP server whose real tool manager can dispatch the new tool.
    """
    server = FastMCP("aftermath-test")
    register_mcp_tools(
        server,
        repository_getter=lambda: cast(Repository | None, repository),
        caps_enforcer_getter=lambda: None,
        claims_getter=lambda: claims,
    )
    return server


def _json_object(value: JsonValue) -> JsonObject:
    """Narrow a decoded JSON value to an object for assertions.

    Args:
        value: JSON value expected to be an object.

    Returns:
        The same value narrowed to :class:`JsonObject`.
    """
    assert isinstance(value, dict)
    return value


def _decode_result(result: object) -> JsonObject:
    """Decode FastMCP's native or serialized ``CallToolResult`` shape.

    Args:
        result: Value returned by the FastMCP tool manager.

    Returns:
        Canonical JSON envelope emitted by the tool.
    """
    raw_text: str
    if isinstance(result, CallToolResult):
        first = result.content[0]
        assert isinstance(first, TextContent)
        raw_text = first.text
    else:
        assert isinstance(result, dict)
        content = result.get("content")
        assert isinstance(content, list)
        first = content[0]
        if isinstance(first, TextContent):
            raw_text = first.text
        else:
            assert isinstance(first, dict)
            text_value = first.get("text")
            assert isinstance(text_value, str)
            raw_text = text_value
    decoded: object = json.loads(raw_text)
    assert isinstance(decoded, dict)
    return cast(JsonObject, decoded)


@pytest.mark.asyncio
async def test_get_ai_review_aftermath_returns_terminal_projection() -> None:
    """Registered scoped delegate receives the canonical aftermath envelope.

    Given a terminal review, active delegate grant, and empty projection,
    When the registered MCP tool is dispatched,
    Then one shared anchor scopes the reads and the full response is returned.
    """
    review = _review()
    repo = _repository(review=review, aftermath=_aftermath(review))
    result = await _server(repo, _claims())._tool_manager.call_tool(
        "get_ai_review_aftermath", {"review_public_id": "review-1"}
    )
    envelope = _decode_result(result)
    assert envelope["success"] is True
    assert envelope["error_code"] is None
    details = _json_object(envelope["details"])
    aftermath = _json_object(details["aftermath"])
    response_review = _json_object(aftermath["review"])
    assert response_review["public_id"] == "review-1"
    assert aftermath["orders"] == []
    assert aftermath["executions"] == []
    assert aftermath["position_cycle_transitions"] == []
    assert aftermath["current_positions"] == []
    scope_as_of = repo.has_grant_for_delegate.await_args.kwargs["as_of"]
    projection_as_of = repo.get_ai_review_aftermath.await_args.kwargs["as_of"]
    assert scope_as_of == projection_as_of


@pytest.mark.asyncio
async def test_get_ai_review_aftermath_rejects_unregistered_delegate() -> None:
    """Unregistered delegate identity returns ``not_a_delegate``.

    Given valid AI-delegate claims without an operational delegate row,
    When the aftermath tool is dispatched,
    Then it fails before loading the review.
    """
    repo = _repository(delegate_registered=False, review=_review())
    result = await _server(repo, _claims())._tool_manager.call_tool(
        "get_ai_review_aftermath", {"review_public_id": "review-1"}
    )
    envelope = _decode_result(result)
    assert envelope["success"] is False
    assert envelope["error_code"] == "not_a_delegate"
    repo.get_ai_review.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_ai_review_aftermath_hides_unknown_review() -> None:
    """Unknown review returns the stable anti-enumeration error.

    Given a registered delegate and no matching review row,
    When the aftermath tool is dispatched,
    Then ``review_not_found`` returns before scope and aggregate reads.
    """
    repo = _repository(review=None)
    result = await _server(repo, _claims())._tool_manager.call_tool(
        "get_ai_review_aftermath", {"review_public_id": "missing"}
    )
    envelope = _decode_result(result)
    assert envelope["error_code"] == "review_not_found"
    repo.has_grant_for_delegate.assert_not_awaited()
    repo.get_ai_review_aftermath.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_ai_review_aftermath_hides_out_of_scope_review() -> None:
    """Revoked delegate grant is indistinguishable from an unknown review.

    Given an existing terminal review outside the caller's active grant,
    When the aftermath tool is dispatched,
    Then it returns ``review_not_found`` and loads no trading projection.
    """
    repo = _repository(review=_review(), scope_ok=False)
    result = await _server(repo, _claims())._tool_manager.call_tool(
        "get_ai_review_aftermath", {"review_public_id": "review-1"}
    )
    envelope = _decode_result(result)
    assert envelope["error_code"] == "review_not_found"
    repo.get_ai_review_aftermath.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_ai_review_aftermath_rejects_pending_review() -> None:
    """Pending review returns explicit ``review_not_terminal`` failure.

    Given an in-scope review whose lifecycle is still pending,
    When the aftermath tool is dispatched,
    Then no activity is projected and the current status is reported.
    """
    repo = _repository(review=_review("pending"))
    result = await _server(repo, _claims())._tool_manager.call_tool(
        "get_ai_review_aftermath", {"review_public_id": "review-1"}
    )
    envelope = _decode_result(result)
    assert envelope["error_code"] == "review_not_terminal"
    details = _json_object(envelope["details"])
    assert details["status"] == "pending"
    repo.get_ai_review_aftermath.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_ai_review_aftermath_fails_closed_if_projection_disappears() -> None:
    """Missing aggregate after policy checks returns ``review_not_found``.

    Given a terminal in-scope review that vanishes before aggregate loading,
    When the aggregate repository method returns ``None``,
    Then the tool fails closed instead of fabricating empty activity.
    """
    repo = _repository(review=_review(), aftermath=None)
    result = await _server(repo, _claims())._tool_manager.call_tool(
        "get_ai_review_aftermath", {"review_public_id": "review-1"}
    )
    envelope = _decode_result(result)
    assert envelope["error_code"] == "review_not_found"


@pytest.mark.asyncio
async def test_get_ai_review_aftermath_requires_read_signals() -> None:
    """Viewer with position access cannot read the AI-review projection.

    Given a viewer whose role lacks ``READ_SIGNALS``,
    When the aftermath tool is dispatched,
    Then the canonical permission-denied envelope returns before repository use.
    """
    repo = _repository(review=_review())
    result = await _server(repo, _claims(UserRole.VIEWER))._tool_manager.call_tool(
        "get_ai_review_aftermath", {"review_public_id": "review-1"}
    )
    envelope = _decode_result(result)
    assert envelope["error_code"] == "permission_denied"
    repo.get_ai_delegate_by_user_public_id.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_ai_review_aftermath_reports_uninitialized_repository() -> None:
    """Pre-lifespan repository state returns ``service_unavailable``.

    Given valid delegate claims before the repository singleton is initialized,
    When the aftermath tool is dispatched,
    Then a structured lifecycle error returns instead of a raw exception.
    """
    result = await _server(None, _claims())._tool_manager.call_tool(
        "get_ai_review_aftermath", {"review_public_id": "review-1"}
    )
    envelope = _decode_result(result)
    assert envelope["error_code"] == "service_unavailable"
