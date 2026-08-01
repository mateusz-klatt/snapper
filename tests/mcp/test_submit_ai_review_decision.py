"""Tests for the ``submit_ai_review_decision`` MCP tool.

Covers the full envelope contract:

- Happy path approve / reject -> ``success=True, error_code=None``.
- Idempotent retry ->
  ``success=True, error_code='decision_already_recorded'``,
  ``isError=False`` so MCP-aware bridges don't surface it as an error.
- All hard errors (review_not_found / not_authorized / peer_resolved /
  review_id_expired) -> ``success=False, isError=True``.
- Invalid decision string (validated server-side) -> ``success=False,
  error_code='invalid_decision'`` WITHOUT calling submit_decision.
- Permission denied without SUBMIT_AI_REVIEW_DECISION capability ->
  ToolError.
- Pre-lifespan repository getter -> ToolError (RuntimeError chain).

The tool body is exercised through MCPServer's tool manager so the
dispatch path matches a real MCP client invocation.
"""

import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock

import jwt
import pytest
from mcp.server import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import CallToolResult
from mcp.types import TextContent

from snapper.application.ai_review.service import ERROR_NOT_SELECTED_BEFORE_FANOUT
from snapper.application.ai_review.service import AiReviewDecisionResult
from snapper.application.ai_review.service import AiReviewService
from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.scope_grant_service import ScopeGrantService
from snapper.auth.tokens import TokenManager
from snapper.core.types import AiReviewResolutionModeEnum
from snapper.core.types import AiReviewStatusEnum
from snapper.mcp.error_envelope import to_call_tool_result
from snapper.mcp.server import TOKEN_CLAIMS_CTX
from snapper.mcp.server import PermissionAwareMCPServer
from snapper.mcp.tools import register_mcp_tools
from tests.mcp.raw_dispatch import call_raw_tool


def _make_claims(
    role: UserRole = UserRole.AI_DELEGATE,
    user_public_id: str = "user-1",
    username: str = "delegate-1",
) -> TokenClaims:
    """Build a :class:`TokenClaims` for tool-permission tests."""
    now = int(datetime.now(UTC).timestamp())
    return TokenClaims(
        sub=user_public_id,
        username=username,
        role=role,
        permissions=None,
        exp=now + 3600,
        iat=now,
        jti="jti",
        sid="sid",
        user_public_id=user_public_id,
        operator_public_ids=["op-1"],
        primary_operator_public_id="op-1",
    )


def _decode_pre_versioning_claims(
    *,
    role: UserRole,
    username: str,
    user_public_id: str,
    permissions: list[str],
) -> TokenClaims:
    """Decode claims from a signed JWT that genuinely OMITS the scope claim.

    A pre-versioning token does not carry ``permission_scope_version`` set to
    null, it carries no such key at all. Encoding a payload without the key and
    decoding it through :meth:`TokenManager.verify_token` exercises the real
    claim-extraction path — ``jwt.decode`` then
    :meth:`TokenClaims.model_validate_json` — so the ``None`` reaching the
    projector comes from the schema default for an absent claim rather than
    from a test assignment. MCP tools consume :class:`TokenClaims` directly,
    so this is the production shape end to end.

    Args:
        role: Role claim for the minted token.
        username: Username claim for the minted token.
        user_public_id: Stable user identifier claim.
        permissions: Explicit legacy permission strings carried by the token.

    Returns:
        Claims decoded from a token with no permission-scope-version key.
    """
    token_manager = TokenManager()
    now = datetime.now(UTC)
    payload: dict[str, str | int | list[str]] = {
        "sub": user_public_id,
        "username": username,
        "role": role.value,
        "permissions": permissions,
        "sid": f"{username}-session",
        "exp": int((now + timedelta(hours=1)).timestamp()),
        "iat": int(now.timestamp()),
        "jti": f"{username}-jti",
        "user_public_id": user_public_id,
        "operator_public_ids": ["op-1"],
        "primary_operator_public_id": "op-1",
    }
    assert "permission_scope_version" not in payload
    token = jwt.encode(
        payload,
        token_manager.settings.auth_secret_key,
        algorithm=token_manager.settings.auth_algorithm,
    )
    claims = token_manager.verify_token(token)
    assert claims is not None
    assert claims.permission_scope_version is None
    return claims


def _build_server(
    repository: Any = None,
    claims: TokenClaims | None = None,
) -> PermissionAwareMCPServer:
    """Construct a MCPServer instance with tools registered + getters wired."""
    server = PermissionAwareMCPServer("test")
    register_mcp_tools(
        server,
        repository_getter=lambda: repository,
        caps_enforcer_getter=lambda: None,
        claims_getter=lambda: claims or _make_claims(),
    )
    return server


@pytest.fixture(autouse=True)
def _clear_singletons() -> Any:
    """Reset AiReviewService + ScopeGrantService singletons between cases."""
    AiReviewService.clear_instance()
    ScopeGrantService.clear_instance()
    yield
    AiReviewService.clear_instance()
    ScopeGrantService.clear_instance()


def _envelope_from(result: CallToolResult) -> dict[str, Any]:
    """Pull the JSON envelope out of a :class:`CallToolResult`.

    Narrows the union-typed ``content[0]`` to :class:`TextContent`
    so ``mypy`` accepts the ``.text`` access; the production helper
    only ever emits a single TextContent item, so the assertion is a
    type narrow not a runtime invariant.
    """
    first = result.content[0]
    assert isinstance(first, TextContent)
    envelope: dict[str, Any] = json.loads(first.text)
    return envelope


def _decode_call_tool_result(result: Any) -> dict[str, Any]:
    """Pull the JSON envelope out of a tool-manager dispatch result.

    MCPServer's tool manager preserves the :class:`CallToolResult`
    return shape verbatim for tools that explicitly declare it as
    return type; older callsites in the suite still see a dict
    serialisation, so the helper handles both paths.
    """
    if isinstance(result, CallToolResult):
        return _envelope_from(result)
    if isinstance(result, dict) and "content" in result:
        text = result["content"][0]
        raw = text["text"] if isinstance(text, dict) else text.text
        envelope: dict[str, Any] = json.loads(raw)
        return envelope
    raise AssertionError(f"Unexpected tool result shape: {result!r}")


class TestToCallToolResult:
    """Envelope helper contract."""

    def test_success_true_no_error_code_yields_iserror_false(self) -> None:
        """Happy path envelope.

        Given success=True + error_code=None + a message,
        When to_call_tool_result wraps it,
        Then isError is False and the JSON envelope round-trips the
        four contract fields with details defaulted to ``{}``.
        """
        result = to_call_tool_result(success=True, error_code=None, message="ok")
        assert result.is_error is False
        envelope = _envelope_from(result)
        assert envelope == {
            "success": True,
            "error_code": None,
            "message": "ok",
            "details": {},
        }

    def test_idempotent_retry_carries_error_code_with_iserror_false(self) -> None:
        """success=True + non-null error_code -> isError=False.

        Given success=True + error_code='decision_already_recorded',
        When to_call_tool_result wraps it,
        Then isError is False so MCP-aware bridges do NOT surface
        idempotent retries as tool errors, while the audit-trail
        discriminator stays in the JSON envelope.
        """
        result = to_call_tool_result(
            success=True,
            error_code="decision_already_recorded",
            message="idempotent retry",
            details={"decision": "approve"},
        )
        assert result.is_error is False
        envelope = _envelope_from(result)
        assert envelope["success"] is True
        assert envelope["error_code"] == "decision_already_recorded"
        assert envelope["details"] == {"decision": "approve"}

    def test_failure_yields_iserror_true(self) -> None:
        """Hard-error envelope.

        Given success=False + error_code='review_not_found',
        When to_call_tool_result wraps it,
        Then isError is True and the envelope JSON carries the
        classifier verbatim.
        """
        result = to_call_tool_result(
            success=False,
            error_code="review_not_found",
            message="no row",
            details={"review_public_id": "ghost"},
        )
        assert result.is_error is True
        envelope = _envelope_from(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "review_not_found"


class TestSubmitAiReviewDecisionTool:
    """Coverage for the ``submit_ai_review_decision`` MCP tool."""

    @staticmethod
    def _stub_submit_decision(
        monkeypatch: pytest.MonkeyPatch, result: AiReviewDecisionResult
    ) -> AsyncMock:
        """Stub the AiReviewService singleton's ``submit_decision``.

        Replaces the bound method on the singleton with an AsyncMock
        via :func:`monkeypatch.setattr` so MCP tool tests don't need a
        full DB round-trip; the tool body is what we want to exercise
        here. The autouse fixture clears the singleton between cases
        so the stub never leaks across tests.
        """
        svc = AiReviewService.get_instance()
        stub = AsyncMock(return_value=result)
        monkeypatch.setattr(svc, "submit_decision", stub)
        return stub

    @pytest.mark.asyncio
    async def test_happy_path_approve_returns_success_envelope(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """First valid approve -> success=True, error_code=None, isError=False.

        Given a stubbed AiReviewService whose submit_decision returns
        the first-valid-decision happy path envelope,
        When the MCP tool is dispatched with decision='approve',
        Then the CallToolResult carries success=True with
        status='resolved_approved' on details and isError=False.
        """
        stub = self._stub_submit_decision(
            monkeypatch,
            AiReviewDecisionResult(
                error_code=None,
                message="Decision recorded.",
                status=AiReviewStatusEnum.RESOLVED_APPROVED,
                resolution_mode=AiReviewResolutionModeEnum.PICK_ONE_PRIMARY,
                dispatch_version=0,
                details={"previous_status": "pending"},
            ),
        )
        server = _build_server(repository=AsyncMock())
        result = await call_raw_tool(
            server,
            "submit_ai_review_decision",
            {"review_id": "rev-1", "decision": "approve", "rationale": "LGTM"},
        )
        envelope = _decode_call_tool_result(result)
        assert envelope["success"] is True
        assert envelope["error_code"] is None
        assert envelope["details"]["status"] == "resolved_approved"
        assert envelope["details"]["resolution_mode"] == "pick_one_primary"
        assert envelope["details"]["dispatch_version"] == 0
        stub.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_v1_delegate_create_scope_keeps_approval_happy_path(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A historical v1 delegate still sees and calls the decision tool.

        Given: An explicit v1 AI_DELEGATE token carrying only historical CREATE_ORDERS.
        When: The token lists tools and submits an approve decision through MCP.
        Then: The shared capability projector both lists the tool and admits the
            call, so visibility and execution stay aligned for legacy tokens.
        """
        stub = self._stub_submit_decision(
            monkeypatch,
            AiReviewDecisionResult(
                error_code=None,
                message="Decision recorded.",
                status=AiReviewStatusEnum.RESOLVED_APPROVED,
                resolution_mode=AiReviewResolutionModeEnum.PICK_ONE_PRIMARY,
                dispatch_version=0,
                details={"previous_status": "pending"},
            ),
        )
        claims = _make_claims().model_copy(
            update={
                "permissions": [Permission.CREATE_ORDERS.value],
                "permission_scope_version": 1,
            }
        )
        server = _build_server(repository=AsyncMock(), claims=claims)

        context_token = TOKEN_CLAIMS_CTX.set(claims)
        try:
            visible_names = {tool.name for tool in await server.list_tools()}
        finally:
            TOKEN_CLAIMS_CTX.reset(context_token)

        result = await call_raw_tool(
            server,
            "submit_ai_review_decision",
            {"review_id": "rev-v1", "decision": "approve"},
        )

        assert "submit_ai_review_decision" in visible_names
        envelope = _decode_call_tool_result(result)
        assert envelope["success"] is True
        assert envelope["error_code"] is None
        assert envelope["details"]["status"] == "resolved_approved"
        stub.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_pre_versioning_delegate_token_sees_and_calls_the_decision_tool(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The production pre-versioning delegate token keeps MCP decision access.

        Given: AI_DELEGATE claims decoded from a signed JWT that OMITS the
            permission-scope-version claim entirely — the shape of the
            production delegate token, which predates scope versioning — whose
            explicit legacy permission list carries create:orders but not the
            dedicated decision permission.
        When: The token lists tools and submits an approve decision through MCP.
        Then: The tool is both listed and callable, pinning on this transport
            the absent-version compatibility whose absence previously hid the
            tool from the delegate and let consults expire unsubmittable.
        """
        stub = self._stub_submit_decision(
            monkeypatch,
            AiReviewDecisionResult(
                error_code=None,
                message="Decision recorded.",
                status=AiReviewStatusEnum.RESOLVED_APPROVED,
                resolution_mode=AiReviewResolutionModeEnum.PICK_ONE_PRIMARY,
                dispatch_version=0,
                details={"previous_status": "pending"},
            ),
        )
        claims = _decode_pre_versioning_claims(
            role=UserRole.AI_DELEGATE,
            username="pre-versioning-delegate",
            user_public_id="pre-versioning-user-1",
            permissions=[
                Permission.CANCEL_ORDERS.value,
                Permission.CREATE_ORDERS.value,
                Permission.MANAGE_POSITIONS.value,
                Permission.READ_BACKTESTS.value,
                Permission.READ_MARKET_DATA.value,
                Permission.READ_ORDERS.value,
                Permission.READ_POSITIONS.value,
                Permission.READ_SIGNALS.value,
                Permission.READ_STRATEGIES.value,
                Permission.READ_SYSTEM_STATUS.value,
            ],
        )
        server = _build_server(repository=AsyncMock(), claims=claims)

        context_token = TOKEN_CLAIMS_CTX.set(claims)
        try:
            visible_names = {tool.name for tool in await server.list_tools()}
        finally:
            TOKEN_CLAIMS_CTX.reset(context_token)

        result = await call_raw_tool(
            server,
            "submit_ai_review_decision",
            {"review_id": "rev-pre-versioning", "decision": "approve"},
        )

        assert claims.permission_scope_version is None
        assert Permission.SUBMIT_AI_REVIEW_DECISION.value not in (claims.permissions or [])
        assert "submit_ai_review_decision" in visible_names
        envelope = _decode_call_tool_result(result)
        assert envelope["success"] is True
        assert envelope["error_code"] is None
        stub.assert_awaited_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("role", "username", "user_public_id"),
        [
            pytest.param(UserRole.OPERATOR, "pre-versioning-operator", "pv-op-1", id="operator"),
            pytest.param(UserRole.ADMIN, "pre-versioning-admin", "pv-admin-1", id="admin"),
        ],
    )
    async def test_pre_versioning_create_only_token_is_denied_for_non_legacy_roles(
        self,
        role: UserRole,
        username: str,
        user_public_id: str,
    ) -> None:
        """An absent version claim does not open the tool to every create-only token.

        Given: Create-only claims decoded from a signed JWT with no
            permission-scope-version claim, on a role outside the legacy
            window — OPERATOR, whose ceiling never grants the decision
            permission, and ADMIN, which is excluded by the branch's
            no-user-administration conjunct despite holding it.
        When: Each lists tools and invokes the decision tool.
        Then: The tool is hidden from both catalogs and the call gate raises
            ToolError, so visibility and execution agree on the denial and the
            absent-version widening stays bounded to the intended ceilings.
        """
        claims = _decode_pre_versioning_claims(
            role=role,
            username=username,
            user_public_id=user_public_id,
            permissions=[Permission.CREATE_ORDERS.value],
        )
        server = _build_server(repository=AsyncMock(), claims=claims)

        context_token = TOKEN_CLAIMS_CTX.set(claims)
        try:
            visible_names = {tool.name for tool in await server.list_tools()}
        finally:
            TOKEN_CLAIMS_CTX.reset(context_token)

        with pytest.raises(ToolError) as exc:
            await call_raw_tool(
                server,
                "submit_ai_review_decision",
                {"review_id": f"rev-{username}", "decision": "approve"},
            )

        assert claims.permission_scope_version is None
        assert "submit_ai_review_decision" not in visible_names
        assert Permission.SUBMIT_AI_REVIEW_DECISION.value in str(exc.value)

    @pytest.mark.asyncio
    async def test_idempotent_retry_keeps_iserror_false(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Idempotent retry surfaces success=True with error_code.

        Given submit_decision returns ``decision_already_recorded``,
        When the MCP tool wraps the envelope,
        Then success=True (not a hard error) and the error_code is
        forwarded so the bridge can audit the duplicate.
        """
        self._stub_submit_decision(
            monkeypatch,
            AiReviewDecisionResult(
                error_code="decision_already_recorded",
                message="Decision already recorded; idempotent retry.",
                status=AiReviewStatusEnum.RESOLVED_APPROVED,
                resolution_mode=AiReviewResolutionModeEnum.PICK_ONE_PRIMARY,
                dispatch_version=0,
                details={"decision": "approve"},
            ),
        )
        server = _build_server(repository=AsyncMock())
        result = await call_raw_tool(
            server,
            "submit_ai_review_decision",
            {"review_id": "rev-1", "decision": "approve"},
        )
        envelope = _decode_call_tool_result(result)
        assert envelope["success"] is True
        assert envelope["error_code"] == "decision_already_recorded"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("error_code", "message"),
        [
            ("review_not_found", "No review with that id."),
            ("not_authorized", "Caller is not registered as an AI delegate."),
            (
                "not_selected_before_fanout",
                "Only the selected delegate may answer before fanout opens.",
            ),
            ("review_already_resolved_by_peer", "Peer beat us."),
            ("review_id_expired", "Deadline elapsed before the decision arrived."),
        ],
    )
    async def test_hard_errors_yield_failure_envelope(
        self, monkeypatch: pytest.MonkeyPatch, error_code: str, message: str
    ) -> None:
        """All hard-error codes -> success=False + isError=True.

        Given submit_decision returns a hard-error envelope,
        When the MCP tool wraps it,
        Then success=False and the classifier round-trips on the
        envelope JSON.
        """
        self._stub_submit_decision(
            monkeypatch,
            AiReviewDecisionResult(
                error_code=error_code,
                message=message,
                status=None,
                resolution_mode=None,
                dispatch_version=None,
                details={},
            ),
        )
        server = _build_server(repository=AsyncMock())
        result = await call_raw_tool(
            server,
            "submit_ai_review_decision",
            {"review_id": "rev-1", "decision": "reject"},
        )
        envelope = _decode_call_tool_result(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == error_code
        assert envelope["message"] == message

    @pytest.mark.asyncio
    async def test_pre_fanout_selection_error_preserves_retry_context(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The MCP failure envelope keeps the server-owned fanout opening instant.

        Given the service rejects a non-selected delegate before fanout,
        When the MCP tool wraps that result,
        Then the distinct error code and retry timestamp survive unchanged.
        """
        self._stub_submit_decision(
            monkeypatch,
            AiReviewDecisionResult(
                error_code=ERROR_NOT_SELECTED_BEFORE_FANOUT,
                message="Only the selected delegate may answer before fanout opens.",
                status=AiReviewStatusEnum.PENDING,
                resolution_mode=None,
                dispatch_version=0,
                details={"fanout_opens_at": "2026-08-01T12:00:30+00:00"},
            ),
        )
        server = _build_server(repository=AsyncMock())

        result = await call_raw_tool(
            server,
            "submit_ai_review_decision",
            {"review_id": "rev-1", "decision": "approve"},
        )

        envelope = _decode_call_tool_result(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == ERROR_NOT_SELECTED_BEFORE_FANOUT
        assert envelope["details"] == {
            "fanout_opens_at": "2026-08-01T12:00:30+00:00",
            "status": "pending",
            "dispatch_version": 0,
        }

    @pytest.mark.asyncio
    async def test_invalid_decision_short_circuits_with_envelope(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Invalid decision string -> envelope error WITHOUT calling submit_decision.

        Given a tool invocation with decision='maybe',
        When the tool body validates against AiReviewDecisionEnum,
        Then it returns success=False, error_code='invalid_decision'
        and submit_decision is never awaited.
        """
        stub = self._stub_submit_decision(
            monkeypatch,
            AiReviewDecisionResult(
                error_code=None,
                message="should never be called",
                status=AiReviewStatusEnum.RESOLVED_APPROVED,
                resolution_mode=AiReviewResolutionModeEnum.PICK_ONE_PRIMARY,
                dispatch_version=0,
                details={},
            ),
        )
        server = _build_server(repository=AsyncMock())
        result = await call_raw_tool(
            server,
            "submit_ai_review_decision",
            {"review_id": "rev-1", "decision": "maybe"},
        )
        envelope = _decode_call_tool_result(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "invalid_decision"
        stub.assert_not_called()

    @pytest.mark.asyncio
    async def test_permission_denied_for_role_without_decision_permission(self) -> None:
        """Role missing SUBMIT_AI_REVIEW_DECISION -> ToolError.

        Given a contrived VIEWER role with no permissions,
        When the MCP tool projects decision capability,
        Then ToolError is raised wrapping a PermissionError that names the
        dedicated decision permission.
        """
        saved = ROLE_PERMISSIONS.get(UserRole.VIEWER)
        ROLE_PERMISSIONS[UserRole.VIEWER] = set()
        server = MCPServer("test")
        register_mcp_tools(
            server,
            repository_getter=lambda: AsyncMock(),
            caps_enforcer_getter=lambda: None,
            claims_getter=lambda: _make_claims(role=UserRole.VIEWER),
        )
        try:
            with pytest.raises(ToolError) as exc:
                await call_raw_tool(
                    server,
                    "submit_ai_review_decision",
                    {"review_id": "rev-1", "decision": "approve"},
                )
            assert Permission.SUBMIT_AI_REVIEW_DECISION.value in str(exc.value)
        finally:
            if saved is not None:
                ROLE_PERMISSIONS[UserRole.VIEWER] = saved

    @pytest.mark.asyncio
    async def test_order_creator_without_decision_permission_is_denied_by_the_call_gate(
        self,
    ) -> None:
        """An order-creating principal without the decision write cannot call the tool.

        Given: A v2 OPERATOR token whose grant carries create:orders but never
            submit:ai_review_decision — the shape the historical call gate admitted.
        When: It invokes the submit_ai_review_decision MCP tool.
        Then: The aligned call gate raises ToolError naming the decision permission.
        """
        claims = _make_claims(role=UserRole.OPERATOR).model_copy(
            update={
                "permissions": sorted(
                    permission.value for permission in ROLE_PERMISSIONS[UserRole.OPERATOR]
                ),
                "permission_scope_version": 2,
            }
        )
        server = _build_server(repository=AsyncMock(), claims=claims)

        with pytest.raises(ToolError) as exc:
            await call_raw_tool(
                server,
                "submit_ai_review_decision",
                {"review_id": "rev-operator", "decision": "approve"},
            )

        assert Permission.CREATE_ORDERS in ROLE_PERMISSIONS[UserRole.OPERATOR]
        assert Permission.SUBMIT_AI_REVIEW_DECISION not in ROLE_PERMISSIONS[UserRole.OPERATOR]
        assert Permission.SUBMIT_AI_REVIEW_DECISION.value in str(exc.value)

    @pytest.mark.asyncio
    async def test_ai_reviewer_passes_the_call_gate_and_is_denied_by_the_service(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The review-only principal now clears the call gate and 403s one layer deeper.

        Given: A v2 AI_REVIEWER token carrying its complete review-only role grant,
            which holds submit:ai_review_decision but no delegate lifecycle identity.
        When: It invokes the submit_ai_review_decision MCP tool.
        Then: The aligned call gate admits it, the service is awaited, and the
            service's ``not_authorized`` envelope is what denies the write.
        """
        stub = self._stub_submit_decision(
            monkeypatch,
            AiReviewDecisionResult(
                error_code="not_authorized",
                message="Caller is not registered as an AI delegate.",
                status=None,
                resolution_mode=None,
                dispatch_version=None,
                details={},
            ),
        )
        claims = _make_claims(role=UserRole.AI_REVIEWER).model_copy(
            update={
                "permissions": sorted(
                    permission.value for permission in ROLE_PERMISSIONS[UserRole.AI_REVIEWER]
                ),
                "permission_scope_version": 2,
            }
        )
        server = _build_server(repository=AsyncMock(), claims=claims)

        result = await call_raw_tool(
            server,
            "submit_ai_review_decision",
            {"review_id": "rev-reviewer", "decision": "approve"},
        )

        envelope = _decode_call_tool_result(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "not_authorized"
        stub.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_repository_not_initialized_yields_tool_error(self) -> None:
        """Pre-lifespan repository -> ToolError, not opaque crash.

        Given the repository_getter returns None (lifespan has not
        initialized the singleton),
        When the tool dispatches,
        Then ToolError is raised wrapping a RuntimeError with the
        canonical "Repository not yet initialized" message.
        """
        server = _build_server(repository=None)
        with pytest.raises(ToolError) as exc:
            await call_raw_tool(
                server,
                "submit_ai_review_decision",
                {"review_id": "rev-1", "decision": "approve"},
            )
        assert "Repository not yet initialized" in str(exc.value)
