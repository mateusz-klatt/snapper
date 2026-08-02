"""Tests for bounded chat and MCP review consultation."""

import json
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from unittest.mock import MagicMock

import pytest
from pydantic import SecretStr

from snapper.core.json_types import JsonObject
from snapper_delegate.chat_completions import AssistantChatMessage
from snapper_delegate.chat_completions import ChatCompletionChoice
from snapper_delegate.chat_completions import ChatCompletionErrorKind
from snapper_delegate.chat_completions import ChatCompletionFailure
from snapper_delegate.chat_completions import ChatCompletionQuotaExhausted
from snapper_delegate.chat_completions import ChatCompletionRequest
from snapper_delegate.chat_completions import ChatCompletionResponse
from snapper_delegate.chat_completions import ChatCompletionResult
from snapper_delegate.chat_completions import ChatCompletionSuccess
from snapper_delegate.chat_completions import ChatCompletionUsage
from snapper_delegate.chat_completions import ChatFunctionCall
from snapper_delegate.chat_completions import ChatFunctionDefinition
from snapper_delegate.chat_completions import ChatRole
from snapper_delegate.chat_completions import ChatTool
from snapper_delegate.chat_completions import ChatToolCall
from snapper_delegate.consult import BoundedConsultRunner
from snapper_delegate.consult import ConsultConfiguration
from snapper_delegate.consult import ConsultOutcome
from snapper_delegate.consult import ReviewConsultContext
from snapper_delegate.control_plane import ControlPlaneError
from snapper_delegate.control_plane import ControlPlaneErrorKind
from snapper_delegate.mcp_bridge import MCPErrorKind
from snapper_delegate.mcp_bridge import MCPToolCallFailure
from snapper_delegate.mcp_bridge import MCPToolCallResult
from snapper_delegate.mcp_bridge import MCPToolCallSuccess
from snapper_delegate.mcp_bridge import MCPToolCatalogFailure
from snapper_delegate.mcp_bridge import MCPToolCatalogResult
from snapper_delegate.mcp_bridge import MCPToolCatalogSuccess

_NOW = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)
_FUTURE = _NOW + timedelta(minutes=5)
_SUCCESS_CONTENT = '{"success":true,"error_code":null,"message":"Decision recorded","details":null}'
_REMOTE_FAILURE_CONTENT = (
    '{"success":false,"error_code":"lookup_failed","message":"Lookup failed","details":null}'
)


class FakeChatClient:
    """Return injected completion outcomes and record every immutable request."""

    def __init__(self, results: list[ChatCompletionResult]) -> None:
        """Retain ordered outcomes for subsequent model turns."""
        self.results = list(results)
        self.requests: list[ChatCompletionRequest] = []

    async def complete(self, request: ChatCompletionRequest) -> ChatCompletionResult:
        """Record the request and consume its injected outcome."""
        self.requests.append(request)
        if not self.results:
            raise AssertionError("Unexpected chat completion request")
        return self.results.pop(0)


class FakeMCPBridge:
    """Expose injected MCP outcomes through the consult bridge protocol."""

    def __init__(
        self,
        catalog_result: MCPToolCatalogResult,
        call_results: list[MCPToolCallResult] | None = None,
    ) -> None:
        """Retain catalog and ordered tool-call outcomes."""
        self.catalog_result = catalog_result
        self.call_results = list(call_results or [])
        self.list_tokens: list[str] = []
        self.calls: list[tuple[str, str, JsonObject]] = []

    async def list_tools(self, access_token: str) -> MCPToolCatalogResult:
        """Record the credential and return the injected catalog result."""
        self.list_tokens.append(access_token)
        return self.catalog_result

    async def call_tool(
        self,
        access_token: str,
        name: str,
        arguments: JsonObject,
    ) -> MCPToolCallResult:
        """Record a call and consume its injected typed result."""
        self.calls.append((access_token, name, arguments))
        if not self.call_results:
            raise AssertionError("Unexpected MCP tool call")
        return self.call_results.pop(0)


type TokenRead = str | ControlPlaneError


class FakeTokenProvider:
    """Return rotated tokens or injected safe token-file failures."""

    def __init__(self, reads: list[TokenRead]) -> None:
        """Retain ordered credential read outcomes."""
        self.reads = list(reads)
        self.read_count = 0

    def read_access_token(self) -> SecretStr:
        """Consume one credential outcome."""
        self.read_count += 1
        if not self.reads:
            raise AssertionError("Unexpected token read")
        result = self.reads.pop(0)
        if isinstance(result, ControlPlaneError):
            raise result
        return SecretStr(result)


class SequenceClock:
    """Return injected wall-clock values in call order."""

    def __init__(self, values: list[datetime]) -> None:
        """Retain ordered clock readings."""
        self.values = list(values)

    def __call__(self) -> datetime:
        """Consume one clock reading."""
        if not self.values:
            raise AssertionError("Unexpected clock read")
        return self.values.pop(0)


def _context(deadline: datetime = _FUTURE) -> ReviewConsultContext:
    """Build one complete review context."""
    return ReviewConsultContext(
        review_public_id="review-1",
        selected_delegate_public_id="delegate-1",
        wallet_public_id="wallet-1",
        dispatch_version=3,
        deadline=deadline,
        signal_envelope={"action": "buy", "confidence": 0.82},
        instrument_metadata={"symbol": "BTC/USD"},
        user_public_id="user-1",
        strategy_public_id="strategy-1",
        instrument_public_id="instrument-1",
        fanout_after=_NOW + timedelta(seconds=30),
    )


def _catalog(tools: list[ChatTool] | None = None) -> MCPToolCatalogSuccess:
    """Build a successful catalog containing the decision tool by default."""
    return MCPToolCatalogSuccess(
        tools=(
            tools
            if tools is not None
            else [
                ChatTool(
                    type="function",
                    function=ChatFunctionDefinition(
                        name="submit_ai_review_decision",
                        description="Submit a review decision",
                        parameters={"type": "object"},
                    ),
                )
            ]
        )
    )


def _tool_call(
    name: str = "submit_ai_review_decision",
    encoded_arguments: str = (
        '{"review_id":"review-1","decision":"approve","rationale":"Looks safe"}'
    ),
) -> ChatToolCall:
    """Build one assistant-requested function call."""
    return ChatToolCall(
        id=f"call-{name}",
        type="function",
        function=ChatFunctionCall(name=name, arguments=encoded_arguments),
    )


def _assistant(
    tool_calls: list[ChatToolCall] | None = None,
    content: str | None = None,
) -> AssistantChatMessage:
    """Build one typed assistant message."""
    return AssistantChatMessage(
        role=ChatRole.ASSISTANT,
        content=content,
        tool_calls=tool_calls,
    )


def _opaque_assistant(tool_call: ChatToolCall) -> AssistantChatMessage:
    """Parse an assistant message with opaque provider extension state."""
    return AssistantChatMessage.model_validate(
        {
            "role": "assistant",
            "content": None,
            "tool_calls": [tool_call.model_dump(mode="json")],
            "reasoning_state": {"step": 7},
            "provider_marker": "opaque",
        }
    )


def _completion(message: AssistantChatMessage) -> ChatCompletionSuccess:
    """Wrap one assistant message in a successful completion result."""
    return ChatCompletionSuccess(
        completion=ChatCompletionResponse(
            choices=[
                ChatCompletionChoice(
                    index=0,
                    message=message,
                    finish_reason="tool_calls" if message.tool_calls else "stop",
                )
            ],
            usage=ChatCompletionUsage(
                prompt_tokens=10,
                completion_tokens=3,
                total_tokens=13,
            ),
        )
    )


def _call_success(
    error_code: str | None = None,
    content: str = _SUCCESS_CONTENT,
) -> MCPToolCallSuccess:
    """Build one successful typed MCP call outcome."""
    return MCPToolCallSuccess(
        content=content,
        error_code=error_code,
        message="Decision recorded",
        details=None,
    )


def _call_failure() -> MCPToolCallFailure:
    """Build one remote MCP failure for model replay."""
    return MCPToolCallFailure(
        error_kind=MCPErrorKind.REMOTE,
        content=_REMOTE_FAILURE_CONTENT,
        error_code="lookup_failed",
        message="Lookup failed",
        details=None,
    )


def _runner(
    chat_client: FakeChatClient,
    bridge: FakeMCPBridge,
    token_provider: FakeTokenProvider,
    max_tool_rounds: int = 3,
    clock: Callable[[], datetime] | None = lambda: _NOW,
) -> BoundedConsultRunner:
    """Build one consult runner around hermetic seams."""
    return BoundedConsultRunner(
        ConsultConfiguration(
            model_alias="review-model",
            max_tool_rounds=max_tool_rounds,
        ),
        chat_client,
        bridge,
        token_provider,
        clock=clock,
    )


@pytest.mark.parametrize("error_code", [None, "decision_already_recorded"])
@pytest.mark.asyncio
async def test_successful_submit_terminates_immediately(error_code: str | None) -> None:
    """Recorded and idempotently recorded decisions are both terminal success.

    Given: A valid decision call and a success envelope with either supported code,
    When: The bounded consult executes the assistant's tool batch,
    Then: It submits immediately and does not execute later calls in that batch.
    """
    submit = _tool_call()
    later = _tool_call("lookup_context", '{"topic":"risk"}')
    chat = FakeChatClient([_completion(_assistant([submit, later]))])
    bridge = FakeMCPBridge(_catalog(), [_call_success(error_code)])
    tokens = FakeTokenProvider(["catalog-token", "tool-token"])
    runner = _runner(chat, bridge, tokens)

    result = await runner.run(_context())

    assert result.outcome is ConsultOutcome.SUBMITTED
    assert bridge.list_tokens == ["catalog-token"]
    assert bridge.calls == [
        (
            "tool-token",
            "submit_ai_review_decision",
            {
                "review_id": "review-1",
                "decision": "approve",
                "rationale": "Looks safe",
            },
        )
    ]
    assert len(chat.requests) == 1


@pytest.mark.asyncio
async def test_tool_replay_preserves_opaque_assistant_extensions() -> None:
    """Opaque assistant state and MCP content survive the next model request.

    Given: A provider-extended assistant message calling a non-terminal tool,
    When: The tool result is replayed into the next bounded round,
    Then: The next request preserves assistant extras and the correlated tool message.
    """
    lookup = _tool_call("lookup_context", '{"topic":"risk"}')
    submit = _tool_call()
    opaque = _opaque_assistant(lookup)
    chat = FakeChatClient(
        [
            _completion(opaque),
            _completion(_assistant([submit])),
        ]
    )
    bridge = FakeMCPBridge(_catalog(), [_call_failure(), _call_success()])
    tokens = FakeTokenProvider(["catalog-token", "lookup-token", "submit-token"])
    runner = _runner(chat, bridge, tokens)

    result = await runner.run(_context())

    assert result.outcome is ConsultOutcome.SUBMITTED
    assert len(chat.requests) == 2
    replay_messages = chat.requests[1].messages
    assert [message.role for message in replay_messages] == [
        ChatRole.SYSTEM,
        ChatRole.USER,
        ChatRole.ASSISTANT,
        ChatRole.TOOL,
    ]
    replayed_assistant = replay_messages[2]
    assert replayed_assistant.__pydantic_extra__ == {
        "reasoning_state": {"step": 7},
        "provider_marker": "opaque",
    }
    replayed_tool = replay_messages[3]
    assert replayed_tool.tool_call_id == lookup.id
    assert replayed_tool.content == _REMOTE_FAILURE_CONTENT
    assert bridge.calls == [
        ("lookup-token", "lookup_context", {"topic": "risk"}),
        (
            "submit-token",
            "submit_ai_review_decision",
            {
                "review_id": "review-1",
                "decision": "approve",
                "rationale": "Looks safe",
            },
        ),
    ]


@pytest.mark.asyncio
async def test_rejected_submit_is_replayed_without_terminal_success() -> None:
    """A valid remote submit rejection does not resolve the active review.

    Given: A safe decision call whose MCP result is a typed remote failure,
    When: The consult replays the server response into another model round,
    Then: It remains unresolved and skips after the following chat failure.
    """
    submit = _tool_call()
    chat = FakeChatClient(
        [
            _completion(_assistant([submit])),
            ChatCompletionFailure(
                error_kind=ChatCompletionErrorKind.TRANSPORT,
                status_code=None,
            ),
        ]
    )
    bridge = FakeMCPBridge(_catalog(), [_call_failure()])
    runner = _runner(
        chat,
        bridge,
        FakeTokenProvider(["catalog-token", "tool-token"]),
    )

    result = await runner.run(_context())

    assert result.outcome is ConsultOutcome.SKIPPED
    assert len(bridge.calls) == 1
    replay = chat.requests[1].messages[-1]
    assert replay.tool_call_id == submit.id
    assert replay.content == _REMOTE_FAILURE_CONTENT


@pytest.mark.asyncio
async def test_initial_messages_contain_required_prompt_and_exact_context() -> None:
    """The first model turn receives the required role and review context.

    Given: A complete consult context and a model failure after request capture,
    When: The consult issues its first completion request,
    Then: It sends system and user messages, the configured model, and cached tools.
    """
    chat = FakeChatClient(
        [
            ChatCompletionFailure(
                error_kind=ChatCompletionErrorKind.TRANSPORT,
                status_code=None,
            )
        ]
    )
    bridge = FakeMCPBridge(_catalog())
    runner = _runner(chat, bridge, FakeTokenProvider(["catalog-token"]))
    context = _context()

    result = await runner.run(context)

    assert result.outcome is ConsultOutcome.SKIPPED
    request = chat.requests[0]
    assert request.model == "review-model"
    assert request.tools == _catalog().tools
    assert [message.role for message in request.messages] == [ChatRole.SYSTEM, ChatRole.USER]
    assert request.messages[0].content is not None
    assert "MUST end by calling submit_ai_review_decision" in request.messages[0].content
    assert request.messages[1].content is not None
    assert json.loads(request.messages[1].content) == json.loads(context.model_dump_json())
    assert "tool_choice" not in request.model_dump(exclude_unset=True)


@pytest.mark.asyncio
async def test_successful_catalog_is_cached_between_consults() -> None:
    """A successful MCP catalog is reused without another credential read.

    Given: One runner processing two reviews after loading a non-empty catalog,
    When: Both model calls return recoverable chat failures,
    Then: The bridge catalog and its access token are used only once.
    """
    failure = ChatCompletionFailure(
        error_kind=ChatCompletionErrorKind.HTTP_STATUS,
        status_code=503,
    )
    chat = FakeChatClient([failure, failure])
    bridge = FakeMCPBridge(_catalog())
    tokens = FakeTokenProvider(["catalog-token"])
    runner = _runner(chat, bridge, tokens)

    first = await runner.run(_context())
    second = await runner.run(_context())

    assert first.outcome is ConsultOutcome.SKIPPED
    assert second.outcome is ConsultOutcome.SKIPPED
    assert bridge.list_tokens == ["catalog-token"]
    assert tokens.read_count == 1
    assert len(chat.requests) == 2


@pytest.mark.parametrize(
    "catalog_result",
    [
        MCPToolCatalogFailure(error_kind=MCPErrorKind.SESSION),
        _catalog([]),
    ],
)
@pytest.mark.asyncio
async def test_unavailable_or_empty_catalog_skips_consult(
    catalog_result: MCPToolCatalogResult,
) -> None:
    """Catalog failures and empty catalogs never reach the model.

    Given: A typed MCP catalog failure or a successful empty catalog,
    When: A consult tries to load tools,
    Then: It skips without issuing a chat completion.
    """
    chat = FakeChatClient([])
    bridge = FakeMCPBridge(catalog_result)
    runner = _runner(chat, bridge, FakeTokenProvider(["catalog-token"]), clock=None)

    result = await runner.run(_context(datetime(2100, 1, 1, tzinfo=UTC)))

    assert result.outcome is ConsultOutcome.SKIPPED
    assert chat.requests == []
    assert bridge.list_tokens == ["catalog-token"]


@pytest.mark.asyncio
async def test_catalog_token_read_failure_skips_before_bridge() -> None:
    """An unreadable rotated token prevents catalog and model calls.

    Given: A token provider reporting a typed token-file failure,
    When: A consult first loads the MCP catalog,
    Then: It skips without sending the bridge any credential.
    """
    chat = FakeChatClient([])
    bridge = FakeMCPBridge(_catalog())
    token_error = ControlPlaneError(ControlPlaneErrorKind.TOKEN_FILE)
    runner = _runner(chat, bridge, FakeTokenProvider([token_error]))

    result = await runner.run(_context())

    assert result.outcome is ConsultOutcome.SKIPPED
    assert bridge.list_tokens == []
    assert bridge.calls == []
    assert chat.requests == []


@pytest.mark.parametrize(
    "completion_result, expected",
    [
        (ChatCompletionQuotaExhausted(status_code=429), ConsultOutcome.QUOTA_DEGRADED),
        (
            ChatCompletionFailure(
                error_kind=ChatCompletionErrorKind.MALFORMED_RESPONSE,
                status_code=200,
            ),
            ConsultOutcome.SKIPPED,
        ),
    ],
)
@pytest.mark.asyncio
async def test_typed_chat_failures_are_terminal_without_tool_calls(
    completion_result: ChatCompletionResult,
    expected: ConsultOutcome,
) -> None:
    """Quota and ordinary chat failures map to distinct terminal outcomes.

    Given: A typed quota exhaustion or non-quota chat failure,
    When: The first bounded model request completes,
    Then: The consult degrades or skips without invoking MCP tools.
    """
    chat = FakeChatClient([completion_result])
    bridge = FakeMCPBridge(_catalog())
    runner = _runner(chat, bridge, FakeTokenProvider(["catalog-token"]))

    result = await runner.run(_context())

    assert result.outcome is expected
    assert bridge.calls == []


@pytest.mark.parametrize(
    "encoded_arguments",
    [
        "not-json",
        '["review-1", "approve"]',
    ],
)
@pytest.mark.asyncio
async def test_invalid_or_non_object_arguments_are_replayed_locally(
    encoded_arguments: str,
) -> None:
    """Malformed and non-object arguments do not cross the MCP boundary.

    Given: A tool call containing invalid JSON or a JSON array,
    When: The consult executes the assistant request,
    Then: It replays a correlated local error and never invokes the tool.
    """
    tool_call = _tool_call("lookup_context", encoded_arguments)
    chat = FakeChatClient(
        [
            _completion(_assistant([tool_call])),
            ChatCompletionFailure(
                error_kind=ChatCompletionErrorKind.TRANSPORT,
                status_code=None,
            ),
        ]
    )
    bridge = FakeMCPBridge(_catalog())
    runner = _runner(chat, bridge, FakeTokenProvider(["catalog-token"]))

    result = await runner.run(_context())

    assert result.outcome is ConsultOutcome.SKIPPED
    assert bridge.calls == []
    replay = chat.requests[1].messages[-1]
    assert replay.role is ChatRole.TOOL
    assert replay.tool_call_id == tool_call.id
    assert replay.content is not None
    assert json.loads(replay.content)["error_code"] == "invalid_tool_arguments"


@pytest.mark.parametrize(
    "arguments",
    [
        {"review_id": "review-other", "decision": "approve", "rationale": "Safe"},
        {"review_id": "review-1", "decision": "hold", "rationale": "Wait"},
        {"review_id": "review-1", "decision": "reject", "rationale": "x" * 4097},
        {"review_id": "review-1", "decision": "reject", "rationale": 17},
    ],
)
@pytest.mark.asyncio
async def test_unsafe_submit_arguments_never_cross_mcp(arguments: JsonObject) -> None:
    """Review binding, decision values, and rationale bounds fail closed.

    Given: Submit arguments violating one active-review invariant,
    When: The assistant calls the decision tool,
    Then: A local unsafe error is replayed without an MCP invocation.
    """
    encoded = json.dumps(arguments)
    submit = _tool_call(encoded_arguments=encoded)
    chat = FakeChatClient(
        [
            _completion(_assistant([submit])),
            ChatCompletionFailure(
                error_kind=ChatCompletionErrorKind.TRANSPORT,
                status_code=None,
            ),
        ]
    )
    bridge = FakeMCPBridge(_catalog())
    runner = _runner(chat, bridge, FakeTokenProvider(["catalog-token"]))

    result = await runner.run(_context())

    assert result.outcome is ConsultOutcome.SKIPPED
    assert bridge.calls == []
    replay = chat.requests[1].messages[-1]
    assert replay.tool_call_id == submit.id
    assert replay.content is not None
    assert json.loads(replay.content)["error_code"] == "unsafe_review_decision"


@pytest.mark.parametrize(
    "rationale",
    [None, "x" * 4096],
)
@pytest.mark.asyncio
async def test_safe_rationale_boundaries_can_submit(rationale: str | None) -> None:
    """Null and maximum-length string rationales satisfy the public contract.

    Given: A bound review, supported decision, and boundary-safe rationale,
    When: The decision tool succeeds,
    Then: The exact arguments reach MCP and the consult submits.
    """
    arguments: JsonObject = {
        "review_id": "review-1",
        "decision": "reject",
        "rationale": rationale,
    }
    submit = _tool_call(encoded_arguments=json.dumps(arguments))
    chat = FakeChatClient([_completion(_assistant([submit]))])
    bridge = FakeMCPBridge(_catalog(), [_call_success()])
    runner = _runner(
        chat,
        bridge,
        FakeTokenProvider(["catalog-token", "tool-token"]),
    )

    result = await runner.run(_context())

    assert result.outcome is ConsultOutcome.SUBMITTED
    assert bridge.calls == [("tool-token", "submit_ai_review_decision", arguments)]


@pytest.mark.parametrize(
    "deadline, now",
    [
        (_NOW, _NOW),
        (_FUTURE.replace(tzinfo=None), _NOW),
        (_FUTURE, _NOW.replace(tzinfo=None)),
    ],
)
@pytest.mark.asyncio
async def test_expired_or_naive_time_skips_before_egress(
    deadline: datetime,
    now: datetime,
) -> None:
    """Expired reviews and naive clocks fail closed before credential use.

    Given: An elapsed deadline, naive deadline, or naive current clock,
    When: A consult starts,
    Then: It skips without reading a token or reaching any network seam.
    """
    chat = FakeChatClient([])
    bridge = FakeMCPBridge(_catalog())
    tokens = FakeTokenProvider([])
    runner = _runner(chat, bridge, tokens, clock=lambda: now)

    result = await runner.run(_context(deadline))

    assert result.outcome is ConsultOutcome.SKIPPED
    assert tokens.read_count == 0
    assert bridge.list_tokens == []
    assert chat.requests == []


@pytest.mark.asyncio
async def test_deadline_expiring_before_tool_call_skips_without_mcp() -> None:
    """A review expiring during model inference cannot submit afterward.

    Given: A future review at start and an elapsed deadline at tool execution,
    When: The assistant returns a valid decision call,
    Then: The consult skips without a second token read or MCP invocation.
    """
    submit = _tool_call()
    chat = FakeChatClient([_completion(_assistant([submit]))])
    bridge = FakeMCPBridge(_catalog())
    tokens = FakeTokenProvider(["catalog-token"])
    clock = SequenceClock([_NOW, _FUTURE])
    runner = _runner(chat, bridge, tokens, clock=clock)

    result = await runner.run(_context())

    assert result.outcome is ConsultOutcome.SKIPPED
    assert tokens.read_count == 1
    assert bridge.calls == []


@pytest.mark.asyncio
async def test_tool_token_read_failure_replays_unsafe_result() -> None:
    """Credential rotation failure at tool time is local and recoverable.

    Given: A catalog token followed by an unreadable replacement token,
    When: The assistant requests a valid decision submission,
    Then: The call is withheld and its local unsafe result reaches the next turn.
    """
    submit = _tool_call()
    chat = FakeChatClient(
        [
            _completion(_assistant([submit])),
            ChatCompletionFailure(
                error_kind=ChatCompletionErrorKind.TRANSPORT,
                status_code=None,
            ),
        ]
    )
    bridge = FakeMCPBridge(_catalog())
    tokens = FakeTokenProvider(
        ["catalog-token", ControlPlaneError(ControlPlaneErrorKind.TOKEN_FILE)]
    )
    runner = _runner(chat, bridge, tokens)

    result = await runner.run(_context())

    assert result.outcome is ConsultOutcome.SKIPPED
    assert tokens.read_count == 2
    assert bridge.calls == []
    replay = chat.requests[1].messages[-1]
    assert replay.content is not None
    assert json.loads(replay.content)["error_code"] == "unsafe_review_decision"


@pytest.mark.asyncio
async def test_text_only_reply_gets_exactly_one_reminder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A text-only assistant receives one reminder and no unbounded retries.

    Given: Two consecutive assistant text replies and capacity for three rounds,
    When: The model never calls a tool,
    Then: The runner appends one reminder and skips after the second reply.
    """
    chat = FakeChatClient(
        [
            _completion(_assistant(content="I approve this setup.")),
            _completion(_assistant(content="Still approved.")),
        ]
    )
    bridge = FakeMCPBridge(_catalog())
    runner = _runner(chat, bridge, FakeTokenProvider(["catalog-token"]), max_tool_rounds=3)

    warning = MagicMock()
    monkeypatch.setattr("snapper_delegate.consult.logger.warning", warning)
    result = await runner.run(_context())

    assert result.outcome is ConsultOutcome.SKIPPED
    assert len(chat.requests) == 2
    reminder = chat.requests[1].messages[-1]
    assert reminder.role is ChatRole.USER
    assert reminder.content is not None
    assert "must now call submit_ai_review_decision" in reminder.content
    warning.assert_called_once_with(
        "Delegate consult ended without decision review_id={}",
        "review-1",
    )


@pytest.mark.asyncio
async def test_single_tool_round_still_allows_one_text_reminder() -> None:
    """The one-time reminder remains independent of the tool-round bound.

    Given: A one-tool-round configuration and two text-only assistant replies,
    When: The model never calls the decision tool,
    Then: The runner sends exactly one reminder before skipping.
    """
    chat = FakeChatClient(
        [
            _completion(_assistant(content="approve")),
            _completion(_assistant(content="still approve")),
        ]
    )
    bridge = FakeMCPBridge(_catalog())
    runner = _runner(chat, bridge, FakeTokenProvider(["catalog-token"]), max_tool_rounds=1)

    result = await runner.run(_context())

    assert result.outcome is ConsultOutcome.SKIPPED
    assert len(chat.requests) == 2


@pytest.mark.asyncio
async def test_nonterminal_tool_at_round_bound_skips_after_execution() -> None:
    """A non-terminal final tool call cannot extend the bounded loop.

    Given: A one-round configuration and a successful non-submit tool call,
    When: The final allowed model turn requests the tool,
    Then: The tool executes once and the consult exits through the round bound.
    """
    lookup = _tool_call("lookup_context", '{"topic":"risk"}')
    chat = FakeChatClient([_completion(_assistant([lookup]))])
    bridge = FakeMCPBridge(_catalog(), [_call_success(content=_SUCCESS_CONTENT)])
    runner = _runner(
        chat,
        bridge,
        FakeTokenProvider(["catalog-token", "tool-token"]),
        max_tool_rounds=1,
    )

    result = await runner.run(_context())

    assert result.outcome is ConsultOutcome.SKIPPED
    assert bridge.calls == [("tool-token", "lookup_context", {"topic": "risk"})]
