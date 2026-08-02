"""Bounded chat and MCP tool orchestration for one AI review consult."""

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from enum import StrEnum
from typing import Protocol
from typing import cast

from loguru import logger
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import SecretStr

from snapper_delegate.chat_completions import ChatCompletionFailure
from snapper_delegate.chat_completions import ChatCompletionQuotaExhausted
from snapper_delegate.chat_completions import ChatCompletionRequest
from snapper_delegate.chat_completions import ChatCompletionResult
from snapper_delegate.chat_completions import ChatMessage
from snapper_delegate.chat_completions import ChatRole
from snapper_delegate.chat_completions import ChatTool
from snapper_delegate.chat_completions import ChatToolCall
from snapper_delegate.control_plane import ControlPlaneError
from snapper_delegate.json_types import JsonObject
from snapper_delegate.mcp_bridge import MCPBridgeClient
from snapper_delegate.mcp_bridge import MCPToolCatalogSuccess

_SYSTEM_PROMPT = (
    "You are a trading review delegate. You MUST end by calling "
    "submit_ai_review_decision with approve or reject and a short rationale. "
    "Never fabricate data. The consult context follows."
)
_REMINDER_PROMPT = (
    "You must now call submit_ai_review_decision for this review with approve or reject "
    "and a short rationale."
)
_INVALID_ARGUMENTS_CONTENT = (
    '{"success":false,"error_code":"invalid_tool_arguments",'
    '"message":"Tool arguments must be a JSON object","details":null}'
)
_UNSAFE_SUBMISSION_CONTENT = (
    '{"success":false,"error_code":"unsafe_review_decision",'
    '"message":"Decision arguments do not match the active review","details":null}'
)
_SUBMIT_TOOL_NAME = "submit_ai_review_decision"
_CONTEXT_MODEL_CONFIG = ConfigDict(
    extra="forbid",
    frozen=True,
    strict=True,
    validate_default=True,
)


class AccessTokenProvider(Protocol):
    """Read the current delegate access token for one MCP use."""

    def read_access_token(self) -> SecretStr:
        """Return the freshly loaded bearer credential.

        Returns:
            The current access token with secret-safe representation.
        """
        ...


class ChatCompletionClient(Protocol):
    """Structural chat-completions surface used by consult orchestration."""

    async def complete(self, request: ChatCompletionRequest) -> ChatCompletionResult:
        """Return one typed completion outcome.

        Args:
            request: Validated model, message, and tool request.

        Returns:
            The successful, quota-exhausted, or failed completion outcome.
        """
        ...


class ReviewConsultContext(BaseModel):
    """Represent the exact context supplied to one review consultation."""

    model_config = _CONTEXT_MODEL_CONFIG

    review_public_id: str = Field(min_length=1)
    selected_delegate_public_id: str = Field(min_length=1)
    wallet_public_id: str = Field(min_length=1)
    dispatch_version: int = Field(ge=0)
    deadline: datetime
    signal_envelope: JsonObject
    instrument_metadata: JsonObject
    user_public_id: str | None = None
    strategy_public_id: str | None = None
    instrument_public_id: str | None = None
    fanout_after: datetime | None = None


@dataclass(frozen=True, slots=True)
class ConsultConfiguration:
    """Configure one model route and its bounded tool loop."""

    model_alias: str
    max_tool_rounds: int


class ConsultOutcome(StrEnum):
    """Terminal outcomes understood by the delegate lifecycle."""

    SUBMITTED = "submitted"
    QUOTA_DEGRADED = "quota_degraded"
    SKIPPED = "skipped"


@dataclass(frozen=True, slots=True)
class ConsultResult:
    """Carry the terminal outcome of one bounded consultation."""

    outcome: ConsultOutcome


@dataclass(frozen=True, slots=True)
class _ToolExecution:
    """Carry one replay message and whether it resolved the review."""

    replay: ChatMessage
    submitted: bool = False
    expired: bool = False


class BoundedConsultRunner:
    """Drive one chat-completions conversation through Snapper MCP tools."""

    def __init__(
        self,
        configuration: ConsultConfiguration,
        chat_client: ChatCompletionClient,
        mcp_bridge: MCPBridgeClient,
        token_provider: AccessTokenProvider,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        """Initialize bounded orchestration without making a network request."""
        self._configuration = configuration
        self._chat_client = chat_client
        self._mcp_bridge = mcp_bridge
        self._token_provider = token_provider
        self._clock = clock or _utc_now
        self._tools: list[ChatTool] | None = None

    async def run(self, context: ReviewConsultContext) -> ConsultResult:
        """Run one consult until submission, quota degradation, or bounded skip.

        Args:
            context: Immutable review facts and deadline for this consultation.

        Returns:
            The terminal consultation outcome.
        """
        if _is_expired(context.deadline, self._clock()):
            return ConsultResult(ConsultOutcome.SKIPPED)
        tools = await self._load_tools()
        if not tools:
            return ConsultResult(ConsultOutcome.SKIPPED)
        messages = _initial_messages(context)
        reminder_used = False
        tool_rounds = 0
        while tool_rounds < self._configuration.max_tool_rounds:
            completion = await self._complete(messages, tools)
            if isinstance(completion, ChatCompletionQuotaExhausted):
                return ConsultResult(ConsultOutcome.QUOTA_DEGRADED)
            if isinstance(completion, ChatCompletionFailure):
                return ConsultResult(ConsultOutcome.SKIPPED)
            assistant = completion.completion.choices[0].message
            messages.append(assistant)
            if assistant.tool_calls:
                tool_rounds += 1
                terminal = await self._execute_tools(context, assistant.tool_calls, messages)
                if terminal is not None:
                    return terminal
                continue
            if reminder_used:
                logger.warning(
                    "Delegate consult ended without decision review_id={}",
                    context.review_public_id,
                )
                return ConsultResult(ConsultOutcome.SKIPPED)
            messages.append(ChatMessage(role=ChatRole.USER, content=_REMINDER_PROMPT))
            reminder_used = True
        return ConsultResult(ConsultOutcome.SKIPPED)

    async def _load_tools(self) -> list[ChatTool] | None:
        """Load and cache the sanitized MCP catalog after the first success."""
        if self._tools is not None:
            return self._tools
        try:
            token = self._token_provider.read_access_token().get_secret_value()
        except ControlPlaneError:
            return None
        result = await self._mcp_bridge.list_tools(token)
        if not isinstance(result, MCPToolCatalogSuccess):
            return None
        self._tools = result.tools
        return self._tools

    async def _complete(
        self,
        messages: list[ChatMessage],
        tools: list[ChatTool],
    ) -> ChatCompletionResult:
        """Request one non-streaming model turn without forcing tool choice."""
        request = ChatCompletionRequest(
            model=self._configuration.model_alias,
            messages=messages,
            tools=tools,
        )
        return await self._chat_client.complete(request)

    async def _execute_tools(
        self,
        context: ReviewConsultContext,
        tool_calls: list[ChatToolCall],
        messages: list[ChatMessage],
    ) -> ConsultResult | None:
        """Execute one assistant tool batch and append every replay result."""
        for tool_call in tool_calls:
            execution = await self._execute_tool(context, tool_call)
            messages.append(execution.replay)
            if execution.expired:
                return ConsultResult(ConsultOutcome.SKIPPED)
            if execution.submitted:
                return ConsultResult(ConsultOutcome.SUBMITTED)
        return None

    async def _execute_tool(
        self,
        context: ReviewConsultContext,
        tool_call: ChatToolCall,
    ) -> _ToolExecution:
        """Validate and execute one model-requested MCP operation."""
        if _is_expired(context.deadline, self._clock()):
            return _local_tool_execution(tool_call.id, _UNSAFE_SUBMISSION_CONTENT, expired=True)
        arguments = _parse_arguments(tool_call.function.arguments)
        if arguments is None:
            return _local_tool_execution(tool_call.id, _INVALID_ARGUMENTS_CONTENT)
        if tool_call.function.name == _SUBMIT_TOOL_NAME and not _safe_submission(
            arguments, context
        ):
            return _local_tool_execution(tool_call.id, _UNSAFE_SUBMISSION_CONTENT)
        try:
            token = self._token_provider.read_access_token().get_secret_value()
        except ControlPlaneError:
            return _local_tool_execution(tool_call.id, _UNSAFE_SUBMISSION_CONTENT)
        result = await self._mcp_bridge.call_tool(
            token,
            tool_call.function.name,
            arguments,
        )
        replay = ChatMessage(
            role=ChatRole.TOOL,
            content=result.content,
            tool_call_id=tool_call.id,
        )
        submitted = tool_call.function.name == _SUBMIT_TOOL_NAME and result.success
        return _ToolExecution(replay=replay, submitted=submitted)


def _utc_now() -> datetime:
    """Return the current timezone-aware UTC wall clock."""
    return datetime.now(UTC)


def _is_expired(deadline: datetime, now: datetime) -> bool:
    """Fail closed for naive clocks and return whether the review expired."""
    if deadline.utcoffset() is None or now.utcoffset() is None:
        return True
    return deadline <= now


def _initial_messages(context: ReviewConsultContext) -> list[ChatMessage]:
    """Build the required system instruction and serialized consult context."""
    return [
        ChatMessage(role=ChatRole.SYSTEM, content=_SYSTEM_PROMPT),
        ChatMessage(role=ChatRole.USER, content=context.model_dump_json()),
    ]


def _parse_arguments(encoded: str) -> JsonObject | None:
    """Decode model tool arguments only when they form a JSON object."""
    try:
        value: object = json.loads(encoded)
    except (json.JSONDecodeError, UnicodeError):
        return None
    if not isinstance(value, dict):
        return None
    return cast(JsonObject, value)


def _safe_submission(arguments: JsonObject, context: ReviewConsultContext) -> bool:
    """Bind a model decision to the active review and public tool contract."""
    if arguments.get("review_id") != context.review_public_id:
        return False
    if arguments.get("decision") not in ("approve", "reject"):
        return False
    rationale = arguments.get("rationale")
    return rationale is None or isinstance(rationale, str) and len(rationale) <= 4096


def _local_tool_execution(
    tool_call_id: str,
    content: str,
    *,
    expired: bool = False,
) -> _ToolExecution:
    """Build a local failure result without invoking the MCP bridge."""
    return _ToolExecution(
        replay=ChatMessage(
            role=ChatRole.TOOL,
            content=content,
            tool_call_id=tool_call_id,
        ),
        expired=expired,
    )
