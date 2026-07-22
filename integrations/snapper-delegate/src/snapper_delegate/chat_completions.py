"""Typed asynchronous client for the standard chat-completions wire protocol."""

from dataclasses import dataclass
from dataclasses import field
from enum import StrEnum
from pathlib import Path
from types import TracebackType
from typing import Literal
from typing import Self

import httpx
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import SecretStr
from pydantic import ValidationError

from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue

_REQUEST_MODEL_CONFIG = ConfigDict(
    extra="forbid",
    frozen=True,
    strict=True,
    validate_default=True,
)

_EXTENSIBLE_MODEL_CONFIG = ConfigDict(
    extra="allow",
    frozen=True,
    strict=True,
    validate_default=True,
)


class ApiKeyFileError(ValueError):
    """Report an unreadable or empty API key file without exposing its contents."""


class ChatRole(StrEnum):
    """Roles supported by the standard chat-completions message shape."""

    SYSTEM = "system"
    USER = "user"
    ASSISTANT = "assistant"
    TOOL = "tool"


class ChatCompletionOutcome(StrEnum):
    """Terminal outcomes from one chat-completions request."""

    SUCCESS = "success"
    QUOTA_EXHAUSTED = "quota_exhausted"
    ERROR = "error"


class ChatCompletionErrorKind(StrEnum):
    """Failure categories that remain distinct from quota exhaustion."""

    HTTP_STATUS = "http_status"
    TRANSPORT = "transport"
    MALFORMED_RESPONSE = "malformed_response"


class _RequestModel(BaseModel):
    """Apply strict validation to locally constructed request models."""

    model_config = _REQUEST_MODEL_CONFIG


class _ExtensibleWireModel(BaseModel):
    """Preserve JSON extension fields received from compatible endpoints."""

    model_config = _EXTENSIBLE_MODEL_CONFIG

    __pydantic_extra__: dict[str, JsonValue] = Field(init=False)


class ChatFunctionDefinition(_RequestModel):
    """Describe a callable function exposed to the model."""

    name: str = Field(min_length=1)
    description: str | None = None
    parameters: JsonObject


class ChatTool(_RequestModel):
    """Wrap one function definition in the standard tool shape."""

    type: Literal["function"]
    function: ChatFunctionDefinition


class ChatFunctionCall(_ExtensibleWireModel):
    """Represent the function name and encoded arguments returned by the model."""

    name: str = Field(min_length=1)
    arguments: str


class ChatToolCall(_ExtensibleWireModel):
    """Represent one assistant-requested function invocation."""

    id: str = Field(min_length=1)
    type: Literal["function"]
    function: ChatFunctionCall


class ChatMessage(_ExtensibleWireModel):
    """Represent one request message while preserving opaque replay state."""

    role: ChatRole
    content: str | None
    tool_calls: list[ChatToolCall] | None = None
    tool_call_id: str | None = None


class AssistantChatMessage(ChatMessage):
    """Represent the assistant message selected from a completion response."""

    role: Literal[ChatRole.ASSISTANT]


class ChatCompletionRequest(_RequestModel):
    """Represent one non-streaming chat-completions request."""

    model: str = Field(min_length=1)
    messages: list[ChatMessage] = Field(min_length=1)
    temperature: float | None = None
    max_tokens: int | None = Field(default=None, gt=0)
    tools: list[ChatTool] | None = Field(default=None, min_length=1)


class ChatCompletionUsage(_ExtensibleWireModel):
    """Report token usage returned for a completed request."""

    prompt_tokens: int = Field(ge=0)
    completion_tokens: int = Field(ge=0)
    total_tokens: int = Field(ge=0)


class ChatCompletionChoice(_ExtensibleWireModel):
    """Represent one assistant choice returned by the endpoint."""

    index: int = Field(ge=0)
    message: AssistantChatMessage
    finish_reason: str | None


class ChatCompletionResponse(_ExtensibleWireModel):
    """Represent the typed response fields needed by the delegate."""

    choices: list[ChatCompletionChoice] = Field(min_length=1)
    usage: ChatCompletionUsage


@dataclass(frozen=True, slots=True)
class ChatCompletionSuccess:
    """Carry a successfully parsed completion response."""

    completion: ChatCompletionResponse
    outcome: Literal[ChatCompletionOutcome.SUCCESS] = field(
        default=ChatCompletionOutcome.SUCCESS,
        init=False,
    )


@dataclass(frozen=True, slots=True)
class ChatCompletionQuotaExhausted:
    """Signal a recoverable quota state that must not terminate the future runner."""

    status_code: Literal[402, 429]
    outcome: Literal[ChatCompletionOutcome.QUOTA_EXHAUSTED] = field(
        default=ChatCompletionOutcome.QUOTA_EXHAUSTED,
        init=False,
    )


@dataclass(frozen=True, slots=True)
class ChatCompletionFailure:
    """Carry a non-quota request failure without retaining sensitive payloads."""

    error_kind: ChatCompletionErrorKind
    status_code: int | None
    outcome: Literal[ChatCompletionOutcome.ERROR] = field(
        default=ChatCompletionOutcome.ERROR,
        init=False,
    )


type ChatCompletionResult = (
    ChatCompletionSuccess | ChatCompletionQuotaExhausted | ChatCompletionFailure
)


def load_api_key_file(api_key_file: str | Path) -> SecretStr:
    """Read and strip an API key without exposing it in representations.

    Args:
        api_key_file: File containing the API key.

    Returns:
        Secret-wrapped non-empty API key.

    Raises:
        ApiKeyFileError: If the file cannot be read or contains only whitespace.
    """
    try:
        value = Path(api_key_file).read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError) as error:
        raise ApiKeyFileError("Unable to read the API key file") from error
    if not value:
        raise ApiKeyFileError("The API key file is empty")
    return SecretStr(value)


class ChatCompletionsClient:
    """Send typed requests to a standard chat-completions endpoint."""

    def __init__(
        self,
        base_url: str,
        api_key_file: str | Path,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout_seconds: float = 30.0,
    ) -> None:
        """Initialize an inert client using a key loaded from a file.

        Args:
            base_url: Endpoint base URL to which the standard path is appended.
            api_key_file: File containing the bearer credential.
            transport: Optional injected transport for hermetic callers and tests.
            timeout_seconds: Per-request timeout in seconds.
        """
        api_key = load_api_key_file(api_key_file)
        self._endpoint_url = f"{base_url.rstrip('/')}/v1/chat/completions"
        self._http_client = httpx.AsyncClient(
            headers={
                "Authorization": f"Bearer {api_key.get_secret_value()}",
                "Content-Type": "application/json",
            },
            timeout=timeout_seconds,
            transport=transport,
        )

    def __repr__(self) -> str:
        """Return a representation that omits endpoint and credential details."""
        return "ChatCompletionsClient()"

    async def __aenter__(self) -> Self:
        """Enter the client lifetime context."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the owned HTTP client when leaving the lifetime context."""
        await self.aclose()

    async def aclose(self) -> None:
        """Close the owned asynchronous HTTP client."""
        await self._http_client.aclose()

    async def complete(self, request: ChatCompletionRequest) -> ChatCompletionResult:
        """Send one request and return a typed terminal outcome.

        Args:
            request: Validated chat-completions request.

        Returns:
            Success, recoverable quota exhaustion, or a typed failure.
        """
        try:
            response = await self._http_client.post(
                self._endpoint_url,
                content=request.model_dump_json(exclude_unset=True),
            )
        except httpx.RequestError:
            return ChatCompletionFailure(
                error_kind=ChatCompletionErrorKind.TRANSPORT,
                status_code=None,
            )
        if response.status_code == 402:
            return ChatCompletionQuotaExhausted(status_code=402)
        if response.status_code == 429:
            return ChatCompletionQuotaExhausted(status_code=429)
        if not response.is_success:
            return ChatCompletionFailure(
                error_kind=ChatCompletionErrorKind.HTTP_STATUS,
                status_code=response.status_code,
            )
        try:
            completion = ChatCompletionResponse.model_validate_json(response.content)
        except ValidationError:
            return ChatCompletionFailure(
                error_kind=ChatCompletionErrorKind.MALFORMED_RESPONSE,
                status_code=response.status_code,
            )
        return ChatCompletionSuccess(completion=completion)
