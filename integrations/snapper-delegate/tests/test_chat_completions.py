"""Tests for the typed asynchronous chat-completions client."""

import logging
from pathlib import Path

import httpx
import pytest

from snapper_delegate.chat_completions import ApiKeyFileError
from snapper_delegate.chat_completions import ChatCompletionErrorKind
from snapper_delegate.chat_completions import ChatCompletionFailure
from snapper_delegate.chat_completions import ChatCompletionOutcome
from snapper_delegate.chat_completions import ChatCompletionQuotaExhausted
from snapper_delegate.chat_completions import ChatCompletionRequest
from snapper_delegate.chat_completions import ChatCompletionsClient
from snapper_delegate.chat_completions import ChatCompletionSuccess
from snapper_delegate.chat_completions import ChatFunctionDefinition
from snapper_delegate.chat_completions import ChatMessage
from snapper_delegate.chat_completions import ChatRole
from snapper_delegate.chat_completions import ChatTool
from snapper_delegate.chat_completions import load_api_key_file

_API_KEY = "offline-test-secret"


def _write_api_key(tmp_path: Path, value: str = _API_KEY) -> Path:
    key_file = tmp_path / "model-api-key"
    key_file.write_text(value, encoding="utf-8")
    return key_file


def _request() -> ChatCompletionRequest:
    return ChatCompletionRequest(
        model="research-primary",
        messages=[ChatMessage(role=ChatRole.USER, content="Review this setup")],
        temperature=0.2,
        max_tokens=128,
        tools=[
            ChatTool(
                type="function",
                function=ChatFunctionDefinition(
                    name="lookup_context",
                    parameters={"type": "object"},
                ),
            )
        ],
    )


@pytest.mark.asyncio
async def test_success_parses_completion_and_sends_expected_request(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A successful response remains typed and the credential remains secret.

    Given: A stripped key file and a mocked successful endpoint,
    When: The client sends a request with messages and a tool definition,
    Then: It sends the standard shape and returns typed content, usage, and opaque state.
    """
    key_file = _write_api_key(tmp_path, f"  {_API_KEY}\n")
    captured_requests: list[httpx.Request] = []
    caplog.set_level(logging.DEBUG)

    def _handler(request: httpx.Request) -> httpx.Response:
        captured_requests.append(request)
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": None,
                            "tool_calls": [
                                {
                                    "id": "call-1",
                                    "type": "function",
                                    "function": {
                                        "name": "lookup_context",
                                        "arguments": '{"topic":"risk"}',
                                    },
                                }
                            ],
                            "reasoning_state": {"sequence": 1},
                            "opaque_marker": None,
                        },
                        "finish_reason": "tool_calls",
                    }
                ],
                "usage": {
                    "prompt_tokens": 12,
                    "completion_tokens": 3,
                    "total_tokens": 15,
                },
            },
        )

    loaded_key = load_api_key_file(key_file)
    assert loaded_key.get_secret_value() == _API_KEY
    assert _API_KEY not in repr(loaded_key)
    client = ChatCompletionsClient(
        "https://models.invalid/proxy/",
        key_file,
        transport=httpx.MockTransport(_handler),
    )
    assert repr(client) == "ChatCompletionsClient()"
    assert _API_KEY not in repr(client)
    async with client:
        result = await client.complete(_request())
        assert isinstance(result, ChatCompletionSuccess)
        replay_request = ChatCompletionRequest(
            model="research-primary",
            messages=[result.completion.choices[0].message],
        )
        replay_result = await client.complete(replay_request)

    assert isinstance(result, ChatCompletionSuccess)
    assert result.outcome is ChatCompletionOutcome.SUCCESS
    choice = result.completion.choices[0]
    assert choice.message.content is None
    assert choice.message.tool_calls is not None
    assert choice.message.tool_calls[0].function.arguments == '{"topic":"risk"}'
    assert choice.finish_reason == "tool_calls"
    assert choice.message.__pydantic_extra__ == {
        "reasoning_state": {"sequence": 1},
        "opaque_marker": None,
    }
    assert result.completion.usage.prompt_tokens == 12
    assert result.completion.usage.completion_tokens == 3
    assert result.completion.usage.total_tokens == 15
    assert isinstance(replay_result, ChatCompletionSuccess)
    assert len(captured_requests) == 2
    sent_request = captured_requests[0]
    assert str(sent_request.url) == "https://models.invalid/proxy/v1/chat/completions"
    assert sent_request.headers["Authorization"] == f"Bearer {_API_KEY}"
    assert sent_request.headers["Content-Type"] == "application/json"
    assert sent_request.content == (
        b'{"model":"research-primary","messages":[{"role":"user","content":'
        b'"Review this setup"}],"temperature":0.2,"max_tokens":128,"tools":'
        b'[{"type":"function","function":{"name":"lookup_context","parameters":'
        b'{"type":"object"}}}]}'
    )
    replayed_request = captured_requests[1]
    assert replayed_request.content == (
        b'{"model":"research-primary","messages":[{"role":"assistant","content":null,'
        b'"tool_calls":[{"id":"call-1","type":"function","function":{"name":'
        b'"lookup_context","arguments":"{\\"topic\\":\\"risk\\"}"}}],"reasoning_state":'
        b'{"sequence":1},"opaque_marker":null}]}'
    )
    assert _API_KEY not in caplog.text


@pytest.mark.parametrize("status_code", [402, 429])
@pytest.mark.asyncio
async def test_quota_statuses_return_recoverable_outcome(
    tmp_path: Path,
    status_code: int,
) -> None:
    """Quota responses are a recoverable result even with an invalid body.

    Given: A mocked endpoint returning either supported quota status and malformed data,
    When: The client completes the request,
    Then: It returns quota_exhausted without raising or parsing the response body.
    """
    key_file = _write_api_key(tmp_path)

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, content=b"not-json", request=request)

    client = ChatCompletionsClient(
        "https://models.invalid",
        key_file,
        transport=httpx.MockTransport(_handler),
    )
    async with client:
        result = await client.complete(_request())

    assert isinstance(result, ChatCompletionQuotaExhausted)
    assert result.outcome is ChatCompletionOutcome.QUOTA_EXHAUSTED
    assert result.status_code == status_code


@pytest.mark.asyncio
async def test_transport_error_returns_typed_failure(tmp_path: Path) -> None:
    """Transport failures stay inside the typed client boundary.

    Given: A mocked transport that cannot connect,
    When: The client sends a completion request,
    Then: It returns a transport failure without propagating the exception.
    """
    key_file = _write_api_key(tmp_path)

    def _handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("offline", request=request)

    client = ChatCompletionsClient(
        "https://models.invalid",
        key_file,
        transport=httpx.MockTransport(_handler),
    )
    async with client:
        result = await client.complete(_request())

    assert isinstance(result, ChatCompletionFailure)
    assert result.outcome is ChatCompletionOutcome.ERROR
    assert result.error_kind is ChatCompletionErrorKind.TRANSPORT
    assert result.status_code is None


@pytest.mark.asyncio
async def test_malformed_success_response_returns_typed_failure(tmp_path: Path) -> None:
    """A malformed success payload is distinguishable from quota exhaustion.

    Given: A mocked successful status with no completion choices,
    When: The client validates the response payload,
    Then: It returns a malformed-response failure instead of raising.
    """
    key_file = _write_api_key(tmp_path)

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"choices": []}, request=request)

    client = ChatCompletionsClient(
        "https://models.invalid",
        key_file,
        transport=httpx.MockTransport(_handler),
    )
    async with client:
        result = await client.complete(_request())

    assert isinstance(result, ChatCompletionFailure)
    assert result.error_kind is ChatCompletionErrorKind.MALFORMED_RESPONSE
    assert result.status_code == 200


@pytest.mark.asyncio
async def test_non_quota_http_error_returns_typed_failure(tmp_path: Path) -> None:
    """Other HTTP failures remain distinct from recoverable quota exhaustion.

    Given: A mocked endpoint returning an unavailable status,
    When: The client completes the request,
    Then: It returns an HTTP-status failure with the original status code.
    """
    key_file = _write_api_key(tmp_path)

    def _handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, content=b"unavailable", request=request)

    client = ChatCompletionsClient(
        "https://models.invalid",
        key_file,
        transport=httpx.MockTransport(_handler),
    )
    async with client:
        result = await client.complete(_request())

    assert isinstance(result, ChatCompletionFailure)
    assert result.error_kind is ChatCompletionErrorKind.HTTP_STATUS
    assert result.status_code == 503


def test_api_key_file_errors_are_generic(tmp_path: Path) -> None:
    """Credential-file failures never echo a credential or file path.

    Given: An empty key file and a separate missing path,
    When: Each path is loaded,
    Then: A narrow generic exception is raised without secret material.
    """
    empty_key_file = _write_api_key(tmp_path, "  \n")
    with pytest.raises(ApiKeyFileError, match="API key file is empty") as empty_error:
        load_api_key_file(empty_key_file)
    missing_key_file = tmp_path / "missing-secret-key"
    with pytest.raises(ApiKeyFileError, match="Unable to read the API key file") as missing_error:
        load_api_key_file(missing_key_file)
    assert str(empty_key_file) not in str(empty_error.value)
    assert str(missing_key_file) not in str(missing_error.value)
