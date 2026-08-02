"""Hermetic contracts for recorded chat-completions vendor responses."""

from pathlib import Path

import httpx
import pytest
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import TypeAdapter

from snapper.core.json_types import JsonObject
from snapper_delegate.chat_completions import ChatCompletionRequest
from snapper_delegate.chat_completions import ChatCompletionsClient
from snapper_delegate.chat_completions import ChatCompletionSuccess
from snapper_delegate.chat_completions import ChatFunctionDefinition
from snapper_delegate.chat_completions import ChatMessage
from snapper_delegate.chat_completions import ChatRole
from snapper_delegate.chat_completions import ChatTool
from snapper_delegate.mcp_bridge import sanitize_chat_tool_schema

_JSON_OBJECT_ADAPTER: TypeAdapter[JsonObject] = TypeAdapter(JsonObject)
_FIXTURE_DIRECTORY = Path(__file__).parent / "fixtures"
_USER_MESSAGE = "Review the recorded market context"


class _VendorContractFixture(BaseModel):
    """Validate one recorded vendor profile loaded from inert JSON."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    base_origin: str
    endpoint_path: str
    model: str
    response: JsonObject


def _load_fixture(filename: str) -> _VendorContractFixture:
    """Load and validate one recorded contract fixture."""
    fixture_path = _FIXTURE_DIRECTORY / filename
    return _VendorContractFixture.model_validate_json(fixture_path.read_bytes())


def _recorded_tool_schema() -> JsonObject:
    """Return a nested MCP schema that exercises the vendor sanitizer."""
    return {
        "$defs": {
            "LookupQuery": {
                "title": "LookupQuery",
                "type": "object",
                "properties": {
                    "topic": {
                        "title": "Topic",
                        "type": "string",
                    }
                },
                "required": ["topic"],
                "additionalProperties": False,
            }
        },
        "type": "object",
        "properties": {"query": {"$ref": "#/$defs/LookupQuery"}},
        "required": ["query"],
        "additionalProperties": False,
    }


def _tools() -> list[ChatTool]:
    """Build the sanitized tool catalog sent by every vendor profile."""
    return [
        ChatTool(
            type="function",
            function=ChatFunctionDefinition(
                name="lookup_context",
                parameters=sanitize_chat_tool_schema(_recorded_tool_schema()),
            ),
        )
    ]


def _json_payload(request: httpx.Request) -> JsonObject:
    """Validate one captured request body as a JSON object."""
    return _JSON_OBJECT_ADAPTER.validate_json(request.content, strict=True)


def _model_payload(model: ChatMessage | ChatTool) -> JsonObject:
    """Convert one validated wire model back to a JSON object."""
    return _JSON_OBJECT_ADAPTER.validate_python(
        model.model_dump(mode="json", exclude_unset=True),
        strict=True,
    )


async def _assert_contract(
    tmp_path: Path,
    fixture: _VendorContractFixture,
) -> ChatCompletionSuccess:
    """Exercise one recorded profile through its initial and replay turns."""
    key_file = tmp_path / "vendor-api-key"
    key_file.write_text("hermetic-contract-key", encoding="utf-8")
    captured_requests: list[httpx.Request] = []

    def _handler(request: httpx.Request) -> httpx.Response:
        captured_requests.append(request)
        return httpx.Response(200, json=fixture.response, request=request)

    tools = _tools()
    initial_request = ChatCompletionRequest(
        model=fixture.model,
        messages=[ChatMessage(role=ChatRole.USER, content=_USER_MESSAGE)],
        tools=tools,
    )
    client = ChatCompletionsClient(
        fixture.base_origin,
        key_file,
        endpoint_path=fixture.endpoint_path,
        transport=httpx.MockTransport(_handler),
    )
    async with client:
        first_result = await client.complete(initial_request)
        assert isinstance(first_result, ChatCompletionSuccess)
        assistant_message = first_result.completion.choices[0].message
        replay_request = ChatCompletionRequest(
            model=fixture.model,
            messages=[assistant_message],
            tools=tools,
        )
        replay_result = await client.complete(replay_request)

    assert isinstance(replay_result, ChatCompletionSuccess)
    assert len(captured_requests) == 2
    expected_url = f"{fixture.base_origin}{fixture.endpoint_path}"
    assert all(str(request.url) == expected_url for request in captured_requests)
    tool_payloads = [_model_payload(tool) for tool in tools]
    assert _json_payload(captured_requests[0]) == {
        "model": fixture.model,
        "messages": [{"role": "user", "content": _USER_MESSAGE}],
        "tools": tool_payloads,
    }
    assert _json_payload(captured_requests[1]) == {
        "model": fixture.model,
        "messages": [_model_payload(assistant_message)],
        "tools": tool_payloads,
    }
    return first_result


@pytest.mark.asyncio
async def test_kimi_k3_vendor_contract_round_trips_reasoning_content(tmp_path: Path) -> None:
    """Kimi uses its origin and replays opaque reasoning content unchanged."""
    result = await _assert_contract(tmp_path, _load_fixture("kimi-k3.json"))

    message = result.completion.choices[0].message
    assert message.__pydantic_extra__ == {
        "reasoning_content": "I compared the quoted context before answering."
    }


@pytest.mark.asyncio
async def test_gemini_25_pro_vendor_contract_round_trips_thought_signature(
    tmp_path: Path,
) -> None:
    """Gemini uses its prefixed path and replays its tool thought signature."""
    result = await _assert_contract(tmp_path, _load_fixture("gemini-2.5-pro.json"))

    message = result.completion.choices[0].message
    assert isinstance(message.content, str)
    assert message.tool_calls is not None
    assert message.tool_calls[0].__pydantic_extra__ == {
        "extra_content": {"google": {"thought_signature": "recorded-signature"}}
    }
    assert result.completion.usage.__pydantic_extra__ == {
        "prompt_tokens_details": {"cached_tokens": 6}
    }
    assert result.completion.usage.total_tokens == 48


@pytest.mark.asyncio
async def test_gpt_vendor_contract_round_trips_standard_response(tmp_path: Path) -> None:
    """GPT uses the standard path and preserves standard opaque fields."""
    result = await _assert_contract(tmp_path, _load_fixture("gpt.json"))

    message = result.completion.choices[0].message
    assert message.__pydantic_extra__ == {"refusal": None}
    assert result.completion.__pydantic_extra__ == {
        "id": "chatcmpl-gpt-contract",
        "object": "chat.completion",
        "created": 1785664802,
        "model": "gpt",
        "system_fingerprint": "fp_contract",
    }


@pytest.mark.asyncio
async def test_claude_vendor_contract_round_trips_standard_response(tmp_path: Path) -> None:
    """Claude uses the standard path and preserves compatibility extras."""
    result = await _assert_contract(tmp_path, _load_fixture("claude.json"))

    message = result.completion.choices[0].message
    assert message.__pydantic_extra__ == {"stop_reason": "end_turn"}
    assert result.completion.__pydantic_extra__ == {
        "id": "msg-claude-contract",
        "object": "chat.completion",
        "created": 1785664803,
        "model": "claude",
        "request_id": "req-claude-contract",
    }
