"""Tests proving unhandled OAuth credential exceptions never reach uvicorn's error log.

``OAuthNoStoreMiddleware`` sits outside Starlette's ``ServerErrorMiddleware``,
which always re-raises after sending its 500, and uvicorn would then log the
exception message and traceback. On an OAuth credential path the middleware
must contain the exception, log only its class with an ``error_id``, and return
normally. Every other path must keep re-raising unchanged.
"""

import asyncio
import json
import logging
import re
from collections.abc import Generator
from typing import Final
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from httpx import Response
from loguru import logger
from starlette.types import Message
from starlette.types import Receive
from starlette.types import Scope
from starlette.types import Send

from snapper.server.oauth_no_store_middleware import OAuthNoStoreFastAPI
from snapper.server.oauth_no_store_middleware import OAuthNoStoreMiddleware
from snapper.server.oauth_paths import OAUTH_CREDENTIAL_PREFIXES
from snapper.server.provenance_middleware import ClientProvenanceMiddleware

_SENTINEL: Final[str] = f"SENTINEL-{uuid4().hex}"
_CREDENTIAL_LOG_MARKER: Final[str] = "on OAuth credential request"
_ERROR_ID_PATTERN: Final[re.Pattern[str]] = re.compile(r"error_id=[0-9a-f]{32}\b")
_CONTROL_CHARACTER_PATHS: Final[tuple[str, ...]] = (
    "/api/mcp/token%0A",
    "/api/mcp/token%0D%0A",
    "/api/mcp/token%09",
)
_NO_STORE_HEADERS: Final[dict[bytes, bytes]] = {
    b"cache-control": b"no-store",
    b"pragma": b"no-cache",
    b"referrer-policy": b"no-referrer",
}


async def _raise_sentinel() -> None:
    """Raise an exception whose message stands in for a submitted credential.

    Raises:
        RuntimeError: Always, carrying the sentinel message.
    """
    raise RuntimeError(_SENTINEL)


def _credential_app() -> OAuthNoStoreFastAPI:
    """Build an ``OAuthNoStoreFastAPI`` whose credential routes all raise the sentinel.

    The ``/{rest:path}`` catch-all is registered last. Starlette's ``$`` anchor
    matches before a single trailing newline, so ``/api/mcp/token%0A`` reaches
    the token route itself, while ``%0D%0A`` and ``%09`` match no specific route
    and reach the catch-all, so every control-character path raises as well.

    Returns:
        Application raising ``RuntimeError`` on every credential request.
    """
    app = OAuthNoStoreFastAPI()
    for prefix in OAUTH_CREDENTIAL_PREFIXES:
        app.add_api_route(prefix, _raise_sentinel, methods=["POST"])
    app.add_api_route("/{rest:path}", _raise_sentinel, methods=["POST"])
    return app


def _credential_scope() -> Scope:
    """Return a minimal ASGI HTTP scope for a token endpoint POST.

    Returns:
        ASGI HTTP scope targeting ``/api/mcp/token``.
    """
    return {"type": "http", "method": "POST", "path": "/api/mcp/token"}


async def _empty_receive() -> Message:
    """Return a single empty HTTP request event.

    Returns:
        ASGI ``http.request`` message with an empty body.
    """
    return {"type": "http.request", "body": b"", "more_body": False}


def _credential_log_lines(messages: list[str]) -> list[str]:
    """Return the captured records written for contained credential exceptions.

    Args:
        messages: Formatted loguru records captured during the test.

    Returns:
        Records produced by the credential containment branch.
    """
    return [message for message in messages if _CREDENTIAL_LOG_MARKER in message]


def _assert_no_store_headers(response: Response) -> None:
    """Assert every OAuth no-store and no-referrer response header is present.

    Args:
        response: HTTP response returned by TestClient.
    """
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["pragma"] == "no-cache"
    assert response.headers["referrer-policy"] == "no-referrer"


def _assert_redacted_log_line(line: str) -> None:
    """Assert a containment record is one line naming only the class and an error id.

    Args:
        line: Formatted loguru record for a contained credential exception.
    """
    assert "RuntimeError" in line
    assert _ERROR_ID_PATTERN.search(line) is not None
    assert _SENTINEL not in line
    assert "Traceback" not in line
    assert "\n" not in line.rstrip("\n")
    assert "\r" not in line
    assert "\t" not in line


@pytest.fixture
def loguru_messages() -> Generator[list[str]]:
    """Capture every loguru record formatted with the default format.

    Yields:
        List receiving one formatted string per emitted record.
    """
    messages: list[str] = []
    sink_id = logger.add(lambda msg: messages.append(str(msg)), level="DEBUG")
    try:
        yield messages
    finally:
        logger.remove(sink_id)


@pytest.mark.parametrize("path", [*OAUTH_CREDENTIAL_PREFIXES, *_CONTROL_CHARACTER_PATHS])
def test_credential_exception_is_contained_and_logged_without_message(
    path: str,
    loguru_messages: list[str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An unhandled credential exception yields a header-safe 500 and a message-free log line.

    Given: A real ``OAuthNoStoreFastAPI`` whose credential routes, and a catch-all
        reached by encoded line-break and tab suffixes, raise ``RuntimeError(SENTINEL)``,
    When: A POST carrying the sentinel in its body is sent with
        ``raise_server_exceptions=True``,
    Then: No exception escapes the app, so uvicorn would have nothing to log; the
        response is a plain 500 with no-store, no-cache, and no-referrer headers;
        and the single captured log line names ``RuntimeError`` with a 32-hex
        ``error_id`` while the sentinel appears in no body, loguru record, or
        standard logging output.

    Args:
        path: Credential request path, possibly with an encoded control character.
        loguru_messages: Captured loguru records.
        caplog: Pytest standard logging capture fixture.
    """
    caplog.set_level(logging.DEBUG)
    client = TestClient(_credential_app(), raise_server_exceptions=True)

    response = client.post(path, content=json.dumps({"client_secret": _SENTINEL}))

    assert response.status_code == 500
    _assert_no_store_headers(response)
    assert response.headers["content-type"] == "text/plain; charset=utf-8"
    assert response.text == "Internal Server Error"
    assert all(_SENTINEL not in message for message in loguru_messages)
    assert _SENTINEL not in caplog.text
    lines = _credential_log_lines(loguru_messages)
    assert len(lines) == 1
    _assert_redacted_log_line(lines[0])


@pytest.mark.asyncio
async def test_exception_before_response_start_sends_plain_500_with_headers(
    loguru_messages: list[str],
) -> None:
    """A failure before any response message is answered by the middleware itself.

    Given: An inner ASGI app that raises ``RuntimeError(SENTINEL)`` before sending anything,
    When: It is called directly through ``OAuthNoStoreMiddleware`` on a credential path,
    Then: The middleware returns normally after sending a complete plain-text 500
        whose headers include no-store, no-cache, and no-referrer, and it logs one
        message-free line.

    Args:
        loguru_messages: Captured loguru records.
    """

    async def _raise_first(_scope: Scope, _receive: Receive, _send: Send) -> None:
        """Raise before emitting any ASGI message."""
        raise RuntimeError(_SENTINEL)

    sent: list[Message] = []

    async def _send(message: Message) -> None:
        """Capture outgoing ASGI messages."""
        sent.append(message)

    await OAuthNoStoreMiddleware(_raise_first)(_credential_scope(), _empty_receive, _send)

    assert len(sent) == 2
    assert sent[0]["type"] == "http.response.start"
    assert sent[0]["status"] == 500
    assert dict(sent[0]["headers"]) == {
        b"content-type": b"text/plain; charset=utf-8",
        b"content-length": b"21",
        **_NO_STORE_HEADERS,
    }
    assert sent[1] == {"type": "http.response.body", "body": b"Internal Server Error"}
    lines = _credential_log_lines(loguru_messages)
    assert len(lines) == 1
    _assert_redacted_log_line(lines[0])


@pytest.mark.asyncio
async def test_exception_after_partial_body_returns_without_sending_more(
    loguru_messages: list[str],
) -> None:
    """A failure mid-stream leaves the started response untouched.

    Given: An inner ASGI app that sends ``http.response.start`` and a partial body
        with ``more_body=True`` and then raises ``RuntimeError(SENTINEL)``,
    When: It is called directly through ``OAuthNoStoreMiddleware`` on a credential path,
    Then: The middleware returns normally, forwards only the two original messages
        with headers injected on the start message, and logs one message-free line.

    Args:
        loguru_messages: Captured loguru records.
    """

    async def _partial_then_raise(_scope: Scope, _receive: Receive, send: Send) -> None:
        """Start a streamed response, send one chunk, then raise."""
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"partial", "more_body": True})
        raise RuntimeError(_SENTINEL)

    sent: list[Message] = []

    async def _send(message: Message) -> None:
        """Capture outgoing ASGI messages."""
        sent.append(message)

    await OAuthNoStoreMiddleware(_partial_then_raise)(_credential_scope(), _empty_receive, _send)

    assert len(sent) == 2
    assert sent[0]["status"] == 200
    assert dict(sent[0]["headers"]) == _NO_STORE_HEADERS
    assert sent[1] == {"type": "http.response.body", "body": b"partial", "more_body": True}
    lines = _credential_log_lines(loguru_messages)
    assert len(lines) == 1
    _assert_redacted_log_line(lines[0])


@pytest.mark.parametrize(
    "path",
    ["/api/orders-boom", "/api/mcp/sse", "/.well-known/oauth-authorization-server"],
)
def test_non_credential_exceptions_still_propagate(path: str, loguru_messages: list[str]) -> None:
    """Exceptions outside credential paths keep their original propagation.

    Given: An ``OAuthNoStoreFastAPI`` whose non-OAuth or surface-only route raises
        ``RuntimeError(SENTINEL)``,
    When: The route is requested with ``raise_server_exceptions=True``,
    Then: The original exception, message included, escapes the application and
        no credential containment record is logged.

    Args:
        path: Non-credential request path under test.
        loguru_messages: Captured loguru records.
    """
    app = OAuthNoStoreFastAPI()
    app.add_api_route(path, _raise_sentinel, methods=["GET"])
    client = TestClient(app, raise_server_exceptions=True)

    with pytest.raises(RuntimeError, match=_SENTINEL):
        client.get(path)

    assert _credential_log_lines(loguru_messages) == []


@pytest.mark.asyncio
async def test_cancelled_error_on_credential_path_propagates(loguru_messages: list[str]) -> None:
    """Cancellation is never swallowed on a credential path.

    Given: An inner ASGI app that raises ``asyncio.CancelledError``,
    When: It is called directly through ``OAuthNoStoreMiddleware`` on a credential path,
    Then: ``asyncio.CancelledError`` propagates, nothing is sent, and nothing is logged.

    Args:
        loguru_messages: Captured loguru records.
    """

    async def _cancelled(_scope: Scope, _receive: Receive, _send: Send) -> None:
        """Raise a cancellation before emitting any ASGI message."""
        raise asyncio.CancelledError

    sent: list[Message] = []

    async def _send(message: Message) -> None:
        """Capture outgoing ASGI messages."""
        sent.append(message)

    with pytest.raises(asyncio.CancelledError):
        await OAuthNoStoreMiddleware(_cancelled)(_credential_scope(), _empty_receive, _send)

    assert sent == []
    assert _credential_log_lines(loguru_messages) == []


def test_provenance_control_row_records_only_exception_class(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The provenance control row keeps its exception outcome without the message.

    Given: An ``OAuthNoStoreFastAPI`` with ``ClientProvenanceMiddleware`` registered and
        ``_record_control`` replaced by a capture, plus a token route raising
        ``RuntimeError(SENTINEL)``,
    When: A POST carrying the sentinel in its body is sent with
        ``raise_server_exceptions=True``,
    Then: The response is 500 with no escaping exception, and the single control
        call records ``outcome="exception"`` and ``detail="RuntimeError"`` with no
        payload and no sentinel in any keyword argument.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
    """
    captured: list[dict[str, object]] = []

    async def _capture_control(_self: ClientProvenanceMiddleware, **kwargs: object) -> None:
        """Record control-row keyword arguments instead of persisting them."""
        captured.append(kwargs)

    monkeypatch.setattr(ClientProvenanceMiddleware, "_record_control", _capture_control)
    app = OAuthNoStoreFastAPI()
    app.add_api_route("/api/mcp/token", _raise_sentinel, methods=["POST"])
    app.add_middleware(ClientProvenanceMiddleware, db_url="sqlite+aiosqlite:///:memory:")
    client = TestClient(app, raise_server_exceptions=True)

    response = client.post("/api/mcp/token", content=json.dumps({"client_secret": _SENTINEL}))

    assert response.status_code == 500
    assert len(captured) == 1
    control = captured[0]
    assert control["path"] == "/api/mcp/token"
    assert control["method"] == "POST"
    assert control["outcome"] == "exception"
    assert control["detail"] == "RuntimeError"
    assert control["payload_dict"] is None
    assert all(_SENTINEL not in str(value) for value in control.values())


@pytest.mark.asyncio
async def test_successful_credential_response_passes_through_unlogged(
    loguru_messages: list[str],
) -> None:
    """A successful credential response is forwarded with headers and no log record.

    Given: An inner ASGI app that sends a complete 200 JSON response,
    When: It is called directly through ``OAuthNoStoreMiddleware`` on a credential path,
    Then: Both messages are forwarded, the start message gains the OAuth headers
        while keeping its own, and no containment record is logged.

    Args:
        loguru_messages: Captured loguru records.
    """

    async def _ok(_scope: Scope, _receive: Receive, send: Send) -> None:
        """Send a complete JSON response."""
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send({"type": "http.response.body", "body": b"{}"})

    sent: list[Message] = []

    async def _send(message: Message) -> None:
        """Capture outgoing ASGI messages."""
        sent.append(message)

    await OAuthNoStoreMiddleware(_ok)(_credential_scope(), _empty_receive, _send)

    assert len(sent) == 2
    assert dict(sent[0]["headers"]) == {b"content-type": b"application/json", **_NO_STORE_HEADERS}
    assert sent[1] == {"type": "http.response.body", "body": b"{}"}
    assert _credential_log_lines(loguru_messages) == []
