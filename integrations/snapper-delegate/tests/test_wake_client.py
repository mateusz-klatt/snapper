"""Hermetic protocol tests for the reconnecting delegate wake client."""

import asyncio
import json
import uuid
from collections.abc import Awaitable
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from pydantic import SecretStr
from pydantic import TypeAdapter

from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper_delegate.control_plane import WsToken
from snapper_delegate.wake_client import AiReviewDecisionAckFrame
from snapper_delegate.wake_client import AiReviewRequestFrame
from snapper_delegate.wake_client import EnvelopeMinter
from snapper_delegate.wake_client import WakeCallbacks
from snapper_delegate.wake_client import WakeClient
from snapper_delegate.wake_client import WakeClientConfig
from snapper_delegate.wake_client import WakeConnectRequest
from snapper_delegate.wake_client import WakeFrame
from snapper_delegate.wake_client import WakeSessionError
from snapper_delegate.wake_client import _backoff_seconds
from snapper_delegate.wake_client import _decode_frame
from snapper_delegate.wake_client import _frame_model
from snapper_delegate.wake_client import _healthy_subscription
from snapper_delegate.wake_client import _safe_socket_close
from snapper_delegate.wake_client import _signal_size
from snapper_delegate.wake_client import _SubscriptionFrame
from snapper_delegate.wake_client import _utc_now
from snapper_delegate.wake_client import _websocket_url
from snapper_delegate.wake_client import default_wake_connect

_JSON_OBJECT_ADAPTER = TypeAdapter(JsonObject)
_TIMESTAMP = "2026-08-02T12:00:00+00:00"
_DEADLINE = "2026-08-02T12:00:25+00:00"


class _FakeCredentials:
    """Return queued access and one-shot credentials without network access."""

    def __init__(
        self,
        ws_tokens: list[WsToken | Exception] | None = None,
        access_tokens: list[SecretStr | Exception] | None = None,
    ) -> None:
        self.ws_tokens = list(ws_tokens or [_ws_token("ws-one")])
        self.access_tokens = list(access_tokens or [SecretStr("access-one")])
        self.mint_calls = 0
        self.read_calls = 0

    async def mint_ws_token(self) -> WsToken:
        """Return or raise the next queued one-shot credential outcome."""
        self.mint_calls += 1
        outcome = self.ws_tokens.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def read_access_token(self) -> SecretStr:
        """Return or raise the next queued access credential outcome."""
        self.read_calls += 1
        outcome = self.access_tokens.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class _ClosingCredentials(_FakeCredentials):
    """Close an owning client while a one-shot token is minted."""

    def __init__(self) -> None:
        super().__init__()
        self.client: WakeClient | None = None

    async def mint_ws_token(self) -> WsToken:
        """Close the client before returning the fresh credential."""
        token = await super().mint_ws_token()
        assert self.client is not None
        await self.client.close()
        return token


class _BlockingCredentials(_FakeCredentials):
    """Hold token minting so concurrent-run protection can be exercised."""

    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def mint_ws_token(self) -> WsToken:
        """Wait for the test to release the first mint operation."""
        self.started.set()
        await self.release.wait()
        return await super().mint_ws_token()


class _FakeSocket:
    """Queue-backed WebSocket with controllable send, receive, and close failures."""

    def __init__(
        self,
        incoming: list[str | bytes | Exception] | None = None,
        *,
        send_failures: list[Exception] | None = None,
        close_failure: Exception | None = None,
        unblock_on_close: bool = True,
    ) -> None:
        self.incoming: asyncio.Queue[str | bytes | Exception] = asyncio.Queue()
        for item in incoming or []:
            self.incoming.put_nowait(item)
        self.send_failures = list(send_failures or [])
        self.close_failure = close_failure
        self.unblock_on_close = unblock_on_close
        self.sent: list[str] = []
        self.close_calls: list[tuple[int, str]] = []

    async def send(self, message: str) -> None:
        """Record a frame or raise the next injected send failure."""
        if self.send_failures:
            raise self.send_failures.pop(0)
        self.sent.append(message)

    async def recv(self) -> str | bytes:
        """Return or raise the next queued receive outcome."""
        outcome = await self.incoming.get()
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """Record close intent, unblock receive, and optionally fail."""
        self.close_calls.append((code, reason))
        if self.unblock_on_close:
            self.incoming.put_nowait(ConnectionError("socket closed"))
        if self.close_failure is not None:
            raise self.close_failure

    def decoded_sent(self) -> list[JsonObject]:
        """Decode all client frames into typed JSON objects."""
        return [_JSON_OBJECT_ADAPTER.validate_json(item) for item in self.sent]


class _FakeConnector:
    """Return queued sockets or transport failures while retaining safe requests."""

    def __init__(self, outcomes: list[_FakeSocket | Exception]) -> None:
        self.outcomes = list(outcomes)
        self.requests: list[WakeConnectRequest] = []

    async def __call__(self, request: WakeConnectRequest) -> _FakeSocket:
        """Record the upgrade request and return its queued outcome."""
        self.requests.append(request)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class _CallbackRecorder:
    """Collect wake callbacks and expose deterministic failure hooks."""

    def __init__(self) -> None:
        self.connections: list[bool] = []
        self.subscriptions = 0
        self.frames: list[WakeFrame] = []
        self.heartbeats = 0
        self.connection_failures = 0
        self.subscription_failures = 0
        self.frame_failures = 0
        self.heartbeat_failures = 0
        self.subscription_hook: Callable[[], Awaitable[None]] | None = None
        self.heartbeat_hook: Callable[[], Awaitable[None]] | None = None

    def bundle(self) -> WakeCallbacks:
        """Build the callback bundle consumed by the client."""
        return WakeCallbacks(
            connection_state=self.connection_state,
            subscribed=self.subscribed,
            frame=self.frame,
            heartbeat=self.heartbeat,
        )

    async def connection_state(self, connected: bool) -> None:
        """Record connection changes or raise one requested callback failure."""
        self.connections.append(connected)
        if self.connection_failures > 0:
            self.connection_failures -= 1
            raise RuntimeError("connection callback")

    async def subscribed(self) -> None:
        """Record healthy subscriptions or raise one requested callback failure."""
        self.subscriptions += 1
        if self.subscription_hook is not None:
            await self.subscription_hook()
        if self.subscription_failures > 0:
            self.subscription_failures -= 1
            raise RuntimeError("subscription callback")

    async def frame(self, frame: WakeFrame) -> None:
        """Record a delivered frame or raise one requested callback failure."""
        if self.frame_failures > 0:
            self.frame_failures -= 1
            raise RuntimeError("frame callback")
        self.frames.append(frame)

    async def heartbeat(self) -> None:
        """Record an application heartbeat and invoke its optional hook."""
        self.heartbeats += 1
        if self.heartbeat_hook is not None:
            await self.heartbeat_hook()
        if self.heartbeat_failures > 0:
            self.heartbeat_failures -= 1
            raise RuntimeError("heartbeat callback")


def _ws_token(value: str) -> WsToken:
    """Build a stable one-shot credential for tests."""
    return WsToken(
        value=SecretStr(value),
        expires_at=datetime(2026, 8, 2, 12, 15, tzinfo=UTC),
    )


def _control_frame(type_name: str, **fields: JsonValue) -> str:
    """Encode one server control frame with optional fields."""
    payload: JsonObject = {"type": type_name}
    payload.update(fields)
    return json.dumps(payload)


def _subscription_frame(
    action: str = "subscribe",
    status: str = "subscribed",
    topics: list[str] | None = None,
) -> str:
    """Encode one subscription result."""
    return _control_frame(
        "subscription_success",
        action=action,
        status=status,
        topics=["ai_reviews."] if topics is None else topics,
        denied_topics=[],
    )


def _envelope(sequence_id: int = 1) -> JsonObject:
    """Return a complete server provenance envelope."""
    return {
        "session_id": "server-session",
        "sequence_id": sequence_id,
        "public_id": f"server-frame-{sequence_id}",
        "timestamp": _TIMESTAMP,
        "topic": "ai_reviews.user.strategy.request",
    }


def _request_frame(
    review_id: str = "review-one",
    dispatch_version: int = 0,
    signal_envelope: JsonObject | None = None,
) -> str:
    """Encode one valid AI review request wake."""
    payload: JsonObject = {
        "type": "ai_review.request",
        **_envelope(dispatch_version + 1),
        "review_public_id": review_id,
        "user_public_id": "user-one",
        "strategy_public_id": "strategy-one",
        "wallet_public_id": "wallet-one",
        "instrument_public_id": "instrument-one",
        "selected_delegate_public_id": "delegate-one",
        "deadline": _DEADLINE,
        "signal_envelope": signal_envelope or {"side": "buy"},
        "instrument_metadata": {"instrument": "BTC/USD"},
        "dispatch_version": dispatch_version,
        "extension": {"future": True},
    }
    return json.dumps(payload, ensure_ascii=False)


def _ack_frame(
    review_id: str = "review-one",
    dispatch_version: int = 1,
) -> str:
    """Encode one valid AI review decision acknowledgement."""
    payload: JsonObject = {
        "type": "ai_review.decision_ack",
        **_envelope(dispatch_version + 1),
        "review_public_id": review_id,
        "user_public_id": "user-one",
        "strategy_public_id": "strategy-one",
        "wallet_public_id": "wallet-one",
        "instrument_public_id": "instrument-one",
        "responding_delegate_public_id": "delegate-one",
        "decision": "approve",
        "new_status": "resolved_approved",
        "resolution_mode": "pick_one_primary",
        "rationale": None,
        "dispatch_version": dispatch_version,
    }
    return json.dumps(payload)


def _handshake_frames(*stream_frames: str | bytes | Exception) -> list[str | bytes | Exception]:
    """Prefix streaming outcomes with one healthy handshake."""
    return [
        _control_frame("auth_required"),
        _control_frame("auth_ok"),
        _control_frame("auth_complete"),
        _subscription_frame(),
        *stream_frames,
    ]


def _client(
    credentials: _FakeCredentials | None = None,
    connector: _FakeConnector | None = None,
    config: WakeClientConfig | None = None,
    minter: EnvelopeMinter | None = None,
) -> WakeClient:
    """Construct one hermetic client with optional seams."""
    return WakeClient(
        "http://snapper.internal:8000",
        credentials or _FakeCredentials(),
        connect_factory=connector or _FakeConnector([]),
        config=config,
        envelope_minter=minter,
    )


async def _wait_until(predicate: Callable[[], bool]) -> None:
    """Yield until a deterministic asynchronous predicate becomes true."""
    for _attempt in range(100):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("asynchronous condition did not become true")


def test_envelope_minter_uses_stable_uuid7_and_monotonic_sequence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every client frame gets fresh provenance under one stable process session."""
    identifiers = [uuid.UUID(int=index, version=7) for index in (1, 2, 3)]
    uuid7 = MagicMock(side_effect=identifiers)
    monkeypatch.setattr("snapper_delegate.wake_client.uuid.uuid7", uuid7)
    clock = MagicMock(return_value=datetime(2026, 8, 2, 12, 0, tzinfo=UTC))
    minter = EnvelopeMinter(clock)

    first = minter.next()
    second = minter.next()

    assert first == {
        "session_id": str(identifiers[0]),
        "sequence_id": 1,
        "public_id": str(identifiers[1]),
        "timestamp": _TIMESTAMP,
        "topic": None,
    }
    assert second["session_id"] == first["session_id"]
    assert second["sequence_id"] == 2
    assert second["public_id"] == str(identifiers[2])
    assert uuid7.call_count == 3
    assert clock.call_count == 2


def test_envelope_minter_default_clock_and_utc_helper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The production minter uses the module UTC clock when none is injected."""
    now = datetime(2026, 8, 2, 12, 0, tzinfo=UTC)
    monkeypatch.setattr("snapper_delegate.wake_client._utc_now", MagicMock(return_value=now))
    monkeypatch.setattr(
        "snapper_delegate.wake_client.uuid.uuid7",
        MagicMock(return_value=uuid.UUID(int=7, version=7)),
    )

    envelope = EnvelopeMinter().next()

    assert envelope["timestamp"] == _TIMESTAMP
    assert _utc_now().tzinfo is UTC


@pytest.mark.parametrize(
    ("base_url", "expected"),
    [
        ("http://snapper:8000", "ws://snapper:8000/api/ws"),
        ("https://snapper.example", "wss://snapper.example/api/ws"),
    ],
)
def test_websocket_url_uses_control_origin(base_url: str, expected: str) -> None:
    """HTTP origins map to the fixed WebSocket endpoint and matching scheme."""
    assert _websocket_url(base_url) == expected


def test_connect_request_repr_hides_authorization() -> None:
    """Upgrade request representations cannot expose bearer contents."""
    request = WakeConnectRequest(
        url="ws://snapper/api/ws",
        additional_headers={"Authorization": "Bearer access-secret"},
    )
    assert "access-secret" not in repr(request)
    assert "Authorization" not in repr(request)


@pytest.mark.asyncio
async def test_default_connect_disables_proxy_protocol_ping_and_bounds_frames(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The production connector uses only the explicit authenticated direct route."""
    socket = _FakeSocket()
    connect = AsyncMock(return_value=socket)
    monkeypatch.setattr("snapper_delegate.wake_client.connect", connect)
    request = WakeConnectRequest(
        url="wss://snapper.example/api/ws",
        additional_headers={"Authorization": "Bearer secret"},
    )

    result = await default_wake_connect(request)

    assert result is socket
    connect.assert_awaited_once_with(
        request.url,
        additional_headers=request.additional_headers,
        proxy=None,
        ping_interval=None,
        max_size=64 * 1024,
    )


def test_client_rejects_nonpositive_dedup_capacity() -> None:
    """A bounded dedup cache must retain at least one entry."""
    with pytest.raises(ValueError, match="dedup_capacity must be positive"):
        _client(config=WakeClientConfig(dedup_capacity=0))


@pytest.mark.parametrize(
    "case",
    [
        (0, 1.0, 30.0, 0.25, 0.5, 1.0),
        (3, 1.0, 4.0, 0.0, 0.5, 4.0),
        (99, 0.0, 30.0, 0.25, 1.0, 0.0),
        (0, 1.0, 30.0, 2.0, 0.0, 0.0),
    ],
)
def test_backoff_is_exponential_capped_jittered_and_nonnegative(
    case: tuple[int, float, float, float, float, float],
) -> None:
    """Reconnect delays obey all cap, exponent, jitter, and lower-bound rules."""
    attempt, base, cap, jitter, random_value, expected = case
    config = WakeClientConfig(
        backoff_base_seconds=base,
        backoff_cap_seconds=cap,
        backoff_jitter_fraction=jitter,
        random_value=lambda: random_value,
    )
    assert _backoff_seconds(attempt, config) == expected


@pytest.mark.parametrize(
    ("action", "status", "topics", "expected"),
    [
        ("subscribe", "subscribed", ["ai_reviews."], True),
        ("subscribe", "partial", ["ai_reviews."], True),
        ("unsubscribe", "subscribed", ["ai_reviews."], False),
        ("subscribe", "denied", ["ai_reviews."], False),
        ("subscribe", "subscribed", [], False),
    ],
)
def test_subscription_health_requires_accepted_subscribe_topics(
    action: str,
    status: str,
    topics: list[str],
    expected: bool,
) -> None:
    """The shared response type is healthy only for an accepted subscribe action."""
    frame = _SubscriptionFrame(
        type="subscription_success",
        action=action,
        status=status,
        topics=topics,
    )
    assert _healthy_subscription(frame) is expected


def test_subscription_health_rejects_a_non_subscription_model() -> None:
    """A matching type string without the subscription result fields is not healthy."""
    frame = _decode_frame(_control_frame("subscription_success", action="subscribe"))
    assert frame is None
    unknown = _decode_frame(_control_frame("future.control"))
    assert unknown is not None
    assert _healthy_subscription(unknown) is False


def test_frame_model_selects_only_locally_consumed_specializations() -> None:
    """Known request, acknowledgement, and subscription types have local models."""
    assert _frame_model("ai_review.request") is AiReviewRequestFrame
    assert _frame_model("ai_review.decision_ack") is AiReviewDecisionAckFrame
    assert _frame_model("subscription_success") is _SubscriptionFrame
    assert _frame_model("future.frame") is None


def test_decode_valid_request_and_ack_preserves_extensions_and_sizes_signal() -> None:
    """Valid data wakes become typed models while opaque future fields survive."""
    request = _decode_frame(_request_frame(signal_envelope={"headline": "żółć"}))
    acknowledgement = _decode_frame(_ack_frame())

    assert isinstance(request, AiReviewRequestFrame)
    assert request.__pydantic_extra__ == {"extension": {"future": True}}
    assert request.timestamp.tzinfo is not None
    assert _signal_size(request) == len('{"headline":"żółć"}'.encode())
    assert isinstance(acknowledgement, AiReviewDecisionAckFrame)
    assert acknowledgement.rationale is None


@pytest.mark.parametrize(
    "raw",
    [
        "not-json",
        b"\xff",
        json.dumps({"missing": "type"}),
        _control_frame("ai_review.request"),
    ],
)
def test_decode_drops_malformed_frames(raw: str | bytes) -> None:
    """Malformed general and known frames are discarded without raising."""
    assert _decode_frame(raw) is None


def test_decode_drops_unencodable_and_oversized_raw_frames() -> None:
    """Raw frame budgets apply before JSON parsing for text and bytes inputs."""
    assert _decode_frame("\ud800") is None
    assert _decode_frame(b"x" * (64 * 1024 + 1)) is None


def test_decode_drops_oversized_signal_but_accepts_unknown_control() -> None:
    """The nested signal budget is independent from forward-compatible controls."""
    oversized = _request_frame(signal_envelope={"value": "x" * (16 * 1024)})
    assert _decode_frame(oversized) is None
    unknown = _decode_frame(_control_frame("future.control", value=True).encode())
    assert unknown is not None
    assert unknown.type == "future.control"


@pytest.mark.asyncio
async def test_handshake_waits_for_auth_complete_and_sends_strict_envelopes() -> None:
    """Auth acknowledgement alone cannot advance subscription or omit provenance."""
    minter = EnvelopeMinter(lambda: datetime(2026, 8, 2, 12, 0, tzinfo=UTC))
    client = _client(minter=minter)
    socket = _FakeSocket([_control_frame("auth_required"), _control_frame("auth_ok")])
    task = asyncio.create_task(client._handshake(socket, SecretStr("ws-secret")))

    await _wait_until(lambda: len(socket.sent) == 1)
    assert task.done() is False
    socket.incoming.put_nowait(_control_frame("future.control"))
    socket.incoming.put_nowait(_control_frame("auth_complete"))
    socket.incoming.put_nowait(_subscription_frame())
    await task

    sent = socket.decoded_sent()
    assert [item["type"] for item in sent] == ["authenticate", "subscribe"]
    assert sent[0]["ws_token"] == "ws-secret"
    assert sent[1]["topics"] == ["ai_reviews."]
    assert sent[0]["session_id"] == sent[1]["session_id"]
    assert [item["sequence_id"] for item in sent] == [1, 2]
    assert all(item["topic"] is None for item in sent)
    assert all(uuid.UUID(str(item["public_id"])).version == 7 for item in sent)


@pytest.mark.asyncio
async def test_handshake_ignores_a_malformed_frame_before_expected_control() -> None:
    """Malformed forward traffic does not prevent a later valid handshake."""
    socket = _FakeSocket(
        [
            "not-json",
            _control_frame("auth_required"),
            _control_frame("auth_complete"),
            _subscription_frame(),
        ]
    )
    await _client()._handshake(socket, SecretStr("ws-secret"))
    assert [item["type"] for item in socket.decoded_sent()] == ["authenticate", "subscribe"]


@pytest.mark.parametrize("failure_type", ["auth_failed", "auth_expired"])
@pytest.mark.asyncio
async def test_handshake_rejects_authentication_failures(failure_type: str) -> None:
    """Authentication failure controls terminate a session at either auth wait."""
    client = _client()
    socket = _FakeSocket([_control_frame(failure_type)])
    with pytest.raises(WakeSessionError, match="authentication failed"):
        await client._handshake(socket, SecretStr("ws-secret"))


@pytest.mark.asyncio
async def test_handshake_times_out_without_server_progress() -> None:
    """A silent peer cannot hold the authentication phase indefinitely."""
    client = _client(config=WakeClientConfig(handshake_timeout_seconds=0.001))
    with pytest.raises(TimeoutError):
        await client._handshake(_FakeSocket(), SecretStr("ws-secret"))


@pytest.mark.parametrize(
    ("action", "status", "topics"),
    [
        ("subscribe", "denied", []),
        ("unsubscribe", "subscribed", ["ai_reviews."]),
        ("subscribe", "subscribed", []),
    ],
)
@pytest.mark.asyncio
async def test_handshake_rejects_unhealthy_subscription_results(
    action: str,
    status: str,
    topics: list[str],
) -> None:
    """Denied, wrong-action, and empty subscription responses force reconnect."""
    socket = _FakeSocket(
        [
            _control_frame("auth_required"),
            _control_frame("auth_complete"),
            _subscription_frame(action, status, topics),
        ]
    )
    with pytest.raises(WakeSessionError, match="subscription rejected"):
        await _client()._handshake(socket, SecretStr("ws-secret"))


@pytest.mark.asyncio
async def test_run_session_rereads_access_token_and_reports_connection_lifecycle() -> None:
    """Each successful upgrade uses the current bearer without exposing it in repr."""
    credentials = _FakeCredentials(
        ws_tokens=[_ws_token("one-shot-secret")],
        access_tokens=[SecretStr("rotated-access-secret")],
    )
    socket = _FakeSocket(_handshake_frames(ConnectionError("disconnect")))
    connector = _FakeConnector([socket])
    recorder = _CallbackRecorder()
    client = _client(credentials, connector)

    with pytest.raises(ConnectionError, match="disconnect"):
        await client._run_session(recorder.bundle())

    assert credentials.mint_calls == 1
    assert credentials.read_calls == 1
    assert connector.requests[0].additional_headers == {
        "Authorization": "Bearer rotated-access-secret"
    }
    assert "rotated-access-secret" not in repr(connector.requests[0])
    assert recorder.connections == [True, False]
    assert recorder.subscriptions == 1
    assert socket.close_calls == [(1000, "client shutdown")]


@pytest.mark.asyncio
async def test_run_session_streams_and_pings_while_subscription_callback_blocks() -> None:
    """A reconnect sweep cannot delay wake receipt, liveness, or session cleanup."""
    subscription_started = asyncio.Event()
    subscription_cancelled = asyncio.Event()
    subscription_release = asyncio.Event()
    recorder = _CallbackRecorder()

    async def _block_subscription() -> None:
        subscription_started.set()
        try:
            await subscription_release.wait()
        finally:
            subscription_cancelled.set()

    recorder.subscription_hook = _block_subscription
    socket = _FakeSocket(_handshake_frames())
    client = _client(
        connector=_FakeConnector([socket]),
        config=WakeClientConfig(heartbeat_interval_seconds=0.0),
    )
    task = asyncio.create_task(client._run_session(recorder.bundle()))

    await subscription_started.wait()
    socket.incoming.put_nowait(_request_frame())
    await _wait_until(lambda: len(recorder.frames) == 1)
    await _wait_until(lambda: recorder.heartbeats > 0)
    assert subscription_release.is_set() is False
    assert subscription_cancelled.is_set() is False
    socket.incoming.put_nowait(ConnectionError("disconnect"))

    with pytest.raises(ConnectionError, match="disconnect"):
        await task
    assert subscription_cancelled.is_set() is True


@pytest.mark.asyncio
async def test_run_session_contains_background_subscription_callback_failure() -> None:
    """A failed reconnect sweep cannot terminate normal frame streaming."""
    recorder = _CallbackRecorder()
    recorder.subscription_failures = 1
    socket = _FakeSocket(_handshake_frames())
    client = _client(connector=_FakeConnector([socket]))
    task = asyncio.create_task(client._run_session(recorder.bundle()))

    await _wait_until(lambda: recorder.subscriptions == 1)
    socket.incoming.put_nowait(_request_frame())
    await _wait_until(lambda: len(recorder.frames) == 1)
    socket.incoming.put_nowait(ConnectionError("disconnect"))

    with pytest.raises(ConnectionError, match="disconnect"):
        await task


@pytest.mark.asyncio
async def test_run_session_stops_after_token_mint_when_close_arrives() -> None:
    """Closing during credential mint avoids reading a bearer or opening a socket."""
    credentials = _ClosingCredentials()
    connector = _FakeConnector([])
    client = _client(credentials, connector)
    credentials.client = client

    await client._run_session(_CallbackRecorder().bundle())

    assert credentials.mint_calls == 1
    assert credentials.read_calls == 0
    assert connector.requests == []


@pytest.mark.asyncio
async def test_run_session_preserves_a_replaced_active_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cleanup cannot clear a newer socket reference installed by another session edge."""
    socket = _FakeSocket(_handshake_frames())
    replacement = _FakeSocket()
    client = _client(connector=_FakeConnector([socket]))

    async def _replace_then_fail(
        active_socket: _FakeSocket,
        callbacks: WakeCallbacks,
    ) -> None:
        del active_socket, callbacks
        client._active_socket = replacement
        raise ConnectionError("stream failed")

    monkeypatch.setattr(client, "_stream", _replace_then_fail)

    with pytest.raises(ConnectionError, match="stream failed"):
        await client._run_session(_CallbackRecorder().bundle())
    assert client._active_socket is replacement


@pytest.mark.asyncio
async def test_run_reconnects_with_backoff_and_resets_only_after_subscription(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A healthy subscription resets attempts while upgrade failures retain backoff."""
    credentials = _FakeCredentials(
        ws_tokens=[_ws_token("ws-one"), _ws_token("ws-two")],
        access_tokens=[SecretStr("access-one"), SecretStr("access-two")],
    )
    socket = _FakeSocket(_handshake_frames(ConnectionError("stream failed")))
    connector = _FakeConnector([ConnectionError("upgrade failed"), socket])
    recorder = _CallbackRecorder()
    client = _client(
        credentials,
        connector,
        WakeClientConfig(backoff_jitter_fraction=0.0),
    )
    delays: list[float] = []

    async def _record_wait(delay: float) -> None:
        delays.append(delay)
        if len(delays) == 2:
            await client.close()

    monkeypatch.setattr(client, "_wait_for_close", _record_wait)

    await client.run(recorder.bundle())

    assert delays == [1.0, 1.0]
    assert credentials.mint_calls == 2
    assert credentials.read_calls == 2
    assert [request.additional_headers["Authorization"] for request in connector.requests] == [
        "Bearer access-one",
        "Bearer access-two",
    ]
    assert recorder.connections == [True, False]
    assert recorder.subscriptions == 1
    assert client._running is False


@pytest.mark.asyncio
async def test_run_rejects_concurrent_invocation_and_restores_state() -> None:
    """One client has exactly one reconnect supervisor at a time."""
    credentials = _BlockingCredentials()
    client = _client(credentials, _FakeConnector([ConnectionError("unused")]))
    task = asyncio.create_task(client.run(_CallbackRecorder().bundle()))
    await credentials.started.wait()

    with pytest.raises(RuntimeError, match="already running"):
        await client.run(_CallbackRecorder().bundle())
    await client.close()
    credentials.release.set()
    await task

    assert client._running is False


@pytest.mark.asyncio
async def test_run_propagates_external_session_cancellation() -> None:
    """Cancellation unrelated to client close remains visible to the task owner."""
    credentials = _BlockingCredentials()
    client = _client(credentials, _FakeConnector([]))
    task = asyncio.create_task(client.run(_CallbackRecorder().bundle()))
    await credentials.started.wait()
    assert client._session_task is not None

    client._session_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert client._running is False


@pytest.mark.asyncio
async def test_run_forever_preserves_replaced_session_task_and_observes_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A replaced task reference is not cleared and a completed session can stop cleanly."""
    client = _client()
    replacement = asyncio.create_task(asyncio.sleep(0))

    async def _replace_and_close(callbacks: WakeCallbacks) -> None:
        del callbacks
        client._session_task = replacement
        client._closed.set()

    monkeypatch.setattr(client, "_run_session", _replace_and_close)

    await client._run_forever(_CallbackRecorder().bundle())
    await replacement
    assert client._session_task is replacement


@pytest.mark.asyncio
async def test_closed_client_run_returns_without_connecting() -> None:
    """A close requested before run makes the reconnect supervisor a no-op."""
    connector = _FakeConnector([])
    client = _client(connector=connector)
    await client.close()
    await client.close()
    await client.run(_CallbackRecorder().bundle())
    assert connector.requests == []


@pytest.mark.asyncio
async def test_wait_for_close_handles_zero_timeout_event_and_elapsed_timeout() -> None:
    """Reconnect waits yield at zero and finish on either close or timeout."""
    zero_client = _client()
    await zero_client._wait_for_close(0.0)
    closed_client = _client()
    await closed_client.close()
    await closed_client._wait_for_close(10.0)
    timeout_client = _client()
    await timeout_client._wait_for_close(0.001)


@pytest.mark.asyncio
async def test_ping_loop_sends_enveloped_liveness_and_stops_on_close() -> None:
    """Application heartbeats remain independent and stop promptly with the client."""
    client = _client(config=WakeClientConfig(heartbeat_interval_seconds=0.001))
    socket = _FakeSocket()
    recorder = _CallbackRecorder()

    async def _close_after_heartbeat() -> None:
        await client.close()

    recorder.heartbeat_hook = _close_after_heartbeat
    await client._ping_loop(socket, recorder.bundle())

    assert recorder.heartbeats == 1
    assert socket.decoded_sent()[0]["type"] == "ping"
    assert socket.close_calls == []


@pytest.mark.asyncio
async def test_ping_loop_contains_send_failure_and_closes_socket() -> None:
    """A failed application ping ends only its transport session."""
    client = _client(config=WakeClientConfig(heartbeat_interval_seconds=0.001))
    socket = _FakeSocket(send_failures=[ConnectionError("send failed")])
    await client._ping_loop(socket, _CallbackRecorder().bundle())
    assert socket.close_calls == [(1000, "client shutdown")]


@pytest.mark.asyncio
async def test_ping_loop_returns_when_close_event_wins_wait() -> None:
    """A close arriving during the heartbeat delay prevents another ping."""
    client = _client(config=WakeClientConfig(heartbeat_interval_seconds=10.0))
    socket = _FakeSocket()
    task = asyncio.create_task(client._ping_loop(socket, _CallbackRecorder().bundle()))
    await asyncio.sleep(0)
    client._closed.set()
    await task
    assert socket.sent == []


@pytest.mark.asyncio
async def test_stream_handles_reauth_wakes_dedup_and_auth_expiry() -> None:
    """The sole receive loop keeps flowing while an in-socket reauth task waits."""
    credentials = _FakeCredentials(ws_tokens=[_ws_token("reauth-secret")])
    client = _client(credentials)
    socket = _FakeSocket()
    recorder = _CallbackRecorder()
    task = asyncio.create_task(client._stream(socket, recorder.bundle()))

    socket.incoming.put_nowait("not-json")
    socket.incoming.put_nowait(_control_frame("reauth_required"))
    await _wait_until(lambda: any(item.get("type") == "reauth" for item in socket.decoded_sent()))
    socket.incoming.put_nowait(_control_frame("reauth_required"))
    socket.incoming.put_nowait(_control_frame("reauth_ok"))
    socket.incoming.put_nowait(_control_frame("pong"))
    socket.incoming.put_nowait(_request_frame())
    socket.incoming.put_nowait(_request_frame())
    socket.incoming.put_nowait(_ack_frame())
    socket.incoming.put_nowait(_control_frame("auth_expired"))

    with pytest.raises(WakeSessionError, match="authentication expired"):
        await task

    assert credentials.mint_calls == 1
    assert [item["type"] for item in socket.decoded_sent()] == ["reauth"]
    assert [frame.type for frame in recorder.frames] == [
        "ai_review.request",
        "ai_review.decision_ack",
    ]


@pytest.mark.asyncio
async def test_stream_returns_without_receive_when_client_is_already_closed() -> None:
    """A preexisting close request bypasses the streaming receive loop."""
    client = _client()
    client._closed.set()
    await client._stream(_FakeSocket(), _CallbackRecorder().bundle())


@pytest.mark.asyncio
async def test_stream_starts_new_reauth_after_prior_task_completes() -> None:
    """A later warning mints a new one-shot token after reauth_ok completed the prior task."""
    credentials = _FakeCredentials(ws_tokens=[_ws_token("reauth-one"), _ws_token("reauth-two")])
    client = _client(credentials)
    socket = _FakeSocket()
    task = asyncio.create_task(client._stream(socket, _CallbackRecorder().bundle()))

    socket.incoming.put_nowait(_control_frame("reauth_required"))
    await _wait_until(lambda: len(socket.sent) == 1)
    socket.incoming.put_nowait(_control_frame("reauth_ok"))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    socket.incoming.put_nowait(_control_frame("reauth_required"))
    await _wait_until(lambda: len(socket.sent) == 2)
    socket.incoming.put_nowait(_control_frame("reauth_ok"))
    socket.incoming.put_nowait(_control_frame("auth_failed"))

    with pytest.raises(WakeSessionError, match="authentication expired"):
        await task
    assert credentials.mint_calls == 2


@pytest.mark.parametrize("failure_stage", ["mint", "send", "timeout"])
@pytest.mark.asyncio
async def test_reauthentication_failure_closes_with_private_reason(failure_stage: str) -> None:
    """Mint, send, and acknowledgement failures cycle the socket without escaping."""
    credentials = _FakeCredentials(
        ws_tokens=(
            [RuntimeError("mint failed")]
            if failure_stage == "mint"
            else [_ws_token("reauth-secret")]
        )
    )
    client = _client(
        credentials,
        config=WakeClientConfig(reauth_timeout_seconds=0.001),
    )
    socket = _FakeSocket(
        send_failures=[ConnectionError("send failed")] if failure_stage == "send" else None
    )
    acknowledgement = asyncio.Event()
    if failure_stage != "timeout":
        acknowledgement.set()

    await client._reauthenticate(socket, acknowledgement)

    assert socket.close_calls == [(4001, "reauthentication failed")]


@pytest.mark.asyncio
async def test_delivery_commits_after_success_updates_versions_and_evicts_oldest() -> None:
    """Dedup is at-least-once, type-aware, version-aware, and capacity bounded."""
    client = _client(config=WakeClientConfig(dedup_capacity=1))
    recorder = _CallbackRecorder()
    request_v0 = _decode_frame(_request_frame(dispatch_version=0))
    request_v1 = _decode_frame(_request_frame(dispatch_version=1))
    request_v2 = _decode_frame(_request_frame(dispatch_version=2))
    acknowledgement = _decode_frame(_ack_frame(dispatch_version=1))
    assert isinstance(request_v0, AiReviewRequestFrame)
    assert isinstance(request_v1, AiReviewRequestFrame)
    assert isinstance(request_v2, AiReviewRequestFrame)
    assert isinstance(acknowledgement, AiReviewDecisionAckFrame)

    await client._deliver(request_v0, recorder.bundle())
    await client._deliver(request_v0, recorder.bundle())
    await client._deliver(request_v1, recorder.bundle())
    await client._deliver(acknowledgement, recorder.bundle())
    await client._deliver(request_v2, recorder.bundle())

    assert [frame.type for frame in recorder.frames] == [
        "ai_review.request",
        "ai_review.request",
        "ai_review.decision_ack",
        "ai_review.request",
    ]
    assert list(client._dedup.items()) == [("ai_review.request:review-one", 2)]


@pytest.mark.asyncio
async def test_delivery_callback_failure_is_retryable() -> None:
    """A failed consumer callback does not poison dedup state for redelivery."""
    client = _client()
    recorder = _CallbackRecorder()
    recorder.frame_failures = 1
    frame = _decode_frame(_request_frame())
    assert isinstance(frame, AiReviewRequestFrame)

    await client._deliver(frame, recorder.bundle())
    await client._deliver(frame, recorder.bundle())

    assert recorder.frames == [frame]
    assert client._dedup["ai_review.request:review-one"] == 0


@pytest.mark.asyncio
async def test_notification_failures_are_contained() -> None:
    """Status, subscription, and heartbeat hooks cannot kill the wake client."""
    client = _client()
    recorder = _CallbackRecorder()
    recorder.connection_failures = 1
    recorder.subscription_failures = 1
    recorder.heartbeat_failures = 1

    await client._notify_connection(recorder.bundle(), True)
    await client._notify_subscribed(recorder.bundle())
    await client._notify_heartbeat(recorder.bundle())

    assert recorder.connections == [True]
    assert recorder.subscriptions == 1
    assert recorder.heartbeats == 1


@pytest.mark.parametrize("close_failure", [None, ConnectionError("close failed")])
@pytest.mark.asyncio
async def test_safe_socket_close_contains_transport_outcomes(
    close_failure: Exception | None,
) -> None:
    """Both successful and failed close handshakes stay inside the boundary."""
    socket = _FakeSocket(close_failure=close_failure)
    await _safe_socket_close(socket, 4001, "cycle")
    assert socket.close_calls == [(4001, "cycle")]


@pytest.mark.asyncio
async def test_close_active_handles_present_and_absent_socket() -> None:
    """Client shutdown is idempotent around an optional active connection."""
    client = _client()
    await client._close_active()
    socket = _FakeSocket()
    client._active_socket = socket
    await client._close_active()
    assert socket.close_calls == [(1000, "client shutdown")]
