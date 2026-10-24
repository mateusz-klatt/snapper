"""Actual SDK receive-loop privacy tests for the owned private decoder.

Controls retain no original authentication bytes; financial raw preservation
and actual SDK control handling are measured separately from ownership grants.
"""

import asyncio
import json
import logging
import pickle
import traceback
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import cast
from weakref import WeakKeyDictionary

import kraken.futures.websocket as futures_websocket
import pytest
from kraken.futures import FuturesWSClient
from kraken.futures.websocket import ConnectFuturesWebsocket
from requests import Session
from websockets.protocol import State

import snapper.infrastructure.exchanges.kraken_futures_private_receive as receive
import snapper.infrastructure.exchanges.private_control_decoder as decoder
from snapper.infrastructure.exchanges.execution_wire_receipts import PrivateWireMessage
from snapper.infrastructure.exchanges.execution_wire_receipts import (
    decode_private_execution_receipt,
)
from snapper.infrastructure.exchanges.execution_wire_receipts import is_execution_control_message

_RUN = "_ConnectFuturesWebsocket__run"
_MARKERS = ("synthetic-private-key", "synthetic-private-challenge", "synthetic-private-signature")
_ACK = (
    '{"event":"subscribed","feed":"fills","api_key":"synthetic-private-key",'
    '"original_challenge":"synthetic-private-challenge",'
    '"signed_challenge":"synthetic-private-signature"}'
)


class _OneSourceSocket:
    """Deliver one exact synthetic private payload to the pinned SDK receive branch."""

    state: State = State.OPEN

    def __init__(self, source: str | bytes) -> None:
        """Keep source lifetime local without a transport or log side channel."""
        self.source = source

    async def recv(self) -> str | bytes:
        """Return only the synthetic original bytes selected by this test."""
        return self.source


def _restore_private_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep real codec/run/registry state isolated without substituting private decode behavior."""
    monkeypatch.setattr(ConnectFuturesWebsocket, _RUN, getattr(ConnectFuturesWebsocket, _RUN))
    monkeypatch.setattr(futures_websocket, "json", futures_websocket.json)
    monkeypatch.setattr(receive._INSTALLED, "delegate", receive._INSTALLED.delegate)
    monkeypatch.setattr(receive, "_OWNED", WeakKeyDictionary())


async def _receive_private_source(monkeypatch: pytest.MonkeyPatch, source: str | bytes) -> object:
    """Exercise actual SDK event handling and logging rather than a direct decoder-only assertion."""
    _restore_private_boundary(monkeypatch)
    captured: list[object] = []

    async def callback(message: object) -> None:
        """Retain the candidate object only locally and stop this one physical receive loop."""
        assert receive.private_callback_owner() is client
        captured.append(message)
        connector.keep_alive = False

    client = FuturesWSClient(
        key="synthetic-key", secret="c3ludGhldGljLXNlY3JldA==", callback=callback
    )
    receive.bind_owned_futures_private_callback(client, callback)
    connector = client._conn
    socket = _OneSourceSocket(source)

    @asynccontextmanager
    async def connect(*args: object, **kwargs: object) -> AsyncIterator[_OneSourceSocket]:
        """Use the pinned SDK's actual receive method without opening an external socket."""
        assert args[0] == "wss://futures.kraken.com/ws/v1"
        try:
            yield socket
        finally:
            socket.state = State.CLOSED

    monkeypatch.setattr(futures_websocket, "connect", connect)
    run = cast(Callable[[asyncio.Event], Awaitable[None]], getattr(connector, _RUN))
    try:
        await run(asyncio.Event())
        assert len(captured) == 1
        return captured[0]
    finally:
        connector.keep_alive = False
        await client.close()
        owned_session = getattr(client, "_FuturesClient__session", None)
        if isinstance(owned_session, Session):
            owned_session.close()


@pytest.mark.parametrize(
    "source",
    (
        '{"event":"subscribed","api_key":"synthetic-private-key",',
        _ACK.replace('"api_key":', '"api_key":"hidden-overwritten-key","api_key":'),
        '{"feed":"fills","hidden":{"api_key":"synthetic-private-key"},"hidden":null}',
        '{"event":"challenge","message":{"api_key":"synthetic-private-key"}}',
        b'{"event":"subscribed","api_key":"synthetic-private-key",\xff}',
    ),
)
async def test_actual_sdk_malformed_auth_never_reaches_raw_source_logging(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, source: str | bytes
) -> None:
    """Malformed or ambiguous auth cannot enter the SDK branch that logs its original data.

    Given: Actual private SDK input containing synthetic key or challenge markers,
    When: The owned decoder handles malformed, duplicate, nested or invalid-UTF8 input,
    Then: The SDK forwards only a sanitized control and logs no original private marker.
    """
    caplog.set_level(logging.DEBUG, logger="kraken.futures.websocket")
    message = await _receive_private_source(monkeypatch, source)

    assert isinstance(message, decoder.TransientPrivateControl)
    assert message.failure_reason is not None and is_execution_control_message(message)
    assert not isinstance(message, PrivateWireMessage)
    assert all(marker not in caplog.text + repr(message) + str(message) for marker in _MARKERS)
    assert "hidden-overwritten-key" not in caplog.text


async def test_actual_sdk_ack_uses_mapping_without_a_serializable_secret_body(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The actual subscription branch needs mapping access but does not need a retained auth body.

    Given: A documented successful ACK with synthetic sensitive echo fields,
    When: The actual SDK appends its private subscription and forwards the transient mapping,
    Then: Repr, ordinary serialization and logs cannot expose those echoes as a raw receipt.
    """
    caplog.set_level(logging.DEBUG, logger="kraken.futures.websocket")
    message = await _receive_private_source(monkeypatch, _ACK)

    assert isinstance(message, decoder.TransientPrivateControl)
    assert message.failure_reason is None and message["event"] == "subscribed"
    assert len(message) == 5
    assert set(message) == {
        "event",
        "feed",
        "api_key",
        "original_challenge",
        "signed_challenge",
    }
    errors: list[str] = []
    for serialize in (json.dumps, pickle.dumps):
        with pytest.raises(TypeError) as caught:
            serialize(message)
        errors.append("".join(traceback.format_exception(caught.value)))
    actual_text = caplog.text + repr(message) + str(message) + "".join(errors)
    assert all(marker not in actual_text for marker in _MARKERS)


@pytest.mark.parametrize(
    ("source", "expected_reason"),
    (
        ("null", "private_object_required"),
        ('{"event":"challenge","message":17}', "challenge_text_invalid"),
        (r'{"event":"challenge","message":"\ud800"}', "challenge_text_invalid"),
        ('{"event":"error","message":"a","message":"b"}', "control_json_ambiguous"),
        ('{"event":"error","count":NaN}', "control_json_ambiguous"),
        (_ACK.replace('"fills"', "null", 1), "control_feed_unclassified"),
        (_ACK.replace('"fills"', '"unrecognized"', 1), "control_feed_unclassified"),
        (
            '{"event":"subscribed","feed":"fills","api_key":"synthetic-private-key"}',
            "control_auth_echo_unavailable",
        ),
        ('{"event":"info","message":"synthetic-private-key"}', None),
        ('{"event":"invented","api_key":"synthetic-private-key"}', "control_unclassified"),
        ('{"feed":"heartbeat","time":1}', None),
        ('{"feed":"unrecognized","message":"synthetic-private-key"}', "private_feed_unclassified"),
        ('{"feed":17,"message":"synthetic-private-key"}', "private_feed_unclassified"),
    ),
)
async def test_actual_sdk_refuses_incomplete_controls_and_keeps_heartbeat_nonfinancial(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    source: str,
    expected_reason: str | None,
) -> None:
    """Control admission handles malformed text, unknown feeds and ordinary nonfinancial events.

    Given: Actual private SDK input with incomplete ACKs, invalid challenges or nonfinancial envelopes,
    When: The real receive loop handles the candidate privacy projection,
    Then: It forwards only the expected safe classification with no raw body or private marker.
    """
    caplog.set_level(logging.DEBUG, logger="kraken.futures.websocket")
    message = await _receive_private_source(monkeypatch, source)

    assert isinstance(message, decoder.TransientPrivateControl)
    assert message.failure_reason == expected_reason
    assert set(message).issubset({"event", "message", "feed"})
    assert not isinstance(message, PrivateWireMessage)
    assert all(marker not in caplog.text + repr(message) + str(message) for marker in _MARKERS)


async def test_actual_sdk_parser_exception_is_sanitized_without_assuming_a_depth_limit(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Parser exception normalization is tested directly without a platform-dependent nesting claim.

    Given: A decoder inspection exception carrying synthetic private source material,
    When: The actual SDK receives the original private frame through that failing boundary,
    Then: Only a fixed sanitized control reaches the callback and no private traceback is logged.
    """

    def fail(source: str | bytes | bytearray, inspection: object) -> object:
        """Raise a representative parser boundary failure without forcing a global recursion limit."""
        raise RecursionError("synthetic-private-key synthetic-private-challenge")

    monkeypatch.setattr(decoder, "_inspect_document", fail)
    caplog.set_level(logging.DEBUG, logger="kraken.futures.websocket")
    message = await _receive_private_source(monkeypatch, _ACK)

    assert isinstance(message, decoder.TransientPrivateControl)
    assert message.failure_reason == "private_json_invalid"
    assert all(marker not in caplog.text + repr(message) for marker in _MARKERS)


@pytest.mark.parametrize(
    "source",
    (
        '{"feed":"fills","fills":[{"qty":1,"qty":2}]}',
        '{"feed":"fills","fills":[{"qty":NaN}]}',
    ),
)
async def test_safely_classified_malformed_financial_bytes_remain_available_for_a_hold(
    monkeypatch: pytest.MonkeyPatch, source: str
) -> None:
    """Financial syntax extensions are preserved before strict interpretation refuses them.

    Given: A known financial feed with no authentication fields and ambiguous numeric material,
    When: It passes through the actual SDK owned decoder and receive callback,
    Then: Its exact original bytes survive while strict financial interpretation refuses them.
    """
    message = await _receive_private_source(monkeypatch, source)

    assert isinstance(message, PrivateWireMessage)
    assert message.wire_bytes == source.encode()
    with pytest.raises(ValueError):
        decode_private_execution_receipt(message.wire_bytes)
