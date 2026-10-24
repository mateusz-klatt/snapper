"""Actual SDK downstream challenge failure and original-body logging counterexample.

These cases distinguish a JSON decode exception from a later SDK
control-handler exception without changing the actual receive implementation.
"""

import asyncio
import json
import logging
import traceback
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
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

_RUN = "_ConnectFuturesWebsocket__run"
_BACKOFF = "_ConnectFuturesWebsocket__get_reconnect_wait"
_PRIVATE_KEY = "SYNTHETIC_PRIVATE_API_KEY_MARKER"
_PRIVATE_SECRET = "SYNTHETIC_PRIVATE_SECRET_MARKERA"
_PRIVATE_SIGNATURE = "SYNTHETIC_PRIVATE_SIGNATURE_MARKER"
_PRIVATE_CHALLENGE = "SYNTHETIC_PRIVATE_CHALLENGE_MARKER"


def _source() -> str:
    """Keep synthetic auth-bearing source separate from the public exception assertions."""
    return json.dumps(
        {
            "event": "challenge",
            "message": _PRIVATE_CHALLENGE,
            "api_key": _PRIVATE_KEY,
            "signed_challenge": _PRIVATE_SIGNATURE,
        }
    )


class _Socket:
    """Deliver an actual private challenge whose local SDK signing key is deliberately malformed."""

    state: State = State.OPEN

    async def recv(self) -> str:
        """Return the same source; actual malformed-secret failure must terminate the first attempt."""
        return _source()


class _TaskFailureBarrier(logging.Handler):
    """Observe the real supervisor warning without replacing its logger or exception handler."""

    def __init__(self, observed: asyncio.Event) -> None:
        """Retain the event that the actual task-exception warning will release."""
        super().__init__()
        self.observed = observed

    def emit(self, record: logging.LogRecord) -> None:
        """Wake the test only after the real SDK task-exception warning was emitted."""
        if "got an exception" in record.getMessage():
            self.observed.set()


def _restore_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    """Restore actual run/codec/registry state after the isolated SDK failure test."""
    monkeypatch.setattr(ConnectFuturesWebsocket, _RUN, getattr(ConnectFuturesWebsocket, _RUN))
    monkeypatch.setattr(futures_websocket, "json", futures_websocket.json)
    monkeypatch.setattr(receive._INSTALLED, "delegate", receive._INSTALLED.delegate)
    monkeypatch.setattr(receive, "_OWNED", WeakKeyDictionary())


@asynccontextmanager
async def _client(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[FuturesWSClient]:
    """Own real SDK sessions while the source/secret markers remain entirely synthetic."""
    _restore_boundary(monkeypatch)

    async def callback(message: Mapping[str, object]) -> None:
        """Refuse unexpected forwarding of the deliberately failing private challenge."""
        assert "python-kraken-sdk" in message

    client = FuturesWSClient(key=_PRIVATE_KEY, secret=_PRIVATE_SECRET, callback=callback)
    receive.bind_owned_futures_private_callback(client, callback)

    @asynccontextmanager
    async def connect(*args: object, **kwargs: object) -> AsyncIterator[_Socket]:
        """Run the actual pinned SDK receive branch without a venue or network connection."""
        assert args[0] == "wss://futures.kraken.com/ws/v1"
        yield _Socket()

    monkeypatch.setattr(futures_websocket, "connect", connect)
    try:
        yield client
    finally:
        client._conn.keep_alive = False
        await client.close()
        owned_session = getattr(client, "_FuturesClient__session", None)
        if isinstance(owned_session, Session):
            owned_session.close()


async def test_downstream_signing_value_error_bypasses_only_the_json_raw_data_logger(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Actual handler errors belong to the try-else suite rather than the JSON-decode except branch.

    Given: An auth-bearing actual SDK challenge and a malformed synthetic base64 signing secret,
    When: The real challenge handler raises after sanitized JSON decoding has succeeded,
    Then: Its propagated traceback contains no auth markers and the raw-data warning is not used.
    """
    caplog.set_level(logging.DEBUG, logger="kraken.futures.websocket")
    async with _client(monkeypatch) as client:
        run = cast(Callable[[asyncio.Event], Awaitable[None]], getattr(client._conn, _RUN))
        with pytest.raises(ValueError) as caught:
            await run(asyncio.Event())
        rendered = caplog.text + "".join(traceback.format_exception(caught.value))
        assert "base64" in str(caught.value).lower()
        for marker in (_PRIVATE_KEY, _PRIVATE_SECRET, _PRIVATE_SIGNATURE, _PRIVATE_CHALLENGE):
            assert marker not in rendered
        assert _source() not in caplog.text
        assert receive._current_scope() is None and receive.private_callback_owner() is None


async def test_actual_reconnect_task_logging_does_not_expose_downstream_auth_source(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """The actual supervisor's task exception logs also require a measured privacy check.

    Given: The same real malformed-secret challenge failure under the SDK reconnect supervisor,
    When: The actual task-exception warning is observed before the real connector is stopped,
    Then: The supervisor exits with no live task and its logs omit all private source markers.
    """
    caplog.set_level(logging.DEBUG, logger="kraken.futures.websocket")
    async with _client(monkeypatch) as client:
        monkeypatch.setattr(client._conn, _BACKOFF, lambda attempts: 0.0)
        observed = asyncio.Event()
        barrier = _TaskFailureBarrier(observed)
        actual_logger = logging.getLogger("kraken.futures.websocket")
        assert futures_websocket.LOG is actual_logger
        monkeypatch.setattr(actual_logger, "disabled", False)
        actual_logger.addHandler(barrier)
        try:
            await client._conn.start()
            supervisor = cast(asyncio.Task[None], client._conn.task)
            observation = asyncio.create_task(observed.wait())
            try:
                await asyncio.wait(
                    (supervisor, observation),
                    timeout=1.0,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                failure = (
                    supervisor.exception()
                    if supervisor.done() and not supervisor.cancelled()
                    else None
                )
                assert observed.is_set(), (
                    "The actual SDK supervisor did not emit its task-exception warning: "
                    f"done={supervisor.done()}, cancelled={supervisor.cancelled()}, "
                    f"state={client._conn.state!r}, exception_occur={client.exception_occur}, "
                    f"exception={failure!r}; captured_logs={caplog.text!r}"
                )
            finally:
                observation.cancel()
                await asyncio.gather(observation, return_exceptions=True)
        finally:
            await client._conn.stop()
            actual_logger.removeHandler(barrier)
        assert client._conn.task.done() and not client._conn.task.cancelled()
        assert not client._conn.keep_alive and not client.exception_occur
        assert "got an exception" in caplog.text and "base64" in caplog.text.lower()
        for marker in (_PRIVATE_KEY, _PRIVATE_SECRET, _PRIVATE_SIGNATURE, _PRIVATE_CHALLENGE):
            assert marker not in caplog.text
        assert _source() not in caplog.text
