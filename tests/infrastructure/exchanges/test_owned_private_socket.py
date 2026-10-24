"""Unrun actual-SDK discovery of a one-shot socket handoff and exact encoded send boundary.

No safe key proof is authored here. The synthetic key guard is explicitly an
instrumentation probe, not an account-ownership producer or native permit.
"""

import asyncio
import json
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any
from typing import cast
from uuid import uuid4
from weakref import WeakKeyDictionary

import kraken.futures.websocket as futures_websocket
import pytest
from kraken.futures import FuturesWSClient
from kraken.futures.websocket import ConnectFuturesWebsocket
from requests import Session
from websockets.protocol import State

import snapper.infrastructure.exchanges.kraken_futures_private_receive as receive
from snapper.infrastructure.exchanges.execution_wire_receipts import PrivateWireMessage
from snapper.infrastructure.exchanges.owned_private_socket import ActualReceiveCell
from snapper.infrastructure.exchanges.owned_private_socket import OriginalReceive
from snapper.infrastructure.exchanges.owned_private_socket import OwnedPrivateSocket
from snapper.infrastructure.exchanges.owned_private_socket import PrivateSocketBoundaryError

_RUN = "_ConnectFuturesWebsocket__run"
_EXPECTED_KEY = "SYNTHETIC_PREPARED_KEY"
_CHANGED_KEY = "SYNTHETIC_CHANGED_DURING_CHALLENGE"
_CHALLENGE = "SYNTHETIC_PRIVATE_CHALLENGE"
_SECRET = "c3ludGhldGljLXNlY3JldA=="
_FILL = '{"feed":"fills","account":"DemoUser","fills":[{"qty":0.100000000000000001}]}'


class _PhysicalSocket:
    """Local provider replies to the actual SDK's encoded challenge/private subscribe requests."""

    def __init__(self, client: FuturesWSClient, mutate: bool) -> None:
        """Retain a real signing client while every auth value remains synthetic and local."""
        self.client = client
        self.mutate = mutate
        self.state = State.OPEN
        self.incoming: asyncio.Queue[str] = asyncio.Queue()
        self.actual_events: list[str] = []
        self.recv_task_identities: list[int] = []

    async def recv(self) -> str:
        """Deliver the exact original object while recording only the actual receive task identity."""
        self.recv_task_identities.append(id(asyncio.current_task()))
        return await self.incoming.get()

    async def send(self, source: str) -> None:
        """Reply only after the actual physical send received the actual encoded SDK source."""
        value: object = json.loads(source)
        assert isinstance(value, dict)
        event = value.get("event")
        assert isinstance(event, str)
        self.actual_events.append(event)
        assert value["api_key"] == _EXPECTED_KEY
        if event == "challenge":
            if self.mutate:
                self.client._key = _CHANGED_KEY
            await self.incoming.put(json.dumps({"event": "challenge", "message": _CHALLENGE}))
            return
        assert event == "subscribe" and value["feed"] == "fills"
        assert value["signed_challenge"] == self.client.get_sign_challenge(_CHALLENGE)
        acknowledgement = {
            "event": "subscribed",
            "feed": "fills",
            "api_key": value["api_key"],
            "original_challenge": value["original_challenge"],
            "signed_challenge": value["signed_challenge"],
        }
        await self.incoming.put(json.dumps(acknowledgement))
        await self.incoming.put(_FILL)

    async def close(self) -> None:
        """Close the same local physical socket used by the actual run."""
        self.state = State.CLOSED


class _Scenario:
    """Own actual SDK resources and observable public facts from one local physical run."""

    def __init__(self, mutate: bool) -> None:
        """Bind a real private subscriber without authoring any account-ownership proof."""
        self.financial_receipts: list[OriginalReceive] = []
        self.cells: list[ActualReceiveCell] = []
        self.guarded_events: list[str] = []
        self.callbacks: list[Mapping[str, object]] = []
        self.task: asyncio.Task[None] | None = None
        self.client = FuturesWSClient(key=_EXPECTED_KEY, secret=_SECRET, callback=self.callback)
        receive.bind_owned_futures_private_callback(self.client, self.callback)
        self.physical = _PhysicalSocket(self.client, mutate)

    async def callback(self, message: Mapping[str, object]) -> None:
        """Keep the actual financial frame local and stop this exact physical receive loop."""
        self.callbacks.append(message)
        if isinstance(message, PrivateWireMessage):
            self.client._conn.keep_alive = False

    def before_send(self, source: str, encoded_at: datetime) -> None:
        """Observe actual encoded key after waits; this synthetic expectation is not a key proof."""
        assert encoded_at.utcoffset() is not None
        document: object = json.loads(source)
        assert isinstance(document, dict)
        if document.get("api_key") != _EXPECTED_KEY:
            raise ValueError("synthetic_prepared_key_mismatch")
        event = document.get("event")
        assert isinstance(event, str)
        self.guarded_events.append(event)

    @asynccontextmanager
    async def connect(self, *args: object, **kwargs: object) -> AsyncIterator[OwnedPrivateSocket]:
        """Construct the cell inside the actual SDK run without assuming await scheduling geometry."""
        assert args[0] == "wss://futures.kraken.com/ws/v1"
        actual_parent = asyncio.current_task()
        assert actual_parent is not None
        cell = ActualReceiveCell(id(actual_parent))
        self.cells.append(cell)
        proxy = OwnedPrivateSocket(self.physical, cell, lambda: str(uuid4()), self.before_send)
        try:
            yield proxy
        finally:
            await proxy.close()

    async def start(self) -> asyncio.Task[None]:
        """Start the actual SDK receive method and await its physical connection readiness."""
        connected = asyncio.Event()
        run = cast(Callable[[asyncio.Event], Awaitable[None]], getattr(self.client._conn, _RUN))
        self.task = asyncio.create_task(run(connected))
        await connected.wait()
        return self.task

    async def close(self) -> None:
        """Cancel/reap only the owned local run and close every actual SDK session."""
        self.client._conn.keep_alive = False
        if self.task is not None:
            if not self.task.done():
                self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        await self.client.close()
        owned_session = getattr(self.client, "_FuturesClient__session", None)
        if isinstance(owned_session, Session):
            owned_session.close()


def _patch_codec(scenario: _Scenario, monkeypatch: pytest.MonkeyPatch) -> None:
    """Measure handoff before the existing decoder without fabricating an original receive."""
    delegate = futures_websocket.json

    class _ObservedCodec(receive._OwnedPrivateJsonCodec):
        """Measure one-shot actual receipt handoff before the existing real private decoder."""

        @staticmethod
        def loads(source: str | bytes | bytearray, **kwargs: Any) -> object:
            """Use no nominal ContextVar as a replacement for actual socket receive."""
            actual = scenario.cells[-1].consume(source)
            assert actual is not None
            assert scenario.cells[-1].consume(source) is None
            result = delegate.loads(source, **kwargs)
            if isinstance(result, PrivateWireMessage):
                assert actual.matches_financial_wire(result.wire_bytes)
                scenario.financial_receipts.append(actual)
            return result

    monkeypatch.setattr(futures_websocket, "json", _ObservedCodec())


@asynccontextmanager
async def _scenario(monkeypatch: pytest.MonkeyPatch, mutate: bool) -> AsyncIterator[_Scenario]:
    """Restore actual installed boundaries and guarantee resource cleanup after either outcome."""
    monkeypatch.setattr(ConnectFuturesWebsocket, _RUN, getattr(ConnectFuturesWebsocket, _RUN))
    monkeypatch.setattr(futures_websocket, "json", futures_websocket.json)
    monkeypatch.setattr(receive._INSTALLED, "delegate", receive._INSTALLED.delegate)
    monkeypatch.setattr(receive, "_OWNED", WeakKeyDictionary())
    scenario = _Scenario(mutate)
    _patch_codec(scenario, monkeypatch)
    monkeypatch.setattr(futures_websocket, "connect", scenario.connect)
    try:
        yield scenario
    finally:
        await scenario.close()


@pytest.mark.parametrize("mutate", (False, True))
async def test_actual_sdk_socket_handoff_survives_wait_for_and_refuses_changed_encoded_key(
    monkeypatch: pytest.MonkeyPatch, mutate: bool
) -> None:
    """Actual socket operations supply geometry which inherited decoder context cannot prove.

    Given: A real SDK subscriber whose actual socket is wrapped by the candidate adapter,
    When: Its actual challenge wait preserves or mutates the key before private encoding,
    Then: Genuine recv is consumed once in the parent and changed key never reaches physical send.
    """
    async with _scenario(monkeypatch, mutate) as scenario:
        task = await scenario.start()
        if mutate:
            with pytest.raises(PrivateSocketBoundaryError, match="private_socket_send_refused"):
                await scenario.client.subscribe(feed="fills")
            assert scenario.physical.actual_events == scenario.guarded_events == ["challenge"]
            assert not scenario.financial_receipts and not scenario.callbacks
        else:
            await scenario.client.subscribe(feed="fills")
            await task
            assert (
                scenario.physical.actual_events
                == scenario.guarded_events
                == ["challenge", "subscribe"]
            )
            assert len(scenario.financial_receipts) == 1 and len(scenario.callbacks) == 2
            assert scenario.physical.recv_task_identities == [id(task)] * 3
            assert scenario.cells[0].consume(_FILL) is None
            assert isinstance(scenario.callbacks[1], PrivateWireMessage)
            assert scenario.callbacks[1].wire_bytes == _FILL.encode()
