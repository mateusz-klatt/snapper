"""Physical receive ownership survives overlap cleanup without leaking into unrelated decoders."""

import asyncio
from types import SimpleNamespace
from typing import cast

import kraken.futures.websocket as futures_websocket
import pytest
from kraken.futures import FuturesWSClient
from kraken.futures.websocket import ConnectFuturesWebsocket
from kraken.utils.utils import WSState
from websockets.protocol import State

from snapper.infrastructure.exchanges.execution_wire_receipts import PrivateWireMessage
from snapper.infrastructure.exchanges.execution_wire_receipts import (
    install_private_execution_wire_capture,
)
from snapper.infrastructure.exchanges.kraken_futures_balance_epoch import (
    active_futures_connection_epoch,
)
from snapper.infrastructure.exchanges.kraken_futures_balance_epoch import (
    install_futures_balance_epoch_capture,
)
from snapper.infrastructure.exchanges.kraken_futures_balance_epoch import (
    private_futures_message_epoch,
)
from tests.infrastructure.exchanges.implementations.test_native_balance_capture_callbacks import (
    _Peer,
)
from tests.infrastructure.exchanges.implementations.test_native_balance_capture_callbacks import (
    _Socket,
)
from tests.infrastructure.exchanges.implementations.test_native_balance_capture_callbacks import (
    _wire,
)

_RUN = "_ConnectFuturesWebsocket__run"
_WAIT = 5.0


@pytest.fixture(autouse=True)
def wire_boundary() -> None:
    """Install the same idempotent boundaries used by the actual application client constructor."""
    install_private_execution_wire_capture()
    install_futures_balance_epoch_capture()


@pytest.mark.parametrize(
    "fault", ("missing", "not_connected", "closed_socket", "stopping", "no_run")
)
def test_actual_connection_state_alone_cannot_invent_a_running_receive_epoch(fault: str) -> None:
    """A current source proof requires the actual live receive owner, not just mutable socket fields.

    Given: Missing SDK ownership or connection flags without a currently running physical receive task,
    When: The source's active epoch is requested,
    Then: No physical source epoch is invented.
    """
    connector = ConnectFuturesWebsocket(
        client=cast(FuturesWSClient, SimpleNamespace()),
        endpoint="futures.kraken.com/ws/v1",
        callback=None,
    )
    connector.state = WSState.CONNECTED
    connector.socket = _Socket()
    if fault == "missing":
        owner: object = object()
    else:
        owner = SimpleNamespace(_conn=connector)
        if fault == "not_connected":
            connector.state = WSState.CONNECTING
        elif fault == "closed_socket":
            connector.socket.state = State.CLOSED
        elif fault == "stopping":
            connector.keep_alive = False
    assert active_futures_connection_epoch(owner) is None


async def test_late_old_receive_cleanup_cannot_erase_a_new_physical_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An old callback's delayed cancellation cannot remove a later receive loop's source epoch.

    Given: A pinned SDK receive loop blocked in an old callback while a replacement socket opens,
    When: The old callback is cancelled after the replacement owns the same connector,
    Then: The new physical epoch remains available until its own receive loop exits.
    """
    install_futures_balance_epoch_capture()
    peer = _Peer(_wire())
    monkeypatch.setattr(futures_websocket, "connect", peer.connect)
    entered = asyncio.Event()
    hold = asyncio.Event()
    old_epoch: list[str] = []

    async def callback(message: dict[str, object]) -> None:
        epoch = private_futures_message_epoch(message)
        assert epoch is not None
        old_epoch.append(epoch)
        entered.set()
        await hold.wait()

    connector = ConnectFuturesWebsocket(
        client=cast(FuturesWSClient, SimpleNamespace()),
        endpoint="futures.kraken.com/ws/v1",
        callback=callback,
    )
    owner = SimpleNamespace(_conn=connector)
    old = asyncio.create_task(getattr(connector, _RUN)(asyncio.Event()))
    newer: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(entered.wait(), _WAIT)
        await asyncio.wait_for(peer.opened.get(), _WAIT)
        newer = asyncio.create_task(getattr(connector, _RUN)(asyncio.Event()))
        await asyncio.wait_for(peer.opened.get(), _WAIT)
        active = active_futures_connection_epoch(owner)
        assert active is not None and active != old_epoch[0]
        old.cancel()
        with pytest.raises(asyncio.CancelledError):
            await old
        assert active_futures_connection_epoch(owner) == active
    finally:
        old.cancel()
        if newer is not None:
            newer.cancel()
        await asyncio.gather(old, *(() if newer is None else (newer,)), return_exceptions=True)
    assert active_futures_connection_epoch(owner) is None


async def test_inherited_receive_context_cannot_label_thread_or_event_loop_callback_decoding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only the actual SDK receive task can attach its physical epoch to a decoded private payload.

    Given: A genuine receive callback that schedules unrelated decoding with inherited context,
    When: A worker thread and a plain event-loop callback decode identical source bytes,
    Then: Both retain bytes without acquiring the original receive task's physical source identity.
    """
    install_futures_balance_epoch_capture()
    peer = _Peer(_wire())
    monkeypatch.setattr(futures_websocket, "connect", peer.connect)

    async def callback(message: dict[str, object]) -> None:
        assert private_futures_message_epoch(message) is not None
        loop = asyncio.get_running_loop()
        completed: asyncio.Future[object] = loop.create_future()

        def decode_outside_task() -> None:
            try:
                completed.set_result(futures_websocket.json.loads(_wire()))
            except BaseException as error:
                completed.set_exception(error)

        loop.call_soon(decode_outside_task)
        unrelated = await asyncio.wait_for(completed, _WAIT)
        threaded = await asyncio.to_thread(futures_websocket.json.loads, _wire())
        for decoded in (unrelated, threaded):
            assert isinstance(decoded, PrivateWireMessage)
            assert private_futures_message_epoch(decoded) is None
            assert decoded.wire_bytes == _wire().encode()
        connector.keep_alive = False

    connector = ConnectFuturesWebsocket(
        client=cast(FuturesWSClient, SimpleNamespace()),
        endpoint="futures.kraken.com/ws/v1",
        callback=callback,
    )
    await getattr(connector, _RUN)(asyncio.Event())
