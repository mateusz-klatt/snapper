"""Physical SDK reconnect attempts cannot relabel old native-balance source messages."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import cast
from uuid import UUID

import kraken.futures.websocket as futures_websocket
import pytest
from kraken.futures import FuturesWSClient
from kraken.futures.websocket import ConnectFuturesWebsocket

from snapper.infrastructure.exchanges.execution_wire_receipts import PrivateExecutionJsonCodec
from snapper.infrastructure.exchanges.execution_wire_receipts import PrivateWireMessage
from snapper.infrastructure.exchanges.kraken_futures_balance_epoch import (
    install_futures_balance_epoch_capture,
)
from snapper.infrastructure.exchanges.kraken_futures_balance_epoch import (
    private_futures_message_epoch,
)

_RUN = "_ConnectFuturesWebsocket__run"
_SOURCE = (
    '{"feed":"balances_snapshot","account":"venue-account-1",'
    '"holding":{"USD":0.100000000000000001},"timestamp":1792742400000,"seq":0}'
)


@pytest.fixture
def epoch_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    """Restore both real SDK boundaries after every isolated installation test."""
    original = getattr(ConnectFuturesWebsocket, _RUN)
    monkeypatch.setattr(ConnectFuturesWebsocket, _RUN, original)
    monkeypatch.setattr(futures_websocket, "json", futures_websocket.json)
    if not getattr(original, "_snapper_native_balance_epoch_capture", False):
        futures_websocket.__dict__["json"] = PrivateExecutionJsonCodec()
    install_futures_balance_epoch_capture()


class _Socket:
    """Deliver one original payload through the actual pinned SDK receive method."""

    async def recv(self) -> str:
        """Supply the same source bytes across independently reconnected physical sockets.

        Returns:
            Genuine test wire payload, without local transport metadata in provider JSON.
        """
        return _SOURCE


async def test_same_sdk_connector_reconnect_keeps_delayed_old_message_epoch(
    epoch_boundary: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two physical runs on one SDK object preserve independent message-bound epochs.

    Given: One long-lived SDK connector opening two physical receive loops,
    When: A retained first message is examined after the second socket delivered a frame,
    Then: Both epochs differ and the delayed first message keeps its original identity.
    """
    captured: list[dict[str, object]] = []

    async def unrelated_decode() -> None:
        message = futures_websocket.json.loads(_SOURCE)
        assert isinstance(message, PrivateWireMessage)
        assert private_futures_message_epoch(message) is None

    async def callback(message: dict[str, object]) -> None:
        captured.append(message)
        await asyncio.create_task(unrelated_decode())
        connector.keep_alive = False

    @asynccontextmanager
    async def connect(*args: object, **kwargs: object) -> AsyncIterator[_Socket]:
        assert args[0] == "wss://futures.kraken.com/ws/v1"
        yield _Socket()

    monkeypatch.setattr(futures_websocket, "connect", connect)
    connector = ConnectFuturesWebsocket(
        client=cast(FuturesWSClient, SimpleNamespace()),
        endpoint="futures.kraken.com/ws/v1",
        callback=callback,
    )
    run = getattr(connector, _RUN)
    await run(asyncio.Event())
    first_epoch = private_futures_message_epoch(captured[0])
    connector.keep_alive = True
    await run(asyncio.Event())
    second_epoch = private_futures_message_epoch(captured[1])
    assert first_epoch is not None and second_epoch is not None
    assert first_epoch != second_epoch
    assert private_futures_message_epoch(captured[0]) == first_epoch
    for message in captured:
        assert isinstance(message, PrivateWireMessage)
        assert message.wire_bytes == _SOURCE.encode()
        assert set(message) == {"feed", "account", "holding", "timestamp", "seq"}
    outside = futures_websocket.json.loads(_SOURCE)
    assert isinstance(outside, PrivateWireMessage)
    assert private_futures_message_epoch(outside) is None


@pytest.mark.parametrize("failure", (RuntimeError, asyncio.CancelledError))
async def test_failed_or_cancelled_callback_resets_epoch_before_same_task_continues(
    epoch_boundary: None, monkeypatch: pytest.MonkeyPatch, failure: type[BaseException]
) -> None:
    """A callback failure cannot leak transport authority into later unrelated JSON decoding.

    Given: A real SDK receive loop whose application callback fails,
    When: The caller catches the failure and decodes another message in the same task,
    Then: The later message retains bytes but has no physical epoch proof.
    """

    async def callback(message: dict[str, object]) -> None:
        assert private_futures_message_epoch(message) is not None
        raise failure("isolated callback failure")

    @asynccontextmanager
    async def connect(*args: object, **kwargs: object) -> AsyncIterator[_Socket]:
        yield _Socket()

    monkeypatch.setattr(futures_websocket, "connect", connect)
    connector = ConnectFuturesWebsocket(
        client=cast(FuturesWSClient, SimpleNamespace()),
        endpoint="futures.kraken.com/ws/v1",
        callback=callback,
    )
    with pytest.raises(failure, match="isolated callback failure"):
        await getattr(connector, _RUN)(asyncio.Event())
    outside = futures_websocket.json.loads(_SOURCE)
    assert isinstance(outside, PrivateWireMessage)
    assert private_futures_message_epoch(outside) is None
    public = futures_websocket.json.loads('{"feed":"ticker","price":1.25}')
    assert isinstance(public, dict)
    assert not isinstance(public, PrivateWireMessage)
    assert private_futures_message_epoch(public) is None


@pytest.mark.parametrize("value", (None, 17, "invalid", "A987FBC9-4BED-4078-8F07-9141BA07C9F3"))
def test_invalid_or_provider_supplied_epoch_cannot_become_transport_proof(value: object) -> None:
    """Only a canonical internal attribute on a genuine boundary object can identify a run.

    Given: A provider mapping or malformed synthetic boundary metadata,
    When: The application reads its physical source epoch,
    Then: It receives no proof while genuine original bytes remain available.
    """
    assert private_futures_message_epoch({"_snapper_futures_physical_epoch": value}) is None
    message = PrivateWireMessage({}, b"{}")
    message.__dict__["_snapper_futures_physical_epoch"] = value
    assert private_futures_message_epoch(message) is None
    assert message.wire_bytes == b"{}"


def test_genuine_canonical_epoch_remains_separate_from_provider_mapping() -> None:
    """A trusted boundary attribute is read without injecting it into provider fields.

    Given: Genuine retained bytes with locally attached canonical receive metadata,
    When: The application projects the physical epoch,
    Then: The UUID is preserved and the provider object remains unchanged.
    """
    epoch = str(UUID(int=1))
    message = PrivateWireMessage({}, b"{}")
    message.__dict__["_snapper_futures_physical_epoch"] = epoch
    assert private_futures_message_epoch(message) == epoch
    assert message == {}


def test_repeated_installation_preserves_same_physical_boundary(epoch_boundary: None) -> None:
    """Repeated client construction must not stack physical receive wrappers.

    Given: The supported epoch boundary is already installed,
    When: A second client requests the same installation,
    Then: The actual run method and decoder objects remain identical.
    """
    run = getattr(ConnectFuturesWebsocket, _RUN)
    codec = futures_websocket.json
    install_futures_balance_epoch_capture()
    assert getattr(ConnectFuturesWebsocket, _RUN) is run
    assert futures_websocket.json is codec


@pytest.mark.parametrize("broken", ("codec", "run", "replaced_codec"))
def test_unsupported_installed_boundary_refuses_without_claiming_epoch(
    epoch_boundary: None, monkeypatch: pytest.MonkeyPatch, broken: str
) -> None:
    """Unexpected SDK or decoder replacement cannot silently retain old proof claims.

    Given: Another component replaced the expected SDK run or module-local JSON boundary,
    When: Native balance instrumentation attempts installation,
    Then: It refuses before any source can acquire an unsupported epoch claim.
    """
    if broken == "run":
        monkeypatch.setattr(ConnectFuturesWebsocket, _RUN, None)
    else:
        monkeypatch.setattr(
            futures_websocket,
            "json",
            PrivateExecutionJsonCodec() if broken == "replaced_codec" else object(),
        )
    with pytest.raises(RuntimeError, match="boundary is unsupported|codec was replaced"):
        install_futures_balance_epoch_capture()
