"""Actual private SDK balance callbacks retain source bytes without reviving disconnected profiles."""

import asyncio
import base64
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import kraken.futures.websocket as futures_websocket
import pytest
from kraken.futures.websocket import ConnectFuturesWebsocket
from websockets.datastructures import Headers
from websockets.exceptions import ConnectionClosedOK
from websockets.protocol import State

from snapper.application.trade.execution_observation_ingestion import (
    DurableExecutionObservationSink,
)
from snapper.application.trade.native_balance_source import get_bound_native_balance_profiles
from snapper.config.credentials import CredentialVersion
from snapper.core.execution_route_authority import client_selection_from_authority
from snapper.data.repository import SQLAlchemyRepository
from snapper.infrastructure.exchanges.contracts import NativeBalanceEntry
from snapper.infrastructure.exchanges.contracts import PrivateExecutionObservation
from snapper.infrastructure.exchanges.execution_wire_receipts import PrivateExecutionJsonCodec
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.kraken_futures_balance_epoch import (
    active_futures_connection_epoch,
)
from snapper.infrastructure.exchanges.kraken_futures_balance_epoch import (
    private_futures_message_epoch,
)
from snapper.messaging.infrastructure.publisher import SequenceTracker
from tests.helpers.unit_admission import seed_admission_scenario

_NOW = datetime(2026, 10, 23, 10, tzinfo=UTC)
_WAIT = 5.0


def _wire(quantity: str = "0.100000000000000001", *, snapshot: bool = True) -> str:
    """Use a genuine JSON number whose exact digits cannot survive a float round trip."""
    return (
        '{"feed":"'
        + ("balances_snapshot" if snapshot else "balances")
        + '","account":"actual-wire-account","timestamp":179274960000'
        + ("0" if snapshot else "1")
        + ',"seq":'
        + ("0" if snapshot else "1")
        + ',"holding":{"USD":'
        + quantity
        + "}}"
    )


def _zero_backoff(connector: ConnectFuturesWebsocket, attempts: int) -> float:
    """Keep only the real SDK's network retry sleep out of this isolated transport fixture."""
    assert attempts >= 0
    return 0.0


class _Socket:
    """A local physical connection with an explicit handshake, state and receive queue."""

    def __init__(self, initial: str | None = None) -> None:
        """Start one distinct open socket and optionally enqueue its initial source snapshot."""
        self.state = State.OPEN
        self.request = SimpleNamespace(
            path="/ws/v1", headers=Headers({"Host": "futures.kraken.com"})
        )
        self.messages: asyncio.Queue[str | None] = asyncio.Queue()
        self.sent: list[str] = []
        if initial is not None:
            self.messages.put_nowait(initial)

    async def recv(self) -> str:
        """Deliver an original payload or a real SDK-recognized closed-connection exception.

        Returns:
            Exact queued JSON text.
        """
        value = await self.messages.get()
        if value is None:
            raise ConnectionClosedOK(None, None)
        return value

    async def send(self, message: str) -> None:
        """Retain only synthetic SDK control traffic inside the local test fixture."""
        self.sent.append(message)

    async def close(self) -> None:
        """Close this physical socket and release an outstanding receive without opening a peer."""
        if self.state == State.OPEN:
            self.state = State.CLOSED
            self.messages.put_nowait(None)


class _Peer:
    """Supply distinct physical sockets to the actual SDK's internal reconnect loop."""

    def __init__(self, initial: str | None) -> None:
        """Send a snapshot only on the first connection; later sockets require an explicit send."""
        self.initial = initial
        self.sockets: list[_Socket] = []
        self.opened: asyncio.Queue[_Socket] = asyncio.Queue()

    @asynccontextmanager
    async def connect(self, *args: object, **kwargs: object) -> AsyncIterator[_Socket]:
        """Replace network I/O while preserving the pinned SDK's real connect/recv/callback path."""
        assert args[0] == "wss://futures.kraken.com/ws/v1"
        socket = _Socket(self.initial if not self.sockets else None)
        self.sockets.append(socket)
        self.opened.put_nowait(socket)
        try:
            yield socket
        finally:
            await socket.close()


@pytest.fixture
async def client(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[KrakenFuturesExchangeClient]:
    """Construct the actual selected client with independent qualified legacy display values."""
    actual = KrakenFuturesExchangeClient(api_key="synthetic", api_secret="c3ludGhldGlj")
    actual.bind_execution_identity(
        str(UUID(int=1)),
        CredentialVersion(
            str(UUID(int=2)), str(UUID(int=1)), "kraken_futures", "api_key_secret", _NOW
        ),
    )
    monkeypatch.setattr(
        actual,
        "read_native_balances",
        AsyncMock(return_value=[NativeBalanceEntry("legacy_only", 9.0, None, None)]),
    )
    monkeypatch.setattr(
        ConnectFuturesWebsocket, "_ConnectFuturesWebsocket__get_reconnect_wait", _zero_backoff
    )
    try:
        yield actual
    finally:
        await actual.disconnect()


def _local_peer(monkeypatch: pytest.MonkeyPatch, initial: str | None = None) -> _Peer:
    """Attach a local source to the actual private SDK without altering any source decoder."""
    peer = _Peer(initial)
    monkeypatch.setattr(futures_websocket, "connect", peer.connect)
    return peer


async def test_real_sdk_reconnect_before_snapshot_and_late_old_frame_never_restore_old_profile(
    client: KrakenFuturesExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Current profile exposure follows the physical source lifecycle, independently of new messages.

    Given: An actual SDK connection that has captured one native profile and then disconnects,
    When: It reconnects without a snapshot and later receives an old retained frame,
    Then: The old proof stays unavailable, a new snapshot starts a new epoch and old replay cannot replace it.
    """
    peer = _local_peer(monkeypatch, _wire())
    captured: list[PrivateExecutionObservation] = []
    commits: asyncio.Queue[str] = asyncio.Queue()
    messages: list[dict[str, object]] = []
    original = client._on_execution_message

    async def relay(message: dict[str, object]) -> None:
        messages.append(message)
        await original(message)

    async def sink(observation: PrivateExecutionObservation) -> str:
        captured.append(observation)
        public_id = str(UUID(int=100 + len(captured)))
        commits.put_nowait(public_id)
        return public_id

    monkeypatch.setattr(client, "_on_execution_message", relay)
    client.set_execution_observation_sink(sink)
    await client._ensure_private_ws_connected()
    first_socket = await asyncio.wait_for(peer.opened.get(), _WAIT)
    await asyncio.wait_for(commits.get(), _WAIT)
    initial = await client.read_native_balance_observation()
    assert initial.route is None
    assert initial.entries[0].currency == "legacy_only"
    proof = initial.source_profiles[0].source_proof
    assert proof.observed_at == captured[0].context.observed_at
    assert private_futures_message_epoch(messages[0]) == proof.connection_epoch_public_id
    assert base64.b64decode(captured[0].material) == _wire().encode()
    await first_socket.close()
    assert not (await client.read_native_balance_observation()).source_profiles
    second_socket = await asyncio.wait_for(peer.opened.get(), _WAIT)
    second_epoch = active_futures_connection_epoch(client._private_ws_client)
    assert second_epoch is not None and second_epoch != proof.connection_epoch_public_id
    assert not (await client.read_native_balance_observation()).source_profiles
    second_socket.messages.put_nowait(_wire("0.25"))
    await asyncio.wait_for(commits.get(), _WAIT)
    current = await client.read_native_balance_observation()
    assert current.source_profiles[0].source_proof.connection_epoch_public_id == second_epoch
    assert current.source_profiles[0].quantities[0].quantity == Decimal("0.25")
    assert current.source_profiles[0].source_proof.observed_at == captured[1].context.observed_at
    second_socket.messages.put_nowait(_wire("0.3", snapshot=False))
    await asyncio.wait_for(commits.get(), _WAIT)
    current = await client.read_native_balance_observation()
    assert current.source_profiles[0].quantities[0].quantity == Decimal("0.3")
    assert current.source_profiles[0].source_proof.tip_sequence == 1
    assert current.source_profiles[0].source_proof.observed_at == captured[2].context.observed_at
    await original(messages[0])
    assert len(captured) == 4
    repeated = await client.read_native_balance_observation()
    assert repeated.source_profiles == current.source_profiles
    await client.disconnect()
    assert not (await client.read_native_balance_observation()).source_profiles


async def test_actual_callback_receipt_is_replayed_from_real_immutable_storage(
    client: KrakenFuturesExchangeClient, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A genuine SDK message connects raw capture, cache exposure and durable exact replay.

    Given: The actual selected SDK client and an isolated real immutable receipt store,
    When: Its decoder receives an original native balance snapshot,
    Then: The exposed precise profile references committed source bytes that durable replay reproduces.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'native-callback.db'}")
    await repository.create_all()
    scenario = await seed_admission_scenario(repository, _NOW, source="kraken_futures")
    actual = client_selection_from_authority(scenario.authority)
    instance = KrakenFuturesExchangeClient(api_key="synthetic", api_secret="c3ludGhldGlj")
    assert actual.credential_public_id is not None and actual.credential_timestamp is not None
    instance.bind_execution_identity(
        actual.wallet_public_id,
        CredentialVersion(
            actual.credential_public_id,
            actual.wallet_public_id,
            actual.exchange,
            "api_key_secret",
            actual.credential_timestamp,
        ),
    )
    monkeypatch.setattr(instance, "read_native_balances", AsyncMock(return_value=[]))
    _local_peer(monkeypatch, _wire())
    committed = asyncio.Event()
    durable = DurableExecutionObservationSink(
        repository, instance.execution_client_selection, lambda: None, SequenceTracker()
    )

    async def sink(observation: PrivateExecutionObservation) -> str:
        public_id = await durable(observation)
        committed.set()
        return public_id

    instance.set_execution_observation_sink(sink)
    try:
        await instance._ensure_private_ws_connected()
        await asyncio.wait_for(committed.wait(), _WAIT)
        observation = await instance.read_native_balance_observation()
        assert observation.route is None and observation.entries == []
        proof = observation.source_profiles[0].source_proof
        assert proof.client_selection == actual
        assert observation.source_profiles[0].quantities[0].quantity == Decimal(
            "0.100000000000000001"
        )
        async with repository.session() as session:
            replayed = await get_bound_native_balance_profiles(session, proof)
        assert replayed == observation.source_profiles
    finally:
        await instance.disconnect()
        await repository.engine.dispose()


@pytest.mark.parametrize("fault", ("reconstructed", "missing_receipt", "missing_selection"))
async def test_unproven_source_cannot_enter_current_native_profile_cache(
    client: KrakenFuturesExchangeClient, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    """A useful-looking payload cannot acquire source authority without actual receipt and owner proof.

    Given: A reconstructed frame or missing committed receipt or selected client identity,
    When: The actual private callback receives that qualified source,
    Then: It exposes no current profile and never manufactures a route for legacy entries.
    """
    captured: list[PrivateExecutionObservation] = []

    async def sink(observation: PrivateExecutionObservation) -> str:
        captured.append(observation)
        return str(UUID(int=300))

    client.set_execution_observation_sink(sink)
    if fault == "missing_receipt":
        monkeypatch.setattr(
            client, "_capture_private_execution_observation", AsyncMock(return_value=None)
        )
    elif fault == "missing_selection":
        monkeypatch.setattr(client, "execution_client_selection", lambda: None)
    if fault == "reconstructed":
        message = PrivateExecutionJsonCodec.loads(_wire())
        assert isinstance(message, dict)
        await client._on_execution_message(message)
    else:
        handled = asyncio.Event()
        original = client._on_execution_message

        async def relay(message: dict[str, object]) -> None:
            await original(message)
            handled.set()

        monkeypatch.setattr(client, "_on_execution_message", relay)
        _local_peer(monkeypatch, _wire())
        await client._ensure_private_ws_connected()
        await asyncio.wait_for(handled.wait(), _WAIT)
    result = await client.read_native_balance_observation()
    assert result.source_profiles == () and result.route is None
    if fault != "missing_receipt":
        assert captured[0].context.kind == "account_cash"
        assert captured[0].context.channel.endswith("unbound.v1") == (fault == "reconstructed")


@pytest.mark.parametrize("fault", ("quantity", "account"))
async def test_genuine_but_uninterpretable_balance_frame_remains_retained_without_current_profile(
    client: KrakenFuturesExchangeClient, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    """A real physical epoch cannot certify malformed native balances or missing account metadata.

    Given: Original source bytes with an invalid native quantity or empty account header,
    When: The actual SDK callback commits its receipt and attempts cache interpretation,
    Then: The full frame remains captured but no current profile or financial route is returned.
    """
    wire = _wire("true") if fault == "quantity" else _wire().replace("actual-wire-account", "")
    _local_peer(monkeypatch, wire)
    captured: list[PrivateExecutionObservation] = []
    handled = asyncio.Event()

    async def sink(observation: PrivateExecutionObservation) -> str:
        captured.append(observation)
        handled.set()
        return str(UUID(int=400))

    client.set_execution_observation_sink(sink)
    await client._ensure_private_ws_connected()
    await asyncio.wait_for(handled.wait(), _WAIT)
    result = await client.read_native_balance_observation()
    assert result.source_profiles == () and result.route is None
    assert base64.b64decode(captured[0].material) == wire.encode()
    assert (captured[0].context.venue_account_id is None) == (fault == "account")


async def test_actual_selected_route_change_invalidates_cached_profile_without_relabeling_it(
    client: KrakenFuturesExchangeClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A proof captured by one selected route cannot become a different current client selection.

    Given: A valid current profile and actual SDK endpoint agreement,
    When: The selected User endpoint disagrees after capture and is later restored,
    Then: The old profile is invalidated and restoring metadata alone cannot reactivate it.
    """
    _local_peer(monkeypatch, _wire())
    handled = asyncio.Event()

    async def sink(observation: PrivateExecutionObservation) -> str:
        handled.set()
        return str(UUID(int=500))

    client.set_execution_observation_sink(sink)
    await client._ensure_private_ws_connected()
    await asyncio.wait_for(handled.wait(), _WAIT)
    assert (await client.read_native_balance_observation()).source_profiles
    user = client._user_client
    assert user is not None
    original = user.url
    monkeypatch.setattr(user, "url", "https://demo-futures.kraken.com")
    assert not (await client.read_native_balance_observation()).source_profiles
    monkeypatch.setattr(user, "url", original)
    assert not (await client.read_native_balance_observation()).source_profiles
