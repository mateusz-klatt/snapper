"""Actual SDK privacy context, public delegation, reconnect and original callback identity."""

import asyncio
import json
import pickle
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import FrozenInstanceError
from dataclasses import asdict
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import cast
from weakref import ReferenceType
from weakref import WeakKeyDictionary
from weakref import ref

import kraken.futures.websocket as futures_websocket
import pytest
from kraken.futures import FuturesWSClient
from kraken.futures.websocket import ConnectFuturesWebsocket
from kraken.utils.utils import WSState
from requests import Session
from websockets.protocol import State

import snapper.infrastructure.exchanges.kraken_futures_private_receive as receive
from snapper.infrastructure.exchanges.execution_wire_receipts import PrivateWireMessage
from snapper.infrastructure.exchanges.kraken_futures_balance_epoch import _CURRENT_EPOCH
from snapper.infrastructure.exchanges.kraken_futures_balance_epoch import (
    private_futures_message_epoch,
)
from snapper.infrastructure.exchanges.private_control_decoder import TransientPrivateControl

_RUN = "_ConnectFuturesWebsocket__run"
_SDK_CALLBACK = "_FuturesWSClient__callback"
_CALLBACK_BINDING = "_snapper_owned_private_callback_binding"
_FILL = '{"feed":"fills","account":"DemoUser","fills":[{"qty":0.100000000000000001}]}'
_ACK = (
    '{"event":"subscribed","feed":"fills","api_key":"synthetic-private-key",'
    '"original_challenge":"synthetic-private-challenge",'
    '"signed_challenge":"synthetic-private-signature"}'
)


@pytest.fixture
def boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    """Restore actual SDK namespace/run state while each case owns its local real clients."""
    monkeypatch.setattr(ConnectFuturesWebsocket, _RUN, getattr(ConnectFuturesWebsocket, _RUN))
    monkeypatch.setattr(futures_websocket, "json", futures_websocket.json)
    monkeypatch.setattr(receive._INSTALLED, "delegate", receive._INSTALLED.delegate)
    monkeypatch.setattr(receive, "_OWNED", WeakKeyDictionary())


class _Socket:
    """Give the pinned SDK one original source and an explicit physical handshake identity."""

    def __init__(self, source: str) -> None:
        """Keep one physical source local without an external network connection."""
        self.source = source
        self.state = State.OPEN
        self.request = SimpleNamespace(path="/ws/v1", headers={"Host": "futures.kraken.com"})

    async def recv(self) -> str:
        """Return the original source until its actual callback stops this physical loop."""
        return self.source


@asynccontextmanager
async def _client(
    owned: bool = True,
    observer: Callable[[Mapping[str, object]], Awaitable[None]] | None = None,
) -> AsyncIterator[tuple[FuturesWSClient, list[Mapping[str, object]]]]:
    """Own actual SDK resources and optionally install the real private callback binder."""
    messages: list[Mapping[str, object]] = []

    async def callback(message: Mapping[str, object]) -> None:
        """Capture only test-local messages while preserving actual callback failure semantics."""
        messages.append(message)
        if observer is not None:
            await observer(message)
        client._conn.keep_alive = False

    client = FuturesWSClient(
        key="synthetic-key", secret="c3ludGhldGljLXNlY3JldA==", callback=callback
    )
    try:
        if owned:
            receive.bind_owned_futures_private_callback(client, callback)
        yield client, messages
    finally:
        client._conn.keep_alive = False
        await client.close()
        owned_session = getattr(client, "_FuturesClient__session", None)
        if isinstance(owned_session, Session):
            owned_session.close()


async def _run(client: FuturesWSClient, source: str, monkeypatch: pytest.MonkeyPatch) -> _Socket:
    """Run the actual pinned receive method with one distinct local physical socket."""
    socket = _Socket(source)

    @asynccontextmanager
    async def connect(*args: object, **kwargs: object) -> AsyncIterator[_Socket]:
        """Preserve the actual SDK context-manager boundary and physical socket state."""
        assert args[0] == "wss://futures.kraken.com/ws/v1"
        try:
            yield socket
        finally:
            socket.state = State.CLOSED

    monkeypatch.setattr(futures_websocket, "connect", connect)
    client._conn.keep_alive = True
    actual = cast(Callable[[asyncio.Event], Awaitable[None]], getattr(client._conn, _RUN))
    await actual(asyncio.Event())
    return socket


async def test_same_connector_reconnect_preserves_original_immutable_context(
    boundary: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each actual receive loop has its own physical identity without rewriting the first receipt.

    Given: One real SDK client and two physical runs on its original connector,
    When: Both callbacks retain genuine original financial messages,
    Then: Their epochs differ while first bytes, local clock and original object remain unchanged.
    """
    async with _client() as (client, messages):
        first_socket = await _run(client, _FILL, monkeypatch)
        first = receive.private_receive_metadata(messages[0])
        assert first is not None and first.client is client and first.socket is first_socket
        assert first.connector is client._conn
        original_clock = first.observed_at
        second_socket = await _run(client, _FILL, monkeypatch)
        second = receive.private_receive_metadata(messages[1])

        assert second is not None and second.socket is second_socket
        assert first.physical_epoch_public_id != second.physical_epoch_public_id
        assert receive.private_receive_metadata(messages[0]) is first
        assert first.observed_at == original_clock and first.client is second.client
        assert isinstance(messages[0], PrivateWireMessage)
        assert messages[0].wire_bytes == _FILL.encode()
        assert private_futures_message_epoch(messages[0]) == first.physical_epoch_public_id
    assert receive.private_callback_owner() is None and receive._current_scope() is None


async def test_actual_callback_cannot_decode_new_physical_context_after_its_epoch_is_lost(
    boundary: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A genuine owned callback still cannot substitute for a missing actual physical epoch.

    Given: A real SDK callback whose original message carries valid physical context,
    When: A later decode occurs after that task's physical epoch becomes unavailable,
    Then: Financial bytes remain available but the new mapping has no physical context.
    """

    async def observer(message: Mapping[str, object]) -> None:
        """Invalidate only the local epoch while exercising the actual installed decoder."""
        assert receive.private_receive_metadata(message) is not None
        token = _CURRENT_EPOCH.set(None)
        try:
            later = futures_websocket.json.loads(_FILL)
        finally:
            _CURRENT_EPOCH.reset(token)
        assert isinstance(later, PrivateWireMessage) and later.wire_bytes == _FILL.encode()
        assert receive.private_receive_metadata(later) is None

    async with _client(observer=observer) as (client, messages):
        await _run(client, _FILL, monkeypatch)
        assert len(messages) == 1


async def test_plain_fallback_mapping_cannot_borrow_an_actual_message_physical_context(
    boundary: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An ordinary reconstructed mapping remains weaker even beside a genuine receive context.

    Given: A real SDK financial frame with valid non-authoritative physical metadata,
    When: An ordinary fallback mapping is passed to the metadata attachment boundary,
    Then: That mapping remains unchanged and cannot acquire genuine private receive context.
    """
    async with _client() as (client, messages):
        await _run(client, _FILL, monkeypatch)
        metadata = receive.private_receive_metadata(messages[0])
        assert metadata is not None
        weaker: dict[str, object] = {"feed": "fills", "qty": 0.1}
        assert receive._attach_metadata(weaker, metadata) is weaker
        assert receive.private_receive_metadata(weaker) is None


async def test_inherited_child_task_cannot_claim_the_parent_receive_or_callback_context(
    boundary: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Task-local context inheritance is not proof of an unrelated task's physical receive call.

    Given: A real private SDK callback running in its physical parent receive task,
    When: It creates a separate task that decodes an unrelated financial message,
    Then: That task obtains neither an owned callback nor private receive metadata.
    """

    async def unrelated() -> None:
        assert receive.private_callback_owner() is None
        value = futures_websocket.json.loads(_FILL)
        assert isinstance(value, PrivateWireMessage)
        assert receive.private_receive_metadata(value) is None
        assert private_futures_message_epoch(value) is None

    async def observe(message: Mapping[str, object]) -> None:
        assert receive.private_callback_owner() is not None
        await asyncio.create_task(unrelated())

    async with _client(observer=observe) as (client, messages):
        await _run(client, _FILL, monkeypatch)
        assert receive.private_receive_metadata(messages[0]) is not None


@pytest.mark.parametrize("failure", (RuntimeError, asyncio.CancelledError))
async def test_callback_failure_resets_actual_private_context_and_owner(
    boundary: None, monkeypatch: pytest.MonkeyPatch, failure: type[BaseException]
) -> None:
    """Failure and cancellation cannot make later unrelated JSON look like a private callback.

    Given: A real SDK private receive callback that raises,
    When: The original callback failure leaves the physical loop,
    Then: All local contexts reset and existing captured source remains immutable.
    """

    async def observe(message: Mapping[str, object]) -> None:
        assert receive.private_callback_owner() is not None
        raise failure("isolated private callback failure")

    async with _client(observer=observe) as (client, messages):
        with pytest.raises(failure, match="isolated private callback failure"):
            await _run(client, _FILL, monkeypatch)
        assert receive.private_callback_owner() is None and receive._current_scope() is None
        assert receive.private_receive_metadata(messages[0]) is not None
        assert isinstance(futures_websocket.json.loads(_ACK), dict)


async def test_delayed_old_callback_keeps_original_client_after_another_client_runs(
    boundary: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A suspended callback does not read its original SDK identity from a mutable current-client slot.

    Given: One owned actual SDK callback paused before another client's physical loop runs,
    When: The old callback resumes after the new client has delivered source,
    Then: Its owner and original metadata still name the first client and socket epoch.
    """
    entered = asyncio.Event()
    release = asyncio.Event()
    owners: list[FuturesWSClient | None] = []

    async def observe(message: Mapping[str, object]) -> None:
        owners.append(receive.private_callback_owner())
        entered.set()
        await release.wait()
        owners.append(receive.private_callback_owner())

    async with _client(observer=observe) as (first_client, first_messages):
        task = asyncio.create_task(_run(first_client, _FILL, monkeypatch))
        try:
            await entered.wait()
            first = receive.private_receive_metadata(first_messages[0])
            async with _client() as (second_client, second_messages):
                await _run(second_client, _FILL, monkeypatch)
                second = receive.private_receive_metadata(second_messages[0])
                assert first is not None and second is not None
                assert first.client is first_client and second.client is second_client
                release.set()
                await task
                assert owners == [first_client, first_client]
                assert receive.private_receive_metadata(first_messages[0]) is first
        finally:
            release.set()
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)


async def test_unregistered_public_decoder_and_outbound_encoding_delegate_unchanged(
    boundary: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Installing private privacy does not replace ordinary public JSON decoding semantics.

    Given: The installed private wrapper and another actual unregistered SDK connector,
    When: That connector receives a public ticker and explicit standard JSON options are used,
    Then: Original public types, failures and encoding remain unchanged with no private metadata.
    """
    async with _client(), _client(owned=False) as (public, messages):
        await _run(public, '{"feed":"ticker","price":1.125}', monkeypatch)
        assert isinstance(messages[0], dict) and messages[0]["price"] == 1.125
        assert receive.private_receive_metadata(messages[0]) is None
        value = futures_websocket.json.loads('{"price":1.125}', parse_float=Decimal)
        assert isinstance(value, dict) and value["price"] == Decimal("1.125")
        with pytest.raises(ValueError):
            futures_websocket.json.loads("invalid-public-json")
        assert futures_websocket.json.dumps({"value": 1}, sort_keys=True) == json.dumps(
            {"value": 1}, sort_keys=True
        )


async def test_control_properties_and_original_metadata_refuse_reassignment(
    boundary: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The callback cannot relabel physical metadata by assigning a property on transient control data.

    Given: One actual successful subscribed control through the pinned SDK mapping branch,
    When: A consumer attempts to replace its fields, failure category or original physical clock,
    Then: Assignment refuses and the original local context remains unchanged.
    """
    async with _client() as (client, messages):
        await _run(client, _ACK, monkeypatch)
        control = messages[0]
        assert isinstance(control, TransientPrivateControl)
        metadata = receive.private_receive_metadata(control)
        assert metadata is not None
        for name in ("receive_metadata", "failure_reason", "_fields", "_receive_metadata"):
            with pytest.raises(AttributeError, match="immutable"):
                setattr(control, name, "changed")
        with pytest.raises(FrozenInstanceError):
            metadata.observed_at = datetime(2000, 1, 1, tzinfo=UTC)
        assert receive.private_receive_metadata(control) is metadata
        assert (
            receive.private_receive_metadata({"_snapper_owned_private_receive_metadata": metadata})
            is None
        )


@pytest.mark.parametrize("changed", ("codec", "run", "delegate"))
async def test_half_restored_installation_refuses_without_self_delegation(
    boundary: None, monkeypatch: pytest.MonkeyPatch, changed: str
) -> None:
    """A mismatched capture pair cannot silently wrap its own codec into infinite recursion.

    Given: A real installed owned SDK boundary whose codec, run marker or delegate is replaced,
    When: Another private client asks for the same installation,
    Then: Installation refuses the incoherent pair before modifying its namespace again.
    """
    async with _client():
        if changed == "codec":
            monkeypatch.setattr(futures_websocket, "json", receive._INSTALLED.delegate)
        elif changed == "run":
            current = getattr(ConnectFuturesWebsocket, _RUN)
            monkeypatch.setattr(ConnectFuturesWebsocket, _RUN, current.__wrapped__)
        else:
            monkeypatch.setattr(receive._INSTALLED, "delegate", None)
        original = futures_websocket.json
        run = getattr(ConnectFuturesWebsocket, _RUN)
        with pytest.raises(RuntimeError, match="replaced"):
            receive.install_owned_futures_private_receive_capture()
        assert futures_websocket.json is original and getattr(ConnectFuturesWebsocket, _RUN) is run


@pytest.mark.parametrize("bad", ("plain", "wrong_type", "epoch", "noncanonical", "naive"))
async def test_provider_fields_and_damaged_local_metadata_are_not_accepted_as_context(
    boundary: None, monkeypatch: pytest.MonkeyPatch, bad: str
) -> None:
    """Private local attributes are distinct from provider fields and must retain valid context shape.

    Given: A real receive context and an unrelated fabricated or damaged mapping,
    When: Its projected physical metadata is requested,
    Then: No valid private context is returned for provider fields or malformed local values.
    """
    async with _client() as (client, messages):
        await _run(client, _FILL, monkeypatch)
        metadata = receive.private_receive_metadata(messages[0])
        assert metadata is not None
        if bad == "plain":
            assert (
                receive.private_receive_metadata(
                    {"_snapper_owned_private_receive_metadata": metadata}
                )
                is None
            )
            return
        value = (
            17
            if bad == "wrong_type"
            else (
                replace(metadata, physical_epoch_public_id="not-a-canonical-uuid")
                if bad == "epoch"
                else replace(metadata, observed_at=metadata.observed_at.replace(tzinfo=None))
            )
        )
        message = PrivateWireMessage({}, b"{}")
        message.__dict__["_snapper_owned_private_receive_metadata"] = value
        assert receive.private_receive_metadata(message) is None


async def test_private_callback_cannot_be_rebound_to_another_handler(
    boundary: None,
) -> None:
    """An existing immutable owned callback is not replaced by a later caller's handler.

    Given: An actual SDK whose original private callback has already been bound,
    When: A different source handler requests the same SDK object,
    Then: The callback identity conflict is refused without changing its original closure.
    """

    async def other(message: Mapping[str, object]) -> None:
        raise AssertionError("A changed private callback must never run")

    async with _client() as (client, messages):
        original = getattr(client, _SDK_CALLBACK)
        with pytest.raises(RuntimeError, match="callback identity changed"):
            receive.bind_owned_futures_private_callback(client, other)
        assert getattr(client, _SDK_CALLBACK) is original and not messages


async def test_registered_connector_cannot_silently_change_its_original_private_owner(
    boundary: None,
) -> None:
    """A nominally matching real SDK type cannot replace the actual connector's registered owner.

    Given: Two real SDK objects and one deliberately inconsistent registered connector edge,
    When: Registration tries to reuse that edge as the first client,
    Then: The inconsistent owner is refused and no account ownership claim is created.
    """
    async with _client() as (first, _), _client() as (second, _):
        receive._OWNED[first._conn] = ref(second)
        with pytest.raises(RuntimeError, match="connector identity changed"):
            receive.register_futures_private_receiver(first)
        empty = cast(FuturesWSClient, SimpleNamespace(_conn=None))
        with pytest.raises(RuntimeError, match="connector identity is unproven"):
            receive.register_futures_private_receiver(empty)


async def test_numeric_decode_failure_preserves_raw_without_a_selected_child_or_epoch(
    boundary: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Unavailable financial decoding preserves bytes without manufacturing interpreted operands.

    Given: A known financial feed whose exact numeric decoder raises at its real boundary,
    When: The actual SDK receives the safely classified original source,
    Then: Original bytes remain available but decoded child, epoch and ownership metadata are absent.
    """

    def fail(data: str | bytes | bytearray, **kwargs: object) -> object:
        raise ValueError("isolated numeric decoder unavailable")

    async with _client() as (client, messages):
        delegate = receive._INSTALLED.delegate
        assert delegate is not None
        monkeypatch.setattr(delegate, "loads", fail)
        await _run(client, _FILL, monkeypatch)
        result = messages[0]
        assert isinstance(result, PrivateWireMessage) and result.wire_bytes == _FILL.encode()
        assert dict(result) == {"feed": "fills"}
        assert private_futures_message_epoch(result) is None
        assert receive.private_receive_metadata(result) is None


async def test_unproven_private_task_still_sanitizes_controls_without_creating_context(
    boundary: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A broken context helper cannot select public raw-error handling inside an owned private run.

    Given: An actual private callback whose task-identity helper becomes unavailable,
    When: It decodes authentication control or tries to enter its owned callback again,
    Then: Control stays repr-safe without physical metadata and the unsupported callback refuses.
    """

    async def observe(message: Mapping[str, object]) -> None:
        owner = receive.private_callback_owner()
        assert owner is not None
        wrapped = getattr(owner, _SDK_CALLBACK)
        with monkeypatch.context() as changed:
            changed.setattr(receive, "_running_task_identity", lambda: None)
            result = futures_websocket.json.loads(_ACK)
            assert isinstance(result, TransientPrivateControl)
            assert receive.private_receive_metadata(result) is None
            with pytest.raises(RuntimeError, match="callback context is unavailable"):
                await wrapped(message)

    async with _client(observer=observe) as (client, messages):
        await _run(client, _FILL, monkeypatch)
        assert receive.private_receive_metadata(messages[0]) is not None


async def test_owned_run_refuses_missing_task_before_entering_sdk_receive(
    boundary: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An owned connector with no verifiable task cannot fall back to unfiltered receive handling.

    Given: An actual registered SDK object before its physical loop starts,
    When: Its run boundary cannot identify a current task,
    Then: It refuses before receiving any private source or callback.
    """
    async with _client() as (client, messages):
        monkeypatch.setattr(receive, "_running_task_identity", lambda: None)
        with pytest.raises(RuntimeError, match="receive task is unavailable"):
            await _run(client, _ACK, monkeypatch)
        assert messages == []


async def test_repeated_original_callback_binding_preserves_the_same_actual_wrapper(
    boundary: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Idempotent setup does not stack receive or callback wrappers on one SDK object.

    Given: One actual SDK whose original source handler has already been bound,
    When: Startup repeats the same binding and installation,
    Then: Both actual wrapper identities remain unchanged and source still reaches the original handler.
    """
    async with _client() as (client, messages):
        binding = getattr(client, _CALLBACK_BINDING)
        assert isinstance(binding, receive._CallbackBinding)
        original = getattr(client, _SDK_CALLBACK)
        run = getattr(ConnectFuturesWebsocket, _RUN)
        codec = futures_websocket.json
        receive.bind_owned_futures_private_callback(client, binding.original)
        receive.install_owned_futures_private_receive_capture()
        assert getattr(client, _SDK_CALLBACK) is original
        assert getattr(ConnectFuturesWebsocket, _RUN) is run and futures_websocket.json is codec
        await _run(client, _FILL, monkeypatch)
        assert len(messages) == 1


async def test_a_live_unbound_sdk_cannot_replace_its_original_callback(
    boundary: None,
) -> None:
    """Private callback ownership must be established before any socket starts delivering data.

    Given: An actual unbound SDK whose connector has already left its initial state,
    When: A caller tries to bind the same apparent original handler after that transition,
    Then: Setup refuses rather than retroactively attaching original-client provenance.
    """
    async with _client(owned=False) as (client, messages):
        original = getattr(client, _SDK_CALLBACK)
        client._conn.state = WSState.CONNECTED
        with pytest.raises(RuntimeError, match="must bind before start"):
            receive.bind_owned_futures_private_callback(client, original)
        assert getattr(client, _SDK_CALLBACK) is original and messages == []


async def test_unknown_original_handler_is_not_replaced_on_a_fresh_sdk(
    boundary: None,
) -> None:
    """Even a fresh SDK must name the exact handler its constructor actually selected.

    Given: One real fresh SDK with its original source handler,
    When: Another handler attempts to bind that client before start,
    Then: The mismatch refuses without replacing the original callback.
    """

    async def other(message: Mapping[str, object]) -> None:
        raise AssertionError("Unproven callback must not run")

    async with _client(owned=False) as (client, messages):
        original = getattr(client, _SDK_CALLBACK)
        with pytest.raises(RuntimeError, match="original callback is unproven"):
            receive.bind_owned_futures_private_callback(client, other)
        assert getattr(client, _SDK_CALLBACK) is original and messages == []


async def test_missing_installed_delegate_refuses_instead_of_decoding_private_source(
    boundary: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An inconsistent installed namespace cannot select a default decoder as a silent fallback.

    Given: One real installed private receive adapter whose actual delegate reference is missing,
    When: Its module-local codec is invoked,
    Then: It refuses with a fixed local error before interpreting source.
    """
    async with _client():
        monkeypatch.setattr(receive._INSTALLED, "delegate", None)
        with pytest.raises(RuntimeError, match="decoder was not installed"):
            futures_websocket.json.loads(_ACK)


async def test_runtime_metadata_cannot_serialize_or_deepcopy_sdk_credential_fields(
    boundary: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Runtime object identity references remain opaque to default metadata serializers.

    Given: Actual received context containing references to a credential-bearing real SDK object,
    When: A generic serializer, deep copy or dataclass conversion sees that context,
    Then: It refuses before traversing SDK internals and its immutable object bundle cannot change.
    """
    async with _client() as (client, messages):
        await _run(client, _FILL, monkeypatch)
        metadata = receive.private_receive_metadata(messages[0])
        assert metadata is not None
        for serialize in (pickle.dumps, deepcopy, asdict):
            with pytest.raises(TypeError, match="cannot be serialized|cannot be copied"):
                serialize(metadata)
        with pytest.raises(AttributeError, match="immutable"):
            metadata._objects.client = object()
        with pytest.raises(TypeError, match="cannot be serialized"):
            pickle.dumps(metadata._objects)
        assert metadata.client is client and "synthetic-key" not in repr(metadata)


class _RetiredReceiver:
    """A weak-referenceable placeholder for a damaged local registry edge."""


async def test_expired_registered_owner_refuses_before_private_source_reaches_sdk_logging(
    boundary: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An old registered connector cannot become public decode merely because its owner disappeared.

    Given: A real private connector with a deliberately damaged dead owner reference,
    When: Its actual receive wrapper starts,
    Then: It refuses before reading or invoking a private callback.
    """
    async with _client() as (client, messages):
        retired = _RetiredReceiver()
        reference = ref(retired)
        del retired
        assert reference() is None
        receive._OWNED[client._conn] = cast(ReferenceType[FuturesWSClient], reference)
        with pytest.raises(RuntimeError, match="original receiver is unavailable"):
            await _run(client, _ACK, monkeypatch)
        assert messages == []


@pytest.mark.parametrize("broken", ("run", "codec"))
def test_unsupported_upstream_boundaries_refuse_before_installing_owned_capture(
    boundary: None, monkeypatch: pytest.MonkeyPatch, broken: str
) -> None:
    """A prior capture installer cannot silently leave an unsupported actual SDK ABI behind.

    Given: Upstream capture ports that return without the required run or codec,
    When: The owned adapter validates those resulting real namespace coordinates,
    Then: Installation refuses the unsupported boundary before receiving source.
    """

    async def unsupported(connector: ConnectFuturesWebsocket, event: asyncio.Event) -> None:
        raise AssertionError("Unsupported SDK run must not execute")

    monkeypatch.setattr(ConnectFuturesWebsocket, _RUN, None if broken == "run" else unsupported)
    monkeypatch.setattr(futures_websocket, "json", object())
    monkeypatch.setattr(receive, "install_private_execution_wire_capture", lambda: None)
    monkeypatch.setattr(receive, "install_futures_balance_epoch_capture", lambda: None)
    with pytest.raises(RuntimeError, match="receive boundary is unsupported"):
        receive.install_owned_futures_private_receive_capture()
