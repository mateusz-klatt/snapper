"""Refuse unproven private boundaries and join real asynchronous cleanup dependencies.

Positive authentication setup uses the existing signed local HTTP and pinned
SDK scenario. Public lookalikes and direct state inputs are refusal candidates;
none can manufacture an issued source ticket or an account ownership proof.
"""

import asyncio
import gc
import json
import pickle
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from weakref import ref

import kraken.futures.websocket as futures_websocket
import pytest
from kraken.futures import FuturesWSClient
from kraken.utils.utils import WSState
from requests import Session
from websockets.protocol import State

import snapper.infrastructure.exchanges.authenticated_private_shutdown as shutdown_module
import snapper.infrastructure.exchanges.kraken_futures_authenticated_socket as bridge
import snapper.infrastructure.exchanges.kraken_futures_private_receive as receive
import tests.infrastructure.exchanges.test_authenticated_private_socket as socket_fixture
from snapper.core.private_frame_ticket import _ActualPrivateFrameIssuer
from snapper.infrastructure.exchanges.authenticated_private_shutdown import OwnedPrivateShutdown
from snapper.infrastructure.exchanges.authenticated_private_shutdown import (
    PrivateShutdownUnresolvedError,
)
from snapper.infrastructure.exchanges.execution_wire_receipts import PrivateWireMessage
from snapper.infrastructure.exchanges.kraken_futures_balance_epoch import _CURRENT_EPOCH
from snapper.infrastructure.exchanges.owned_private_socket import ActualReceiveCell
from snapper.infrastructure.exchanges.owned_private_socket import OriginalReceive
from snapper.infrastructure.exchanges.owned_private_socket import OwnedPrivateSocket
from snapper.infrastructure.exchanges.owned_private_socket import PrivateSocketBoundaryError
from snapper.infrastructure.exchanges.private_control_decoder import TransientPrivateControl
from snapper.infrastructure.exchanges.private_socket_authentication import (
    PreparedPrivateRuntimeBinding,
)
from snapper.infrastructure.exchanges.private_socket_authentication import (
    PrivateAuthenticationError,
)
from snapper.infrastructure.exchanges.private_socket_authentication import PrivateRuntimeSelection
from snapper.infrastructure.exchanges.private_socket_authentication import (
    PrivateSocketAuthentication,
)
from snapper.infrastructure.exchanges.private_socket_authentication import _same_text
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_account_authority import native_authority_scenario
from tests.helpers.native_account_authority import native_authority_uuid

_NOW = datetime(2026, 10, 24, 16, tzinfo=UTC)
_URL = "wss://futures.kraken.com/ws/v1"


@dataclass
class _BoundarySocket:
    """A local physical dependency exposing original returns and genuine raised failures."""

    entity: object = "original private source"
    receive_error: BaseException | None = None
    send_error: BaseException | None = None
    close_error: BaseException | None = None
    state: State = State.OPEN
    request: object = field(
        default_factory=lambda: SimpleNamespace(
            path="/ws/v1", headers={"Host": "futures.kraken.com"}
        )
    )
    sent: list[str] = field(default_factory=list)

    async def recv(self) -> str | bytes:
        """Return the actual selected object or propagate the physical receive failure."""
        if self.receive_error is not None:
            raise self.receive_error
        return cast(str | bytes, self.entity)

    async def send(self, source: str) -> None:
        """Record only completed physical sends, without any authentication assertion."""
        if self.send_error is not None:
            raise self.send_error
        self.sent.append(source)

    async def close(self) -> None:
        """Complete or fail the actual dependency close."""
        if self.close_error is not None:
            raise self.close_error
        self.state = State.CLOSED


async def _socket_proxy(socket: _BoundarySocket) -> tuple[OwnedPrivateSocket, ActualReceiveCell]:
    """Bind a real receive handoff to this actual task without issuing authentication."""
    task = asyncio.current_task()
    assert task is not None
    cell = ActualReceiveCell(id(task))

    def before_send(source: str, observed_at: datetime) -> None:
        """Require a real dispatch clock without treating this probe as an account key guard."""
        assert source and observed_at.tzinfo is UTC

    return OwnedPrivateSocket(socket, cell, lambda: native_authority_uuid(8000), before_send), cell


@asynccontextmanager
async def _bound_scenario(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[tuple[NativeAuthorityScenario, socket_fixture._BridgeScenario]]:
    """Reuse actual committed prepared-key ownership, without substituting a token or proof."""
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        socket_fixture._scenario(source, monkeypatch, "valid") as value,
    ):
        yield source, value


def _binding(value: socket_fixture._BridgeScenario) -> bridge._AuthenticatedClientBinding:
    """Read the actual binding installed by the production prepared-key bridge."""
    binding: object = getattr(value.ws, bridge._BINDING_ATTRIBUTE)
    assert isinstance(binding, bridge._AuthenticatedClientBinding)
    return binding


def _unbound_state(value: socket_fixture._BridgeScenario) -> PrivateSocketAuthentication:
    """Create only a protocol refusal candidate lacking any actual socket issuer or receive cell."""
    actual = _binding(value)
    return PrivateSocketAuthentication(
        actual.runtime, native_authority_uuid(8001), actual.client_capture_public_id
    )


async def test_original_receive_and_socket_refuse_copying_and_relabeling() -> None:
    """Keep original receive identity and live socket ownership opaque and immutable.

    Given: A local physical socket has delivered its original source through a real handoff.
    When: Callers copy, serialize or relabel its objects before sending and closing the socket.
    Then: Transfer attempts refuse, dispatch preserves exact text and close invalidates the handoff.
    """
    physical = _BoundarySocket()
    proxy, cell = await _socket_proxy(physical)
    source = await proxy.recv()
    received = cell.consume(source)
    assert received is not None and received.is_source(source)
    assert proxy.state is State.OPEN and proxy.request is physical.request
    assert "original private source" not in repr(received) + repr(proxy)
    for opaque in (received, proxy):
        for serialize in (pickle.dumps, deepcopy):
            with pytest.raises(TypeError):
                serialize(opaque)
    with pytest.raises(AttributeError, match="immutable"):
        received.receive_public_id = native_authority_uuid(8002)
    assert not OriginalReceive("\ud800", _NOW, native_authority_uuid(8003)).matches_financial_wire(
        b"replacement"
    )
    await proxy.send("original encoded request")
    assert physical.sent == ["original encoded request"]
    await proxy.close()
    assert proxy.state is State.CLOSED and cell.consume(source) is None


async def test_receive_cell_refuses_forged_and_ambiguous_physical_sources() -> None:
    """Refuse unissued or ambiguous original receive handoffs.

    Given: One cell receives a public lookalike and another has two pending physical receives.
    When: A caller tries to consume either candidate source or record after invalidation.
    Then: Both cells remain closed and select no original receive.
    """
    physical = _BoundarySocket()
    _, forged_cell = await _socket_proxy(physical)
    forged_cell.record(OriginalReceive("forged", _NOW, native_authority_uuid(8004)))
    assert forged_cell.consume("forged") is None
    forged_cell.record(OriginalReceive("later", _NOW, native_authority_uuid(8005)))
    assert forged_cell.consume("later") is None
    proxy, ambiguous_cell = await _socket_proxy(physical)
    first = await proxy.recv()
    await proxy.recv()
    assert ambiguous_cell.consume(first) is None


@pytest.mark.parametrize(
    "failure, expected, message",
    (
        (asyncio.CancelledError(), asyncio.CancelledError, "^$"),
        (TimeoutError("private marker"), TimeoutError, "private_socket_receive_timeout"),
        (
            RuntimeError("private marker"),
            PrivateSocketBoundaryError,
            "private_socket_receive_failed",
        ),
    ),
)
async def test_receive_errors_preserve_cancellation_and_hide_physical_details(
    failure: BaseException, expected: type[BaseException], message: str
) -> None:
    """Preserve receive cancellation while hiding physical failure details.

    Given: The physical receive raises cancellation, a timeout or another private failure.
    When: The owned socket awaits that actual dependency.
    Then: Cancellation survives and other errors expose only their fixed safe classification.
    """
    proxy, _ = await _socket_proxy(_BoundarySocket(receive_error=failure))
    with pytest.raises(expected, match=message) as caught:
        await proxy.recv()
    assert "private marker" not in str(caught.value)


async def test_receive_refuses_a_non_websocket_entity() -> None:
    """Reject physical receive values outside the WebSocket entity contract.

    Given: The physical dependency returns an integer instead of text or bytes.
    When: The owned socket receives and the caller attempts to consume that value.
    Then: The type boundary refuses and the cell contains no original receive.
    """
    proxy, cell = await _socket_proxy(_BoundarySocket(entity=17))
    with pytest.raises(PrivateSocketBoundaryError, match="private_socket_receive_type_invalid"):
        await proxy.recv()
    assert cell.consume(17) is None


@pytest.mark.parametrize("operation", ("send", "close"))
@pytest.mark.parametrize("cancelled", (False, True))
async def test_physical_send_and_close_failures_have_safe_cancellation_semantics(
    operation: str, cancelled: bool
) -> None:
    """Keep physical send and close failures private without swallowing cancellation.

    Given: The physical send or close raises cancellation or a private runtime error.
    When: The owned socket invokes the selected operation.
    Then: Cancellation propagates, other failures are sanitized and no send completes.
    """
    failure = asyncio.CancelledError() if cancelled else RuntimeError("private marker")
    physical = _BoundarySocket(send_error=failure, close_error=failure)
    proxy, _ = await _socket_proxy(physical)
    expected = asyncio.CancelledError if cancelled else PrivateSocketBoundaryError
    with pytest.raises(expected) as caught:
        if operation == "send":
            await proxy.send("original encoded request")
        else:
            await proxy.close()
    assert "private marker" not in str(caught.value)
    assert not physical.sent


@pytest.mark.parametrize(
    "stop_error, force_error, expected",
    (
        (None, None, None),
        (RuntimeError("private stop detail"), None, "private_socket_original_close_failed"),
        (
            asyncio.CancelledError(),
            RuntimeError("private force detail"),
            "private_socket_forced_close_failed",
        ),
    ),
)
async def test_supervised_shutdown_reaps_actual_receivers_after_original_or_forced_close(
    stop_error: BaseException | None, force_error: BaseException | None, expected: str | None
) -> None:
    """Join actual receiver dependencies before releasing shutdown ownership.

    Given: Cleanup owns one pending receiver and one completed receiver.
    When: Original close succeeds or fails and requires the actual fallback cleanup.
    Then: Both receivers are joined before release and only a safe failure classification remains.
    """
    events: list[str] = []
    waiting = asyncio.Event()

    async def receiver() -> None:
        """Remain a real pending cleanup dependency until cancellation is joined."""
        await waiting.wait()

    async def finished() -> None:
        """Supply an already completed real dependency whose result still needs retrieval."""

    pending = asyncio.create_task(receiver())
    complete = asyncio.create_task(finished())
    await complete

    async def stop() -> None:
        """Exercise the original caller-owned cleanup and its real failure outcome."""
        events.append("stop")
        if stop_error is not None:
            raise stop_error

    async def force() -> None:
        """Exercise fallback cleanup only after the original function fails."""
        events.append("force")
        if force_error is not None:
            raise force_error

    def quiesce() -> None:
        """Revoke future work before creating the independent shutdown task."""
        events.append("quiesce")

    def release() -> None:
        """Release only after each actual receiver has terminated."""
        assert pending.done() and complete.done()
        events.append("release")

    shutdown = OwnedPrivateShutdown(stop, force, lambda: (pending, complete), quiesce, release)
    assert not shutdown.resolved and shutdown.failure is None
    assert repr(shutdown) == "<OwnedPrivateShutdown:opaque>"
    for serialize in (pickle.dumps, deepcopy):
        with pytest.raises(TypeError):
            serialize(shutdown)
    await shutdown.close()
    assert shutdown.resolved and shutdown.failure == expected
    assert events == ["quiesce", "stop", *(["force"] if stop_error is not None else []), "release"]
    assert pending.cancelled()


@pytest.mark.parametrize("cancelled", (False, True))
async def test_shutdown_failed_final_release_remains_explicitly_unresolved(
    monkeypatch: pytest.MonkeyPatch, cancelled: bool
) -> None:
    """Retain unresolved shutdown ownership after a failed final release.

    Given: Original cleanup completes but its final release callback can fail or cancel.
    When: The independent shutdown task reaches that actual callback boundary.
    Then: Close reports the failure and supervision retains a fixed unresolved classification.
    """
    monkeypatch.setattr(shutdown_module, "_SUPERVISED_SHUTDOWNS", set())

    async def stop() -> None:
        """Complete the actual cleanup dependency without claiming final ownership release."""

    def release() -> None:
        """Fail at the real final callback boundary with either cancellation or an ordinary error."""
        if cancelled:
            raise asyncio.CancelledError()
        raise RuntimeError("private release detail")

    shutdown = OwnedPrivateShutdown(stop, stop, tuple, lambda: None, release)
    expected = asyncio.CancelledError if cancelled else RuntimeError
    with pytest.raises(expected):
        await shutdown.close()
    await asyncio.sleep(0)
    classification = (
        "private_socket_cleanup_cancelled" if cancelled else "private_socket_cleanup_failed"
    )
    assert not shutdown.resolved and shutdown.failure == classification
    assert shutdown in shutdown_module._SUPERVISED_SHUTDOWNS


async def test_shutdown_refuses_the_actual_receiver_to_itself_dependency() -> None:
    """Refuse shutdown dependencies that would make a receiver await itself.

    Given: The task requesting close is itself listed as an actual receiver.
    When: It awaits the owned shutdown.
    Then: The self dependency is refused before either cleanup function starts.
    """
    current = asyncio.current_task()
    assert current is not None

    async def forbidden() -> None:
        """Fail if cleanup is allowed to start before detecting the actual self dependency."""
        pytest.fail("Self-dependent cleanup must not start")

    shutdown = OwnedPrivateShutdown(
        forbidden,
        forbidden,
        lambda: (cast(asyncio.Task[None], current),),
        lambda: None,
        lambda: None,
    )
    with pytest.raises(
        PrivateShutdownUnresolvedError, match="private_socket_close_from_receive_task"
    ):
        await shutdown.close()
    assert not shutdown.resolved


@pytest.mark.parametrize("left", (17, "\ud800"))
def test_control_text_comparison_refuses_nontext_and_unencodable_sources(left: object) -> None:
    """Reject control echoes that cannot be compared as exact UTF-8 text.

    Given: A control value is nontext or contains an unencodable Unicode surrogate.
    When: The exact echo comparator checks it against the expected private text.
    Then: Comparison returns false without exposing or coercing the candidate.
    """
    assert not _same_text(left, "expected private echo")


async def test_prepared_runtime_and_unbound_control_state_refuse_changed_inputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep prepared runtime identity and unbound control state from granting ownership.

    Given: A real prepared key belongs to one SDK client and another SDK client exists.
    When: The runtime provider changes clients or unbound and closed state receives controls.
    Then: Identity changes and closed dispatch refuse while unproven controls issue no ticket.
    """
    async with _bound_scenario(tmp_path, monkeypatch) as (source, value):
        current = value.current()
        with pytest.raises(AttributeError, match="immutable"):
            current.selection = source.selection
        other = FuturesWSClient(key="synthetic-key", secret="c3ludGhldGljLXNlY3JldA==")
        try:
            changed = PrivateRuntimeSelection(current.user, other, current.selection)
            runtime = PreparedPrivateRuntimeBinding(
                value.ws, value.prepared.require_token(), lambda: changed
            )
            with pytest.raises(
                PrivateAuthenticationError, match="private_auth_actual_client_changed"
            ):
                runtime.current_proof(datetime.now(UTC))
        finally:
            await other.close()
            owned_session = getattr(other, "_FuturesClient__session", None)
            if isinstance(owned_session, Session):
                owned_session.close()
        state = _unbound_state(value)
        received = OriginalReceive(socket_fixture._FILL, _NOW, native_authority_uuid(8006))
        control = TransientPrivateControl({"event": "error"}, None)
        state.observe_control(control, received)
        assert state.financial_ticket(PrivateWireMessage({}, b"{}"), received) is None
        state._observe_challenge(
            TransientPrivateControl({"event": "challenge", "message": "x"}, None), _NOW
        )
        state._observe_ack(
            TransientPrivateControl({"event": "subscribed", "feed": "unknown"}, None), _NOW
        )
        state.close()
        with pytest.raises(PrivateAuthenticationError, match="private_auth_socket_closed"):
            state.before_send("{}", _NOW)


@pytest.mark.parametrize(
    "document, expected",
    (
        ("{", "private_auth_outbound_json_invalid"),
        ("\ud800", "private_auth_outbound_json_invalid"),
        (
            json.dumps({"event": "subscribe", "api_key": "synthetic-key", "feed": "fills"}),
            "private_auth_actual_challenge_unproven",
        ),
    ),
)
async def test_prepared_dispatch_refuses_invalid_source_and_missing_actual_challenge(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, document: str, expected: str
) -> None:
    """Refuse invalid outbound JSON and subscriptions without an actual challenge.

    Given: Real key preparation exists but the candidate state has no physical challenge.
    When: Dispatch receives malformed JSON, invalid Unicode or an unproven subscription.
    Then: It raises the precise safe refusal before any feed can gain ownership.
    """
    async with _bound_scenario(tmp_path, monkeypatch) as (_, value):
        with pytest.raises(PrivateAuthenticationError, match=expected):
            _unbound_state(value).before_send(document, datetime.now(UTC))


async def test_prepared_dispatch_blocks_repeated_challenge_and_unknown_or_unsubscribed_feeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Invalidate ambiguous challenges and unsupported subscription dispatches.

    Given: An unbound protocol state can verify the actual prepared key.
    When: Challenges repeat or subscription and unsubscribe requests name unsupported feeds.
    Then: The affected state is blocked while unrelated ping dispatch remains nonauthoritative.
    """
    async with _bound_scenario(tmp_path, monkeypatch) as (_, value):
        state = _unbound_state(value)
        state.before_send(json.dumps({"event": "ping"}), datetime.now(UTC))
        challenge = json.dumps({"event": "challenge", "api_key": "synthetic-key"})
        state.before_send(challenge, datetime.now(UTC))
        state.before_send(challenge, datetime.now(UTC))
        for event in ("subscribe", "unsubscribe"):
            for feed in ("unknown", 17, "fills"):
                candidate = _unbound_state(value)
                document = {"event": event, "api_key": "synthetic-key", "feed": feed}
                if event == "subscribe" and feed == "fills":
                    continue
                candidate.before_send(json.dumps(document), datetime.now(UTC))
                assert candidate._blocked
        assert state._blocked == {"fills", "account_log"}


async def test_binding_idempotence_and_missing_shutdown_refuse_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Allow only the original fresh binding to be registered idempotently.

    Given: Actual prepared ownership has installed one fresh client binding.
    When: Callers repeat registration, replace its provider or omit shutdown and connect proof.
    Then: Only exact original registration succeeds and every incomplete lifecycle refuses.
    """
    async with _bound_scenario(tmp_path, monkeypatch) as (_, value):
        binding = _binding(value)
        token = value.prepared.require_token()
        assert repr(binding) == "<AuthenticatedPrivateClientBinding:opaque>"
        assert bridge.bind_authenticated_private_client(value.ws, token, value.current) is binding
        with pytest.raises(
            PrivateAuthenticationError, match="private_socket_existing_binding_changed"
        ):
            bridge.bind_authenticated_private_client(value.ws, token, lambda: value.current())
        fresh = bridge._AuthenticatedClientBinding(value.ws, token, value.current)
        assert not fresh.close_resolved
        with pytest.raises(PrivateAuthenticationError, match="private_socket_shutdown_not_bound"):
            await fresh.close()
        with pytest.raises(
            PrivateAuthenticationError, match="private_socket_actual_connect_not_observed"
        ):
            fresh._open_socket(native_authority_uuid(8007), ActualReceiveCell(0), object())
        original_state = value.ws._conn.state
        try:
            value.ws._conn.state = WSState.CONNECTED
            with pytest.raises(
                PrivateAuthenticationError, match="private_socket_binding_requires_fresh_client"
            ):
                bridge.bind_authenticated_private_client(value.ws, token, value.current)
        finally:
            value.ws._conn.state = original_state


async def test_actual_binding_refuses_a_loop_callback_without_an_actual_receive_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Require an actual receive task even when prepared ownership already exists.

    Given: A real prepared client binding is available to the event loop.
    When: A taskless loop callback attempts to open its authenticated socket state.
    Then: The binding reports the missing actual receive task without issuing a socket.
    """
    async with _bound_scenario(tmp_path, monkeypatch) as (_, value):
        result: asyncio.Future[str] = asyncio.get_running_loop().create_future()

        def attempt() -> None:
            """Observe the genuine taskless loop callback, without replacing asyncio task inspection."""
            try:
                _binding(value)._open_socket(
                    native_authority_uuid(8008), ActualReceiveCell(0), bridge._ACTUAL_CONNECT_SEAL
                )
            except PrivateAuthenticationError as error:
                result.set_result(str(error))

        asyncio.get_running_loop().call_soon(attempt)
        assert await result == "private_socket_actual_receive_task_missing"


async def test_cancelled_receive_cleanup_does_not_read_a_cancelled_task_exception(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Consume cancelled receiver completion without reading a cancelled exception.

    Given: A real receiver task has started and is subsequently cancelled and joined.
    When: The prepared binding receives that completed task through its cleanup callback.
    Then: Cleanup returns normally and retains no live receive dependency.
    """
    async with _bound_scenario(tmp_path, monkeypatch) as (_, value):
        started = asyncio.Event()

        async def receiver() -> None:
            """Supply an actual cancellable dependency without granting it socket authentication."""
            started.set()
            await asyncio.Event().wait()

        task = asyncio.create_task(receiver())
        await started.wait()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        _binding(value)._receive_finished(task)
        assert task.cancelled() and not _binding(value)._receive_tasks_snapshot()


@pytest.mark.parametrize("invalid", ("request", "path", "headers", "host", "url"))
def test_actual_handshake_requires_requested_url_path_and_host(invalid: str) -> None:
    """Require every requested and observed physical handshake coordinate.

    Given: The requested URL, path, headers or Host is missing or mismatched.
    When: The boundary verifies that physical handshake against the selected route.
    Then: It refuses the unproven route without inferring an alias or replacement endpoint.
    """
    physical = _BoundarySocket()
    requested: tuple[object, ...] = (_URL,)
    if invalid == "request":
        requested = ()
    elif invalid == "path":
        physical.request = SimpleNamespace(path="/other", headers={"Host": "futures.kraken.com"})
    elif invalid == "headers":
        physical.request = SimpleNamespace(path="/ws/v1", headers=None)
    elif invalid == "host":
        physical.request = SimpleNamespace(path="/ws/v1", headers={"Host": "other.invalid"})
    else:
        requested = ("wss://other.invalid/ws/v1",)
    with pytest.raises(
        PrivateAuthenticationError, match="private_socket_actual_handshake_route_unproven"
    ):
        bridge._verify_handshake(physical, requested, _URL)


async def test_bridge_delegates_public_traffic_and_refuses_missing_installed_boundaries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Preserve public delegation while refusing lost private installation boundaries.

    Given: Actual prepared setup has installed the private decoder and physical connect delegates.
    When: Public traffic passes through or delegates, registrations and ticket candidates change.
    Then: Public behavior remains ordinary and missing or unissued private boundaries fail closed.
    """
    async with _bound_scenario(tmp_path, monkeypatch) as (_, value):
        assert bridge._AuthenticatedJsonCodec.loads('{"public":17}') == {"public": 17}
        assert json.loads(bridge._AuthenticatedJsonCodec.dumps({"public": 17})) == {"public": 17}
        assert bridge._actual_binding(None) is None
        async with bridge._owned_connect(_URL) as physical:
            assert isinstance(physical, socket_fixture._ProviderSocket)
        with monkeypatch.context() as scoped:
            scoped.setattr(bridge._INSTALLED, "decoder", None)
            with pytest.raises(
                PrivateAuthenticationError, match="private_socket_decoder_delegate_missing"
            ):
                bridge._AuthenticatedJsonCodec.loads("{}")
            with pytest.raises(
                PrivateAuthenticationError, match="private_socket_encoder_delegate_missing"
            ):
                bridge._AuthenticatedJsonCodec.dumps({})
            with pytest.raises(
                PrivateAuthenticationError, match="private_socket_installed_boundary_changed"
            ):
                bridge.install_authenticated_private_socket_bridge()
        with monkeypatch.context() as scoped:
            scoped.setattr(bridge._INSTALLED, "connect", None)
            with pytest.raises(
                PrivateAuthenticationError, match="private_socket_connect_delegate_missing"
            ):
                async with bridge._owned_connect(_URL):
                    pytest.fail("A missing physical delegate must not open a context")
        with monkeypatch.context() as scoped:
            scoped.setattr(futures_websocket, "connect", None)
            with pytest.raises(
                PrivateAuthenticationError, match="private_socket_install_boundary_unsupported"
            ):
                bridge.install_authenticated_private_socket_bridge()
        raw = PrivateWireMessage({"feed": "fills"}, b'{"feed":"fills"}')
        assert bridge.authenticated_private_frame_ticket(raw) is None
        raw.__dict__[bridge._TICKET_ATTRIBUTE] = object()
        assert bridge.authenticated_private_frame_ticket(raw) is None
        assert (
            bridge.authenticated_private_frame_ticket(
                {"feed": "fills", bridge._TICKET_ATTRIBUTE: "provider-authored-ticket-shape"}
            )
            is None
        )
        assert bridge._actual_binding(receive._PrivateRun(value.ws, value.ws._conn, 0)) is _binding(
            value
        )
        monkeypatch.delitem(bridge._AUTHENTICATED_CLIENTS, value.ws._conn)
        assert bridge._actual_binding(receive._PrivateRun(value.ws, value.ws._conn, 0)) is None


async def test_registered_binding_cannot_downgrade_after_its_original_weak_owner_disappears(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse registered ownership whose original weak binding no longer exists.

    Given: A real prepared binding has disappeared while its weak registry entry remains.
    When: The private run looks up that original registered owner.
    Then: Lookup reports unavailable ownership instead of silently selecting public behavior.
    """
    async with _bound_scenario(tmp_path, monkeypatch) as (_, value):
        missing = bridge._AuthenticatedClientBinding(
            value.ws, value.prepared.require_token(), value.current
        )
        lost = ref(missing)
        del missing
        gc.collect()
        assert lost() is None
        monkeypatch.setitem(bridge._AUTHENTICATED_CLIENTS, value.ws._conn, lost)
        with pytest.raises(
            PrivateAuthenticationError, match="private_socket_original_binding_unavailable"
        ):
            bridge._actual_binding(receive._PrivateRun(value.ws, value.ws._conn, 0))


async def test_copied_receive_scope_without_an_actual_epoch_cannot_open_an_authenticated_socket(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reject nominal receive context that lacks an actual physical epoch.

    Given: A copied run label names the current task but carries no real connection epoch.
    When: It attempts the owned connect path or constructs an authenticated socket wrapper.
    Then: Both boundaries refuse issuance and the original context variables are restored.
    """
    async with _bound_scenario(tmp_path, monkeypatch) as (_, value):
        task = asyncio.current_task()
        assert task is not None
        scope = receive._PrivateRun(value.ws, value.ws._conn, id(task))
        private_token = receive._CURRENT_PRIVATE_RUN.set(scope)
        epoch_token = _CURRENT_EPOCH.set(None)
        try:
            with pytest.raises(
                PrivateAuthenticationError, match="private_socket_actual_connect_scope_unproven"
            ):
                async with bridge._owned_connect(_URL):
                    pytest.fail("A nominal receive label must not establish an actual socket")
            state = _unbound_state(value)
            with pytest.raises(
                PrivateAuthenticationError, match="private_socket_actual_epoch_missing"
            ):
                bridge._AuthenticatedSocket(
                    _BoundarySocket(), ActualReceiveCell(id(task)), state, scope
                )
        finally:
            _CURRENT_EPOCH.reset(epoch_token)
            receive._CURRENT_PRIVATE_RUN.reset(private_token)


async def test_actual_socket_rejects_inactive_dispatch_and_duplicate_issuance_binding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep actual socket issuance tied to its original active lifecycle.

    Given: A genuinely prepared client has opened its actual owned physical socket.
    When: Foreign decoder input, duplicate issuer binding and dispatch after close are attempted.
    Then: No source is consumed or reissued and inactive dispatch refuses after physical closure.
    """
    async with _bound_scenario(tmp_path, monkeypatch) as (_, value):
        await value.start()
        proxy = value.ws._conn.socket
        assert isinstance(proxy, bridge._AuthenticatedSocket)
        assert proxy.take_original("foreign source") is None
        unissued = OriginalReceive("foreign source", _NOW, native_authority_uuid(8009))
        assert proxy.observe_decoded(17, unissued) == 17
        with pytest.raises(PrivateAuthenticationError, match="private_auth_socket_issuer_changed"):
            proxy._authentication._bind_actual_socket_issuer(
                _ActualPrivateFrameIssuer(), ActualReceiveCell(0)
            )
        await proxy.close()
        assert value.sockets[0].state is State.CLOSED
        with pytest.raises(
            PrivateAuthenticationError, match="private_socket_actual_dispatch_scope_changed"
        ):
            proxy._guard_authentication_send("{}", datetime.now(UTC))
