"""Unrun real SQLite/signed-local-HTTP/pinned-SDK ownership bridge counterexamples.

Every positive key proof comes from the actual closed producer and requests
adapter. No caller-authored generic balance or shape-only token is a permit.
No venue connection, financial order or product activation is involved.
"""

import asyncio
import json
import logging
import pickle
import sqlite3
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from contextlib import asynccontextmanager
from contextlib import closing
from copy import deepcopy
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Literal
from typing import cast
from weakref import WeakKeyDictionary

import kraken.futures.websocket as futures_websocket
import pytest
from kraken.futures import FuturesWSClient
from kraken.futures.websocket import ConnectFuturesWebsocket
from requests import Session
from websockets.protocol import State

import snapper.infrastructure.exchanges.kraken_futures_authenticated_socket as bridge
import snapper.infrastructure.exchanges.kraken_futures_private_receive as receive
from snapper.application.trade.native_private_owner_preparation import PreparedPrivateOwnerRead
from snapper.application.trade.native_private_owner_preparation import prepare_native_private_owner
from snapper.core.private_frame_ticket import AuthenticatedPrivateFrameTicket
from snapper.core.private_frame_ticket import require_authenticated_private_frame
from snapper.data.native_account_identity_store import find_native_account_identity
from snapper.infrastructure.exchanges.authenticated_private_shutdown import (
    PrivateShutdownUnresolvedError,
)
from snapper.infrastructure.exchanges.execution_wire_receipts import PrivateWireMessage
from snapper.infrastructure.exchanges.owned_private_socket import OriginalReceive
from snapper.infrastructure.exchanges.owned_private_socket import PrivateSocketBoundaryError
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyToken
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyTokenError
from snapper.infrastructure.exchanges.private_socket_authentication import PrivateRuntimeSelection
from tests.helpers.native_account_authority import NativeAuthorityScenario
from tests.helpers.native_account_authority import native_authority_scenario
from tests.helpers.native_account_authority import native_authority_uuid

_RUN = "_ConnectFuturesWebsocket__run"
_BACKOFF = "_ConnectFuturesWebsocket__get_reconnect_wait"
_KEY = "synthetic-key"
_CHANGED = "SYNTHETIC_CHANGED_DURING_CHALLENGE"
_CHALLENGE = "SYNTHETIC_PRIVATE_CHALLENGE_MARKER"
_FILL = '{"feed":"fills","account":"DemoUser","fills":[{"qty":0.100000000000000001}]}'


@dataclass(frozen=True)
class _RuntimeContainer:
    """A genuine dataclass whose generic traversal must stop at its opaque runtime member."""

    member: object


class _ProviderSocket:
    """Actual encoded SDK requests drive the local provider's original control/financial replies."""

    def __init__(self, owner: _BridgeScenario) -> None:
        """Keep only test-local queues and safe observed event names; no real network is opened."""
        self.owner = owner
        self.incoming: asyncio.Queue[str] = asyncio.Queue()
        self.state = State.OPEN
        self.request = SimpleNamespace(path="/ws/v1", headers={"Host": "futures.kraken.com"})
        self.sent_events: list[str] = []
        self.reads = 0

    async def recv(self) -> str:
        """Return original source objects to the SDK's actual wait_for child task."""
        self.reads += 1
        if self.owner.mode == "reconnect" and self is self.owner.sockets[0] and self.reads > 3:
            raise ConnectionError("synthetic provider physical disconnect")
        return await self.incoming.get()

    def _assert_proof_committed(self) -> None:
        """Observe the actual proof using an independent SQLite connection at physical auth send."""
        path = self.owner.source.adapter.path
        with closing(sqlite3.connect(f"file:{path}?mode=ro", uri=True)) as connection:
            rows = connection.execute(
                "SELECT public_id, canonical_payload FROM native_account_identity_proofs"
            ).fetchall()
        assert self.owner.proof_public_id in {row[0] for row in rows}
        assert all(_KEY not in row[1] and _CHALLENGE not in row[1] for row in rows)
        self.owner.proof_seen_at_send += 1

    def _acknowledgement(self, value: dict[str, object]) -> dict[str, object]:
        """Generate exact or deliberately invalid source ACKs without storing auth bodies in results."""
        result = {
            "event": "subscribed",
            "feed": value["feed"],
            "api_key": value["api_key"],
            "original_challenge": value["original_challenge"],
            "signed_challenge": value["signed_challenge"],
        }
        if self.owner.mode == "wrong_key":
            result["api_key"] = _CHANGED
        elif self.owner.mode == "wrong_challenge":
            result["original_challenge"] = "SYNTHETIC_WRONG_CHALLENGE"
        elif self.owner.mode == "missing_signature":
            result.pop("signed_challenge")
        elif self.owner.mode == "failed_ack":
            result["event"] = "subscribed_failed"
        return result

    async def send(self, source: str) -> None:
        """Measure actual physical invocation, including ACK arrival before send backpressure releases."""
        value: object = json.loads(source)
        assert isinstance(value, dict) and value["api_key"] == _KEY
        event = value.get("event")
        assert isinstance(event, str)
        self.sent_events.append(event)
        self._assert_proof_committed()
        if event == "challenge":
            if self.owner.mode == "mutated_key":
                self.owner.ws._key = _CHANGED
            await self.incoming.put(json.dumps({"event": "challenge", "message": _CHALLENGE}))
            return
        assert event == "subscribe" and value["feed"] == self.owner.subscription_feed
        assert value["signed_challenge"] == self.owner.ws.get_sign_challenge(_CHALLENGE)
        ack = json.dumps(self._acknowledgement(value))
        if self.owner.mode == "financial_before_ack":
            await self.incoming.put(self.owner.financial_wire)
        await self.incoming.put(ack)
        if self.owner.mode == "duplicate_ack":
            await self.incoming.put(ack)
        if self.owner.mode != "financial_before_ack":
            await self.incoming.put(self.owner.financial_wire)
        if self.owner.mode == "backpressure":
            await self.owner.release_send.wait()
        self.owner.physical_send_completed.set()

    async def close(self) -> None:
        """Close the same actual local physical socket, preserving no current ownership afterward."""
        self.state = State.CLOSED


class _BridgeScenario:
    """One actual closed owner read and actual registered private subscriber."""

    def __init__(
        self,
        source: NativeAuthorityScenario,
        prepared: PreparedPrivateOwnerRead,
        mode: str,
        subscription_feed: Literal["fills", "account_log"] = "fills",
        financial_wire: str | None = None,
    ) -> None:
        """Own public test observations while the opaque production token stays inside its real path."""
        self.source = source
        self.prepared = prepared
        self.mode = mode
        self.subscription_feed = subscription_feed
        self.financial_wire = _FILL if financial_wire is None else financial_wire
        self.proof_public_id = ""
        self.proof_seen_at_send = 0
        self.sockets: list[_ProviderSocket] = []
        self.messages: list[Mapping[str, object]] = []
        self.tickets: list[AuthenticatedPrivateFrameTicket | None] = []
        self.frame_seen = asyncio.Event()
        self.callback_cancelled = asyncio.Event()
        self.release_callback = asyncio.Event()
        self.self_close_refusal: str | None = None
        self.release_send = asyncio.Event()
        self.physical_send_completed = asyncio.Event()
        self.task: asyncio.Task[None] | None = None
        self.ws = FuturesWSClient(
            key=_KEY, secret="c3ludGhldGljLXNlY3JldA==", callback=self.callback
        )
        source.client._private_ws_client = self.ws

    def current(self) -> PrivateRuntimeSelection:
        """Read actual parent-owned instances instead of guessing identities from a decoded frame."""
        user = self.source.client._user_client
        ws = self.source.client._private_ws_client
        selected = self.source.client.execution_client_selection()
        assert user is not None and ws is not None and selected is not None
        return PrivateRuntimeSelection(user, ws, selected)

    async def callback(self, message: Mapping[str, object]) -> None:
        """Observe only real SDK forwarding and preserve the original already issued ticket."""
        self.messages.append(message)
        if isinstance(message, PrivateWireMessage):
            self.tickets.append(bridge.authenticated_private_frame_ticket(message))
            self.frame_seen.set()
            if self.mode in {"callback_close", "callback_close_child"}:
                try:
                    if self.mode == "callback_close_child":
                        await asyncio.create_task(self.ws.close())
                    else:
                        await self.ws.close()
                except PrivateShutdownUnresolvedError as error:
                    self.self_close_refusal = str(error)
            if self.mode == "blocked_callback":
                while not self.release_callback.is_set():
                    try:
                        await self.release_callback.wait()
                    except asyncio.CancelledError:
                        self.callback_cancelled.set()
            if self.mode != "reconnect" or len(self.tickets) >= 2:
                self.ws._conn.keep_alive = False

    @asynccontextmanager
    async def connect(self, *args: object, **kwargs: object) -> AsyncIterator[_ProviderSocket]:
        """Supply the local actual SDK context; the production bridge wraps this exact socket."""
        assert args[0] == "wss://futures.kraken.com/ws/v1"
        socket = _ProviderSocket(self)
        self.sockets.append(socket)
        try:
            yield socket
        finally:
            socket.state = State.CLOSED

    async def start(self) -> asyncio.Task[None]:
        """Use the actual receive method after production prepared-key/socket registration."""
        connected = asyncio.Event()
        run = cast(Callable[[asyncio.Event], Awaitable[None]], getattr(self.ws._conn, _RUN))

        async def run_connector() -> None:
            """Await the actual SDK receive coroutine through its declared asynchronous interface."""
            await run(connected)

        self.task = asyncio.create_task(run_connector())
        await connected.wait()
        return self.task

    async def close(self) -> None:
        """Cancel/reap only the owned receive task and close all actual SDK sessions."""
        self.release_send.set()
        self.release_callback.set()
        self.ws._conn.keep_alive = False
        if self.task is not None:
            if not self.task.done():
                self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
        await self.ws.close()
        owned_session = getattr(self.ws, "_FuturesClient__session", None)
        if isinstance(owned_session, Session):
            owned_session.close()
        self.prepared.close()


def _restore_bridge(monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolate actual module-local delegates while preserving each production installer call."""
    monkeypatch.setattr(ConnectFuturesWebsocket, _RUN, getattr(ConnectFuturesWebsocket, _RUN))
    monkeypatch.setattr(futures_websocket, "json", futures_websocket.json)
    monkeypatch.setattr(futures_websocket, "connect", futures_websocket.connect)
    monkeypatch.setattr(receive._INSTALLED, "delegate", receive._INSTALLED.delegate)
    monkeypatch.setattr(receive, "_OWNED", WeakKeyDictionary())
    monkeypatch.setattr(bridge._INSTALLED, "decoder", bridge._INSTALLED.decoder)
    monkeypatch.setattr(bridge._INSTALLED, "connect", bridge._INSTALLED.connect)
    monkeypatch.setattr(bridge, "_AUTHENTICATED_CLIENTS", WeakKeyDictionary())


@asynccontextmanager
async def _scenario(
    source: NativeAuthorityScenario,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    subscription_feed: Literal["fills", "account_log"] = "fills",
    financial_wire: str | None = None,
) -> AsyncIterator[_BridgeScenario]:
    """Prepare one actual same-attempt owner before constructing or authenticating the private SDK."""
    with monkeypatch.context() as scoped:
        _restore_bridge(scoped)
        prepared = await prepare_native_private_owner(
            source.repository, source.client, source.tracker
        )
        assert prepared.outcome.payload.result.kind == "captured"
        async with source.repository.session() as session:
            proof = await find_native_account_identity(
                session, prepared.outcome.payload.attempt_public_id
            )
            assert proof is not None and proof.payload.matches_owner
        value = _BridgeScenario(source, prepared, mode, subscription_feed, financial_wire)
        value.proof_public_id = proof.public_id
        try:
            scoped.setattr(futures_websocket, "connect", value.connect)
            receive.bind_owned_futures_private_callback(value.ws, value.callback)
            bridge.bind_authenticated_private_client(
                value.ws, prepared.require_token(), value.current
            )
            yield value
        finally:
            await value.close()


@pytest.mark.parametrize("mode", ("valid", "backpressure"))
async def test_actual_key_commit_authentication_and_receive_mint_original_ticket(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    """A real signed closed read and real SDK auth path produce a source-backed runtime capability.

    Given: Actual retained key identity and same-attempt accounts source from local signed HTTP,
    When: Actual SDK challenge/subscribe/ACK/financial receive runs, including send backpressure,
    Then: The exact original frame gains its committed proof while its account label stays unchanged.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _scenario(source, monkeypatch, mode) as value,
    ):
        task = await value.start()
        subscription = asyncio.create_task(value.ws.subscribe(feed="fills"))
        await value.frame_seen.wait()
        if mode == "backpressure":
            assert not value.physical_send_completed.is_set() and not subscription.done()
        value.release_send.set()
        await subscription
        await task
        ticket = value.tickets[0]
        assert ticket is not None and value.proof_seen_at_send == 2
        context = require_authenticated_private_frame(ticket, _FILL.encode())
        assert context.key_proof_public_id == value.proof_public_id
        assert context.client_selection == source.selection
        assert context.subscription_feed == "fills" and context.raw_feed == "fills"
        assert value.messages[-1]["account"] == "DemoUser"
        assert ticket.matches_financial_wire(_FILL.encode())
        assert require_authenticated_private_frame(ticket, _FILL.encode()) == context


@pytest.mark.parametrize(
    "mode",
    (
        "wrong_key",
        "wrong_challenge",
        "missing_signature",
        "failed_ack",
        "duplicate_ack",
        "financial_before_ack",
    ),
)
async def test_actual_bad_or_late_ack_preserves_financial_wire_without_ownership(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Invalid control paths preserve financial raw while refusing ownership and secret logging.

    Given: Actual committed key proof followed by a malformed or late physical subscription ACK,
    When: The real SDK still forwards a safely classified financial frame,
    Then: Its exact bytes survive with no authenticated ticket or synthetic secret in logs.
    """
    caplog.set_level(logging.DEBUG, logger="kraken.futures.websocket")
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _scenario(source, monkeypatch, mode) as value,
    ):
        task = await value.start()
        await value.ws.subscribe(feed="fills")
        await task
        assert value.tickets == [None]
        financial = value.messages[-1]
        assert isinstance(financial, PrivateWireMessage) and financial.wire_bytes == _FILL.encode()
        assert _KEY not in caplog.text and _CHALLENGE not in caplog.text


async def test_actual_challenge_wait_key_mutation_is_refused_before_physical_subscribe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A later mutable SDK key cannot replace the actual key proven by the closed preflight.

    Given: Actual prepared identity and the SDK's genuine awaited challenge exchange,
    When: The private SDK key changes before its actual auth request encoding,
    Then: The changed subscription never reaches the physical socket and no financial ticket exists.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _scenario(source, monkeypatch, "mutated_key") as value,
    ):
        await value.start()
        with pytest.raises(PrivateSocketBoundaryError, match="private_socket_send_refused"):
            await value.ws.subscribe(feed="fills")
        assert value.sockets[0].sent_events == ["challenge"]
        assert not value.tickets


async def test_public_ticket_constructor_cannot_forge_an_actual_registered_receive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Equal public fields and bytes cannot mint the private source capability accepted by persistence.

    Given: One actual issued ticket from the real closed key/socket/ACK/receive path,
    When: A caller constructs a second same-shaped ticket or inserts it into a reconstructed mapping,
    Then: The constructed candidate is refused while the original historical ticket remains valid.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _scenario(source, monkeypatch, "valid") as value,
    ):
        task = await value.start()
        await value.ws.subscribe(feed="fills")
        await task
        actual = value.tickets[0]
        assert actual is not None
        forged = AuthenticatedPrivateFrameTicket(actual.context, _FILL.encode())
        assert forged.matches_financial_wire(_FILL.encode())
        with pytest.raises(ValueError, match="not actually issued"):
            require_authenticated_private_frame(forged, _FILL.encode())
        reconstructed = PrivateWireMessage({"feed": "fills"}, _FILL.encode())
        reconstructed.__dict__["_snapper_authenticated_private_frame_ticket"] = forged
        assert bridge.authenticated_private_frame_ticket(reconstructed) is None
        assert require_authenticated_private_frame(actual, _FILL.encode()) == actual.context


async def test_public_key_token_constructor_cannot_borrow_an_actual_safe_proof(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An actual safe stored proof cannot authorize an arbitrarily constructed expected-key token.

    Given: Actual retained native identity from the real local signed HTTP producer,
    When: A caller manually constructs a token with that public observation,
    Then: Proof binding refuses it because actual prepared-header transport never issued the token.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _scenario(source, monkeypatch, "valid") as value,
    ):
        async with source.repository.session() as session:
            proof = await find_native_account_identity(
                session, value.prepared.outcome.payload.attempt_public_id
            )
            assert proof is not None
        user = source.client._user_client
        assert user is not None
        forged = PreparedKeyToken(user, _KEY, proof.payload.observation)
        with pytest.raises(PreparedKeyTokenError, match="not_transport_issued"):
            forged.bind_committed_proof(proof.public_id, proof.payload)


async def test_runtime_boxes_and_issued_tickets_refuse_generic_secret_traversal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Runtime SDK pointers and original frame capabilities must not serialize by generic traversal.

    Given: Actual prepared ownership and a real issued financial receive ticket,
    When: Generic repr, pickle, deepcopy or dataclass conversion targets those opaque runtime boxes,
    Then: The operation refuses without exposing synthetic key/challenge/signature material.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _scenario(source, monkeypatch, "valid") as value,
    ):
        task = await value.start()
        await value.ws.subscribe(feed="fills")
        await task
        ticket = value.tickets[0]
        assert ticket is not None
        boxes = (ticket, value.prepared.require_token(), value.current())
        for box in boxes:
            assert _KEY not in repr(box) and _CHALLENGE not in repr(box)
            for serialize in (pickle.dumps, deepcopy):
                with pytest.raises(TypeError):
                    serialize(box)
            with pytest.raises(TypeError):
                asdict(_RuntimeContainer(box))
        fake_receive = OriginalReceive(
            _FILL, ticket.context.frame_observed_at, native_authority_uuid(900)
        )
        assert fake_receive.matches_financial_wire(_FILL.encode())


async def test_actual_account_log_subscription_preserves_its_original_namespace_and_precise_wire(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Account-log ownership requires its own actual encoded subscription and matching ACK.

    Given: Real selected-key/account HTTP and a local provider offering one original account-log entry,
    When: The pinned SDK subscribes to account_log and receives its exact ACK and original JSON,
    Then: The issued ticket names account_log with unchanged financial bytes and no fabricated UID header.
    """
    wire = '{"feed":"account_log","new_entry":{"id":17,"booking_uid":"cash-source-17","date":"2026-10-23T15:00:00Z","asset":"USD","collateral":"USD","margin_account":"flex","info":"transfer","old_balance":0,"new_balance":100.000000000000000001}}'
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _scenario(source, monkeypatch, "valid", "account_log", wire) as value,
    ):
        task = await value.start()
        await value.ws.subscribe(feed="account_log")
        await task
        ticket = value.tickets[0]
        assert ticket is not None
        context = require_authenticated_private_frame(ticket, wire.encode())
        assert context.subscription_feed == context.raw_feed == "account_log"
        assert ticket.matches_financial_wire(wire.encode())
        assert "account" not in value.messages[-1]
        assert value.proof_seen_at_send == 2


@pytest.mark.parametrize("accessor", ("context", "validator"))
async def test_actual_ticket_public_context_views_cannot_change_original_receive_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, accessor: str
) -> None:
    """Safe public context views cannot relabel the actual source facts cached by an issued ticket.

    Given: Actual signed preflight and pinned SDK challenge/subscribe/ACK/financial receive,
    When: A consumer changes the public context model view returned by either supported accessor,
    Then: Later verified context retains its original physical receive identity and source bytes.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _scenario(source, monkeypatch, "valid") as value,
    ):
        task = await value.start()
        await value.ws.subscribe(feed="fills")
        await task
        ticket = value.tickets[0]
        assert ticket is not None
        original = require_authenticated_private_frame(ticket, _FILL.encode())
        view = (
            ticket.context
            if accessor == "context"
            else require_authenticated_private_frame(ticket, _FILL.encode())
        )
        view.__dict__["frame_receive_public_id"] = native_authority_uuid(9999)
        assert require_authenticated_private_frame(ticket, _FILL.encode()) == original


async def test_actual_owned_close_survives_double_cancellation_until_original_callback_joins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep unresolved original ownership occupied while actual callback cancellation is delayed.

    Given: A real issued owner and pinned SDK callback holding its genuine original receive task.
    When: Two external close callers are cancelled while that callback delays its actual termination.
    Then: The independently supervised cleanup retains the token until the real callback joins.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _scenario(source, monkeypatch, "blocked_callback") as value,
    ):
        run = await value.start()
        await value.ws.subscribe(feed="fills")
        await value.frame_seen.wait()
        first = asyncio.create_task(value.ws.close())
        await value.callback_cancelled.wait()
        assert not bridge.authenticated_private_close_resolved(value.ws)
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        second = asyncio.create_task(value.ws.close())
        await asyncio.sleep(0)
        second.cancel()
        with pytest.raises(asyncio.CancelledError):
            await second
        assert not run.done() and not bridge.authenticated_private_close_resolved(value.ws)
        user = source.client._user_client
        assert user is not None
        token = value.prepared.require_token()
        token.assert_prepared_key(user, source.selection, _KEY, source.clock.current)
        ticket = value.tickets[0]
        assert ticket is not None
        value.release_callback.set()
        await run
        await value.ws.close()
        assert bridge.authenticated_private_close_resolved(value.ws)
        with pytest.raises(PreparedKeyTokenError, match="closed"):
            token.assert_prepared_key(user, source.selection, _KEY, source.clock.current)
        assert require_authenticated_private_frame(ticket, _FILL.encode()).key_proof_public_id


@pytest.mark.parametrize("child_task", (False, True))
async def test_actual_receive_callback_cannot_wait_on_its_own_owned_close(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, child_task: bool
) -> None:
    """Refuse the real receive-to-cleanup dependency cycle before a cleanup task can be created.

    Given: The actual pinned SDK is inside its original authenticated financial callback.
    When: That callback attempts to close the same SDK object whose receive it currently occupies.
    Then: It receives a fixed self-join refusal and a later external close joins and releases normally.
    """
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        _scenario(
            source, monkeypatch, "callback_close_child" if child_task else "callback_close"
        ) as value,
    ):
        run = await value.start()
        await value.ws.subscribe(feed="fills")
        await run
        assert value.self_close_refusal == "private_socket_close_from_receive_task"
        assert not bridge.authenticated_private_close_resolved(value.ws)
        assert value.tickets[0] is not None
        await value.ws.close()
        assert bridge.authenticated_private_close_resolved(value.ws)
