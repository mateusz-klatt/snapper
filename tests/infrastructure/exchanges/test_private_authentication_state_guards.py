"""Refuse actual private authentication state changes at original SDK send and receive boundaries."""

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Literal

import pytest

import snapper.infrastructure.exchanges.kraken_futures_authenticated_socket as bridge
import tests.infrastructure.exchanges.test_authenticated_private_socket as socket_fixture
from snapper.core.private_frame_ticket import _ActualPrivateFrameIssuer
from snapper.infrastructure.exchanges.execution_wire_receipts import PrivateWireMessage
from snapper.infrastructure.exchanges.owned_private_socket import ActualReceiveCell
from snapper.infrastructure.exchanges.owned_private_socket import OwnedPrivateSocket
from snapper.infrastructure.exchanges.prepared_private_key import PreparedKeyProofReference
from snapper.infrastructure.exchanges.private_socket_authentication import (
    PreparedPrivateRuntimeBinding,
)
from snapper.infrastructure.exchanges.private_socket_authentication import (
    PrivateAuthenticationError,
)
from snapper.infrastructure.exchanges.private_socket_authentication import (
    PrivateSocketAuthentication,
)
from tests.helpers.native_account_authority import native_authority_scenario


def _retain_actual_states(monkeypatch: pytest.MonkeyPatch) -> list[PrivateSocketAuthentication]:
    """Observe the actual issuer-binding callback while preserving its original validation and objects."""
    states: list[PrivateSocketAuthentication] = []
    original = PrivateSocketAuthentication._bind_actual_socket_issuer

    def bind_original(
        state: PrivateSocketAuthentication,
        issuer: _ActualPrivateFrameIssuer,
        actual_receive_cell: ActualReceiveCell,
    ) -> None:
        """Retain only the actual state after the real connect adapter has bound its issued objects."""
        original(state, issuer, actual_receive_cell)
        states.append(state)

    monkeypatch.setattr(PrivateSocketAuthentication, "_bind_actual_socket_issuer", bind_original)
    return states


@pytest.mark.parametrize("event", ("error", "info"))
async def test_actual_error_or_information_response_cannot_replace_subscription_acknowledgement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, event: Literal["error", "info"]
) -> None:
    """Keep original financial bytes held when the real provider sends a non-ACK control response.

    Given: Committed key proof and a real SDK challenge lead to an actual encoded subscription.
    When: The local provider changes its response event to error or info before original receive.
    Then: The consumed control blocks or leaves pending authentication, and the financial ticket is absent.
    """
    states = _retain_actual_states(monkeypatch)
    original_ack = socket_fixture._ProviderSocket._acknowledgement
    original_recv = socket_fixture._ProviderSocket.recv
    boundary_states: list[tuple[bool, bool]] = []

    def provider_response(
        socket: socket_fixture._ProviderSocket, encoded: dict[str, object]
    ) -> dict[str, object]:
        """Fault only the actual provider response event, retaining its real encoded-request origin."""
        response = original_ack(socket, encoded)
        response["event"] = event
        return response

    async def observe_financial_boundary(socket: socket_fixture._ProviderSocket) -> str:
        """Inspect actual consumed-control state before the unchanged original financial receive."""
        entity = await original_recv(socket)
        if entity is socket.owner.financial_wire:
            assert len(states) == 1
            state = states[0]
            assert "fills" not in state._acknowledged
            boundary_states.append(("fills" in state._blocked, "fills" in state._pending))
        return entity

    monkeypatch.setattr(socket_fixture._ProviderSocket, "_acknowledgement", provider_response)
    monkeypatch.setattr(socket_fixture._ProviderSocket, "recv", observe_financial_boundary)
    async with (
        native_authority_scenario(tmp_path, monkeypatch) as source,
        socket_fixture._scenario(source, monkeypatch, "valid") as value,
    ):
        task = await value.start()
        await value.ws.subscribe(feed="fills")
        await task
        assert value.proof_seen_at_send == 2
        assert boundary_states == [(event == "error", event == "info")]
        assert any(message.get("event") == event for message in value.messages)
        assert value.tickets == [None]
        financial = value.messages[-1]
        assert isinstance(financial, PrivateWireMessage)
        assert financial.wire_bytes == value.financial_wire.encode()


@pytest.mark.parametrize("fault", ("key", "issuer"))
async def test_actual_financial_receive_rechecks_current_key_and_original_socket_issuer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: Literal["key", "issuer"]
) -> None:
    """Preserve original financial bytes while refusing changes after a successful actual ACK.

    Given: The genuine local SDK has consumed its matching ACK with a committed actual key proof.
    When: Its real key changes or its original issuer is lost immediately before physical financial receive.
    Then: Current proof or issuer validation refuses safely, leaving that financial frame without a ticket.
    """
    states = _retain_actual_states(monkeypatch)
    original_recv = socket_fixture._ProviderSocket.recv
    original_proof = PreparedPrivateRuntimeBinding.current_proof
    proof_refusals: list[str] = []
    lost_issuers: list[_ActualPrivateFrameIssuer] = []
    changed_boundaries: list[str] = []

    def observe_current_proof(
        binding: PreparedPrivateRuntimeBinding, observed_at: datetime
    ) -> PreparedKeyProofReference:
        """Call the actual runtime proof and retain only its fixed public refusal classification."""
        try:
            return original_proof(binding, observed_at)
        except PrivateAuthenticationError as error:
            proof_refusals.append(str(error))
            raise

    async def change_at_original_receive(socket: socket_fixture._ProviderSocket) -> str:
        """Change one adversarial dependency after real ACK consumption and before source handoff."""
        entity = await original_recv(socket)
        if entity is socket.owner.financial_wire:
            assert len(states) == 1
            state = states[0]
            assert "fills" in state._acknowledged and "fills" not in state._pending
            issuer = state._actual_socket_issuer
            assert issuer is not None
            if fault == "key":
                socket.owner.ws._key = "SYNTHETIC_KEY_CHANGED_BEFORE_ORIGINAL_FINANCIAL_RECEIVE"
            else:
                lost_issuers.append(issuer)
                state._actual_socket_issuer = None
            changed_boundaries.append(fault)
        return entity

    monkeypatch.setattr(PreparedPrivateRuntimeBinding, "current_proof", observe_current_proof)
    monkeypatch.setattr(socket_fixture._ProviderSocket, "recv", change_at_original_receive)
    try:
        async with (
            native_authority_scenario(tmp_path, monkeypatch) as source,
            socket_fixture._scenario(source, monkeypatch, "valid") as value,
        ):
            task = await value.start()
            await value.ws.subscribe(feed="fills")
            await task
            assert changed_boundaries == [fault]
            assert value.proof_seen_at_send == 2 and value.tickets == [None]
            expected = ["private_auth_actual_key_or_route_changed"] if fault == "key" else []
            assert proof_refusals == expected
            financial = value.messages[-1]
            assert isinstance(financial, PrivateWireMessage)
            assert financial.wire_bytes == value.financial_wire.encode()
    finally:
        for issuer in lost_issuers:
            issuer.close()


async def test_repeated_original_encoded_subscription_revokes_an_actually_acknowledged_feed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Refuse same-feed reauthentication through the actual owned proxy after a real matching ACK.

    Given: The actual SDK encoded a subscription and consumed its provider-generated matching ACK.
    When: The identical transient encoded source is sent again through that same actual owned proxy.
    Then: The acknowledged feed is blocked before the pending original financial receive can gain a ticket.
    """
    states = _retain_actual_states(monkeypatch)
    original_send = socket_fixture._ProviderSocket.send
    original_recv = socket_fixture._ProviderSocket.recv
    financial_waiting, release_financial = asyncio.Event(), asyncio.Event()
    encoded_subscriptions: list[str] = []

    async def retain_original_encoded_send(
        socket: socket_fixture._ProviderSocket, entity: str
    ) -> None:
        """Keep the actual SDK's encoded subscription only in this test's transient memory."""
        document: object = json.loads(entity)
        assert isinstance(document, dict)
        if document.get("event") == "subscribe":
            encoded_subscriptions.append(entity)
        await original_send(socket, entity)

    async def await_original_financial_release(socket: socket_fixture._ProviderSocket) -> str:
        """Pause only the actual financial return, after the preceding real ACK was consumed."""
        entity = await original_recv(socket)
        if entity is socket.owner.financial_wire:
            financial_waiting.set()
            await release_financial.wait()
        return entity

    monkeypatch.setattr(socket_fixture._ProviderSocket, "send", retain_original_encoded_send)
    monkeypatch.setattr(socket_fixture._ProviderSocket, "recv", await_original_financial_release)
    try:
        async with (
            native_authority_scenario(tmp_path, monkeypatch) as source,
            socket_fixture._scenario(source, monkeypatch, "valid") as value,
        ):
            task = await value.start()
            await value.ws.subscribe(feed="fills")
            await financial_waiting.wait()
            assert len(states) == 1 and len(encoded_subscriptions) == 1
            state = states[0]
            assert "fills" in state._acknowledged and "fills" not in state._pending
            proxy = value.ws._conn.socket
            assert isinstance(proxy, OwnedPrivateSocket)
            assert isinstance(proxy, bridge._AuthenticatedSocket)
            await proxy.send(encoded_subscriptions[0])
            assert encoded_subscriptions[1] is encoded_subscriptions[0]
            assert "fills" in state._blocked and "fills" not in state._acknowledged
            assert value.sockets[0].sent_events == ["challenge", "subscribe", "subscribe"]
            release_financial.set()
            await task
            assert value.proof_seen_at_send == 3 and value.tickets == [None]
            financial = value.messages[-1]
            assert isinstance(financial, PrivateWireMessage)
            assert financial.wire_bytes == value.financial_wire.encode()
    finally:
        release_financial.set()
        encoded_subscriptions.clear()
