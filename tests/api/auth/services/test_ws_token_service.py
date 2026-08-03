"""Tests for WebSocket message dispatcher."""

import json
from typing import Any
from typing import cast

import pytest
from fastapi import WebSocket
from fastapi import WebSocketDisconnect

from snapper.api.auth.services.ws_token_service import WsTokenService
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.websocket_auth import WebSocketAuthManager
from snapper.interface.websocket.connection_manager import WebSocketConnectionManager
from snapper.interface.websocket.dispatcher import dispatch_messages
from snapper.messaging.infrastructure.publisher import SequenceTracker


class DummyWebSocket:
    """Fake WebSocket that returns queued messages."""

    def __init__(self, messages: list[str]) -> None:
        """Initialize the instance."""
        self._messages = messages
        self.sent: list[str] = []

    async def receive_text(self) -> str:
        """Return next queued message or raise disconnect."""
        if not self._messages:
            raise WebSocketDisconnect()
        return self._messages.pop(0)

    async def send_text(self, data: str) -> None:
        """Record sent message."""
        self.sent.append(data)


class DummyManager:
    """Placeholder connection manager for tests."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self._tracker = SequenceTracker()

    @property
    def tracker(self) -> SequenceTracker:
        """Provide sequence tracker for provenance stamping."""
        return self._tracker


class DummyWsAuthManager:
    """Placeholder WebSocket auth manager for tests.

    Carries the connection's principal because ``dispatch_messages`` resolves
    it from the manager once per message rather than taking it as an argument.
    """

    def __init__(self, principal: AuthPrincipal | None = None) -> None:
        """Store the principal this stub hands back to the dispatch loop."""
        self.principal = principal

    def get_authenticated_user(self, websocket: Any) -> AuthPrincipal | None:
        """Return the principal currently registered for the connection."""
        return self.principal

    async def on_client_ping(self, principal: Any) -> None:
        """No-op liveness hook matching the real manager's ping interface."""


class DummyTokenService:
    """Placeholder WS token service for tests."""

    pass


@pytest.mark.asyncio
async def test_dispatch_messages_handles_unknown_type() -> None:
    """Verify unknown message type returns error response.

    Given: A websocket with unknown message type queued,
    When: dispatch_messages processes the message,
    Then: Error response with "Invalid message format" is sent.
    """
    websocket = DummyWebSocket(messages=[json.dumps({"type": "unknown"})])
    manager = DummyManager()
    user = AuthPrincipal(username="alice", role=UserRole.VIEWER)
    ws_auth_manager = DummyWsAuthManager(user)
    token_service = DummyTokenService()
    await dispatch_messages(
        cast(WebSocket, websocket),
        cast(WebSocketConnectionManager, manager),
        cast(WebSocketAuthManager, ws_auth_manager),
        cast(WsTokenService, token_service),
    )
    assert len(websocket.sent) == 1
    payload: dict[str, Any] = json.loads(websocket.sent[0])
    assert payload["type"] == "error"
    assert "Invalid message format" in payload["message"]


class DisconnectingWebSocket:
    """Fake WebSocket that immediately disconnects."""

    async def receive_text(self) -> str:
        """Raise WebSocketDisconnect immediately."""
        raise WebSocketDisconnect()

    async def send_text(self, data: str) -> None:
        """Assert failure since no messages should be sent."""
        raise AssertionError("No messages should be sent on disconnect")


@pytest.mark.asyncio
async def test_dispatch_messages_handles_disconnect() -> None:
    """Verify WebSocketDisconnect is handled gracefully.

    Given: A websocket that raises WebSocketDisconnect,
    When: dispatch_messages is called,
    Then: Function completes without sending messages.
    """
    websocket = DisconnectingWebSocket()
    manager = DummyManager()
    user = AuthPrincipal(username="alice", role=UserRole.VIEWER)
    ws_auth_manager = DummyWsAuthManager(user)
    token_service = DummyTokenService()
    await dispatch_messages(
        cast(WebSocket, websocket),
        cast(WebSocketConnectionManager, manager),
        cast(WebSocketAuthManager, ws_auth_manager),
        cast(WsTokenService, token_service),
    )


class ErroringWebSocket:
    """Fake WebSocket that raises exception on receive."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.sent: list[str] = []

    async def receive_text(self) -> str:
        """Raise simulated internal error."""
        raise ValueError("Simulated internal error")

    async def send_text(self, data: str) -> None:
        """Record sent message."""
        self.sent.append(data)


@pytest.mark.asyncio
async def test_dispatch_messages_handles_unexpected_exception() -> None:
    """Verify unexpected exceptions send error message.

    Given: A websocket that raises unexpected exception,
    When: dispatch_messages is called,
    Then: Error message is sent before terminating.
    """
    websocket = ErroringWebSocket()
    manager = DummyManager()
    user = AuthPrincipal(username="bob", role=UserRole.VIEWER)
    ws_auth_manager = DummyWsAuthManager(user)
    token_service = DummyTokenService()
    await dispatch_messages(
        cast(WebSocket, websocket),
        cast(WebSocketConnectionManager, manager),
        cast(WebSocketAuthManager, ws_auth_manager),
        cast(WsTokenService, token_service),
    )
    assert len(websocket.sent) == 1


class MultiMessageWebSocket:
    """Fake WebSocket that returns multiple queued messages."""

    def __init__(self, messages: list[str]) -> None:
        """Initialize the instance."""
        self._messages = messages
        self.sent: list[str] = []

    async def receive_text(self) -> str:
        """Return next queued message or raise disconnect."""
        if not self._messages:
            raise WebSocketDisconnect()
        return self._messages.pop(0)

    async def send_text(self, data: str) -> None:
        """Record sent message."""
        self.sent.append(data)


class MockConnectionManager:
    """Mock connection manager tracking subscriptions."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.active_connections: list[Any] = [object()]
        self._subscriptions: dict[Any, set[str]] = {}
        self._tracker = SequenceTracker()

    @property
    def tracker(self) -> SequenceTracker:
        """Provide sequence tracker for provenance stamping."""
        return self._tracker

    def get_client_subscriptions(self, websocket: Any) -> set[str]:
        """Return subscriptions for given websocket."""
        return self._subscriptions.get(websocket, set())


@pytest.mark.asyncio
async def test_dispatch_messages_loop_continues_after_ping() -> None:
    """Verify message loop continues after processing ping.

    Given: A websocket with multiple ping messages queued,
    When: dispatch_messages processes messages,
    Then: All messages are processed with pong responses.
    """
    websocket = MultiMessageWebSocket(
        messages=[
            json.dumps(
                {
                    "type": "ping",
                    "session_id": "",
                    "sequence_id": 0,
                    "public_id": "test",
                    "timestamp": "2024-01-01T00:00:00Z",
                }
            ),
            json.dumps(
                {
                    "type": "ping",
                    "session_id": "",
                    "sequence_id": 0,
                    "public_id": "test",
                    "timestamp": "2024-01-01T00:00:00Z",
                }
            ),
        ]
    )
    manager = MockConnectionManager()
    user = AuthPrincipal(username="alice", role=UserRole.VIEWER)
    ws_auth_manager = DummyWsAuthManager(user)
    token_service = DummyTokenService()
    await dispatch_messages(
        cast(WebSocket, websocket),
        cast(WebSocketConnectionManager, manager),
        cast(WebSocketAuthManager, ws_auth_manager),
        cast(WsTokenService, token_service),
    )
    assert len(websocket.sent) == 2
    pong1 = json.loads(websocket.sent[0])
    assert pong1["type"] == "pong"
    pong2 = json.loads(websocket.sent[1])
    assert pong2["type"] == "pong"
