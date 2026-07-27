"""Tests for the ping-path delegate-liveness hook in the WS dispatcher.

Covers the dispatcher side of plan P1's GAP-2 fix: the ping branch of
the dispatch table routes through ``_handle_ping_with_liveness`` which
answers the pong first, then hands the principal to
:meth:`WebSocketAuthManager.on_client_ping` so a connected AI delegate
keeps refreshing ``ai_delegates.last_seen_at`` between connect and
disconnect.
"""

import json
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import WebSocket
from fastapi import WebSocketDisconnect

from snapper.api.auth.services.ws_token_service import WsTokenService
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.websocket_auth import WebSocketAuthManager
from snapper.interface.websocket.connection_manager import WebSocketConnectionManager
from snapper.interface.websocket.dispatcher import _handle_ping_with_liveness
from snapper.interface.websocket.dispatcher import dispatch_messages
from snapper.messaging.infrastructure.publisher import SequenceTracker


class RecordingWebSocket:
    """Fake WebSocket returning queued messages and recording sends."""

    def __init__(self, messages: list[str] | None = None) -> None:
        """Initialize the instance."""
        self._messages = messages or []
        self.sent: list[str] = []

    async def receive_text(self) -> str:
        """Return next queued message or raise disconnect."""
        if not self._messages:
            raise WebSocketDisconnect()
        return self._messages.pop(0)

    async def send_text(self, data: str) -> None:
        """Record sent message."""
        self.sent.append(data)


class StubConnectionManager:
    """Connection manager stub with a live tracker."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.active_connections: list[Any] = [object()]
        self._tracker = SequenceTracker()

    @property
    def tracker(self) -> SequenceTracker:
        """Provide sequence tracker for provenance stamping."""
        return self._tracker


def _delegate_principal() -> AuthPrincipal:
    """Build an AI_DELEGATE principal carrying ``delegate_public_id``."""
    return AuthPrincipal(
        username="delegate-x",
        role=UserRole.AI_DELEGATE,
        user_public_id="user-1",
        operator_public_ids=["op-1"],
        delegate_public_id="del-1",
    )


def _ping_payload() -> str:
    """Build a valid client ping frame."""
    return json.dumps(
        {
            "type": "ping",
            "session_id": "",
            "sequence_id": 0,
            "public_id": "test",
            "timestamp": "2026-07-03T00:00:00Z",
        }
    )


@pytest.mark.asyncio
async def test_dispatch_stops_when_the_principal_is_gone() -> None:
    """Verify the loop ends fail-closed once the registry no longer holds a principal.

    Given: A connection whose auth-manager entry has been removed, with a ping
        frame still queued,
    When: dispatch_messages receives that frame,
    Then: The frame is NOT dispatched and the loop ends.

    The dispatcher deliberately takes no principal argument, so a resolution of
    None is the only signal that authority is gone. Continuing would mean
    serving the message under whatever authority the previous message carried,
    which is the exact defect the per-message resolution exists to prevent.
    """
    websocket = RecordingWebSocket(messages=[_ping_payload()])
    manager = StubConnectionManager()
    ws_auth_manager = MagicMock(
        on_client_ping=AsyncMock(),
        get_authenticated_user=MagicMock(return_value=None),
    )
    await dispatch_messages(
        cast(WebSocket, websocket),
        cast(WebSocketConnectionManager, manager),
        cast(WebSocketAuthManager, ws_auth_manager),
        cast(WsTokenService, MagicMock()),
    )
    assert websocket.sent == []
    ws_auth_manager.on_client_ping.assert_not_awaited()


@pytest.mark.asyncio
async def test_ping_wrapper_answers_pong_then_bumps_liveness() -> None:
    """Verify pong is sent before the liveness hook runs.

    Given: A delegate principal and a recording auth manager,
    When: _handle_ping_with_liveness runs,
    Then: The pong frame is already sent when on_client_ping is awaited.
    """
    websocket = RecordingWebSocket()
    manager = StubConnectionManager()
    user = _delegate_principal()
    order: list[str] = []

    async def record_ping(principal: AuthPrincipal) -> None:
        order.append(f"bump:{len(websocket.sent)}")
        assert principal is user

    ws_auth_manager = MagicMock(on_client_ping=AsyncMock(side_effect=record_ping))
    await _handle_ping_with_liveness(
        cast(WebSocket, websocket),
        cast(WebSocketConnectionManager, manager),
        user,
        cast(WebSocketAuthManager, ws_auth_manager),
    )
    assert len(websocket.sent) == 1
    assert json.loads(websocket.sent[0])["type"] == "pong"
    assert order == ["bump:1"]


@pytest.mark.asyncio
async def test_dispatch_ping_routes_through_liveness_hook() -> None:
    """Verify a ping dispatched through the message loop bumps liveness.

    Given: A queued ping frame and a recording auth manager,
    When: dispatch_messages processes the frame,
    Then: A pong is sent and on_client_ping receives the principal.
    """
    websocket = RecordingWebSocket(messages=[_ping_payload()])
    manager = StubConnectionManager()
    user = _delegate_principal()
    ws_auth_manager = MagicMock(
        on_client_ping=AsyncMock(),
        get_authenticated_user=MagicMock(return_value=user),
    )
    await dispatch_messages(
        cast(WebSocket, websocket),
        cast(WebSocketConnectionManager, manager),
        cast(WebSocketAuthManager, ws_auth_manager),
        cast(WsTokenService, MagicMock()),
    )
    assert len(websocket.sent) == 1
    assert json.loads(websocket.sent[0])["type"] == "pong"
    ws_auth_manager.on_client_ping.assert_awaited_once_with(user)
