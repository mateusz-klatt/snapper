"""WebSocket ping handler for connection health checks.

This module handles ping/pong messages for WebSocket connection
keep-alive and health monitoring.
"""

from datetime import UTC
from datetime import datetime
from uuid import uuid7

from fastapi import WebSocket

from snapper.interface.websocket.connection_manager import WebSocketConnectionManager
from snapper.interface.websocket.schemas import WSPongResponse

__all__ = [
    "handle_ping",
]


async def handle_ping(websocket: WebSocket, manager: WebSocketConnectionManager) -> None:
    """Handle ping request from client.

    Sends pong response with current server timestamp and connection count.

    Args:
        websocket: The WebSocket connection.
        manager: WebSocket connection manager for stats.
    """
    pong = WSPongResponse(
        timestamp=datetime.now(UTC),
        active_connections=len(manager.active_connections),
        session_id=manager.tracker.session_id,
        sequence_id=manager.tracker.next_sequence("server.telemetry"),
        public_id=str(uuid7()),
    )
    await websocket.send_text(pong.model_dump_json())
