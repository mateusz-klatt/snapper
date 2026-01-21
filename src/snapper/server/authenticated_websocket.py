"""Authenticated WebSocket router for real-time data streaming.

This module provides the WebSocket endpoint that requires authentication
before allowing subscription to real-time market data topics.

Authentication Flow:
    1. Client connects to ``/snapper/api/ws``
    2. Server validates origin header against allowed origins
    3. Client sends auth message with WebSocket token
    4. Server validates token and extracts user profile
    5. Server sends auth_complete with allowed topics for user's role
    6. Client can subscribe to topics based on permissions

Topics are role-based:
    - VIEWER: market data topics only
    - OPERATOR: market data + signals
    - ADMIN: all topics including system events

Example:
    Client-side WebSocket connection::

        const ws = new WebSocket('wss://host/snapper/api/ws');
        ws.onopen = () => {
            ws.send(JSON.stringify({
                type: 'auth',
                token: wsToken
            }));
        };
"""

from fastapi import APIRouter
from fastapi import WebSocket
from fastapi import WebSocketDisconnect
from loguru import logger

from snapper.api.auth.services.ws_token_service import get_ws_token_service
from snapper.auth.websocket_auth import get_ws_auth_manager
from snapper.config.settings import get_settings
from snapper.interface.websocket.bridge import ZmqWebSocketBridgeService
from snapper.interface.websocket.connection_manager import WebSocketConnectionManager
from snapper.interface.websocket.dispatcher import dispatch_messages
from snapper.interface.websocket.dispatcher import send_auth_complete
from snapper.interface.websocket.handlers.auth import authenticate_websocket
from snapper.interface.websocket.helpers import build_allowed_origins
from snapper.interface.websocket.helpers import get_allowed_topics_for_role
from snapper.interface.websocket.helpers import has_trading_permission
from snapper.interface.websocket.helpers import validate_origin

__all__ = [
    "create_authenticated_websocket_router",
    "get_allowed_topics_for_role",
    "has_trading_permission",
]


def create_authenticated_websocket_router(manager: WebSocketConnectionManager) -> APIRouter:
    """Create WebSocket router with authentication.

    Args:
        manager: WebSocket connection manager for handling connections
            and topic subscriptions.

    Returns:
        APIRouter with ``/ws`` WebSocket endpoint that requires
        token-based authentication before allowing subscriptions.
    """
    router = APIRouter()
    ws_auth_manager = get_ws_auth_manager()
    ws_token_service = get_ws_token_service()
    settings = get_settings()
    static_allowed_origins = build_allowed_origins(settings, settings.server_port)

    @router.websocket("/ws")
    async def authenticated_websocket_endpoint(websocket: WebSocket) -> None:
        await websocket.accept()
        allowed_origins = static_allowed_origins.copy()
        try:
            app_settings = websocket.app.state.settings
            allowed_origins = build_allowed_origins(app_settings, settings.server_port)
        except AttributeError:
            pass
        if not await validate_origin(websocket, allowed_origins):
            return
        zmq_bridge = manager.zmq_bridge
        if zmq_bridge is None:
            zmq_bridge = ZmqWebSocketBridgeService(manager)
            manager.attach_bridge(zmq_bridge)
        authenticated = False
        user = None
        try:
            auth_result = await authenticate_websocket(websocket, ws_auth_manager, ws_token_service)
            if not auth_result.success or auth_result.user is None:
                return
            user = auth_result.user
            authenticated = True
            await manager.connect(websocket, accept=False)
            if auth_result.ws_payload is not None:
                await send_auth_complete(
                    websocket, manager, user, auth_result.ws_payload, ws_auth_manager
                )
            await dispatch_messages(websocket, manager, user, ws_auth_manager, ws_token_service)
        except WebSocketDisconnect:
            logger.info(
                f"WebSocket disconnected for user {user.username if user else 'unauthenticated'}"
            )
        except Exception as exc:
            logger.exception("WebSocket error: {}", exc)
        finally:
            if authenticated:
                await manager.disconnect(websocket)
            ws_auth_manager.disconnect(websocket)

    return router
