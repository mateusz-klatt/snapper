"""Authenticated WebSocket router for real-time data streaming.

This module provides the WebSocket endpoint that requires authentication
before allowing subscription to real-time market data topics.
Authentication Flow
    1. Client connects to ``/api/ws``
    2. Server validates origin header against allowed origins
    3. Client sends auth message with WebSocket token
    4. Server validates token and extracts user profile
    5. Server sends auth_complete with allowed topics for user's role
    6. Client can subscribe to topics based on permissions
Topics are permission-category based
    VIEWER receives read-only categories such as market data, trade
    events, strategy status, system status, backtests, and
    notifications.
    OPERATOR adds trade commands, signals, strategy control, process
    administration, and AI-review topics.
    AI_DELEGATE receives the narrowed automation categories granted by
    its permissions, including market data, trade commands, trade
    events, signal streams, strategy status, system status, backtests,
    and AI-review topics.
    ADMIN receives every category, including admin topics.

Example:
    Client-side WebSocket connection
        const ws = new WebSocket('wss://host/api/ws')
        ws.onopen = () => {
            ws.send(JSON.stringify({
                type: 'auth'
                token: wsToken.
"""

from fastapi import APIRouter
from fastapi import WebSocket
from fastapi import WebSocketDisconnect
from loguru import logger

from snapper.api.auth.services.ws_token_service import WsTokenService
from snapper.api.auth.services.ws_token_service import get_ws_token_service
from snapper.auth.websocket_auth import WebSocketAuthManager
from snapper.auth.websocket_auth import get_ws_auth_manager
from snapper.config.settings import get_settings
from snapper.data.repository import Repository
from snapper.data.repository import get_repository
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


def _resolve_allowed_origins(
    websocket: WebSocket, static_origins: set[str], server_port: int
) -> set[str]:
    """Resolve allowed origins using app state settings if available.

    Args:
        websocket: WebSocket connection instance.
        static_origins: Default allowed origins from bootstrap settings.
        server_port: Server port number.

    Returns:
        Set of allowed origin strings.
    """
    try:
        app_settings = websocket.app.state.settings
        return build_allowed_origins(app_settings, server_port)
    except AttributeError:
        return static_origins.copy()


def _ensure_zmq_bridge(manager: WebSocketConnectionManager) -> None:
    """Ensure the ZMQ bridge is attached to the connection manager.

    Args:
        manager: WebSocket connection manager.
    """
    if manager.zmq_bridge is None:
        bridge = ZmqWebSocketBridgeService(manager)
        manager.attach_bridge(bridge)


async def _authenticate_and_dispatch(
    websocket: WebSocket,
    manager: WebSocketConnectionManager,
    ws_auth_manager: WebSocketAuthManager,
    ws_token_service: WsTokenService,
    repository: Repository,
    state: list[bool],
    db_url: str | None = None,
) -> None:
    """Authenticate, connect, and run the WebSocket dispatch loop.

    Sets state[0] to True once the websocket is connected to the manager
    so the caller knows cleanup is needed even if an exception occurs later.

    Args:
        websocket: Accepted WebSocket connection.
        manager: WebSocket connection manager.
        ws_auth_manager: WebSocket authentication manager.
        ws_token_service: WebSocket token service.
        repository: Active :class:`Repository` threaded through to the
            DB-backed verify path.
        state: Single-element list; set to [True] once connected.
        db_url: Optional database URL for control recording.
    """
    auth_result = await authenticate_websocket(
        websocket, ws_auth_manager, ws_token_service, manager.tracker, repository
    )
    if not auth_result.success or auth_result.user is None:
        return
    user = auth_result.user
    state[0] = True
    await manager.connect(websocket, accept=False)
    if auth_result.ws_payload is not None:
        await send_auth_complete(
            websocket,
            manager,
            user,
            auth_result.ws_payload,
            ws_auth_manager,
            db_url,
        )
    await dispatch_messages(websocket, manager, user, ws_auth_manager, ws_token_service, db_url)


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
        allowed_origins = _resolve_allowed_origins(
            websocket, static_allowed_origins, settings.server_port
        )
        if not await validate_origin(websocket, allowed_origins, manager.tracker):
            return
        _ensure_zmq_bridge(manager)
        authenticated: list[bool] = [False]
        repository = get_repository(settings.db_url)
        try:
            await _authenticate_and_dispatch(
                websocket,
                manager,
                ws_auth_manager,
                ws_token_service,
                repository,
                authenticated,
                db_url=settings.db_url,
            )
        except WebSocketDisconnect:
            logger.info("WebSocket disconnected for user unauthenticated")
        except Exception as exc:
            logger.exception("WebSocket error: {}", exc)
        finally:
            if authenticated[0]:
                await manager.disconnect(websocket)
            ws_auth_manager.disconnect(websocket)

    return router
