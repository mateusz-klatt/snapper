"""WebSocket message dispatcher.

This module handles incoming WebSocket messages, routing them to
appropriate handlers based on message type.
"""

from datetime import UTC
from datetime import datetime
from typing import Annotated

from fastapi import WebSocket
from fastapi import WebSocketDisconnect
from loguru import logger
from pydantic import TypeAdapter
from pydantic import ValidationError

from snapper.api.auth.schemas.ws_token import WsTokenPayload
from snapper.api.auth.services.ws_token_service import WsTokenService
from snapper.auth.schemas.user import UserProfile
from snapper.auth.websocket_auth import WebSocketAuthManager
from snapper.interface.websocket.connection_manager import WebSocketConnectionManager
from snapper.interface.websocket.handlers.auth import handle_reauth
from snapper.interface.websocket.handlers.ping import handle_ping
from snapper.interface.websocket.handlers.subscribe import handle_get_subscriptions
from snapper.interface.websocket.handlers.subscribe import handle_subscribe
from snapper.interface.websocket.handlers.subscribe import handle_unsubscribe
from snapper.interface.websocket.handlers.topics import handle_get_topic_suggestions
from snapper.interface.websocket.helpers import get_allowed_topics_for_role
from snapper.interface.websocket.schemas import WSAuthCompleteResponse
from snapper.interface.websocket.schemas import WSAuthOkResponse
from snapper.interface.websocket.schemas import WSErrorResponse
from snapper.interface.websocket.schemas import WSGetSubscriptionsRequest
from snapper.interface.websocket.schemas import WSGetTopicSuggestionsRequest
from snapper.interface.websocket.schemas import WSPingRequest
from snapper.interface.websocket.schemas import WSReauthRequest
from snapper.interface.websocket.schemas import WSSubscribeRequest
from snapper.interface.websocket.schemas import WSUnsubscribeRequest

WSClientMessage = Annotated[
    WSSubscribeRequest
    | WSUnsubscribeRequest
    | WSPingRequest
    | WSGetSubscriptionsRequest
    | WSGetTopicSuggestionsRequest
    | WSReauthRequest,
    "type",
]
_client_message_adapter: TypeAdapter[WSClientMessage] = TypeAdapter(WSClientMessage)
__all__ = [
    "send_auth_complete",
    "dispatch_messages",
]


async def send_auth_complete(
    websocket: WebSocket,
    manager: WebSocketConnectionManager,
    user: UserProfile,
    ws_payload: "WsTokenPayload",
    ws_auth_manager: WebSocketAuthManager,
) -> None:
    """Send authentication completion messages to client.

    Sends auth_ok followed by auth_complete with available topics
    and session information.

    Args:
        websocket: The authenticated WebSocket connection.
        manager: WebSocket connection manager.
        user: Authenticated user profile.
        ws_payload: WebSocket token payload with expiration.
        ws_auth_manager: Manager for WebSocket authentication state.
    """
    auth_ok = WSAuthOkResponse(exp=datetime.fromtimestamp(ws_payload.exp, UTC))
    await websocket.send_text(auth_ok.model_dump_json())
    allowed_topics = get_allowed_topics_for_role(user.role)
    session_expires_at_dt = ws_auth_manager.get_connection_expiration(websocket)
    auth_complete = WSAuthCompleteResponse(
        available_topics=allowed_topics,
        user_role=user.role.value,
        session_expires_at=session_expires_at_dt,
        ws_token_exp=datetime.fromtimestamp(ws_payload.exp, UTC),
    )
    await websocket.send_text(auth_complete.model_dump_json())


async def dispatch_messages(
    websocket: WebSocket,
    manager: WebSocketConnectionManager,
    user: UserProfile,
    ws_auth_manager: WebSocketAuthManager,
    ws_token_service: "WsTokenService",
) -> None:
    """Main message dispatch loop for WebSocket connection.

    Receives messages, validates them, and routes to appropriate handlers.
    Handles re-authentication, subscriptions, pings, and topic suggestions.

    Args:
        websocket: The authenticated WebSocket connection.
        manager: WebSocket connection manager.
        user: Authenticated user profile.
        ws_auth_manager: Manager for WebSocket authentication state.
        ws_token_service: Service for verifying ws_tokens.
    """
    try:
        while True:
            raw_message = await websocket.receive_text()
            try:
                message = _client_message_adapter.validate_json(raw_message)
            except ValidationError as e:
                error_msg = WSErrorResponse(
                    message=f"Invalid message format: {e.error_count()} errors"
                )
                await websocket.send_text(error_msg.model_dump_json())
                continue
            if isinstance(message, WSReauthRequest):
                success = await handle_reauth(
                    websocket, message, user, ws_auth_manager, ws_token_service
                )
                if not success:
                    break
                continue
            if isinstance(message, WSSubscribeRequest):
                await handle_subscribe(websocket, message, manager, user.role)
            elif isinstance(message, WSUnsubscribeRequest):
                await handle_unsubscribe(websocket, message, manager)
            elif isinstance(message, WSGetSubscriptionsRequest):
                await handle_get_subscriptions(websocket, manager, user.role)
            elif isinstance(message, WSGetTopicSuggestionsRequest):
                await handle_get_topic_suggestions(websocket, manager, message, user.role)
            else:
                assert isinstance(message, WSPingRequest)
                await handle_ping(websocket, manager)
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for user {user.username}")
    except Exception as exc:
        logger.exception("WebSocket error: {}", exc)
        error_msg = WSErrorResponse(message="Internal server error")
        await websocket.send_text(error_msg.model_dump_json())
