"""WebSocket message dispatcher.

This module handles incoming WebSocket messages, routing them to
appropriate handlers based on message type.
"""

from collections.abc import Awaitable
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from typing import Annotated
from typing import Any

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
    _manager: WebSocketConnectionManager,
    user: UserProfile,
    ws_payload: "WsTokenPayload",
    ws_auth_manager: WebSocketAuthManager,
) -> None:
    """Send authentication completion messages to client.

    Sends auth_ok followed by auth_complete with available topics
    and session information.

    Args:
        websocket: The authenticated WebSocket connection.
        _manager: WebSocket connection manager (reserved for interface compatibility).
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
        user_role=user.role,
        session_expires_at=session_expires_at_dt,
        ws_token_exp=datetime.fromtimestamp(ws_payload.exp, UTC),
    )
    await websocket.send_text(auth_complete.model_dump_json())


def _try_parse_message(raw_message: str) -> WSClientMessage | WSErrorResponse:
    """Attempt to parse a raw WebSocket message into a typed client message.

    Args:
        raw_message: Raw JSON string from WebSocket.

    Returns:
        Parsed client message on success, or WSErrorResponse on validation failure.
    """
    try:
        return _client_message_adapter.validate_json(raw_message)
    except ValidationError as e:
        return WSErrorResponse(message=f"Invalid message format: {e.error_count()} errors")


async def _handle_one_message(
    websocket: WebSocket,
    manager: WebSocketConnectionManager,
    user: UserProfile,
    ws_auth_manager: WebSocketAuthManager,
    ws_token_service: "WsTokenService",
    raw_message: str,
) -> bool:
    """Process a single incoming WebSocket message.

    Args:
        websocket: The authenticated WebSocket connection.
        manager: WebSocket connection manager.
        user: Authenticated user profile.
        ws_auth_manager: Manager for WebSocket authentication state.
        ws_token_service: Service for verifying ws_tokens.
        raw_message: Raw JSON string received from the client.

    Returns:
        True to continue the dispatch loop, False to break.
    """
    parsed = _try_parse_message(raw_message)
    if isinstance(parsed, WSErrorResponse):
        await websocket.send_text(parsed.model_dump_json())
        return True
    if isinstance(parsed, WSReauthRequest):
        success = await handle_reauth(websocket, parsed, user, ws_auth_manager, ws_token_service)
        return success
    await _dispatch_single_message(websocket, parsed, manager, user)
    return True


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
            should_continue = await _handle_one_message(
                websocket, manager, user, ws_auth_manager, ws_token_service, raw_message
            )
            if not should_continue:
                break
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for user {user.username}")
    except Exception as exc:
        logger.exception("WebSocket error: {}", exc)
        error_msg = WSErrorResponse(message="Internal server error")
        await websocket.send_text(error_msg.model_dump_json())


def _build_dispatch_table(
    websocket: WebSocket,
    manager: WebSocketConnectionManager,
    user: UserProfile,
) -> dict[type, Callable[[Any], Awaitable[None]]]:
    """Build a message-type-to-handler dispatch table.

    Args:
        websocket: The authenticated WebSocket connection.
        manager: WebSocket connection manager.
        user: Authenticated user profile.

    Returns:
        Dictionary mapping message types to async handler callables.
    """
    return {
        WSSubscribeRequest: lambda msg: handle_subscribe(websocket, msg, manager, user.role),
        WSUnsubscribeRequest: lambda msg: handle_unsubscribe(websocket, msg, manager),
        WSGetSubscriptionsRequest: lambda msg: handle_get_subscriptions(
            websocket, manager, user.role
        ),
        WSGetTopicSuggestionsRequest: lambda msg: handle_get_topic_suggestions(
            websocket, manager, msg, user.role
        ),
        WSPingRequest: lambda msg: handle_ping(websocket, manager),
    }


async def _dispatch_single_message(
    websocket: WebSocket,
    message: WSClientMessage,
    manager: WebSocketConnectionManager,
    user: UserProfile,
) -> None:
    """Route a validated message to its handler.

    Args:
        websocket: The authenticated WebSocket connection.
        message: Validated client message.
        manager: WebSocket connection manager.
        user: Authenticated user profile.
    """
    dispatch_table = _build_dispatch_table(websocket, manager, user)
    handler = dispatch_table.get(type(message))
    assert handler is not None, f"Unhandled message type: {type(message).__name__}"
    await handler(message)
