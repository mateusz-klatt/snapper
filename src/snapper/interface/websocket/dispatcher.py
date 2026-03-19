"""WebSocket message dispatcher.

This module handles incoming WebSocket messages, routing them to
appropriate handlers based on message type.  After handling each
message, a control record is written to the ``control`` table for
audit purposes (auth, subscribe, error events).
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
from snapper.core.redact import redact
from snapper.data.models import Control
from snapper.data.repository import get_repository
from snapper.interface.websocket.connection_manager import WebSocketConnectionManager
from snapper.interface.websocket.gap_detection import WsClientGapDetector
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
from snapper.messaging.infrastructure.publisher import SequenceTracker

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


async def _record_ws_control(
    db_url: str | None,
    tracker: SequenceTracker,
    message_type: str,
    outcome: str,
    detail: str | None = None,
    raw_payload: str | None = None,
) -> None:
    """Write a control row for a WebSocket event (non-blocking).

    Any DB failure is logged and swallowed so the WebSocket handler
    is never disrupted.

    Args:
        db_url: Database URL; skip recording when None.
        tracker: Sequence tracker for provenance fields.
        message_type: Discriminator (e.g. ``auth``, ``subscribe``, ``error``).
        outcome: ``ok``, ``error``, or ``exception``.
        detail: Optional error detail message.
        raw_payload: Optional raw message payload (will be redacted).
    """
    if db_url is None:
        return
    try:
        redacted = redact(raw_payload)
        repo = get_repository(db_url)
        now = datetime.now(UTC)
        row = Control(
            transport="ws",
            direction="inbound",
            message_type=message_type,
            outcome=outcome,
            detail=detail,
            payload=redacted,
            client_session_id=None,
            client_public_id=None,
            session_id=tracker.session_id,
            sequence_id=tracker.next_sequence("control"),
            timestamp=now,
        )
        async with repo.session() as session:
            session.add(row)
            await session.commit()
    except Exception as exc:
        logger.warning("WS control record write failed (non-blocking): {}", exc)


async def send_auth_complete(
    websocket: WebSocket,
    manager: WebSocketConnectionManager,
    user: UserProfile,
    ws_payload: WsTokenPayload,
    ws_auth_manager: WebSocketAuthManager,
    db_url: str | None = None,
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
        db_url: Optional database URL for control recording.
    """
    auth_ok = WSAuthOkResponse(
        exp=datetime.fromtimestamp(ws_payload.exp, UTC),
        session_id=manager.tracker.session_id,
        sequence_id=manager.tracker.next_sequence("control"),
    )
    await websocket.send_text(auth_ok.model_dump_json())
    allowed_topics = get_allowed_topics_for_role(user.role)
    session_expires_at_dt = ws_auth_manager.get_connection_expiration(websocket)
    auth_complete = WSAuthCompleteResponse(
        available_topics=allowed_topics,
        user_role=user.role,
        session_expires_at=session_expires_at_dt,
        ws_token_exp=datetime.fromtimestamp(ws_payload.exp, UTC),
        session_id=manager.tracker.session_id,
        sequence_id=manager.tracker.next_sequence("control"),
    )
    await websocket.send_text(auth_complete.model_dump_json())
    await _record_ws_control(db_url, manager.tracker, "auth", "ok")


def _try_parse_message(
    raw_message: str, manager: WebSocketConnectionManager
) -> WSClientMessage | WSErrorResponse:
    """Attempt to parse a raw WebSocket message into a typed client message.

    Args:
        raw_message: Raw JSON string from WebSocket.
        manager: WebSocket connection manager for provenance stamping.

    Returns:
        Parsed client message on success, or WSErrorResponse on validation failure.
    """
    try:
        return _client_message_adapter.validate_json(raw_message)
    except ValidationError as e:
        return WSErrorResponse(
            message=f"Invalid message format: {e.error_count()} errors",
            session_id=manager.tracker.session_id,
            sequence_id=manager.tracker.next_sequence("control"),
        )


async def _handle_one_message(
    websocket: WebSocket,
    manager: WebSocketConnectionManager,
    user: UserProfile,
    ws_auth_manager: WebSocketAuthManager,
    ws_token_service: WsTokenService,
    raw_message: str,
    client_gap_detector: WsClientGapDetector,
    db_url: str | None = None,
) -> bool:
    """Process a single incoming WebSocket message.

    Args:
        websocket: The authenticated WebSocket connection.
        manager: WebSocket connection manager.
        user: Authenticated user profile.
        ws_auth_manager: Manager for WebSocket authentication state.
        ws_token_service: Service for verifying ws_tokens.
        raw_message: Raw JSON string received from the client.
        client_gap_detector: Per-connection gap detector for client provenance.
        db_url: Optional database URL for control recording.

    Returns:
        True to continue the dispatch loop, False to break.
    """
    client_gap_detector.inspect(raw_message)
    parsed = _try_parse_message(raw_message, manager)
    if isinstance(parsed, WSErrorResponse):
        await websocket.send_text(parsed.model_dump_json())
        await _record_ws_control(
            db_url,
            manager.tracker,
            "error",
            "error",
            detail=parsed.message,
            raw_payload=raw_message,
        )
        return True
    msg_type = type(parsed).__name__
    if isinstance(parsed, WSReauthRequest):
        success = await handle_reauth(
            websocket, parsed, user, ws_auth_manager, ws_token_service, manager.tracker
        )
        await _record_ws_control(
            db_url,
            manager.tracker,
            "reauth",
            "ok" if success else "error",
            raw_payload=raw_message,
        )
        return success
    await _dispatch_single_message(websocket, parsed, manager, user)
    await _record_ws_control(
        db_url,
        manager.tracker,
        msg_type,
        "ok",
        raw_payload=raw_message,
    )
    return True


async def dispatch_messages(
    websocket: WebSocket,
    manager: WebSocketConnectionManager,
    user: UserProfile,
    ws_auth_manager: WebSocketAuthManager,
    ws_token_service: WsTokenService,
    db_url: str | None = None,
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
        db_url: Optional database URL for control recording.
    """
    client_gap_detector = WsClientGapDetector()
    try:
        while True:
            raw_message = await websocket.receive_text()
            should_continue = await _handle_one_message(
                websocket,
                manager,
                user,
                ws_auth_manager,
                ws_token_service,
                raw_message,
                client_gap_detector,
                db_url,
            )
            if not should_continue:
                break
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for user {user.username}")
    except Exception as exc:
        logger.exception("WebSocket error: {}", exc)
        error_msg = WSErrorResponse(
            message="Internal server error",
            session_id=manager.tracker.session_id,
            sequence_id=manager.tracker.next_sequence("control"),
        )
        await websocket.send_text(error_msg.model_dump_json())
        await _record_ws_control(
            db_url,
            manager.tracker,
            "error",
            "exception",
            detail=f"{type(exc).__name__}: {exc}",
        )


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
