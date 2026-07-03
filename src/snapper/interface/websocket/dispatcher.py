"""WebSocket message dispatcher.

This module handles incoming WebSocket messages, routing them to
appropriate handlers based on message type.  After handling each
message, a control record is written to the ``control`` table for
audit purposes (auth, subscribe, error events).  Ping/pong messages
are recorded as telemetry rows, gated by the
``telemetry_recording_enabled`` bootstrap setting.
"""

import json
from collections.abc import Awaitable
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from typing import Annotated
from typing import Any
from uuid import uuid7

from fastapi import WebSocket
from fastapi import WebSocketDisconnect
from loguru import logger
from pydantic import TypeAdapter
from pydantic import ValidationError

from snapper.api.auth.schemas.ws_token import WsTokenPayload
from snapper.api.auth.services.ws_token_service import WsTokenService
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.websocket_auth import WebSocketAuthManager
from snapper.config.settings import get_settings
from snapper.core.redact import redact
from snapper.data.models import Control
from snapper.data.models import Telemetry
from snapper.data.repository import get_repository
from snapper.interface.websocket.connection_manager import WebSocketConnectionManager
from snapper.interface.websocket.gap_detection import WsClientGapDetector
from snapper.interface.websocket.handlers.auth import handle_reauth
from snapper.interface.websocket.handlers.ping import handle_ping
from snapper.interface.websocket.handlers.subscribe import handle_get_subscriptions
from snapper.interface.websocket.handlers.subscribe import handle_subscribe
from snapper.interface.websocket.handlers.subscribe import handle_unsubscribe
from snapper.interface.websocket.helpers import get_allowed_topics_for_role
from snapper.interface.websocket.models import SERVER_CONTROL_SEQ
from snapper.interface.websocket.schemas import WSAuthCompleteResponse
from snapper.interface.websocket.schemas import WSAuthOkResponse
from snapper.interface.websocket.schemas import WSErrorResponse
from snapper.interface.websocket.schemas import WSGetSubscriptionsRequest
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
    | WSReauthRequest,
    "type",
]
_client_message_adapter: TypeAdapter[WSClientMessage] = TypeAdapter(WSClientMessage)
__all__ = [
    "send_auth_complete",
    "dispatch_messages",
    "_record_ws_control",
    "_record_ws_telemetry",
    "_extract_client_provenance",
]


def _extract_client_provenance(raw_payload: str | None) -> tuple[str | None, str | None]:
    """Extract client_session_id and client_public_id from a raw JSON payload.

    Parses the payload once and looks for ``session_id`` and ``public_id``
    fields that the client may have stamped for causation linkage.

    Args:
        raw_payload: Raw JSON string from the client, or None.

    Returns:
        Tuple of (client_session_id, client_public_id). Both may be None
        when the payload is missing, unparseable, or lacks provenance fields.
    """
    if raw_payload is None:
        return None, None
    try:
        parsed: Any = json.loads(raw_payload)
    except (json.JSONDecodeError, TypeError):
        return None, None
    if not isinstance(parsed, dict):
        return None, None
    client_sid: str | None = parsed.get("session_id") or None
    client_pid: str | None = parsed.get("public_id") or None
    return client_sid, client_pid


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
    is never disrupted.  Client provenance fields (``session_id`` and
    ``public_id``) are extracted from the raw payload for causation linkage.

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
        client_sid, client_pid = _extract_client_provenance(raw_payload)
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
            client_session_id=client_sid,
            client_public_id=client_pid,
            session_id=tracker.session_id,
            sequence_id=tracker.next_sequence(SERVER_CONTROL_SEQ),
            timestamp=now,
        )
        async with repo.session() as session:
            session.add(row)
            await session.commit()
    except Exception as exc:
        logger.warning("WS control record write failed (non-blocking): {}", exc)


async def _record_ws_telemetry(
    db_url: str | None,
    tracker: SequenceTracker,
    message_type: str,
    raw_payload: str | None = None,
) -> None:
    """Write a telemetry row for a data-plane WebSocket event (non-blocking).

    The sequence counter always increments (via the tracker) regardless
    of whether the row is actually persisted. Persistence is gated by the
    ``telemetry_recording_enabled`` bootstrap setting.

    Args:
        db_url: Database URL; skip recording when None.
        tracker: Sequence tracker for provenance fields.
        message_type: Discriminator (e.g. ``ping``, ``pong``).
        raw_payload: Optional raw message payload.
    """
    seq = tracker.next_sequence("server.telemetry")
    settings = get_settings()
    if db_url is None or not settings.telemetry_recording_enabled:
        return
    try:
        repo = get_repository(db_url)
        now = datetime.now(UTC)
        row = Telemetry(
            transport="ws",
            direction="inbound",
            message_type=message_type,
            payload=raw_payload,
            session_id=tracker.session_id,
            sequence_id=seq,
            timestamp=now,
        )
        async with repo.session() as session:
            session.add(row)
            await session.commit()
    except Exception as exc:
        logger.warning("WS telemetry record write failed (non-blocking): {}", exc)


async def send_auth_complete(
    websocket: WebSocket,
    manager: WebSocketConnectionManager,
    user: AuthPrincipal,
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
        sequence_id=manager.tracker.next_sequence(SERVER_CONTROL_SEQ),
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
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
        sequence_id=manager.tracker.next_sequence(SERVER_CONTROL_SEQ),
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
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
            sequence_id=manager.tracker.next_sequence(SERVER_CONTROL_SEQ),
            public_id=str(uuid7()),
            timestamp=datetime.now(UTC),
        )


async def _handle_one_message(
    websocket: WebSocket,
    manager: WebSocketConnectionManager,
    user: AuthPrincipal,
    ws_auth_manager: WebSocketAuthManager,
    ws_token_service: WsTokenService,
    raw_message: str,
    client_gap_detector: WsClientGapDetector,
    dispatch_table: dict[type, Callable[[Any], Awaitable[None]]],
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
        dispatch_table: Pre-built handler dispatch table (one per connection).
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
    await _dispatch_single_message(parsed, dispatch_table)
    if isinstance(parsed, WSPingRequest):
        await _record_ws_telemetry(
            db_url,
            manager.tracker,
            "ping",
            raw_payload=raw_message,
        )
    else:
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
    user: AuthPrincipal,
    ws_auth_manager: WebSocketAuthManager,
    ws_token_service: WsTokenService,
    db_url: str | None = None,
) -> None:
    """Main message dispatch loop for WebSocket connection.

    Receives messages, validates them, and routes to appropriate handlers.
    Handles re-authentication, subscriptions, and pings.

    The dispatch table is built once per connection (not per message) to
    avoid repeated ``get_settings()``/``get_repository()`` calls and
    closure allocations in the hot path.

    Args:
        websocket: The authenticated WebSocket connection.
        manager: WebSocket connection manager.
        user: Authenticated user profile.
        ws_auth_manager: Manager for WebSocket authentication state.
        ws_token_service: Service for verifying ws_tokens.
        db_url: Optional database URL for control recording.
    """
    client_gap_detector = WsClientGapDetector()
    dispatch_table = _build_dispatch_table(websocket, manager, user, ws_auth_manager)
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
                dispatch_table,
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
            sequence_id=manager.tracker.next_sequence(SERVER_CONTROL_SEQ),
            public_id=str(uuid7()),
            timestamp=datetime.now(UTC),
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
    user: AuthPrincipal,
    ws_auth_manager: WebSocketAuthManager,
) -> dict[type, Callable[[Any], Awaitable[None]]]:
    """Build a message-type-to-handler dispatch table.

    Args:
        websocket: The authenticated WebSocket connection.
        manager: WebSocket connection manager.
        user: Authenticated user profile.
        ws_auth_manager: Auth manager whose ping hook refreshes AI-delegate
            liveness (``ai_delegates.last_seen_at``) so admission control
            keeps a connected delegate inside its heartbeat window.

    Returns:
        Dictionary mapping message types to async handler callables.
    """
    settings = get_settings()
    repository = get_repository(settings.db_url)
    return {
        WSSubscribeRequest: lambda msg: handle_subscribe(websocket, msg, manager, user, repository),
        WSUnsubscribeRequest: lambda msg: handle_unsubscribe(websocket, msg, manager),
        WSGetSubscriptionsRequest: lambda msg: handle_get_subscriptions(
            websocket, manager, user.role
        ),
        WSPingRequest: lambda msg: _handle_ping_with_liveness(
            websocket, manager, user, ws_auth_manager
        ),
    }


async def _handle_ping_with_liveness(
    websocket: WebSocket,
    manager: WebSocketConnectionManager,
    user: AuthPrincipal,
    ws_auth_manager: WebSocketAuthManager,
) -> None:
    """Answer a client ping, then refresh delegate liveness.

    Pong latency stays first-class: the liveness bump runs after the
    pong is sent and is itself throttled + fail-soft inside
    :meth:`WebSocketAuthManager.on_client_ping`, so a slow or failing
    DB write can never delay or break the keep-alive exchange.

    Args:
        websocket: The authenticated WebSocket connection.
        manager: WebSocket connection manager.
        user: Authenticated principal for this connection.
        ws_auth_manager: Auth manager owning the liveness hook.
    """
    await handle_ping(websocket, manager)
    await ws_auth_manager.on_client_ping(user)


async def _dispatch_single_message(
    message: WSClientMessage,
    dispatch_table: dict[type, Callable[[Any], Awaitable[None]]],
) -> None:
    """Route a validated message to its handler.

    Args:
        message: Validated client message.
        dispatch_table: Pre-built handler dispatch table (one per connection).
    """
    handler = dispatch_table.get(type(message))
    assert handler is not None, f"Unhandled message type: {type(message).__name__}"
    await handler(message)
