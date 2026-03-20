"""WebSocket subscription handlers.

This module handles topic subscription and unsubscription requests
with role-based access control and ZMQ bridge integration.
"""

from fastapi import WebSocket

from snapper.auth.domain.roles import UserRole
from snapper.interface.websocket.connection_manager import WebSocketConnectionManager
from snapper.interface.websocket.helpers import filter_topics
from snapper.interface.websocket.helpers import get_allowed_topics_for_role
from snapper.interface.websocket.helpers import role_allowed_categories
from snapper.interface.websocket.schemas import WSErrorResponse
from snapper.interface.websocket.schemas import WSSubscribeRequest
from snapper.interface.websocket.schemas import WSSubscriptionsListResponse
from snapper.interface.websocket.schemas import WSSubscriptionSuccessResponse
from snapper.interface.websocket.schemas import WSUnsubscribeRequest
from snapper.messaging.topics.validation import validate_subscription_pattern

__all__ = [
    "handle_subscribe",
    "handle_unsubscribe",
    "handle_get_subscriptions",
]


async def handle_subscribe(
    websocket: WebSocket,
    message: WSSubscribeRequest,
    manager: WebSocketConnectionManager,
    role: UserRole,
) -> None:
    """Handle topic subscription request.

    Validates topic patterns, checks permissions, and registers
    subscriptions with both the connection manager and ZMQ bridge.

    Args:
        websocket: The WebSocket connection.
        message: Subscription request with topic list.
        manager: WebSocket connection manager.
        role: User's role for permission checking.
    """
    topics: list[str] = []
    invalid_topics: list[tuple[str, str]] = []
    for raw_topic in message.topics:
        cleaned = raw_topic.strip()
        if cleaned:
            is_valid, error_msg_str = validate_subscription_pattern(cleaned)
            if is_valid:
                topics.append(cleaned)
            else:
                invalid_topics.append((cleaned, error_msg_str))
    if invalid_topics:
        error_details = [f"{topic}: {error}" for topic, error in invalid_topics]
        error_msg = WSErrorResponse(
            message=f"Invalid topic format: {', '.join(error_details)}",
            session_id=manager.tracker.session_id,
            sequence_id=manager.tracker.next_sequence("server.control"),
        )
        await websocket.send_text(error_msg.model_dump_json())
        return
    allowed_topics = get_allowed_topics_for_role(role)
    allowed_set = set(allowed_topics)
    allowed_categories = role_allowed_categories(role)
    allowed, denied = filter_topics(topics, allowed_set, allowed_categories)
    if not allowed and denied:
        response = WSSubscriptionSuccessResponse(
            action="subscribe",
            status="denied",
            topics=[],
            denied_topics=denied,
            active_subscriptions=list(manager.get_client_subscriptions(websocket)),
            session_id=manager.tracker.session_id,
            sequence_id=manager.tracker.next_sequence("server.control"),
        )
        await websocket.send_text(response.model_dump_json())
        return
    if not allowed:
        response = WSSubscriptionSuccessResponse(
            action="subscribe",
            status="no_topics",
            topics=[],
            denied_topics=denied,
            active_subscriptions=list(manager.get_client_subscriptions(websocket)),
            session_id=manager.tracker.session_id,
            sequence_id=manager.tracker.next_sequence("server.control"),
        )
        await websocket.send_text(response.model_dump_json())
        return
    zmq_topics = [str(topic).strip() for topic in allowed if topic]
    for ui_topic in allowed:
        manager.subscribe_client(websocket, ui_topic)
    bridge = manager.zmq_bridge
    if bridge is None:
        error_msg = WSErrorResponse(
            message="ZMQ bridge is not available",
            session_id=manager.tracker.session_id,
            sequence_id=manager.tracker.next_sequence("server.control"),
        )
        await websocket.send_text(error_msg.model_dump_json())
        return
    await bridge.add_subscription(websocket, zmq_topics)
    response = WSSubscriptionSuccessResponse(
        action="subscribe",
        status="partial" if denied else "subscribed",
        topics=allowed,
        denied_topics=denied,
        active_subscriptions=list(manager.get_client_subscriptions(websocket)),
        zmq_topics=zmq_topics,
        message=f"Access denied to topics: {denied}" if denied else None,
        session_id=manager.tracker.session_id,
        sequence_id=manager.tracker.next_sequence("server.control"),
    )
    await websocket.send_text(response.model_dump_json())


async def handle_unsubscribe(
    websocket: WebSocket,
    message: WSUnsubscribeRequest,
    manager: WebSocketConnectionManager,
) -> None:
    """Handle topic unsubscription request.

    Removes subscriptions from both the connection manager and ZMQ bridge.

    Args:
        websocket: The WebSocket connection.
        message: Unsubscription request with topic list.
        manager: WebSocket connection manager.
    """
    topics: list[str] = []
    for raw_topic in message.topics:
        cleaned = raw_topic.strip()
        if cleaned:
            topics.append(cleaned)
    current_subscriptions = manager.get_client_subscriptions(websocket)
    allowed = [topic for topic in topics if topic in current_subscriptions]
    denied = [topic for topic in topics if topic not in current_subscriptions]
    if not allowed:
        response = WSSubscriptionSuccessResponse(
            action="unsubscribe",
            status="no_topics",
            topics=[],
            denied_topics=denied,
            active_subscriptions=list(current_subscriptions),
            session_id=manager.tracker.session_id,
            sequence_id=manager.tracker.next_sequence("server.control"),
        )
        await websocket.send_text(response.model_dump_json())
        return
    zmq_topics = [str(topic).strip() for topic in allowed if topic]
    bridge = manager.zmq_bridge
    if bridge is None:
        error_msg = WSErrorResponse(
            message="ZMQ bridge is not available",
            session_id=manager.tracker.session_id,
            sequence_id=manager.tracker.next_sequence("server.control"),
        )
        await websocket.send_text(error_msg.model_dump_json())
        return
    for topic in allowed:
        manager.unsubscribe_client(websocket, topic)
    await bridge.remove_subscription(websocket, zmq_topics)
    response = WSSubscriptionSuccessResponse(
        action="unsubscribe",
        status="partial" if denied else "unsubscribed",
        topics=allowed,
        denied_topics=denied,
        active_subscriptions=list(manager.get_client_subscriptions(websocket)),
        zmq_topics=zmq_topics,
        message=f"Not subscribed to topics: {denied}" if denied else None,
        session_id=manager.tracker.session_id,
        sequence_id=manager.tracker.next_sequence("server.control"),
    )
    await websocket.send_text(response.model_dump_json())


async def handle_get_subscriptions(
    websocket: WebSocket, manager: WebSocketConnectionManager, role: UserRole
) -> None:
    """Handle get subscriptions request.

    Returns list of active subscriptions and available topics for the user.

    Args:
        websocket: The WebSocket connection.
        manager: WebSocket connection manager.
        role: User's role for available topics.
    """
    allowed_topics = get_allowed_topics_for_role(role)
    response = WSSubscriptionsListResponse(
        subscriptions=list(manager.get_client_subscriptions(websocket)),
        available_topics=allowed_topics,
        total_available=len(allowed_topics),
        session_id=manager.tracker.session_id,
        sequence_id=manager.tracker.next_sequence("server.control"),
    )
    await websocket.send_text(response.model_dump_json())
