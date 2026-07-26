"""WebSocket subscription handlers.

This module handles topic subscription and unsubscription requests
with permission-based access control and ZMQ bridge integration.
"""

from datetime import UTC
from datetime import datetime
from uuid import uuid7

from fastapi import WebSocket

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.core.types import SubscriptionActionEnum
from snapper.core.types import SubscriptionStatusEnum
from snapper.data.repository import Repository
from snapper.interface.websocket.connection_manager import WebSocketConnectionManager
from snapper.interface.websocket.helpers import get_allowed_topics_for_role
from snapper.interface.websocket.models import SERVER_CONTROL_SEQ
from snapper.interface.websocket.schemas import WSErrorResponse
from snapper.interface.websocket.schemas import WSSubscribeRequest
from snapper.interface.websocket.schemas import WSSubscriptionsListResponse
from snapper.interface.websocket.schemas import WSSubscriptionSuccessResponse
from snapper.interface.websocket.schemas import WSUnsubscribeRequest
from snapper.interface.websocket.topic_authorization import BACKTEST_PREFIX
from snapper.interface.websocket.topic_authorization import partition_authorized_topics
from snapper.messaging.topics.schemas import REGISTRY_ROOTS
from snapper.messaging.topics.validation import validate_subscription_pattern

__all__ = [
    "handle_subscribe",
    "handle_unsubscribe",
    "handle_get_subscriptions",
]


def _invalid_registry_root_error(topic: str) -> str | None:
    """Return the registry-root validation error for prefix topics."""
    if not topic.endswith("."):
        return None
    if topic in REGISTRY_ROOTS or topic.startswith(BACKTEST_PREFIX):
        return None
    registry_roots = ", ".join(sorted(REGISTRY_ROOTS))
    return (
        f"Prefix '{topic}' is not a registry root. Allowed prefix subscriptions: {registry_roots}"
    )


def _validate_ws_topics(
    raw_topics: list[str],
) -> tuple[list[str], list[tuple[str, str]]]:
    """Validate and classify raw topic strings for WS subscription.

    Applies standard topic validation and additionally rejects prefix
    patterns that are not TOPIC_REGISTRY roots (intermediate prefixes
    like ``market.kraken.`` are not allowed for WS clients).

    Exception: the backtest family (``backtest.{wallet}.``,
    ``backtest.{wallet}.{run}.``) legitimately subscribes to
    wallet-scoped or run-scoped prefixes; those pass
    :func:`_validate_backtest_prefix` instead of the registry-root
    check. Per-prefix wallet-scope RBAC is enforced later in
    :func:`handle_subscribe`.

    Args:
        raw_topics: Raw topic strings from client request.

    Returns:
        Tuple of (valid_topics, invalid_topics) where invalid_topics
        contains (topic, error_message) pairs.
    """
    valid: list[str] = []
    invalid: list[tuple[str, str]] = []
    for raw_topic in raw_topics:
        cleaned = raw_topic.strip()
        if not cleaned:
            continue
        is_valid, error_msg_str = validate_subscription_pattern(cleaned)
        if not is_valid:
            invalid.append((cleaned, error_msg_str))
            continue
        registry_root_error = _invalid_registry_root_error(cleaned)
        if registry_root_error is not None:
            invalid.append((cleaned, registry_root_error))
            continue
        valid.append(cleaned)
    return valid, invalid


async def handle_subscribe(
    websocket: WebSocket,
    message: WSSubscribeRequest,
    manager: WebSocketConnectionManager,
    principal: AuthPrincipal,
    repository: Repository | None = None,
) -> None:
    """Handle topic subscription request.

    Validates topic patterns, checks category-level RBAC, enforces the
    backtest per-subscription wallet-scope rule, runs the AI review-principal
    wallet-scope filter, and registers subscriptions with both the
    connection manager and ZMQ bridge.

    Signature takes the full ``AuthPrincipal`` (not just the role) so
    backtest subscriptions can be constrained to the caller's
    ``active_wallet_public_id``. See :func:`_enforce_backtest_wallet_scope`
    and :func:`_enforce_ai_delegate_wallet_scope` for the authoritative
    scope rules.

    Args:
        websocket: The WebSocket connection.
        message: Subscription request with topic list.
        manager: WebSocket connection manager.
        principal: Authenticated caller — permissions + wallet scope.
        repository: Repository for the AI review-principal wallet-scope
            filter. Required when ``delegate_public_id`` is populated;
            optional otherwise because the filter fast-paths it.
    """
    topics, invalid_topics = _validate_ws_topics(message.topics)
    if invalid_topics:
        error_details = [f"{topic}: {error}" for topic, error in invalid_topics]
        error_msg = WSErrorResponse(
            message=f"Invalid topic format: {', '.join(error_details)}",
            session_id=manager.tracker.session_id,
            sequence_id=manager.tracker.next_sequence(SERVER_CONTROL_SEQ),
            public_id=str(uuid7()),
            timestamp=datetime.now(UTC),
        )
        await websocket.send_text(error_msg.model_dump_json())
        return
    allowed, denied = await partition_authorized_topics(
        topics=topics,
        principal=principal,
        repository=repository,
        as_of=datetime.now(UTC),
    )
    if not allowed and denied:
        response = WSSubscriptionSuccessResponse(
            action=SubscriptionActionEnum.SUBSCRIBE,
            status=SubscriptionStatusEnum.DENIED,
            topics=[],
            denied_topics=denied,
            active_subscriptions=list(manager.get_client_subscriptions(websocket)),
            session_id=manager.tracker.session_id,
            sequence_id=manager.tracker.next_sequence(SERVER_CONTROL_SEQ),
            public_id=str(uuid7()),
            timestamp=datetime.now(UTC),
        )
        await websocket.send_text(response.model_dump_json())
        return
    if not allowed:
        response = WSSubscriptionSuccessResponse(
            action=SubscriptionActionEnum.SUBSCRIBE,
            status=SubscriptionStatusEnum.NO_TOPICS,
            topics=[],
            denied_topics=denied,
            active_subscriptions=list(manager.get_client_subscriptions(websocket)),
            session_id=manager.tracker.session_id,
            sequence_id=manager.tracker.next_sequence(SERVER_CONTROL_SEQ),
            public_id=str(uuid7()),
            timestamp=datetime.now(UTC),
        )
        await websocket.send_text(response.model_dump_json())
        return
    for topic in allowed:
        manager.subscribe_client(websocket, topic)
    bridge = manager.zmq_bridge
    if bridge is None:
        error_msg = WSErrorResponse(
            message="ZMQ bridge is not available",
            session_id=manager.tracker.session_id,
            sequence_id=manager.tracker.next_sequence(SERVER_CONTROL_SEQ),
            public_id=str(uuid7()),
            timestamp=datetime.now(UTC),
        )
        await websocket.send_text(error_msg.model_dump_json())
        return
    await bridge.add_subscription(websocket, allowed)
    response = WSSubscriptionSuccessResponse(
        action=SubscriptionActionEnum.SUBSCRIBE,
        status=SubscriptionStatusEnum.PARTIAL if denied else SubscriptionStatusEnum.SUBSCRIBED,
        topics=allowed,
        denied_topics=denied,
        active_subscriptions=list(manager.get_client_subscriptions(websocket)),
        message=f"Access denied to topics: {denied}" if denied else None,
        session_id=manager.tracker.session_id,
        sequence_id=manager.tracker.next_sequence(SERVER_CONTROL_SEQ),
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
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
            action=SubscriptionActionEnum.UNSUBSCRIBE,
            status=SubscriptionStatusEnum.NO_TOPICS,
            topics=[],
            denied_topics=denied,
            active_subscriptions=list(current_subscriptions),
            session_id=manager.tracker.session_id,
            sequence_id=manager.tracker.next_sequence(SERVER_CONTROL_SEQ),
            public_id=str(uuid7()),
            timestamp=datetime.now(UTC),
        )
        await websocket.send_text(response.model_dump_json())
        return
    bridge = manager.zmq_bridge
    if bridge is None:
        error_msg = WSErrorResponse(
            message="ZMQ bridge is not available",
            session_id=manager.tracker.session_id,
            sequence_id=manager.tracker.next_sequence(SERVER_CONTROL_SEQ),
            public_id=str(uuid7()),
            timestamp=datetime.now(UTC),
        )
        await websocket.send_text(error_msg.model_dump_json())
        return
    for topic in allowed:
        manager.unsubscribe_client(websocket, topic)
    await bridge.remove_subscription(websocket, allowed)
    response = WSSubscriptionSuccessResponse(
        action=SubscriptionActionEnum.UNSUBSCRIBE,
        status=SubscriptionStatusEnum.PARTIAL if denied else SubscriptionStatusEnum.UNSUBSCRIBED,
        topics=allowed,
        denied_topics=denied,
        active_subscriptions=list(manager.get_client_subscriptions(websocket)),
        message=f"Not subscribed to topics: {denied}" if denied else None,
        session_id=manager.tracker.session_id,
        sequence_id=manager.tracker.next_sequence(SERVER_CONTROL_SEQ),
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
    )
    await websocket.send_text(response.model_dump_json())


async def handle_get_subscriptions(
    websocket: WebSocket,
    manager: WebSocketConnectionManager,
    role: UserRole,
    token_permissions: list[str] | None = None,
    permission_scope_version: int | None = None,
) -> None:
    """Handle get subscriptions request.

    Returns list of active subscriptions and available topics for the user.

    Args:
        websocket: The WebSocket connection.
        manager: WebSocket connection manager.
        role: User's role for available topics.
        token_permissions: Permission strings carried by the JWT, or
            ``None`` for the backward-compatible full-role grant.
        permission_scope_version: Version governing scope compatibility.
    """
    allowed_topics = get_allowed_topics_for_role(
        role,
        token_permissions,
        permission_scope_version,
    )
    response = WSSubscriptionsListResponse(
        subscriptions=list(manager.get_client_subscriptions(websocket)),
        available_topics=allowed_topics,
        total_available=len(allowed_topics),
        session_id=manager.tracker.session_id,
        sequence_id=manager.tracker.next_sequence(SERVER_CONTROL_SEQ),
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
    )
    await websocket.send_text(response.model_dump_json())
