"""WebSocket subscription handlers.

This module handles topic subscription and unsubscription requests
with role-based access control and ZMQ bridge integration.
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
from snapper.interface.websocket.helpers import filter_topics
from snapper.interface.websocket.helpers import get_allowed_topics_for_role
from snapper.interface.websocket.helpers import parse_wallet_scoped_topic
from snapper.interface.websocket.helpers import role_allowed_categories
from snapper.interface.websocket.models import SERVER_CONTROL_SEQ
from snapper.interface.websocket.schemas import WSErrorResponse
from snapper.interface.websocket.schemas import WSSubscribeRequest
from snapper.interface.websocket.schemas import WSSubscriptionsListResponse
from snapper.interface.websocket.schemas import WSSubscriptionSuccessResponse
from snapper.interface.websocket.schemas import WSUnsubscribeRequest
from snapper.messaging.topics.schemas import REGISTRY_ROOTS
from snapper.messaging.topics.validation import validate_subscription_pattern

__all__ = [
    "handle_subscribe",
    "handle_unsubscribe",
    "handle_get_subscriptions",
]


_BACKTEST_PREFIX = "backtest."


def _extract_backtest_wallet_public_id(topic: str) -> str | None:
    """Extract the wallet public id from a backtest topic.

    Returns ``None`` for the bare ``backtest.`` root or malformed
    walletless bodies.
    """
    body = topic[len(_BACKTEST_PREFIX) :].removesuffix(".")
    if not body:
        return None
    wallet_public_id, _, _remainder = body.partition(".")
    return wallet_public_id or None


def _is_backtest_topic_allowed(topic: str, principal: AuthPrincipal) -> bool:
    """Return whether the principal may subscribe to a backtest topic."""
    if principal.role == UserRole.ADMIN:
        return True
    active_wallet = principal.active_wallet_public_id
    if active_wallet is None:
        return False
    return _extract_backtest_wallet_public_id(topic) == active_wallet


def _invalid_registry_root_error(topic: str) -> str | None:
    """Return the registry-root validation error for prefix topics."""
    if not topic.endswith("."):
        return None
    if topic in REGISTRY_ROOTS or topic.startswith(_BACKTEST_PREFIX):
        return None
    registry_roots = ", ".join(sorted(REGISTRY_ROOTS))
    return (
        f"Prefix '{topic}' is not a registry root. "
        f"Allowed prefix subscriptions: {registry_roots}"
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


async def _enforce_ai_delegate_wallet_scope(
    topics: list[str],
    principal: AuthPrincipal,
    repository: Repository | None,
    as_of: datetime,
) -> tuple[list[str], list[str]]:
    """Split wallet-scoped topics into (allowed, denied) for AI_DELEGATE.

    Subscribe-time filter. Fast-paths any non-AI_DELEGATE principal
    (role gate returns the topic set unchanged). For AI_DELEGATE
    the filter:

    1. Computes the delegate's allowed ``(exchange, native_symbol)``
       pairs via ``repository.list_scope_grant_instrument_pairs`` (one
       read per subscribe call — no caching so scope changes
       take effect immediately).
    2. Decomposes every topic with
       :func:`parse_wallet_scoped_topic`.
    3. Passes non-wallet-scoped topics through unchanged (market,
       system, backtest, accruals, admin, and the ``signals.paper.*``
       sandbox per §D2).
    4. Allows wallet-scoped topics whose pair is in the delegate's
       set; denies everything else so they surface as
       ``topic_outside_scope`` in the response envelope.

    Raising on missing ``repository`` is intentional for the
    AI_DELEGATE path: a delegate principal reaching this filter
    without a live repository reference indicates a runtime wiring
    bug, and silently passing topics through would leak wallet scope.

    Args:
        topics: Already shape-validated topic list.
        principal: Authenticated caller; role gates the whole filter.
        repository: Repository for the scope-grant pair projection.
            Ignored for non-AI_DELEGATE roles. Required for
            AI_DELEGATE.
        as_of: Bus time for the temporal scope read.

    Returns:
        Tuple of (allowed, denied) in original input order.
    """
    if principal.role != UserRole.AI_DELEGATE:
        return topics, []
    if repository is None:
        raise RuntimeError(
            "AI_DELEGATE subscribe reached the wallet-scope filter without a "
            "repository reference; dispatch table wiring is broken"
        )
    allowed_pairs = await repository.list_scope_grant_instrument_pairs(
        principal.operator_public_ids, as_of
    )
    allowed: list[str] = []
    denied: list[str] = []
    for topic in topics:
        pair = parse_wallet_scoped_topic(topic)
        if pair is None:
            allowed.append(topic)
            continue
        if pair in allowed_pairs:
            allowed.append(topic)
        else:
            denied.append(topic)
    return allowed, denied


def _enforce_backtest_wallet_scope(
    topics: list[str], principal: AuthPrincipal
) -> tuple[list[str], list[str]]:
    """Split ``backtest.*`` topics into (allowed, denied) by wallet RBAC.

    Authoritative matrix
    ====== ============ ========================= ===============================
    Role ``backtest.`` ``backtest.{own_wallet}.`` ``backtest.{foreign_wallet}.*``
    ====== ============ ========================= ===============================
    VIEWER denied accepted denied
    OPER. denied accepted denied
    ADMIN accepted accepted accepted
    ====== ============ ========================= ===============================
    Wallet segment is extracted from the second dotted segment (the
    topic validator has already proven it is a UUID7). Non-backtest
    topics pass through unchanged on the allowed side.

    Args:
        topics: Already-validated topics (shape-correct but scope
            unchecked).
        principal: Authenticated caller. ``principal.role`` drives
            the matrix; ``principal.active_wallet_public_id`` is the
            only wallet non-admin roles may subscribe to.

    Returns:
        Tuple of (wallet_allowed, wallet_denied) in original order.
    """
    wallet_allowed: list[str] = []
    wallet_denied: list[str] = []
    for topic in topics:
        if not topic.startswith(_BACKTEST_PREFIX) or _is_backtest_topic_allowed(topic, principal):
            wallet_allowed.append(topic)
            continue
        wallet_denied.append(topic)
    return wallet_allowed, wallet_denied


async def handle_subscribe(
    websocket: WebSocket,
    message: WSSubscribeRequest,
    manager: WebSocketConnectionManager,
    principal: AuthPrincipal,
    repository: Repository | None = None,
) -> None:
    """Handle topic subscription request.

    Validates topic patterns, checks category-level RBAC, enforces the
    backtest per-subscription wallet-scope rule, runs the AI_DELEGATE
    wallet-scope filter, and registers subscriptions with both the
    connection manager and ZMQ bridge.

    Signature takes the full ``AuthPrincipal`` (not just the role) so
    backtest subscriptions can be constrained to the caller's
    ``active_wallet_public_id``. See :func:`_enforce_backtest_wallet_scope`
    and :func:`_enforce_ai_delegate_wallet_scope` for the authoritative
    role/prefix matrices.

    Args:
        websocket: The WebSocket connection.
        message: Subscription request with topic list.
        manager: WebSocket connection manager.
        principal: Authenticated caller — role + wallet scope.
        repository: Repository for the AI_DELEGATE wallet-scope
            filter. Required for AI_DELEGATE principals; optional for
            other roles (the filter fast-paths non-AI_DELEGATE calls).
    """
    role = principal.role
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
    topics, wallet_denied = _enforce_backtest_wallet_scope(topics, principal)
    now = datetime.now(UTC)
    topics, ai_delegate_denied = await _enforce_ai_delegate_wallet_scope(
        topics, principal, repository, now
    )
    if ai_delegate_denied:
        wallet_denied = [*wallet_denied, *ai_delegate_denied]
    allowed_topics = get_allowed_topics_for_role(role)
    allowed_set = set(allowed_topics)
    allowed_categories = role_allowed_categories(role)
    allowed, denied = filter_topics(topics, allowed_set, allowed_categories)
    if wallet_denied:
        denied = [*denied, *wallet_denied]
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
        sequence_id=manager.tracker.next_sequence(SERVER_CONTROL_SEQ),
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
    )
    await websocket.send_text(response.model_dump_json())
