"""ZMQ-to-WebSocket bridge service.

This module bridges ZMQ pub/sub topics to WebSocket clients, handling
subscription management, message forwarding, throttling, and backpressure.
Control-plane events (invalid topic errors, disconnect) are recorded to
the ``control`` table for audit purposes.
"""

import asyncio
import contextlib
import json
import logging
import time
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from typing import Any
from uuid import uuid7

import zmq
import zmq.asyncio
from fastapi import WebSocket

from snapper.auth.scope_grant_service import get_scope_grant_service
from snapper.auth.websocket_auth import WebSocketAuthManager
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper.data.models import Control
from snapper.data.repository import get_repository
from snapper.interface.websocket.models import SERVER_CONTROL_SEQ
from snapper.interface.websocket.models import ConnectionStats
from snapper.interface.websocket.models import SubscriptionStatsSnapshot
from snapper.interface.websocket.models import SubscriptionTopicDetail
from snapper.interface.websocket.models import TopicConfigurationModel
from snapper.interface.websocket.models import TopicMetricsModel
from snapper.interface.websocket.models import TopicMetricSnapshot
from snapper.interface.websocket.models import TopicSubscriptionModel
from snapper.interface.websocket.schemas import WSErrorResponse
from snapper.interface.websocket.scope_filter import AI_REVIEWS_TOPIC_PREFIX
from snapper.interface.websocket.scope_filter import ALERTS_TOPIC_PREFIX
from snapper.interface.websocket.scope_filter import ORDERS_EVENTS_TOPIC_PREFIX
from snapper.interface.websocket.scope_filter import PORTFOLIO_ACCOUNTS_TOPIC_PREFIX
from snapper.interface.websocket.scope_filter import WalletAccessCache
from snapper.interface.websocket.scope_filter import _enforce_validated_account_state_scope
from snapper.interface.websocket.scope_filter import enforce_ai_review_scope
from snapper.interface.websocket.scope_filter import enforce_alerts_scope
from snapper.interface.websocket.scope_filter import enforce_orders_events_scope
from snapper.interface.websocket.scope_filter import validate_account_state_event
from snapper.messaging.infrastructure.gap_detector import GapDetector
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import HWM_MARKET_DATA
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.data import AccountStateChangedEventData
from snapper.messaging.schemas.messages import GapEnvelope
from snapper.messaging.topics.builders import is_order_topic
from snapper.messaging.topics.schemas import REGISTRY_ROOTS
from snapper.messaging.topics.schemas import TOPIC_REGISTRY
from snapper.messaging.topics.validation import _validate_backtest_prefix
from snapper.utils.logging import set_log_context

logger = logging.getLogger(__name__)

MAX_PENDING_MESSAGES_MARKET = 100

MAX_PENDING_MESSAGES_TRADE = 1000

SEND_TIMEOUT_SECONDS = 1.0

type _AccountTrailingKey = tuple[WebSocket, str, str]
type _AccountTrailingFrame = tuple[str, AccountStateChangedEventData]


@dataclass(frozen=True)
class _DispatchFrame:
    """Normalized frame and shared fan-out state for one dispatch cycle."""

    topic: str
    received_topic: str
    message_str: str
    current_time: float
    max_pending: int
    ai_review_payload: dict[str, Any] | None
    orders_events_payload: JsonObject | None
    account_state_payload: AccountStateChangedEventData | None
    alerts_payload: dict[str, Any] | None
    wallet_access_cache: WalletAccessCache
    wallet_scope_as_of: datetime | None


@dataclass(frozen=True)
class BridgeClientRetirement:
    """Bridge resources captured by synchronous client retirement."""

    topics: tuple[str, ...]
    trailing_tasks: tuple[asyncio.Task[None], ...]


class ZmqWebSocketBridgeService:
    """Service that bridges ZMQ topics to WebSocket clients.

    Manages ZMQ subscriptions, forwards messages to WebSocket clients,
    handles throttling per-topic, and implements backpressure control.

    Attributes:
        connection_manager: WebSocket connection manager reference.
        topic_subscriptions: Mapping of topic to {websocket: subscription}
            dict. Dict-of-dict shape (vs the earlier list-of-subscription)
            gives O(1) membership checks, O(1) unsubscribe, and O(1)
            client-state lookup during fan-out — critical on hot WS
            topics (1000+ msg/sec) and disconnect storms.
        client_subscriptions: Mapping of WebSocket to subscribed topics.
        zmq_subscribers: Mapping of topic to ZMQ socket.
        subscriber_tasks: Mapping of topic to subscription loop task.
        context: ZMQ async context.
        settings: Application settings.
        topic_metrics: Mapping of topic to metrics.
        available_topics: Mapping of topic to configuration.
    """

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Get default parameters from settings.

        Args:
            settings: Application settings instance.

        Returns:
            Dictionary with default parameters.
        """
        return {
            "connection_manager": None,
        }

    def __init__(self, connection_manager: Any) -> None:
        """Initialize the ZMQ-WebSocket bridge.

        Args:
            connection_manager: WebSocket connection manager instance.
        """
        self.connection_manager = connection_manager
        self.topic_subscriptions: dict[str, dict[WebSocket, TopicSubscriptionModel]] = {}
        self.client_subscriptions: dict[WebSocket, set[str]] = {}
        self.zmq_subscribers: dict[str, zmq.asyncio.Socket] = {}
        self.subscriber_tasks: dict[str, asyncio.Task[None]] = {}
        self.context: zmq.asyncio.Context | None = None
        self.settings = get_settings()
        self.topic_metrics: dict[str, TopicMetricsModel] = {}
        self._gap_detector: GapDetector = GapDetector("bridge")
        self.available_topics: dict[str, TopicConfigurationModel] = self._build_topic_config()
        self._shutdown_event: asyncio.Event | None = None
        self._account_trailing_frames: dict[_AccountTrailingKey, _AccountTrailingFrame] = {}
        self._account_trailing_tasks: dict[_AccountTrailingKey, asyncio.Task[None]] = {}

    async def _record_bridge_control(
        self,
        message_type: str,
        outcome: str,
        detail: str | None = None,
    ) -> None:
        """Write a control row for a ZMQ bridge event (non-blocking).

        Any DB failure is logged and swallowed so the bridge is never
        disrupted.

        Args:
            message_type: Discriminator (e.g. ``zmq_subscribe``, ``zmq_unsubscribe``).
            outcome: ``ok``, ``error``, or ``exception``.
            detail: Optional error detail message.
        """
        db_url: str = self.settings.db_url
        if not db_url:
            return
        try:
            tracker: SequenceTracker = self.connection_manager.tracker
            repo = get_repository(db_url)
            now = datetime.now(UTC)
            row = Control(
                transport="zmq",
                direction="inbound",
                message_type=message_type,
                outcome=outcome,
                detail=detail,
                payload=None,
                client_session_id=None,
                client_public_id=None,
                session_id=tracker.session_id,
                sequence_id=tracker.next_sequence(SERVER_CONTROL_SEQ),
                timestamp=now,
            )
            async with repo.session() as session:
                session.add(row)
                await session.commit()
        except Exception as exc:
            logger.warning("Bridge control record write failed (non-blocking): %s", exc)

    def _build_topic_config(self) -> dict[str, TopicConfigurationModel]:
        """Build topic configuration from registry.

        Returns:
            Dictionary mapping topic names to configurations.
        """
        config = {}
        for topic_schema in TOPIC_REGISTRY:
            endpoint: str = self.settings.zmq_broker_xpub
            config[topic_schema.pattern] = TopicConfigurationModel(
                endpoint=endpoint,
                pattern=topic_schema.pattern,
                throttle_ms=topic_schema.throttle_ms,
                throttle_per_topic=topic_schema.throttle_per_topic,
            )
        return config

    def _find_matching_pattern(self, topic: str) -> TopicConfigurationModel | None:
        """Find configuration for a topic by exact match or pattern prefix.

        Args:
            topic: The topic name to match.

        Returns:
            Topic configuration or None if no match found.
        """
        if topic in self.available_topics:
            return self.available_topics[topic]
        for config in self.available_topics.values():
            pattern = config.pattern
            if topic == pattern or topic.startswith(pattern):
                return config
        return None

    def _ensure_client_topics(self, websocket: WebSocket) -> set[str]:
        """Return the tracked topic set for a WebSocket client."""
        if websocket not in self.client_subscriptions:
            self.client_subscriptions[websocket] = set()
        return self.client_subscriptions[websocket]

    def _remove_client_if_empty(self, websocket: WebSocket) -> None:
        """Remove client tracking once it has no subscriptions."""
        if websocket in self.client_subscriptions and not self.client_subscriptions[websocket]:
            del self.client_subscriptions[websocket]

    def _registration_authority_is_current(
        self,
        websocket: WebSocket,
        expected_connection_generation: int | None,
        authority_is_current: Callable[[], bool] | None,
    ) -> bool:
        """Return whether a client may still mutate bridge subscriptions."""
        if self.connection_manager is not None:
            if expected_connection_generation is None:
                if not self.connection_manager.is_connection_active(websocket):
                    return False
            elif not self.connection_manager.is_connection_current(
                websocket,
                expected_connection_generation,
            ):
                return False
        return authority_is_current is None or authority_is_current()

    def retire_client(self, websocket: WebSocket) -> BridgeClientRetirement:
        """Synchronously remove a socket from every bridge send registry.

        The returned resource bundle is finalized asynchronously after the
        no-await barrier has made every in-flight registration check fail.

        Args:
            websocket: Client whose subscriptions are being revoked.

        Returns:
            Topics and cancelled trailing tasks requiring async cleanup.
        """
        topics = set(self.client_subscriptions.pop(websocket, set()))
        topics.update(
            topic
            for topic, subscriptions in self.topic_subscriptions.items()
            if websocket in subscriptions
        )
        for topic in topics:
            subscriptions = self.topic_subscriptions.get(topic)
            if subscriptions is None:
                continue
            subscriptions.pop(websocket, None)
            if topic in self.topic_metrics:
                self.topic_metrics[topic].active_subscribers = len(subscriptions)
        trailing_tasks = self._retire_client_trailing_tasks(websocket)
        return BridgeClientRetirement(
            topics=tuple(sorted(topics)),
            trailing_tasks=trailing_tasks,
        )

    def _retire_client_trailing_tasks(
        self,
        websocket: WebSocket,
    ) -> tuple[asyncio.Task[None], ...]:
        """Cancel trailing-delivery tasks for one synchronously retired client."""
        keys = tuple(key for key in self._account_trailing_tasks if key[0] is websocket)
        if not keys:
            return ()
        current_task = asyncio.current_task()
        trailing_tasks: list[asyncio.Task[None]] = []
        for key in keys:
            task = self._account_trailing_tasks.pop(key)
            self._account_trailing_frames.pop(key, None)
            if task is current_task:
                continue
            task.cancel()
            trailing_tasks.append(task)
        return tuple(trailing_tasks)

    async def finalize_retired_client(
        self,
        retirement: BridgeClientRetirement,
    ) -> None:
        """Drain retired tasks and stop topics that remain subscriber-free.

        Args:
            retirement: Resources captured by :meth:`retire_client`.
        """
        if retirement.trailing_tasks:
            await asyncio.gather(*retirement.trailing_tasks, return_exceptions=True)
        for topic in retirement.topics:
            subscriptions = self.topic_subscriptions.get(topic)
            if subscriptions is None or subscriptions:
                continue
            await self._stop_zmq_subscription(topic)
            subscriptions = self.topic_subscriptions.get(topic)
            if subscriptions:
                await self.start_zmq_subscriber(topic)
            elif subscriptions is not None:
                del self.topic_subscriptions[topic]

    @staticmethod
    def _get_prefix_subscription_error(topic: str) -> str | None:
        """Return validation detail for invalid prefix subscriptions."""
        if not topic.endswith(".") or topic in REGISTRY_ROOTS:
            return None
        if topic.startswith("backtest."):
            bt_valid, bt_err = _validate_backtest_prefix(topic)
            if bt_valid:
                return None
            return f"Malformed backtest prefix rejected: {topic} ({bt_err})"
        return f"Intermediate prefix rejected: {topic}"

    def _default_topic_throttle_ms(self, topic: str, fallback: int = 100) -> int:
        """Return configured throttle for a topic or a fallback value."""
        topic_config = self._find_matching_pattern(topic)
        if topic_config is None:
            return fallback
        return topic_config.throttle_ms

    def _topic_throttle_per_topic(self, topic: str) -> bool:
        """Return whether ``topic``'s throttle applies per received topic.

        True only for schema families that opt in (heartbeats); an exact
        subscription not registered as a pattern falls back to False (a single
        topic has nothing to throttle per-topic against).

        Args:
            topic: The subscription topic (client key).

        Returns:
            The matching schema's ``throttle_per_topic``, or False.
        """
        topic_config = self._find_matching_pattern(topic)
        if topic_config is None:
            return False
        return topic_config.throttle_per_topic

    def _register_topic_subscription(
        self,
        websocket: WebSocket,
        topic: str,
        throttle_ms: int,
        client_id: str = "",
        throttle_per_topic: bool = False,
    ) -> bool:
        """Track subscription state and return whether it is the first subscriber."""
        if (
            self.connection_manager is not None
            and not self.connection_manager.is_connection_active(websocket)
        ):
            return False
        client_topics = self._ensure_client_topics(websocket)
        client_topics.add(topic)
        subscriptions = self.topic_subscriptions.setdefault(topic, {})
        metrics = self.topic_metrics.setdefault(topic, TopicMetricsModel())
        subscriptions[websocket] = TopicSubscriptionModel(
            websocket=websocket,
            throttle_ms=throttle_ms,
            client_id=client_id,
            throttle_per_topic=throttle_per_topic,
        )
        metrics.active_subscribers = len(subscriptions)
        return len(subscriptions) == 1

    def _is_client_subscription_topic_valid(self, topic: str) -> bool:
        """Validate a multi-topic client subscription request."""
        error_detail = self._get_prefix_subscription_error(topic)
        if error_detail is None:
            return True
        logger.warning(error_detail)
        return False

    def _websocket_has_topic_subscription(self, websocket: WebSocket, topic: str) -> bool:
        """Return whether a WebSocket is already subscribed to a topic."""
        return websocket in self.topic_subscriptions.get(topic, {})

    async def _reject_unknown_websocket_topic(self, websocket: WebSocket, topic: str) -> bool:
        """Send invalid-topic feedback and record control-plane telemetry."""
        available = list(self.available_topics)
        error_msg = (
            f"Topic '{topic}' does not match any known pattern. Available patterns: {available}"
        )
        logger.warning(error_msg)
        try:
            tracker = self.connection_manager.tracker
            error_response = WSErrorResponse(
                message=f"Invalid topic: {error_msg}",
                session_id=tracker.session_id,
                sequence_id=tracker.next_sequence(SERVER_CONTROL_SEQ),
                public_id=str(uuid7()),
                timestamp=datetime.now(UTC),
            )
            await websocket.send_text(error_response.model_dump_json())
        except Exception as e:
            logger.exception(f"Failed to send error to client: {e}")
        await self._record_bridge_control(
            "zmq_subscribe",
            "error",
            detail=f"Invalid topic: {topic}",
        )
        return False

    async def _can_subscribe_websocket_topic(self, websocket: WebSocket, topic: str) -> bool:
        """Validate a single-topic WebSocket subscription request."""
        error_detail = self._get_prefix_subscription_error(topic)
        if error_detail is not None:
            logger.warning(error_detail)
            await self._record_bridge_control(
                "zmq_subscribe",
                "error",
                detail=error_detail,
            )
            return False
        if self._find_matching_pattern(topic) is not None:
            return True
        return await self._reject_unknown_websocket_topic(websocket, topic)

    async def subscribe_client(
        self,
        websocket: WebSocket,
        topics: list[str],
        expected_connection_generation: int | None = None,
        authority_is_current: Callable[[], bool] | None = None,
    ) -> bool:
        """Subscribe a WebSocket client to multiple topics.

        Args:
            websocket: The WebSocket connection.
            topics: List of topic names to subscribe to.
            expected_connection_generation: Connection lease captured before
                asynchronous authorization.
            authority_is_current: Optional identity guard for the authenticated
                principal that initiated the subscription.

        Returns:
            Whether the captured connection authority remained current.
        """
        if not self._registration_authority_is_current(
            websocket,
            expected_connection_generation,
            authority_is_current,
        ):
            return False
        client_id = f"{id(websocket)}"
        logger.info(f"Client {client_id} subscribing to topics: {topics}")
        self._ensure_client_topics(websocket)
        for topic in topics:
            if not self._registration_authority_is_current(
                websocket,
                expected_connection_generation,
                authority_is_current,
            ):
                return False
            if not self._is_client_subscription_topic_valid(topic):
                continue
            should_start = self._register_topic_subscription(
                websocket=websocket,
                topic=topic,
                throttle_ms=self._default_topic_throttle_ms(topic),
                client_id=client_id,
                throttle_per_topic=self._topic_throttle_per_topic(topic),
            )
            if should_start:
                await self._start_zmq_subscription(topic)
                if not self._registration_authority_is_current(
                    websocket,
                    expected_connection_generation,
                    authority_is_current,
                ):
                    retirement = self.retire_client(websocket)
                    await self.finalize_retired_client(retirement)
                    return False
        self._remove_client_if_empty(websocket)
        logger.info(
            f"Client {client_id} subscribed. Active subscriptions: "
            f"{len(self.client_subscriptions.get(websocket, set()))}"
        )
        return True

    async def unsubscribe_client(self, websocket: WebSocket, topics: list[str]) -> None:
        """Unsubscribe a WebSocket client from multiple topics.

        Args:
            websocket: The WebSocket connection.
            topics: List of topic names to unsubscribe from.
        """
        client_id = f"{id(websocket)}"
        logger.info(f"Client {client_id} unsubscribing from topics: {topics}")
        if websocket not in self.client_subscriptions:
            return
        for topic in topics:
            self.client_subscriptions[websocket].discard(topic)
            if topic in self.topic_subscriptions:
                removed = self.topic_subscriptions[topic].pop(websocket, None)
                if removed is not None:
                    await self._cancel_account_trailing_for_subscription(websocket, topic)
                if topic in self.topic_metrics:
                    self.topic_metrics[topic].active_subscribers = len(
                        self.topic_subscriptions[topic]
                    )
                if not self.topic_subscriptions[topic]:
                    await self._stop_zmq_subscription(topic)
                    del self.topic_subscriptions[topic]
        if not self.client_subscriptions[websocket]:
            del self.client_subscriptions[websocket]
        logger.info(f"Client {client_id} unsubscribed from {len(topics)} topics")

    async def disconnect_client(self, websocket: WebSocket) -> None:
        """Disconnect a client and clean up all subscriptions.

        Args:
            websocket: The WebSocket connection to disconnect.
        """
        client_id = f"{id(websocket)}"
        logger.info(f"Client {client_id} disconnected, cleaning up subscriptions")
        if websocket not in self.client_subscriptions:
            with contextlib.suppress(Exception):
                await websocket.close()
            await self._record_bridge_control(
                "zmq_disconnect",
                "ok",
                detail=f"Client {client_id} disconnected (no subscriptions)",
            )
            return
        client_topics = list(self.client_subscriptions[websocket])
        await self.unsubscribe_client(websocket, client_topics)
        with contextlib.suppress(Exception):
            await websocket.close()
        await self._record_bridge_control(
            "zmq_disconnect",
            "ok",
            detail=f"Client {client_id} disconnected, unsubscribed from {len(client_topics)} topics",
        )
        logger.info(f"Client {client_id} cleanup complete")

    async def _start_zmq_subscription(self, topic: str) -> None:
        """Start ZMQ subscription for a topic.

        Args:
            topic: The topic name to subscribe to.
        """
        if topic in self.subscriber_tasks:
            logger.warning(f"ZMQ subscription for {topic} already active")
            return
        logger.info(f"Starting ZMQ subscription for topic: {topic}")
        topic_config = self._find_matching_pattern(topic)
        if not topic_config:
            logger.error(f"No configuration found for topic: {topic} (no matching pattern)")
            return
        try:
            if self.context is None:
                raise RuntimeError("ZMQ context must be initialized in start()")
            socket = self.context.socket(zmq.SUB)
            apply_hwm(socket, rcvhwm=HWM_MARKET_DATA)
            socket.connect(topic_config.endpoint)
            socket.setsockopt(zmq.SUBSCRIBE, topic_config.pattern.encode("utf-8"))
            self.zmq_subscribers[topic] = socket
            task = asyncio.create_task(self._zmq_subscription_loop(topic, socket))
            self.subscriber_tasks[topic] = task
            logger.info(f"ZMQ subscription started for {topic} on {topic_config.endpoint}")
        except Exception as e:
            logger.exception(f"Failed to start ZMQ subscription for {topic}: {e}")
        await asyncio.sleep(0)

    async def _stop_zmq_subscription(self, topic: str) -> None:
        """Stop ZMQ subscription for a topic.

        Args:
            topic: The topic name to unsubscribe from.
        """
        logger.info(f"Stopping ZMQ subscription for topic: {topic}")
        if topic in self.subscriber_tasks:
            task = self.subscriber_tasks[topic]
            current = asyncio.current_task()
            if current is task:
                logger.debug(f"Self-await detected for {topic}, scheduling deferred cleanup")
                task.cancel()
            else:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            del self.subscriber_tasks[topic]
        if topic in self.zmq_subscribers:
            socket = self.zmq_subscribers[topic]
            socket.setsockopt(zmq.LINGER, 0)
            socket.close()
            del self.zmq_subscribers[topic]
        self._gap_detector.reset_topic(topic)
        logger.info(f"ZMQ subscription stopped for topic: {topic}")

    def _check_gap(self, topic: str, received_topic: str, payload_str: str) -> bool:
        """Run gap detection on a received message.

        Parses the payload as a GapEnvelope to extract session_id,
        sequence_id, and type. The gap detector stream key is the
        received ZMQ topic, matching the publisher-side SequenceTracker
        which maintains per-topic monotonic counters.

        Args:
            topic: Subscription topic key used for metrics lookup.
            received_topic: Exact topic from the ZMQ frame, used as
                the gap detector stream key.
            payload_str: Raw JSON payload string.

        Returns:
            True if the payload is valid JSON and gap detection ran,
            False if the payload is malformed and the message must be dropped.
        """
        try:
            envelope = GapEnvelope.model_validate_json(payload_str)
            if not self._gap_detector.check(
                received_topic,
                envelope.session_id,
                envelope.sequence_id,
                wallet_public_id=envelope.wallet_public_id,
            ):
                if topic in self.topic_metrics:
                    self.topic_metrics[topic].invalid_messages += 1
                return False
            return True
        except Exception:
            logger.warning("Dropping malformed message on topic %s", received_topic)
            if topic in self.topic_metrics:
                self.topic_metrics[topic].invalid_messages += 1
            return False

    async def _process_zmq_message(self, topic: str, message_parts: list[bytes]) -> None:
        """Process a single received ZMQ multipart message.

        Drops messages whose payload is not valid JSON (strict mode).

        Args:
            topic: The topic being subscribed to.
            message_parts: Raw multipart message bytes from ZMQ.
        """
        if len(message_parts) != 2:
            logger.warning(
                f"Invalid message format for {topic}: expected 2 parts, "
                f"got {len(message_parts)}"
            )
            return
        topic_bytes, payload_bytes = message_parts
        received_topic = topic_bytes.decode("utf-8")
        payload_str = payload_bytes.decode("utf-8")
        if not self._check_gap(topic, received_topic, payload_str):
            return
        if topic in self.topic_metrics:
            self.topic_metrics[topic].received_count += 1
            self.topic_metrics[topic].last_message_ts = time.time()
        await self._forward_to_clients(topic, received_topic, payload_str)

    async def _zmq_receive_loop(self, topic: str, socket: zmq.asyncio.Socket) -> None:
        """Inner receive loop that processes messages until cancelled.

        Args:
            topic: The topic being subscribed to.
            socket: The ZMQ socket to receive from.
        """
        while True:
            try:
                message_parts = await socket.recv_multipart()
                await self._process_zmq_message(topic, message_parts)
            except zmq.ZMQError as e:
                logger.exception(f"ZMQ error in subscription loop for {topic}: {e}")
                await asyncio.sleep(1)
            except Exception as e:
                logger.exception(f"Unexpected error in subscription loop for {topic}: {e}")
                if topic in self.topic_metrics:
                    self.topic_metrics[topic].error_count += 1
                await asyncio.sleep(1)

    async def _zmq_subscription_loop(self, topic: str, socket: zmq.asyncio.Socket) -> None:
        """Main loop for receiving ZMQ messages and forwarding to clients.

        Args:
            topic: The topic being subscribed to.
            socket: The ZMQ socket to receive from.
        """
        logger.info(f"ZMQ subscription loop started for {topic}")
        try:
            await self._zmq_receive_loop(topic, socket)
        except asyncio.CancelledError:
            logger.info(f"ZMQ subscription loop cancelled for {topic}")
            raise
        except Exception as e:
            logger.exception(f"Fatal error in subscription loop for {topic}: {e}")

    def _is_trade_topic(self, topic: str) -> bool:
        """Check if a topic is trade-related (orders.commands/orders.events).

        Uses centralized is_order_topic() for 2-level prefix matching.
        For example, 'orders.events.kraken.BTC-USD' matches 'orders.events'.

        Args:
            topic: The topic name to check.

        Returns:
            True if the topic is trade-related.
        """
        return is_order_topic(topic)

    def _get_max_pending(self, topic: str) -> int:
        """Get maximum pending messages for a topic based on category.

        Args:
            topic: The topic name.

        Returns:
            Maximum pending message count.
        """
        if self._is_trade_topic(topic):
            return MAX_PENDING_MESSAGES_TRADE
        return MAX_PENDING_MESSAGES_MARKET

    def _is_throttled(
        self,
        subscription: TopicSubscriptionModel,
        current_time: float,
        topic: str,
        received_topic: str,
    ) -> bool:
        """Check if a subscription should be throttled.

        For a ``throttle_per_topic`` subscription (heartbeats) the throttle
        window is tracked per received topic, so one component's frames never
        throttle another's under a shared root subscription; otherwise the
        single per-subscription ``last_sent`` is used.

        Args:
            subscription: The subscription to check.
            current_time: Current timestamp.
            topic: Subscription topic (client key) for metrics tracking.
            received_topic: The actual topic from the ZMQ frame, keying the
                per-topic throttle window.

        Returns:
            True if the message should be throttled.
        """
        last_sent = (
            subscription.last_sent_by_topic.get(received_topic, 0.0)
            if subscription.throttle_per_topic
            else subscription.last_sent
        )
        if current_time - last_sent < (subscription.throttle_ms / 1000.0):
            self._increment_throttled_count(topic)
            return True
        return False

    def _increment_throttled_count(self, topic: str) -> None:
        """Count one coalesced or dropped frame when metrics are available."""
        if topic in self.topic_metrics:
            self.topic_metrics[topic].throttled_count += 1

    def _is_registered_subscription(
        self,
        subscription: TopicSubscriptionModel,
        topic: str,
    ) -> bool:
        """Return whether the registry still owns this exact subscription."""
        registered = self.topic_subscriptions.get(topic, {}).get(subscription.websocket)
        return registered is subscription

    def _coalesce_account_frame(
        self,
        *,
        subscription: TopicSubscriptionModel,
        topic: str,
        received_topic: str,
        message_str: str,
        payload: AccountStateChangedEventData,
        current_time: float,
    ) -> bool:
        """Queue the newest account frame when its wallet window is open.

        Args:
            subscription: Destination subscription owning throttle state.
            topic: Subscription root used for metrics and registration checks.
            received_topic: Exact wallet topic, which keys the coalescing window.
            message_str: Validated raw frame retained for eventual delivery.
            payload: Strict typed event retained for a fresh scope check.
            current_time: Receive timestamp used to calculate the window close.

        Returns:
            ``True`` when delivery is owned by a trailing worker.
        """
        key: _AccountTrailingKey = (subscription.websocket, topic, received_topic)
        if key in self._account_trailing_tasks:
            self._account_trailing_frames[key] = (message_str, payload)
            self._increment_throttled_count(topic)
            return True
        if not self._is_throttled(subscription, current_time, topic, received_topic):
            return False
        self._account_trailing_frames[key] = (message_str, payload)
        last_sent = (
            subscription.last_sent_by_topic.get(received_topic, 0.0)
            if subscription.throttle_per_topic
            else subscription.last_sent
        )
        delay = max(0.0, last_sent + (subscription.throttle_ms / 1000.0) - current_time)
        task = asyncio.create_task(
            self._deliver_account_trailing(key, subscription, delay),
            name=f"account-trailing:{subscription.client_id}:{received_topic}",
        )
        self._account_trailing_tasks[key] = task
        return True

    async def _deliver_account_trailing(
        self,
        key: _AccountTrailingKey,
        subscription: TopicSubscriptionModel,
        initial_delay: float,
    ) -> None:
        """Deliver each latest account frame at its wallet window's trailing edge.

        Args:
            key: Subscriber, root, and received-wallet topic identity.
            subscription: Exact subscription instance that queued the worker.
            initial_delay: Seconds remaining in the current throttle window.
        """
        try:
            await asyncio.sleep(initial_delay)
            _, topic, received_topic = key
            while True:
                if not self._is_registered_subscription(subscription, topic):
                    return
                message_str, payload = self._account_trailing_frames.pop(key)
                frame = _DispatchFrame(
                    topic=topic,
                    received_topic=received_topic,
                    message_str=message_str,
                    current_time=time.time(),
                    max_pending=self._get_max_pending(topic),
                    ai_review_payload=None,
                    orders_events_payload=None,
                    account_state_payload=payload,
                    alerts_payload=None,
                    wallet_access_cache={},
                    wallet_scope_as_of=datetime.now(UTC),
                )
                await self._dispatch_to_subscription(
                    subscription=subscription,
                    frame=frame,
                    apply_throttle=False,
                )
                if key not in self._account_trailing_frames:
                    return
                await asyncio.sleep(subscription.throttle_ms / 1000.0)
        finally:
            current_task = asyncio.current_task()
            if self._account_trailing_tasks.get(key) is current_task:
                self._account_trailing_tasks.pop(key, None)
                self._account_trailing_frames.pop(key, None)

    async def _cancel_account_trailing_keys(
        self,
        keys: tuple[_AccountTrailingKey, ...],
    ) -> None:
        """Cancel and drain selected trailing workers without self-awaiting.

        Args:
            keys: Exact trailing worker identities being retired.
        """
        current_task = asyncio.current_task()
        draining: list[asyncio.Task[None]] = []
        for key in keys:
            task = self._account_trailing_tasks.pop(key)
            self._account_trailing_frames.pop(key, None)
            if task is current_task:
                continue
            task.cancel()
            draining.append(task)
        if draining:
            await asyncio.gather(*draining, return_exceptions=True)

    async def _cancel_account_trailing_for_subscription(
        self,
        websocket: WebSocket,
        topic: str,
    ) -> None:
        """Cancel trailing deliveries owned by one removed subscription."""
        keys = tuple(
            key for key in self._account_trailing_tasks if key[0] is websocket and key[1] == topic
        )
        await self._cancel_account_trailing_keys(keys)

    async def _cancel_all_account_trailing(self) -> None:
        """Cancel and drain every bridge-owned account trailing worker."""
        await self._cancel_account_trailing_keys(tuple(self._account_trailing_tasks))

    async def _handle_backpressure(
        self,
        subscription: TopicSubscriptionModel,
        topic: str,
        max_pending: int,
        is_trade: bool,
    ) -> bool:
        """Handle backpressure for a subscription that exceeded pending limit.

        Args:
            subscription: The subscription exceeding limits.
            topic: Topic name for metrics and logging.
            max_pending: Maximum allowed pending messages.
            is_trade: Whether this is a trade topic.

        Returns:
            True if the message should be dropped (backpressure applied).
        """
        if subscription.pending_count < max_pending:
            return False
        if topic in self.topic_metrics:
            self.topic_metrics[topic].dropped_count += 1
        if is_trade:
            logger.error(
                f"Backpressure overflow for trade topic {topic}, "
                f"client {subscription.client_id}: "
                f"pending={subscription.pending_count}, max={max_pending}. "
                f"Closing connection to prevent data loss."
            )
            with contextlib.suppress(Exception):
                await self.disconnect_client(subscription.websocket)
        else:
            logger.debug(
                f"Backpressure: dropping market data for client "
                f"{subscription.client_id}, pending={subscription.pending_count}"
            )
        return True

    async def _try_send_message(
        self,
        subscription: TopicSubscriptionModel,
        topic: str,
        message_str: str,
        current_time: float,
        received_topic: str,
    ) -> None:
        """Attempt to send a message to a single subscriber.

        Args:
            subscription: Target subscription.
            topic: Topic name for metrics.
            message_str: Message payload to send.
            current_time: Current timestamp for last_sent update.
            received_topic: The actual topic from the ZMQ frame, keying the
                per-topic throttle window for ``throttle_per_topic`` subs.
        """
        subscription.pending_count += 1
        try:
            async with asyncio.timeout(SEND_TIMEOUT_SECONDS):
                await subscription.websocket.send_text(message_str)
        except TimeoutError:
            if topic in self.topic_metrics:
                self.topic_metrics[topic].timeout_count += 1
            logger.warning(
                f"Send timeout for client {subscription.client_id} on topic {topic}, "
                f"disconnecting slow client"
            )
            subscription.pending_count = max(0, subscription.pending_count - 1)
            with contextlib.suppress(Exception):
                await self.disconnect_client(subscription.websocket)
            return
        if subscription.throttle_per_topic:
            subscription.last_sent_by_topic[received_topic] = current_time
        else:
            subscription.last_sent = current_time
        subscription.pending_count = max(0, subscription.pending_count - 1)
        if topic in self.topic_metrics:
            self.topic_metrics[topic].forwarded_count += 1

    def _validate_topic_match(self, topic: str, received_topic: str) -> bool:
        """Check whether received_topic matches the subscription contract.

        Root subscriptions (ending with ``"."``) require a prefix match;
        exact subscriptions require equality.  Mismatches are logged and
        counted as invalid.

        Args:
            topic: The subscription topic (client key).
            received_topic: The actual topic from ZMQ message frame.

        Returns:
            True when the topic matches, False when it should be dropped.
        """
        is_root = topic.endswith(".")
        if is_root and not received_topic.startswith(topic):
            logger.warning(
                "Bridge routing mismatch: received_topic=%s does not match "
                "root subscription=%s, dropping message",
                received_topic,
                topic,
            )
            if topic in self.topic_metrics:
                self.topic_metrics[topic].invalid_messages += 1
            return False
        if not is_root and received_topic != topic:
            logger.warning(
                "Bridge routing mismatch: received_topic=%s does not match "
                "exact subscription=%s, dropping message",
                received_topic,
                topic,
            )
            if topic in self.topic_metrics:
                self.topic_metrics[topic].invalid_messages += 1
            return False
        return True

    async def _forward_to_clients(self, topic: str, received_topic: str, message_str: str) -> None:
        """Forward a message to all subscribed WebSocket clients.

        Implements throttling and backpressure control. Trade topics
        disconnect slow clients, market data topics drop messages.

        Validates that ``received_topic`` matches the subscription contract:
        registry root subscriptions require prefix match, exact topic
        subscriptions require equality. Mismatches are logged and dropped.

        Args:
            topic: The subscription topic (client key).
            received_topic: The actual topic from ZMQ message frame.
            message_str: The message payload string.
        """
        if topic not in self.topic_subscriptions:
            return
        if not self._validate_topic_match(topic, received_topic):
            return
        ai_review_payload = self._maybe_parse_ai_review_payload(topic, message_str)
        if topic.startswith(AI_REVIEWS_TOPIC_PREFIX) and ai_review_payload is None:
            logger.warning(
                "Dropping malformed ai_reviews.* frame (failed JSON / non-dict envelope) "
                "for topic=%s — would otherwise bypass per-frame scope filter",
                topic,
            )
            return
        orders_events_payload = self._maybe_parse_orders_events_payload(topic, message_str)
        if not self._orders_events_frame_is_valid(topic, orders_events_payload):
            return
        account_state_payload = self._maybe_parse_account_state_payload(
            topic,
            received_topic,
            message_str,
        )
        if not self._account_state_frame_is_valid(topic, account_state_payload):
            return
        alerts_payload = self._maybe_parse_alerts_payload(topic, message_str)
        if not self._alerts_frame_is_valid(topic, alerts_payload):
            return
        wallet_access_cache: WalletAccessCache = {}
        wallet_scope_as_of = (
            datetime.now(UTC)
            if orders_events_payload is not None or account_state_payload is not None
            else None
        )
        current_time = time.time()
        max_pending = self._get_max_pending(topic)
        frame = _DispatchFrame(
            topic=topic,
            received_topic=received_topic,
            message_str=message_str,
            current_time=current_time,
            max_pending=max_pending,
            ai_review_payload=ai_review_payload,
            orders_events_payload=orders_events_payload,
            account_state_payload=account_state_payload,
            alerts_payload=alerts_payload,
            wallet_access_cache=wallet_access_cache,
            wallet_scope_as_of=wallet_scope_as_of,
        )
        snapshot = tuple(self.topic_subscriptions[topic].values())
        for subscription in snapshot:
            await self._dispatch_to_subscription(
                subscription=subscription,
                frame=frame,
            )

    def _alerts_frame_is_valid(self, topic: str, alerts_payload: dict[str, Any] | None) -> bool:
        """Return False (and log + count) when an ``alerts.*`` frame is malformed.

        Non-alerts topics short-circuit to ``True``. A ``None`` payload on
        an ``alerts.*`` topic means :meth:`_maybe_parse_alerts_payload`
        already rejected the frame (JSON / shape / discriminator). The
        metrics counter is bumped at this canonical drop site rather
        than inside the parse helper to keep the parser side-effect-free.
        """
        if not topic.startswith(ALERTS_TOPIC_PREFIX):
            return True
        if alerts_payload is not None:
            return True
        logger.warning(
            "Dropping malformed alerts.* frame (failed JSON / non-dict envelope / "
            "missing or non-string user_public_id) for topic=%s — would otherwise "
            "bypass per-frame scope filter",
            topic,
        )
        if topic in self.topic_metrics:
            self.topic_metrics[topic].invalid_messages += 1
        return False

    def _orders_events_frame_is_valid(
        self, topic: str, orders_events_payload: JsonObject | None
    ) -> bool:
        """Return False (and log + count) when an ``orders.events.*`` frame is malformed.

        Non-orders.events topics short-circuit to ``True``. A ``None``
        payload on an orders.events.* topic means
        :meth:`_maybe_parse_orders_events_payload` already rejected the
        frame (JSON / shape / discriminator). The metrics counter is
        bumped at this canonical drop site rather than inside the parse
        helper to keep the parser side-effect-free.
        """
        return self._wallet_scoped_frame_is_valid(
            topic=topic,
            topic_prefix=ORDERS_EVENTS_TOPIC_PREFIX,
            payload=orders_events_payload,
            family="orders.events",
        )

    def _account_state_frame_is_valid(
        self,
        topic: str,
        account_state_payload: AccountStateChangedEventData | None,
    ) -> bool:
        """Return whether a ``portfolio.accounts.*`` frame is well formed.

        Args:
            topic: Subscription topic used for fan-out.
            account_state_payload: Strict typed event or ``None``.

        Returns:
            ``False`` for malformed account invalidations, otherwise ``True``.
        """
        if not topic.startswith(PORTFOLIO_ACCOUNTS_TOPIC_PREFIX):
            return True
        if account_state_payload is not None:
            return True
        return self._drop_wallet_scoped_frame(
            topic=topic,
            family="portfolio.accounts",
            detail="failed strict event schema, UUID7 topic, or topic-payload wallet match",
        )

    def _wallet_scoped_frame_is_valid(
        self,
        *,
        topic: str,
        topic_prefix: str,
        payload: JsonObject | None,
        family: str,
    ) -> bool:
        """Validate the parsed envelope required by a wallet scope filter.

        Args:
            topic: Subscription topic used for fan-out.
            topic_prefix: Wallet-scoped family owned by the caller.
            payload: Parsed envelope or ``None`` after a parse failure.
            family: Human-readable topic family for diagnostics.

        Returns:
            ``False`` after logging and counting a malformed family frame.
        """
        if not topic.startswith(topic_prefix):
            return True
        if payload is not None:
            return True
        return self._drop_wallet_scoped_frame(
            topic=topic,
            family=family,
            detail="failed JSON / non-dict envelope / missing or non-string wallet_public_id",
        )

    def _drop_wallet_scoped_frame(self, *, topic: str, family: str, detail: str) -> bool:
        """Log, count, and reject one invalid wallet-scoped frame.

        Args:
            topic: Subscription topic used for fan-out.
            family: Human-readable topic family for diagnostics.
            detail: Validation failure detail.

        Returns:
            Always ``False`` so callers can return the canonical drop verdict.
        """
        logger.warning(
            "Dropping invalid %s.* frame (%s) for topic=%s — would otherwise "
            "bypass per-frame scope filter",
            family,
            detail,
            topic,
        )
        if topic in self.topic_metrics:
            self.topic_metrics[topic].invalid_messages += 1
        return False

    def _subscription_frame_is_throttled(
        self,
        subscription: TopicSubscriptionModel,
        frame: _DispatchFrame,
        apply_throttle: bool,
    ) -> bool:
        """Return whether throttling consumed or rejected one subscriber frame."""
        if not apply_throttle:
            return False
        if frame.account_state_payload is not None:
            return self._coalesce_account_frame(
                subscription=subscription,
                topic=frame.topic,
                received_topic=frame.received_topic,
                message_str=frame.message_str,
                payload=frame.account_state_payload,
                current_time=frame.current_time,
            )
        return self._is_throttled(
            subscription,
            frame.current_time,
            frame.topic,
            frame.received_topic,
        )

    async def _subscription_scope_is_allowed(
        self,
        subscription: TopicSubscriptionModel,
        frame: _DispatchFrame,
    ) -> bool:
        """Return whether every applicable per-frame scope gate admits a frame."""
        if frame.ai_review_payload is not None and not await self._enforce_ai_review_scope(
            subscription=subscription,
            topic=frame.topic,
            payload=frame.ai_review_payload,
        ):
            return False
        if frame.orders_events_payload is not None and not await self._enforce_orders_events_scope(
            subscription=subscription,
            topic=frame.topic,
            payload=frame.orders_events_payload,
            access_cache=frame.wallet_access_cache,
            as_of=frame.wallet_scope_as_of,
        ):
            return False
        if frame.account_state_payload is not None and not await self._enforce_account_state_scope(
            subscription=subscription,
            topic=frame.received_topic,
            payload=frame.account_state_payload,
            access_cache=frame.wallet_access_cache,
            as_of=frame.wallet_scope_as_of,
        ):
            return False
        return frame.alerts_payload is None or self._enforce_alerts_scope(
            subscription=subscription,
            topic=frame.topic,
            payload=frame.alerts_payload,
        )

    async def _dispatch_to_subscription(
        self,
        *,
        subscription: TopicSubscriptionModel,
        frame: _DispatchFrame,
        apply_throttle: bool = True,
    ) -> None:
        """Apply throttle + per-frame scope + backpressure filters to one subscriber.

        Sends the message through :meth:`_try_send_message` when all
        gates pass. Disconnects the client on any send-side exception
        so a misbehaving socket cannot wedge the fan-out loop.
        """
        try:
            if not self._is_registered_subscription(subscription, frame.topic):
                return
            if self._subscription_frame_is_throttled(
                subscription=subscription,
                frame=frame,
                apply_throttle=apply_throttle,
            ):
                return
            if not await self._subscription_scope_is_allowed(
                subscription=subscription,
                frame=frame,
            ):
                return
            if not self._is_registered_subscription(subscription, frame.topic):
                return
            if await self._handle_backpressure(
                subscription,
                frame.topic,
                frame.max_pending,
                self._is_trade_topic(frame.topic),
            ):
                return
            if not self._is_registered_subscription(subscription, frame.topic):
                return
            await self._try_send_message(
                subscription,
                frame.topic,
                frame.message_str,
                frame.current_time,
                frame.received_topic,
            )
        except Exception as e:
            if not self._is_registered_subscription(subscription, frame.topic):
                return
            logger.warning(f"Failed to send message to client {subscription.client_id}: {e}")
            with contextlib.suppress(Exception):
                await self.disconnect_client(subscription.websocket)

    def _maybe_parse_ai_review_payload(self, topic: str, message_str: str) -> dict[str, Any] | None:
        """Parse an ``ai_reviews.*`` frame's JSON payload exactly once.

        Returns ``None`` for non-AI-review topics (no per-frame scope
        check needed) AND for malformed payloads (defensive: a
        non-dict payload would have failed downstream serialisation
        anyway, so dropping it here costs nothing).
        """
        if not topic.startswith(AI_REVIEWS_TOPIC_PREFIX):
            return None
        try:
            parsed = json.loads(message_str)
        except json.JSONDecodeError:
            return None
        if not isinstance(parsed, dict):
            return None
        return parsed

    def _maybe_parse_alerts_payload(self, topic: str, message_str: str) -> dict[str, Any] | None:
        """Parse an ``alerts.*`` frame's JSON payload exactly once.

        Returns ``None`` for non-alerts topics (no per-frame scope
        check needed) AND for malformed payloads. Fail-closed: the
        four guards below MUST drop the frame before
        :func:`enforce_alerts_scope` is invoked, so the filter never
        sees malformed input.

        Guards (each returns ``None``):

        - JSON decode failure.
        - Non-dict top-level payload.
        - Missing ``user_public_id`` key.
        - Non-string ``user_public_id`` value.

        The ``user_public_id`` discriminator is what the per-frame
        filter consults. A non-string value would otherwise reach the
        filter and get dropped there anyway, but pre-screening at the
        bridge keeps the filter contract clean and lets the bridge
        increment ``invalid_messages`` at the canonical drop site.
        """
        if not topic.startswith(ALERTS_TOPIC_PREFIX):
            return None
        try:
            parsed = json.loads(message_str)
        except json.JSONDecodeError:
            return None
        if not isinstance(parsed, dict):
            return None
        user_public_id = parsed.get("user_public_id")
        if not isinstance(user_public_id, str):
            return None
        return parsed

    def _maybe_parse_orders_events_payload(self, topic: str, message_str: str) -> JsonObject | None:
        """Parse an ``orders.events.*`` frame's JSON payload exactly once.

        Returns ``None`` for non-orders.events. topics (no per-frame
        scope check needed) AND for malformed payloads. Fail-closed:
        the four guards below MUST drop the frame before
        :func:`enforce_orders_events_scope` is invoked, so the filter
        never sees malformed input.

        Guards (each returns ``None``):

        - JSON decode failure.
        - Non-dict top-level payload.
        - Missing ``wallet_public_id`` key.
        - Non-string ``wallet_public_id`` value.

        The ``wallet_public_id`` discriminator is what the per-frame
        filter consults. A non-string value would otherwise reach the
        filter and get dropped there anyway, but pre-screening at the
        bridge keeps the filter contract clean and lets the bridge
        increment ``invalid_messages`` at the canonical drop site.
        """
        return self._maybe_parse_wallet_scoped_payload(
            topic=topic,
            topic_prefix=ORDERS_EVENTS_TOPIC_PREFIX,
            message_str=message_str,
        )

    def _maybe_parse_account_state_payload(
        self,
        topic: str,
        received_topic: str,
        message_str: str,
    ) -> AccountStateChangedEventData | None:
        """Parse and validate one ``portfolio.accounts.*`` event.

        Args:
            topic: Subscription topic used to select the account family.
            received_topic: Full wallet topic carried by the ZMQ frame.
            message_str: Raw JSON payload.

        Returns:
            Strict typed event when its schema and topic invariants hold.
        """
        if not topic.startswith(PORTFOLIO_ACCOUNTS_TOPIC_PREFIX):
            return None
        return validate_account_state_event(
            topic=received_topic,
            payload=message_str,
        )

    def _maybe_parse_wallet_scoped_payload(
        self,
        *,
        topic: str,
        topic_prefix: str,
        message_str: str,
    ) -> JsonObject | None:
        """Parse the wallet key required by a per-frame scope filter.

        Args:
            topic: Subscription topic used for fan-out.
            topic_prefix: Wallet-scoped family owned by the caller.
            message_str: Raw JSON payload.

        Returns:
            Parsed envelope with a string wallet id, otherwise ``None``.
        """
        if not topic.startswith(topic_prefix):
            return None
        try:
            parsed = json.loads(message_str)
        except json.JSONDecodeError:
            return None
        if not isinstance(parsed, dict):
            return None
        wallet_public_id = parsed.get("wallet_public_id")
        if not isinstance(wallet_public_id, str):
            return None
        return parsed

    def _enforce_alerts_scope(
        self,
        *,
        subscription: TopicSubscriptionModel,
        topic: str,
        payload: Mapping[str, Any],
    ) -> bool:
        """Per-frame scope filter for the ``alerts.*`` family.

        Resolves the destination socket's principal via the
        :class:`WebSocketAuthManager` singleton and delegates to
        :func:`enforce_alerts_scope`. Forward iff the helper returns
        ``True``.
        """
        principal = WebSocketAuthManager.get_instance().get_authenticated_user(
            subscription.websocket
        )
        return enforce_alerts_scope(
            topic=topic,
            connection_principal=principal,
            payload=payload,
        )

    async def _enforce_orders_events_scope(
        self,
        *,
        subscription: TopicSubscriptionModel,
        topic: str,
        payload: Mapping[str, JsonValue],
        access_cache: WalletAccessCache,
        as_of: datetime | None,
    ) -> bool:
        """Per-frame scope filter for the ``orders.events.*`` family.

        Resolves the destination socket's principal via the
        :class:`WebSocketAuthManager` singleton and delegates to
        :func:`enforce_orders_events_scope`. Forward iff the helper
        returns ``True``.
        """
        principal = WebSocketAuthManager.get_instance().get_authenticated_user(
            subscription.websocket
        )
        return await enforce_orders_events_scope(
            topic=topic,
            connection_principal=principal,
            payload=payload,
            scope_grant_service=get_scope_grant_service(),
            as_of=as_of,
            accessible_wallets_cache=access_cache,
        )

    async def _enforce_account_state_scope(
        self,
        *,
        subscription: TopicSubscriptionModel,
        topic: str,
        payload: AccountStateChangedEventData,
        access_cache: WalletAccessCache,
        as_of: datetime | None,
    ) -> bool:
        """Apply wallet scope to one ``portfolio.accounts.*`` subscriber.

        Args:
            subscription: Destination subscription and socket.
            topic: Subscription topic used for fan-out.
            payload: Parsed account invalidation envelope.
            access_cache: Frame-local accessible-wallet cache.
            as_of: Wall-clock shared across the frame's fan-out.

        Returns:
            Whether the account invalidation may reach the destination.
        """
        principal = WebSocketAuthManager.get_instance().get_authenticated_user(
            subscription.websocket
        )
        return await _enforce_validated_account_state_scope(
            topic=topic,
            connection_principal=principal,
            event=payload,
            scope_grant_service=get_scope_grant_service(),
            as_of=as_of,
            accessible_wallets_cache=access_cache,
        )

    async def _enforce_ai_review_scope(
        self,
        *,
        subscription: TopicSubscriptionModel,
        topic: str,
        payload: Mapping[str, Any],
    ) -> bool:
        """Per-frame scope filter for the ``ai_reviews.*`` family.

        Resolves the destination socket's principal via the
        :class:`WebSocketAuthManager` singleton + delegates to
        :func:`enforce_ai_review_scope`. Forward iff the helper
        returns ``True``.
        """
        principal = WebSocketAuthManager.get_instance().get_authenticated_user(
            subscription.websocket
        )
        return await enforce_ai_review_scope(
            topic=topic,
            connection_principal=principal,
            payload=payload,
            scope_grant_service=get_scope_grant_service(),
        )

    async def start_zmq_subscriber(self, topic: str) -> None:
        """Start a ZMQ subscriber for a specific topic.

        Args:
            topic: The topic name to subscribe to.
        """
        if topic in self.subscriber_tasks:
            return
        config = self.available_topics.get(topic)
        if not config:
            config = self._find_matching_pattern(topic)
        if not config:
            logger.warning(f"Unknown topic: {topic}")
            return
        try:
            if self.context is None:
                raise RuntimeError("ZMQ context must be initialized in start()")
            socket = self.context.socket(zmq.SUB)
            apply_hwm(socket, rcvhwm=HWM_MARKET_DATA)
            socket.connect(config.endpoint)
            socket.setsockopt(zmq.SUBSCRIBE, topic.encode())
            self.zmq_subscribers[topic] = socket
            task = asyncio.create_task(self._handle_zmq_messages(topic, socket, config))
            self.subscriber_tasks[topic] = task
            logger.info(f"Started ZMQ subscriber for topic: {topic}")
        except Exception as e:
            logger.exception(f"Failed to start ZMQ subscriber for {topic}: {e}")
        await asyncio.sleep(0)

    async def stop_zmq_subscriber(self, topic: str) -> None:
        """Stop a ZMQ subscriber for a specific topic.

        Args:
            topic: The topic name to unsubscribe from.
        """
        if topic in self.subscriber_tasks:
            self.subscriber_tasks[topic].cancel()
            del self.subscriber_tasks[topic]
        if topic in self.zmq_subscribers:
            self.zmq_subscribers[topic].setsockopt(zmq.LINGER, 0)
            self.zmq_subscribers[topic].close()
            del self.zmq_subscribers[topic]
        self._gap_detector.reset_topic(topic)
        logger.info(f"Stopped ZMQ subscriber for topic: {topic}")
        await asyncio.sleep(0)

    async def _handle_zmq_messages(
        self, topic: str, socket: zmq.asyncio.Socket, _config: TopicConfigurationModel
    ) -> None:
        """Handle incoming ZMQ messages for a topic.

        Routes through ``_process_zmq_message`` which applies backpressure,
        pending-count tracking, and metrics via ``_forward_to_clients``.

        Args:
            topic: The topic being handled.
            socket: The ZMQ socket to receive from.
            _config: Topic configuration supplied by the caller. The
                handler routes by ``topic`` and socket frames while
                keeping this argument for the subscriber callback
                interface.
        """
        try:
            while True:
                parts = await socket.recv_multipart()
                try:
                    await self._process_zmq_message(topic, parts)
                except Exception as e:
                    logger.exception(f"Error processing ZMQ message for {topic}: {e}")
        except asyncio.CancelledError:
            logger.info(f"ZMQ message handler for {topic} cancelled")
            raise
        except Exception as e:
            logger.exception(f"ZMQ message handler for {topic} failed: {e}")

    async def subscribe_websocket(
        self,
        websocket: WebSocket,
        topic: str,
        throttle_ms: int | None = None,
        expected_connection_generation: int | None = None,
        authority_is_current: Callable[[], bool] | None = None,
    ) -> bool:
        """Subscribe a WebSocket to a topic with optional throttle.

        Args:
            websocket: The WebSocket connection.
            topic: The topic to subscribe to.
            throttle_ms: Optional throttle override in milliseconds. Omitted
                values use the matching topic registry entry.
            expected_connection_generation: Connection lease captured before
                asynchronous authorization.
            authority_is_current: Optional identity guard for the authenticated
                principal that initiated the subscription.

        Returns:
            True if subscription successful, False otherwise.
        """
        if not self._registration_authority_is_current(
            websocket,
            expected_connection_generation,
            authority_is_current,
        ):
            return False
        if not await self._can_subscribe_websocket_topic(websocket, topic):
            return False
        if not self._registration_authority_is_current(
            websocket,
            expected_connection_generation,
            authority_is_current,
        ):
            return False
        if self._websocket_has_topic_subscription(websocket, topic):
            logger.debug(f"WebSocket already subscribed to topic: {topic}")
            return True
        effective_throttle_ms = self._default_topic_throttle_ms(topic)
        if throttle_ms is not None:
            effective_throttle_ms = throttle_ms
        self._register_topic_subscription(
            websocket,
            topic,
            effective_throttle_ms,
            throttle_per_topic=self._topic_throttle_per_topic(topic),
        )
        await self.start_zmq_subscriber(topic)
        if not self._registration_authority_is_current(
            websocket,
            expected_connection_generation,
            authority_is_current,
        ):
            retirement = self.retire_client(websocket)
            await self.finalize_retired_client(retirement)
            return False
        logger.info(f"WebSocket subscribed to topic: {topic} (throttle: {effective_throttle_ms}ms)")
        return True

    async def unsubscribe_websocket(self, websocket: WebSocket, topic: str) -> bool:
        """Unsubscribe a WebSocket from a topic.

        Args:
            websocket: The WebSocket connection.
            topic: The topic to unsubscribe from.

        Returns:
            True if unsubscription successful, False if not subscribed.
        """
        if topic not in self.topic_subscriptions:
            return False
        removed = self.topic_subscriptions[topic].pop(websocket, None)
        if removed is None:
            return False
        await self._cancel_account_trailing_for_subscription(websocket, topic)
        logger.info(f"WebSocket unsubscribed from topic: {topic}")
        live_subscriptions = self.topic_subscriptions.get(topic, {})
        registered = live_subscriptions.get(websocket)
        if topic in self.topic_metrics:
            self.topic_metrics[topic].active_subscribers = len(live_subscriptions)
        if registered is not None and registered is not removed:
            return True
        if websocket in self.client_subscriptions:
            self.client_subscriptions[websocket].discard(topic)
            if not self.client_subscriptions[websocket]:
                del self.client_subscriptions[websocket]
        if not self.topic_subscriptions[topic]:
            await self.stop_zmq_subscriber(topic)
            del self.topic_subscriptions[topic]
            logger.info(f"Stopped ZMQ subscriber for topic {topic} (no more clients)")
        return True

    async def unsubscribe_websocket_all(self, websocket: WebSocket) -> int:
        """Unsubscribe a WebSocket from all topics.

        Args:
            websocket: The WebSocket connection.

        Returns:
            Number of topics unsubscribed from.
        """
        topics_to_unsubscribe: list[str] = [
            topic
            for topic, subscriptions in self.topic_subscriptions.items()
            if websocket in subscriptions
        ]
        for topic in topics_to_unsubscribe:
            await self.unsubscribe_websocket(websocket, topic)
        if topics_to_unsubscribe:
            count = len(topics_to_unsubscribe)
            logger.info(f"WebSocket unsubscribed from {count} topics: {topics_to_unsubscribe}")
        return len(topics_to_unsubscribe)

    def get_subscription_stats(self) -> SubscriptionStatsSnapshot:
        """Get detailed subscription statistics.

        Returns:
            Snapshot with topic counts, subscriber counts, and per-topic details.
        """
        topics: dict[str, SubscriptionTopicDetail] = {}
        for topic, subscriptions in self.topic_subscriptions.items():
            config = self.available_topics[topic]
            topics[topic] = SubscriptionTopicDetail(
                subscribers=len(subscriptions),
                endpoint=config.endpoint,
                pattern=config.pattern,
                throttle_ms=config.throttle_ms,
            )
        return SubscriptionStatsSnapshot(
            total_topics=len(self.available_topics),
            active_topics=len(self.topic_subscriptions),
            total_subscribers=sum(len(subs) for subs in self.topic_subscriptions.values()),
            topics=topics,
        )

    def get_available_topics(self) -> list[str]:
        """Get list of all available topic names.

        Returns:
            List of topic names from registry.
        """
        return list(self.available_topics)

    def get_connection_stats(self) -> ConnectionStats:
        """Get connection-related statistics.

        Returns:
            ConnectionStats with counts of subscribers, tasks, topics, and clients.
        """
        return ConnectionStats(
            zmq_subscribers=len(self.zmq_subscribers),
            subscriber_tasks=len(self.subscriber_tasks),
            active_topics=len(self.topic_subscriptions),
            active_clients=len(self.client_subscriptions),
        )

    def get_topic_stats(self) -> dict[str, TopicMetricSnapshot]:
        """Get per-topic metrics statistics.

        Returns:
            Dictionary mapping topic names to their metric snapshots.
        """
        stats: dict[str, TopicMetricSnapshot] = {}
        for topic, metrics in self.topic_metrics.items():
            config = self.available_topics.get(topic)
            stats[topic] = TopicMetricSnapshot(
                active_subscribers=metrics.active_subscribers,
                received=metrics.received_count,
                forwarded=metrics.forwarded_count,
                throttled=metrics.throttled_count,
                dropped=metrics.dropped_count,
                timeout=metrics.timeout_count,
                errors=metrics.error_count,
                invalid_messages=metrics.invalid_messages,
                last_message_ts=metrics.last_message_ts,
                throttle_ms=config.throttle_ms if config else None,
                pattern=config.pattern if config else None,
            )
        return stats

    async def cleanup(self) -> None:
        """Clean up all ZMQ resources and subscriptions."""
        subscriber_tasks = tuple(self.subscriber_tasks.values())
        for task in subscriber_tasks:
            task.cancel()
        self.subscriber_tasks.clear()
        await asyncio.gather(*subscriber_tasks, return_exceptions=True)
        await self._cancel_all_account_trailing()
        for socket in self.zmq_subscribers.values():
            socket.setsockopt(zmq.LINGER, 0)
            socket.close()
        self.zmq_subscribers.clear()
        if self.context is not None:
            self.context.term()
            self.context = None
        logger.info("ZMQ WebSocket bridge cleaned up")
        await asyncio.sleep(0)

    async def start(self) -> None:
        """Start the ZMQ-WebSocket bridge service.

        Creates ZMQ context and waits for shutdown signal.
        """
        set_log_context("zmq:bridge")
        if self.context is None:
            self.context = zmq.asyncio.Context()
            logger.info("ZMQ WebSocket bridge: Context created")
        logger.info("ZMQ WebSocket bridge started")
        if self._shutdown_event is None:
            self._shutdown_event = asyncio.Event()
        try:
            await self._shutdown_event.wait()
        except asyncio.CancelledError:
            logger.info("ZMQ WebSocket bridge start task cancelled")
            raise
        finally:
            self._shutdown_event = None
            logger.debug("ZMQ WebSocket bridge start coroutine exiting")

    async def stop(self) -> None:
        """Stop the ZMQ-WebSocket bridge service."""
        if self._shutdown_event and not self._shutdown_event.is_set():
            self._shutdown_event.set()
        await self.cleanup()
        logger.info("ZMQ WebSocket bridge stopped")

    async def add_subscription(
        self,
        websocket: WebSocket,
        topics: list[str],
        expected_connection_generation: int | None = None,
        authority_is_current: Callable[[], bool] | None = None,
    ) -> bool:
        """Add subscriptions for a WebSocket to multiple topics.

        Args:
            websocket: The WebSocket connection.
            topics: List of topics to subscribe to.
            expected_connection_generation: Connection lease captured before
                asynchronous authorization.
            authority_is_current: Optional identity guard for the authenticated
                principal that initiated the subscription.

        Returns:
            Whether the captured connection authority remained current.
        """
        for topic in topics:
            if not self._registration_authority_is_current(
                websocket,
                expected_connection_generation,
                authority_is_current,
            ):
                return False
            await self.subscribe_websocket(
                websocket,
                topic,
                expected_connection_generation=expected_connection_generation,
                authority_is_current=authority_is_current,
            )
        return self._registration_authority_is_current(
            websocket,
            expected_connection_generation,
            authority_is_current,
        )

    async def remove_subscription(self, websocket: WebSocket, topics: list[str]) -> None:
        """Remove subscriptions for a WebSocket from multiple topics.

        Args:
            websocket: The WebSocket connection.
            topics: List of topics to unsubscribe from.
        """
        for topic in topics:
            await self.unsubscribe_websocket(websocket, topic)

    async def remove_client(self, websocket: WebSocket) -> None:
        """Remove a client and all its subscriptions.

        Args:
            websocket: The WebSocket connection to remove.
        """
        retirement = self.retire_client(websocket)
        await self.finalize_retired_client(retirement)
