"""ZMQ-to-WebSocket bridge service.

This module bridges ZMQ pub/sub topics to WebSocket clients, handling
subscription management, message forwarding, throttling, and backpressure.
Control-plane events (invalid topic errors, disconnect) are recorded to
the ``control`` table for audit purposes.
"""

import asyncio
import contextlib
import logging
import time
from datetime import UTC
from datetime import datetime
from typing import Any
from uuid import uuid7

import zmq
import zmq.asyncio
from fastapi import WebSocket

from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
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
from snapper.messaging.infrastructure.gap_detector import GapDetector
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import HWM_MARKET_DATA
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.messages import GapEnvelope
from snapper.messaging.topics.builders import is_order_topic
from snapper.messaging.topics.schemas import REGISTRY_ROOTS
from snapper.messaging.topics.schemas import TOPIC_REGISTRY
from snapper.utils.logging import set_log_context

logger = logging.getLogger(__name__)

MAX_PENDING_MESSAGES_MARKET = 100

MAX_PENDING_MESSAGES_TRADE = 1000

SEND_TIMEOUT_SECONDS = 1.0


class ZmqWebSocketBridgeService:
    """Service that bridges ZMQ topics to WebSocket clients.

    Manages ZMQ subscriptions, forwards messages to WebSocket clients,
    handles throttling per-topic, and implements backpressure control.

    Attributes:
        connection_manager: WebSocket connection manager reference.
        topic_subscriptions: Mapping of topic to list of subscriptions.
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
        self.topic_subscriptions: dict[str, list[TopicSubscriptionModel]] = {}
        self.client_subscriptions: dict[WebSocket, set[str]] = {}
        self.zmq_subscribers: dict[str, zmq.asyncio.Socket] = {}
        self.subscriber_tasks: dict[str, asyncio.Task[None]] = {}
        self.context: zmq.asyncio.Context | None = None
        self.settings = get_settings()
        self.topic_metrics: dict[str, TopicMetricsModel] = {}
        self._gap_detector: GapDetector = GapDetector("bridge")
        self.available_topics: dict[str, TopicConfigurationModel] = self._build_topic_config()
        self._shutdown_event: asyncio.Event | None = None

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

    async def subscribe_client(self, websocket: WebSocket, topics: list[str]) -> None:
        """Subscribe a WebSocket client to multiple topics.

        Args:
            websocket: The WebSocket connection.
            topics: List of topic names to subscribe to.
        """
        client_id = f"{id(websocket)}"
        logger.info(f"Client {client_id} subscribing to topics: {topics}")
        if websocket not in self.client_subscriptions:
            self.client_subscriptions[websocket] = set()
        for topic in topics:
            if topic.endswith(".") and topic not in REGISTRY_ROOTS:
                logger.warning(
                    "Rejected intermediate prefix subscription: %s (not a registry root)", topic
                )
                continue
            self.client_subscriptions[websocket].add(topic)
            if topic not in self.topic_subscriptions:
                self.topic_subscriptions[topic] = []
                self.topic_metrics[topic] = TopicMetricsModel()
            subscription = TopicSubscriptionModel(
                websocket=websocket,
                throttle_ms=self.available_topics.get(
                    topic, TopicConfigurationModel("", "", 100)
                ).throttle_ms,
                client_id=client_id,
            )
            self.topic_subscriptions[topic].append(subscription)
            self.topic_metrics[topic].active_subscribers = len(self.topic_subscriptions[topic])
            if len(self.topic_subscriptions[topic]) == 1:
                await self._start_zmq_subscription(topic)
        if websocket in self.client_subscriptions and not self.client_subscriptions[websocket]:
            del self.client_subscriptions[websocket]
        logger.info(
            f"Client {client_id} subscribed. Active subscriptions: "
            f"{len(self.client_subscriptions.get(websocket, set()))}"
        )

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
                self.topic_subscriptions[topic] = [
                    sub for sub in self.topic_subscriptions[topic] if sub.websocket != websocket
                ]
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
            assert self.context is not None, "ZMQ context must be initialized in start()"
            socket = self.context.socket(zmq.SUB)
            apply_hwm(socket, rcvhwm=HWM_MARKET_DATA)
            socket.connect(topic_config.endpoint)
            socket.setsockopt(zmq.SUBSCRIBE, topic_config.pattern.encode("utf-8"))
            self.zmq_subscribers[topic] = socket
            task = asyncio.create_task(self._zmq_subscription_loop(topic, socket))
            self.subscriber_tasks[topic] = task
            logger.info(f"ZMQ subscription started for {topic} on {topic_config.endpoint}")
        except Exception as e:
            logger.error(f"Failed to start ZMQ subscription for {topic}: {e}")
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
                received_topic, envelope.session_id, envelope.sequence_id
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
                logger.error(f"ZMQ error in subscription loop for {topic}: {e}")
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Unexpected error in subscription loop for {topic}: {e}")
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
            logger.error(f"Fatal error in subscription loop for {topic}: {e}")

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
        self, subscription: TopicSubscriptionModel, current_time: float, topic: str
    ) -> bool:
        """Check if a subscription should be throttled.

        Args:
            subscription: The subscription to check.
            current_time: Current timestamp.
            topic: Topic name for metrics tracking.

        Returns:
            True if the message should be throttled.
        """
        if current_time - subscription.last_sent < (subscription.throttle_ms / 1000.0):
            if topic in self.topic_metrics:
                self.topic_metrics[topic].throttled_count += 1
            return True
        return False

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
    ) -> None:
        """Attempt to send a message to a single subscriber.

        Args:
            subscription: Target subscription.
            topic: Topic name for metrics.
            message_str: Message payload to send.
            current_time: Current timestamp for last_sent update.
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
        current_time = time.time()
        max_pending = self._get_max_pending(topic)
        is_trade = self._is_trade_topic(topic)
        for subscription in self.topic_subscriptions[topic][:]:
            try:
                if self._is_throttled(subscription, current_time, topic):
                    continue
                if await self._handle_backpressure(subscription, topic, max_pending, is_trade):
                    continue
                await self._try_send_message(subscription, topic, message_str, current_time)
            except Exception as e:
                logger.warning(f"Failed to send message to client {subscription.client_id}: {e}")
                with contextlib.suppress(Exception):
                    await self.disconnect_client(subscription.websocket)

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
            assert self.context is not None, "ZMQ context must be initialized in start()"
            socket = self.context.socket(zmq.SUB)
            apply_hwm(socket, rcvhwm=HWM_MARKET_DATA)
            socket.connect(config.endpoint)
            socket.setsockopt(zmq.SUBSCRIBE, topic.encode())
            self.zmq_subscribers[topic] = socket
            task = asyncio.create_task(self._handle_zmq_messages(topic, socket, config))
            self.subscriber_tasks[topic] = task
            logger.info(f"Started ZMQ subscriber for topic: {topic}")
        except Exception as e:
            logger.error(f"Failed to start ZMQ subscriber for {topic}: {e}")
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
            _config: Topic configuration (unused; reserved for future filtering).
        """
        try:
            while True:
                parts = await socket.recv_multipart()
                try:
                    await self._process_zmq_message(topic, parts)
                except Exception as e:
                    logger.error(f"Error processing ZMQ message for {topic}: {e}")
        except asyncio.CancelledError:
            logger.info(f"ZMQ message handler for {topic} cancelled")
            raise
        except Exception as e:
            logger.error(f"ZMQ message handler for {topic} failed: {e}")

    async def subscribe_websocket(
        self, websocket: WebSocket, topic: str, throttle_ms: int = 100
    ) -> bool:
        """Subscribe a WebSocket to a topic with optional throttle.

        Args:
            websocket: The WebSocket connection.
            topic: The topic to subscribe to.
            throttle_ms: Throttle interval in milliseconds.

        Returns:
            True if subscription successful, False otherwise.
        """
        if topic.endswith(".") and topic not in REGISTRY_ROOTS:
            logger.warning(
                "Rejected intermediate prefix subscription: %s (not a registry root)", topic
            )
            await self._record_bridge_control(
                "zmq_subscribe",
                "error",
                detail=f"Intermediate prefix rejected: {topic}",
            )
            return False
        topic_config = self._find_matching_pattern(topic)
        if not topic_config:
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
                logger.error(f"Failed to send error to client: {e}")
            await self._record_bridge_control(
                "zmq_subscribe",
                "error",
                detail=f"Invalid topic: {topic}",
            )
            return False
        if topic in self.topic_subscriptions:
            for sub in self.topic_subscriptions[topic]:
                if sub.websocket == websocket:
                    logger.debug(f"WebSocket already subscribed to topic: {topic}")
                    return True
        subscription = TopicSubscriptionModel(websocket=websocket, throttle_ms=throttle_ms)
        if topic not in self.topic_subscriptions:
            self.topic_subscriptions[topic] = []
        self.topic_subscriptions[topic].append(subscription)
        if websocket not in self.client_subscriptions:
            self.client_subscriptions[websocket] = set()
        self.client_subscriptions[websocket].add(topic)
        if topic not in self.topic_metrics:
            self.topic_metrics[topic] = TopicMetricsModel()
        self.topic_metrics[topic].active_subscribers += 1
        await self.start_zmq_subscriber(topic)
        logger.info(f"WebSocket subscribed to topic: {topic} (throttle: {throttle_ms}ms)")
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
        to_remove = None
        for sub in self.topic_subscriptions[topic]:
            if sub.websocket == websocket:
                to_remove = sub
                break
        if to_remove:
            self.topic_subscriptions[topic].remove(to_remove)
            logger.info(f"WebSocket unsubscribed from topic: {topic}")
            if topic in self.topic_metrics:
                self.topic_metrics[topic].active_subscribers = max(
                    0, self.topic_metrics[topic].active_subscribers - 1
                )
            if websocket in self.client_subscriptions:
                self.client_subscriptions[websocket].discard(topic)
                if not self.client_subscriptions[websocket]:
                    del self.client_subscriptions[websocket]
            if not self.topic_subscriptions[topic]:
                await self.stop_zmq_subscriber(topic)
                del self.topic_subscriptions[topic]
                logger.info(f"Stopped ZMQ subscriber for topic {topic} (no more clients)")
            return True
        return False

    async def unsubscribe_websocket_all(self, websocket: WebSocket) -> int:
        """Unsubscribe a WebSocket from all topics.

        Args:
            websocket: The WebSocket connection.

        Returns:
            Number of topics unsubscribed from.
        """
        topics_to_unsubscribe: list[str] = []
        for topic, subscriptions in self.topic_subscriptions.items():
            for sub in subscriptions:
                if sub.websocket == websocket:
                    topics_to_unsubscribe.append(topic)
                    break
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
        for task in self.subscriber_tasks.values():
            task.cancel()
        self.subscriber_tasks.clear()
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

    async def add_subscription(self, websocket: WebSocket, topics: list[str]) -> None:
        """Add subscriptions for a WebSocket to multiple topics.

        Args:
            websocket: The WebSocket connection.
            topics: List of topics to subscribe to.
        """
        for topic in topics:
            await self.subscribe_websocket(websocket, topic)

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
        await self.unsubscribe_websocket_all(websocket)
