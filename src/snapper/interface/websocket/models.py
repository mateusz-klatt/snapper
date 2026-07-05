"""WebSocket models for subscription and topic management.

This module defines dataclass models used internally by the WebSocket
infrastructure for managing subscriptions, topic configurations,
and metrics collection.
"""

from dataclasses import dataclass
from dataclasses import field

from fastapi import WebSocket

from snapper.core.types import SubscriptionAction

__all__ = [
    "ConnectionStats",
    "SERVER_CONTROL_SEQ",
    "SubscriptionRequestModel",
    "SubscriptionStatsSnapshot",
    "SubscriptionTopicDetail",
    "TopicConfigurationModel",
    "TopicMetricSnapshot",
    "TopicMetricsModel",
    "TopicSubscriptionModel",
    "UITopicModel",
    "WsStatsSnapshot",
]


SERVER_CONTROL_SEQ: str = "server.control"


@dataclass
class UITopicModel:
    """UI topic mapping model.

    Maps UI topic names to ZMQ topics with throttling configuration.

    Attributes:
        zmq_topic: The underlying ZMQ topic name.
        category: Topic category (market, trade, system, etc.).
        throttle_ms: Minimum time between messages in milliseconds.
    """

    zmq_topic: str
    category: str
    throttle_ms: int = 100


@dataclass
class SubscriptionRequestModel:
    """Subscription request model.

    Represents a client subscription/unsubscription request.

    Attributes:
        action: The action to perform (subscribe/unsubscribe).
        topics: List of topic names to operate on.
        client_id: Optional client identifier.
    """

    action: SubscriptionAction
    topics: list[str]
    client_id: str | None = None


@dataclass
class TopicSubscriptionModel:
    """Active topic subscription model.

    Tracks a client's subscription to a specific topic with throttling state.

    Attributes:
        websocket: The WebSocket connection.
        throttle_ms: Minimum time between messages in milliseconds.
        last_sent: Timestamp of last sent message (per-subscription throttle).
        throttle_per_topic: When True, throttle each received topic
            independently via ``last_sent_by_topic`` instead of the single
            ``last_sent``; set for heartbeat root subscriptions so one
            component's frames never throttle another's.
        last_sent_by_topic: Per-received-topic last-sent timestamps, used only
            when ``throttle_per_topic`` is True. Bounded by the number of
            distinct topics matched under the subscription.
        client_id: Client identifier for logging.
        pending_count: Number of messages pending acknowledgment.
    """

    websocket: WebSocket
    throttle_ms: int = 100
    last_sent: float = 0.0
    throttle_per_topic: bool = False
    last_sent_by_topic: dict[str, float] = field(default_factory=dict)
    client_id: str = ""
    pending_count: int = 0


@dataclass
class TopicConfigurationModel:
    """Topic configuration model.

    Defines how to connect to and handle a specific topic.

    Attributes:
        endpoint: ZMQ endpoint address.
        pattern: Topic pattern for ZMQ subscription.
        throttle_ms: Default throttle interval in milliseconds.
        throttle_per_topic: Whether the throttle is applied per received
            topic rather than per subscription (heartbeat families).
    """

    endpoint: str
    pattern: str
    throttle_ms: int = 100
    throttle_per_topic: bool = False


@dataclass
class TopicMetricsModel:
    """Topic metrics model.

    Collects statistics about message flow for a topic.

    Attributes:
        received_count: Total messages received from ZMQ.
        throttled_count: Messages dropped due to throttling.
        forwarded_count: Messages successfully forwarded to clients.
        error_count: Number of errors encountered.
        dropped_count: Messages dropped due to backpressure.
        timeout_count: Messages that timed out during send.
        invalid_messages: Messages that could not be parsed as a typed envelope.
        last_message_ts: Timestamp of last received message.
        active_subscribers: Current number of subscribers.
    """

    received_count: int = 0
    throttled_count: int = 0
    forwarded_count: int = 0
    error_count: int = 0
    dropped_count: int = 0
    timeout_count: int = 0
    invalid_messages: int = 0
    last_message_ts: float = 0.0
    active_subscribers: int = 0


@dataclass
class ConnectionStats:
    """Connection-level statistics from the ZMQ-WebSocket bridge.

    Attributes:
        active_connections: Number of active WebSocket connections.
        zmq_subscribers: Number of active ZMQ subscriber sockets.
        subscriber_tasks: Number of running subscriber asyncio tasks.
        active_topics: Number of topics with at least one subscriber.
        active_clients: Number of unique connected clients.
    """

    active_connections: int = 0
    zmq_subscribers: int = 0
    subscriber_tasks: int = 0
    active_topics: int = 0
    active_clients: int = 0


@dataclass
class TopicMetricSnapshot:
    """Point-in-time snapshot of metrics for a single topic.

    Attributes:
        active_subscribers: Current subscriber count for this topic.
        received: Total messages received from ZMQ.
        forwarded: Messages successfully forwarded to clients.
        throttled: Messages dropped due to throttling.
        dropped: Messages dropped due to backpressure.
        timeout: Messages that timed out during send.
        errors: Number of errors encountered.
        invalid_messages: Messages that could not be parsed as a typed envelope.
        last_message_ts: Timestamp of last received message.
        throttle_ms: Configured throttle interval (None if unconfigured).
        pattern: ZMQ subscription pattern (None if unconfigured).
    """

    active_subscribers: int = 0
    received: int = 0
    forwarded: int = 0
    throttled: int = 0
    dropped: int = 0
    timeout: int = 0
    errors: int = 0
    invalid_messages: int = 0
    last_message_ts: float = 0.0
    throttle_ms: int | None = None
    pattern: str | None = None


@dataclass
class WsStatsSnapshot:
    """Aggregated WebSocket and ZMQ bridge statistics.

    Attributes:
        connections: Connection-level statistics.
        topics: Per-topic metrics keyed by topic name.
    """

    connections: ConnectionStats
    topics: dict[str, TopicMetricSnapshot]


@dataclass
class SubscriptionTopicDetail:
    """Per-topic subscription detail.

    Attributes:
        subscribers: Number of active subscribers for this topic.
        endpoint: ZMQ endpoint address for the topic.
        pattern: ZMQ subscription pattern.
        throttle_ms: Configured throttle interval in milliseconds.
    """

    subscribers: int
    endpoint: str
    pattern: str
    throttle_ms: int


@dataclass
class SubscriptionStatsSnapshot:
    """Subscription statistics snapshot from the bridge.

    Attributes:
        total_topics: Total number of available topics.
        active_topics: Number of topics with active subscribers.
        total_subscribers: Total subscriber count across all topics.
        topics: Per-topic subscription details.
    """

    total_topics: int
    active_topics: int
    total_subscribers: int
    topics: dict[str, SubscriptionTopicDetail]
