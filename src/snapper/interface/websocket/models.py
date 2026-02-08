"""WebSocket models for subscription and topic management.

This module defines dataclass models used internally by the WebSocket
infrastructure for managing subscriptions, topic configurations,
and metrics collection.
"""

from dataclasses import dataclass

from fastapi import WebSocket

from snapper.core.types import SubscriptionAction

__all__ = [
    "UITopicModel",
    "SubscriptionRequestModel",
    "TopicSubscriptionModel",
    "TopicConfigurationModel",
    "TopicMetricsModel",
]


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
        last_sent: Timestamp of last sent message.
        client_id: Client identifier for logging.
        pending_count: Number of messages pending acknowledgment.
    """

    websocket: WebSocket
    throttle_ms: int = 100
    last_sent: float = 0.0
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
    """

    endpoint: str
    pattern: str
    throttle_ms: int = 100


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
        last_message_ts: Timestamp of last received message.
        active_subscribers: Current number of subscribers.
    """

    received_count: int = 0
    throttled_count: int = 0
    forwarded_count: int = 0
    error_count: int = 0
    dropped_count: int = 0
    timeout_count: int = 0
    last_message_ts: float = 0.0
    active_subscribers: int = 0
