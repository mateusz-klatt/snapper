"""Health check and monitoring schemas for the REST API.

This module defines response schemas for various health endpoints that provide
insight into system status, ZMQ bridge health, and WebSocket statistics.
"""

from datetime import datetime
from typing import Any

from pydantic import Field

from snapper.api.schemas.base import StrictApiSchema
from snapper.core.types import ComponentStatus
from snapper.core.types import HealthStatus

_CONN_STATS_DESC = "Connection statistics"


class HealthTopics(StrictApiSchema):
    """Topic availability statistics.

    Attributes:
        available: Total number of available topics.
        active: Number of currently active topics with subscribers.
    """

    available: int = Field(description="Total number of available topics")
    active: int = Field(description="Number of currently active topics")


class HealthCheckResponse(StrictApiSchema):
    """Main health check endpoint response.

    Provides overall service health status including version,
    connection statistics, and topic availability.

    Attributes:
        status: Overall service health status (healthy/unhealthy).
        timestamp: Timestamp of the health check.
        version: Application version string.
        connections: Connection statistics dictionary.
        topics: Topic availability information.
    """

    status: HealthStatus = Field(description="Overall service health status")
    timestamp: datetime = Field(description="Timestamp of the health check")
    version: str = Field(description="Application version")
    connections: dict[str, Any] = Field(description=_CONN_STATS_DESC)
    topics: HealthTopics = Field(description="Topics availability")


class ZmqComponents(StrictApiSchema):
    """ZMQ infrastructure component status.

    Attributes:
        zmq_context: ZMQ context status (active/inactive).
        websocket_manager: WebSocket manager status.
        active_connections: Number of active WebSocket connections.
    """

    zmq_context: ComponentStatus = Field(description="ZMQ context status")
    websocket_manager: ComponentStatus = Field(description="WebSocket manager status")
    active_connections: int = Field(description="Number of active WebSocket connections")


class ZmqConfig(StrictApiSchema):
    """ZMQ configuration information.

    Attributes:
        available_topics: List of available ZMQ topics for subscription.
    """

    available_topics: list[str] = Field(description="List of available ZMQ topics")


class ZmqHealthResponse(StrictApiSchema):
    """ZMQ bridge health check response.

    Provides detailed status of the ZMQ-to-WebSocket bridge including
    component health, configuration, and message statistics.

    Attributes:
        status: Overall ZMQ bridge health status.
        timestamp: Timestamp of the health check.
        components: Component status details.
        config: ZMQ configuration.
        connections: Connection statistics.
        message_stats: Message statistics per topic.
        errors: Error messages if unhealthy.
    """

    status: HealthStatus = Field(description="Overall ZMQ bridge health status")
    timestamp: datetime = Field(description="Timestamp of the health check")
    components: ZmqComponents = Field(description="Component status details")
    config: ZmqConfig = Field(description="ZMQ configuration")
    connections: dict[str, Any] = Field(description=_CONN_STATS_DESC)
    message_stats: dict[str, Any] = Field(description="Message statistics per topic")
    errors: list[str] = Field(default_factory=list, description="Error messages if unhealthy")


class WebSocketStats(StrictApiSchema):
    """WebSocket connection statistics.

    Attributes:
        active_connections: Number of active WebSocket connections.
        topic_subscribers: Subscriber count per topic.
        client_count: Total client count.
    """

    active_connections: int = Field(description="Number of active WebSocket connections")
    topic_subscribers: dict[str, int] = Field(description="Subscriber count per topic")
    client_count: int = Field(description="Total client count")


class ZmqBridgeStats(StrictApiSchema):
    """ZMQ bridge statistics.

    Attributes:
        active_topics: Number of active ZMQ topics.
        subscriber_tasks: Number of subscriber tasks.
        available_topics: List of available topics.
    """

    active_topics: int = Field(description="Number of active ZMQ topics")
    subscriber_tasks: int = Field(description="Number of subscriber tasks")
    available_topics: list[str] = Field(description="List of available topics")


class WsStatsConfig(StrictApiSchema):
    """WebSocket statistics configuration.

    Attributes:
        broker_xpub: ZMQ broker XPUB endpoint address.
        heartbeat_interval_ms: Heartbeat interval in milliseconds.
    """

    broker_xpub: str = Field(description="ZMQ broker XPUB endpoint")
    heartbeat_interval_ms: int = Field(description="Heartbeat interval in milliseconds")


class SubscriptionsStats(StrictApiSchema):
    """Subscription statistics.

    Attributes:
        per_topic: Subscriber count per topic.
        per_client: Topics subscribed per client.
    """

    per_topic: dict[str, int] = Field(description="Subscriber count per topic")
    per_client: dict[str, list[str]] = Field(description="Topics subscribed per client")


class WsStatsResponse(StrictApiSchema):
    """WebSocket statistics endpoint response.

    Comprehensive statistics about WebSocket connections, ZMQ bridge,
    and subscription state.

    Attributes:
        websocket: WebSocket statistics.
        zmq_bridge: ZMQ bridge statistics.
        connections: Connection statistics.
        topics: Topic message statistics.
        subscriptions: Subscription details.
        config: Configuration details.
    """

    websocket: WebSocketStats = Field(description="WebSocket statistics")
    zmq_bridge: ZmqBridgeStats = Field(description="ZMQ bridge statistics")
    connections: dict[str, Any] = Field(description=_CONN_STATS_DESC)
    topics: dict[str, Any] = Field(description="Topic message statistics")
    subscriptions: SubscriptionsStats = Field(description="Subscription details")
    config: WsStatsConfig = Field(description="Configuration details")


class SettingCategoriesResponse(StrictApiSchema):
    """Setting categories list response.

    Attributes:
        categories: List of unique setting categories.
    """

    categories: list[str] = Field(description="List of unique setting categories")


__all__ = [
    "HealthCheckResponse",
    "HealthTopics",
    "WsStatsResponse",
    "WsStatsConfig",
    "WebSocketStats",
    "ZmqBridgeStats",
    "SubscriptionsStats",
    "ZmqHealthResponse",
    "ZmqComponents",
    "ZmqConfig",
    "SettingCategoriesResponse",
]
