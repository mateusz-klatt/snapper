"""Health check and monitoring schemas for the REST API.

This module defines response schemas for various health endpoints that provide
insight into system status, ZMQ bridge health, and WebSocket statistics.

Structural sub-schemas (nested fields) are plain BaseModel with extra="forbid"
and carry no provenance fields.  Top-level response schemas use the
PayloadResponse[T, Data] envelope so every REST reply has a consistent shape.
"""

from typing import Literal

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

from snapper.api.schemas.base import PayloadListResponse
from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictDataSchema
from snapper.core.types import ComponentStatus
from snapper.core.types import HealthStatus

_CONN_STATS_DESC = "Connection statistics"


class ConnectionStats(BaseModel):
    """Connection-level statistics from the ZMQ-WebSocket bridge.

    Attributes:
        active_connections: Number of active WebSocket connections.
        zmq_subscribers: Number of active ZMQ subscriber sockets.
        subscriber_tasks: Number of running subscriber asyncio tasks.
        active_topics: Number of topics with at least one subscriber.
        active_clients: Number of unique connected clients.
    """

    model_config = ConfigDict(extra="forbid")

    active_connections: int = Field(default=0, description="Active WebSocket connections")
    zmq_subscribers: int = Field(default=0, description="Active ZMQ subscriber sockets")
    subscriber_tasks: int = Field(default=0, description="Running subscriber tasks")
    active_topics: int = Field(default=0, description="Topics with subscribers")
    active_clients: int = Field(default=0, description="Unique connected clients")


class TopicMetricSnapshot(BaseModel):
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

    model_config = ConfigDict(extra="forbid")

    active_subscribers: int = Field(default=0, description="Current subscriber count")
    received: int = Field(default=0, description="Total messages received")
    forwarded: int = Field(default=0, description="Messages forwarded to clients")
    throttled: int = Field(default=0, description="Messages dropped by throttling")
    dropped: int = Field(default=0, description="Messages dropped by backpressure")
    timeout: int = Field(default=0, description="Messages timed out during send")
    errors: int = Field(default=0, description="Errors encountered")
    invalid_messages: int = Field(default=0, description="Messages with unparseable envelope")
    last_message_ts: float = Field(default=0.0, description="Last message timestamp")
    throttle_ms: int | None = Field(default=None, description="Throttle interval ms")
    pattern: str | None = Field(default=None, description="ZMQ subscription pattern")


class HealthTopics(BaseModel):
    """Topic subscription statistics.

    Attributes:
        active: Number of currently active topics with subscribers.
    """

    model_config = ConfigDict(extra="forbid")

    active: int = Field(description="Number of currently active topics")


class GapStats(BaseModel):
    """Gap detection telemetry counters for a single detector.

    Attributes:
        gaps_detected: Total missing messages detected.
        session_resets: Producer session resets observed.
        duplicates: Duplicate or reordered messages observed.
        mid_stream_joins: Subscriptions that started mid-stream.
        rejected_unstamped: Messages rejected due to missing provenance.
    """

    model_config = ConfigDict(extra="forbid")

    gaps_detected: int = Field(default=0, description="Total missing messages detected")
    session_resets: int = Field(default=0, description="Producer session resets observed")
    duplicates: int = Field(default=0, description="Duplicate or reordered messages")
    mid_stream_joins: int = Field(default=0, description="Subscriptions started mid-stream")
    rejected_unstamped: int = Field(default=0, description="Messages without provenance")


class GapDetectionStats(BaseModel):
    """Aggregated gap detection statistics from all detectors.

    Attributes:
        bridge: Gap stats from the ZMQ-to-WebSocket bridge detector.
        rest_clients: Per-session gap stats from REST client detectors.
    """

    model_config = ConfigDict(extra="forbid")

    bridge: GapStats = Field(description="ZMQ bridge gap detection stats")
    rest_clients: dict[str, GapStats] = Field(
        default={},
        description="Per-session REST client gap stats",
    )


class HealthCheckData(StrictDataSchema[Literal["health_check"]]):
    """Domain data for the main health check endpoint.

    Provides overall service health status including version,
    connection statistics, topic availability, and gap detection stats.

    Attributes:
        type: Payload item type discriminator.
        status: Overall service health status (healthy/warning/error).
        version: Application version string.
        connections: Connection statistics.
        topics: Topic availability information.
        gap_detection: Gap detection statistics from all detectors.
    """

    type: Literal["health_check"] = "health_check"
    status: HealthStatus = Field(description="Overall service health status")
    version: str = Field(description="Application version")
    connections: ConnectionStats = Field(description=_CONN_STATS_DESC)
    topics: HealthTopics = Field(description="Topics availability")
    gap_detection: GapDetectionStats = Field(description="Gap detection statistics")


class HealthCheckResponse(PayloadResponse[Literal["health_check_response"], HealthCheckData]):
    """Main health check endpoint response.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["health_check_response"] = "health_check_response"


class ZmqComponents(BaseModel):
    """ZMQ infrastructure component status.

    Attributes:
        zmq_context: ZMQ context status (active/inactive).
        websocket_manager: WebSocket manager status.
        active_connections: Number of active WebSocket connections.
    """

    model_config = ConfigDict(extra="forbid")

    zmq_context: ComponentStatus = Field(description="ZMQ context status")
    websocket_manager: ComponentStatus = Field(description="WebSocket manager status")
    active_connections: int = Field(description="Number of active WebSocket connections")


class ZmqConfig(BaseModel):
    """ZMQ configuration information.

    Attributes:
        available_topics: List of available ZMQ topics for subscription.
    """

    model_config = ConfigDict(extra="forbid")

    available_topics: list[str] = Field(description="List of available ZMQ topics")


class ZmqHealthData(StrictDataSchema[Literal["zmq_health"]]):
    """Domain data for the ZMQ bridge health check.

    Provides detailed status of the ZMQ-to-WebSocket bridge including
    component health, configuration, and message statistics.

    Attributes:
        type: Payload item type discriminator.
        status: Overall ZMQ bridge health status.
        components: Component status details.
        config: ZMQ configuration.
        connections: Connection statistics.
        message_stats: Message statistics per topic.
        errors: Error messages if not healthy.
    """

    type: Literal["zmq_health"] = "zmq_health"
    status: HealthStatus = Field(description="Overall ZMQ bridge health status")
    components: ZmqComponents = Field(description="Component status details")
    config: ZmqConfig = Field(description="ZMQ configuration")
    connections: ConnectionStats = Field(description=_CONN_STATS_DESC)
    message_stats: dict[str, TopicMetricSnapshot] = Field(
        description="Message statistics per topic"
    )
    errors: list[str] = Field(default=[], description="Error messages if not healthy")


class ZmqHealthResponse(PayloadResponse[Literal["zmq_health_response"], ZmqHealthData]):
    """ZMQ bridge health check endpoint response.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["zmq_health_response"] = "zmq_health_response"


class WebSocketStats(BaseModel):
    """WebSocket connection statistics.

    Attributes:
        active_connections: Number of active WebSocket connections.
        topic_subscribers: Subscriber count per topic.
        client_count: Total client count.
    """

    model_config = ConfigDict(extra="forbid")

    active_connections: int = Field(description="Number of active WebSocket connections")
    topic_subscribers: dict[str, int] = Field(description="Subscriber count per topic")
    client_count: int = Field(description="Total client count")


class ZmqBridgeStats(BaseModel):
    """ZMQ bridge statistics.

    Attributes:
        active_topics: Number of active ZMQ topics.
        subscriber_tasks: Number of subscriber tasks.
        available_topics: List of available topics.
    """

    model_config = ConfigDict(extra="forbid")

    active_topics: int = Field(description="Number of active ZMQ topics")
    subscriber_tasks: int = Field(description="Number of subscriber tasks")
    available_topics: list[str] = Field(description="List of available topics")


class WsStatsConfig(BaseModel):
    """WebSocket statistics configuration.

    Attributes:
        broker_xpub: ZMQ broker XPUB endpoint address.
        heartbeat_interval_ms: Heartbeat interval in milliseconds.
    """

    model_config = ConfigDict(extra="forbid")

    broker_xpub: str = Field(description="ZMQ broker XPUB endpoint")
    heartbeat_interval_ms: int = Field(description="Heartbeat interval in milliseconds")


class SubscriptionsStats(BaseModel):
    """Subscription statistics.

    Attributes:
        per_topic: Subscriber count per topic.
        per_client: Topics subscribed per client.
    """

    model_config = ConfigDict(extra="forbid")

    per_topic: dict[str, int] = Field(description="Subscriber count per topic")
    per_client: dict[str, list[str]] = Field(description="Topics subscribed per client")


class WsStatsData(StrictDataSchema[Literal["ws_stats"]]):
    """Domain data for the WebSocket statistics endpoint.

    Comprehensive statistics about WebSocket connections, ZMQ bridge,
    and subscription state.

    Attributes:
        type: Payload item type discriminator.
        websocket: WebSocket statistics.
        zmq_bridge: ZMQ bridge statistics.
        connections: Connection statistics.
        topics: Topic message statistics.
        subscriptions: Subscription details.
        config: Configuration details.
    """

    type: Literal["ws_stats"] = "ws_stats"
    websocket: WebSocketStats = Field(description="WebSocket statistics")
    zmq_bridge: ZmqBridgeStats = Field(description="ZMQ bridge statistics")
    connections: ConnectionStats = Field(description=_CONN_STATS_DESC)
    topics: dict[str, TopicMetricSnapshot] = Field(description="Topic message statistics")
    subscriptions: SubscriptionsStats = Field(description="Subscription details")
    config: WsStatsConfig = Field(description="Configuration details")


class WsStatsResponse(PayloadResponse[Literal["ws_stats_response"], WsStatsData]):
    """WebSocket statistics endpoint response.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["ws_stats_response"] = "ws_stats_response"


class SettingCategoriesResponse(PayloadListResponse[Literal["setting_categories"], str]):
    """Setting categories list response.

    Attributes:
        payload: List of unique setting categories.
        count: Number of categories.
    """

    type: Literal["setting_categories"] = "setting_categories"


__all__ = [
    "ConnectionStats",
    "GapDetectionStats",
    "GapStats",
    "HealthCheckData",
    "HealthCheckResponse",
    "HealthTopics",
    "SettingCategoriesResponse",
    "SubscriptionsStats",
    "TopicMetricSnapshot",
    "WebSocketStats",
    "WsStatsConfig",
    "WsStatsData",
    "WsStatsResponse",
    "ZmqBridgeStats",
    "ZmqComponents",
    "ZmqConfig",
    "ZmqHealthData",
    "ZmqHealthResponse",
]
