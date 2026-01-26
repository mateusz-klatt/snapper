"""Topic schema definitions for the messaging system.

Provides schema validation and configuration for ZeroMQ pub/sub topics.
"""

from dataclasses import dataclass
from typing import Any

__all__ = [
    "TopicSchema",
    "TOPIC_REGISTRY",
    "get_topic_config",
    "topic_exists",
    "get_topics_by_category",
    "get_all_topic_names",
    "validate_message_schema",
]


@dataclass
class TopicSchema:
    """Schema definition for a messaging topic.

    Defines the structure, validation rules, and metadata for a topic
    in the pub/sub messaging system.
    """

    name: str
    pattern: str
    description: str
    category: str
    throttle_ms: int = 100
    required_fields: list[str] | None = None
    sample_data: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        """Normalize the instance after initialization."""
        if self.required_fields is None:
            self.required_fields = []
        if self.sample_data is None:
            self.sample_data = {}


TOPIC_REGISTRY: dict[str, TopicSchema] = {
    "market": TopicSchema(
        name="market",
        pattern="market.",
        description="All market data (candles, ticks, trades) from all exchanges and instruments",
        category="market",
        throttle_ms=100,
        required_fields=["instrument", "exchange", "type", "timestamp"],
        sample_data={
            "instrument": "BTC-USD",
            "exchange": "kraken",
            "type": "candles",
            "timeframe": "1m",
            "timestamp": 1640995200000,
            "open": 47000.0,
            "high": 47100.0,
            "low": 46900.0,
            "close": 47050.0,
            "volume": 1.234,
        },
    ),
    "signals": TopicSchema(
        name="signals",
        pattern="signals.",
        description="Trading signals from strategy algorithms for specific exchange and instrument",
        category="strategy",
        throttle_ms=500,
        required_fields=["instrument", "exchange", "signal_type", "strength", "timestamp"],
        sample_data={
            "instrument": "BTC-USD",
            "exchange": "kraken",
            "signal_type": "buy",
            "strength": 0.85,
            "timestamp": 1640995200000,
        },
    ),
    "strategy.signals": TopicSchema(
        name="strategy.signals",
        pattern="strategy.",
        description="Trading signals from strategy algorithms (paper trading)",
        category="strategy",
        throttle_ms=500,
        required_fields=["id", "strategy", "instrument", "signal_type", "strength", "timestamp"],
        sample_data={
            "id": "signal_789",
            "strategy": "momentum",
            "instrument": "BTC-USD",
            "signal_type": "buy",
            "strength": 0.85,
            "timestamp": 1640995200000,
        },
    ),
    "system.heartbeats.": TopicSchema(
        name="system.heartbeats.*",
        pattern="system.heartbeats.",
        description="System component health and status updates (per-process topics)",
        category="system",
        throttle_ms=1000,
        required_fields=["component", "status", "timestamp", "lag_ms"],
        sample_data={
            "component": "strategy_macd_btc_1h",
            "status": "ok",
            "timestamp": 1640995200000,
            "lag_ms": 15,
        },
    ),
    "admin": TopicSchema(
        name="admin",
        pattern="admin.",
        description="Administrative topics (users, settings, system config) - ADMIN role only",
        category="admin",
        throttle_ms=1000,
        required_fields=["resource", "action", "timestamp"],
        sample_data={
            "resource": "users",
            "action": "created",
            "user_id": "user_123",
            "timestamp": 1640995200000,
        },
    ),
    "orders.commands": TopicSchema(
        name="orders.commands",
        pattern="orders.commands.",
        description=(
            "Order commands from trader to executor. "
            "Suffix indicates command type: submit, cancel, replace. "
            "Format: orders.commands.{exchange}.{instrument}.{command}"
        ),
        category="trade",
        throttle_ms=0,
        required_fields=["type", "exchange", "instrument", "client_order_id"],
        sample_data={
            "type": "order_req",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "side": "buy",
            "order_type": "limit",
            "quantity": 0.5,
            "price": 47000.0,
            "client_order_id": "client_12345",
        },
    ),
    "orders.events": TopicSchema(
        name="orders.events",
        pattern="orders.events.",
        description=(
            "Order events from executor to trader/UI. "
            "Suffix indicates event type: submitted, accepted, rejected, fill, etc. "
            "Payload varies: FillEnvelope for 'fill', OrderStatusEnvelope for others. "
            "Format: orders.events.{exchange}.{instrument}.{event}"
        ),
        category="trade",
        throttle_ms=0,
        required_fields=["type", "exchange", "instrument", "client_order_id"],
        sample_data={
            "type": "fill",
            "trade_id": "trade_67890",
            "exchange_order_id": "KRAKEN-ABC123",
            "client_order_id": "client_12345",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "side": "buy",
            "size": 0.5,
            "price": 47000.0,
            "fee": 0.26,
            "fee_asset": "USD",
            "status": "filled",
        },
    ),
}


def get_topic_config(topic_name: str) -> TopicSchema | None:
    """Retrieve the schema configuration for a topic.

    Args:
        topic_name: Name of the topic to look up.

    Returns:
        TopicSchema if found, None otherwise.
    """
    return TOPIC_REGISTRY.get(topic_name)


def get_topics_by_category(category: str) -> dict[str, TopicSchema]:
    """Get all topics belonging to a specific category.

    Args:
        category: Category name to filter by.

    Returns:
        Dictionary mapping topic names to their schemas.
    """
    return {name: schema for name, schema in TOPIC_REGISTRY.items() if schema.category == category}


def topic_exists(topic_name: str) -> bool:
    """Check if a topic is registered in the system.

    Args:
        topic_name: Name of the topic to check.

    Returns:
        True if the topic exists, False otherwise.
    """
    return topic_name in TOPIC_REGISTRY


def get_all_topic_names() -> list[str]:
    """Get names of all registered topics.

    Returns:
        List of all topic names in the registry.
    """
    return list(TOPIC_REGISTRY.keys())


def validate_message_schema(topic_name: str, message: dict[str, Any]) -> tuple[bool, list[str]]:
    """Validate a message against its topic schema.

    Args:
        topic_name: Name of the topic the message belongs to.
        message: Message dictionary to validate.

    Returns:
        Tuple of (is_valid, missing_fields) where missing_fields is empty if valid.
    """
    schema = get_topic_config(topic_name)
    if not schema:
        return False, [f"Unknown topic: {topic_name}"]
    if not schema.required_fields:
        return True, []
    missing_fields = [field for field in schema.required_fields if field not in message]
    return len(missing_fields) == 0, missing_fields
