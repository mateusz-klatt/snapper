"""Topic schema definitions for the messaging system.

Provides schema validation and configuration for ZeroMQ pub/sub topics.
Each entry defines a subscription prefix pattern, throttle rate, and
RBAC category for the bridge and connection manager.
"""

from dataclasses import dataclass

__all__ = [
    "TopicSchema",
    "TOPIC_REGISTRY",
    "REGISTRY_ROOTS",
    "get_topics_by_category",
]


@dataclass(frozen=True, slots=True)
class TopicSchema:
    """Schema definition for a messaging topic family.

    Attributes:
        pattern: ZMQ subscription prefix (e.g. ``"market."``).
        category: RBAC category for permission filtering.
        throttle_ms: Bridge-side throttle interval in milliseconds.
    """

    pattern: str
    category: str
    throttle_ms: int = 100


TOPIC_REGISTRY: tuple[TopicSchema, ...] = (
    TopicSchema(pattern="market.", category="market", throttle_ms=100),
    TopicSchema(pattern="signals.", category="signals", throttle_ms=500),
    TopicSchema(pattern="system.heartbeats.", category="system", throttle_ms=1000),
    TopicSchema(pattern="admin.", category="admin", throttle_ms=1000),
    TopicSchema(pattern="orders.commands.", category="trade", throttle_ms=0),
    TopicSchema(pattern="orders.events.", category="trade", throttle_ms=0),
    TopicSchema(pattern="accruals.", category="accruals", throttle_ms=1000),
    TopicSchema(pattern="backtest.", category="backtest", throttle_ms=250),
    TopicSchema(pattern="alerts.", category="notifications", throttle_ms=500),
)


REGISTRY_ROOTS: frozenset[str] = frozenset(schema.pattern for schema in TOPIC_REGISTRY)
"""Immutable set of all TOPIC_REGISTRY root patterns (e.g. ``"market."``, ``"signals."``).

WS clients may only subscribe to these prefixes — intermediate prefixes
like ``"market.kraken."`` are rejected.  Used by both the subscribe
handler and the bridge for defense-in-depth validation.
"""


def get_topics_by_category(category: str) -> list[TopicSchema]:
    """Get all topic schemas belonging to a specific category.

    Args:
        category: Category name to filter by.

    Returns:
        List of matching TopicSchema instances.
    """
    return [schema for schema in TOPIC_REGISTRY if schema.category == category]
