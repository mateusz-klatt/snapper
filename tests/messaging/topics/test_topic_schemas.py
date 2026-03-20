"""Tests for ZMQ topic schemas and registry utilities."""

from snapper.messaging.topics.schemas import TOPIC_REGISTRY
from snapper.messaging.topics.schemas import TopicSchema
from snapper.messaging.topics.schemas import get_topics_by_category


class TestTopicSchema:
    """Tests for TopicSchema data class."""

    def test_creation_with_defaults(self) -> None:
        """TopicSchema applies default throttle_ms when not specified.

        Given: Required schema parameters only,
        When: TopicSchema is created,
        Then: throttle_ms defaults to 100.
        """
        schema = TopicSchema(pattern="test.", category="test")
        assert schema.pattern == "test."
        assert schema.category == "test"
        assert schema.throttle_ms == 100

    def test_creation_with_custom_throttle(self) -> None:
        """TopicSchema stores custom throttle_ms.

        Given: Schema parameters with explicit throttle_ms,
        When: TopicSchema is created,
        Then: Custom throttle_ms is stored.
        """
        schema = TopicSchema(pattern="test.", category="test", throttle_ms=500)
        assert schema.throttle_ms == 500

    def test_frozen(self) -> None:
        """TopicSchema is immutable.

        Given: A TopicSchema instance,
        When: Attempting to modify a field,
        Then: FrozenInstanceError is raised.
        """
        schema = TopicSchema(pattern="test.", category="test")
        try:
            schema.pattern = "other."
            raise AssertionError("Expected FrozenInstanceError")
        except AttributeError:
            pass


class TestTopicRegistry:
    """Tests for TOPIC_REGISTRY contents."""

    def test_contains_core_patterns(self) -> None:
        """TOPIC_REGISTRY contains all core topic patterns.

        Given: TOPIC_REGISTRY tuple,
        When: Checking for core patterns,
        Then: market, signals, heartbeats, orders, admin patterns all exist.
        """
        patterns = {s.pattern for s in TOPIC_REGISTRY}
        expected = {
            "market.",
            "signals.",
            "system.heartbeats.",
            "orders.commands.",
            "orders.events.",
        }
        assert expected <= patterns

    def test_is_tuple(self) -> None:
        """TOPIC_REGISTRY is an immutable tuple.

        Given: TOPIC_REGISTRY,
        When: Checking its type,
        Then: It is a tuple of TopicSchema instances.
        """
        assert isinstance(TOPIC_REGISTRY, tuple)
        assert all(isinstance(s, TopicSchema) for s in TOPIC_REGISTRY)

    def test_trade_topics_have_zero_throttle(self) -> None:
        """Order command and event topics have zero throttle.

        Given: TOPIC_REGISTRY with trade topics,
        When: Checking throttle_ms for trade category,
        Then: All trade topics have throttle_ms=0.
        """
        trade = [s for s in TOPIC_REGISTRY if s.category == "trade"]
        assert len(trade) == 2
        assert all(s.throttle_ms == 0 for s in trade)


class TestTopicUtilities:
    """Tests for topic utility functions."""

    def test_get_topics_by_category_market(self) -> None:
        """get_topics_by_category filters correctly.

        Given: Topics registered in different categories,
        When: Filtering by 'market',
        Then: Returns only market schemas.
        """
        market = get_topics_by_category("market")
        assert len(market) == 1
        assert market[0].pattern == "market."

    def test_get_topics_by_category_strategy(self) -> None:
        """get_topics_by_category returns strategy topics.

        Given: Topics with category 'strategy',
        When: Filtering by 'strategy',
        Then: Returns signals schema only.
        """
        strategy = get_topics_by_category("strategy")
        assert len(strategy) == 1
        assert strategy[0].pattern == "signals."

    def test_get_topics_by_category_empty(self) -> None:
        """get_topics_by_category returns empty list for unknown category.

        Given: No topics in 'nonexistent' category,
        When: Filtering by 'nonexistent',
        Then: Returns empty list.
        """
        assert get_topics_by_category("nonexistent") == []
