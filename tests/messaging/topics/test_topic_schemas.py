"""Tests for ZMQ topic schemas and registry utilities."""

from typing import Any

from snapper.messaging.topics.schemas import TOPIC_REGISTRY
from snapper.messaging.topics.schemas import TopicSchema
from snapper.messaging.topics.schemas import get_all_topic_names
from snapper.messaging.topics.schemas import get_topic_config
from snapper.messaging.topics.schemas import get_topics_by_category
from snapper.messaging.topics.schemas import topic_exists
from snapper.messaging.topics.schemas import validate_message_schema


class TestZMQTopicSchema:
    """Tests for TopicSchema data class."""

    def test_zmq_topic_schema_creation(self) -> None:
        """Test TopicSchema instantiation with required fields only.

        Given: Required schema parameters (name, pattern, description, category),
        When: TopicSchema is created,
        Then: All fields are set and defaults applied (throttle_ms=100, empty lists/dicts).
        """
        schema = TopicSchema(
            name="test.topic", pattern="test.", description="Test topic", category="test"
        )
        assert schema.name == "test.topic"
        assert schema.pattern == "test."
        assert schema.description == "Test topic"
        assert schema.category == "test"
        assert schema.throttle_ms == 100
        assert schema.required_fields == []
        assert schema.sample_data == {}

    def test_zmq_topic_schema_with_optional_fields(self) -> None:
        """Test TopicSchema with all optional fields.

        Given: Schema parameters including optional throttle_ms, required_fields, sample_data,
        When: TopicSchema is created,
        Then: All optional values are stored correctly.
        """
        schema = TopicSchema(
            name="test.topic",
            pattern="test.",
            description="Test topic",
            category="test",
            throttle_ms=200,
            required_fields=["field1", "field2"],
            sample_data={"key": "value"},
        )
        assert schema.throttle_ms == 200
        assert schema.required_fields == ["field1", "field2"]
        assert schema.sample_data == {"key": "value"}


class TestTopicRegistry:
    """Tests for TOPIC_REGISTRY functionality."""

    def test_topic_registry_contains_expected_topics(self) -> None:
        """Test TOPIC_REGISTRY contains core topics.

        Given: TOPIC_REGISTRY with registered schemas,
        When: Checking for core topics (market, strategy, system),
        Then: All expected topics exist as TopicSchema instances.
        """
        expected_topics = [
            "market",
            "strategy.signals",
            "system.heartbeats.",
        ]
        for topic in expected_topics:
            assert topic in TOPIC_REGISTRY
            assert isinstance(TOPIC_REGISTRY[topic], TopicSchema)

    def test_market_schema(self) -> None:
        """Test market schema configuration.

        Given: TOPIC_REGISTRY with market topic,
        When: Retrieving market schema,
        Then: Schema has correct name, pattern, category, and required fields.
        """
        schema = TOPIC_REGISTRY["market"]
        assert schema.name == "market"
        assert schema.pattern == "market."
        assert schema.category == "market"
        assert schema.required_fields is not None
        assert "instrument" in schema.required_fields
        assert "exchange" in schema.required_fields
        assert schema.sample_data is not None


class TestTopicUtilities:
    """Tests for topic utility functions."""

    def test_get_topic_config(self) -> None:
        """Test get_topic_config retrieval and None handling.

        Given: Registered and non-existent topic names,
        When: Calling get_topic_config,
        Then: Returns schema for valid topic, None for unknown.
        """
        schema = get_topic_config("market")
        assert schema is not None
        assert schema.name == "market"
        schema = get_topic_config("nonexistent.topic")
        assert schema is None

    def test_get_topics_by_category(self) -> None:
        """Test filtering topics by category.

        Given: Topics registered in different categories,
        When: Calling get_topics_by_category,
        Then: Returns only topics matching the category.
        """
        market_topics = get_topics_by_category("market")
        assert "market" in market_topics
        assert "trade.orders" not in market_topics
        strategy_topics = get_topics_by_category("strategy")
        assert "strategy.signals" in strategy_topics
        assert "market" not in strategy_topics

    def test_topic_exists(self) -> None:
        """Test topic_exists boolean checks.

        Given: Existing and non-existing topic names,
        When: Calling topic_exists,
        Then: Returns True for registered topics, False otherwise.
        """
        assert topic_exists("market") is True
        assert topic_exists("nonexistent.topic") is False

    def test_get_all_topic_names(self) -> None:
        """Test listing all registered topic names.

        Given: TOPIC_REGISTRY with multiple topics,
        When: Calling get_all_topic_names,
        Then: Returns list containing all topic names.
        """
        topic_names = get_all_topic_names()
        assert isinstance(topic_names, list)
        assert "market" in topic_names
        assert len(topic_names) >= 4

    def test_validate_message_schema_valid(self) -> None:
        """Test validation passes with all required fields.

        Given: Message with all required fields for market topic,
        When: Calling validate_message_schema,
        Then: Returns (True, []).
        """
        message: dict[str, Any] = {
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
        }
        is_valid, missing_fields = validate_message_schema("market", message)
        assert is_valid is True
        assert missing_fields == []

    def test_validate_message_schema_missing_fields(self) -> None:
        """Test validation detects missing required fields.

        Given: Message missing exchange and type fields,
        When: Calling validate_message_schema for market topic,
        Then: Returns (False, list_of_missing_fields).
        """
        message: dict[str, Any] = {
            "instrument": "BTC-USD",
            "timestamp": 1640995200000,
        }
        is_valid, missing_fields = validate_message_schema("market", message)
        assert is_valid is False
        assert "exchange" in missing_fields
        assert "type" in missing_fields

    def test_validate_message_schema_unknown_topic(self) -> None:
        """Test validation fails for unknown topic.

        Given: Any message and unknown topic name,
        When: Calling validate_message_schema,
        Then: Returns (False, ["Unknown topic: ..."]).
        """
        message: dict[str, Any] = {"key": "value"}
        is_valid, missing_fields = validate_message_schema("unknown.topic", message)
        assert is_valid is False
        assert len(missing_fields) == 1
        assert "Unknown topic: unknown.topic" in missing_fields

    def test_validate_message_schema_no_required_fields(self) -> None:
        """Test topic with no required fields accepts any message.

        Given: Temporary topic schema with empty required_fields,
        When: Validating any message against it,
        Then: Returns (True, []).
        """
        original_registry = TOPIC_REGISTRY.copy()
        TOPIC_REGISTRY["test.topic"] = TopicSchema(
            name="test.topic",
            pattern="test.",
            description="Test topic",
            category="test",
            required_fields=[],
        )
        try:
            message = {"any": "data"}
            is_valid, missing_fields = validate_message_schema("test.topic", message)
            assert is_valid is True
            assert missing_fields == []
        finally:
            TOPIC_REGISTRY.clear()
            TOPIC_REGISTRY.update(original_registry)
