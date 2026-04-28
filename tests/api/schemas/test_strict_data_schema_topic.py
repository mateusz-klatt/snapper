"""Tests for the topic field + publish_to chokepoint on StrictDataSchema.

The bridge's wire-contract gains a ``topic`` field on every
StrictDataSchema-derived payload, and ``publish_to(topic)`` is the
single point in the codebase that stamps it. These tests pin down
the contract: default-None on construction, explicit set persists
through round-trip, ``publish_to`` produces UTF-8 JSON bytes with the
topic populated, and an existing topic on the producer-side instance
is overwritten by the call (publisher-side wins).
"""

import json
from datetime import UTC
from datetime import datetime
from typing import Literal

import pytest

from snapper.api.schemas.base import StrictDataSchema


class _ExampleData(StrictDataSchema[Literal["example"]]):
    """Minimal StrictDataSchema subclass for these tests."""

    type: Literal["example"] = "example"
    payload_value: int


@pytest.fixture
def example_instance() -> _ExampleData:
    """Build a fully-populated _ExampleData with deterministic provenance."""
    return _ExampleData(
        sequence_id=1,
        public_id="01933b00-0000-7000-8000-000000000001",
        timestamp=datetime(2026, 4, 28, tzinfo=UTC),
        session_id="sess-1",
        payload_value=42,
    )


class TestTopicFieldDefault:
    """``topic`` is optional with default None on every subclass."""

    def test_default_topic_is_none(self, example_instance: _ExampleData) -> None:
        """Constructing without topic leaves it None."""
        assert example_instance.topic is None

    def test_explicit_topic_persists(self) -> None:
        """Setting topic at construction persists on the instance."""
        instance = _ExampleData(
            sequence_id=1,
            public_id="01933b00-0000-7000-8000-000000000001",
            timestamp=datetime(2026, 4, 28, tzinfo=UTC),
            session_id="sess-1",
            payload_value=42,
            topic="market.kraken.BTC-USD.signals",
        )
        assert instance.topic == "market.kraken.BTC-USD.signals"

    def test_default_topic_serializes_as_null(self, example_instance: _ExampleData) -> None:
        """Pydantic includes default-None topic in the JSON shape (always null on the wire)."""
        as_dict = json.loads(example_instance.to_json())
        assert "topic" in as_dict
        assert as_dict["topic"] is None


class TestPublishTo:
    """``publish_to`` stamps topic + emits UTF-8 JSON bytes."""

    def test_returns_utf8_bytes(self, example_instance: _ExampleData) -> None:
        """The chokepoint helper returns ``bytes`` ready for send_multipart."""
        payload = example_instance.publish_to("test.topic")
        assert isinstance(payload, bytes)

    def test_serialized_payload_contains_topic(self, example_instance: _ExampleData) -> None:
        """The serialized JSON has the topic field populated with the supplied value."""
        payload = example_instance.publish_to("market.kraken.BTC-USD.signals")
        as_dict = json.loads(payload)
        assert as_dict["topic"] == "market.kraken.BTC-USD.signals"

    def test_overwrites_preset_topic(self) -> None:
        """A topic preset at construction is overwritten by ``publish_to`` (publisher wins)."""
        instance = _ExampleData(
            sequence_id=1,
            public_id="01933b00-0000-7000-8000-000000000001",
            timestamp=datetime(2026, 4, 28, tzinfo=UTC),
            session_id="sess-1",
            payload_value=42,
            topic="market.kraken.OLD-TOPIC",
        )
        payload = instance.publish_to("market.kraken.NEW-TOPIC")
        as_dict = json.loads(payload)
        assert as_dict["topic"] == "market.kraken.NEW-TOPIC"

    def test_does_not_mutate_source_instance(self, example_instance: _ExampleData) -> None:
        """``publish_to`` uses model_copy, leaving the source instance unchanged."""
        example_instance.publish_to("market.test.topic")
        assert example_instance.topic is None

    def test_preserves_envelope_and_payload_fields(self, example_instance: _ExampleData) -> None:
        """Every other field round-trips identically through publish_to."""
        payload = example_instance.publish_to("test.topic")
        as_dict = json.loads(payload)
        assert as_dict["type"] == "example"
        assert as_dict["sequence_id"] == 1
        assert as_dict["public_id"] == "01933b00-0000-7000-8000-000000000001"
        assert as_dict["session_id"] == "sess-1"
        assert as_dict["payload_value"] == 42

    def test_round_trip_via_from_json(self, example_instance: _ExampleData) -> None:
        """A payload stamped by ``publish_to`` parses back via ``from_json`` identically."""
        payload = example_instance.publish_to("market.test.topic")
        parsed = _ExampleData.from_json(payload.decode("utf-8"))
        assert parsed.topic == "market.test.topic"
        assert parsed.sequence_id == example_instance.sequence_id
        assert parsed.public_id == example_instance.public_id
        assert parsed.session_id == example_instance.session_id
        assert parsed.payload_value == example_instance.payload_value
