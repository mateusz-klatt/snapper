"""Tests for MessagePublisher.send stamping the topic field on every payload.

Phase 2 promotes ``StrictDataSchema.publish_to(topic) -> bytes`` as
the chokepoint that stamps the routing key on every production
payload before it crosses the wire. ``MessagePublisher.send`` is one
of three production publish call sites that route through it; these
tests pin down the contract end-to-end (build a real
``StrictDataSchema`` subclass, call ``send``, assert the serialized
bytes carry the expected ``topic`` value).
"""

import json
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import TickData


def _make_tick(tracker: SequenceTracker, topic: str | None = None) -> TickData:
    """Build a complete TickData instance with optional preset topic."""
    return TickData(
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence("market.kraken.BTC-USD.ticks"),
        public_id="01933b00-0000-7000-8000-000000000001",
        timestamp=datetime(2026, 4, 28, tzinfo=UTC),
        exchange="kraken",
        instrument="BTC-USD",
        volume=1.0,
        topic=topic,
    )


def _make_mock_publisher() -> MagicMock:
    """Build a mock ValidatedPublisher with async send_multipart."""
    mock = MagicMock()
    mock.send_multipart = AsyncMock()
    mock.close = MagicMock()
    mock.setsockopt = MagicMock()
    return mock


@pytest.fixture
def tracker() -> SequenceTracker:
    """Fresh SequenceTracker for each test."""
    return SequenceTracker()


@pytest.fixture
def publisher(tracker: SequenceTracker) -> tuple[MessagePublisher, MagicMock]:
    """MessagePublisher wired around a mock ValidatedPublisher."""
    mock = _make_mock_publisher()
    return (MessagePublisher(mock, tracker), mock)


class TestMessagePublisherSendStampsTopic:
    """``MessagePublisher.send`` MUST stamp the stream_key onto the topic field."""

    @pytest.mark.asyncio
    async def test_send_stamps_stream_key_as_topic(
        self,
        tracker: SequenceTracker,
        publisher: tuple[MessagePublisher, MagicMock],
    ) -> None:
        """Send call places stream_key into the serialized ``topic`` field."""
        message_publisher, mock_publisher = publisher
        tick = _make_tick(tracker)
        await message_publisher.send("market.kraken.BTC-USD.ticks", tick)
        mock_publisher.send_multipart.assert_awaited_once()
        call_args = mock_publisher.send_multipart.await_args
        sent_payload: bytes = call_args.args[1]
        as_dict = json.loads(sent_payload)
        assert as_dict["topic"] == "market.kraken.BTC-USD.ticks"

    @pytest.mark.asyncio
    async def test_send_overrides_preset_topic(
        self,
        tracker: SequenceTracker,
        publisher: tuple[MessagePublisher, MagicMock],
    ) -> None:
        """A topic preset on the producer-side instance is overwritten by stream_key."""
        message_publisher, mock_publisher = publisher
        tick = _make_tick(tracker, topic="market.OLD-TOPIC")
        await message_publisher.send("market.kraken.BTC-USD.ticks", tick)
        sent_payload: bytes = mock_publisher.send_multipart.await_args.args[1]
        as_dict = json.loads(sent_payload)
        assert as_dict["topic"] == "market.kraken.BTC-USD.ticks"

    @pytest.mark.asyncio
    async def test_send_does_not_mutate_source_instance(
        self,
        tracker: SequenceTracker,
        publisher: tuple[MessagePublisher, MagicMock],
    ) -> None:
        """The publisher uses model_copy so the producer-side instance is unchanged."""
        message_publisher, _mock = publisher
        tick = _make_tick(tracker)
        original_topic = tick.topic
        await message_publisher.send("market.kraken.BTC-USD.ticks", tick)
        assert tick.topic == original_topic

    @pytest.mark.asyncio
    async def test_send_preserves_envelope_and_payload_fields(
        self,
        tracker: SequenceTracker,
        publisher: tuple[MessagePublisher, MagicMock],
    ) -> None:
        """All other fields round-trip identically through the publisher."""
        message_publisher, mock_publisher = publisher
        tick = _make_tick(tracker)
        await message_publisher.send("market.kraken.BTC-USD.ticks", tick)
        sent_payload: bytes = mock_publisher.send_multipart.await_args.args[1]
        as_dict = json.loads(sent_payload)
        assert as_dict["type"] == "tick"
        assert as_dict["session_id"] == tracker.session_id
        assert as_dict["sequence_id"] == tick.sequence_id
        assert as_dict["public_id"] == "01933b00-0000-7000-8000-000000000001"
        assert as_dict["exchange"] == "kraken"
        assert as_dict["instrument"] == "BTC-USD"
        assert as_dict["volume"] == 1.0

    @pytest.mark.asyncio
    async def test_send_forwards_flags_to_underlying_publisher(
        self,
        tracker: SequenceTracker,
        publisher: tuple[MessagePublisher, MagicMock],
    ) -> None:
        """Optional ZMQ flags pass through to send_multipart unchanged."""
        message_publisher, mock_publisher = publisher
        tick = _make_tick(tracker)
        await message_publisher.send("market.kraken.BTC-USD.ticks", tick, flags=2)
        call_kwargs = mock_publisher.send_multipart.await_args.kwargs
        assert call_kwargs.get("flags") == 2
