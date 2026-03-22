"""Tests for SequenceTracker and MessagePublisher."""

import json
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import TickData


def _make_tick(
    tracker: SequenceTracker,
    topic: str = "market.kraken.BTC-USD.ticks",
    exchange: str = "kraken",
    instrument: str = "BTC-USD",
) -> TickData:
    """Build a complete TickData instance with provenance from tracker."""
    return TickData(
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence(topic),
        public_id="test-public-id",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        exchange=exchange,
        instrument=instrument,
        volume=1.0,
    )


def _make_mock_publisher() -> MagicMock:
    """Build a mock ValidatedPublisher with async send_multipart."""
    mock = MagicMock()
    mock.send_multipart = AsyncMock()
    mock.close = MagicMock()
    mock.setsockopt = MagicMock()
    return mock


class TestSequenceTracker:
    """Tests for SequenceTracker session identity and counters."""

    def test_session_id_is_valid_uuid7(self) -> None:
        """SequenceTracker generates a valid UUID7 string as session_id.

        Given: A new SequenceTracker,
        When: Accessing session_id,
        Then: The value is a valid UUID string.
        """
        tracker = SequenceTracker()
        UUID(tracker.session_id)

    def test_next_sequence_monotonic_for_same_topic(self) -> None:
        """next_sequence returns 1, 2, 3 for consecutive calls on the same topic.

        Given: A SequenceTracker,
        When: Calling next_sequence three times with the same topic,
        Then: Returns 1, 2, 3.
        """
        tracker = SequenceTracker()
        assert tracker.next_sequence("market.kraken.BTC-USD.ticks") == 1
        assert tracker.next_sequence("market.kraken.BTC-USD.ticks") == 2
        assert tracker.next_sequence("market.kraken.BTC-USD.ticks") == 3

    def test_next_sequence_independent_per_topic(self) -> None:
        """next_sequence maintains independent counters per topic.

        Given: A SequenceTracker,
        When: Calling next_sequence with two different topics,
        Then: Each topic starts at 1 independently.
        """
        tracker = SequenceTracker()
        assert tracker.next_sequence("topic.a") == 1
        assert tracker.next_sequence("topic.b") == 1
        assert tracker.next_sequence("topic.a") == 2
        assert tracker.next_sequence("topic.b") == 2


class TestMessagePublisher:
    """Tests for MessagePublisher send and delegation."""

    @pytest.mark.asyncio
    async def test_send_serializes_and_routes(self) -> None:
        """Send serializes data and calls send_multipart with stream_key.

        Given: MessagePublisher with mock publisher,
        When: Sending a complete TickData with stream_key,
        Then: send_multipart receives the stream_key and serialized bytes.
        """
        mock_pub = _make_mock_publisher()
        tracker = SequenceTracker()
        mp = MessagePublisher(mock_pub, tracker)

        topic = "market.kraken.BTC-USD.ticks"
        tick = _make_tick(tracker, topic)
        await mp.send(topic, tick)

        mock_pub.send_multipart.assert_awaited_once()
        call_args = mock_pub.send_multipart.call_args
        assert call_args[0][0] == topic
        payload = json.loads(call_args[0][1])
        assert payload["session_id"] == tracker.session_id
        assert payload["sequence_id"] == 1

    @pytest.mark.asyncio
    async def test_send_preserves_provenance_from_producer(self) -> None:
        """Send does not modify the data — provenance comes from the producer.

        Given: MessagePublisher,
        When: Sending a TickData with sequence_id=42,
        Then: The serialized payload has sequence_id=42 (no overwriting).
        """
        mock_pub = _make_mock_publisher()
        tracker = SequenceTracker()
        mp = MessagePublisher(mock_pub, tracker)

        tick = TickData(
            session_id="custom-session",
            sequence_id=42,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            volume=1.0,
        )
        await mp.send("market.kraken.BTC-USD.ticks", tick)

        payload = json.loads(mock_pub.send_multipart.call_args[0][1])
        assert payload["session_id"] == "custom-session"
        assert payload["sequence_id"] == 42

    @pytest.mark.asyncio
    async def test_send_with_flags(self) -> None:
        """Send passes flags to send_multipart.

        Given: MessagePublisher,
        When: Sending with flags=1,
        Then: send_multipart receives flags=1.
        """
        mock_pub = _make_mock_publisher()
        tracker = SequenceTracker()
        mp = MessagePublisher(mock_pub, tracker)

        tick = _make_tick(tracker)
        await mp.send("market.kraken.BTC-USD.ticks", tick, flags=1)

        call_kwargs = mock_pub.send_multipart.call_args[1]
        assert call_kwargs["flags"] == 1

    @pytest.mark.asyncio
    async def test_tracker_property_exposes_shared_tracker(self) -> None:
        """Tracker property returns the injected SequenceTracker.

        Given: MessagePublisher created with a tracker,
        When: Accessing .tracker,
        Then: Returns the same tracker instance.
        """
        mock_pub = _make_mock_publisher()
        tracker = SequenceTracker()
        mp = MessagePublisher(mock_pub, tracker)

        assert mp.tracker is tracker

    @pytest.mark.asyncio
    async def test_reconnect_continues_counters(self) -> None:
        """New MessagePublisher with same tracker continues counter state.

        Given: A tracker used by one MessagePublisher that sent one message,
        When: Creating a new MessagePublisher with the same tracker and sending,
        Then: The sequence_id continues from where the first left off.
        """
        tracker = SequenceTracker()
        topic = "market.kraken.BTC-USD.ticks"

        mock_pub1 = _make_mock_publisher()
        mp1 = MessagePublisher(mock_pub1, tracker)
        tick1 = _make_tick(tracker, topic)
        await mp1.send(topic, tick1)

        mock_pub2 = _make_mock_publisher()
        mp2 = MessagePublisher(mock_pub2, tracker)
        tick2 = _make_tick(tracker, topic)
        await mp2.send(topic, tick2)

        first_payload = json.loads(mock_pub1.send_multipart.call_args[0][1])
        second_payload = json.loads(mock_pub2.send_multipart.call_args[0][1])
        assert first_payload["sequence_id"] == 1
        assert second_payload["sequence_id"] == 2
        assert first_payload["session_id"] == second_payload["session_id"]

    def test_close_delegates_to_inner_publisher(self) -> None:
        """Close delegates to the underlying ValidatedPublisher.

        Given: MessagePublisher wrapping a mock,
        When: Calling close,
        Then: Inner publisher close is called.
        """
        mock_pub = _make_mock_publisher()
        tracker = SequenceTracker()
        mp = MessagePublisher(mock_pub, tracker)

        mp.close()

        mock_pub.close.assert_called_once()

    def test_setsockopt_delegates_to_inner_publisher(self) -> None:
        """Setsockopt delegates to the underlying ValidatedPublisher.

        Given: MessagePublisher wrapping a mock,
        When: Calling setsockopt,
        Then: Inner publisher setsockopt is called with same args.
        """
        mock_pub = _make_mock_publisher()
        tracker = SequenceTracker()
        mp = MessagePublisher(mock_pub, tracker)

        mp.setsockopt(1, 0)

        mock_pub.setsockopt.assert_called_once_with(1, 0)

    def test_session_id_returns_tracker_session_id(self) -> None:
        """session_id property returns the tracker's session_id.

        Given: MessagePublisher with a tracker,
        When: Accessing session_id,
        Then: Returns the tracker session_id.
        """
        mock_pub = _make_mock_publisher()
        tracker = SequenceTracker()
        mp = MessagePublisher(mock_pub, tracker)

        assert mp.session_id == tracker.session_id
