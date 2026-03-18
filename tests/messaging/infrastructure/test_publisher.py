"""Tests for SequenceTracker and MessagePublisher."""

import json
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import TickData


def _make_tick(exchange: str = "kraken", instrument: str = "BTC-USD") -> TickData:
    """Build a minimal TickData instance for testing."""
    return TickData(exchange=exchange, instrument=instrument, volume=1.0)


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
    """Tests for MessagePublisher provenance stamping and delegation."""

    @pytest.mark.asyncio
    async def test_publish_stamps_session_and_sequence(self) -> None:
        """Publish stamps session_id and sequence_id on the payload.

        Given: MessagePublisher with mock publisher and tracker,
        When: Publishing a TickData message,
        Then: The serialized payload contains session_id and sequence_id.
        """
        mock_pub = _make_mock_publisher()
        tracker = SequenceTracker()
        mp = MessagePublisher(mock_pub, tracker)

        tick = _make_tick()
        await mp.publish(tick)

        mock_pub.send_multipart.assert_awaited_once()
        call_args = mock_pub.send_multipart.call_args
        payload_bytes: bytes = call_args[0][1]
        payload = json.loads(payload_bytes)
        assert payload["session_id"] == tracker.session_id
        assert payload["sequence_id"] == 1

    @pytest.mark.asyncio
    async def test_publish_with_explicit_topic_override(self) -> None:
        """Publish uses explicit topic when provided.

        Given: MessagePublisher,
        When: Publishing with topic override,
        Then: send_multipart receives the overridden topic.
        """
        mock_pub = _make_mock_publisher()
        tracker = SequenceTracker()
        mp = MessagePublisher(mock_pub, tracker)

        tick = _make_tick()
        await mp.publish(tick, topic="custom.topic.override")

        call_args = mock_pub.send_multipart.call_args
        assert call_args[0][0] == "custom.topic.override"

    @pytest.mark.asyncio
    async def test_publish_increments_sequence_per_topic(self) -> None:
        """Publish increments sequence_id for each call on the same topic.

        Given: MessagePublisher,
        When: Publishing two messages to the same topic,
        Then: sequence_id is 1 then 2 in the payloads.
        """
        mock_pub = _make_mock_publisher()
        tracker = SequenceTracker()
        mp = MessagePublisher(mock_pub, tracker)

        await mp.publish(_make_tick())
        await mp.publish(_make_tick())

        first_payload = json.loads(mock_pub.send_multipart.call_args_list[0][0][1])
        second_payload = json.loads(mock_pub.send_multipart.call_args_list[1][0][1])
        assert first_payload["sequence_id"] == 1
        assert second_payload["sequence_id"] == 2

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

    @pytest.mark.asyncio
    async def test_publish_calls_send_multipart_with_topic_and_bytes(self) -> None:
        """Publish calls send_multipart with derived topic and encoded payload.

        Given: MessagePublisher,
        When: Publishing a TickData,
        Then: send_multipart receives the correct topic string and bytes payload.
        """
        mock_pub = _make_mock_publisher()
        tracker = SequenceTracker()
        mp = MessagePublisher(mock_pub, tracker)

        tick = _make_tick(exchange="kraken", instrument="ETH-USD")
        await mp.publish(tick)

        call_args = mock_pub.send_multipart.call_args
        assert call_args[0][0] == "market.kraken.ETH-USD.ticks"
        assert isinstance(call_args[0][1], bytes)

    @pytest.mark.asyncio
    async def test_publish_does_not_mutate_original(self) -> None:
        """Publish creates a model_copy and does not mutate the original data.

        Given: MessagePublisher and a TickData with default session_id/sequence_id,
        When: Publishing,
        Then: The original data still has empty session_id and zero sequence_id.
        """
        mock_pub = _make_mock_publisher()
        tracker = SequenceTracker()
        mp = MessagePublisher(mock_pub, tracker)

        tick = _make_tick()
        await mp.publish(tick)

        assert tick.session_id == ""
        assert tick.sequence_id == 0

    @pytest.mark.asyncio
    async def test_reconnect_continues_counters(self) -> None:
        """New MessagePublisher with same tracker continues counter state.

        Given: A tracker used by one MessagePublisher that published one message,
        When: Creating a new MessagePublisher with the same tracker and publishing,
        Then: The sequence_id continues from where the first left off.
        """
        mock_pub1 = _make_mock_publisher()
        tracker = SequenceTracker()
        mp1 = MessagePublisher(mock_pub1, tracker)
        await mp1.publish(_make_tick())

        mock_pub2 = _make_mock_publisher()
        mp2 = MessagePublisher(mock_pub2, tracker)
        await mp2.publish(_make_tick())

        first_payload = json.loads(mock_pub1.send_multipart.call_args[0][1])
        second_payload = json.loads(mock_pub2.send_multipart.call_args[0][1])
        assert first_payload["sequence_id"] == 1
        assert second_payload["sequence_id"] == 2
        assert first_payload["session_id"] == second_payload["session_id"]
