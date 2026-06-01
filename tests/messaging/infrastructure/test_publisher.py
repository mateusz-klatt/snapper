"""Tests for SequenceTracker and MessagePublisher."""

import json
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from uuid import UUID

import pytest

import snapper.messaging.infrastructure.publisher as publisher_module
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.publisher import build_audit_publisher
from snapper.messaging.infrastructure.publisher import shutdown_audit_publisher
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


class _FakeAuditSocket:
    """Minimal stand-in for a ZMQ PUB socket used by the audit helpers."""

    def __init__(self, connect_error: Exception | None = None) -> None:
        """Record interactions and optionally raise from ``connect``.

        Args:
            connect_error: Exception to raise from ``connect`` to simulate
                a momentarily-unavailable broker; ``None`` connects cleanly.
        """
        self.connect_error = connect_error
        self.connect_addr: str | None = None
        self.sockopts: list[tuple[int, int]] = []
        self.closed = False
        self.hwm_applied = False

    def connect(self, addr: str) -> None:
        """Record the connect target or raise the configured error."""
        self.connect_addr = addr
        if self.connect_error is not None:
            raise self.connect_error

    def setsockopt(self, option: int, value: int) -> None:
        """Record a socket option assignment."""
        self.sockopts.append((option, value))

    def close(self) -> None:
        """Record that the socket was closed."""
        self.closed = True


class _FakeAuditContext:
    """Minimal stand-in for ``zmq.asyncio.Context`` for the audit helpers."""

    def __init__(self, socket: _FakeAuditSocket) -> None:
        """Store the socket to hand out and init teardown state.

        Args:
            socket: The fake socket returned by :meth:`socket`.
        """
        self._socket = socket
        self.terminated = False

    def socket(self, kind: object) -> _FakeAuditSocket:
        """Return the pre-built fake socket regardless of ``kind``."""
        return self._socket

    def term(self) -> None:
        """Record that the context was terminated."""
        self.terminated = True


class TestBuildAuditPublisher:
    """Tests for the shared audit-publisher build helper."""

    def test_builds_publisher_and_connects_to_xsub(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A clean build connects the PUB socket and applies the audit HWM.

        Given: ZMQ context and socket creation succeed,
        When: build_audit_publisher is called with an XSUB endpoint,
        Then: it returns a MessagePublisher plus the context, connects the
            socket to the endpoint, applies the audit HWM, and leaves
            LINGER untouched (only shutdown sets it).
        """
        fake_socket = _FakeAuditSocket()
        fake_context = _FakeAuditContext(fake_socket)
        hwm_calls: list[int] = []

        def _apply_hwm(sock: object, *, sndhwm: int) -> None:
            hwm_calls.append(sndhwm)
            assert sock is fake_socket
            fake_socket.hwm_applied = True

        fake_zmq = SimpleNamespace(
            PUB="PUB",
            LINGER=17,
            asyncio=SimpleNamespace(Context=lambda: fake_context),
        )
        monkeypatch.setattr(publisher_module, "zmq", fake_zmq)
        monkeypatch.setattr(publisher_module, "apply_hwm", _apply_hwm)

        publisher, context = build_audit_publisher("tcp://broker:7500")

        assert isinstance(publisher, MessagePublisher)
        assert context is fake_context
        assert fake_socket.connect_addr == "tcp://broker:7500"
        assert hwm_calls == [publisher_module.HWM_AUDIT]
        assert fake_socket.sockopts == []
        assert not fake_socket.closed
        assert not fake_context.terminated

    def test_connect_failure_cleans_up_socket_and_context(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A connect failure tears down the partial socket with LINGER=0.

        Given: socket creation succeeds but connect raises,
        When: build_audit_publisher is called,
        Then: the socket gets LINGER=0 then close, the context is
            terminated, and the original error re-raises so nothing leaks.
        """
        boom = RuntimeError("broker down")
        fake_socket = _FakeAuditSocket(connect_error=boom)
        fake_context = _FakeAuditContext(fake_socket)
        fake_zmq = SimpleNamespace(
            PUB="PUB",
            LINGER=17,
            asyncio=SimpleNamespace(Context=lambda: fake_context),
        )
        monkeypatch.setattr(publisher_module, "zmq", fake_zmq)
        monkeypatch.setattr(publisher_module, "apply_hwm", lambda sock, **kwargs: None)

        with pytest.raises(RuntimeError, match="broker down"):
            build_audit_publisher("tcp://broker:7500")

        assert (17, 0) in fake_socket.sockopts
        assert fake_socket.closed
        assert fake_context.terminated

    def test_socket_creation_failure_only_terminates_context(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A socket-creation failure terminates the context with no socket.

        Given: the context is created but ``context.socket`` raises before
            any socket exists,
        When: build_audit_publisher is called,
        Then: the cleanup skips the socket guards (raw_socket is None) and
            only terminates the context, then re-raises.
        """
        fake_context = MagicMock()
        fake_context.socket.side_effect = RuntimeError("no socket")
        fake_zmq = SimpleNamespace(
            PUB="PUB",
            LINGER=17,
            asyncio=SimpleNamespace(Context=lambda: fake_context),
        )
        monkeypatch.setattr(publisher_module, "zmq", fake_zmq)
        monkeypatch.setattr(publisher_module, "apply_hwm", lambda sock, **kwargs: None)

        with pytest.raises(RuntimeError, match="no socket"):
            build_audit_publisher("tcp://broker:7500")

        fake_context.term.assert_called_once_with()


class TestShutdownAuditPublisher:
    """Tests for the shared audit-publisher shutdown helper."""

    def test_sets_linger_zero_then_closes_then_terminates(self) -> None:
        """Shutdown sets LINGER=0, closes the publisher, then terms the context.

        Given: a publisher and context,
        When: shutdown_audit_publisher is called,
        Then: the publisher gets LINGER=0 and close, and the context is
            terminated.
        """
        publisher = MagicMock()
        context = MagicMock()

        shutdown_audit_publisher(publisher, context)

        publisher.setsockopt.assert_called_once()
        publisher.close.assert_called_once_with()
        context.term.assert_called_once_with()

    def test_none_arguments_are_noops(self) -> None:
        """Passing None for both arguments performs no work and does not raise.

        Given: both arguments are None (construction failed or unwired),
        When: shutdown_audit_publisher is called,
        Then: it returns without error.
        """
        shutdown_audit_publisher(None, None)

    def test_cleanup_exceptions_are_suppressed(self) -> None:
        """Every cleanup step is suppressed so shutdown never raises.

        Given: setsockopt, close, and term all raise,
        When: shutdown_audit_publisher is called,
        Then: it completes silently and still attempts close after a failed
            setsockopt and term after a failed close.
        """
        publisher = MagicMock()
        publisher.setsockopt.side_effect = RuntimeError("setsockopt boom")
        publisher.close.side_effect = RuntimeError("close boom")
        context = MagicMock()
        context.term.side_effect = RuntimeError("term boom")

        shutdown_audit_publisher(publisher, context)

        publisher.setsockopt.assert_called_once()
        publisher.close.assert_called_once_with()
        context.term.assert_called_once_with()
