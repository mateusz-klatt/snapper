"""Tests for validated ZMQ socket wrappers."""

from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
import zmq

from snapper.messaging.infrastructure.validated_socket import HWM_AUDIT
from snapper.messaging.infrastructure.validated_socket import HWM_BROKER
from snapper.messaging.infrastructure.validated_socket import HWM_MARKET_DATA
from snapper.messaging.infrastructure.validated_socket import HWM_ORDER_FLOW
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.topics.validation import TopicValidationError


class TestValidatedPublisher:
    """Tests for ValidatedPublisher socket wrapper."""

    @pytest.mark.asyncio
    async def test_send_multipart_valid_topic(self) -> None:
        """Test send_multipart with valid hierarchical topic.

        Given: ValidatedPublisher wrapping mock socket,
        When: Sending message with valid topic format,
        Then: Socket send_multipart called with encoded topic and payload.
        """
        mock_socket = AsyncMock()
        mock_socket.setsockopt = MagicMock()
        mock_socket.close = MagicMock()
        publisher = ValidatedPublisher(mock_socket)
        await publisher.send_multipart(
            topic="market.kraken.BTC-USD.candles.1m",
            payload=b'{"test": "data"}',
        )
        mock_socket.send_multipart.assert_called_once()
        call_args = mock_socket.send_multipart.call_args
        assert call_args[0][0] == [b"market.kraken.BTC-USD.candles.1m", b'{"test": "data"}']

    @pytest.mark.asyncio
    async def test_send_multipart_invalid_topic_raises(self) -> None:
        """Test TopicValidationError on malformed topic.

        Given: ValidatedPublisher wrapping mock socket,
        When: Sending message with invalid topic format,
        Then: TopicValidationError raised, socket not called.
        """
        mock_socket = AsyncMock()
        mock_socket.setsockopt = MagicMock()
        mock_socket.close = MagicMock()
        publisher = ValidatedPublisher(mock_socket)
        with pytest.raises(TopicValidationError, match="Invalid topic 'BTC-USD:1h'"):
            await publisher.send_multipart(
                topic="BTC-USD:1h",
                payload=b'{"test": "data"}',
            )
        mock_socket.send_multipart.assert_not_called()

    @pytest.mark.asyncio
    async def test_send_multipart_with_flags(self) -> None:
        """Test flags passthrough to socket.

        Given: ValidatedPublisher wrapping mock socket,
        When: Sending with zmq.NOBLOCK flag,
        Then: Flag is passed to underlying socket.
        """
        mock_socket = AsyncMock()
        mock_socket.setsockopt = MagicMock()
        mock_socket.close = MagicMock()
        publisher = ValidatedPublisher(mock_socket)
        await publisher.send_multipart(
            topic="market.kraken.BTC-USD.candles.1m",
            payload=b'{"test": "data"}',
            flags=zmq.NOBLOCK,
        )
        call_args = mock_socket.send_multipart.call_args
        assert call_args[1]["flags"] == zmq.NOBLOCK

    def test_close_without_linger(self) -> None:
        """Test close sets linger and delegates to wrapped socket.

        Given: ValidatedPublisher wrapping mock socket,
        When: Calling close(),
        Then: Linger is disabled and underlying socket close called.
        """
        mock_socket = MagicMock()
        publisher = ValidatedPublisher(mock_socket)
        publisher.close()
        mock_socket.setsockopt.assert_called_once_with(zmq.LINGER, 0)
        mock_socket.close.assert_called_once_with()

    def test_del_closes_publisher_socket(self) -> None:
        """Test publisher destructor closes the wrapped socket.

        Given: ValidatedPublisher wrapping mock socket,
        When: __del__ is invoked,
        Then: Underlying socket is closed.
        """
        mock_socket = MagicMock()
        publisher = ValidatedPublisher(mock_socket)
        publisher.__del__()
        mock_socket.close.assert_called_once_with()

    def test_del_ignores_publisher_close_error(self) -> None:
        """Test publisher destructor suppresses close errors."""
        mock_socket = MagicMock()
        mock_socket.close.side_effect = RuntimeError("close failed")
        publisher = ValidatedPublisher(mock_socket)
        publisher.__del__()
        mock_socket.close.assert_called_once_with()


class TestValidatedSubscriber:
    """Tests for ValidatedSubscriber socket wrapper."""

    def test_subscribe_valid_topic(self) -> None:
        """Test subscribe with valid full topic.

        Given: ValidatedSubscriber wrapping mock socket,
        When: Subscribing to valid topic string,
        Then: Socket setsockopt_string called with SUBSCRIBE.
        """
        mock_socket = MagicMock()
        subscriber = ValidatedSubscriber(mock_socket)
        subscriber.subscribe("market.kraken.BTC-USD.candles.")
        mock_socket.setsockopt_string.assert_called_once_with(
            zmq.SUBSCRIBE,
            "market.kraken.BTC-USD.candles.",
        )

    def test_subscribe_valid_prefix_pattern(self) -> None:
        """Test subscribe with prefix pattern.

        Given: ValidatedSubscriber wrapping mock socket,
        When: Subscribing to prefix pattern,
        Then: Socket setsockopt_string called with prefix.
        """
        mock_socket = MagicMock()
        subscriber = ValidatedSubscriber(mock_socket)
        subscriber.subscribe("market.kraken.")
        mock_socket.setsockopt_string.assert_called_once_with(zmq.SUBSCRIBE, "market.kraken.")

    def test_subscribe_invalid_pattern_raises(self) -> None:
        """Test TopicValidationError on invalid subscription.

        Given: ValidatedSubscriber wrapping mock socket,
        When: Subscribing with invalid pattern,
        Then: TopicValidationError raised, socket not called.
        """
        mock_socket = MagicMock()
        subscriber = ValidatedSubscriber(mock_socket)
        with pytest.raises(TopicValidationError, match="Invalid subscription pattern 'BTC-USD:1h'"):
            subscriber.subscribe("BTC-USD:1h")
        mock_socket.setsockopt_string.assert_not_called()

    def test_unsubscribe(self) -> None:
        """Test unsubscribe removes topic.

        Given: ValidatedSubscriber wrapping mock socket,
        When: Unsubscribing from topic,
        Then: Socket setsockopt_string called with UNSUBSCRIBE.
        """
        mock_socket = MagicMock()
        subscriber = ValidatedSubscriber(mock_socket)
        subscriber.unsubscribe("market.kraken.BTC-USD.candles.1m")
        mock_socket.setsockopt_string.assert_called_once_with(
            zmq.UNSUBSCRIBE,
            "market.kraken.BTC-USD.candles.1m",
        )

    @pytest.mark.asyncio
    async def test_recv_multipart(self) -> None:
        """Test recv_multipart unpacks topic and payload.

        Given: Mock socket returning 2-part message,
        When: Calling recv_multipart,
        Then: Returns decoded topic string and raw payload bytes.
        """
        mock_socket = AsyncMock()
        mock_socket.setsockopt = MagicMock()
        mock_socket.close = MagicMock()
        mock_socket.recv_multipart.return_value = [
            b"market.kraken.BTC-USD.candles.1m",
            b'{"test": "data"}',
        ]
        subscriber = ValidatedSubscriber(mock_socket)
        topic, payload = await subscriber.recv_multipart()
        assert topic == "market.kraken.BTC-USD.candles.1m"
        assert payload == b'{"test": "data"}'

    @pytest.mark.asyncio
    async def test_recv_multipart_invalid_format_raises(self) -> None:
        """Test ValueError on malformed message frames.

        Given: Mock socket returning single-part message,
        When: Calling recv_multipart,
        Then: ValueError raised with expected parts count.
        """
        mock_socket = AsyncMock()
        mock_socket.setsockopt = MagicMock()
        mock_socket.close = MagicMock()
        mock_socket.recv_multipart.return_value = [b"only_one_part"]
        subscriber = ValidatedSubscriber(mock_socket)
        with pytest.raises(ValueError, match="Expected 2-part message, got 1 parts"):
            await subscriber.recv_multipart()

    def test_close(self) -> None:
        """Test close sets linger and delegates to wrapped socket.

        Given: ValidatedSubscriber wrapping mock socket,
        When: Calling close(),
        Then: Linger is disabled and underlying socket close called.
        """
        mock_socket = MagicMock()
        subscriber = ValidatedSubscriber(mock_socket)
        subscriber.close()
        mock_socket.setsockopt.assert_called_once_with(zmq.LINGER, 0)
        mock_socket.close.assert_called_once()

    def test_del_closes_subscriber_socket(self) -> None:
        """Test subscriber destructor closes the wrapped socket.

        Given: ValidatedSubscriber wrapping mock socket,
        When: __del__ is invoked,
        Then: Underlying socket is closed.
        """
        mock_socket = MagicMock()
        subscriber = ValidatedSubscriber(mock_socket)
        subscriber.__del__()
        mock_socket.close.assert_called_once()

    def test_del_ignores_subscriber_close_error(self) -> None:
        """Test subscriber destructor suppresses close errors."""
        mock_socket = MagicMock()
        mock_socket.close.side_effect = RuntimeError("close failed")
        subscriber = ValidatedSubscriber(mock_socket)
        subscriber.__del__()
        mock_socket.close.assert_called_once()


class TestHwmConstants:
    """Tests for ZMQ high water mark constants."""

    def test_order_flow_is_unlimited(self) -> None:
        """Test order flow HWM is zero (unlimited).

        Given: HWM_ORDER_FLOW constant,
        When: Checked,
        Then: Value is 0 meaning unlimited buffering.
        """
        assert HWM_ORDER_FLOW == 0

    def test_broker_hwm_is_generous(self) -> None:
        """Test broker HWM is larger than default.

        Given: HWM_BROKER constant,
        When: Compared to libzmq default of 1000,
        Then: Value is significantly higher.
        """
        assert HWM_BROKER == 10_000

    def test_market_data_hwm_above_default(self) -> None:
        """Test market data HWM exceeds libzmq default.

        Given: HWM_MARKET_DATA constant,
        When: Compared to default,
        Then: Value provides burst tolerance.
        """
        assert HWM_MARKET_DATA == 5_000

    def test_audit_hwm_is_generous(self) -> None:
        """Test audit HWM minimizes message loss.

        Given: HWM_AUDIT constant,
        When: Checked,
        Then: Value is high to preserve audit trail.
        """
        assert HWM_AUDIT == 10_000


class TestApplyHwm:
    """Tests for apply_hwm helper function."""

    def test_apply_sndhwm_only(self) -> None:
        """Test setting only send high water mark.

        Given: A mock socket,
        When: apply_hwm called with sndhwm only,
        Then: Only SNDHWM set on socket.
        """
        sock = MagicMock()
        apply_hwm(sock, sndhwm=5000)
        sock.setsockopt.assert_called_once_with(zmq.SNDHWM, 5000)

    def test_apply_rcvhwm_only(self) -> None:
        """Test setting only receive high water mark.

        Given: A mock socket,
        When: apply_hwm called with rcvhwm only,
        Then: Only RCVHWM set on socket.
        """
        sock = MagicMock()
        apply_hwm(sock, rcvhwm=10000)
        sock.setsockopt.assert_called_once_with(zmq.RCVHWM, 10000)

    def test_apply_both_hwm(self) -> None:
        """Test setting both send and receive high water marks.

        Given: A mock socket,
        When: apply_hwm called with both sndhwm and rcvhwm,
        Then: Both options set on socket.
        """
        sock = MagicMock()
        apply_hwm(sock, sndhwm=5000, rcvhwm=10000)
        assert sock.setsockopt.call_count == 2
        sock.setsockopt.assert_any_call(zmq.SNDHWM, 5000)
        sock.setsockopt.assert_any_call(zmq.RCVHWM, 10000)

    def test_apply_no_args_is_noop(self) -> None:
        """Test apply_hwm with no arguments does nothing.

        Given: A mock socket,
        When: apply_hwm called without sndhwm or rcvhwm,
        Then: No setsockopt calls made.
        """
        sock = MagicMock()
        apply_hwm(sock)
        sock.setsockopt.assert_not_called()

    def test_apply_zero_means_unlimited(self) -> None:
        """Test zero value means unlimited buffering.

        Given: A mock socket,
        When: apply_hwm called with HWM_ORDER_FLOW (0),
        Then: Socket option set to 0.
        """
        sock = MagicMock()
        apply_hwm(sock, sndhwm=HWM_ORDER_FLOW, rcvhwm=HWM_ORDER_FLOW)
        sock.setsockopt.assert_any_call(zmq.SNDHWM, 0)
        sock.setsockopt.assert_any_call(zmq.RCVHWM, 0)
