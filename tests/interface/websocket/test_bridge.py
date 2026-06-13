"""Tests for ZMQ-WebSocket bridge service core functionality."""

import asyncio
import json
import time
from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import zmq
import zmq.asyncio

from snapper.interface.websocket.bridge import MAX_PENDING_MESSAGES_TRADE
from snapper.interface.websocket.bridge import ZmqWebSocketBridgeService
from snapper.interface.websocket.models import TopicConfigurationModel
from snapper.interface.websocket.models import TopicMetricsModel
from snapper.interface.websocket.models import TopicSubscriptionModel


class DummyWebSocket:
    """Dummy WebSocket for testing bridge operations."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.send_text = AsyncMock()
        self.send_json = AsyncMock()
        self.close = AsyncMock()

    def __hash__(self) -> int:
        """Magic method."""
        return id(self)

    def __eq__(self, other: object) -> bool:
        """Magic method."""
        return self is other


@pytest.mark.asyncio
async def test_get_default_parameters_returns_placeholder() -> None:
    """Get default kwargs returns connection_manager placeholder.

    Given: The ZmqWebSocketBridgeService class,
    When: Calling get_default_parameters,
    Then: Returns dict with connection_manager key set to None.
    """
    default = ZmqWebSocketBridgeService.get_default_parameters(settings=AsyncMock())
    assert "connection_manager" in default
    assert default["connection_manager"] is None


@pytest.mark.asyncio
async def test_unsubscribe_client_updates_metrics_and_stops_subscription() -> None:
    """Unsubscribe client updates metrics and stops ZMQ subscription.

    Given: A client subscribed to a topic,
    When: The client unsubscribes,
    Then: Metrics are updated and ZMQ subscription is stopped.
    """
    bridge = ZmqWebSocketBridgeService(connection_manager=None)
    ws: Any = DummyWebSocket()
    bridge.topic_subscriptions["topic1"] = {ws: TopicSubscriptionModel(websocket=ws, throttle_ms=0)}
    bridge.client_subscriptions[ws] = {"topic1"}
    bridge.topic_metrics["topic1"] = TopicMetricsModel(active_subscribers=1)
    bridge._stop_zmq_subscription = AsyncMock()
    await bridge.unsubscribe_client(ws, ["topic1"])
    assert "topic1" not in bridge.topic_subscriptions
    assert ws not in bridge.client_subscriptions
    assert bridge.topic_metrics["topic1"].active_subscribers == 0
    bridge._stop_zmq_subscription.assert_awaited_once_with("topic1")


@pytest.mark.asyncio
async def test_forward_to_clients_raw_passthrough() -> None:
    """Forward to clients passes raw JSON to websocket.

    Given: A subscription for a topic,
    When: A raw JSON message is forwarded,
    Then: The exact JSON string is sent to the websocket.
    """
    bridge = ZmqWebSocketBridgeService(connection_manager=None)
    ws: Any = DummyWebSocket()
    bridge.topic_subscriptions["foo"] = {ws: TopicSubscriptionModel(websocket=ws, throttle_ms=0)}
    bridge.topic_metrics["foo"] = TopicMetricsModel()
    raw_json = '{"foo": "bar"}'
    await bridge._forward_to_clients("foo", "foo", raw_json)
    ws.send_text.assert_awaited_once_with(raw_json)


@pytest.mark.asyncio
async def test_forward_to_clients_backpressure_trade_disconnects_client() -> None:
    """Forward to clients disconnects on trade topic backpressure.

    Given: A trade subscription with max pending messages,
    When: Another message is forwarded,
    Then: The client is disconnected and message is dropped.
    """
    bridge = ZmqWebSocketBridgeService(connection_manager=None)
    ws: Any = DummyWebSocket()
    topic = "orders.commands.kraken."
    sub = TopicSubscriptionModel(websocket=ws, throttle_ms=0, client_id="c1")
    sub.pending_count = bridge._get_max_pending(topic)
    bridge.topic_subscriptions[topic] = {sub.websocket: sub}
    bridge.topic_metrics[topic] = TopicMetricsModel()
    bridge.disconnect_client = AsyncMock()
    order_payload = json.dumps(
        {
            "public_id": "1",
            "instrument": "BTC-USD",
            "exchange": "kraken",
            "side": "buy",
            "order_type": "limit",
            "size": 1.0,
            "price": 10_000.0,
            "status": "open",
            "created_at": datetime.now(tz=UTC).isoformat(),
            "updated_at": datetime.now(tz=UTC).isoformat(),
        }
    )
    await bridge._forward_to_clients(topic, topic, order_payload)
    assert bridge.topic_metrics[topic].dropped_count == 1
    bridge.disconnect_client.assert_awaited_once_with(ws)


@pytest.mark.asyncio
async def test_forward_to_clients_timeout_disconnects(monkeypatch: pytest.MonkeyPatch) -> None:
    """Forward to clients disconnects on timeout.

    Given: A subscription with a slow websocket,
    When: Send times out,
    Then: The client is disconnected and timeout count is incremented.
    """
    bridge = ZmqWebSocketBridgeService(connection_manager=None)
    ws: Any = DummyWebSocket()
    ws.send_text = AsyncMock(side_effect=asyncio.TimeoutError)
    topic = "market.data"
    sub = TopicSubscriptionModel(websocket=ws, throttle_ms=0, client_id="c2")
    bridge.topic_subscriptions[topic] = {sub.websocket: sub}
    bridge.topic_metrics[topic] = TopicMetricsModel()
    bridge.disconnect_client = AsyncMock()
    candle_payload = {
        "instrument": "ETH-USD",
        "exchange": "kraken",
        "timeframe": "1m",
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": 1.5,
        "volume": 100.0,
        "vwap": None,
        "trades": 10,
        "timestamp": datetime.now(tz=UTC).isoformat(),
    }
    await bridge._forward_to_clients(topic, topic, candle_payload)
    assert bridge.topic_metrics[topic].timeout_count == 1
    bridge.disconnect_client.assert_awaited_once_with(ws)


@pytest.mark.asyncio
async def test_start_zmq_subscription_missing_config_no_socket() -> None:
    """Start ZMQ subscription handles missing config.

    Given: A bridge with no available topics,
    When: Starting subscription for unknown topic,
    Then: No socket is created.
    """
    bridge = ZmqWebSocketBridgeService(connection_manager=None)
    bridge.context = AsyncMock()
    await bridge._start_zmq_subscription("unknown.topic")
    assert "unknown.topic" not in bridge.zmq_subscribers


class TestBridgeMissingBranches:
    """Tests for bridge missing branch coverage."""

    @pytest.mark.asyncio
    async def test_unsubscribe_client_topic_not_in_subscriptions(self) -> None:
        """Unsubscribe client handles topic not in subscriptions.

        Given: A client with a subscription not in topic_subscriptions,
        When: Unsubscribing from that topic,
        Then: Client subscriptions are cleaned up without error.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        ws: Any = DummyWebSocket()
        bridge.client_subscriptions[ws] = {"non_existent_topic"}
        await bridge.unsubscribe_client(ws, ["non_existent_topic"])
        assert ws not in bridge.client_subscriptions

    @pytest.mark.asyncio
    async def test_zmq_subscription_loop_json_decode_error(self) -> None:
        """Subscription loop drops invalid JSON in strict mode.

        Given: A ZMQ subscription loop receiving messages,
        When: An invalid JSON message is received,
        Then: The message is dropped and not forwarded to clients.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge.topic_metrics["test_topic"] = TopicMetricsModel()
        mock_socket = MagicMock()
        call_count = 0

        async def mock_recv_multipart() -> list[bytes]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [b"test_topic", b"invalid json {{{"]
            raise asyncio.CancelledError()

        mock_socket.recv_multipart = mock_recv_multipart
        forward_calls: list[tuple[str, str, str]] = []

        async def mock_forward(topic_name: str, received_topic: str, payload_str: str) -> None:
            forward_calls.append((topic_name, received_topic, payload_str))

        bridge._forward_to_clients = mock_forward
        with pytest.raises(asyncio.CancelledError):
            await bridge._zmq_subscription_loop("test_topic", mock_socket)
        assert len(forward_calls) == 0
        assert bridge.topic_metrics["test_topic"].invalid_messages == 1

    @pytest.mark.asyncio
    async def test_zmq_subscription_loop_unexpected_error_with_metrics(self) -> None:
        """Subscription loop increments error count on unexpected error.

        Given: A ZMQ subscription loop with metrics,
        When: An unexpected error occurs,
        Then: Error count is incremented.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge.topic_metrics["test_topic"] = TopicMetricsModel()
        mock_socket = MagicMock()
        call_count = 0

        async def mock_recv_multipart() -> list[bytes]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return [b"test_topic", b'{"data": "value", "session_id": "s1", "sequence_id": 1}']
            if call_count == 2:
                raise RuntimeError("Unexpected ZMQ failure")
            raise asyncio.CancelledError()

        mock_socket.recv_multipart = mock_recv_multipart
        bridge._forward_to_clients = AsyncMock()
        with pytest.raises(asyncio.CancelledError):
            await bridge._zmq_subscription_loop("test_topic", mock_socket)
        assert bridge.topic_metrics["test_topic"].error_count >= 1

    @pytest.mark.asyncio
    async def test_forward_to_clients_dropped_with_metrics(self) -> None:
        """Forward to clients increments dropped count on backpressure.

        Given: A subscription with max pending messages,
        When: Another message is forwarded,
        Then: Dropped count is incremented.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        ws: Any = DummyWebSocket()
        topic = "market.ticks.btc"
        sub = TopicSubscriptionModel(websocket=ws, throttle_ms=0, client_id="c1")
        sub.pending_count = 100
        bridge.topic_subscriptions[topic] = {sub.websocket: sub}
        bridge.topic_metrics[topic] = TopicMetricsModel()
        tick_payload = {
            "instrument": "BTC-USD",
            "exchange": "kraken",
            "bid": 49999.0,
            "ask": 50001.0,
            "last": 50000.0,
            "volume": 1.0,
            "timestamp": datetime.now(tz=UTC).isoformat(),
        }
        await bridge._forward_to_clients(topic, topic, tick_payload)
        assert bridge.topic_metrics[topic].dropped_count == 1

    @pytest.mark.asyncio
    async def test_forward_to_clients_timeout_with_metrics(self) -> None:
        """Forward to clients increments timeout count on timeout.

        Given: A subscription with a slow websocket,
        When: Send times out,
        Then: Timeout count is incremented.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        ws: Any = DummyWebSocket()

        async def slow_send(msg: str) -> None:
            await asyncio.sleep(10)

        ws.send_text = slow_send
        topic = "market.ticks.eth"
        sub = TopicSubscriptionModel(websocket=ws, throttle_ms=0, client_id="c2")
        bridge.topic_subscriptions[topic] = {sub.websocket: sub}
        bridge.topic_metrics[topic] = TopicMetricsModel()
        bridge.disconnect_client = AsyncMock()
        tick_payload = {
            "instrument": "ETH-USD",
            "exchange": "kraken",
            "bid": 2999.0,
            "ask": 3001.0,
            "last": 3000.0,
            "volume": 2.0,
            "timestamp": datetime.now(tz=UTC).isoformat(),
        }
        with patch.object(bridge, "_get_max_pending", return_value=1000):
            await bridge._forward_to_clients(topic, topic, tick_payload)
        assert bridge.topic_metrics[topic].timeout_count == 1

    @pytest.mark.asyncio
    async def test_start_zmq_subscriber_already_running(self) -> None:
        """Start ZMQ subscriber skips when already running.

        Given: An active subscriber task for a topic,
        When: Starting subscriber for the same topic,
        Then: The existing task is preserved.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge.context = MagicMock()

        async def dummy_task() -> None:
            await asyncio.sleep(100)

        existing_task = asyncio.create_task(dummy_task())
        bridge.subscriber_tasks["market.ticks"] = existing_task
        await bridge.start_zmq_subscriber("market.ticks")
        existing_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await existing_task

    @pytest.mark.asyncio
    async def test_start_zmq_subscriber_unknown_topic_pattern_fallback(self) -> None:
        """Start ZMQ subscriber handles unknown topic pattern.

        Given: A bridge with no matching topic pattern,
        When: Starting subscriber for unknown topic,
        Then: No socket is created.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge.context = MagicMock()
        mock_socket = MagicMock()
        bridge.context.socket.return_value = mock_socket
        await bridge.start_zmq_subscriber("completely.unknown.topic.pattern")
        assert "completely.unknown.topic.pattern" not in bridge.zmq_subscribers

    @pytest.mark.asyncio
    async def test_start_zmq_subscriber_socket_creation_error(self) -> None:
        """Start ZMQ subscriber handles socket creation error.

        Given: A ZMQ context that fails on socket creation,
        When: Starting subscriber,
        Then: No socket is tracked and error is handled.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge.context = MagicMock()
        bridge.context.socket.side_effect = zmq.ZMQError(errno=1, msg="Socket error")
        await bridge.start_zmq_subscriber("market.ticks")
        assert "market.ticks" not in bridge.zmq_subscribers

    @pytest.mark.asyncio
    async def test_forward_to_clients_timeout_increments_count(self) -> None:
        """Forward to clients increments timeout count on TimeoutError.

        Given: A subscription with a websocket that raises TimeoutError,
        When: Forwarding a message,
        Then: Timeout count is incremented in topic metrics.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        ws: Any = DummyWebSocket()
        ws.send_text.side_effect = TimeoutError()
        topic = "market.ticks"
        sub = TopicSubscriptionModel(websocket=ws, throttle_ms=0, client_id="c3")
        bridge.topic_subscriptions[topic] = {sub.websocket: sub}
        bridge.topic_metrics[topic] = TopicMetricsModel()
        tick_data_str = json.dumps(
            {
                "type": "tick",
                "instrument": "BTC-USD",
                "exchange": "kraken",
                "bid": 49999.0,
                "ask": 50001.0,
                "timestamp": datetime.now(tz=UTC).isoformat(),
            }
        )
        await bridge._forward_to_clients(topic, topic, tick_data_str)
        assert bridge.topic_metrics[topic].timeout_count == 1

    @pytest.mark.asyncio
    async def test_forward_to_clients_disconnect_on_send_error(self) -> None:
        """Forward to clients disconnects client on send error.

        Given: A subscription with a failing websocket,
        When: Forwarding a message and send fails,
        Then: Client is disconnected via disconnect_client.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        ws: Any = DummyWebSocket()
        ws.send_text.side_effect = Exception("Connection closed")
        topic = "market.ticks"
        sub = TopicSubscriptionModel(websocket=ws, throttle_ms=0, client_id="c4")
        bridge.topic_subscriptions[topic] = {sub.websocket: sub}
        bridge.topic_metrics[topic] = TopicMetricsModel(active_subscribers=1)
        bridge.disconnect_client = AsyncMock()
        tick_data_str = json.dumps(
            {
                "type": "tick",
                "instrument": "BTC-USD",
                "exchange": "kraken",
                "bid": 49999.0,
                "ask": 50001.0,
                "timestamp": datetime.now(tz=UTC).isoformat(),
            }
        )
        await bridge._forward_to_clients(topic, topic, tick_data_str)
        bridge.disconnect_client.assert_awaited_once_with(ws)

    @pytest.mark.asyncio
    async def test_unsubscribe_websocket_updates_metrics(self) -> None:
        """Verify unsubscribe updates topic metrics.

        Given: A WebSocket subscribed to a topic,
        When: Unsubscribing from the topic,
        Then: Active subscribers count is decremented.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        ws: Any = DummyWebSocket()
        topic = "market.ticks"
        sub = TopicSubscriptionModel(websocket=ws, throttle_ms=0, client_id="c5")
        bridge.topic_subscriptions[topic] = {sub.websocket: sub}
        bridge.client_subscriptions[ws] = {topic}
        bridge.topic_metrics[topic] = TopicMetricsModel(active_subscribers=1)
        bridge.stop_zmq_subscriber = AsyncMock()
        result = await bridge.unsubscribe_websocket(ws, topic)
        assert result is True
        assert bridge.topic_metrics[topic].active_subscribers == 0

    @pytest.mark.asyncio
    async def test_unsubscribe_websocket_cleans_client_subscriptions(self) -> None:
        """Verify unsubscribe cleans client subscriptions.

        Given: A WebSocket subscribed to a single topic,
        When: Unsubscribing from that topic,
        Then: Client is removed from client_subscriptions.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        ws: Any = DummyWebSocket()
        topic = "market.candles"
        sub = TopicSubscriptionModel(websocket=ws, throttle_ms=0, client_id="c6")
        bridge.topic_subscriptions[topic] = {sub.websocket: sub}
        bridge.client_subscriptions[ws] = {topic}
        bridge.topic_metrics[topic] = TopicMetricsModel(active_subscribers=1)
        bridge.stop_zmq_subscriber = AsyncMock()
        await bridge.unsubscribe_websocket(ws, topic)
        assert ws not in bridge.client_subscriptions


class TestStopZmqSubscriberCoverage:
    """Tests for stopping ZMQ subscriber."""

    @pytest.mark.asyncio
    async def test_stop_zmq_subscriber_cancels_task(self) -> None:
        """Verify stopping subscriber cancels its task.

        Given: A running subscriber task,
        When: Stopping the subscriber,
        Then: Task is cancelled and removed from tasks dict.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        topic = "test.topic"
        mock_task = MagicMock()
        bridge.subscriber_tasks[topic] = mock_task
        await bridge.stop_zmq_subscriber(topic)
        mock_task.cancel.assert_called_once()
        assert topic not in bridge.subscriber_tasks

    @pytest.mark.asyncio
    async def test_stop_zmq_subscriber_closes_socket(self) -> None:
        """Verify stopping subscriber closes its socket.

        Given: A subscriber with active socket,
        When: Stopping the subscriber,
        Then: Socket is closed and removed from subscribers dict.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        topic = "test.topic"
        mock_socket = MagicMock()
        bridge.zmq_subscribers[topic] = mock_socket
        await bridge.stop_zmq_subscriber(topic)
        mock_socket.close.assert_called_once()
        assert topic not in bridge.zmq_subscribers


class TestStopZmqSubscriptionCoverage:
    """Tests for stopping ZMQ subscription internal method."""

    @pytest.mark.asyncio
    async def test_stop_when_no_task(self) -> None:
        """Verify stop handles missing task gracefully.

        Given: A topic with no subscriber task,
        When: Stopping the subscription,
        Then: No error is raised.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        topic = "test.topic"
        await bridge._stop_zmq_subscription(topic)

    @pytest.mark.asyncio
    async def test_stop_when_no_socket(self) -> None:
        """Verify stop cancels task when no socket present.

        Given: A subscriber task but no socket,
        When: Stopping the subscription,
        Then: Task is cancelled and removed.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        topic = "test.topic"

        async def dummy_task() -> None:
            await asyncio.sleep(10)

        task = asyncio.create_task(dummy_task())
        bridge.subscriber_tasks[topic] = task
        await bridge._stop_zmq_subscription(topic)
        assert topic not in bridge.subscriber_tasks
        assert task.cancelled()


class TestHandleZmqMessagesCoverage:
    """Tests for ZMQ message handling."""

    @pytest.mark.asyncio
    async def test_handle_zmq_messages_cancelled(self) -> None:
        """Verify cancelled error exits message loop gracefully.

        Given: A ZMQ socket that raises CancelledError,
        When: Handling messages,
        Then: CancelledError propagates after logging.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        topic = "test.topic"
        config = TopicConfigurationModel(pattern=topic, endpoint="tcp://localhost:5555")
        mock_socket = MagicMock()
        mock_socket.recv_multipart = AsyncMock(side_effect=asyncio.CancelledError())
        with pytest.raises(asyncio.CancelledError):
            await bridge._handle_zmq_messages(topic, mock_socket, config)

    @pytest.mark.asyncio
    async def test_handle_zmq_messages_fatal_exception(self) -> None:
        """Verify fatal exception is logged and method exits.

        Given: A ZMQ socket that raises RuntimeError,
        When: Handling messages,
        Then: Error is logged and method returns.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        topic = "test.topic"
        config = TopicConfigurationModel(pattern=topic, endpoint="tcp://localhost:5555")
        mock_socket = MagicMock()
        mock_socket.recv_multipart = AsyncMock(side_effect=RuntimeError("Socket error"))
        with patch("snapper.interface.websocket.bridge.logger") as mock_logger:
            await bridge._handle_zmq_messages(topic, mock_socket, config)
            mock_logger.exception.assert_called()

    @pytest.mark.asyncio
    async def test_handle_zmq_messages_inner_exception(self) -> None:
        """Verify message processing error is logged.

        Given: Invalid UTF-8 encoded message data,
        When: Processing the message,
        Then: Error is logged and loop continues.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        topic = "test.topic"
        config = TopicConfigurationModel(pattern=topic, endpoint="tcp://localhost:5555")
        bad_topic = b"\xff\xfe"
        bad_data = b"\xff\xfe"
        mock_socket = MagicMock()
        mock_socket.recv_multipart = AsyncMock(
            side_effect=[(bad_topic, bad_data), asyncio.CancelledError()]
        )
        with patch("snapper.interface.websocket.bridge.logger") as mock_logger:
            with pytest.raises(asyncio.CancelledError):
                await bridge._handle_zmq_messages(topic, mock_socket, config)
            assert any(
                "Error processing ZMQ message" in str(call)
                for call in mock_logger.exception.call_args_list
            )


class TestForwardToClientsTimeoutCoverage:
    """Tests for WebSocket send timeout handling in _forward_to_clients."""

    @pytest.mark.asyncio
    async def test_send_timeout_disconnects_slow_client(self) -> None:
        """Verify timeout disconnects slow client and updates metrics.

        Given: A subscription with WebSocket that times out on send,
        When: Forwarding a message,
        Then: Client is disconnected and timeout count incremented.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        topic = "market.candles"
        mock_ws = AsyncMock()
        mock_ws.send_text = AsyncMock(side_effect=TimeoutError())
        sub = TopicSubscriptionModel(websocket=mock_ws, throttle_ms=0, client_id="slow-client")
        bridge.topic_subscriptions[topic] = {sub.websocket: sub}
        bridge.topic_metrics[topic] = TopicMetricsModel()
        bridge.client_subscriptions[mock_ws] = {topic}
        bridge.disconnect_client = AsyncMock()
        await bridge._forward_to_clients(topic, topic, '{"type":"candle"}')
        bridge.disconnect_client.assert_awaited_once_with(mock_ws)
        assert bridge.topic_metrics[topic].timeout_count == 1

    @pytest.mark.asyncio
    async def test_send_timeout_without_metrics(self) -> None:
        """Verify timeout handling works without metrics initialized.

        Given: A subscription without topic metrics,
        When: WebSocket send times out,
        Then: Client is disconnected without metrics update.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        topic = "market.candles"
        mock_ws = AsyncMock()
        mock_ws.send_text = AsyncMock(side_effect=TimeoutError())
        sub = TopicSubscriptionModel(websocket=mock_ws, throttle_ms=0, client_id="slow-client")
        bridge.topic_subscriptions[topic] = {sub.websocket: sub}
        bridge.client_subscriptions[mock_ws] = {topic}
        bridge.disconnect_client = AsyncMock()
        await bridge._forward_to_clients(topic, topic, '{"type":"candle"}')
        bridge.disconnect_client.assert_awaited_once_with(mock_ws)

    @pytest.mark.asyncio
    async def test_disconnect_during_iteration(self) -> None:
        """Verify disconnect handles concurrent subscription list modification.

        Given: Multiple subscriptions with timeouts,
        When: Disconnect modifies subscription list during iteration,
        Then: Iteration over snapshot copy handles modification gracefully.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        topic = "market.candles"
        mock_ws1 = AsyncMock()
        mock_ws1.send_text = AsyncMock(side_effect=TimeoutError())
        mock_ws2 = AsyncMock()
        mock_ws2.send_text = AsyncMock(side_effect=TimeoutError())
        sub1 = TopicSubscriptionModel(websocket=mock_ws1, throttle_ms=0, client_id="c1")
        sub2 = TopicSubscriptionModel(websocket=mock_ws2, throttle_ms=0, client_id="c2")
        bridge.topic_subscriptions[topic] = {sub1.websocket: sub1, sub2.websocket: sub2}
        bridge.topic_metrics[topic] = TopicMetricsModel(active_subscribers=2)
        bridge.client_subscriptions[mock_ws1] = {topic}
        bridge.client_subscriptions[mock_ws2] = {topic}
        call_count = [0]

        async def disconnect_clears_list(ws: Any) -> None:
            """Simulate disconnect clearing the subscription list."""
            call_count[0] += 1
            if call_count[0] == 1:
                bridge.topic_subscriptions[topic].clear()

        bridge.disconnect_client = AsyncMock(side_effect=disconnect_clears_list)
        await bridge._forward_to_clients(topic, topic, '{"type":"candle"}')
        assert bridge.disconnect_client.await_count == 2


class TestUnsubscribeWebsocketAllCoverage:
    """Tests for unsubscribe_websocket_all method coverage."""

    @pytest.mark.asyncio
    async def test_unsubscribe_all_when_no_subscriptions(self) -> None:
        """Verify unsubscribe all returns zero when client has no subscriptions.

        Given: A bridge with no subscriptions for a client,
        When: Calling unsubscribe_websocket_all,
        Then: Returns count of zero.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        ws: Any = DummyWebSocket()
        count = await bridge.unsubscribe_websocket_all(ws)
        assert count == 0

    @pytest.mark.asyncio
    async def test_unsubscribe_all_iterates_through_non_matching_subs(self) -> None:
        """Verify unsubscribe all skips non-matching subscriptions.

        Given: A topic with subscriptions from other clients,
        When: Calling unsubscribe_websocket_all for target client,
        Then: Other clients' subscriptions remain unchanged.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        ws_target: Any = DummyWebSocket()
        ws_other1: Any = DummyWebSocket()
        ws_other2: Any = DummyWebSocket()
        topic1 = "market.candles"
        bridge.topic_subscriptions[topic1] = {
            ws_other1: TopicSubscriptionModel(
                websocket=ws_other1, throttle_ms=0, client_id="other1"
            ),
            ws_other2: TopicSubscriptionModel(
                websocket=ws_other2, throttle_ms=0, client_id="other2"
            ),
        }
        bridge.topic_metrics[topic1] = TopicMetricsModel(active_subscribers=2)
        bridge.client_subscriptions[ws_other1] = {topic1}
        bridge.client_subscriptions[ws_other2] = {topic1}
        count = await bridge.unsubscribe_websocket_all(ws_target)
        assert count == 0
        assert len(bridge.topic_subscriptions[topic1]) == 2

    @pytest.mark.asyncio
    async def test_unsubscribe_all_with_other_clients_in_topic(self) -> None:
        """Verify unsubscribe all preserves other clients in shared topics.

        Given: A topic with target client and other clients subscribed,
        When: Calling unsubscribe_websocket_all for target,
        Then: Target is removed, other clients remain subscribed.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        ws_target: Any = DummyWebSocket()
        ws_other1: Any = DummyWebSocket()
        ws_other2: Any = DummyWebSocket()
        topic = "market.candles"
        bridge.topic_subscriptions[topic] = {
            ws_other1: TopicSubscriptionModel(
                websocket=ws_other1, throttle_ms=0, client_id="other1"
            ),
            ws_other2: TopicSubscriptionModel(
                websocket=ws_other2, throttle_ms=0, client_id="other2"
            ),
            ws_target: TopicSubscriptionModel(
                websocket=ws_target, throttle_ms=0, client_id="target"
            ),
        }
        bridge.client_subscriptions[ws_target] = {topic}
        bridge.client_subscriptions[ws_other1] = {topic}
        bridge.client_subscriptions[ws_other2] = {topic}
        bridge.topic_metrics[topic] = TopicMetricsModel(active_subscribers=3)
        bridge.stop_zmq_subscriber = AsyncMock()
        count = await bridge.unsubscribe_websocket_all(ws_target)
        assert count == 1
        assert len(bridge.topic_subscriptions[topic]) == 2
        client_ids = [s.client_id for s in bridge.topic_subscriptions[topic].values()]
        assert "other1" in client_ids
        assert "other2" in client_ids

    @pytest.mark.asyncio
    async def test_unsubscribe_all_logs_count(self) -> None:
        """Verify unsubscribe all logs and returns topic count.

        Given: A client subscribed to multiple topics,
        When: Calling unsubscribe_websocket_all,
        Then: Returns count and logs unsubscribe info.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        ws: Any = DummyWebSocket()
        topic1 = "market.candles"
        topic2 = "market.ticks"
        bridge.topic_subscriptions[topic1] = {
            ws: TopicSubscriptionModel(websocket=ws, throttle_ms=0)
        }
        bridge.topic_subscriptions[topic2] = {
            ws: TopicSubscriptionModel(websocket=ws, throttle_ms=0)
        }
        bridge.client_subscriptions[ws] = {topic1, topic2}
        bridge.topic_metrics[topic1] = TopicMetricsModel()
        bridge.topic_metrics[topic2] = TopicMetricsModel()
        bridge.stop_zmq_subscriber = AsyncMock()
        with patch("snapper.interface.websocket.bridge.logger") as mock_logger:
            count = await bridge.unsubscribe_websocket_all(ws)
            assert count == 2
            mock_logger.info.assert_called()


class TestUnsubscribeWebsocketStopsZmqCoverage:
    """Tests for ZMQ subscription stop on last client unsubscribe."""

    @pytest.mark.asyncio
    async def test_unsubscribe_stops_zmq_when_last_client(self) -> None:
        """Verify ZMQ subscription stops when last client unsubscribes.

        Given: A single client subscribed to a topic,
        When: Client unsubscribes from the topic,
        Then: ZMQ subscription is stopped and topic removed.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        ws: Any = DummyWebSocket()
        topic = "market.candles"
        sub = TopicSubscriptionModel(websocket=ws, throttle_ms=0, client_id="last-client")
        bridge.topic_subscriptions[topic] = {sub.websocket: sub}
        bridge.client_subscriptions[ws] = {topic}
        bridge.topic_metrics[topic] = TopicMetricsModel(active_subscribers=1)
        bridge.stop_zmq_subscriber = AsyncMock()
        await bridge.unsubscribe_websocket(ws, topic)
        bridge.stop_zmq_subscriber.assert_awaited_once_with(topic)
        assert topic not in bridge.topic_subscriptions

    @pytest.mark.asyncio
    async def test_unsubscribe_when_not_in_client_subscriptions(self) -> None:
        """Verify unsubscribe handles orphaned topic subscriptions.

        Given: A topic subscription without client tracking,
        When: Unsubscribing the WebSocket,
        Then: Returns success and cleans up topic.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        ws: Any = DummyWebSocket()
        topic = "market.candles"
        sub = TopicSubscriptionModel(websocket=ws, throttle_ms=0, client_id="orphan")
        bridge.topic_subscriptions[topic] = {sub.websocket: sub}
        bridge.topic_metrics[topic] = TopicMetricsModel(active_subscribers=1)
        bridge.stop_zmq_subscriber = AsyncMock()
        result = await bridge.unsubscribe_websocket(ws, topic)
        assert result is True
        bridge.stop_zmq_subscriber.assert_awaited_once_with(topic)
        assert topic not in bridge.topic_subscriptions


class TestStartStopCoverage:
    """Tests for bridge start and stop lifecycle."""

    @pytest.mark.asyncio
    async def test_start_creates_context(self) -> None:
        """Verify start creates ZMQ context when none exists.

        Given: A bridge with no ZMQ context,
        When: Starting the bridge,
        Then: ZMQ context is created.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge.context = None

        async def stop_after_delay() -> None:
            await asyncio.sleep(0.05)
            await bridge.stop()

        with patch("zmq.asyncio.Context") as mock_context_cls:
            mock_context_cls.return_value = MagicMock()
            await asyncio.gather(
                bridge.start(),
                stop_after_delay(),
            )
        mock_context_cls.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_reuses_existing_context(self) -> None:
        """Verify start keeps an already-created ZMQ context.

        Given: A bridge whose context was already created (same-instance
            restart),
        When: Starting the bridge,
        Then: No new ZMQ context is constructed.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge.context = MagicMock()

        async def stop_after_delay() -> None:
            await asyncio.sleep(0.05)
            await bridge.stop()

        with patch("zmq.asyncio.Context") as mock_context_cls:
            await asyncio.gather(
                bridge.start(),
                stop_after_delay(),
            )
        mock_context_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_cancellation(self) -> None:
        """Verify start handles task cancellation.

        Given: A running bridge start task,
        When: Task is cancelled,
        Then: CancelledError is raised properly.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge._shutdown_event = asyncio.Event()

        async def cancel_after_delay() -> None:
            await asyncio.sleep(0.05)
            task.cancel()

        with patch("zmq.asyncio.Context"):
            task = asyncio.create_task(bridge.start())
            cancel_task = asyncio.create_task(cancel_after_delay())
            with pytest.raises(asyncio.CancelledError):
                await task
            await cancel_task

    @pytest.mark.asyncio
    async def test_stop_when_already_stopped(self) -> None:
        """Verify stop is idempotent when already stopped.

        Given: A bridge already stopped or never started,
        When: Calling stop,
        Then: No error occurs.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge._shutdown_event = None
        await bridge.stop()
        bridge._shutdown_event = asyncio.Event()
        bridge._shutdown_event.set()
        await bridge.stop()


class MockWebSocket:
    """Mock WebSocket with identity semantics for testing."""

    def __hash__(self) -> int:
        """Magic method."""
        return id(self)

    def __eq__(self, other: object) -> bool:
        """Magic method."""
        return self is other


class TestUnsubscribeClientStopZmqSubscription:
    """Tests for unsubscribe_client ZMQ subscription management."""

    @pytest.mark.asyncio
    async def test_unsubscribe_client_stops_zmq_when_empty(self) -> None:
        """Verify ZMQ subscription stops when topic has no subscribers.

        Given: A single client subscribed to a topic,
        When: Client unsubscribes via unsubscribe_client,
        Then: ZMQ subscription is stopped for that topic.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        ws: Any = MockWebSocket()
        topic = "market.candles"
        bridge.topic_subscriptions[topic] = {
            ws: TopicSubscriptionModel(websocket=ws, throttle_ms=0)
        }
        bridge.client_subscriptions[ws] = {topic}
        bridge.topic_metrics[topic] = TopicMetricsModel(active_subscribers=1)
        bridge._stop_zmq_subscription = AsyncMock()
        await bridge.unsubscribe_client(ws, [topic])
        bridge._stop_zmq_subscription.assert_awaited_once_with(topic)
        assert topic not in bridge.topic_subscriptions

    @pytest.mark.asyncio
    async def test_unsubscribe_preserves_other_subs(self) -> None:
        """Verify unsubscribing from one topic preserves others.

        Given: A client subscribed to multiple topics,
        When: Unsubscribing from one topic,
        Then: Other topic subscriptions remain.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        ws: Any = MockWebSocket()
        topic1 = "market.candles"
        topic2 = "orders"
        bridge.topic_subscriptions[topic1] = {
            ws: TopicSubscriptionModel(websocket=ws, throttle_ms=0)
        }
        bridge.topic_subscriptions[topic2] = {
            ws: TopicSubscriptionModel(websocket=ws, throttle_ms=0)
        }
        bridge.client_subscriptions[ws] = {topic1, topic2}
        bridge.topic_metrics[topic1] = TopicMetricsModel(active_subscribers=1)
        bridge.topic_metrics[topic2] = TopicMetricsModel(active_subscribers=1)
        bridge._stop_zmq_subscription = AsyncMock()
        await bridge.unsubscribe_client(ws, [topic1])
        assert topic2 in bridge.topic_subscriptions
        assert bridge.client_subscriptions[ws] == {topic2}


class TestStartZmqSubscription:
    """Tests for _start_zmq_subscription method."""

    @pytest.mark.asyncio
    async def test_returns_early_for_unknown_topic(self) -> None:
        """Verify start returns early for unconfigured topics.

        Given: A bridge with no topic configuration,
        When: Starting ZMQ subscription for unknown topic,
        Then: No subscriber task is created.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge.available_topics = {}
        await bridge._start_zmq_subscription("unknown.topic")
        assert "unknown.topic" not in bridge.subscriber_tasks

    @pytest.mark.asyncio
    async def test_skips_if_already_subscribed(self) -> None:
        """Verify start skips already active subscriptions.

        Given: A bridge with existing subscriber task for topic,
        When: Starting subscription for same topic,
        Then: No duplicate task is created.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge.available_topics = {
            "test": TopicConfigurationModel(
                endpoint="tcp://localhost:5555",
                pattern="test",
            )
        }
        bridge.subscriber_tasks["test"] = MagicMock()
        await bridge._start_zmq_subscription("test")
        assert len(bridge.subscriber_tasks) == 1


class TestStopZmqSubscriber:
    """Tests for _stop_zmq_subscription method."""

    @pytest.mark.asyncio
    async def test_stops_and_cleans_up(self) -> None:
        """Verify stop cleans up socket and task.

        Given: A bridge with active ZMQ subscription,
        When: Stopping the subscription,
        Then: Socket is closed and task removed.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        topic = "test.topic"
        mock_socket = MagicMock()
        mock_socket.close = MagicMock()
        bridge.zmq_subscribers[topic] = mock_socket
        task = asyncio.create_task(asyncio.sleep(10))
        bridge.subscriber_tasks[topic] = task
        await bridge._stop_zmq_subscription(topic)
        assert topic not in bridge.zmq_subscribers
        assert topic not in bridge.subscriber_tasks
        task.cancel()


class TestForwardToClientsCore:
    """Tests for _forward_to_clients method."""

    @pytest.mark.asyncio
    async def test_returns_early_for_no_subscriptions(self) -> None:
        """Verify forward returns early when no subscriptions exist.

        Given: A bridge with no topic subscriptions,
        When: Forwarding a message,
        Then: Method returns without error.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge.topic_subscriptions = {}
        await bridge._forward_to_clients("unknown", "unknown", '{"type": "test"}')

    @pytest.mark.asyncio
    async def test_sends_to_subscribers(self) -> None:
        """Verify message is sent to all subscribers.

        Given: A topic with subscribed clients,
        When: Forwarding a message,
        Then: All subscribers receive the message.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        topic = "market.candles"
        mock_ws = AsyncMock()
        bridge.topic_subscriptions[topic] = {
            mock_ws: TopicSubscriptionModel(websocket=mock_ws, throttle_ms=0, last_sent=0.0)
        }
        bridge.topic_metrics[topic] = TopicMetricsModel()
        message_str = '{"type": "candle", "instrument": "BTC-USD"}'
        await bridge._forward_to_clients(topic, topic, message_str)
        mock_ws.send_text.assert_awaited_once_with(message_str)

    @pytest.mark.asyncio
    async def test_throttles_messages(self) -> None:
        """Verify throttling prevents rapid message sending.

        Given: A subscription with recent last_sent timestamp,
        When: Forwarding within throttle window,
        Then: Message is not sent.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        topic = "market.candles"
        mock_ws = AsyncMock()
        now = time.time()
        bridge.topic_subscriptions[topic] = {
            mock_ws: TopicSubscriptionModel(websocket=mock_ws, throttle_ms=1000, last_sent=now)
        }
        bridge.topic_metrics[topic] = TopicMetricsModel()
        message_str = '{"type": "candle"}'
        await bridge._forward_to_clients(topic, topic, message_str)
        mock_ws.send_text.assert_not_awaited()


class TestCleanup:
    """Tests for bridge cleanup method."""

    @pytest.mark.asyncio
    async def test_cleans_all_resources(self) -> None:
        """Verify cleanup removes all tracked resources.

        Given: A bridge with active subscriptions and tasks,
        When: Calling cleanup,
        Then: All resources are cleaned up.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        topic = "test.topic"
        bridge.topic_subscriptions[topic] = {}
        bridge.topic_metrics[topic] = TopicMetricsModel()
        task = asyncio.create_task(asyncio.sleep(10))
        bridge.subscriber_tasks[topic] = task
        mock_socket = MagicMock()
        bridge.zmq_subscribers[topic] = mock_socket
        await bridge.cleanup()
        assert len(bridge.subscriber_tasks) == 0
        task.cancel()


class TestRemoveSubscription:
    """Tests for remove_subscription method."""

    @pytest.mark.asyncio
    async def test_removes_subscription(self) -> None:
        """Verify subscription is removed from topic.

        Given: A client subscribed to a topic,
        When: Removing the subscription,
        Then: Subscription is removed and metrics updated.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge._stop_zmq_subscription = AsyncMock()
        topic = "test.topic"
        mock_ws = MagicMock()
        sub = TopicSubscriptionModel(websocket=mock_ws, throttle_ms=0)
        bridge.topic_subscriptions[topic] = {sub.websocket: sub}
        bridge.topic_metrics[topic] = TopicMetricsModel(active_subscribers=1)
        bridge.client_subscriptions[mock_ws] = {topic}
        await bridge.remove_subscription(mock_ws, [topic])
        assert len(bridge.topic_subscriptions.get(topic, [])) == 0
        assert bridge.topic_metrics[topic].active_subscribers == 0


class TestRemoveClient:
    """Tests for remove_client method."""

    @pytest.mark.asyncio
    async def test_removes_client_from_topic(self) -> None:
        """Verify client is removed from all topics.

        Given: A client subscribed to a topic,
        When: Removing the client,
        Then: Client subscription is removed from topic.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge._stop_zmq_subscription = AsyncMock()
        topic = "test.topic"
        mock_ws = MagicMock()
        sub = TopicSubscriptionModel(websocket=mock_ws, throttle_ms=0)
        bridge.topic_subscriptions[topic] = {sub.websocket: sub}
        bridge.client_subscriptions[mock_ws] = {topic}
        await bridge.remove_client(mock_ws)
        assert len(bridge.topic_subscriptions.get(topic, [])) == 0


class TestSubscribeWebsocketBranchCoverage:
    """Tests for subscribe_client branch coverage."""

    @pytest.mark.asyncio
    async def test_creates_new_subscription(self) -> None:
        """Verify new subscription is created for topic.

        Given: A bridge with available topic configuration,
        When: Subscribing a client to the topic,
        Then: Topic subscription is created.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge._start_zmq_subscription = AsyncMock()
        bridge.available_topics = {
            "market.": TopicConfigurationModel(
                endpoint="tcp://localhost:5555",
                pattern="market.",
            )
        }
        mock_ws = MagicMock()
        await bridge.subscribe_client(mock_ws, ["market."])
        assert "market." in bridge.topic_subscriptions


class TestUnsubscribeWebsocketBranchCoverage:
    """Tests for unsubscribe_client branch coverage."""

    @pytest.mark.asyncio
    async def test_handles_missing_subscription(self) -> None:
        """Verify unsubscribe handles non-existent subscription.

        Given: A bridge with no subscriptions,
        When: Unsubscribing from non-existent topic,
        Then: No error occurs.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        mock_ws = MagicMock()
        await bridge.unsubscribe_client(mock_ws, ["nonexistent"])


class TestUnsubscribeWebsocketAllBranchCoverage:
    """Tests for unsubscribe_websocket_all branch coverage."""

    @pytest.mark.asyncio
    async def test_unsubscribes_all_topics(self) -> None:
        """Verify all topics are unsubscribed.

        Given: A client subscribed to multiple topics,
        When: Calling unsubscribe_websocket_all,
        Then: All subscriptions removed and count returned.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        mock_ws = MagicMock()
        bridge._stop_zmq_subscription = AsyncMock()
        topic1 = "market."
        topic2 = "orders"
        bridge.topic_subscriptions[topic1] = {
            mock_ws: TopicSubscriptionModel(websocket=mock_ws, throttle_ms=0)
        }
        bridge.topic_subscriptions[topic2] = {
            mock_ws: TopicSubscriptionModel(websocket=mock_ws, throttle_ms=0)
        }
        bridge.client_subscriptions[mock_ws] = {topic1, topic2}
        bridge.topic_metrics[topic1] = TopicMetricsModel()
        bridge.topic_metrics[topic2] = TopicMetricsModel()
        count = await bridge.unsubscribe_websocket_all(mock_ws)
        assert mock_ws not in bridge.client_subscriptions
        assert count == 2


class TestForwardToClientsBackpressure:
    """Tests for backpressure handling in _forward_to_clients."""

    @pytest.mark.asyncio
    async def test_drops_message_on_backpressure(self) -> None:
        """Verify message dropped when backpressure limit reached.

        Given: A subscription at maximum pending messages,
        When: Forwarding another message,
        Then: Client is disconnected due to backpressure.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge.disconnect_client = AsyncMock()
        topic = "orders.commands."
        mock_ws = AsyncMock()
        sub = TopicSubscriptionModel(
            websocket=mock_ws,
            throttle_ms=0,
            pending_count=MAX_PENDING_MESSAGES_TRADE,
        )
        bridge.topic_subscriptions[topic] = {sub.websocket: sub}
        bridge.topic_metrics[topic] = TopicMetricsModel()
        message_str = '{"type": "order_request", "public_id": "123"}'
        await bridge._forward_to_clients(topic, topic, message_str)
        bridge.disconnect_client.assert_awaited_once()


class TestForwardToClientsThrottling:
    """Tests for message throttling in _forward_to_clients."""

    @pytest.mark.asyncio
    async def test_throttles_messages(self) -> None:
        """Verify throttling prevents sending within window.

        Given: A subscription with recent send timestamp,
        When: Forwarding another message within throttle window,
        Then: Message is throttled and count incremented.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        topic = "market.candles"
        mock_ws = AsyncMock()
        now = time.time()
        sub = TopicSubscriptionModel(
            websocket=mock_ws,
            throttle_ms=1000,
            last_sent=now,
        )
        bridge.topic_subscriptions[topic] = {sub.websocket: sub}
        bridge.topic_metrics[topic] = TopicMetricsModel()
        message_str = '{"type": "candle"}'
        await bridge._forward_to_clients(topic, topic, message_str)
        mock_ws.send_text.assert_not_awaited()
        assert bridge.topic_metrics[topic].throttled_count == 1


class TestForwardToClientsExceptionHandling:
    """Tests for exception handling in _forward_to_clients."""

    @pytest.mark.asyncio
    async def test_handles_send_failure(self) -> None:
        """Verify send failure disconnects client.

        Given: A subscription with WebSocket that fails on send,
        When: Forwarding a message,
        Then: Client is disconnected.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge.disconnect_client = AsyncMock()
        topic = "market.candles"
        mock_ws = AsyncMock()
        mock_ws.send_text.side_effect = Exception("Send failed")
        sub = TopicSubscriptionModel(websocket=mock_ws, throttle_ms=0, last_sent=0.0)
        bridge.topic_subscriptions[topic] = {sub.websocket: sub}
        bridge.topic_metrics[topic] = TopicMetricsModel()
        message_str = '{"type": "candle"}'
        await bridge._forward_to_clients(topic, topic, message_str)
        bridge.disconnect_client.assert_awaited_once_with(mock_ws)


class TestForwardToClientsBranchCoverage:
    """Tests for _forward_to_clients branch coverage."""

    @pytest.mark.asyncio
    async def test_updates_last_sent_on_success(self) -> None:
        """Verify last_sent timestamp updated on successful send.

        Given: A subscription with zero last_sent,
        When: Successfully forwarding a message,
        Then: last_sent timestamp is updated.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        topic = "market.candles"
        mock_ws = AsyncMock()
        sub = TopicSubscriptionModel(websocket=mock_ws, throttle_ms=0, last_sent=0.0)
        bridge.topic_subscriptions[topic] = {sub.websocket: sub}
        bridge.topic_metrics[topic] = TopicMetricsModel()
        message_str = '{"type": "candle"}'
        await bridge._forward_to_clients(topic, topic, message_str)
        assert sub.last_sent > 0

    @pytest.mark.asyncio
    async def test_handles_timeout(self) -> None:
        """Verify timeout error disconnects client.

        Given: A subscription with WebSocket that times out,
        When: Forwarding a message,
        Then: Client is disconnected and timeout count updated.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge.disconnect_client = AsyncMock()
        topic = "market.candles"
        mock_ws = AsyncMock()
        mock_ws.send_text.side_effect = TimeoutError()
        sub = TopicSubscriptionModel(websocket=mock_ws, throttle_ms=0, last_sent=0.0)
        bridge.topic_subscriptions[topic] = {sub.websocket: sub}
        bridge.topic_metrics[topic] = TopicMetricsModel()
        message_str = '{"type": "candle"}'
        await bridge._forward_to_clients(topic, topic, message_str)
        bridge.disconnect_client.assert_awaited_once()
        assert bridge.topic_metrics[topic].timeout_count == 1


class TestZmqSubscriptionLoopBranchCoverage:
    """Tests for _zmq_subscription_loop branch coverage."""

    @pytest.mark.asyncio
    async def test_received_message_without_metrics(self) -> None:
        """Verify message processing works without metrics.

        Given: A bridge with no topic metrics initialized,
        When: Receiving a valid stamped ZMQ message,
        Then: Message is forwarded without error.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge._forward_to_clients = AsyncMock()
        topic = "market.BTC"
        mock_socket = MagicMock(spec=zmq.asyncio.Socket)
        valid_message = [
            topic.encode(),
            b'{"type": "candle", "open": 100, "session_id": "s1", "sequence_id": 1}',
        ]
        mock_socket.recv_multipart = AsyncMock(
            side_effect=[valid_message, asyncio.CancelledError()]
        )
        with pytest.raises(asyncio.CancelledError):
            await bridge._zmq_subscription_loop(topic, mock_socket)
        bridge._forward_to_clients.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_json_error_without_metrics(self) -> None:
        """Verify JSON error handling without metrics.

        Given: A bridge with no topic metrics,
        When: Receiving message with invalid JSON,
        Then: Error is handled without exception.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        topic = "market.BTC"
        mock_socket = MagicMock(spec=zmq.asyncio.Socket)
        invalid_json_message = [topic.encode(), b"not valid json"]
        mock_socket.recv_multipart = AsyncMock(
            side_effect=[invalid_json_message, asyncio.CancelledError()]
        )
        with pytest.raises(asyncio.CancelledError):
            await bridge._zmq_subscription_loop(topic, mock_socket)

    @pytest.mark.asyncio
    async def test_unexpected_error_without_metrics(self) -> None:
        """Verify unexpected error handling without metrics.

        Given: A bridge with no topic metrics,
        When: Receiving RuntimeError from socket,
        Then: Error is handled and loop continues.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        topic = "market.BTC"
        mock_socket = MagicMock(spec=zmq.asyncio.Socket)
        mock_socket.recv_multipart = AsyncMock(
            side_effect=[RuntimeError("Unexpected"), asyncio.CancelledError()]
        )
        with pytest.raises(asyncio.CancelledError):
            await bridge._zmq_subscription_loop(topic, mock_socket)


class TestStartZmqSubscriberBranchCoverage:
    """Tests for start_zmq_subscriber branch coverage."""

    @pytest.mark.asyncio
    async def test_unknown_topic_no_config(self) -> None:
        """Verify start handles unknown topic gracefully.

        Given: A bridge with no topic configuration,
        When: Starting subscriber for unknown topic,
        Then: Returns without error.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge.available_topics = {}
        await bridge.start_zmq_subscriber("unknown.topic.that.doesnt.exist")


class TestHandleZmqMessagesBranchCoverage:
    """Tests for _handle_zmq_messages branch coverage."""

    @pytest.mark.asyncio
    async def test_forwards_raw_json(self) -> None:
        """Verify raw JSON is forwarded without modification.

        Given: A bridge receiving ZMQ message with JSON payload,
        When: Handling the message,
        Then: Raw JSON string is forwarded to websockets.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge._process_zmq_message = AsyncMock()
        topic = "market.BTC"
        config = TopicConfigurationModel(
            endpoint="tcp://localhost:5555",
            pattern=topic,
        )
        mock_socket = MagicMock(spec=zmq.asyncio.Socket)
        raw_json = '{"type": "candle", "open": 100}'
        valid_message = [topic.encode(), raw_json.encode()]
        mock_socket.recv_multipart = AsyncMock(
            side_effect=[valid_message, asyncio.CancelledError()]
        )
        with pytest.raises(asyncio.CancelledError):
            await bridge._handle_zmq_messages(topic, mock_socket, config)
        bridge._process_zmq_message.assert_awaited_once()
        args = bridge._process_zmq_message.call_args[0]
        assert args[0] == topic
        assert args[1] == valid_message


class TestCheckGap:
    """Tests for ZmqWebSocketBridgeService._check_gap typed envelope parsing."""

    def test_valid_envelope_calls_gap_detector(self) -> None:
        """Valid provenance envelope reaches the gap detector and returns True.

        Given: A JSON payload with session_id and sequence_id,
        When: _check_gap is called,
        Then: GapDetector.check is called with the correct values and True is returned.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge.topic_metrics["market.ticks"] = TopicMetricsModel()
        payload = '{"type":"tick","session_id":"abc","sequence_id":5}'
        with patch.object(bridge._gap_detector, "check") as mock_check:
            result = bridge._check_gap("market.ticks", "market.kraken.BTC-USD.ticks", payload)
        mock_check.assert_called_once_with(
            "market.kraken.BTC-USD.ticks", "abc", 5, wallet_public_id=""
        )
        assert bridge.topic_metrics["market.ticks"].invalid_messages == 0
        assert result is True

    def test_missing_provenance_fields_rejects_unstamped(self) -> None:
        """Envelope without provenance fields is rejected as unstamped.

        Given: A valid JSON payload without session_id or sequence_id,
        When: _check_gap is called,
        Then: GapDetector.check rejects the message, False is returned,
            and invalid_messages is incremented.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge.topic_metrics["market.ticks"] = TopicMetricsModel()
        payload = '{"type":"tick"}'
        result = bridge._check_gap("market.ticks", "market.kraken.BTC-USD.ticks", payload)
        assert result is False
        assert bridge.topic_metrics["market.ticks"].invalid_messages == 1

    def test_unstamped_without_metrics_does_not_raise(self) -> None:
        """Unstamped message without metrics entry does not raise, returns False.

        Given: A valid JSON payload without provenance and no metrics entry,
        When: _check_gap is called,
        Then: False is returned without exception.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        payload = '{"type":"tick"}'
        result = bridge._check_gap("market.ticks", "market.kraken.BTC-USD.ticks", payload)
        assert result is False

    def test_malformed_json_logs_warning_and_increments_counter(self) -> None:
        """Malformed JSON logs a warning, increments the counter, and returns False.

        Given: A payload that is not valid JSON,
        When: _check_gap is called,
        Then: Warning is logged, invalid_messages incremented, gap detector not called, False returned.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge.topic_metrics["market.ticks"] = TopicMetricsModel()
        with (
            patch.object(bridge._gap_detector, "check") as mock_check,
            patch("snapper.interface.websocket.bridge.logger") as mock_logger,
        ):
            result = bridge._check_gap("market.ticks", "market.kraken.BTC-USD.ticks", "not-json{{{")
        mock_check.assert_not_called()
        mock_logger.warning.assert_called_once()
        assert bridge.topic_metrics["market.ticks"].invalid_messages == 1
        assert result is False

    def test_malformed_json_without_metrics_does_not_raise(self) -> None:
        """Malformed JSON without a matching metrics entry does not raise, returns False.

        Given: A payload that is not valid JSON and no metrics entry for the topic,
        When: _check_gap is called,
        Then: Warning is logged, no exception raised, False returned.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        with patch("snapper.interface.websocket.bridge.logger") as mock_logger:
            result = bridge._check_gap("market.ticks", "market.kraken.BTC-USD.ticks", "{{invalid}}")
        mock_logger.warning.assert_called_once()
        assert result is False

    @pytest.mark.asyncio
    async def test_malformed_json_drops_message(self) -> None:
        """Message with malformed JSON is dropped and not forwarded to clients.

        Given: A bridge with a subscription and a malformed payload,
        When: _process_zmq_message processes the message,
        Then: _forward_to_clients is never called (strict drop mode).
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge.topic_metrics["market.ticks"] = TopicMetricsModel()
        forwarded: list[tuple[str, str, str]] = []

        async def capture_forward(topic_name: str, received: str, payload: str) -> None:
            forwarded.append((topic_name, received, payload))

        bridge._forward_to_clients = capture_forward
        await bridge._process_zmq_message(
            "market.ticks",
            [b"market.kraken.BTC-USD.ticks", b"{{not-json}}"],
        )
        assert len(forwarded) == 0

    @pytest.mark.asyncio
    async def test_valid_json_is_forwarded(self) -> None:
        """Message with valid JSON payload is forwarded to clients.

        Given: A bridge with a subscription and a valid JSON payload,
        When: _process_zmq_message processes the message,
        Then: _forward_to_clients is called with the raw payload string.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge.topic_metrics["market.ticks"] = TopicMetricsModel()
        forwarded: list[tuple[str, str, str]] = []

        async def capture_forward(topic_name: str, received: str, payload: str) -> None:
            forwarded.append((topic_name, received, payload))

        bridge._forward_to_clients = capture_forward
        payload = '{"type":"tick","session_id":"s1","sequence_id":1}'
        await bridge._process_zmq_message(
            "market.ticks",
            [b"market.kraken.BTC-USD.ticks", payload.encode()],
        )
        assert len(forwarded) == 1
        assert forwarded[0][2] == payload


class TestForwardToClientsRoutingMismatch:
    """Tests for defense-in-depth routing mismatch detection in _forward_to_clients."""

    @pytest.mark.asyncio
    async def test_root_pattern_mismatch_drops_message(self) -> None:
        """Root-pattern subscription drops message when received_topic does not match.

        Given: A root-pattern subscription ``market.`` with metrics,
        When: Forwarding a message whose received_topic does NOT start with ``market.``,
        Then: Message is dropped and invalid_messages counter increments.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        ws: Any = DummyWebSocket()
        topic = "market."
        bridge.topic_subscriptions[topic] = {
            ws: TopicSubscriptionModel(websocket=ws, throttle_ms=0),
        }
        bridge.topic_metrics[topic] = TopicMetricsModel()
        await bridge._forward_to_clients(topic, "signals.paper.FOO", '{"type":"candle"}')
        ws.send_text.assert_not_called()
        assert bridge.topic_metrics[topic].invalid_messages == 1

    @pytest.mark.asyncio
    async def test_root_pattern_mismatch_without_metrics(self) -> None:
        """Root-pattern mismatch drops message even when no metrics entry exists.

        Given: A root-pattern subscription ``market.`` without a metrics entry,
        When: Forwarding a mismatched message,
        Then: Message is dropped without error.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        ws: Any = DummyWebSocket()
        topic = "market."
        bridge.topic_subscriptions[topic] = {
            ws: TopicSubscriptionModel(websocket=ws, throttle_ms=0),
        }
        await bridge._forward_to_clients(topic, "signals.paper.FOO", '{"type":"candle"}')
        ws.send_text.assert_not_called()

    @pytest.mark.asyncio
    async def test_exact_topic_mismatch_drops_message(self) -> None:
        """Exact-topic subscription drops message when received_topic differs.

        Given: An exact-topic subscription ``market.kraken.BTC-USD.ticks`` with metrics,
        When: Forwarding a message with a different received_topic,
        Then: Message is dropped and invalid_messages counter increments.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        ws: Any = DummyWebSocket()
        topic = "market.kraken.BTC-USD.ticks"
        bridge.topic_subscriptions[topic] = {
            ws: TopicSubscriptionModel(websocket=ws, throttle_ms=0),
        }
        bridge.topic_metrics[topic] = TopicMetricsModel()
        await bridge._forward_to_clients(topic, "market.kraken.ETH-USD.ticks", '{"type":"tick"}')
        ws.send_text.assert_not_called()
        assert bridge.topic_metrics[topic].invalid_messages == 1

    @pytest.mark.asyncio
    async def test_exact_topic_mismatch_without_metrics(self) -> None:
        """Exact-topic mismatch drops message even when no metrics entry exists.

        Given: An exact-topic subscription without a metrics entry,
        When: Forwarding a mismatched message,
        Then: Message is dropped without error.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        ws: Any = DummyWebSocket()
        topic = "market.kraken.BTC-USD.ticks"
        bridge.topic_subscriptions[topic] = {
            ws: TopicSubscriptionModel(websocket=ws, throttle_ms=0),
        }
        await bridge._forward_to_clients(topic, "market.kraken.ETH-USD.ticks", '{"type":"tick"}')
        ws.send_text.assert_not_called()


class TestBridgeIntermediatePrefixDefense:
    """Defense-in-depth: bridge rejects intermediate prefix subscriptions."""

    @pytest.mark.asyncio
    async def test_subscribe_client_skips_intermediate_prefix(self) -> None:
        """subscribe_client silently skips intermediate prefixes.

        Given: A bridge with no prior subscriptions,
        When: subscribe_client is called with an intermediate prefix,
        Then: The prefix is not added to topic_subscriptions.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        bridge.context = MagicMock()
        ws = DummyWebSocket()
        await bridge.subscribe_client(ws, ["market.kraken."])
        assert "market.kraken." not in bridge.topic_subscriptions
        assert ws not in bridge.client_subscriptions

    @pytest.mark.asyncio
    async def test_subscribe_client_accepts_registry_root(self) -> None:
        """subscribe_client accepts valid registry root prefixes.

        Given: A bridge with ZMQ context,
        When: subscribe_client is called with a registry root,
        Then: The subscription is registered.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        mock_ctx = MagicMock()
        mock_socket = MagicMock()
        mock_socket.connect = MagicMock()
        mock_socket.setsockopt = MagicMock()
        mock_ctx.socket = MagicMock(return_value=mock_socket)
        bridge.context = mock_ctx
        ws = DummyWebSocket()
        await bridge.subscribe_client(ws, ["market."])
        assert "market." in bridge.topic_subscriptions

    @pytest.mark.asyncio
    async def test_subscribe_websocket_rejects_intermediate_prefix(self) -> None:
        """subscribe_websocket returns False for intermediate prefixes.

        Given: A bridge with connection_manager,
        When: subscribe_websocket is called with an intermediate prefix,
        Then: Returns False and records a control event.
        """
        mock_cm = MagicMock()
        bridge = ZmqWebSocketBridgeService(connection_manager=mock_cm)
        bridge._record_bridge_control = AsyncMock()
        ws = DummyWebSocket()
        result = await bridge.subscribe_websocket(ws, "market.kraken.")
        assert result is False
        bridge._record_bridge_control.assert_awaited_once()


class TestAiReviewScopeFilterWiring:
    """Bridge calls enforce_ai_review_scope per-subscription."""

    def _make_bridge_with_subscription(self) -> tuple[Any, Any]:
        """Build a bridge with one subscription on the ai_reviews family."""
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        ws: Any = DummyWebSocket()
        bridge.topic_subscriptions["ai_reviews."] = {
            ws: TopicSubscriptionModel(websocket=ws, throttle_ms=0)
        }
        bridge.topic_metrics["ai_reviews."] = TopicMetricsModel()
        return bridge, ws

    @pytest.mark.asyncio
    async def test_maybe_parse_returns_none_for_non_ai_review_topic(self) -> None:
        """Non-``ai_reviews.*`` topic short-circuits parser to None.

        Given: A bridge instance,
        When: _maybe_parse_ai_review_payload is called with topic 'market.',
        Then: Returns None without parsing the payload.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        result = bridge._maybe_parse_ai_review_payload("market.kraken.BTC", '{"x": 1}')
        assert result is None

    @pytest.mark.asyncio
    async def test_maybe_parse_returns_dict_for_valid_ai_review_payload(self) -> None:
        """Valid JSON dict on ``ai_reviews.*`` -> parsed dict.

        Given: A bridge instance,
        When: _maybe_parse_ai_review_payload is called with valid JSON,
        Then: Returns the parsed dict.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        result = bridge._maybe_parse_ai_review_payload(
            "ai_reviews.user-1.strat-1.request",
            '{"wallet_public_id": "wal-A", "instrument_public_id": "inst-A"}',
        )
        assert result == {"wallet_public_id": "wal-A", "instrument_public_id": "inst-A"}

    @pytest.mark.asyncio
    async def test_maybe_parse_returns_none_for_malformed_json(self) -> None:
        """Malformed JSON on ``ai_reviews.*`` -> None (defensive).

        Given: A bridge instance,
        When: _maybe_parse_ai_review_payload is called with bad JSON,
        Then: Returns None instead of raising.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        result = bridge._maybe_parse_ai_review_payload(
            "ai_reviews.user-1.strat-1.request",
            "not-json",
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_maybe_parse_returns_none_for_non_dict_payload(self) -> None:
        """JSON array on ``ai_reviews.*`` -> None (envelope must be a dict).

        Given: A bridge instance,
        When: _maybe_parse_ai_review_payload is called with a JSON list,
        Then: Returns None.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager=None)
        result = bridge._maybe_parse_ai_review_payload(
            "ai_reviews.user-1.strat-1.request",
            "[1, 2, 3]",
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_forward_drops_ai_review_frame_when_scope_check_denies(self) -> None:
        """Bridge skips send when enforce_ai_review_scope returns False.

        Given: A bridge with one ai_reviews subscription,
        When: _forward_to_clients is called and the scope filter denies,
        Then: send_text is NOT called on the WebSocket.
        """
        bridge, ws = self._make_bridge_with_subscription()
        with patch(
            "snapper.interface.websocket.bridge.enforce_ai_review_scope",
            new=AsyncMock(return_value=False),
        ):
            await bridge._forward_to_clients(
                "ai_reviews.",
                "ai_reviews.user-1.strat-1.request",
                json.dumps({"wallet_public_id": "wal-A", "instrument_public_id": "inst-A"}),
            )
        ws.send_text.assert_not_called()

    @pytest.mark.asyncio
    async def test_forward_drops_malformed_ai_review_frame_before_iterating(self) -> None:
        """Malformed ``ai_reviews.*`` frame -> early return, never iterates subs.

        Defense-in-depth fix. ``_check_gap`` already
        rejects malformed JSON before forwarding in production, but
        if a non-dict envelope ever reached ``_forward_to_clients``
        it would otherwise bypass the per-frame scope check (which is
        only invoked when ``ai_review_payload is not None``). The
        bridge now explicitly drops the frame and logs a warning.

        Given: A bridge with one ai_reviews subscription,
        When: ``_forward_to_clients`` is called with a JSON array body,
        Then: ``send_text`` is NOT called AND
        ``enforce_ai_review_scope`` is NOT consulted (early return).
        """
        bridge, ws = self._make_bridge_with_subscription()
        with patch(
            "snapper.interface.websocket.bridge.enforce_ai_review_scope",
            new=AsyncMock(),
        ) as filter_mock:
            await bridge._forward_to_clients(
                "ai_reviews.",
                "ai_reviews.user-1.strat-1.request",
                "[1, 2, 3]",
            )
        ws.send_text.assert_not_called()
        filter_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_forward_sends_ai_review_frame_when_scope_check_allows(self) -> None:
        """Bridge sends frame when enforce_ai_review_scope returns True.

        Given: A bridge with one ai_reviews subscription,
        When: _forward_to_clients is called and the scope filter allows,
        Then: send_text is called with the raw JSON message.
        """
        bridge, ws = self._make_bridge_with_subscription()
        raw_json = json.dumps({"wallet_public_id": "wal-A", "instrument_public_id": "inst-A"})
        with patch(
            "snapper.interface.websocket.bridge.enforce_ai_review_scope",
            new=AsyncMock(return_value=True),
        ):
            await bridge._forward_to_clients(
                "ai_reviews.",
                "ai_reviews.user-1.strat-1.request",
                raw_json,
            )
        ws.send_text.assert_awaited_once_with(raw_json)
