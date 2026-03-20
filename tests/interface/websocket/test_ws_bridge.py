"""Tests for ZMQ-WebSocket bridge service."""

import asyncio
import contextlib
import json
import time
from collections.abc import Coroutine
from collections.abc import Generator
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import PropertyMock
from unittest.mock import patch

import pytest
import zmq
import zmq.asyncio
from fastapi import WebSocket

from snapper.config.app import AppSettings
from snapper.interface.websocket.bridge import MAX_PENDING_MESSAGES_MARKET
from snapper.interface.websocket.bridge import MAX_PENDING_MESSAGES_TRADE
from snapper.interface.websocket.bridge import ZmqWebSocketBridgeService
from snapper.interface.websocket.models import ConnectionStats
from snapper.interface.websocket.models import TopicConfigurationModel
from snapper.interface.websocket.models import TopicMetricsModel
from snapper.interface.websocket.models import TopicMetricSnapshot
from snapper.interface.websocket.models import TopicSubscriptionModel
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import OrderData


@pytest.fixture
def bridge() -> ZmqWebSocketBridgeService:
    """Provide ZmqWebSocketBridgeService with mock settings."""
    mock_settings = MagicMock()
    mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
    mock_cm = MagicMock()
    type(mock_cm).tracker = PropertyMock(return_value=SequenceTracker())
    with patch("snapper.interface.websocket.bridge.get_settings", return_value=mock_settings):
        instance = ZmqWebSocketBridgeService(mock_cm)
    instance.context = MagicMock()
    instance.available_topics = {
        "market.candles.": TopicConfigurationModel(
            endpoint="tcp://127.0.0.1:5555",
            pattern="market.candles.",
            throttle_ms=100,
        ),
        "orders": TopicConfigurationModel(
            endpoint="tcp://127.0.0.1:5556",
            pattern="orders",
            throttle_ms=100,
        ),
    }
    return instance


@pytest.mark.asyncio
async def test_unsubscribe_updates_metrics_and_stops(bridge: ZmqWebSocketBridgeService) -> None:
    """Unsubscribe updates metrics and stops ZMQ subscription.

    Given: A client subscribed to a topic,
    When: The client unsubscribes from the topic,
    Then: Metrics are updated and ZMQ subscription is stopped.
    """
    websocket = MagicMock()
    with patch.object(bridge, "_start_zmq_subscription", new_callable=AsyncMock):
        await bridge.subscribe_client(websocket, ["market.candles."])
    bridge.topic_metrics["market.candles."].active_subscribers = len(
        bridge.topic_subscriptions["market.candles."]
    )
    with patch.object(bridge, "_stop_zmq_subscription", new_callable=AsyncMock) as stop_mock:
        await bridge.unsubscribe_client(websocket, ["market.candles."])
    stop_mock.assert_awaited_once_with("market.candles.")
    assert "market.candles." not in bridge.topic_subscriptions


@pytest.mark.asyncio
async def test_unsubscribe_skips_metrics_when_missing(
    bridge: ZmqWebSocketBridgeService,
) -> None:
    """Unsubscribe skips metrics update when metrics are missing.

    Given: A client subscribed to a topic without metrics tracking,
    When: The client unsubscribes from the topic,
    Then: Unsubscribe completes without error and subscription is cleaned up.
    """
    topic = "market.candles."
    websocket = MagicMock()
    bridge.client_subscriptions[websocket] = {topic}
    bridge.topic_subscriptions[topic] = [TopicSubscriptionModel(websocket=websocket)]
    with patch.object(bridge, "_stop_zmq_subscription", new_callable=AsyncMock) as stop_mock:
        await bridge.unsubscribe_client(websocket, [topic])
    stop_mock.assert_awaited_once_with(topic)
    assert topic not in bridge.topic_metrics
    assert topic not in bridge.topic_subscriptions
    assert websocket not in bridge.client_subscriptions


@pytest.mark.asyncio
async def test_stop_zmq_subscription_self_task(bridge: ZmqWebSocketBridgeService) -> None:
    """Stop ZMQ subscription handles self-cancellation.

    Given: A running subscriber task for a topic,
    When: The task attempts to stop itself,
    Then: The task is cancelled and removed from tracking.
    """
    topic = "orders"
    running_task = asyncio.create_task(asyncio.sleep(0.1))
    bridge.subscriber_tasks[topic] = running_task
    bridge.zmq_subscribers[topic] = MagicMock()
    with patch("asyncio.current_task", return_value=running_task):
        await bridge._stop_zmq_subscription(topic)
    assert topic not in bridge.subscriber_tasks
    with contextlib.suppress(asyncio.CancelledError):
        await running_task
    assert running_task.cancelled()


@pytest.mark.asyncio
async def test_subscription_loop_invalid_format_continues(
    bridge: ZmqWebSocketBridgeService,
) -> None:
    """Subscription loop continues after receiving invalid message format.

    Given: A ZMQ subscription loop receiving messages,
    When: An invalid message format is received,
    Then: The loop continues without incrementing received count.
    """
    topic = "market.candles."
    bridge.topic_metrics[topic] = TopicMetricsModel()
    fake_socket = MagicMock()
    fake_socket.recv_multipart = AsyncMock(
        side_effect=[
            [topic.encode()],
            asyncio.CancelledError(),
        ]
    )
    with pytest.raises(asyncio.CancelledError):
        await bridge._zmq_subscription_loop(topic, fake_socket)
    assert bridge.topic_metrics[topic].received_count == 0


@pytest.mark.asyncio
async def test_subscription_loop_fatal_error_path(bridge: ZmqWebSocketBridgeService) -> None:
    """Subscription loop logs fatal error when sleep fails.

    Given: A ZMQ subscription loop encountering an error,
    When: Error recovery sleep also fails,
    Then: The fatal error is logged.
    """
    topic = "market.candles."
    fake_socket = MagicMock()
    fake_socket.recv_multipart = AsyncMock(side_effect=zmq.ZMQError(zmq.EAGAIN))
    with (
        patch(
            "snapper.interface.websocket.bridge.asyncio.sleep",
            AsyncMock(side_effect=RuntimeError("sleep fail")),
        ),
        patch("snapper.interface.websocket.bridge.logger") as mock_logger,
    ):
        await bridge._zmq_subscription_loop(topic, fake_socket)
    mock_logger.error.assert_any_call(f"Fatal error in subscription loop for {topic}: sleep fail")


def _make_candle_json() -> str:
    return json.dumps(
        {
            "type": "candle",
            "instrument": "BTCUSD",
            "exchange": "kraken",
            "timestamp": datetime.now(tz=UTC).isoformat(),
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 10.0,
            "timeframe": "1m",
        }
    )


def _make_order_json() -> str:
    return json.dumps(
        {
            "type": "order",
            "public_id": "1",
            "instrument": "BTCUSD",
            "exchange": "kraken",
            "side": "buy",
            "size": 1.0,
            "price": 10.0,
            "status": "filled",
            "created_at": datetime.now(tz=UTC).isoformat(),
            "order_type": "limit",
        }
    )


@pytest.mark.asyncio
async def test_forward_to_clients_trade_backpressure(
    bridge: ZmqWebSocketBridgeService,
) -> None:
    """Forward to clients disconnects on trade backpressure.

    Given: A trade topic subscription with max pending messages,
    When: Another message is forwarded,
    Then: The client is disconnected and message is dropped.
    """
    topic = "orders.events."
    subscription = TopicSubscriptionModel(
        websocket=AsyncMock(),
        throttle_ms=0,
        last_sent=0.0,
        client_id="trade",
        pending_count=MAX_PENDING_MESSAGES_TRADE,
    )
    bridge.topic_subscriptions[topic] = [subscription]
    bridge.topic_metrics[topic] = TopicMetricsModel()
    with patch.object(bridge, "disconnect_client", new_callable=AsyncMock) as disconnect_mock:
        await bridge._forward_to_clients(topic, topic, _make_order_json())
    disconnect_mock.assert_awaited_once()
    assert bridge.topic_metrics[topic].dropped_count == 1
    assert subscription.pending_count == MAX_PENDING_MESSAGES_TRADE


@pytest.mark.asyncio
async def test_forward_to_clients_trade_backpressure_without_metrics(
    bridge: ZmqWebSocketBridgeService,
) -> None:
    """Forward to clients handles backpressure without metrics.

    Given: A trade topic subscription with max pending but no metrics,
    When: Another message is forwarded,
    Then: The client is disconnected without metrics update.
    """
    topic = "orders.events."
    subscription = TopicSubscriptionModel(
        websocket=AsyncMock(),
        throttle_ms=0,
        last_sent=0.0,
        client_id="trade-nometrics",
        pending_count=MAX_PENDING_MESSAGES_TRADE,
    )
    bridge.topic_subscriptions[topic] = [subscription]
    with patch.object(bridge, "disconnect_client", new_callable=AsyncMock) as disconnect_mock:
        await bridge._forward_to_clients(topic, topic, _make_order_json())
    disconnect_mock.assert_awaited_once()
    cast(AsyncMock, subscription.websocket.send_text).assert_not_awaited()
    assert topic not in bridge.topic_metrics


@pytest.mark.asyncio
async def test_forward_to_clients_market_backpressure(
    bridge: ZmqWebSocketBridgeService,
) -> None:
    """Forward to clients drops market messages on backpressure.

    Given: A market topic subscription with max pending messages,
    When: Another message is forwarded,
    Then: The message is dropped and dropped count is incremented.
    """
    topic = "market.candles."
    websocket = AsyncMock()
    subscription = TopicSubscriptionModel(
        websocket=websocket,
        throttle_ms=0,
        last_sent=0.0,
        client_id="market",
        pending_count=MAX_PENDING_MESSAGES_MARKET,
    )
    bridge.topic_subscriptions[topic] = [subscription]
    bridge.topic_metrics[topic] = TopicMetricsModel()
    await bridge._forward_to_clients(topic, topic, _make_candle_json())
    assert bridge.topic_metrics[topic].dropped_count == 1
    send_mock = cast(AsyncMock, subscription.websocket.send_text)
    send_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_forward_to_clients_timeout_disconnects(
    bridge: ZmqWebSocketBridgeService,
) -> None:
    """Forward to clients disconnects on timeout.

    Given: A subscription with a slow websocket,
    When: Send times out,
    Then: The client is disconnected and timeout count is incremented.
    """
    topic = "market.candles."
    websocket = AsyncMock()
    websocket.send_text.side_effect = TimeoutError()
    subscription = TopicSubscriptionModel(
        websocket=websocket,
        throttle_ms=0,
        last_sent=0.0,
        client_id="slow",
    )
    bridge.topic_subscriptions[topic] = [subscription]
    bridge.topic_metrics[topic] = TopicMetricsModel()
    with patch.object(bridge, "disconnect_client", new_callable=AsyncMock) as disconnect_mock:
        await bridge._forward_to_clients(topic, topic, _make_candle_json())
    disconnect_mock.assert_awaited_once_with(websocket)
    assert bridge.topic_metrics[topic].timeout_count == 1
    assert subscription.pending_count == 0


@pytest.mark.asyncio
async def test_forward_to_clients_timeout_without_metrics(
    bridge: ZmqWebSocketBridgeService,
) -> None:
    """Forward to clients handles timeout without metrics.

    Given: A subscription without metrics tracking,
    When: Send times out,
    Then: The client is disconnected without metrics error.
    """
    topic = "market.candles."
    websocket = AsyncMock()
    websocket.send_text.side_effect = TimeoutError()
    subscription = TopicSubscriptionModel(
        websocket=websocket,
        throttle_ms=0,
        last_sent=0.0,
        client_id="slow-nometrics",
    )
    bridge.topic_subscriptions[topic] = [subscription]
    with patch.object(bridge, "disconnect_client", new_callable=AsyncMock) as disconnect_mock:
        await bridge._forward_to_clients(topic, topic, _make_candle_json())
    disconnect_mock.assert_awaited_once_with(websocket)
    assert topic not in bridge.topic_metrics
    assert subscription.pending_count == 0


@pytest.mark.asyncio
async def test_forward_to_clients_throttled_increments_metrics(
    bridge: ZmqWebSocketBridgeService,
) -> None:
    """Forward to clients increments throttled count.

    Given: A subscription with active throttling,
    When: A message is forwarded within throttle window,
    Then: The message is throttled and count is incremented.
    """
    topic = "market.candles."
    websocket = AsyncMock()
    now = time.time()
    subscription = TopicSubscriptionModel(
        websocket=websocket,
        throttle_ms=200,
        last_sent=now,
        client_id="throttled",
    )
    bridge.topic_subscriptions[topic] = [subscription]
    bridge.topic_metrics[topic] = TopicMetricsModel()
    await bridge._forward_to_clients(topic, f"{topic}BTCUSD", _make_candle_json())
    assert bridge.topic_metrics[topic].throttled_count == 1
    send_mock = cast(AsyncMock, subscription.websocket.send_text)
    send_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_forward_to_clients_throttled_without_metrics(
    bridge: ZmqWebSocketBridgeService,
) -> None:
    """Forward to clients handles throttling without metrics.

    Given: A subscription with throttling but no metrics,
    When: A message is forwarded within throttle window,
    Then: The message is not sent.
    """
    topic = "market.candles."
    websocket = AsyncMock()
    now = time.time()
    subscription = TopicSubscriptionModel(
        websocket=websocket,
        throttle_ms=200,
        last_sent=now,
        client_id="throttled-nometrics",
    )
    bridge.topic_subscriptions[topic] = [subscription]
    await bridge._forward_to_clients(topic, f"{topic}BTCUSD", _make_candle_json())
    websocket.send_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_forward_to_clients_trade_sends_when_no_backpressure(
    bridge: ZmqWebSocketBridgeService,
) -> None:
    """Forward to clients sends trade messages when no backpressure.

    Given: A trade topic subscription with no pending messages,
    When: A message is forwarded,
    Then: The message is sent and forwarded count is incremented.
    """
    topic = "orders"
    websocket = AsyncMock()
    subscription = TopicSubscriptionModel(
        websocket=websocket,
        throttle_ms=0,
        last_sent=0.0,
        client_id="trade-ok",
        pending_count=0,
    )
    bridge.topic_subscriptions[topic] = [subscription]
    bridge.topic_metrics[topic] = TopicMetricsModel()
    await bridge._forward_to_clients(topic, topic, _make_order_json())
    websocket.send_text.assert_awaited_once()
    assert bridge.topic_metrics[topic].forwarded_count == 1


@pytest.mark.asyncio
async def test_start_zmq_subscriber_unknown_topic(
    bridge: ZmqWebSocketBridgeService,
) -> None:
    """Start ZMQ subscriber does nothing for unknown topic.

    Given: An empty available_topics registry,
    When: Starting a subscriber for an unknown topic,
    Then: No socket or task is created.
    """
    bridge.available_topics = {}
    bridge.context = MagicMock()
    await bridge.start_zmq_subscriber("missing")
    assert "missing" not in bridge.zmq_subscribers
    assert "missing" not in bridge.subscriber_tasks


@pytest.mark.asyncio
async def test_start_zmq_subscription_missing_pattern(
    bridge: ZmqWebSocketBridgeService,
) -> None:
    """Start ZMQ subscription handles missing pattern.

    Given: Available topics not containing requested topic,
    When: Starting a subscription for unknown topic,
    Then: No subscriber is created.
    """
    bridge.available_topics = {"market.candles.": bridge.available_topics["market.candles."]}
    bridge.context = MagicMock()
    await bridge._start_zmq_subscription("strategy.signals")
    assert "strategy.signals" not in bridge.zmq_subscribers
    assert "strategy.signals" not in bridge.subscriber_tasks


@pytest.mark.asyncio
async def test_start_zmq_subscription_already_active(
    bridge: ZmqWebSocketBridgeService,
) -> None:
    """Start ZMQ subscription skips when already active.

    Given: An active subscriber task for a topic,
    When: Starting a subscription for the same topic,
    Then: The existing task is preserved.
    """
    topic = "market.candles."
    bridge.context = MagicMock()
    running = asyncio.create_task(asyncio.sleep(0.01))
    bridge.subscriber_tasks[topic] = running
    try:
        await bridge._start_zmq_subscription(topic)
    finally:
        running.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await running
    assert bridge.subscriber_tasks.get(topic) is running
    assert topic not in bridge.zmq_subscribers


@pytest.mark.asyncio
async def test_start_zmq_subscriber_success_creates_task(
    bridge: ZmqWebSocketBridgeService,
) -> None:
    """Start ZMQ subscriber creates socket and task.

    Given: A configured topic with endpoint,
    When: Starting a subscriber for the topic,
    Then: A socket is created and task is started.
    """
    topic = "market.candles."
    mock_socket = MagicMock()
    mock_context = cast(MagicMock, bridge.context)
    mock_context.socket.return_value = mock_socket
    tasks: list[asyncio.Task[None]] = []
    real_create_task = asyncio.create_task

    async def handler(*args: object, **kwargs: object) -> None:
        return None

    def create_task_stub(coro: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        task: asyncio.Task[None] = real_create_task(coro)
        tasks.append(task)
        return task

    with (
        patch.object(bridge, "_handle_zmq_messages", new=handler),
        patch(
            "snapper.interface.websocket.bridge.asyncio.create_task", side_effect=create_task_stub
        ),
    ):
        await bridge.start_zmq_subscriber(topic)
    await asyncio.gather(*tasks)
    mock_context.socket.assert_called_once()
    assert mock_socket.setsockopt.call_count == 2
    mock_socket.setsockopt.assert_any_call(zmq.RCVHWM, 5000)
    assert len(tasks) == 1
    assert topic in bridge.subscriber_tasks
    assert topic in bridge.zmq_subscribers


@pytest.mark.asyncio
async def test_forward_to_clients_send_error_disconnects(
    bridge: ZmqWebSocketBridgeService,
) -> None:
    """Forward to clients disconnects on send error.

    Given: A subscription with a failing websocket,
    When: Send raises an exception,
    Then: The client is disconnected.
    """
    topic = "market.candles."
    websocket = AsyncMock()
    websocket.send_text.side_effect = RuntimeError("boom")
    subscription = TopicSubscriptionModel(
        websocket=websocket,
        throttle_ms=0,
        last_sent=0.0,
        client_id="err",
    )
    bridge.topic_subscriptions[topic] = [subscription]
    bridge.topic_metrics[topic] = TopicMetricsModel()
    with patch.object(bridge, "disconnect_client", new_callable=AsyncMock) as disconnect_mock:
        await bridge._forward_to_clients(topic, topic, _make_candle_json())
    disconnect_mock.assert_awaited_once_with(websocket)


@pytest.mark.asyncio
async def test_unsubscribe_retains_topic_when_other_clients_present(
    bridge: ZmqWebSocketBridgeService,
) -> None:
    """Unsubscribe retains topic when other clients are subscribed.

    Given: Multiple clients subscribed to the same topic,
    When: One client unsubscribes,
    Then: The topic subscription remains for other clients.
    """
    topic = "market.candles."
    ws1 = MagicMock()
    ws2 = MagicMock()
    with patch.object(bridge, "_start_zmq_subscription", new_callable=AsyncMock):
        await bridge.subscribe_client(ws1, [topic])
        await bridge.subscribe_client(ws2, [topic])
    with patch.object(bridge, "_stop_zmq_subscription", new_callable=AsyncMock) as stop_mock:
        await bridge.unsubscribe_client(ws1, [topic])
    assert topic in bridge.topic_subscriptions
    stop_mock.assert_not_called()
    assert bridge.client_subscriptions[ws2] == {topic}


@pytest.mark.asyncio
async def test_forward_to_clients_throttled_skip(
    bridge: ZmqWebSocketBridgeService,
) -> None:
    """Forward to clients skips throttled messages.

    Given: A subscription with active throttle window,
    When: A message is forwarded,
    Then: The message is skipped without sending.
    """
    topic = "market.candles."
    websocket = AsyncMock()
    subscription = TopicSubscriptionModel(
        websocket=websocket,
        throttle_ms=500,
        last_sent=123.0,
        client_id="ws-throttle",
    )
    bridge.topic_subscriptions[topic] = [subscription]
    bridge.topic_metrics[topic] = TopicMetricsModel(active_subscribers=1)
    with patch("snapper.interface.websocket.bridge.time.time", return_value=123.0):
        await bridge._forward_to_clients(topic, topic, _make_candle_json())
    websocket.send_text.assert_not_awaited()
    assert bridge.topic_metrics[topic].active_subscribers == 1


@pytest.mark.asyncio
async def test_handle_zmq_messages_forwards_raw_json(
    bridge: ZmqWebSocketBridgeService,
) -> None:
    """Handle ZMQ messages forwards raw JSON payload.

    Given: A ZMQ socket receiving messages,
    When: A valid JSON message is received,
    Then: The raw JSON is forwarded to websockets.
    """
    topic = "market.candles."
    config = bridge.available_topics[topic]
    mock_socket = MagicMock()
    raw_json = '{"type":"candle","instrument":"BTCUSD","exchange":"kraken"}'
    mock_socket.recv_multipart = AsyncMock(
        side_effect=[
            [topic.encode(), raw_json.encode()],
            asyncio.CancelledError(),
        ]
    )
    process_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    async def process_stub(*args: object, **kwargs: object) -> None:
        process_calls.append((args, kwargs))

    with (
        patch.object(bridge, "_process_zmq_message", new=process_stub),
        pytest.raises(asyncio.CancelledError),
    ):
        await bridge._handle_zmq_messages(topic, mock_socket, config)
    assert len(process_calls) == 1
    args = process_calls[0][0]
    assert args[0] == topic
    assert args[1] == [topic.encode(), raw_json.encode()]


@pytest.mark.asyncio
async def test_forward_to_clients_timeout_disconnects_inline(
    bridge: ZmqWebSocketBridgeService,
) -> None:
    """Forward to clients disconnects on timeout inline.

    Given: A subscription with a slow websocket,
    When: Send times out,
    Then: The client is disconnected and timeout metric is incremented.
    """
    topic = "market.candles."
    websocket = AsyncMock()
    websocket.send_text.side_effect = TimeoutError()
    subscription = TopicSubscriptionModel(
        websocket=websocket,
        throttle_ms=0,
        last_sent=0.0,
        client_id="ws1",
    )
    bridge.topic_subscriptions[topic] = [subscription]
    bridge.topic_metrics[topic] = TopicMetricsModel(active_subscribers=1)
    with patch.object(bridge, "disconnect_client", new_callable=AsyncMock) as disconnect_mock:
        await bridge._forward_to_clients(topic, topic, _make_candle_json())
    disconnect_mock.assert_awaited_once_with(websocket)
    assert bridge.topic_metrics[topic].timeout_count == 1


class TestZMQBridgeRemainingCoverage:
    """Additional coverage tests for ZmqWebSocketBridgeService."""

    @pytest.fixture
    def connection_manager(self) -> MagicMock:
        """Provide mock connection manager."""
        mgr = MagicMock()
        type(mgr).tracker = PropertyMock(return_value=SequenceTracker())
        return mgr

    @pytest.fixture
    def bridge(self, connection_manager: MagicMock) -> ZmqWebSocketBridgeService:
        """Provide ZMQ bridge instance."""
        return ZmqWebSocketBridgeService(connection_manager)

    @pytest.fixture
    def mock_websocket(self) -> MagicMock:
        """Provide mock WebSocket."""
        return MagicMock(spec=WebSocket)

    @pytest.mark.asyncio
    async def test_handle_zmq_messages_success(
        self, bridge: ZmqWebSocketBridgeService, mock_websocket: MagicMock
    ) -> None:
        """Verify ZMQ messages are handled and forwarded.

        Given: A bridge with ZMQ socket receiving messages,
        When: Handling ZMQ messages,
        Then: Messages are forwarded to websockets.
        """
        mock_socket = MagicMock(spec=zmq.asyncio.Socket)
        config = TopicConfigurationModel(
            endpoint="tcp://localhost:5555",
            pattern="test.topic",
            throttle_ms=100,
        )
        topic_bytes = b"test.topic"
        data_dict: dict[str, Any] = {
            "type": "candle",
            "instrument": "EUR-USD",
            "exchange": "kraken",
            "open": 1.1000,
            "high": 1.2,
            "low": 1.0,
            "close": 1.15,
            "volume": 100.0,
            "timestamp": "2024-01-01T00:00:00+00:00",
        }
        data_str = json.dumps(data_dict)
        data_bytes = data_str.encode()
        with patch.object(bridge, "_process_zmq_message", new_callable=AsyncMock) as mock_process:
            call_count = 0

            async def mock_recv_side_effect() -> list[bytes]:
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    return [topic_bytes, data_bytes]
                else:
                    raise asyncio.CancelledError()

            mock_socket.recv_multipart = AsyncMock(side_effect=mock_recv_side_effect)
            handle_messages = bridge._handle_zmq_messages
            with contextlib.suppress(asyncio.CancelledError):
                await handle_messages("test.topic", mock_socket, config)
            mock_process.assert_called_once()
            args = mock_process.call_args[0]
            assert args[0] == "test.topic"
            assert args[1] == [topic_bytes, data_bytes]

    @pytest.mark.asyncio
    async def test_handle_zmq_messages_general_exception(
        self, bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify socket exceptions are handled gracefully.

        Given: A ZMQ socket that raises exception,
        When: Handling messages,
        Then: Exception is caught and method returns.
        """
        mock_socket = MagicMock(spec=zmq.asyncio.Socket)
        config = TopicConfigurationModel(
            endpoint="tcp://localhost:5555",
            pattern="test.topic",
            throttle_ms=100,
        )
        mock_socket.recv_multipart = AsyncMock(side_effect=Exception("Socket error"))
        handle_messages = bridge._handle_zmq_messages
        await handle_messages("test.topic", mock_socket, config)

    @pytest.mark.asyncio
    async def test_forward_to_clients_no_subscriptions(
        self, bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify forwarding handles missing subscriptions.

        Given: A bridge with no topic subscriptions,
        When: Forwarding a message,
        Then: Method returns without error.
        """
        test_data_str = '{"type": "test", "data": "value"}'
        await bridge._forward_to_clients("nonexistent", "test.topic", test_data_str)

    @pytest.mark.asyncio
    async def test_forward_to_clients_with_throttling(
        self, bridge: ZmqWebSocketBridgeService, mock_websocket: MagicMock
    ) -> None:
        """Verify throttling prevents rapid message sending.

        Given: A subscription with recent send timestamp,
        When: Forwarding within throttle window,
        Then: Message is not sent.
        """
        topic = "market.kraken.BTC-USD.candles"
        subscription = TopicSubscriptionModel(
            websocket=mock_websocket,
            throttle_ms=1000,
            last_sent=1000.0,
        )
        bridge.topic_subscriptions[topic] = [subscription]
        with patch("time.time", return_value=1000.5):
            mock_websocket.send_text = AsyncMock()
            test_data_str = json.dumps(
                {
                    "type": "candle",
                    "instrument": "BTC-USD",
                    "exchange": "kraken",
                    "timeframe": "1m",
                    "open": 50000.0,
                    "high": 51000.0,
                    "low": 49000.0,
                    "close": 50500.0,
                    "volume": 100.0,
                    "timestamp": "2024-01-01T00:00:00+00:00",
                }
            )
            await bridge._forward_to_clients(topic, topic, test_data_str)
            mock_websocket.send_text.assert_not_called()

    @pytest.mark.asyncio
    async def test_forward_to_clients_send_success(
        self, bridge: ZmqWebSocketBridgeService, mock_websocket: MagicMock
    ) -> None:
        """Verify successful message forwarding.

        Given: A subscription with zero last_sent,
        When: Forwarding a message,
        Then: Message is sent to WebSocket.
        """
        topic = "market.kraken.BTC-USD.candles"
        subscription = TopicSubscriptionModel(
            websocket=mock_websocket, throttle_ms=100, last_sent=0.0
        )
        bridge.topic_subscriptions[topic] = [subscription]
        mock_websocket.send_text = AsyncMock()
        test_data_str = json.dumps(
            {
                "type": "candle",
                "instrument": "BTC-USD",
                "exchange": "kraken",
                "timeframe": "1m",
                "open": 50000.0,
                "high": 51000.0,
                "low": 49000.0,
                "close": 50500.0,
                "volume": 100.0,
                "timestamp": "2024-01-01T00:00:00+00:00",
            }
        )
        await bridge._forward_to_clients(topic, topic, test_data_str)
        mock_websocket.send_text.assert_called_once_with(test_data_str)

    @pytest.mark.asyncio
    async def test_forward_to_clients_send_failure_disconnects(
        self, bridge: ZmqWebSocketBridgeService, mock_websocket: MagicMock
    ) -> None:
        """Verify send failure disconnects client.

        Given: A subscription with failing WebSocket,
        When: Forwarding a message,
        Then: Client is disconnected via disconnect_client.
        """
        topic = "market.kraken.BTC-USD.candles"
        subscription = TopicSubscriptionModel(
            websocket=mock_websocket, throttle_ms=100, last_sent=0.0
        )
        bridge.topic_subscriptions[topic] = [subscription]
        mock_websocket.send_text = AsyncMock(side_effect=Exception("Send failed"))
        bridge.disconnect_client = AsyncMock()
        test_data_str = json.dumps(
            {
                "type": "candle",
                "instrument": "BTC-USD",
                "exchange": "kraken",
                "timeframe": "1m",
                "open": 50000.0,
                "high": 51000.0,
                "low": 49000.0,
                "close": 50500.0,
                "volume": 100.0,
                "timestamp": "2024-01-01T00:00:00+00:00",
            }
        )
        await bridge._forward_to_clients(topic, topic, test_data_str)
        bridge.disconnect_client.assert_awaited_once_with(mock_websocket)

    @pytest.mark.asyncio
    async def test_forward_to_clients_send_timeout_disconnects(
        self, bridge: ZmqWebSocketBridgeService, mock_websocket: MagicMock
    ) -> None:
        """Verify timeout handling disconnects client.

        Given: A subscription with WebSocket that times out,
        When: Forwarding a message,
        Then: Client is disconnected and timeout count updated.
        """
        topic = "market.kraken.BTC-USD.candles"
        subscription = TopicSubscriptionModel(
            websocket=mock_websocket, throttle_ms=100, last_sent=0.0, client_id="slow_client"
        )
        bridge.topic_subscriptions[topic] = [subscription]
        bridge.topic_metrics[topic] = TopicMetricsModel()
        mock_websocket.send_text = AsyncMock(side_effect=TimeoutError())
        bridge.disconnect_client = AsyncMock()
        test_data_str = json.dumps(
            {
                "type": "candle",
                "instrument": "BTC-USD",
                "exchange": "kraken",
                "timeframe": "1m",
                "open": 50000.0,
                "high": 51000.0,
                "low": 49000.0,
                "close": 50500.0,
                "volume": 100.0,
                "timestamp": "2024-01-01T00:00:00+00:00",
            }
        )
        await bridge._forward_to_clients(topic, topic, test_data_str)
        bridge.disconnect_client.assert_awaited_once_with(mock_websocket)
        assert bridge.topic_metrics[topic].timeout_count == 1


class TestSubscribeWebsocket:
    """Tests for WebSocket subscription to ZMQ topics."""

    @pytest.fixture
    def connection_manager(self) -> MagicMock:
        """Provide mock connection manager."""
        mgr = MagicMock()
        type(mgr).tracker = PropertyMock(return_value=SequenceTracker())
        return mgr

    @pytest.fixture
    def bridge(self, connection_manager: MagicMock) -> ZmqWebSocketBridgeService:
        """Provide bridge with available topics configured."""
        bridge = ZmqWebSocketBridgeService(connection_manager)
        bridge.available_topics = {
            "market.kraken.BTC-USD.candles.1m": TopicConfigurationModel(
                pattern="market.*.*.candles.*",
                endpoint="tcp://localhost:5555",
                throttle_ms=100,
            ),
            "market.kraken.ETH-USD.ticks": TopicConfigurationModel(
                pattern="market.*.*.ticks",
                endpoint="tcp://localhost:5556",
                throttle_ms=50,
            ),
        }
        return bridge

    @pytest.fixture
    def mock_websocket(self) -> AsyncMock:
        """Provide mock WebSocket with send_json capability."""
        ws = AsyncMock(spec=WebSocket)
        ws.send_json = AsyncMock()
        return ws

    @pytest.mark.asyncio
    async def test_subscribe_websocket_success(
        self, bridge: ZmqWebSocketBridgeService, mock_websocket: AsyncMock
    ) -> None:
        """Verify successful subscription to valid topic.

        Given: A bridge with available topics,
        When: Subscribing to a valid topic,
        Then: Subscription is created and ZMQ subscriber started.
        """
        topic = "market.kraken.BTC-USD.candles.1m"
        with patch.object(bridge, "start_zmq_subscriber", new=AsyncMock()) as mock_start:
            result = await bridge.subscribe_websocket(mock_websocket, topic)
        assert result is True
        assert topic in bridge.topic_subscriptions
        assert len(bridge.topic_subscriptions[topic]) == 1
        assert bridge.topic_subscriptions[topic][0].websocket == mock_websocket
        assert mock_websocket in bridge.client_subscriptions
        assert topic in bridge.client_subscriptions[mock_websocket]
        assert topic in bridge.topic_metrics
        assert bridge.topic_metrics[topic].active_subscribers == 1
        mock_start.assert_called_once_with(topic)

    @pytest.mark.asyncio
    async def test_subscribe_websocket_invalid_topic(
        self, bridge: ZmqWebSocketBridgeService, mock_websocket: AsyncMock
    ) -> None:
        """Verify invalid topic returns error.

        Given: A bridge with specific available topics,
        When: Subscribing to invalid topic,
        Then: Error is sent to client and returns False.
        """
        topic = "invalid.unknown.topic"
        result = await bridge.subscribe_websocket(mock_websocket, topic)
        assert result is False
        assert topic not in bridge.topic_subscriptions
        mock_websocket.send_text.assert_called_once()
        sent_json = json.loads(mock_websocket.send_text.call_args[0][0])
        assert sent_json["type"] == "error"
        assert topic in sent_json["message"]

    @pytest.mark.asyncio
    async def test_subscribe_websocket_invalid_topic_send_error_fails(
        self, bridge: ZmqWebSocketBridgeService, mock_websocket: AsyncMock
    ) -> None:
        """Verify error sending failure is handled.

        Given: A WebSocket that fails to send,
        When: Subscribing to invalid topic,
        Then: Returns False without raising exception.
        """
        topic = "invalid.unknown.topic"
        mock_websocket.send_text = AsyncMock(side_effect=Exception("Connection closed"))
        result = await bridge.subscribe_websocket(mock_websocket, topic)
        assert result is False

    @pytest.mark.asyncio
    async def test_subscribe_websocket_already_subscribed(
        self, bridge: ZmqWebSocketBridgeService, mock_websocket: AsyncMock
    ) -> None:
        """Verify duplicate subscription is idempotent.

        Given: A WebSocket already subscribed to a topic,
        When: Subscribing again to the same topic,
        Then: Returns True but doesn't duplicate subscription.
        """
        topic = "market.kraken.BTC-USD.candles.1m"
        with patch.object(bridge, "start_zmq_subscriber", new=AsyncMock()):
            result1 = await bridge.subscribe_websocket(mock_websocket, topic)
            result2 = await bridge.subscribe_websocket(mock_websocket, topic)
        assert result1 is True
        assert result2 is True
        assert len(bridge.topic_subscriptions[topic]) == 1

    @pytest.mark.asyncio
    async def test_subscribe_websocket_custom_throttle(
        self, bridge: ZmqWebSocketBridgeService, mock_websocket: AsyncMock
    ) -> None:
        """Verify custom throttle is applied.

        Given: A valid subscription request,
        When: Subscribing with custom throttle,
        Then: Subscription uses custom throttle value.
        """
        topic = "market.kraken.BTC-USD.candles.1m"
        custom_throttle = 500
        with patch.object(bridge, "start_zmq_subscriber", new=AsyncMock()):
            result = await bridge.subscribe_websocket(
                mock_websocket, topic, throttle_ms=custom_throttle
            )
        assert result is True
        assert bridge.topic_subscriptions[topic][0].throttle_ms == custom_throttle

    @pytest.mark.asyncio
    async def test_subscribe_websocket_multiple_clients(
        self, bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify multiple clients can subscribe to same topic.

        Given: Multiple WebSocket clients,
        When: Both subscribe to the same topic,
        Then: Both subscriptions are tracked independently.
        """
        topic = "market.kraken.BTC-USD.candles.1m"
        ws1 = AsyncMock(spec=WebSocket)
        ws2 = AsyncMock(spec=WebSocket)
        with patch.object(bridge, "start_zmq_subscriber", new=AsyncMock()):
            await bridge.subscribe_websocket(ws1, topic)
            await bridge.subscribe_websocket(ws2, topic)
        assert len(bridge.topic_subscriptions[topic]) == 2
        assert bridge.topic_metrics[topic].active_subscribers == 2


class TestUnsubscribeWebsocket:
    """Tests for WebSocket unsubscription from ZMQ topics."""

    @pytest.fixture
    def connection_manager(self) -> MagicMock:
        """Provide mock connection manager."""
        mgr = MagicMock()
        type(mgr).tracker = PropertyMock(return_value=SequenceTracker())
        return mgr

    @pytest.fixture
    def bridge(self, connection_manager: MagicMock) -> ZmqWebSocketBridgeService:
        """Provide bridge with available topics configured."""
        bridge = ZmqWebSocketBridgeService(connection_manager)
        bridge.available_topics = {
            "market.kraken.BTC-USD.candles.1m": TopicConfigurationModel(
                pattern="market.*.*.candles.*",
                endpoint="tcp://localhost:5555",
                throttle_ms=100,
            ),
        }
        return bridge

    @pytest.fixture
    def mock_websocket(self) -> AsyncMock:
        """Provide mock WebSocket."""
        return AsyncMock(spec=WebSocket)

    @pytest.mark.asyncio
    async def test_unsubscribe_websocket_success(
        self, bridge: ZmqWebSocketBridgeService, mock_websocket: AsyncMock
    ) -> None:
        """Verify successful unsubscription removes client.

        Given: A WebSocket subscribed to a topic,
        When: Unsubscribing from the topic,
        Then: Subscription is removed and ZMQ subscriber stopped.
        """
        topic = "market.kraken.BTC-USD.candles.1m"
        with patch.object(bridge, "start_zmq_subscriber", new=AsyncMock()):
            await bridge.subscribe_websocket(mock_websocket, topic)
        with patch.object(bridge, "stop_zmq_subscriber", new=AsyncMock()) as mock_stop:
            result = await bridge.unsubscribe_websocket(mock_websocket, topic)
        assert result is True
        assert topic not in bridge.topic_subscriptions
        mock_stop.assert_called_once_with(topic)

    @pytest.mark.asyncio
    async def test_unsubscribe_websocket_topic_not_found(
        self, bridge: ZmqWebSocketBridgeService, mock_websocket: AsyncMock
    ) -> None:
        """Verify unsubscribing from nonexistent topic returns False.

        Given: A bridge with no subscriptions,
        When: Unsubscribing from nonexistent topic,
        Then: Returns False.
        """
        result = await bridge.unsubscribe_websocket(mock_websocket, "nonexistent.topic")
        assert result is False

    @pytest.mark.asyncio
    async def test_unsubscribe_websocket_client_not_subscribed(
        self, bridge: ZmqWebSocketBridgeService, mock_websocket: AsyncMock
    ) -> None:
        """Verify unsubscribing non-subscribed client returns False.

        Given: A topic with another client subscribed,
        When: Unsubscribing a different client,
        Then: Returns False and keeps other subscription.
        """
        topic = "market.kraken.BTC-USD.candles.1m"
        other_ws = AsyncMock(spec=WebSocket)
        with patch.object(bridge, "start_zmq_subscriber", new=AsyncMock()):
            await bridge.subscribe_websocket(other_ws, topic)
        result = await bridge.unsubscribe_websocket(mock_websocket, topic)
        assert result is False
        assert len(bridge.topic_subscriptions[topic]) == 1

    @pytest.mark.asyncio
    async def test_unsubscribe_websocket_updates_metrics(
        self, bridge: ZmqWebSocketBridgeService, mock_websocket: AsyncMock
    ) -> None:
        """Verify unsubscription updates topic metrics.

        Given: A WebSocket subscribed to a topic,
        When: Unsubscribing from the topic,
        Then: Active subscribers count is decremented.
        """
        topic = "market.kraken.BTC-USD.candles.1m"
        with patch.object(bridge, "start_zmq_subscriber", new=AsyncMock()):
            await bridge.subscribe_websocket(mock_websocket, topic)
        assert bridge.topic_metrics[topic].active_subscribers == 1
        with patch.object(bridge, "stop_zmq_subscriber", new=AsyncMock()):
            await bridge.unsubscribe_websocket(mock_websocket, topic)
        if topic in bridge.topic_metrics:
            assert bridge.topic_metrics[topic].active_subscribers == 0

    @pytest.mark.asyncio
    async def test_unsubscribe_websocket_removes_client_tracking(
        self, bridge: ZmqWebSocketBridgeService, mock_websocket: AsyncMock
    ) -> None:
        """Verify client is removed from tracking after unsubscribing.

        Given: A WebSocket subscribed to a topic,
        When: Unsubscribing from all topics,
        Then: Client is removed from client_subscriptions.
        """
        topic = "market.kraken.BTC-USD.candles.1m"
        with patch.object(bridge, "start_zmq_subscriber", new=AsyncMock()):
            await bridge.subscribe_websocket(mock_websocket, topic)
        assert mock_websocket in bridge.client_subscriptions
        with patch.object(bridge, "stop_zmq_subscriber", new=AsyncMock()):
            await bridge.unsubscribe_websocket(mock_websocket, topic)
        assert mock_websocket not in bridge.client_subscriptions

    @pytest.mark.asyncio
    async def test_unsubscribe_websocket_keeps_other_subscribers(
        self, bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify unsubscribing one client keeps others.

        Given: Two clients subscribed to same topic,
        When: One client unsubscribes,
        Then: Other client remains subscribed.
        """
        topic = "market.kraken.BTC-USD.candles.1m"
        ws1 = AsyncMock(spec=WebSocket)
        ws2 = AsyncMock(spec=WebSocket)
        with patch.object(bridge, "start_zmq_subscriber", new=AsyncMock()):
            await bridge.subscribe_websocket(ws1, topic)
            await bridge.subscribe_websocket(ws2, topic)
        result = await bridge.unsubscribe_websocket(ws1, topic)
        assert result is True
        assert len(bridge.topic_subscriptions[topic]) == 1
        assert bridge.topic_subscriptions[topic][0].websocket == ws2


class TestUnsubscribeWebsocketAll:
    """Tests for bulk unsubscription of WebSocket from all topics."""

    @pytest.fixture
    def connection_manager(self) -> MagicMock:
        """Provide mock connection manager."""
        mgr = MagicMock()
        type(mgr).tracker = PropertyMock(return_value=SequenceTracker())
        return mgr

    @pytest.fixture
    def bridge(self, connection_manager: MagicMock) -> ZmqWebSocketBridgeService:
        """Provide bridge with multiple available topics."""
        bridge = ZmqWebSocketBridgeService(connection_manager)
        bridge.available_topics = {
            "market.kraken.BTC-USD.candles.1m": TopicConfigurationModel(
                pattern="market.*.*.candles.*",
                endpoint="tcp://localhost:5555",
                throttle_ms=100,
            ),
            "market.kraken.ETH-USD.ticks": TopicConfigurationModel(
                pattern="market.*.*.ticks",
                endpoint="tcp://localhost:5556",
                throttle_ms=50,
            ),
            "signals.strategy.rsi": TopicConfigurationModel(
                pattern="signals.*.*",
                endpoint="tcp://localhost:5557",
                throttle_ms=200,
            ),
        }
        return bridge

    @pytest.fixture
    def mock_websocket(self) -> AsyncMock:
        """Provide mock WebSocket."""
        return AsyncMock(spec=WebSocket)

    @pytest.mark.asyncio
    async def test_unsubscribe_websocket_all_multiple_topics(
        self, bridge: ZmqWebSocketBridgeService, mock_websocket: AsyncMock
    ) -> None:
        """Verify unsubscribing from all topics at once.

        Given: A WebSocket subscribed to multiple topics,
        When: Calling unsubscribe_websocket_all,
        Then: All subscriptions are removed.
        """
        topics = [
            "market.kraken.BTC-USD.candles.1m",
            "market.kraken.ETH-USD.ticks",
            "signals.strategy.rsi",
        ]
        with patch.object(bridge, "start_zmq_subscriber", new=AsyncMock()):
            for topic in topics:
                await bridge.subscribe_websocket(mock_websocket, topic)
        with patch.object(bridge, "stop_zmq_subscriber", new=AsyncMock()):
            count = await bridge.unsubscribe_websocket_all(mock_websocket)
        assert count == 3
        assert mock_websocket not in bridge.client_subscriptions
        for topic in topics:
            assert topic not in bridge.topic_subscriptions

    @pytest.mark.asyncio
    async def test_unsubscribe_websocket_all_no_subscriptions(
        self, bridge: ZmqWebSocketBridgeService, mock_websocket: AsyncMock
    ) -> None:
        """Verify unsubscribe_all with no subscriptions returns zero.

        Given: A WebSocket with no subscriptions,
        When: Calling unsubscribe_websocket_all,
        Then: Returns 0.
        """
        count = await bridge.unsubscribe_websocket_all(mock_websocket)
        assert count == 0

    @pytest.mark.asyncio
    async def test_unsubscribe_websocket_all_preserves_other_clients(
        self, bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify unsubscribe_all preserves other clients.

        Given: Two clients subscribed to same topic,
        When: One client unsubscribes from all,
        Then: Other client remains subscribed.
        """
        topic = "market.kraken.BTC-USD.candles.1m"
        ws1 = AsyncMock(spec=WebSocket)
        ws2 = AsyncMock(spec=WebSocket)
        with patch.object(bridge, "start_zmq_subscriber", new=AsyncMock()):
            await bridge.subscribe_websocket(ws1, topic)
            await bridge.subscribe_websocket(ws2, topic)
        with patch.object(bridge, "stop_zmq_subscriber", new=AsyncMock()):
            count = await bridge.unsubscribe_websocket_all(ws1)
        assert count == 1
        assert len(bridge.topic_subscriptions[topic]) == 1
        assert bridge.topic_subscriptions[topic][0].websocket == ws2


class TestGetSubscriptionStats:
    """Tests for ZmqWebSocketBridgeService.get_subscription_stats method."""

    @pytest.fixture
    def connection_manager(self) -> MagicMock:
        """Provide mock connection manager."""
        mgr = MagicMock()
        type(mgr).tracker = PropertyMock(return_value=SequenceTracker())
        return mgr

    @pytest.fixture
    def bridge(self, connection_manager: MagicMock) -> ZmqWebSocketBridgeService:
        """Provide ZMQ bridge with topic configurations."""
        bridge = ZmqWebSocketBridgeService(connection_manager)
        bridge.available_topics = {
            "market.kraken.BTC-USD.candles.1m": TopicConfigurationModel(
                pattern="market.*.*.candles.*",
                endpoint="tcp://localhost:5555",
                throttle_ms=100,
            ),
            "market.kraken.ETH-USD.ticks": TopicConfigurationModel(
                pattern="market.*.*.ticks",
                endpoint="tcp://localhost:5556",
                throttle_ms=50,
            ),
        }
        return bridge

    def test_get_subscription_stats_empty(self, bridge: ZmqWebSocketBridgeService) -> None:
        """Verify subscription stats returns empty data when no subscriptions.

        Given: A bridge with available topics but no active subscriptions,
        When: Getting subscription stats,
        Then: Returns stats with zero active topics and subscribers.
        """
        stats = bridge.get_subscription_stats()
        assert stats.total_topics == 2
        assert stats.active_topics == 0
        assert stats.total_subscribers == 0
        assert stats.topics == {}

    @pytest.mark.asyncio
    async def test_get_subscription_stats_with_subscriptions(
        self, bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify subscription stats reflects active subscriptions.

        Given: A bridge with available topics,
        When: Multiple clients subscribe to a topic,
        Then: Stats show correct subscriber count and topic details.
        """
        topic = "market.kraken.BTC-USD.candles.1m"
        ws1 = AsyncMock(spec=WebSocket)
        ws2 = AsyncMock(spec=WebSocket)
        with patch.object(bridge, "start_zmq_subscriber", new=AsyncMock()):
            await bridge.subscribe_websocket(ws1, topic)
            await bridge.subscribe_websocket(ws2, topic)
        stats = bridge.get_subscription_stats()
        assert stats.total_topics == 2
        assert stats.active_topics == 1
        assert stats.total_subscribers == 2
        assert topic in stats.topics
        assert stats.topics[topic].subscribers == 2
        assert stats.topics[topic].endpoint == "tcp://localhost:5555"
        assert stats.topics[topic].throttle_ms == 100


class TestGetAvailableTopics:
    """Tests for ZmqWebSocketBridgeService.get_available_topics method."""

    @pytest.fixture
    def connection_manager(self) -> MagicMock:
        """Provide mock connection manager."""
        mgr = MagicMock()
        type(mgr).tracker = PropertyMock(return_value=SequenceTracker())
        return mgr

    @pytest.fixture
    def bridge(self, connection_manager: MagicMock) -> ZmqWebSocketBridgeService:
        """Provide ZMQ bridge with topic configurations."""
        bridge = ZmqWebSocketBridgeService(connection_manager)
        bridge.available_topics = {
            "topic.one": TopicConfigurationModel(
                pattern="topic.*", endpoint="tcp://localhost:5555", throttle_ms=100
            ),
            "topic.two": TopicConfigurationModel(
                pattern="topic.*", endpoint="tcp://localhost:5556", throttle_ms=100
            ),
        }
        return bridge

    def test_get_available_topics(self, bridge: ZmqWebSocketBridgeService) -> None:
        """Verify get_available_topics returns all configured topics.

        Given: A bridge with two configured topics,
        When: Getting available topics,
        Then: Returns list containing both topic names.
        """
        topics = bridge.get_available_topics()
        assert len(topics) == 2
        assert "topic.one" in topics
        assert "topic.two" in topics

    def test_get_available_topics_empty(self, connection_manager: MagicMock) -> None:
        """Verify get_available_topics returns empty list when no topics.

        Given: A bridge with no configured topics,
        When: Getting available topics,
        Then: Returns empty list.
        """
        bridge = ZmqWebSocketBridgeService(connection_manager)
        bridge.available_topics = {}
        topics = bridge.get_available_topics()
        assert topics == []


class TestGetTopicStats:
    """Tests for ZmqWebSocketBridgeService.get_topic_stats method."""

    @pytest.fixture
    def connection_manager(self) -> MagicMock:
        """Provide mock connection manager."""
        mgr = MagicMock()
        type(mgr).tracker = PropertyMock(return_value=SequenceTracker())
        return mgr

    @pytest.fixture
    def bridge(self, connection_manager: MagicMock) -> ZmqWebSocketBridgeService:
        """Provide ZMQ bridge with topic configurations."""
        bridge = ZmqWebSocketBridgeService(connection_manager)
        bridge.available_topics = {
            "market.kraken.BTC-USD.candles.1m": TopicConfigurationModel(
                pattern="market.*.*.candles.*",
                endpoint="tcp://localhost:5555",
                throttle_ms=100,
            ),
        }
        return bridge

    @pytest.mark.asyncio
    async def test_get_topic_stats_with_metrics(self, bridge: ZmqWebSocketBridgeService) -> None:
        """Verify topic stats returns correct metrics.

        Given: A bridge with a subscribed topic and tracked metrics,
        When: Getting topic stats,
        Then: Returns all metric counts for the topic.
        """
        topic = "market.kraken.BTC-USD.candles.1m"
        ws = AsyncMock(spec=WebSocket)
        with patch.object(bridge, "start_zmq_subscriber", new=AsyncMock()):
            await bridge.subscribe_websocket(ws, topic)
        bridge.topic_metrics[topic].received_count = 100
        bridge.topic_metrics[topic].forwarded_count = 95
        bridge.topic_metrics[topic].throttled_count = 5
        bridge.topic_metrics[topic].dropped_count = 2
        bridge.topic_metrics[topic].timeout_count = 1
        bridge.topic_metrics[topic].error_count = 0
        bridge.topic_metrics[topic].invalid_messages = 3
        stats = bridge.get_topic_stats()
        assert topic in stats
        assert isinstance(stats[topic], TopicMetricSnapshot)
        assert stats[topic].received == 100
        assert stats[topic].forwarded == 95
        assert stats[topic].throttled == 5
        assert stats[topic].dropped == 2
        assert stats[topic].timeout == 1
        assert stats[topic].errors == 0
        assert stats[topic].invalid_messages == 3
        assert stats[topic].active_subscribers == 1

    def test_get_topic_stats_empty(self, bridge: ZmqWebSocketBridgeService) -> None:
        """Verify topic stats returns empty dict when no metrics.

        Given: A bridge with no tracked topic metrics,
        When: Getting topic stats,
        Then: Returns empty dictionary.
        """
        stats = bridge.get_topic_stats()
        assert stats == {}


class TestGetConnectionStats:
    """Tests for ZmqWebSocketBridgeService.get_connection_stats method."""

    @pytest.fixture
    def connection_manager(self) -> MagicMock:
        """Provide mock connection manager."""
        mgr = MagicMock()
        type(mgr).tracker = PropertyMock(return_value=SequenceTracker())
        return mgr

    @pytest.fixture
    def bridge(self, connection_manager: MagicMock) -> ZmqWebSocketBridgeService:
        """Provide ZMQ bridge with topic configurations."""
        bridge = ZmqWebSocketBridgeService(connection_manager)
        bridge.available_topics = {
            "market.kraken.BTC-USD.candles.1m": TopicConfigurationModel(
                pattern="market.*.*.candles.*",
                endpoint="tcp://localhost:5555",
                throttle_ms=100,
            ),
        }
        return bridge

    @pytest.mark.asyncio
    async def test_get_connection_stats(self, bridge: ZmqWebSocketBridgeService) -> None:
        """Verify connection stats reflects active clients and topics.

        Given: A bridge with multiple clients subscribed to a topic,
        When: Getting connection stats,
        Then: Returns correct active clients and topics count.
        """
        topic = "market.kraken.BTC-USD.candles.1m"
        ws1 = AsyncMock(spec=WebSocket)
        ws2 = AsyncMock(spec=WebSocket)
        with patch.object(bridge, "start_zmq_subscriber", new=AsyncMock()):
            await bridge.subscribe_websocket(ws1, topic)
            await bridge.subscribe_websocket(ws2, topic)
        stats = bridge.get_connection_stats()
        assert isinstance(stats, ConnectionStats)
        assert stats.active_clients == 2
        assert stats.active_topics == 1


class TestZMQSubscriptionLoop:
    """Tests for ZMQ subscription loop behavior."""

    @pytest.fixture
    def bridge(self) -> ZmqWebSocketBridgeService:
        """Provide ZMQ bridge with topic configurations."""
        mock_connection_manager = MagicMock()
        bridge = ZmqWebSocketBridgeService(mock_connection_manager)
        bridge.available_topics = {
            "market.prices": TopicConfigurationModel(
                endpoint="tcp://localhost:5555",
                pattern="market.prices",
                throttle_ms=100,
            ),
            "signals.kraken.BTC-USD.live": TopicConfigurationModel(
                endpoint="tcp://localhost:5556",
                pattern="signals.kraken.BTC-USD.live",
                throttle_ms=100,
            ),
        }
        return bridge

    @pytest.mark.asyncio
    async def test_zmq_subscription_loop_invalid_message_format(
        self, bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify subscription loop handles invalid message formats.

        Given: A ZMQ socket returning messages with wrong part count,
        When: Processing messages in subscription loop,
        Then: Logs warnings for invalid formats and continues processing.
        """
        topic = "market.prices"
        mock_socket = AsyncMock()
        mock_socket.recv_multipart.side_effect = [
            [b"single_part"],
            [b"topic", b"payload", b"extra"],
            asyncio.CancelledError(),
        ]
        bridge.topic_metrics[topic] = MagicMock()
        bridge.topic_metrics[topic].received_count = 0
        bridge.topic_metrics[topic].error_count = 0
        with (
            patch("snapper.interface.websocket.bridge.logger") as mock_logger,
            pytest.raises(asyncio.CancelledError),
        ):
            await bridge._zmq_subscription_loop(topic, mock_socket)
        warning_calls = list(mock_logger.warning.call_args_list)
        assert len(warning_calls) == 2
        assert "expected 2 parts, got 1" in warning_calls[0][0][0]
        assert "expected 2 parts, got 3" in warning_calls[1][0][0]

    @pytest.mark.asyncio
    async def test_zmq_subscription_loop_json_parse_error(
        self, bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify subscription loop drops invalid JSON in strict mode.

        Given: A ZMQ socket returning message with invalid JSON payload,
        When: Processing message in subscription loop,
        Then: Message is dropped, received_count not incremented, invalid_messages incremented.
        """
        topic = "market.prices"
        mock_socket = AsyncMock()
        mock_socket.recv_multipart.side_effect = [
            [b"market.prices", b"invalid_json{"],
            asyncio.CancelledError(),
        ]
        bridge.topic_metrics[topic] = MagicMock()
        bridge.topic_metrics[topic].received_count = 0
        bridge.topic_metrics[topic].error_count = 0
        bridge.topic_metrics[topic].last_message_ts = 0
        bridge.topic_metrics[topic].invalid_messages = 0
        forwarded_messages: list[tuple[str, str, str]] = []

        async def mock_forward(topic_name: str, received_topic: str, payload_str: str) -> None:
            forwarded_messages.append((topic_name, received_topic, payload_str))

        bridge._forward_to_clients = mock_forward
        with pytest.raises(asyncio.CancelledError):
            await bridge._zmq_subscription_loop(topic, mock_socket)
        assert len(forwarded_messages) == 0
        assert bridge.topic_metrics[topic].received_count == 0
        assert bridge.topic_metrics[topic].invalid_messages == 1

    @pytest.mark.asyncio
    async def test_zmq_subscription_loop_zmq_error_recovery(
        self, bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify subscription loop recovers from ZMQ errors.

        Given: A ZMQ socket that throws ZMQError then returns valid data,
        When: Processing messages in subscription loop,
        Then: Logs error, sleeps for backoff, and continues processing.
        """
        topic = "market.prices"
        mock_socket = AsyncMock()
        mock_socket.recv_multipart.side_effect = [
            zmq.ZMQError(),
            [b"market.prices", b'{"price": 100, "session_id": "test-session", "sequence_id": 1}'],
            asyncio.CancelledError(),
        ]
        bridge.topic_metrics[topic] = MagicMock()
        bridge.topic_metrics[topic].received_count = 0
        bridge.topic_metrics[topic].error_count = 0
        bridge.topic_metrics[topic].last_message_ts = 0
        bridge._forward_to_clients = AsyncMock()
        with (
            patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
            patch("snapper.interface.websocket.bridge.logger") as mock_logger,
            pytest.raises(asyncio.CancelledError),
        ):
            await bridge._zmq_subscription_loop(topic, mock_socket)
        error_calls = list(mock_logger.error.call_args_list)
        assert any("ZMQ error in subscription loop" in str(call) for call in error_calls)
        mock_sleep.assert_called_with(1)
        assert bridge.topic_metrics[topic].received_count == 1
        bridge._forward_to_clients.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_zmq_subscription_success(self, bridge: ZmqWebSocketBridgeService) -> None:
        """Verify ZMQ subscription starts successfully.

        Given: A bridge with ZMQ context and available topic configuration,
        When: Starting ZMQ subscription for a topic,
        Then: Creates socket, connects, subscribes, and starts loop task.
        """
        topic = "market.prices"
        config = bridge.available_topics[topic]
        mock_socket = MagicMock()
        bridge.context = MagicMock()
        bridge.context.socket.return_value = mock_socket
        mock_task = MagicMock()

        def mock_create_task(coro: Any) -> MagicMock:
            if hasattr(coro, "close"):
                coro.close()
            return mock_task

        with (
            patch("asyncio.create_task", side_effect=mock_create_task) as mock_create_task_func,
            patch("snapper.interface.websocket.bridge.logger") as mock_logger,
        ):
            await bridge._start_zmq_subscription(topic)
        bridge.context.socket.assert_called_once_with(zmq.SUB)
        mock_socket.connect.assert_called_once_with(config.endpoint)
        assert mock_socket.setsockopt.call_count == 2
        mock_socket.setsockopt.assert_any_call(zmq.RCVHWM, 5000)
        mock_socket.setsockopt.assert_any_call(zmq.SUBSCRIBE, config.pattern.encode("utf-8"))
        assert bridge.zmq_subscribers[topic] == mock_socket
        assert bridge.subscriber_tasks[topic] == mock_task
        mock_create_task_func.assert_called_once()
        info_calls = list(mock_logger.info.call_args_list)
        assert any("Starting ZMQ subscription for topic" in str(call) for call in info_calls)
        assert any("ZMQ subscription started for" in str(call) for call in info_calls)

    @pytest.mark.asyncio
    async def test_start_zmq_subscription_already_active(
        self, bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify ZMQ subscription skips already active topics.

        Given: A bridge with an already active subscription task for a topic,
        When: Attempting to start subscription for same topic,
        Then: Logs warning and does not create duplicate socket.
        """
        topic = "market.prices"
        bridge.subscriber_tasks[topic] = MagicMock()
        bridge.context = MagicMock()
        with patch("snapper.interface.websocket.bridge.logger") as mock_logger:
            await bridge._start_zmq_subscription(topic)
        warning_calls = list(mock_logger.warning.call_args_list)
        assert any("already active" in str(call) for call in warning_calls)
        bridge.context.socket.assert_not_called()

    @pytest.mark.asyncio
    async def test_start_zmq_subscription_unknown_topic(
        self, bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify ZMQ subscription handles unknown topics.

        Given: A bridge without configuration for a topic,
        When: Attempting to start subscription for unknown topic,
        Then: Logs error and does not create socket.
        """
        topic = "unknown.topic"
        with patch("snapper.interface.websocket.bridge.logger") as mock_logger:
            await bridge._start_zmq_subscription(topic)
        error_calls = list(mock_logger.error.call_args_list)
        assert any("No configuration found for topic" in str(call) for call in error_calls)

    @pytest.mark.asyncio
    async def test_start_zmq_subscription_socket_error(
        self, bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify ZMQ subscription handles socket creation errors.

        Given: A bridge with context that fails on socket creation,
        When: Attempting to start subscription,
        Then: Logs error and does not add to subscriber tasks.
        """
        topic = "market.prices"
        bridge.context = MagicMock()
        bridge.context.socket.side_effect = Exception("Socket creation failed")
        with patch("snapper.interface.websocket.bridge.logger") as mock_logger:
            await bridge._start_zmq_subscription(topic)
        error_calls = list(mock_logger.error.call_args_list)
        assert any("Failed to start ZMQ subscription" in str(call) for call in error_calls)
        assert topic not in bridge.subscriber_tasks
        assert topic not in bridge.zmq_subscribers


class TestZMQSubscriptionLoops:
    """Tests for ZMQ subscription loop management and message processing."""

    @pytest.fixture
    def mock_connection_manager(self) -> MagicMock:
        """Provide mock connection manager."""
        return MagicMock()

    @pytest.fixture
    def mock_settings(self) -> MagicMock:
        """Provide mock settings with ZMQ configuration."""
        settings = MagicMock()
        settings.zmq_enable_broker = False
        settings.zmq_market_url = "tcp://127.0.0.1:5555"
        settings.zmq_trade_url = "tcp://127.0.0.1:5556"
        settings.zmq_strategy_url = "tcp://127.0.0.1:5557"
        settings.zmq_heartbeat_url = "tcp://127.0.0.1:5558"
        settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        settings.zmq_heartbeat_interval_ms = 1000
        return settings

    @pytest.fixture
    def zmq_bridge(
        self, mock_connection_manager: MagicMock, mock_settings: MagicMock
    ) -> ZmqWebSocketBridgeService:
        """Provide ZMQ bridge with mocked context and settings."""
        with (
            patch("snapper.interface.websocket.bridge.get_settings", return_value=mock_settings),
            patch("zmq.asyncio.Context") as mock_context_class,
        ):
            mock_context = MagicMock()
            mock_context_class.return_value = mock_context
            bridge = ZmqWebSocketBridgeService(mock_connection_manager)
            bridge.context = mock_context
            return bridge

    @pytest.mark.asyncio
    async def test_start_zmq_subscription_success(
        self, zmq_bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify ZMQ subscription succeeds with proper setup.

        Given: A bridge with available topic configuration,
        When: Subscribing a client to the topic,
        Then: Client subscription is tracked successfully.
        """
        topic = "market.candles"
        config = TopicConfigurationModel(
            endpoint="tcp://127.0.0.1:5555",
            pattern="market.candles",
            throttle_ms=100,
        )
        zmq_bridge.available_topics[topic] = config
        mock_socket = MagicMock()
        mock_context = MagicMock()
        mock_context.socket.return_value = mock_socket
        zmq_bridge.context = mock_context
        mock_websocket = MagicMock()
        await zmq_bridge.subscribe_client(mock_websocket, [topic])
        assert topic in zmq_bridge.client_subscriptions[mock_websocket]

    @pytest.mark.asyncio
    async def test_subscription_with_socket_error(
        self, zmq_bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify client subscription handles socket errors gracefully.

        Given: A bridge with context that fails on socket creation,
        When: Subscribing a client to a topic,
        Then: Client subscription is still tracked despite socket error.
        """
        topic = "market.candles"
        config = TopicConfigurationModel(
            endpoint="tcp://127.0.0.1:5555",
            pattern="market.candles",
            throttle_ms=100,
        )
        zmq_bridge.available_topics[topic] = config
        mock_context = MagicMock()
        mock_context.socket.side_effect = Exception("Socket creation failed")
        zmq_bridge.context = mock_context
        mock_websocket = MagicMock()
        await zmq_bridge.subscribe_client(mock_websocket, [topic])
        assert mock_websocket in zmq_bridge.client_subscriptions

    @pytest.mark.asyncio
    async def test_zmq_message_processing_mocked(
        self, zmq_bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify ZMQ message forwarding to subscribed clients.

        Given: A bridge with a client subscribed to a topic,
        When: Forwarding a message to clients,
        Then: Message is sent to the subscribed WebSocket.
        """
        topic = "market.kraken.BTC-USD.candles"
        mock_websocket = MagicMock()
        mock_websocket.send_text = AsyncMock()
        subscription = TopicSubscriptionModel(
            websocket=mock_websocket, throttle_ms=100, last_sent=0.0, client_id="test_client"
        )
        zmq_bridge.topic_subscriptions[topic] = [subscription]
        zmq_bridge.topic_metrics[topic] = TopicMetricsModel()
        test_payload = {
            "instrument": "BTC-USD",
            "exchange": "kraken",
            "timeframe": "1m",
            "open": 50000.0,
            "high": 51000.0,
            "low": 49000.0,
            "close": 50500.0,
            "volume": 100.0,
            "timestamp": "2024-01-01T00:00:00+00:00",
        }
        await zmq_bridge._forward_to_clients(topic, topic, test_payload)
        mock_websocket.send_text.assert_called_once()

    @pytest.mark.asyncio
    async def test_zmq_message_throttling(self, zmq_bridge: ZmqWebSocketBridgeService) -> None:
        """Verify message throttling based on throttle_ms setting.

        Given: A subscription with high throttle_ms recently sent,
        When: Attempting to forward another message immediately,
        Then: Message is not sent due to throttling.
        """
        topic = "market.kraken.BTC-USD.candles"
        mock_websocket = MagicMock()
        mock_websocket.send_text = AsyncMock()
        current_time = time.time()
        subscription = TopicSubscriptionModel(
            websocket=mock_websocket,
            throttle_ms=1000,
            last_sent=current_time,
            client_id="test_client",
        )
        zmq_bridge.topic_subscriptions[topic] = [subscription]
        with patch("time.time", return_value=current_time):
            test_payload = {
                "instrument": "BTC-USD",
                "exchange": "kraken",
                "timeframe": "1m",
                "open": 50000.0,
                "high": 51000.0,
                "low": 49000.0,
                "close": 50500.0,
                "volume": 100.0,
                "timestamp": "2024-01-01T00:00:00+00:00",
            }
            await zmq_bridge._forward_to_clients(topic, topic, test_payload)
            mock_websocket.send_text.assert_not_called()

    @pytest.mark.asyncio
    async def test_topic_metrics_tracking(self, zmq_bridge: ZmqWebSocketBridgeService) -> None:
        """Verify topic metrics tracking works correctly.

        Given: A bridge with topic metrics for a topic,
        When: Setting various metric counts,
        Then: Metrics are stored and retrievable correctly.
        """
        topic = "market.candles"
        zmq_bridge.topic_metrics[topic] = TopicMetricsModel()
        metrics = zmq_bridge.topic_metrics[topic]
        metrics.received_count = 10
        metrics.forwarded_count = 8
        metrics.throttled_count = 2
        metrics.error_count = 0
        assert metrics.received_count == 10
        assert metrics.forwarded_count == 8
        assert metrics.throttled_count == 2
        assert metrics.error_count == 0

    @pytest.mark.asyncio
    async def test_websocket_disconnection_cleanup(
        self, zmq_bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify client unsubscription cleans up subscriptions.

        Given: A client subscribed to a topic,
        When: Client unsubscribes from the topic,
        Then: Subscription data is cleaned up properly.
        """
        topic = "market.candles"
        mock_websocket = MagicMock()
        await zmq_bridge.subscribe_client(mock_websocket, [topic])
        assert mock_websocket in zmq_bridge.client_subscriptions
        assert topic in zmq_bridge.topic_subscriptions
        await zmq_bridge.unsubscribe_client(mock_websocket, [topic])
        if mock_websocket in zmq_bridge.client_subscriptions:
            assert len(zmq_bridge.client_subscriptions[mock_websocket]) == 0
        if topic in zmq_bridge.topic_subscriptions:
            assert len(zmq_bridge.topic_subscriptions[topic]) == 0


TEST_TIMEOUT = 5.0


@pytest.fixture
def mock_connection_manager() -> MagicMock:
    """Provide mock connection manager."""
    return MagicMock()


@pytest.fixture
def mock_websocket() -> MagicMock:
    """Provide mock WebSocket with async send."""
    ws = MagicMock(spec=WebSocket)
    ws.send_text = AsyncMock()
    return ws


@pytest.fixture
def mock_settings() -> AppSettings:
    """Provide mock AppSettings with ZMQ configuration."""
    settings = MagicMock(spec=AppSettings)
    settings.zmq_enable_broker = False
    settings.zmq_market_url = "tcp://localhost:5555"
    settings.zmq_trade_url = "tcp://localhost:5556"
    settings.zmq_strategy_url = "tcp://localhost:5557"
    settings.zmq_heartbeat_url = "tcp://localhost:5558"
    return settings


@pytest.fixture
def zmq_bridge(
    mock_connection_manager: MagicMock, mock_settings: AppSettings
) -> ZmqWebSocketBridgeService:
    """Provide ZMQ bridge with mocked context."""
    with (
        patch("snapper.interface.websocket.bridge.get_settings", return_value=mock_settings),
        patch("zmq.asyncio.Context") as mock_context_class,
    ):
        mock_context = MagicMock()
        mock_context_class.return_value = mock_context
        bridge = ZmqWebSocketBridgeService(mock_connection_manager)
        bridge.context = mock_context
        return bridge


class TestZMQWebSocketBridge:
    """Tests for ZmqWebSocketBridgeService core functionality."""

    @pytest.mark.asyncio
    async def test_subscribe_client(
        self, zmq_bridge: ZmqWebSocketBridgeService, mock_websocket: AsyncMock
    ) -> None:
        """Verify client subscription to multiple topics.

        Given: A bridge and WebSocket client,
        When: Subscribing client to multiple topics,
        Then: Client subscriptions and metrics are tracked correctly.
        """
        topics = ["market.candles", "signals.kraken.BTC-USD.live"]
        start_calls: list[str] = []

        async def start_stub(topic: str) -> None:
            start_calls.append(topic)

        with patch.object(zmq_bridge, "_start_zmq_subscription", new=start_stub):
            await zmq_bridge.subscribe_client(mock_websocket, topics)
        assert mock_websocket in zmq_bridge.client_subscriptions
        assert zmq_bridge.client_subscriptions[mock_websocket] == set(topics)
        for topic in topics:
            assert topic in zmq_bridge.topic_subscriptions
            assert len(zmq_bridge.topic_subscriptions[topic]) == 1
        for topic in topics:
            assert topic in zmq_bridge.topic_metrics
            assert zmq_bridge.topic_metrics[topic].active_subscribers == 1
        assert start_calls == topics

    @pytest.mark.asyncio
    async def test_unsubscribe_client(
        self, zmq_bridge: ZmqWebSocketBridgeService, mock_websocket: AsyncMock
    ) -> None:
        """Verify client unsubscription from topic.

        Given: A client subscribed to multiple topics,
        When: Unsubscribing from one topic,
        Then: Only unsubscribed topic is removed, others remain.
        """
        await zmq_bridge.subscribe_client(
            mock_websocket, ["market.candles", "signals.kraken.BTC-USD.live"]
        )
        stop_calls: list[str] = []

        async def stop_stub(topic: str) -> None:
            stop_calls.append(topic)

        with patch.object(zmq_bridge, "_stop_zmq_subscription", new=stop_stub):
            await zmq_bridge.unsubscribe_client(mock_websocket, ["market.candles"])
        assert zmq_bridge.client_subscriptions[mock_websocket] == {"signals.kraken.BTC-USD.live"}
        assert "market.candles" not in zmq_bridge.topic_subscriptions
        assert stop_calls == ["market.candles"]

    @pytest.mark.asyncio
    async def test_disconnect_client(
        self, zmq_bridge: ZmqWebSocketBridgeService, mock_websocket: AsyncMock
    ) -> None:
        """Verify client disconnection removes all subscriptions.

        Given: A client subscribed to multiple topics,
        When: Disconnecting the client,
        Then: Client is removed from all subscriptions.
        """
        mock_websocket.close = AsyncMock()
        topics = ["market.candles", "signals.kraken.BTC-USD.live"]
        start_calls: list[str] = []
        stop_calls: list[str] = []

        async def start_stub(topic: str) -> None:
            start_calls.append(topic)

        async def stop_stub(topic: str) -> None:
            stop_calls.append(topic)

        with (
            patch.object(zmq_bridge, "_start_zmq_subscription", new=start_stub),
            patch.object(zmq_bridge, "_stop_zmq_subscription", new=stop_stub),
        ):
            await zmq_bridge.subscribe_client(mock_websocket, topics)
            await zmq_bridge.disconnect_client(mock_websocket)
        assert mock_websocket not in zmq_bridge.client_subscriptions
        assert start_calls == topics
        assert set(stop_calls) == set(topics)


class TestZMQBridgeIntegration:
    """Integration tests for ZMQ bridge subscription flows."""

    @pytest.mark.asyncio
    async def test_full_subscription_flow(
        self, zmq_bridge: ZmqWebSocketBridgeService, mock_websocket: AsyncMock
    ) -> None:
        """Verify complete subscribe/unsubscribe flow.

        Given: A bridge and WebSocket client,
        When: Subscribing and then unsubscribing from topics,
        Then: Client and topic state is managed correctly throughout.
        """
        topics = ["market.candles"]
        start_calls: list[str] = []
        stop_calls: list[str] = []

        async def start_stub(topic: str) -> None:
            start_calls.append(topic)

        async def stop_stub(topic: str) -> None:
            stop_calls.append(topic)

        with (
            patch.object(zmq_bridge, "_start_zmq_subscription", new=start_stub),
            patch.object(zmq_bridge, "_stop_zmq_subscription", new=stop_stub),
        ):
            await zmq_bridge.subscribe_client(mock_websocket, topics)
            assert mock_websocket in zmq_bridge.client_subscriptions
            await zmq_bridge.unsubscribe_client(mock_websocket, topics)
            assert mock_websocket not in zmq_bridge.client_subscriptions
            assert start_calls == topics
            assert stop_calls == topics

    @pytest.mark.asyncio
    async def test_subscription_error_handling(
        self, zmq_bridge: ZmqWebSocketBridgeService, mock_websocket: AsyncMock
    ) -> None:
        """Verify subscription handles ZMQ errors.

        Given: A bridge with failing ZMQ subscription,
        When: Attempting to subscribe a client,
        Then: Error is raised but client subscription is tracked.
        """
        topics = ["market.candles"]

        async def start_fail(topic: str) -> None:
            raise RuntimeError("ZMQ Error")

        with (
            patch.object(zmq_bridge, "_start_zmq_subscription", new=start_fail),
            pytest.raises(RuntimeError, match="ZMQ Error"),
        ):
            await zmq_bridge.subscribe_client(mock_websocket, topics)
        assert mock_websocket in zmq_bridge.client_subscriptions

    @pytest.mark.asyncio
    async def test_throttling_configuration(self, zmq_bridge: ZmqWebSocketBridgeService) -> None:
        """Verify all topics have throttling configuration.

        Given: A bridge with available topics,
        When: Checking topic configurations,
        Then: All configs have throttle_ms attribute >= 0.
        """
        for config in zmq_bridge.available_topics.values():
            assert hasattr(config, "throttle_ms")
            assert config.throttle_ms >= 0

    @pytest.mark.asyncio
    async def test_websocket_error_handling(self, zmq_bridge: ZmqWebSocketBridgeService) -> None:
        """Verify WebSocket send errors disconnect broken clients.

        Given: A subscribed client that raises exception on send,
        When: Forwarding a message to the client,
        Then: Client is disconnected via disconnect_client.
        """
        mock_websocket = AsyncMock(spec=WebSocket)
        topic = "market.candles"
        await zmq_bridge.subscribe_client(mock_websocket, [topic])
        mock_websocket.send_text.side_effect = Exception("Connection closed")
        subscription = TopicSubscriptionModel(websocket=mock_websocket, throttle_ms=100)
        zmq_bridge.topic_subscriptions[topic] = [subscription]
        valid_candle_payload = json.dumps(
            {
                "type": "candle",
                "instrument": "BTC-USD",
                "exchange": "kraken",
                "timeframe": "1m",
                "open": 50000.0,
                "high": 50100.0,
                "low": 49900.0,
                "close": 50050.0,
                "volume": 100.0,
                "timestamp": "2024-01-01T00:00:00+00:00",
            }
        )
        zmq_bridge.disconnect_client = AsyncMock()
        await zmq_bridge._forward_to_clients(topic, "market.BTCUSD.candles", valid_candle_payload)
        zmq_bridge.disconnect_client.assert_awaited_once_with(mock_websocket)


class TestZMQBridgeHelperMethods:
    """Tests for ZMQ bridge helper methods."""

    def test_topic_configuration_categories(
        self, mock_connection_manager: MagicMock, mock_settings: AppSettings
    ) -> None:
        """Verify topic configurations are categorized correctly.

        Given: A topic registry with various topic categories,
        When: Creating a ZMQ bridge,
        Then: Topics are registered in available_topics by category.
        """
        with (
            patch("snapper.interface.websocket.bridge.get_settings", return_value=mock_settings),
            patch("snapper.interface.websocket.bridge.TOPIC_REGISTRY") as mock_registry,
        ):
            mock_registry.items.return_value = [
                ("market.candles", MagicMock(category="market", pattern="test", throttle_ms=100)),
                (
                    "signals.kraken.BTC-USD.live",
                    MagicMock(category="trade", pattern="test", throttle_ms=200),
                ),
                (
                    "strategy.signals",
                    MagicMock(category="strategy", pattern="test", throttle_ms=300),
                ),
                (
                    "system.heartbeats.",
                    MagicMock(category="system", pattern="test", throttle_ms=400),
                ),
                ("unknown.topic", MagicMock(category="unknown", pattern="test", throttle_ms=500)),
            ]
            bridge = ZmqWebSocketBridgeService(mock_connection_manager)
            assert "market.candles" in bridge.available_topics
            assert "signals.kraken.BTC-USD.live" in bridge.available_topics
            assert "strategy.signals" in bridge.available_topics
            assert "system.heartbeats." in bridge.available_topics


class TestZMQBridgePublicInterface:
    """Tests for ZMQ bridge public interface methods."""

    @pytest.mark.asyncio
    async def test_concurrent_subscriptions(self, zmq_bridge: ZmqWebSocketBridgeService) -> None:
        """Verify concurrent subscriptions are handled correctly.

        Given: Multiple WebSocket clients,
        When: Subscribing all clients to same topic concurrently,
        Then: All subscriptions are tracked with correct subscriber count.
        """
        clients = [AsyncMock(spec=WebSocket) for _ in range(5)]
        topic = "market.candles"
        tasks = [zmq_bridge.subscribe_client(client, [topic]) for client in clients]
        await asyncio.gather(*tasks)
        assert len(zmq_bridge.topic_subscriptions[topic]) == 5
        assert zmq_bridge.topic_metrics[topic].active_subscribers == 5

    @pytest.mark.asyncio
    async def test_subscription_state_consistency(
        self, zmq_bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify subscription state consistency after partial unsubscribe.

        Given: A client subscribed to multiple topics,
        When: Unsubscribing from one topic,
        Then: State remains consistent for remaining subscriptions.
        """
        client = AsyncMock(spec=WebSocket)
        topics = ["market.candles", "signals.kraken.BTC-USD.live", "strategy.signals"]
        await zmq_bridge.subscribe_client(client, topics)
        assert client in zmq_bridge.client_subscriptions
        assert zmq_bridge.client_subscriptions[client] == set(topics)
        for topic in topics:
            assert topic in zmq_bridge.topic_subscriptions
            assert topic in zmq_bridge.topic_metrics
            assert zmq_bridge.topic_metrics[topic].active_subscribers == 1
        await zmq_bridge.unsubscribe_client(client, ["market.candles"])
        remaining = {"signals.kraken.BTC-USD.live", "strategy.signals"}
        assert zmq_bridge.client_subscriptions[client] == remaining
        assert "market.candles" not in zmq_bridge.topic_subscriptions

    @pytest.mark.asyncio
    async def test_edge_case_operations(self, zmq_bridge: ZmqWebSocketBridgeService) -> None:
        """Verify bridge handles edge case operations gracefully.

        Given: A bridge with no active subscriptions,
        When: Attempting unsubscribe/disconnect on untracked client,
        Then: Operations complete without errors.
        """
        client = AsyncMock(spec=WebSocket)
        await zmq_bridge.unsubscribe_client(client, ["market.candles"])
        assert client not in zmq_bridge.client_subscriptions
        await zmq_bridge.disconnect_client(client)
        assert client not in zmq_bridge.client_subscriptions
        await zmq_bridge.subscribe_client(client, [])

    @pytest.mark.asyncio
    async def test_topic_subscription_cleanup(self, zmq_bridge: ZmqWebSocketBridgeService) -> None:
        """Verify ZMQ subscription is stopped when last subscriber leaves.

        Given: A single client subscribed to a topic,
        When: Client unsubscribes from the topic,
        Then: ZMQ subscription is stopped for that topic.
        """
        client = AsyncMock(spec=WebSocket)
        topic = "market.candles"
        await zmq_bridge.subscribe_client(client, [topic])
        assert topic in zmq_bridge.topic_subscriptions
        stop_calls: list[str] = []

        async def stop_stub(stop_topic: str) -> None:
            stop_calls.append(stop_topic)

        with patch.object(zmq_bridge, "_stop_zmq_subscription", new=stop_stub):
            await zmq_bridge.unsubscribe_client(client, [topic])
            assert stop_calls == [topic]
        assert topic not in zmq_bridge.topic_subscriptions


def make_tick_payload_json() -> str:
    """Create JSON tick payload for testing."""
    return json.dumps(
        {
            "type": "tick",
            "instrument": "BTC-USD",
            "exchange": "kraken",
            "bid": 50000.0,
            "ask": 50001.0,
            "last": 50000.5,
            "volume": 1234.5,
            "timestamp": datetime.now(tz=UTC).isoformat(),
        }
    )


class TestZMQForwardToClients:
    """Tests for ZMQ bridge _forward_to_clients method."""

    @pytest.fixture
    def bridge(self) -> ZmqWebSocketBridgeService:
        """Provide ZMQ bridge with empty subscriptions."""
        mock_connection_manager = MagicMock()
        bridge = ZmqWebSocketBridgeService(mock_connection_manager)
        bridge.topic_subscriptions = {}
        bridge.topic_metrics = {}
        return bridge

    @pytest.mark.asyncio
    async def test_forward_to_clients_no_subscriptions(
        self, bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify forwarding to empty subscriptions is no-op.

        Given: A bridge with no subscriptions for a topic,
        When: Forwarding a message to clients,
        Then: No errors occur and method completes.
        """
        topic = "market.kraken.BTC-USD.ticks"
        received_topic = "market.kraken.BTC-USD.ticks"
        payload_json = make_tick_payload_json()
        assert topic not in bridge.topic_subscriptions
        await bridge._forward_to_clients(topic, received_topic, payload_json)

    @pytest.mark.asyncio
    async def test_forward_to_clients_with_throttling(
        self, bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify throttling applies correctly to subscriptions.

        Given: Two subscriptions with different throttle timings,
        When: Forwarding a message,
        Then: Throttled subscription skips, non-throttled receives message.
        """
        topic = "market.kraken.BTC-USD.ticks"
        received_topic = "market.kraken.BTC-USD.ticks"
        payload_json = make_tick_payload_json()
        mock_ws1 = AsyncMock()
        mock_ws2 = AsyncMock()
        current_time = time.time()
        subscription1 = TopicSubscriptionModel(
            websocket=mock_ws1,
            throttle_ms=1000,
            last_sent=current_time - 0.5,
            client_id="client1",
        )
        subscription2 = TopicSubscriptionModel(
            websocket=mock_ws2,
            throttle_ms=500,
            last_sent=current_time - 1.0,
            client_id="client2",
        )
        bridge.topic_subscriptions[topic] = [subscription1, subscription2]
        bridge.topic_metrics[topic] = MagicMock()
        bridge.topic_metrics[topic].throttled_count = 0
        bridge.topic_metrics[topic].forwarded_count = 0
        await bridge._forward_to_clients(topic, received_topic, payload_json)
        assert mock_ws1.send_text.call_count == 0
        assert mock_ws2.send_text.call_count == 1
        assert bridge.topic_metrics[topic].throttled_count == 1
        assert bridge.topic_metrics[topic].forwarded_count == 1
        sent_message = mock_ws2.send_text.call_args[0][0]
        assert sent_message == payload_json
        assert subscription2.last_sent >= current_time

    @pytest.mark.asyncio
    async def test_forward_to_clients_websocket_error(
        self, bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify WebSocket errors trigger client disconnection.

        Given: A subscription with WebSocket that throws on send,
        When: Forwarding a message to the client,
        Then: Client is disconnected and warning is logged.
        """
        topic = "market.kraken.BTC-USD.ticks"
        received_topic = "market.kraken.BTC-USD.ticks"
        payload_json = make_tick_payload_json()
        mock_ws = AsyncMock()
        mock_ws.send_text.side_effect = Exception("WebSocket closed")
        subscription = TopicSubscriptionModel(
            websocket=mock_ws,
            throttle_ms=0,
            last_sent=0,
            client_id="client1",
        )
        bridge.topic_subscriptions[topic] = [subscription]
        bridge.topic_metrics[topic] = MagicMock()
        bridge.topic_metrics[topic].throttled_count = 0
        bridge.topic_metrics[topic].forwarded_count = 0
        with (
            patch.object(bridge, "disconnect_client", new_callable=AsyncMock) as mock_disconnect,
            patch("snapper.interface.websocket.bridge.logger") as mock_logger,
        ):
            await bridge._forward_to_clients(topic, received_topic, payload_json)
        mock_logger.warning.assert_called_once()
        assert "Failed to send message to client client1" in mock_logger.warning.call_args[0][0]
        mock_disconnect.assert_called_once_with(mock_ws)
        assert bridge.topic_metrics[topic].forwarded_count == 0

    @pytest.mark.asyncio
    async def test_forward_to_clients_no_metrics(self, bridge: ZmqWebSocketBridgeService) -> None:
        """Verify forwarding works without metrics tracking.

        Given: A subscription without topic metrics initialized,
        When: Forwarding a message,
        Then: Message is sent successfully.
        """
        topic = "market.kraken.BTC-USD.ticks"
        received_topic = "market.kraken.BTC-USD.ticks"
        payload_json = make_tick_payload_json()
        mock_ws = AsyncMock()
        subscription = TopicSubscriptionModel(
            websocket=mock_ws,
            throttle_ms=0,
            last_sent=0,
            client_id="client1",
        )
        bridge.topic_subscriptions[topic] = [subscription]
        await bridge._forward_to_clients(topic, received_topic, payload_json)
        mock_ws.send_text.assert_called_once()
        sent_message = mock_ws.send_text.call_args[0][0]
        assert sent_message == payload_json

    @pytest.mark.asyncio
    async def test_forward_to_clients_multiple_clients(
        self, bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify message forwarded to all subscribed clients.

        Given: Multiple clients subscribed to a topic,
        When: Forwarding a message,
        Then: All clients receive the message and metrics updated.
        """
        topic = "market.kraken.BTC-USD.ticks"
        received_topic = "market.kraken.BTC-USD.ticks"
        payload_json = make_tick_payload_json()
        mock_ws1 = AsyncMock()
        mock_ws2 = AsyncMock()
        mock_ws3 = AsyncMock()
        subscriptions = [
            TopicSubscriptionModel(
                websocket=mock_ws1, throttle_ms=0, last_sent=0, client_id="client1"
            ),
            TopicSubscriptionModel(
                websocket=mock_ws2, throttle_ms=0, last_sent=0, client_id="client2"
            ),
            TopicSubscriptionModel(
                websocket=mock_ws3, throttle_ms=0, last_sent=0, client_id="client3"
            ),
        ]
        bridge.topic_subscriptions[topic] = subscriptions
        bridge.topic_metrics[topic] = MagicMock()
        bridge.topic_metrics[topic].throttled_count = 0
        bridge.topic_metrics[topic].forwarded_count = 0
        await bridge._forward_to_clients(topic, received_topic, payload_json)
        mock_ws1.send_text.assert_called_once()
        mock_ws2.send_text.assert_called_once()
        mock_ws3.send_text.assert_called_once()
        assert bridge.topic_metrics[topic].throttled_count == 0
        assert bridge.topic_metrics[topic].forwarded_count == 3
        for subscription in subscriptions:
            assert subscription.last_sent > 0


@pytest.fixture
def mock_connection_manager_v2() -> MagicMock:
    """Provide mock connection manager (v2)."""
    return MagicMock()


@pytest.fixture
def mock_websocket_v2() -> MagicMock:
    """Provide mock WebSocket (v2)."""
    ws = MagicMock(spec=WebSocket)
    ws.send_text = AsyncMock()
    return ws


@pytest.fixture
def mock_settings_v2() -> AppSettings:
    """Provide mock AppSettings with extended ZMQ config (v2)."""
    settings = MagicMock(spec=AppSettings)
    settings.zmq_enable_broker = False
    settings.zmq_market_url = "tcp://127.0.0.1:5555"
    settings.zmq_trade_url = "tcp://127.0.0.1:5556"
    settings.zmq_strategy_url = "tcp://127.0.0.1:5557"
    settings.zmq_heartbeat_url = "tcp://127.0.0.1:5558"
    settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
    settings.zmq_heartbeat_interval_ms = 1000
    return settings


@pytest.fixture
def zmq_bridge_v2(
    mock_connection_manager: MagicMock, mock_settings: AppSettings
) -> Generator[ZmqWebSocketBridgeService]:
    """Provide ZMQ bridge generator (v2)."""
    with (
        patch("snapper.interface.websocket.bridge.get_settings", return_value=mock_settings),
        patch("zmq.asyncio.Context") as mock_context_class,
    ):
        mock_context = MagicMock()
        mock_context_class.return_value = mock_context
        bridge = ZmqWebSocketBridgeService(mock_connection_manager)
        bridge.context = mock_context
        yield bridge


class TestZMQWebSocketBridgeCore:
    """Core tests for ZmqWebSocketBridgeService subscription management."""

    @pytest.mark.asyncio
    async def test_subscribe_client_new_topic(
        self, zmq_bridge: ZmqWebSocketBridgeService, mock_websocket: MagicMock
    ) -> None:
        """Verify subscribing client to new topics.

        Given: A bridge and WebSocket client,
        When: Subscribing client to new topics,
        Then: Subscriptions are created and ZMQ subscriptions started.
        """
        topics = ["market.candles.", "signals.kraken.BTC-USD.live"]
        with patch.object(zmq_bridge, "_start_zmq_subscription") as mock_start:
            await zmq_bridge.subscribe_client(mock_websocket, topics)
        assert mock_websocket in zmq_bridge.client_subscriptions
        assert zmq_bridge.client_subscriptions[mock_websocket] == set(topics)
        for topic in topics:
            assert topic in zmq_bridge.topic_subscriptions
            assert len(zmq_bridge.topic_subscriptions[topic]) == 1
            assert zmq_bridge.topic_subscriptions[topic][0].websocket == mock_websocket
        for topic in topics:
            assert topic in zmq_bridge.topic_metrics
            assert zmq_bridge.topic_metrics[topic].active_subscribers == 1
        assert mock_start.call_count == 2

    @pytest.mark.asyncio
    async def test_subscribe_client_existing_client(
        self, zmq_bridge: ZmqWebSocketBridgeService, mock_websocket: MagicMock
    ) -> None:
        """Verify adding subscription to existing client.

        Given: A client already subscribed to one topic,
        When: Subscribing same client to additional topic,
        Then: Both topics are in client's subscriptions.
        """
        await zmq_bridge.subscribe_client(mock_websocket, ["market.candles."])
        with patch.object(zmq_bridge, "_start_zmq_subscription") as mock_start:
            await zmq_bridge.subscribe_client(mock_websocket, ["signals.kraken.BTC-USD.live"])
        assert zmq_bridge.client_subscriptions[mock_websocket] == {
            "market.candles.",
            "signals.kraken.BTC-USD.live",
        }
        mock_start.assert_called_once_with("signals.kraken.BTC-USD.live")

    @pytest.mark.asyncio
    async def test_subscribe_client_existing_topic(
        self, zmq_bridge: ZmqWebSocketBridgeService, mock_websocket: MagicMock
    ) -> None:
        """Verify second client subscribing to existing topic.

        Given: A topic with one subscriber,
        When: Second client subscribes to same topic,
        Then: Both clients tracked, no new ZMQ subscription started.
        """
        mock_websocket2 = MagicMock(spec=WebSocket)
        await zmq_bridge.subscribe_client(mock_websocket, ["market.candles."])
        with patch.object(zmq_bridge, "_start_zmq_subscription") as mock_start:
            await zmq_bridge.subscribe_client(mock_websocket2, ["market.candles."])
        assert len(zmq_bridge.topic_subscriptions["market.candles."]) == 2
        assert zmq_bridge.topic_metrics["market.candles."].active_subscribers == 2
        mock_start.assert_not_called()

    @pytest.mark.asyncio
    async def test_unsubscribe_client(
        self, zmq_bridge: ZmqWebSocketBridgeService, mock_websocket: MagicMock
    ) -> None:
        """Verify client unsubscription removes from topic.

        Given: A client subscribed to multiple topics,
        When: Unsubscribing from one topic,
        Then: Topic subscription stopped, other topic remains.
        """
        await zmq_bridge.subscribe_client(
            mock_websocket, ["market.candles.", "signals.kraken.BTC-USD.live"]
        )
        with patch.object(zmq_bridge, "_stop_zmq_subscription") as mock_stop:
            await zmq_bridge.unsubscribe_client(mock_websocket, ["market.candles."])
        assert zmq_bridge.client_subscriptions[mock_websocket] == {"signals.kraken.BTC-USD.live"}
        assert "market.candles." not in zmq_bridge.topic_subscriptions
        mock_stop.assert_called_once_with("market.candles.")

    @pytest.mark.asyncio
    async def test_unsubscribe_client_with_other_subscribers(
        self, zmq_bridge: ZmqWebSocketBridgeService, mock_websocket: MagicMock
    ) -> None:
        """Verify unsubscribe keeps topic alive if others subscribed.

        Given: Two clients subscribed to same topic,
        When: One client unsubscribes,
        Then: Topic remains with one subscriber, ZMQ not stopped.
        """
        mock_websocket2 = MagicMock(spec=WebSocket)
        await zmq_bridge.subscribe_client(mock_websocket, ["market.candles."])
        await zmq_bridge.subscribe_client(mock_websocket2, ["market.candles."])
        with patch.object(zmq_bridge, "_stop_zmq_subscription") as mock_stop:
            await zmq_bridge.unsubscribe_client(mock_websocket, ["market.candles."])
        assert len(zmq_bridge.topic_subscriptions["market.candles."]) == 1
        assert zmq_bridge.topic_metrics["market.candles."].active_subscribers == 1
        mock_stop.assert_not_called()

    @pytest.mark.asyncio
    async def test_unsubscribe_client_not_subscribed(
        self, zmq_bridge: ZmqWebSocketBridgeService, mock_websocket: MagicMock
    ) -> None:
        """Verify unsubscribe handles untracked client gracefully.

        Given: A client not tracked in subscriptions,
        When: Attempting to unsubscribe,
        Then: No error occurs, client remains not tracked.
        """
        await zmq_bridge.unsubscribe_client(mock_websocket, ["market.candles."])
        assert mock_websocket not in zmq_bridge.client_subscriptions

    @pytest.mark.asyncio
    async def test_disconnect_client(
        self, zmq_bridge: ZmqWebSocketBridgeService, mock_websocket: MagicMock
    ) -> None:
        """Verify disconnect removes client from all topics.

        Given: A client subscribed to multiple topics,
        When: Disconnecting the client,
        Then: Unsubscribe called with all client's topics.
        """
        await zmq_bridge.subscribe_client(
            mock_websocket, ["market.candles.", "signals.kraken.BTC-USD.live"]
        )
        mock_unsub = AsyncMock()
        with patch.object(zmq_bridge, "unsubscribe_client", new=mock_unsub):
            await zmq_bridge.disconnect_client(mock_websocket)
        mock_unsub.assert_called_once()
        call_args = mock_unsub.call_args[0]
        assert call_args[0] == mock_websocket
        assert set(call_args[1]) == {"market.candles.", "signals.kraken.BTC-USD.live"}

    @pytest.mark.asyncio
    async def test_disconnect_client_not_tracked(
        self, zmq_bridge: ZmqWebSocketBridgeService, mock_websocket: MagicMock
    ) -> None:
        """Verify disconnect handles untracked client gracefully.

        Given: A client not tracked in subscriptions,
        When: Attempting to disconnect,
        Then: No error occurs, client remains not tracked.
        """
        await zmq_bridge.disconnect_client(mock_websocket)
        assert mock_websocket not in zmq_bridge.client_subscriptions


TEST_TIMEOUT = 5.0


@pytest.fixture
def mock_settings_v2_v2() -> AppSettings:
    """Provide mock AppSettings with collection config."""
    settings = MagicMock(spec=AppSettings)
    settings.ZMQ_COLLECT_PORT = 5555
    settings.ZMQ_HOST = "127.0.0.1"
    settings.ZMQ_TIMEOUT_MS = 1000
    settings.enable_collection = True
    settings.zmq_enable_broker = False
    settings.zmq_market_url = "tcp://127.0.0.1:5555"
    settings.zmq_trade_url = "tcp://127.0.0.1:5556"
    settings.zmq_strategy_url = "tcp://127.0.0.1:5557"
    settings.zmq_heartbeat_url = "tcp://127.0.0.1:5558"
    settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
    settings.zmq_heartbeat_interval_ms = 1000
    return settings


@pytest.fixture
def mock_connection_manager_v2_v2() -> MagicMock:
    """Provide mock connection manager (v2_v2)."""
    return MagicMock()


@pytest.fixture
def zmq_bridge_v2_v2(
    mock_connection_manager: MagicMock, mock_settings: AppSettings
) -> Generator[ZmqWebSocketBridgeService]:
    """Provide ZMQ bridge generator (v2_v2)."""
    with (
        patch("snapper.interface.websocket.bridge.get_settings", return_value=mock_settings),
        patch("zmq.asyncio.Context") as mock_context_class,
    ):
        mock_context = MagicMock()
        mock_context_class.return_value = mock_context
        bridge = ZmqWebSocketBridgeService(mock_connection_manager)
        bridge.context = mock_context
        yield bridge


@pytest.fixture
def mock_websocket_v2_v2() -> AsyncMock:
    """Provide async mock WebSocket (v2_v2)."""
    websocket = AsyncMock(spec=WebSocket)
    websocket.send_text = AsyncMock()
    websocket.receive_text = AsyncMock()
    websocket.close = AsyncMock()
    return websocket


class TestZMQBridgeAdditionalCoverage:
    """Additional coverage tests for ZmqWebSocketBridgeService."""

    @pytest.mark.asyncio
    async def test_subscribe_client_new_topic(self, zmq_bridge: ZmqWebSocketBridgeService) -> None:
        """Verify subscribing to a completely new topic.

        Given: A bridge without any subscriptions,
        When: Subscribing a client to a new topic,
        Then: Topic subscription and client tracking are created.
        """
        mock_websocket = AsyncMock(spec=WebSocket)
        topic = "test.new.topic"
        with patch.object(zmq_bridge, "_start_zmq_subscription") as mock_start:
            await zmq_bridge.subscribe_client(mock_websocket, [topic])
        assert topic in zmq_bridge.topic_subscriptions
        assert len(zmq_bridge.topic_subscriptions[topic]) == 1
        assert zmq_bridge.topic_subscriptions[topic][0].websocket == mock_websocket
        assert mock_websocket in zmq_bridge.client_subscriptions
        assert topic in zmq_bridge.client_subscriptions[mock_websocket]
        mock_start.assert_called_once_with(topic)

    @pytest.mark.asyncio
    async def test_unsubscribe_client_existing_topic(
        self, zmq_bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify unsubscribing removes topic when last subscriber.

        Given: A client as sole subscriber to a topic,
        When: Unsubscribing from the topic,
        Then: Topic is removed and ZMQ subscription stopped.
        """
        mock_websocket = AsyncMock(spec=WebSocket)
        topic = "market.candles"
        await zmq_bridge.subscribe_client(mock_websocket, [topic])
        with patch.object(zmq_bridge, "_stop_zmq_subscription") as mock_stop:
            await zmq_bridge.unsubscribe_client(mock_websocket, [topic])
        assert topic not in zmq_bridge.topic_subscriptions
        if mock_websocket in zmq_bridge.client_subscriptions:
            assert len(zmq_bridge.client_subscriptions[mock_websocket]) == 0
        mock_stop.assert_called_once_with(topic)

    @pytest.mark.asyncio
    async def test_start_blocks_until_stop(self, zmq_bridge: ZmqWebSocketBridgeService) -> None:
        """Verify bridge start blocks until stop is called.

        Given: A bridge with no context,
        When: Starting the bridge and calling stop,
        Then: Start task completes after stop is called.
        """
        zmq_bridge.context = None
        start_task = asyncio.create_task(zmq_bridge.start())
        await asyncio.sleep(0)
        assert not start_task.done()
        await zmq_bridge.stop()
        await asyncio.wait_for(start_task, timeout=0.1)

    @pytest.mark.asyncio
    async def test_disconnect_client_with_subscriptions(
        self, zmq_bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify disconnect unsubscribes from all client topics.

        Given: A client subscribed to multiple topics,
        When: Disconnecting the client,
        Then: Unsubscribe is called with all subscribed topics.
        """
        mock_websocket = AsyncMock(spec=WebSocket)
        topics = ["market.candles", "signals.kraken.BTC-USD.live"]
        await zmq_bridge.subscribe_client(mock_websocket, topics)
        with patch.object(zmq_bridge, "unsubscribe_client") as mock_unsub:
            await zmq_bridge.disconnect_client(mock_websocket)
        mock_unsub.assert_called_once()
        call_args = mock_unsub.call_args[0]
        assert call_args[0] == mock_websocket
        assert set(call_args[1]) == set(topics)

    @pytest.mark.asyncio
    async def test_start_zmq_subscription_creates_socket(
        self, zmq_bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify ZMQ subscription creates socket and task.

        Given: A bridge with ZMQ context,
        When: Starting ZMQ subscription for a topic,
        Then: Socket is created and subscription task started.
        """
        topic = "market"
        mock_socket = MagicMock()
        mock_zmq_loop = MagicMock(return_value=None)
        with (
            patch.object(
                zmq_bridge.context, "socket", return_value=mock_socket
            ) as mock_socket_method,
            patch.object(zmq_bridge, "_zmq_subscription_loop", new=mock_zmq_loop),
        ):
            mock_task = MagicMock()
            mock_task.done.return_value = False
            with patch("asyncio.create_task", return_value=mock_task):
                await zmq_bridge._start_zmq_subscription(topic)
        mock_socket_method.assert_called_once()
        assert topic in zmq_bridge.zmq_subscribers
        assert topic in zmq_bridge.subscriber_tasks

    @pytest.mark.asyncio
    async def test_stop_zmq_subscription_cleanup(
        self, zmq_bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify stopping ZMQ subscription cleans up resources.

        Given: A bridge with active ZMQ subscription,
        When: Stopping the subscription,
        Then: Socket is closed, task cancelled, and removed from tracking.
        """
        topic = "market"
        mock_socket = MagicMock()

        async def dummy_task() -> None:
            await asyncio.sleep(10)

        mock_task = asyncio.create_task(dummy_task())
        zmq_bridge.zmq_subscribers[topic] = mock_socket
        zmq_bridge.subscriber_tasks[topic] = mock_task
        await zmq_bridge._stop_zmq_subscription(topic)
        assert topic not in zmq_bridge.zmq_subscribers
        assert topic not in zmq_bridge.subscriber_tasks
        assert mock_task.cancelled()
        mock_socket.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_forward_to_clients_success(self, zmq_bridge: ZmqWebSocketBridgeService) -> None:
        """Verify message forwarded to all subscribed WebSockets.

        Given: Multiple WebSockets subscribed to a topic,
        When: Forwarding a message,
        Then: All WebSockets receive the message.
        """
        topic = "market.kraken.BTC-USD.candles"
        zmq_topic = "market.kraken.BTC-USD.candles.1m"
        message_str = '{"type":"tick","instrument":"BTC-USD","exchange":"kraken","timestamp":"2024-01-01T00:00:00+00:00"}'
        mock_ws1 = AsyncMock(spec=WebSocket)
        mock_ws2 = AsyncMock(spec=WebSocket)
        zmq_bridge.topic_subscriptions[topic] = [
            TopicSubscriptionModel(websocket=mock_ws1),
            TopicSubscriptionModel(websocket=mock_ws2),
        ]
        await zmq_bridge._forward_to_clients(topic, zmq_topic, message_str)
        for ws in [mock_ws1, mock_ws2]:
            ws.send_text.assert_called_once_with(message_str)

    @pytest.mark.asyncio
    async def test_forward_to_clients_throttled(
        self, zmq_bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify throttled subscriptions skip message.

        Given: A subscription that was recently sent to,
        When: Forwarding another message within throttle window,
        Then: Message is not sent to throttled WebSocket.
        """
        topic = "market.kraken.BTC-USD.candles"
        zmq_topic = "market.kraken.BTC-USD.candles.1m"
        message_str = '{"type":"tick","instrument":"BTC-USD"}'
        throttle_ms = 1000
        mock_websocket = AsyncMock(spec=WebSocket)
        subscription = TopicSubscriptionModel(websocket=mock_websocket, throttle_ms=throttle_ms)
        subscription.last_sent = time.time()
        zmq_bridge.topic_subscriptions[topic] = [subscription]
        await zmq_bridge._forward_to_clients(topic, zmq_topic, message_str)
        mock_websocket.send_text.assert_not_called()

    @pytest.mark.asyncio
    async def test_forward_to_clients_error_handling_disconnects(
        self, zmq_bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify error handling disconnects failed client.

        Given: A subscription with WebSocket that fails on send,
        When: Forwarding a message,
        Then: Client is disconnected via disconnect_client.
        """
        topic = "market.kraken.BTC-USD.candles"
        zmq_topic = "market.kraken.BTC-USD.candles.1m"
        message_str = '{"type":"tick","instrument":"BTC-USD"}'
        throttle_ms = 100
        mock_websocket = AsyncMock(spec=WebSocket)
        subscription = TopicSubscriptionModel(websocket=mock_websocket, throttle_ms=throttle_ms)
        zmq_bridge.topic_subscriptions[topic] = [subscription]
        mock_websocket.send_text.side_effect = Exception("Send failed")
        zmq_bridge.disconnect_client = AsyncMock()
        await zmq_bridge._forward_to_clients(topic, zmq_topic, message_str)
        zmq_bridge.disconnect_client.assert_awaited_once_with(mock_websocket)

    @pytest.mark.asyncio
    async def test_client_subscription_edge_cases(
        self, zmq_bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify subscription edge cases are handled.

        Given: A client subscribed to a topic,
        When: Subscribing again or unsubscribing from unknown topic,
        Then: Operations complete without errors.
        """
        mock_websocket = AsyncMock(spec=WebSocket)
        topic = "signals.kraken.BTC-USD.live"
        await zmq_bridge.subscribe_client(mock_websocket, [topic])
        initial_count = len(zmq_bridge.topic_subscriptions[topic])
        await zmq_bridge.subscribe_client(mock_websocket, [topic])
        assert len(zmq_bridge.topic_subscriptions[topic]) == initial_count + 1
        assert len(zmq_bridge.client_subscriptions[mock_websocket]) == 1
        await zmq_bridge.unsubscribe_client(mock_websocket, ["strategy.signals"])
        assert topic in zmq_bridge.topic_subscriptions
        assert topic in zmq_bridge.client_subscriptions[mock_websocket]

    @pytest.mark.asyncio
    async def test_metrics_tracking(self, zmq_bridge: ZmqWebSocketBridgeService) -> None:
        """Verify metrics are tracked through subscription lifecycle.

        Given: A bridge with topic subscriptions,
        When: Subscribing and unsubscribing a client,
        Then: Topic subscriptions are tracked correctly.
        """
        topic = "market.candles"
        mock_websocket = AsyncMock(spec=WebSocket)
        await zmq_bridge.subscribe_client(mock_websocket, [topic])
        assert topic in zmq_bridge.topic_subscriptions
        assert len(zmq_bridge.topic_subscriptions[topic]) == 1
        await zmq_bridge.unsubscribe_client(mock_websocket, [topic])
        assert topic not in zmq_bridge.topic_subscriptions


class TestZmqWsBridgeE2ESmoke:
    """End-to-end smoke tests for ZMQ WebSocket bridge."""

    @pytest.fixture
    def bridge_with_context(self) -> ZmqWebSocketBridgeService:
        """Provide ZMQ bridge with mocked context."""
        with patch("snapper.interface.websocket.bridge.get_settings") as mock_settings:
            mock_settings.return_value.zmq_broker_xpub = "tcp://localhost:5556"
            mock_settings.return_value.zmq_publisher = "tcp://localhost:5555"
            bridge = ZmqWebSocketBridgeService(connection_manager=MagicMock())
            bridge.context = MagicMock(spec=zmq.asyncio.Context)
            return bridge

    @pytest.mark.asyncio
    async def test_raw_json_passthrough_for_fills(
        self, bridge_with_context: ZmqWebSocketBridgeService
    ) -> None:
        """Verify execution data JSON is passed through unchanged.

        Given: A subscription for executions topic,
        When: Forwarding an ExecutionData as JSON,
        Then: Raw JSON is sent to WebSocket without modification.
        """
        mock_ws = AsyncMock()
        mock_ws.send_text = AsyncMock()
        topic = "orders.events."
        bridge_with_context.topic_subscriptions[topic] = [
            TopicSubscriptionModel(
                websocket=mock_ws,
                throttle_ms=0,
                client_id="test-client",
            )
        ]
        bridge_with_context.topic_metrics[topic] = MagicMock()
        bridge_with_context.topic_metrics[topic].forwarded_count = 0
        fill = ExecutionData(
            session_id="",
            sequence_id=0,
            trade_id="trade-1",
            exchange_order_id="exec-1",
            client_order_id="order-123",
            exchange="kraken",
            instrument="BTC-USD",
            side="buy",
            size=0.5,
            price=50000.0,
            fee=5.0,
            fee_asset="USD",
            status="filled",
            executed_at=datetime(2024, 1, 1, tzinfo=UTC),
        )
        raw_json = fill.model_dump_json()
        await bridge_with_context._forward_to_clients(
            topic, "orders.events.kraken.BTC-USD.executed", raw_json
        )
        mock_ws.send_text.assert_called_once_with(raw_json)

    @pytest.mark.asyncio
    async def test_raw_json_passthrough_for_orders(
        self, bridge_with_context: ZmqWebSocketBridgeService
    ) -> None:
        """Verify order data JSON is passed through unchanged.

        Given: A subscription for orders topic,
        When: Forwarding an OrderData as JSON,
        Then: Raw JSON is sent to WebSocket without modification.
        """
        mock_ws = AsyncMock()
        mock_ws.send_text = AsyncMock()
        topic = "orders.events."
        bridge_with_context.topic_subscriptions[topic] = [
            TopicSubscriptionModel(
                websocket=mock_ws,
                throttle_ms=0,
                client_id="test-client",
            )
        ]
        bridge_with_context.topic_metrics[topic] = MagicMock()
        bridge_with_context.topic_metrics[topic].forwarded_count = 0
        order = OrderData(
            session_id="",
            sequence_id=0,
            exchange_order_id=None,
            client_order_id="order-789",
            instrument="BTC-USD",
            exchange="kraken",
            side="sell",
            size=1.0,
            price=51000.0,
            order_type="limit",
            status="accepted",
            filled_size=0.0,
            created_at=datetime(2024, 1, 1, tzinfo=UTC),
        )
        raw_json = order.model_dump_json()
        await bridge_with_context._forward_to_clients(
            topic, "orders.events.kraken.BTC-USD.accepted", raw_json
        )
        mock_ws.send_text.assert_called_once_with(raw_json)


class TestPatternMatching:
    """Tests for ZMQ topic pattern matching functionality."""

    @pytest.fixture
    def bridge(self) -> ZmqWebSocketBridgeService:
        """Provide ZMQ bridge with default patterns."""
        with patch("snapper.interface.websocket.bridge.get_settings") as mock_settings:
            mock_settings.return_value.zmq_broker_xpub = "tcp://localhost:5556"
            mock_settings.return_value.zmq_publisher = "tcp://localhost:5555"
            return ZmqWebSocketBridgeService(connection_manager=MagicMock())

    def test_executions_pattern_matches_fill_topics(
        self, bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify orders.events pattern matches fill topics.

        Given: A bridge with orders.events pattern configured,
        When: Finding matching pattern for fill topic,
        Then: Returns config with orders.events pattern.
        """
        config = bridge._find_matching_pattern("orders.events.kraken.BTC-USD.executed")
        assert config is not None
        assert config.pattern == "orders.events."

    def test_orders_pattern_matches_status_topics(self, bridge: ZmqWebSocketBridgeService) -> None:
        """Verify orders.events pattern matches status topics.

        Given: A bridge with orders.events pattern configured,
        When: Finding matching pattern for status topic,
        Then: Returns config with orders.events pattern.
        """
        config = bridge._find_matching_pattern("orders.events.kraken.BTC-USD.accepted")
        assert config is not None
        assert config.pattern == "orders.events."

    def test_orders_pattern_matches_new_topics(self, bridge: ZmqWebSocketBridgeService) -> None:
        """Verify orders.commands pattern matches submit command topics.

        Given: A bridge with orders.commands pattern configured,
        When: Finding matching pattern for submit command topic,
        Then: Returns config with orders.commands pattern.
        """
        config = bridge._find_matching_pattern("orders.commands.zonda.ETH-PLN.submit")
        assert config is not None
        assert config.pattern == "orders.commands."


class TestBackpressure:
    """Tests for ZMQ bridge backpressure handling."""

    @pytest.fixture
    def bridge(self) -> ZmqWebSocketBridgeService:
        """Provide ZMQ bridge for backpressure tests."""
        with patch("snapper.interface.websocket.bridge.get_settings") as mock_settings:
            mock_settings.return_value.zmq_broker_xpub = "tcp://localhost:5556"
            mock_settings.return_value.zmq_publisher = "tcp://localhost:5555"
            return ZmqWebSocketBridgeService(connection_manager=MagicMock())

    @pytest.mark.asyncio
    async def test_backpressure_disconnects_trade_clients(
        self, bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify trade clients disconnected when pending messages exceed limit.

        Given: A trade subscription at max pending message limit,
        When: Forwarding another message,
        Then: Client is disconnected due to backpressure.
        """
        mock_ws = AsyncMock()
        bridge.disconnect_client = AsyncMock()
        topic = "orders.events."
        sub = TopicSubscriptionModel(
            websocket=mock_ws,
            throttle_ms=0,
            client_id="slow-client",
            pending_count=MAX_PENDING_MESSAGES_TRADE,
        )
        bridge.topic_subscriptions[topic] = [sub]
        bridge.topic_metrics[topic] = TopicMetricsModel()
        raw_json = '{"type": "order", "public_id": "123"}'
        await bridge._forward_to_clients(topic, "orders.events.kraken.BTC-USD.accepted", raw_json)
        bridge.disconnect_client.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_backpressure_drops_market_messages(
        self, bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Verify market messages dropped when pending messages exceed limit.

        Given: A market subscription at max pending message limit,
        When: Forwarding another message,
        Then: Message is dropped and dropped_count incremented.
        """
        mock_ws = AsyncMock()
        topic = "market.candles."
        sub = TopicSubscriptionModel(
            websocket=mock_ws,
            throttle_ms=0,
            client_id="slow-client",
            pending_count=MAX_PENDING_MESSAGES_MARKET,
        )
        bridge.topic_subscriptions[topic] = [sub]
        bridge.topic_metrics[topic] = TopicMetricsModel()
        raw_json = '{"type": "candle", "instrument": "BTC-USD"}'
        await bridge._forward_to_clients(topic, "market.candles.BTC-USD.1m", raw_json)
        mock_ws.send_text.assert_not_awaited()
        assert bridge.topic_metrics[topic].dropped_count == 1


class TestDataSerialization:
    """Tests for data model JSON serialization."""

    def test_fill_envelope_serialization_matches_expected_format(self) -> None:
        """Verify ExecutionData serializes to expected JSON format.

        Given: An ExecutionData with all required fields,
        When: Serializing to JSON,
        Then: JSON contains all fields with correct values.
        """
        fill = ExecutionData(
            session_id="",
            sequence_id=0,
            trade_id="trade-123",
            exchange_order_id="exchange-fill-123",
            client_order_id="order-123",
            instrument="BTC-USD",
            exchange="kraken",
            side="buy",
            size=0.5,
            price=50000.0,
            fee=5.0,
            fee_asset="USD",
            status="filled",
            executed_at=datetime(2024, 1, 1, tzinfo=UTC),
        )
        json_data = fill.model_dump_json()
        parsed = json.loads(json_data)
        assert parsed["type"] == "execution"
        assert parsed["exchange_order_id"] == "exchange-fill-123"
        assert parsed["client_order_id"] == "order-123"
        assert parsed["exchange"] == "kraken"
        assert parsed["side"] == "buy"
        assert parsed["size"] == pytest.approx(0.5)

    def test_order_status_envelope_serialization_matches_expected_format(self) -> None:
        """Verify OrderData serializes to expected JSON format.

        Given: An OrderData with all required fields,
        When: Serializing to JSON,
        Then: JSON contains all fields with correct values.
        """
        order = OrderData(
            session_id="",
            sequence_id=0,
            exchange_order_id="exchange-789",
            client_order_id="order-789",
            instrument="BTC-USD",
            exchange="kraken",
            side="sell",
            size=1.0,
            price=51000.0,
            order_type="limit",
            status="submitted",
            filled_size=0.0,
            created_at=datetime(2024, 1, 1, tzinfo=UTC),
        )
        json_data = order.model_dump_json()
        parsed = json.loads(json_data)
        assert parsed["type"] == "order"
        assert parsed["client_order_id"] == "order-789"
        assert parsed["exchange"] == "kraken"
        assert parsed["status"] == "submitted"


class TestBridgeControlRecording:
    """Tests for _record_bridge_control in ZmqWebSocketBridgeService."""

    @pytest.fixture
    def recording_bridge(self) -> ZmqWebSocketBridgeService:
        """Provide bridge with db_url configured for control recording."""
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.db_url = "sqlite+aiosqlite:///:memory:"
        mock_cm = MagicMock()
        type(mock_cm).tracker = PropertyMock(return_value=SequenceTracker())
        with patch("snapper.interface.websocket.bridge.get_settings", return_value=mock_settings):
            instance = ZmqWebSocketBridgeService(mock_cm)
        instance.context = MagicMock()
        instance.available_topics = {
            "market.candles.": TopicConfigurationModel(
                endpoint="tcp://127.0.0.1:5555",
                pattern="market.candles.",
                throttle_ms=100,
            ),
        }
        return instance

    @pytest.mark.asyncio
    async def test_record_bridge_control_writes_row(
        self, recording_bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Bridge control row is persisted with correct fields."""
        mock_session = AsyncMock(add=MagicMock())
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_repo = MagicMock()
        mock_repo.session.return_value = mock_ctx

        with patch(
            "snapper.interface.websocket.bridge.get_repository",
            return_value=mock_repo,
        ):
            await recording_bridge._record_bridge_control(
                "zmq_subscribe", "error", detail="Invalid topic: foo"
            )

        mock_session.add.assert_called_once()
        row = mock_session.add.call_args[0][0]
        assert row.transport == "zmq"
        assert row.direction == "inbound"
        assert row.message_type == "zmq_subscribe"
        assert row.outcome == "error"
        assert row.detail == "Invalid topic: foo"
        mock_session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_record_bridge_control_non_blocking(
        self, recording_bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Bridge control DB failure is swallowed without raising."""
        mock_session = AsyncMock(add=MagicMock())
        mock_session.commit = AsyncMock(side_effect=RuntimeError("db down"))
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_repo = MagicMock()
        mock_repo.session.return_value = mock_ctx

        with patch(
            "snapper.interface.websocket.bridge.get_repository",
            return_value=mock_repo,
        ):
            await recording_bridge._record_bridge_control("zmq_disconnect", "ok")

    @pytest.mark.asyncio
    async def test_record_bridge_control_skips_empty_db_url(self) -> None:
        """No DB call when db_url is empty."""
        mock_settings = MagicMock()
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.db_url = ""
        mock_cm = MagicMock()
        type(mock_cm).tracker = PropertyMock(return_value=SequenceTracker())
        with patch("snapper.interface.websocket.bridge.get_settings", return_value=mock_settings):
            instance = ZmqWebSocketBridgeService(mock_cm)

        with patch(
            "snapper.interface.websocket.bridge.get_repository",
        ) as mock_get_repo:
            await instance._record_bridge_control("zmq_subscribe", "ok")
        mock_get_repo.assert_not_called()

    @pytest.mark.asyncio
    async def test_subscribe_websocket_invalid_topic_records_control(
        self, recording_bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Invalid topic subscription records control with error outcome."""
        mock_ws = AsyncMock()
        mock_session = AsyncMock(add=MagicMock())
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_repo = MagicMock()
        mock_repo.session.return_value = mock_ctx

        with patch(
            "snapper.interface.websocket.bridge.get_repository",
            return_value=mock_repo,
        ):
            result = await recording_bridge.subscribe_websocket(mock_ws, "invalid.topic")

        assert result is False
        mock_session.add.assert_called_once()
        row = mock_session.add.call_args[0][0]
        assert row.message_type == "zmq_subscribe"
        assert row.outcome == "error"
        assert "invalid.topic" in row.detail

    @pytest.mark.asyncio
    async def test_disconnect_client_records_control(
        self, recording_bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Client disconnect records control with ok outcome."""
        mock_ws = MagicMock()
        mock_ws.close = AsyncMock()
        mock_session = AsyncMock(add=MagicMock())
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_repo = MagicMock()
        mock_repo.session.return_value = mock_ctx

        with patch(
            "snapper.interface.websocket.bridge.get_repository",
            return_value=mock_repo,
        ):
            await recording_bridge.disconnect_client(mock_ws)

        mock_session.add.assert_called_once()
        row = mock_session.add.call_args[0][0]
        assert row.message_type == "zmq_disconnect"
        assert row.outcome == "ok"

    @pytest.mark.asyncio
    async def test_disconnect_client_with_subscriptions_records_control(
        self, recording_bridge: ZmqWebSocketBridgeService
    ) -> None:
        """Client disconnect with active subscriptions records control."""
        mock_ws = MagicMock()
        mock_ws.close = AsyncMock()
        recording_bridge.client_subscriptions[mock_ws] = {"market.candles."}
        recording_bridge.topic_subscriptions["market.candles."] = [
            TopicSubscriptionModel(websocket=mock_ws, throttle_ms=100)
        ]

        mock_session = AsyncMock(add=MagicMock())
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_repo = MagicMock()
        mock_repo.session.return_value = mock_ctx

        with (
            patch(
                "snapper.interface.websocket.bridge.get_repository",
                return_value=mock_repo,
            ),
            patch.object(recording_bridge, "_stop_zmq_subscription", new_callable=AsyncMock),
        ):
            await recording_bridge.disconnect_client(mock_ws)

        mock_session.add.assert_called_once()
        row = mock_session.add.call_args[0][0]
        assert row.message_type == "zmq_disconnect"
        assert row.outcome == "ok"
        assert "1 topics" in row.detail
