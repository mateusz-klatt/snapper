"""Tests for WebSocket connection manager."""

import json
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest
from fastapi import WebSocket
from pydantic import BaseModel

from snapper.interface.websocket.connection_manager import WebSocketConnectionManager


class MockMessage(BaseModel):
    """Mock Pydantic message model for testing."""

    type: str
    data: str | None = None
    symbol: str | None = None


@pytest.fixture
def mock_websocket() -> Mock:
    """Provide mock WebSocket for testing."""
    ws = Mock(spec=WebSocket)
    ws.client = Mock()
    ws.client.host = "127.0.0.1"
    ws.client.port = 12345
    ws.accept = AsyncMock()
    ws.send_text = AsyncMock()
    ws.close = AsyncMock()
    return ws


@pytest.fixture
def mock_websocket_2() -> Mock:
    """Provide second mock WebSocket for multi-client tests."""
    ws = Mock(spec=WebSocket)
    ws.client = Mock()
    ws.client.host = "127.0.0.1"
    ws.client.port = 54321
    ws.accept = AsyncMock()
    ws.send_text = AsyncMock()
    ws.close = AsyncMock()
    return ws


@pytest.fixture
def connection_manager() -> WebSocketConnectionManager:
    """Provide fresh WebSocketConnectionManager instance."""
    return WebSocketConnectionManager()


class TestConnectionManagerInitialization:
    """Tests for WebSocketConnectionManager initialization."""

    def test_initialization(self, connection_manager: WebSocketConnectionManager) -> None:
        """Connection manager initializes with empty state.

        Given: A new connection manager instance,
        When: Checking initial state,
        Then: All collections are empty and ZMQ bridge exists.
        """
        assert len(connection_manager.active_connections) == 0
        assert len(connection_manager.client_subscriptions) == 0
        assert len(connection_manager.topic_subscribers) == 0
        assert connection_manager.zmq_bridge is not None


class TestConnectionManagement:
    """Tests for WebSocket connection management operations."""

    @pytest.mark.asyncio
    async def test_connect_new_websocket(
        self, connection_manager: WebSocketConnectionManager, mock_websocket: Mock
    ) -> None:
        """Connect adds websocket to active connections.

        Given: A connection manager with no connections,
        When: A websocket connects with accept=True,
        Then: The websocket is added and accept() is called.
        """
        await connection_manager.connect(mock_websocket, accept=True)
        assert mock_websocket in connection_manager.active_connections
        assert mock_websocket in connection_manager.client_subscriptions
        assert len(connection_manager.client_subscriptions[mock_websocket]) == 0
        mock_websocket.accept.assert_called_once()

    @pytest.mark.asyncio
    async def test_connect_without_accept(
        self, connection_manager: WebSocketConnectionManager, mock_websocket: Mock
    ) -> None:
        """Connect without accept skips WebSocket.accept() call.

        Given: A connection manager,
        When: A websocket connects with accept=False,
        Then: The websocket is added but accept() is not called.
        """
        await connection_manager.connect(mock_websocket, accept=False)
        assert mock_websocket in connection_manager.active_connections
        mock_websocket.accept.assert_not_called()

    @pytest.mark.asyncio
    async def test_connect_duplicate_websocket(
        self, connection_manager: WebSocketConnectionManager, mock_websocket: Mock
    ) -> None:
        """Connect handles duplicate websocket connections.

        Given: A websocket already connected,
        When: The same websocket connects again,
        Then: No duplicate is added to the connections list.
        """
        await connection_manager.connect(mock_websocket)
        await connection_manager.connect(mock_websocket)
        assert connection_manager.active_connections.count(mock_websocket) == 1

    @pytest.mark.asyncio
    async def test_disconnect_websocket(
        self, connection_manager: WebSocketConnectionManager, mock_websocket: Mock
    ) -> None:
        """Disconnect removes websocket and cleans up subscriptions.

        Given: A connected websocket with subscriptions,
        When: The websocket disconnects,
        Then: The websocket and its subscriptions are removed.
        """
        await connection_manager.connect(mock_websocket)
        connection_manager.subscribe_client(mock_websocket, "test.topic")
        await connection_manager.disconnect(mock_websocket)
        assert mock_websocket not in connection_manager.active_connections
        assert mock_websocket not in connection_manager.client_subscriptions
        assert "test.topic" not in connection_manager.topic_subscribers

    @pytest.mark.asyncio
    async def test_disconnect_unknown_websocket(
        self, connection_manager: WebSocketConnectionManager, mock_websocket: Mock
    ) -> None:
        """Disconnect handles unknown websocket gracefully.

        Given: A connection manager with no connections,
        When: Disconnecting an unknown websocket,
        Then: No error is raised.
        """
        await connection_manager.disconnect(mock_websocket)


class TestSubscriptionManagement:
    """Tests for client topic subscription management."""

    def test_subscribe_client_to_topic(
        self, connection_manager: WebSocketConnectionManager, mock_websocket: Mock
    ) -> None:
        """Subscribe adds client to topic subscription.

        Given: A connection manager,
        When: A client subscribes to a topic,
        Then: The topic appears in client subscriptions and client in topic subscribers.
        """
        connection_manager.subscribe_client(mock_websocket, "market.btc")
        assert "market.btc" in connection_manager.get_client_subscriptions(mock_websocket)
        assert mock_websocket in connection_manager.get_topic_subscribers("market.btc")

    def test_subscribe_client_to_multiple_topics(
        self, connection_manager: WebSocketConnectionManager, mock_websocket: Mock
    ) -> None:
        """Subscribe handles multiple topics for one client.

        Given: A connection manager,
        When: A client subscribes to multiple topics,
        Then: All topics appear in client subscriptions.
        """
        connection_manager.subscribe_client(mock_websocket, "market.btc")
        connection_manager.subscribe_client(mock_websocket, "market.eth")
        subs = connection_manager.get_client_subscriptions(mock_websocket)
        assert "market.btc" in subs
        assert "market.eth" in subs
        assert len(subs) == 2

    def test_multiple_clients_subscribe_to_same_topic(
        self,
        connection_manager: WebSocketConnectionManager,
        mock_websocket: Mock,
        mock_websocket_2: Mock,
    ) -> None:
        """Multiple clients can subscribe to the same topic.

        Given: A connection manager,
        When: Two clients subscribe to the same topic,
        Then: Both clients appear in topic subscribers.
        """
        connection_manager.subscribe_client(mock_websocket, "market.btc")
        connection_manager.subscribe_client(mock_websocket_2, "market.btc")
        subscribers = connection_manager.get_topic_subscribers("market.btc")
        assert mock_websocket in subscribers
        assert mock_websocket_2 in subscribers
        assert len(subscribers) == 2

    def test_unsubscribe_client_from_topic(
        self, connection_manager: WebSocketConnectionManager, mock_websocket: Mock
    ) -> None:
        """Unsubscribe removes client from topic.

        Given: A client subscribed to a topic,
        When: The client unsubscribes,
        Then: The topic is removed from client subscriptions.
        """
        connection_manager.subscribe_client(mock_websocket, "market.btc")
        connection_manager.unsubscribe_client(mock_websocket, "market.btc")
        assert "market.btc" not in connection_manager.get_client_subscriptions(mock_websocket)
        assert not connection_manager.has_topic_subscribers("market.btc")

    def test_unsubscribe_removes_topic_when_no_subscribers(
        self, connection_manager: WebSocketConnectionManager, mock_websocket: Mock
    ) -> None:
        """Unsubscribe removes topic entry when no subscribers remain.

        Given: A single client subscribed to a topic,
        When: The client unsubscribes,
        Then: The topic is removed from topic_subscribers.
        """
        connection_manager.subscribe_client(mock_websocket, "market.btc")
        connection_manager.unsubscribe_client(mock_websocket, "market.btc")
        assert "market.btc" not in connection_manager.topic_subscribers

    def test_unsubscribe_from_non_subscribed_topic(
        self, connection_manager: WebSocketConnectionManager, mock_websocket: Mock
    ) -> None:
        """Unsubscribe handles non-subscribed topic gracefully.

        Given: A client not subscribed to a topic,
        When: The client unsubscribes from that topic,
        Then: No error is raised.
        """
        connection_manager.unsubscribe_client(mock_websocket, "market.btc")

    def test_has_topic_subscribers_returns_false_for_empty(
        self, connection_manager: WebSocketConnectionManager
    ) -> None:
        """Has topic subscribers returns false for nonexistent topic.

        Given: A connection manager with no subscriptions,
        When: Checking for subscribers on a topic,
        Then: Returns False.
        """
        assert not connection_manager.has_topic_subscribers("nonexistent.topic")

    def test_has_topic_subscribers_returns_true(
        self, connection_manager: WebSocketConnectionManager, mock_websocket: Mock
    ) -> None:
        """Has topic subscribers returns true when subscribers exist.

        Given: A client subscribed to a topic,
        When: Checking for subscribers on that topic,
        Then: Returns True.
        """
        connection_manager.subscribe_client(mock_websocket, "market.btc")
        assert connection_manager.has_topic_subscribers("market.btc")


class TestMessaging:
    """Tests for WebSocket messaging operations."""

    @pytest.mark.asyncio
    async def test_send_personal_message(
        self, connection_manager: WebSocketConnectionManager, mock_websocket: Mock
    ) -> None:
        """Send personal message delivers to specific websocket.

        Given: A connection manager,
        When: Sending a personal message,
        Then: The message is sent via send_text.
        """
        await connection_manager.send_personal_message("test message", mock_websocket)
        mock_websocket.send_text.assert_called_once_with("test message")

    @pytest.mark.asyncio
    async def test_send_personal_message_handles_error(
        self, connection_manager: WebSocketConnectionManager, mock_websocket: Mock
    ) -> None:
        """Send personal message handles send error by disconnecting.

        Given: A connected websocket that fails on send,
        When: Sending a personal message,
        Then: The websocket is removed from active connections.
        """
        await connection_manager.connect(mock_websocket)
        mock_websocket.send_text.side_effect = Exception("Connection error")
        await connection_manager.send_personal_message("test", mock_websocket)
        assert mock_websocket not in connection_manager.active_connections

    @pytest.mark.asyncio
    async def test_broadcast_to_all_connections(
        self,
        connection_manager: WebSocketConnectionManager,
        mock_websocket: Mock,
        mock_websocket_2: Mock,
    ) -> None:
        """Broadcast sends message to all active connections.

        Given: Multiple connected websockets,
        When: Broadcasting a message,
        Then: All connections receive the message.
        """
        await connection_manager.connect(mock_websocket)
        await connection_manager.connect(mock_websocket_2)
        message = MockMessage(type="test", data="broadcast")
        await connection_manager.broadcast(message)
        expected = message.model_dump_json()
        mock_websocket.send_text.assert_called_once_with(expected)
        mock_websocket_2.send_text.assert_called_once_with(expected)

    @pytest.mark.asyncio
    async def test_broadcast_with_no_connections(
        self, connection_manager: WebSocketConnectionManager
    ) -> None:
        """Broadcast handles no connections gracefully.

        Given: A connection manager with no connections,
        When: Broadcasting a message,
        Then: No error is raised.
        """
        message = MockMessage(type="test")
        await connection_manager.broadcast(message)

    @pytest.mark.asyncio
    async def test_broadcast_removes_failed_connections(
        self,
        connection_manager: WebSocketConnectionManager,
        mock_websocket: Mock,
        mock_websocket_2: Mock,
    ) -> None:
        """Broadcast removes connections that fail to receive.

        Given: Multiple connected websockets with one failing,
        When: Broadcasting a message,
        Then: The failing connection is removed.
        """
        await connection_manager.connect(mock_websocket)
        await connection_manager.connect(mock_websocket_2)
        mock_websocket.send_text.side_effect = Exception("Failed")
        await connection_manager.broadcast(MockMessage(type="test"))
        assert mock_websocket not in connection_manager.active_connections
        assert mock_websocket_2 in connection_manager.active_connections

    @pytest.mark.asyncio
    async def test_broadcast_to_topic(
        self,
        connection_manager: WebSocketConnectionManager,
        mock_websocket: Mock,
        mock_websocket_2: Mock,
    ) -> None:
        """Broadcast to topic sends only to topic subscribers.

        Given: Two connected websockets with one subscribed to a topic,
        When: Broadcasting to that topic,
        Then: Only the subscribed websocket receives the message.
        """
        await connection_manager.connect(mock_websocket)
        await connection_manager.connect(mock_websocket_2)
        connection_manager.subscribe_client(mock_websocket, "market.btc")
        message = MockMessage(type="market_update", symbol="BTC")
        await connection_manager.broadcast_to_topic("market.btc", message)
        expected = message.model_dump_json()
        mock_websocket.send_text.assert_called_once_with(expected)
        mock_websocket_2.send_text.assert_not_called()

    @pytest.mark.asyncio
    async def test_broadcast_to_topic_with_no_subscribers(
        self, connection_manager: WebSocketConnectionManager
    ) -> None:
        """Broadcast to topic handles no subscribers gracefully.

        Given: A connection manager with no topic subscribers,
        When: Broadcasting to that topic,
        Then: No error is raised.
        """
        await connection_manager.broadcast_to_topic("empty.topic", MockMessage(type="test"))

    @pytest.mark.asyncio
    async def test_broadcast_to_topic_removes_failed_connections(
        self,
        connection_manager: WebSocketConnectionManager,
        mock_websocket: Mock,
        mock_websocket_2: Mock,
    ) -> None:
        """Broadcast to topic removes failed connections.

        Given: Two subscribers with one failing on send,
        When: Broadcasting to the topic,
        Then: The failing connection is removed.
        """
        await connection_manager.connect(mock_websocket)
        await connection_manager.connect(mock_websocket_2)
        connection_manager.subscribe_client(mock_websocket, "market.btc")
        connection_manager.subscribe_client(mock_websocket_2, "market.btc")
        mock_websocket.send_text.side_effect = Exception("Failed")
        await connection_manager.broadcast_to_topic("market.btc", MockMessage(type="test"))
        assert mock_websocket not in connection_manager.active_connections
        assert mock_websocket_2 in connection_manager.active_connections


class TestCleanup:
    """Tests for connection manager cleanup operations."""

    @pytest.mark.asyncio
    async def test_cleanup_closes_all_connections(
        self,
        connection_manager: WebSocketConnectionManager,
        mock_websocket: Mock,
        mock_websocket_2: Mock,
    ) -> None:
        """Cleanup closes all active connections.

        Given: Multiple connected websockets,
        When: Cleanup is called,
        Then: All connections are closed and list is cleared.
        """
        await connection_manager.connect(mock_websocket)
        await connection_manager.connect(mock_websocket_2)
        await connection_manager.cleanup()
        mock_websocket.close.assert_called_once()
        mock_websocket_2.close.assert_called_once()
        assert len(connection_manager.active_connections) == 0

    @pytest.mark.asyncio
    async def test_cleanup_clears_all_data_structures(
        self, connection_manager: WebSocketConnectionManager, mock_websocket: Mock
    ) -> None:
        """Cleanup clears all data structures.

        Given: A connected websocket with subscriptions,
        When: Cleanup is called,
        Then: All tracking structures are cleared.
        """
        await connection_manager.connect(mock_websocket)
        connection_manager.subscribe_client(mock_websocket, "test.topic")
        await connection_manager.cleanup()
        assert len(connection_manager.active_connections) == 0
        assert len(connection_manager.client_subscriptions) == 0
        assert len(connection_manager.topic_subscribers) == 0

    @pytest.mark.asyncio
    async def test_cleanup_handles_close_errors(
        self, connection_manager: WebSocketConnectionManager, mock_websocket: Mock
    ) -> None:
        """Cleanup handles close errors gracefully.

        Given: A connected websocket that fails on close,
        When: Cleanup is called,
        Then: The connection is still removed.
        """
        await connection_manager.connect(mock_websocket)
        mock_websocket.close.side_effect = Exception("Close error")
        await connection_manager.cleanup()
        assert len(connection_manager.active_connections) == 0


class TestResponseHelpers:
    """Tests for response helper methods."""

    @pytest.mark.asyncio
    async def test_send_response(
        self, connection_manager: WebSocketConnectionManager, mock_websocket: Mock
    ) -> None:
        """Send response serializes and sends Pydantic model.

        Given: A connection manager and a Pydantic response model,
        When: Sending the response,
        Then: The model is serialized to JSON and sent.
        """
        response = MockMessage(type="success", data="value123")
        await connection_manager.send_response(mock_websocket, response)
        expected = response.model_dump_json()
        mock_websocket.send_text.assert_called_once_with(expected)

    @pytest.mark.asyncio
    async def test_send_error(
        self, connection_manager: WebSocketConnectionManager, mock_websocket: Mock
    ) -> None:
        """Send error sends formatted error message.

        Given: A connection manager,
        When: Sending an error message,
        Then: An error response with type and message is sent.
        """
        await connection_manager.send_error(mock_websocket, "Test error")
        assert mock_websocket.send_text.called
        call_args = mock_websocket.send_text.call_args[0][0]
        response = json.loads(call_args)
        assert response["type"] == "error"
        assert response["message"] == "Test error"


class TestStats:
    """Tests for connection manager statistics."""

    def test_get_stats_returns_connections_count(
        self, connection_manager: WebSocketConnectionManager, mock_websocket: Mock
    ) -> None:
        """Get stats returns connection statistics.

        Given: A connection manager,
        When: Getting stats,
        Then: Connection counts are returned.
        """
        stats = connection_manager.get_stats()
        assert "connections" in stats
        assert "active_connections" in stats["connections"]
        assert stats["connections"]["active_connections"] == 0

    @pytest.mark.asyncio
    async def test_get_stats_with_active_connections(
        self, connection_manager: WebSocketConnectionManager, mock_websocket: Mock
    ) -> None:
        """Get stats reflects active connections.

        Given: A connected websocket,
        When: Getting stats,
        Then: Active connections count is 1.
        """
        await connection_manager.connect(mock_websocket)
        stats = connection_manager.get_stats()
        assert stats["connections"]["active_connections"] == 1


class TestZmqBridgeNone:
    """Tests for operations without ZMQ bridge."""

    @pytest.mark.asyncio
    async def test_disconnect_without_zmq_bridge(self, mock_websocket: Mock) -> None:
        """Disconnect works without ZMQ bridge.

        Given: A connection manager with zmq_bridge=None,
        When: A websocket disconnects,
        Then: The disconnect completes without error.
        """
        manager = WebSocketConnectionManager()
        manager.zmq_bridge = None
        await manager.connect(mock_websocket)
        manager.subscribe_client(mock_websocket, "test.topic")
        await manager.disconnect(mock_websocket)
        assert mock_websocket not in manager.active_connections
        assert mock_websocket not in manager.client_subscriptions

    @pytest.mark.asyncio
    async def test_cleanup_without_zmq_bridge(self, mock_websocket: Mock) -> None:
        """Cleanup works without ZMQ bridge.

        Given: A connection manager with zmq_bridge=None,
        When: Cleanup is called,
        Then: All structures are cleared without error.
        """
        manager = WebSocketConnectionManager()
        manager.zmq_bridge = None
        await manager.connect(mock_websocket)
        manager.subscribe_client(mock_websocket, "test.topic")
        await manager.cleanup()
        assert len(manager.active_connections) == 0
        assert len(manager.client_subscriptions) == 0
        assert len(manager.topic_subscribers) == 0


class TestSendErrorHandling:
    """Tests for send error handling and recovery."""

    @pytest.mark.asyncio
    async def test_send_response_error_triggers_disconnect(self, mock_websocket: Mock) -> None:
        """Send response error triggers disconnect.

        Given: A connected websocket that fails on send,
        When: Sending a response,
        Then: The websocket is removed from active connections.
        """
        manager = WebSocketConnectionManager()
        await manager.connect(mock_websocket)
        mock_websocket.send_text = AsyncMock(side_effect=RuntimeError("Connection closed"))
        response = MockMessage(type="test", data="value")
        await manager.send_response(mock_websocket, response)
        assert mock_websocket not in manager.active_connections
