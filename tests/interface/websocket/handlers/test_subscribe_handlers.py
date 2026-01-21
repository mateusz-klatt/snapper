"""Tests for WebSocket subscription handlers."""

import json
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.auth.domain.roles import UserRole
from snapper.interface.websocket.handlers.subscribe import handle_subscribe
from snapper.interface.websocket.handlers.subscribe import handle_unsubscribe
from snapper.interface.websocket.schemas import WSSubscribeRequest
from snapper.interface.websocket.schemas import WSUnsubscribeRequest


class TestHandleSubscribeEdgeCases:
    """Edge case tests for WebSocket subscribe handler."""

    @pytest.fixture
    def mock_websocket(self) -> AsyncMock:
        """Provide mock WebSocket with send_text capability."""
        ws = AsyncMock()
        ws.send_text = AsyncMock()
        return ws

    @pytest.fixture
    def mock_manager(self) -> MagicMock:
        """Provide mock WebSocket connection manager."""
        manager = MagicMock()
        manager.get_client_subscriptions = MagicMock(return_value=set())
        manager.subscribe_client = MagicMock()
        manager.unsubscribe_client = MagicMock()
        manager.zmq_bridge = MagicMock()
        manager.zmq_bridge.add_subscription = AsyncMock()
        manager.zmq_bridge.remove_subscription = AsyncMock()
        manager.topic_manager = MagicMock()
        manager.topic_manager.get_all_topics = MagicMock(return_value=[])
        return manager

    @pytest.mark.asyncio
    async def test_subscribe_with_invalid_topic_format_in_list(
        self, mock_websocket: AsyncMock, mock_manager: MagicMock
    ) -> None:
        """Subscribe rejects invalid topic format.

        Given: A list of topics with invalid format (double dots),
        When: Handling subscribe request,
        Then: Returns error response with invalid topic names.
        """
        message = WSSubscribeRequest(topics=["invalid..topic", "another..bad"])
        await handle_subscribe(mock_websocket, message, mock_manager, UserRole.ADMIN)
        mock_websocket.send_text.assert_called_once()
        response = json.loads(mock_websocket.send_text.call_args[0][0])
        assert response["type"] == "error"
        assert "Invalid topic format" in response["message"]
        assert "invalid..topic" in response["message"]

    @pytest.mark.asyncio
    async def test_subscribe_empty_and_whitespace_topics(
        self, mock_websocket: AsyncMock, mock_manager: MagicMock
    ) -> None:
        """Subscribe handles empty and whitespace topics.

        Given: A list with empty and whitespace-only topics,
        When: Handling subscribe request,
        Then: Returns success with no_topics status.
        """
        message = WSSubscribeRequest(topics=["  ", ""])
        await handle_subscribe(mock_websocket, message, mock_manager, UserRole.ADMIN)
        mock_websocket.send_text.assert_called_once()
        response = json.loads(mock_websocket.send_text.call_args[0][0])
        assert response["type"] == "subscription_success"
        assert response["status"] == "no_topics"

    @pytest.mark.asyncio
    async def test_subscribe_when_all_topics_denied(
        self, mock_websocket: AsyncMock, mock_manager: MagicMock
    ) -> None:
        """Subscribe denies all topics when role lacks permission.

        Given: Admin topics and a viewer role,
        When: Handling subscribe request,
        Then: Returns success with denied status and all denied topics.
        """
        message = WSSubscribeRequest(topics=["admin.users", "admin.settings"])
        await handle_subscribe(mock_websocket, message, mock_manager, UserRole.VIEWER)
        mock_websocket.send_text.assert_called_once()
        response = json.loads(mock_websocket.send_text.call_args[0][0])
        assert response["type"] == "subscription_success"
        assert response["status"] == "denied"
        assert len(response["denied_topics"]) == 2

    @pytest.mark.asyncio
    async def test_subscribe_when_zmq_bridge_unavailable(
        self, mock_websocket: AsyncMock, mock_manager: MagicMock
    ) -> None:
        """Subscribe returns error when ZMQ bridge unavailable.

        Given: A manager with zmq_bridge set to None,
        When: Subscribing to valid topics,
        Then: Returns error message about unavailable ZMQ bridge.
        """
        mock_manager.zmq_bridge = None
        valid_topic = "market.kraken.BTC-USD.candles.1m"
        mock_manager.topic_manager.get_all_topics = MagicMock(return_value=[valid_topic])
        message = WSSubscribeRequest(topics=[valid_topic])
        with patch(
            "snapper.interface.websocket.handlers.subscribe.get_allowed_topics_for_role",
            return_value=[valid_topic],
        ), patch(
            "snapper.interface.websocket.handlers.subscribe.filter_topics",
            return_value=([valid_topic], []),
        ):
            await handle_subscribe(mock_websocket, message, mock_manager, UserRole.ADMIN)
        calls = mock_websocket.send_text.call_args_list
        found_error = False
        for call in calls:
            response = json.loads(call[0][0])
            if response.get("type") == "error" and "ZMQ bridge" in response.get("message", ""):
                found_error = True
                break
        assert found_error, "Expected ZMQ bridge error message"


class TestHandleUnsubscribeEdgeCases:
    """Edge case tests for WebSocket unsubscribe handler."""

    @pytest.fixture
    def mock_websocket(self) -> AsyncMock:
        """Provide mock WebSocket with send_text capability."""
        ws = AsyncMock()
        ws.send_text = AsyncMock()
        return ws

    @pytest.fixture
    def mock_manager(self) -> MagicMock:
        """Provide mock connection manager with subscriptions."""
        manager = MagicMock()
        manager.get_client_subscriptions = MagicMock(
            return_value={"market.kraken.BTC-USD.candles.1m"}
        )
        manager.unsubscribe_client = MagicMock()
        manager.zmq_bridge = MagicMock()
        manager.zmq_bridge.remove_subscription = AsyncMock()
        return manager

    @pytest.mark.asyncio
    async def test_unsubscribe_topics_not_subscribed(
        self, mock_websocket: AsyncMock, mock_manager: MagicMock
    ) -> None:
        """Unsubscribe handles topics not currently subscribed.

        Given: Client subscribed to BTC-USD but not ETH-PLN,
        When: Unsubscribing from ETH-PLN,
        Then: Returns success with no_topics status and denied topic.
        """
        message = WSUnsubscribeRequest(topics=["market.zonda.ETH-PLN.candles.1m"])
        await handle_unsubscribe(mock_websocket, message, mock_manager)
        mock_websocket.send_text.assert_called_once()
        response = json.loads(mock_websocket.send_text.call_args[0][0])
        assert response["type"] == "subscription_success"
        assert response["status"] == "no_topics"
        assert "market.zonda.ETH-PLN.candles.1m" in response["denied_topics"]

    @pytest.mark.asyncio
    async def test_unsubscribe_when_zmq_bridge_unavailable(
        self, mock_websocket: AsyncMock, mock_manager: MagicMock
    ) -> None:
        """Unsubscribe returns error when ZMQ bridge unavailable.

        Given: A manager with zmq_bridge set to None,
        When: Unsubscribing from topics,
        Then: Returns error about unavailable ZMQ bridge.
        """
        mock_manager.zmq_bridge = None
        message = WSUnsubscribeRequest(topics=["market.kraken.BTC-USD.candles.1m"])
        await handle_unsubscribe(mock_websocket, message, mock_manager)
        mock_websocket.send_text.assert_called_once()
        response = json.loads(mock_websocket.send_text.call_args[0][0])
        assert response["type"] == "error"
        assert "ZMQ bridge is not available" in response["message"]

    @pytest.mark.asyncio
    async def test_unsubscribe_with_whitespace_only_topics(
        self, mock_websocket: AsyncMock, mock_manager: MagicMock
    ) -> None:
        """Unsubscribe filters whitespace topics and processes valid ones.

        Given: Mixed whitespace and valid topics with active subscription,
        When: Unsubscribing,
        Then: Only valid topic is processed and returned in response.
        """
        mock_manager.get_client_subscriptions = MagicMock(
            return_value={"market.kraken.BTC-USD.ticks"}
        )
        message = WSUnsubscribeRequest(topics=["   ", "\t", "", "market.kraken.BTC-USD.ticks"])
        await handle_unsubscribe(mock_websocket, message, mock_manager)
        mock_websocket.send_text.assert_called_once()
        response = json.loads(mock_websocket.send_text.call_args[0][0])
        assert response["type"] == "subscription_success"
        assert "market.kraken.BTC-USD.ticks" in response["topics"]
