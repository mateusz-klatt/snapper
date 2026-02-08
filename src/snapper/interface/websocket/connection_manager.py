"""WebSocket connection manager.

This module manages WebSocket connections, client subscriptions,
and message broadcasting with ZMQ bridge integration.
"""

import logging

from fastapi import WebSocket
from pydantic import BaseModel

from snapper.interface.websocket.bridge import ZmqWebSocketBridgeService
from snapper.interface.websocket.models import ConnectionStats
from snapper.interface.websocket.models import WsStatsSnapshot
from snapper.interface.websocket.schemas import WSErrorResponse

logger = logging.getLogger(__name__)


class WebSocketConnectionManager:
    """Manages active WebSocket connections and subscriptions.

    Provides connection lifecycle management, subscription tracking,
    message broadcasting, and ZMQ bridge coordination.

    Attributes:
        active_connections: List of currently connected WebSockets.
        client_subscriptions: Mapping of WebSocket to subscribed topics.
        topic_subscribers: Mapping of topic to subscribed WebSockets.
        zmq_bridge: ZMQ-to-WebSocket bridge service.
    """

    def __init__(self) -> None:
        """Initialize connection manager with empty state."""
        self.active_connections: list[WebSocket] = []
        self.client_subscriptions: dict[WebSocket, set[str]] = {}
        self.topic_subscribers: dict[str, set[WebSocket]] = {}
        self.zmq_bridge = ZmqWebSocketBridgeService(self)

    async def connect(self, websocket: WebSocket, *, accept: bool = True) -> None:
        """Register a new WebSocket connection.

        Args:
            websocket: The WebSocket connection to register.
            accept: Whether to accept the connection. Defaults to True.
        """
        if accept:
            await websocket.accept()
        if websocket not in self.active_connections:
            self.active_connections.append(websocket)
        self.client_subscriptions[websocket] = set()
        logger.info(f"WebSocket connected: {websocket.client}")

    async def disconnect(self, websocket: WebSocket) -> None:
        """Unregister a WebSocket connection and clean up subscriptions.

        Args:
            websocket: The WebSocket connection to unregister.
        """
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        if websocket in self.client_subscriptions:
            topics = self.client_subscriptions[websocket].copy()
            for topic in topics:
                self.unsubscribe_client(websocket, topic)
            del self.client_subscriptions[websocket]
        if self.zmq_bridge:
            await self.zmq_bridge.disconnect_client(websocket)
        logger.info(f"WebSocket disconnected: {websocket.client}")

    def subscribe_client(self, websocket: WebSocket, topic: str) -> None:
        """Subscribe a client to a topic.

        Args:
            websocket: The WebSocket connection.
            topic: The topic to subscribe to.
        """
        if websocket not in self.client_subscriptions:
            self.client_subscriptions[websocket] = set()
        self.client_subscriptions[websocket].add(topic)
        if topic not in self.topic_subscribers:
            self.topic_subscribers[topic] = set()
        self.topic_subscribers[topic].add(websocket)
        logger.info(f"Client {websocket.client} subscribed to topic: {topic}")

    def unsubscribe_client(self, websocket: WebSocket, topic: str) -> None:
        """Unsubscribe a client from a topic.

        Args:
            websocket: The WebSocket connection.
            topic: The topic to unsubscribe from.
        """
        if websocket in self.client_subscriptions:
            self.client_subscriptions[websocket].discard(topic)
        if topic in self.topic_subscribers:
            self.topic_subscribers[topic].discard(websocket)
            if not self.topic_subscribers[topic]:
                del self.topic_subscribers[topic]
        logger.info(f"Client {websocket.client} unsubscribed from topic: {topic}")

    def get_client_subscriptions(self, websocket: WebSocket) -> set[str]:
        """Get all topics a client is subscribed to.

        Args:
            websocket: The WebSocket connection.

        Returns:
            Set of subscribed topic names.
        """
        return self.client_subscriptions.get(websocket, set())

    def get_topic_subscribers(self, topic: str) -> set[WebSocket]:
        """Get all clients subscribed to a topic.

        Args:
            topic: The topic name.

        Returns:
            Set of subscribed WebSocket connections.
        """
        return self.topic_subscribers.get(topic, set())

    def has_topic_subscribers(self, topic: str) -> bool:
        """Check if a topic has any subscribers.

        Args:
            topic: The topic name.

        Returns:
            True if the topic has at least one subscriber.
        """
        return topic in self.topic_subscribers and len(self.topic_subscribers[topic]) > 0

    async def send_personal_message(self, message: str, websocket: WebSocket) -> None:
        """Send a message to a specific client.

        Args:
            message: The message string to send.
            websocket: The target WebSocket connection.
        """
        try:
            await websocket.send_text(message)
        except Exception as e:
            logger.error(f"Failed to send personal message: {e}")
            await self.disconnect(websocket)

    async def broadcast(self, message: BaseModel) -> None:
        """Broadcast a message to all connected clients.

        Args:
            message: The Pydantic model to broadcast as JSON.
        """
        if not self.active_connections:
            return
        message_str = message.model_dump_json()
        disconnected: list[WebSocket] = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message_str)
            except Exception as e:
                logger.error(f"Failed to broadcast to client: {e}")
                disconnected.append(connection)
        for connection in disconnected:
            await self.disconnect(connection)

    async def broadcast_to_topic(self, topic: str, message: BaseModel) -> None:
        """Broadcast a message to all subscribers of a topic.

        Args:
            topic: The topic to broadcast to.
            message: The Pydantic model to broadcast as JSON.
        """
        subscribers = self.get_topic_subscribers(topic)
        if not subscribers:
            return
        message_str = message.model_dump_json()
        disconnected: list[WebSocket] = []
        for websocket in subscribers:
            try:
                await websocket.send_text(message_str)
            except Exception as e:
                logger.error(f"Failed to send to topic {topic} subscriber: {e}")
                disconnected.append(websocket)
        for connection in disconnected:
            await self.disconnect(connection)

    async def cleanup(self) -> None:
        """Clean up all connections and resources."""
        logger.info("Cleaning up connection manager")
        for websocket in self.active_connections[:]:
            try:
                await websocket.close()
            except Exception as e:
                logger.error(f"Error closing websocket: {e}")
        self.active_connections.clear()
        self.client_subscriptions.clear()
        self.topic_subscribers.clear()
        if self.zmq_bridge:
            await self.zmq_bridge.cleanup()
        logger.info("Connection manager cleanup complete")

    async def send_response(self, websocket: WebSocket, response: BaseModel) -> None:
        """Send a response model to a client.

        Args:
            websocket: The target WebSocket connection.
            response: The Pydantic model to send as JSON.
        """
        try:
            await websocket.send_text(response.model_dump_json())
        except Exception as exc:
            logger.error(f"Failed to send response: {exc}")
            await self.disconnect(websocket)

    async def send_error(self, websocket: WebSocket, error_message: str) -> None:
        """Send an error response to a client.

        Args:
            websocket: The target WebSocket connection.
            error_message: The error message string.
        """
        error = WSErrorResponse(message=error_message)
        await self.send_response(websocket, error)

    def get_stats(self) -> WsStatsSnapshot:
        """Get connection and topic statistics.

        Returns:
            WsStatsSnapshot with connection counts and topic metrics.
        """
        if self.zmq_bridge:
            conn_stats = self.zmq_bridge.get_connection_stats()
            conn_stats.active_connections = len(self.active_connections)
            topic_stats = self.zmq_bridge.get_topic_stats()
        else:
            conn_stats = ConnectionStats(active_connections=len(self.active_connections))
            topic_stats = {}
        return WsStatsSnapshot(connections=conn_stats, topics=topic_stats)
