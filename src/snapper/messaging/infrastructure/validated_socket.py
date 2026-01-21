"""Validated ZMQ socket wrappers with topic validation.

This module provides wrapper classes around ZMQ PUB and SUB sockets that
validate topic strings before sending or subscribing. This prevents
invalid topic hierarchies from entering the messaging system.

Validation ensures topics follow the structured hierarchy:
- market.{exchange}.{instrument}.{type}
- orders.{exchange}.{instrument}.{type}
- executions.{exchange}.{instrument}.fill
- signals.{exchange}.{instrument}.live
- system.{type}[.{component}[.{name}]]
- admin.{resource}

Classes
-------
ValidatedPublisher
    PUB socket wrapper validating topics before send.
ValidatedSubscriber
    SUB socket wrapper validating patterns before subscribe.

Exceptions
----------
TopicValidationError
    Raised when a topic string fails validation.

Example:
-------
Publishing with validation::

    ctx = zmq.asyncio.Context()
    raw_socket = ctx.socket(zmq.PUB)
    raw_socket.connect("tcp://localhost:5555")

    publisher = ValidatedPublisher(raw_socket)
    await publisher.send_multipart("market.kraken.BTC-USD.ticks", payload)

    # This raises TopicValidationError:
    await publisher.send_multipart("invalid-topic", payload)

Subscribing with validation::

    raw_socket = ctx.socket(zmq.SUB)
    raw_socket.connect("tcp://localhost:5556")

    subscriber = ValidatedSubscriber(raw_socket)
    subscriber.subscribe("market.kraken.")  # Prefix pattern OK
    topic, payload = await subscriber.recv_multipart()
"""

import zmq
import zmq.asyncio
from loguru import logger

from snapper.messaging.topics.validation import TopicValidationError
from snapper.messaging.topics.validation import validate_subscription_pattern
from snapper.messaging.topics.validation import validate_topic

__all__ = ["ValidatedPublisher", "ValidatedSubscriber"]


class ValidatedPublisher:
    """Topic-validated wrapper around a ZMQ PUB socket.

    Validates topic strings before sending messages, ensuring they conform
    to the messaging system's topic hierarchy. Invalid topics raise
    TopicValidationError.

    Attributes:
        _socket: Underlying async ZMQ PUB socket.

    Example:
        ::

            raw_socket = context.socket(zmq.PUB)
            raw_socket.connect("tcp://localhost:5555")
            publisher = ValidatedPublisher(raw_socket)

            await publisher.send_multipart(
                "market.kraken.BTC-USD.ticks",
                tick_data.encode()
            )
    """

    def __init__(self, socket: zmq.asyncio.Socket):
        """Initialize the validated publisher.

        Args:
            socket: An async ZMQ PUB socket to wrap.
        """
        self._socket = socket

    async def send_multipart(
        self,
        topic: str,
        payload: bytes,
        *,
        flags: int = 0,
    ) -> None:
        """Send a message with topic validation.

        Validates the topic against messaging hierarchy rules before sending.
        The message is sent as two frames: [topic, payload].

        Args:
            topic: Topic string (e.g., "market.kraken.BTC-USD.ticks").
            payload: Message payload as bytes.
            flags: Optional ZMQ send flags.

        Raises:
            TopicValidationError: If topic fails validation.
        """
        is_valid, error_msg = validate_topic(topic)
        if not is_valid:
            raise TopicValidationError(f"Invalid topic '{topic}': {error_msg}")
        logger.debug(f"ZMQ PUB: {topic} ({len(payload)} bytes)")
        await self._socket.send_multipart(
            [topic.encode("utf-8"), payload],
            flags=flags,
        )

    def close(self) -> None:
        """Close the underlying socket."""
        self._socket.close()

    def setsockopt(self, option: int, value: int) -> None:
        """Set a socket option.

        Args:
            option: ZMQ option constant (e.g., zmq.LINGER).
            value: Option value.
        """
        self._socket.setsockopt(option, value)


class ValidatedSubscriber:
    """Pattern-validated wrapper around a ZMQ SUB socket.

    Validates subscription patterns before subscribing, ensuring they
    conform to the messaging system's topic hierarchy. Invalid patterns
    raise TopicValidationError.

    Supports both full topic subscriptions and prefix patterns
    (ending with '.').

    Attributes:
        _socket: Underlying async ZMQ SUB socket.

    Example:
        ::

            raw_socket = context.socket(zmq.SUB)
            raw_socket.connect("tcp://localhost:5556")
            subscriber = ValidatedSubscriber(raw_socket)

            subscriber.subscribe("market.kraken.")  # All Kraken market data
            topic, payload = await subscriber.recv_multipart()
    """

    def __init__(self, socket: zmq.asyncio.Socket):
        """Initialize the validated subscriber.

        Args:
            socket: An async ZMQ SUB socket to wrap.
        """
        self._socket = socket

    def subscribe(
        self,
        pattern: str,
    ) -> None:
        """Subscribe to a topic pattern with validation.

        Validates the pattern against messaging hierarchy rules. Patterns
        can be full topics or prefixes (ending with '.').

        Args:
            pattern: Topic or prefix pattern to subscribe to.

        Raises:
            TopicValidationError: If pattern fails validation.
        """
        is_valid, error_msg = validate_subscription_pattern(pattern)
        if not is_valid:
            raise TopicValidationError(f"Invalid subscription pattern '{pattern}': {error_msg}")
        logger.info(f"ZMQ SUB: Subscribing to pattern: {pattern}")
        self._socket.setsockopt_string(zmq.SUBSCRIBE, pattern)

    def unsubscribe(self, pattern: str) -> None:
        """Unsubscribe from a topic pattern.

        Args:
            pattern: Topic or prefix pattern to unsubscribe from.
        """
        logger.info(f"ZMQ SUB: Unsubscribing from pattern: {pattern}")
        self._socket.setsockopt_string(zmq.UNSUBSCRIBE, pattern)

    async def recv_multipart(self, flags: int = 0) -> tuple[str, bytes]:
        """Receive a multipart message.

        Expects a 2-part message: [topic, payload].

        Args:
            flags: Optional ZMQ receive flags.

        Returns:
            Tuple of (topic string, payload bytes).

        Raises:
            ValueError: If message doesn't have exactly 2 parts.
        """
        parts = await self._socket.recv_multipart(flags=flags)
        if len(parts) != 2:
            raise ValueError(f"Expected 2-part message, got {len(parts)} parts")
        topic = parts[0].decode("utf-8")
        payload = parts[1]
        logger.debug(f"ZMQ SUB: Received {topic} ({len(payload)} bytes)")
        return topic, payload

    def close(self) -> None:
        """Close the underlying socket."""
        self._socket.close()

    def setsockopt(self, option: int, value: int) -> None:
        """Set a socket option.

        Args:
            option: ZMQ option constant (e.g., zmq.LINGER).
            value: Option value.
        """
        self._socket.setsockopt(option, value)
