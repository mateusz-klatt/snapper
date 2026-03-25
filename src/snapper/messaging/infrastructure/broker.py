"""ZeroMQ XPUB/XSUB message broker implementations.

This module provides central message broker components that route messages between
publishers and subscribers using the XPUB/XSUB proxy pattern. The broker acts as
a central hub, eliminating the need for publishers to know subscriber addresses.

Architecture
------------
The broker implements a bidirectional proxy:

    Publishers ──[connect]──> XSUB ─────┐
                                        │ proxy loop
    Subscribers <──[connect]── XPUB <───┘

- XSUB socket: Binds and receives messages from publishers
- XPUB socket: Binds and delivers messages to subscribers
- Subscription messages flow from XPUB to XSUB (back-channel)

Classes
-------
ZmqBrokerProcess
    Async broker using asyncio, integrates with process manager.
ZmqBrokerThread
    Synchronous broker using threading, for simpler use cases.

Both implementations:
- Support custom endpoint configuration
- Handle graceful shutdown with socket cleanup
- Provide status introspection via `get_status()`

Example:
-------
Using async broker::

    broker = ZmqBrokerProcess(
        xsub_endpoint="tcp://*:5555",
        xpub_endpoint="tcp://*:5556"
    )
    await broker.start()
    # ... application runs ...
    await broker.stop()

Using threaded broker::

    broker = ZmqBrokerThread()
    broker.start()
    # ... application runs ...
    broker.stop()
"""

import asyncio
import contextlib
import threading
from dataclasses import dataclass
from typing import Any

import zmq
import zmq.asyncio
from loguru import logger

from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.messaging.infrastructure.validated_socket import HWM_BROKER
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.utils.logging import set_log_context


@dataclass
class BrokerStatus:
    """ZMQ broker status snapshot.

    Attributes:
        running: Whether the broker proxy loop is active.
        xsub_endpoint: Endpoint where publishers connect.
        xpub_endpoint: Endpoint where subscribers connect.
    """

    running: bool
    xsub_endpoint: str
    xpub_endpoint: str


@register_process(
    "zmq_broker",
    description="ZeroMQ XPUB/XSUB message broker",
    priority=10,
    role=ProcessRoleEnum.CORE,
    tags=("zmq", "broker", "infrastructure"),
    enabled=True,
    mode="thread",
)
class ZmqBrokerProcess(RegisterableProcess):
    """Async ZMQ XPUB/XSUB broker as a RegisterableProcess.

    This broker implements the XPUB/XSUB proxy pattern using asyncio for
    non-blocking operation. It integrates with the process manager for
    lifecycle management and monitoring.

    The broker creates two bound sockets:
    - XSUB: Publishers connect here to send messages
    - XPUB: Subscribers connect here to receive messages

    Messages are forwarded bidirectionally: data flows XSUB -> XPUB,
    while subscriptions flow XPUB -> XSUB.

    Attributes:
        settings: Application settings instance.
        xsub_endpoint: Endpoint where publishers connect (e.g., "tcp://*:5555").
        xpub_endpoint: Endpoint where subscribers connect (e.g., "tcp://*:5556").
        context: ZMQ async context for socket creation.
        xsub_socket: XSUB socket receiving from publishers.
        xpub_socket: XPUB socket sending to subscribers.
        running: Flag indicating if broker is active.

    Example:
        Using with process manager::

            broker = ZmqBrokerProcess()
            await broker.start()  # Non-blocking
            status = broker.get_status()
            await broker.stop()
    """

    @staticmethod
    def get_default_kwargs(settings: AppSettings) -> dict[str, Any]:
        """Get default keyword arguments from application settings.

        Args:
            settings: Application settings containing ZMQ endpoint configuration.

        Returns:
            Dictionary with xsub_endpoint and xpub_endpoint from settings.
        """
        return {
            "xsub_endpoint": settings.zmq_broker_xsub,
            "xpub_endpoint": settings.zmq_broker_xpub,
        }

    def __init__(self, xsub_endpoint: str | None = None, xpub_endpoint: str | None = None):
        """Initialize the async ZMQ broker.

        Args:
            xsub_endpoint: Endpoint for publishers to connect. Defaults to settings value.
            xpub_endpoint: Endpoint for subscribers to connect. Defaults to settings value.
        """
        self.settings = get_settings()
        self.xsub_endpoint = xsub_endpoint or self.settings.zmq_broker_xsub
        self.xpub_endpoint = xpub_endpoint or self.settings.zmq_broker_xpub
        self.context: zmq.asyncio.Context | None = None
        self.xsub_socket: zmq.asyncio.Socket | None = None
        self.xpub_socket: zmq.asyncio.Socket | None = None
        self.proxy_task: asyncio.Task[None] | None = None
        self.running = False

    async def start(self) -> None:
        """Start the async broker and begin forwarding messages.

        Creates ZMQ context, binds XSUB and XPUB sockets, and starts
        the proxy loop as an asyncio task. Idempotent - does nothing
        if already running.

        Raises:
            zmq.ZMQError: If socket binding fails (e.g., address in use).
        """
        set_log_context("zmq:broker")
        if self.running:
            logger.warning("Broker already running")
            return
        self.context = zmq.asyncio.Context()
        self.xsub_socket = self.context.socket(zmq.XSUB)
        apply_hwm(self.xsub_socket, rcvhwm=HWM_BROKER)
        self.xsub_socket.bind(self.xsub_endpoint)
        self.xpub_socket = self.context.socket(zmq.XPUB)
        apply_hwm(self.xpub_socket, sndhwm=HWM_BROKER)
        self.xpub_socket.bind(self.xpub_endpoint)
        self.proxy_task = asyncio.create_task(self._proxy_loop())
        self.running = True
        logger.info(f"ZMQ Broker started: {self.xsub_endpoint} -> {self.xpub_endpoint}")

    async def stop(self) -> None:
        """Stop the broker and clean up resources.

        Cancels the proxy task, closes sockets with LINGER=0 to discard
        pending messages, and terminates the ZMQ context. Idempotent.
        """
        if not self.running:
            return
        self.running = False
        if self.proxy_task:
            self.proxy_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.proxy_task
        if self.xsub_socket:
            self.xsub_socket.setsockopt(zmq.LINGER, 0)
            self.xsub_socket.close()
        if self.xpub_socket:
            self.xpub_socket.setsockopt(zmq.LINGER, 0)
            self.xpub_socket.close()
        if self.context:
            self.context.term()
        logger.info("ZMQ Broker stopped")

    async def _forward_polled_messages(self, events: dict[Any, int]) -> None:
        """Forward messages based on poll results.

        Args:
            events: Dictionary mapping socket objects to event flags.
        """
        for socket, event in events.items():
            if not (event & zmq.POLLIN):
                continue
            if socket == self.xsub_socket and self.xpub_socket:
                message = await socket.recv_multipart(zmq.NOBLOCK)
                await self.xpub_socket.send_multipart(message)
            elif socket == self.xpub_socket and self.xsub_socket:
                message = await socket.recv_multipart(zmq.NOBLOCK)
                await self.xsub_socket.send_multipart(message)

    async def _proxy_loop(self) -> None:
        """Forward messages between XSUB and XPUB sockets.

        Polls both sockets for incoming messages and forwards them to
        the opposite socket. XSUB receives from publishers and forwards
        to XPUB. XPUB receives subscriptions and forwards to XSUB.

        Runs until `self.running` becomes False or task is cancelled.
        """
        try:
            while self.running:
                try:
                    async with asyncio.timeout(1.0):
                        events = await self._poll_sockets()
                    await self._forward_polled_messages(events)
                except (TimeoutError, zmq.Again):
                    continue
        except Exception as e:
            logger.error(f"Broker proxy error: {e}")

    async def _poll_sockets(self) -> dict[Any, int]:
        """Poll XSUB and XPUB sockets for incoming messages.

        Returns:
            Dictionary mapping socket objects to event flags.
        """
        poller = zmq.asyncio.Poller()
        poller.register(self.xsub_socket, zmq.POLLIN)
        poller.register(self.xpub_socket, zmq.POLLIN)
        events = await poller.poll()
        return dict(events)

    def get_status(self) -> dict[str, Any]:
        """Get current broker status for monitoring.

        Returns:
            Dictionary containing running state and endpoint addresses.
        """
        return {
            "running": self.running,
            "xsub_endpoint": self.xsub_endpoint,
            "xpub_endpoint": self.xpub_endpoint,
        }


class ZmqBrokerThread:
    """Synchronous ZMQ XPUB/XSUB broker using threading.

    A simpler alternative to ZmqBrokerProcess for applications that don't
    use asyncio. Runs the proxy loop in a background daemon thread.

    The broker creates two bound sockets:
    - XSUB: Publishers connect here to send messages
    - XPUB: Subscribers connect here to receive messages

    Messages are forwarded bidirectionally using a polling loop with
    100ms timeout for responsive shutdown.

    Attributes:
        settings: Application settings instance.
        xsub_endpoint: Endpoint where publishers connect.
        xpub_endpoint: Endpoint where subscribers connect.
        context: ZMQ synchronous context for socket creation.
        running: Flag indicating if broker is active.

    Example:
        Basic usage::

            broker = ZmqBrokerThread()
            broker.start()
            # ... application runs ...
            broker.stop()
    """

    def __init__(self, xsub_endpoint: str | None = None, xpub_endpoint: str | None = None):
        """Initialize the threaded ZMQ broker.

        Args:
            xsub_endpoint: Endpoint for publishers to connect. Defaults to settings value.
            xpub_endpoint: Endpoint for subscribers to connect. Defaults to settings value.
        """
        self.settings = get_settings()
        self.xsub_endpoint = xsub_endpoint or self.settings.zmq_broker_xsub
        self.xpub_endpoint = xpub_endpoint or self.settings.zmq_broker_xpub
        self.context: zmq.Context[zmq.Socket[bytes]] | None = None
        self.xsub_socket: zmq.Socket[bytes] | None = None
        self.xpub_socket: zmq.Socket[bytes] | None = None
        self.proxy_thread: threading.Thread | None = None
        self.running = False
        self._stop_event = threading.Event()

    def start(self) -> None:
        """Start the broker in a background daemon thread.

        Creates ZMQ context, binds XSUB and XPUB sockets, and starts
        the proxy loop in a daemon thread. Idempotent - does nothing
        if already running.

        Raises:
            zmq.ZMQError: If socket binding fails.
        """
        if self.running:
            logger.warning("Broker already running")
            return
        self.context = zmq.Context()
        self.xsub_socket = self.context.socket(zmq.XSUB)
        apply_hwm(self.xsub_socket, rcvhwm=HWM_BROKER)
        self.xsub_socket.bind(self.xsub_endpoint)
        self.xpub_socket = self.context.socket(zmq.XPUB)
        apply_hwm(self.xpub_socket, sndhwm=HWM_BROKER)
        self.xpub_socket.bind(self.xpub_endpoint)
        self.proxy_thread = threading.Thread(target=self._proxy_loop, daemon=True)
        self.proxy_thread.start()
        self.running = True
        logger.info(f"ZMQ Broker started: {self.xsub_endpoint} -> {self.xpub_endpoint}")

    def stop(self) -> None:
        """Stop the broker and clean up resources.

        Signals the proxy thread to stop, waits up to 5 seconds for
        graceful shutdown, then closes sockets and terminates context.
        Idempotent.
        """
        if not self.running:
            return
        self.running = False
        self._stop_event.set()
        if self.proxy_thread and self.proxy_thread.is_alive():
            self.proxy_thread.join(timeout=5.0)
        if self.xsub_socket:
            self.xsub_socket.setsockopt(zmq.LINGER, 0)
            self.xsub_socket.close()
        if self.xpub_socket:
            self.xpub_socket.setsockopt(zmq.LINGER, 0)
            self.xpub_socket.close()
        if self.context:
            self.context.term()
        logger.info("ZMQ Broker stopped")

    def _proxy_loop(self) -> None:
        """Forward messages between XSUB and XPUB sockets.

        Polls both sockets with 100ms timeout and forwards messages
        bidirectionally. Runs until stop event is set.
        """
        try:
            if not self.xsub_socket or not self.xpub_socket:
                return
            poller = zmq.Poller()
            poller.register(self.xsub_socket, zmq.POLLIN)
            poller.register(self.xpub_socket, zmq.POLLIN)
            while not self._stop_event.is_set():
                try:
                    socks = dict(poller.poll(timeout=100))
                    if self.xsub_socket in socks:
                        message = self.xsub_socket.recv_multipart(zmq.NOBLOCK)
                        self.xpub_socket.send_multipart(message)
                    if self.xpub_socket in socks:
                        message = self.xpub_socket.recv_multipart(zmq.NOBLOCK)
                        self.xsub_socket.send_multipart(message)
                except zmq.Again:
                    continue
        except zmq.ContextTerminated:
            pass
        except Exception as e:
            logger.error(f"Broker proxy error: {e}")

    def get_status(self) -> BrokerStatus:
        """Get current broker status for monitoring.

        Returns:
            BrokerStatus with running state and endpoint addresses.
        """
        return BrokerStatus(
            running=self.running,
            xsub_endpoint=self.xsub_endpoint,
            xpub_endpoint=self.xpub_endpoint,
        )
