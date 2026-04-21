"""ZeroMQ XPUB/XSUB message broker implementations.

This module provides central message broker components that route messages between
publishers and subscribers using the XPUB/XSUB proxy pattern. The broker acts as
a central hub, eliminating the need for publishers to know subscriber addresses.
Architecture
The broker implements a bidirectional proxy
    Publishers ──[connect]──> XSUB ─────┐
                                        │ proxy loop
    Subscribers <──[connect]── XPUB <───┘
XSUB socket: Binds and receives messages from publishers
XPUB socket: Binds and delivers messages to subscribers
Subscription messages flow from XPUB to XSUB (back-channel)
Classes
ZmqBrokerProcess
    Async broker using asyncio, integrates with process manager.
ZmqBrokerThread
    Synchronous broker using threading, for simpler use cases.
Both implementations
Support custom endpoint configuration
Handle graceful shutdown with socket cleanup
Provide status introspection via `get_status()`
Example
Using async broker
    broker = ZmqBrokerProcess(
        xsub_endpoint="tcp://*:5555"
        xpub_endpoint="tcp://*:5556"
    await broker.start()
    #... application runs...
    await broker.stop()
Using threaded broker
    broker = ZmqBrokerThread()
    broker.start()
    #... application runs...
    broker.stop().
"""

import asyncio
import contextlib
import threading
from dataclasses import dataclass
from typing import Any

import zmq
import zmq.asyncio
from loguru import logger

from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.process_parameters import BrokerParameters
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
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
    parameters_model=BrokerParameters,
    enabled=True,
    mode=ProcessModeEnum.THREAD,
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
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Get default parameters from application settings.

        Args:
            settings: Application settings containing ZMQ endpoint configuration.

        Returns:
            Dictionary with xsub_endpoint and xpub_endpoint from settings.
        """
        return {
            "xsub_endpoint": settings.zmq_broker_xsub,
            "xpub_endpoint": settings.zmq_broker_xpub,
        }

    def __init__(
        self,
        xsub_endpoint: str | None = None,
        xpub_endpoint: str | None = None,
        xpub_verbose: bool = False,
    ):
        """Initialize the async ZMQ broker.

        Args:
            xsub_endpoint: Endpoint for publishers to connect. Defaults to settings value.
            xpub_endpoint: Endpoint for subscribers to connect. Defaults to settings value.
            xpub_verbose: When True, enable XPUB_VERBOSE sockopt (forwards every
                subscription frame including duplicates) and track subscriptions
                into ``_observed_subscriptions``. Used by the backtest ZMQ replay
                engine to prove end-to-end PUB→SUB wiring before streaming. Default
                False preserves live-broker behaviour.
        """
        self.settings = get_settings()
        self.xsub_endpoint = xsub_endpoint or self.settings.zmq_broker_xsub
        self.xpub_endpoint = xpub_endpoint or self.settings.zmq_broker_xpub
        self.xpub_verbose = xpub_verbose
        self.context: zmq.asyncio.Context | None = None
        self.xsub_socket: zmq.asyncio.Socket | None = None
        self.xpub_socket: zmq.asyncio.Socket | None = None
        self.proxy_task: asyncio.Task[None] | None = None
        self.running = False
        self._observed_subscriptions: dict[bytes, int] | None = None
        self._observation_changed: asyncio.Condition | None = None

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
        self.xsub_endpoint = self._resolve_endpoint(self.xsub_socket, self.xsub_endpoint)
        self.xpub_socket = self.context.socket(zmq.XPUB)
        apply_hwm(self.xpub_socket, sndhwm=HWM_BROKER)
        if self.xpub_verbose:
            self.xpub_socket.setsockopt(zmq.XPUB_VERBOSE, 1)
            self._observed_subscriptions = {}
            self._observation_changed = asyncio.Condition()
        self.xpub_socket.bind(self.xpub_endpoint)
        self.xpub_endpoint = self._resolve_endpoint(self.xpub_socket, self.xpub_endpoint)
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

    @staticmethod
    def _resolve_endpoint(socket: Any, configured: str) -> str:
        """Return the actual bound endpoint, resolving OS-assigned ports.

        When ``configured`` requests an OS-assigned port (``:0`` or ``:*``),
        query ``LAST_ENDPOINT`` to get the concrete tcp address so callers
        connecting later (e.g. ephemeral backtest replay endpoints) know the
        real port. Returns the configured endpoint unchanged for non-tcp
        transports or when the socket stub does not support the query.
        """
        needs_resolution = configured.endswith(":0") or configured.endswith(":*")
        if not needs_resolution:
            return configured
        getsockopt = getattr(socket, "getsockopt", None)
        if getsockopt is None:
            return configured
        actual = getsockopt(zmq.LAST_ENDPOINT)
        if isinstance(actual, bytes):
            return actual.decode()
        if isinstance(actual, str):
            return actual
        return configured

    async def _handle_subscription_frame(self, message: list[bytes]) -> bool:
        r"""Forward a XPUB-side frame and update the observed-set if applicable.

        Returns True when the frame was a subscription/unsubscription frame
        (``b"\x01" + topic`` / ``b"\x00" + topic``) and was both forwarded
        and recorded in ``_observed_subscriptions``. Returns False for non-
        subscription frames (caller should perform a normal forward) or when
        verbose tracking is disabled.

        The forward happens BEFORE the dict mutation so a mid-flight cancel
        never publishes a false ack of a subscription that did not reach
        XSUB. Mutation is followed by ``notify_all()`` on a condition variable
        — race-free against waiters that are between the dict pre-check and
        the suspend point in ``wait_for_subscription``.
        """
        if (
            self._observed_subscriptions is None
            or self._observation_changed is None
            or not message
            or self.xsub_socket is None
        ):
            return False
        frame = message[0]
        if not frame or frame[:1] not in (b"\x01", b"\x00"):
            return False
        await self.xsub_socket.send_multipart(message)
        topic = frame[1:]
        if frame[:1] == b"\x01":
            self._observed_subscriptions[topic] = self._observed_subscriptions.get(topic, 0) + 1
        else:
            count = self._observed_subscriptions.get(topic, 0)
            if count <= 1:
                self._observed_subscriptions.pop(topic, None)
            else:
                self._observed_subscriptions[topic] = count - 1
        async with self._observation_changed:
            self._observation_changed.notify_all()
        return True

    async def _forward_polled_messages(self, events: dict[Any, int]) -> None:
        r"""Forward messages based on poll results.

        XSUB → XPUB carries data frames (publishers to subscribers).
        XPUB → XSUB carries subscription frames (``b"\x01" + topic`` for
        subscribe, ``b"\x00" + topic`` for unsubscribe). When ``xpub_verbose``
        is on, every subscription frame from each subscriber is forwarded
        (including duplicates) so the broker can update an observed-set the
        backtest replay engine uses to confirm wiring before streaming.

        ``continue`` (not ``return``) is used after handling each socket so
        the outer loop still services the other ready socket within the
        same poll cycle.

        Args:
            events: Dictionary mapping socket objects to event flags.
        """
        for socket, event in events.items():
            if not (event & zmq.POLLIN):
                continue
            if socket == self.xsub_socket and self.xpub_socket:
                message = await socket.recv_multipart(zmq.NOBLOCK)
                await self.xpub_socket.send_multipart(message)
                continue
            if socket == self.xpub_socket and self.xsub_socket:
                message = await socket.recv_multipart(zmq.NOBLOCK)
                if await self._handle_subscription_frame(message):
                    continue
                await self.xsub_socket.send_multipart(message)

    async def wait_for_subscription(self, topic_prefix: bytes, timeout: float) -> None:
        """Return when a SUB has subscribed to a topic starting with ``topic_prefix``.

        Cheap defence-in-depth check used by the backtest ZMQ replay engine
        before issuing the echo-ack handshake. Only available when the broker
        was started with ``xpub_verbose=True``.

        Race-free w.r.t. concurrent subscription frames: re-checks the dict
        while holding the condition's lock before suspending. Producers acquire
        the same lock around ``notify_all()``, so the change cannot land
        between the consumer's pre-check and its ``wait()``.

        Args:
            topic_prefix: Byte prefix to match against observed subscription
                topics (e.g. ``b"market."``).
            timeout: Maximum seconds to wait before raising ``TimeoutError``.

        Raises:
            RuntimeError: If broker was not started with ``xpub_verbose=True``.
            TimeoutError: If no matching subscription is observed before
                ``timeout`` elapses; message includes the current observed-set.
        """
        if self._observed_subscriptions is None or self._observation_changed is None:
            raise RuntimeError("broker was not started with xpub_verbose=True")
        deadline = asyncio.get_running_loop().time() + timeout
        async with self._observation_changed:
            while True:
                if any(topic.startswith(topic_prefix) for topic in self._observed_subscriptions):
                    return
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise TimeoutError(
                        f"no subscription for prefix {topic_prefix!r} within {timeout}s; "
                        f"observed={list(self._observed_subscriptions)}"
                    )
                try:
                    async with asyncio.timeout(remaining):
                        await self._observation_changed.wait()
                except TimeoutError:
                    continue

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
