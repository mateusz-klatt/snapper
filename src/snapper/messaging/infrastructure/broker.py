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
import threading
from dataclasses import dataclass
from typing import Any
from typing import Final

import zmq
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

_POLL_TIMEOUT_MS: Final[int] = 100
"""Per-iteration poll timeout (ms) in the blocking proxy thread.

100ms balances responsiveness (cooperative shutdown via
``_stop_event``) against CPU idle when no traffic flows. The proxy
thread runs entirely off the FastAPI asyncio event loop — there is
no per-message yield latency cost like the prior asyncio
implementation, so the cap-tuning history (256→64→16) that mattered
on the event loop is irrelevant here. We forward every available
message per poll wake-up without any artificial cap."""


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
    """Threaded blocking ZMQ XPUB/XSUB broker as a RegisterableProcess.

    Refactored 2026-05-18 from an asyncio-loop proxy to a blocking
    proxy running in a dedicated daemon thread. The async event loop
    no longer participates in message forwarding, freeing FastAPI
    request handlers + DB writer coroutines to run without
    competition. Public ``start()``/``stop()``/``get_status()``/
    ``wait_for_subscription()`` API and the XSUB/XPUB endpoint
    contract are unchanged so the wider system needs no edits.

    The broker creates two bound sockets:
    - XSUB: Publishers connect here to send messages.
    - XPUB: Subscribers connect here to receive messages.

    Messages are forwarded bidirectionally inside the proxy thread:
    data flows XSUB -> XPUB; subscriptions flow XPUB -> XSUB. With
    ``xpub_verbose=True`` every subscription frame from each
    subscriber is forwarded (including duplicates) so the broker can
    update an observed-set the backtest replay engine uses to
    confirm wiring before streaming.

    Attributes:
        settings: Application settings instance.
        xsub_endpoint: Endpoint where publishers connect (e.g., "tcp://*:5555").
        xpub_endpoint: Endpoint where subscribers connect (e.g., "tcp://*:5556").
        context: Synchronous ZMQ context for socket creation.
        xsub_socket: XSUB socket receiving from publishers.
        xpub_socket: XPUB socket sending to subscribers.
        running: Flag indicating if broker is active.

    Example:
        Using with process manager::

            broker = ZmqBrokerProcess()
            await broker.start()  # Non-blocking — spawns the proxy thread.
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
        """Initialise the threaded ZMQ broker facade.

        Args:
            xsub_endpoint: Endpoint for publishers to connect. Defaults to settings value.
            xpub_endpoint: Endpoint for subscribers to connect. Defaults to settings value.
            xpub_verbose: When True, enable XPUB_VERBOSE sockopt (forwards every
                subscription frame including duplicates) and track subscriptions
                into ``_observed_subscriptions``. Used by the backtest ZMQ replay
                engine to prove end-to-end PUB->SUB wiring before streaming. Default
                False preserves live-broker behaviour.
        """
        self.settings = get_settings()
        self.xsub_endpoint = xsub_endpoint or self.settings.zmq_broker_xsub
        self.xpub_endpoint = xpub_endpoint or self.settings.zmq_broker_xpub
        self.xpub_verbose = xpub_verbose
        self.context: zmq.Context[zmq.Socket[bytes]] | None = None
        self.xsub_socket: zmq.Socket[bytes] | None = None
        self.xpub_socket: zmq.Socket[bytes] | None = None
        self.proxy_thread: threading.Thread | None = None
        self.running = False
        self._observed_subscriptions: dict[bytes, int] | None = None
        self._observation_changed: threading.Condition | None = None
        self._stop_event = threading.Event()

    async def start(self) -> None:
        """Start the broker by spawning the proxy thread.

        Creates a synchronous ZMQ context, binds XSUB and XPUB
        sockets, optionally enables ``XPUB_VERBOSE`` subscription
        tracking, and launches the blocking proxy loop in a daemon
        thread. Socket creation + bind are wrapped in
        :func:`asyncio.to_thread` so the calling event loop never
        blocks on ZMQ ``bind()`` syscalls under contention.
        Idempotent.

        Raises:
            zmq.ZMQError: If socket binding fails (e.g., address in use).
        """
        set_log_context("zmq:broker")
        if self.running:
            logger.warning("Broker already running")
            return
        await asyncio.to_thread(self._sync_start)
        self.running = True
        logger.info(f"ZMQ Broker started: {self.xsub_endpoint} -> {self.xpub_endpoint}")

    def _sync_start(self) -> None:
        """Bind sockets and spawn the proxy thread (called via to_thread)."""
        self._stop_event.clear()
        self.context = zmq.Context()
        self.xsub_socket = self.context.socket(zmq.XSUB)
        apply_hwm(self.xsub_socket, rcvhwm=HWM_BROKER)
        self.xsub_socket.bind(self.xsub_endpoint)
        self.xsub_endpoint = self._resolve_endpoint(self.xsub_socket, self.xsub_endpoint)
        self.xpub_socket = self.context.socket(zmq.XPUB)
        apply_hwm(self.xpub_socket, sndhwm=HWM_BROKER)
        if self.xpub_verbose:
            self.xpub_socket.setsockopt(zmq.XPUB_VERBOSE, 1)
            self._observed_subscriptions = {}
            self._observation_changed = threading.Condition()
        self.xpub_socket.bind(self.xpub_endpoint)
        self.xpub_endpoint = self._resolve_endpoint(self.xpub_socket, self.xpub_endpoint)
        self.proxy_thread = threading.Thread(
            target=self._proxy_loop, name="zmq-broker-proxy", daemon=True
        )
        self.proxy_thread.start()

    async def stop(self) -> None:
        """Stop the broker and clean up resources.

        Signals the proxy thread to stop, joins it with a timeout,
        closes sockets with LINGER=0 to discard pending messages,
        and terminates the ZMQ context. The shutdown sequence runs
        in a worker thread so the calling event loop never blocks
        on ``context.term()`` (which can hang briefly while sockets
        finish draining). Also notifies any waiters on the
        subscription condition so :meth:`wait_for_subscription`
        callers wake up promptly when the broker dies. Idempotent.
        """
        if not self.running:
            return
        self.running = False
        await asyncio.to_thread(self._sync_stop)
        logger.info("ZMQ Broker stopped")

    def _sync_stop(self) -> None:
        """Signal the proxy thread, join it, and tear down sockets."""
        self._stop_event.set()
        condition = self._observation_changed
        if condition is not None:
            with condition:
                condition.notify_all()
        thread = self.proxy_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=5.0)
        self.proxy_thread = None
        if self.xsub_socket is not None:
            self.xsub_socket.setsockopt(zmq.LINGER, 0)
            self.xsub_socket.close()
        if self.xpub_socket is not None:
            self.xpub_socket.setsockopt(zmq.LINGER, 0)
            self.xpub_socket.close()
        if self.context is not None:
            self.context.term()

    @staticmethod
    def _resolve_endpoint(socket: Any, configured: str) -> str:
        """Return the actual bound endpoint, resolving OS-assigned ports.

        When ``configured`` requests an OS-assigned port (``:0`` or ``:*``),
        query ``LAST_ENDPOINT`` to get the concrete tcp address so callers
        connecting later (e.g. ephemeral backtest replay endpoints) know the
        real port. Returns the configured endpoint unchanged for non-tcp
        transports or when the socket stub does not support the query.
        """
        needs_resolution = configured.endswith((":0", ":*"))
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

    def _handle_subscription_frame(self, message: list[bytes]) -> bool:
        r"""Forward a XPUB-side frame and update the observed-set if applicable.

        Returns True when the frame was a subscription/unsubscription frame
        (``b"\x01" + topic`` / ``b"\x00" + topic``) and was both forwarded
        and recorded in ``_observed_subscriptions``. Returns False for non-
        subscription frames (caller should perform a normal forward) or when
        verbose tracking is disabled.

        The forward happens BEFORE the dict mutation so a mid-flight stop
        never publishes a false ack of a subscription that did not reach
        XSUB. Mutation is followed by ``notify_all()`` on a
        :class:`threading.Condition` — race-free against waiters that are
        between the dict pre-check and the suspend point in
        :meth:`wait_for_subscription`.

        Sync (not async) because this is called from the proxy thread.
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
        self.xsub_socket.send_multipart(message)
        topic = frame[1:]
        if frame[:1] == b"\x01":
            self._observed_subscriptions[topic] = self._observed_subscriptions.get(topic, 0) + 1
        else:
            count = self._observed_subscriptions.get(topic, 0)
            if count <= 1:
                self._observed_subscriptions.pop(topic, None)
            else:
                self._observed_subscriptions[topic] = count - 1
        with self._observation_changed:
            self._observation_changed.notify_all()
        return True

    async def wait_for_subscription(self, topic_prefix: bytes) -> None:
        """Return when a SUB has subscribed to a topic starting with ``topic_prefix``.

        Cheap defence-in-depth check used by the backtest ZMQ replay engine
        before issuing the echo-ack handshake. Only available when the broker
        was started with ``xpub_verbose=True``.

        Bridges the sync :class:`threading.Condition` (signalled by the
        proxy thread) onto the asyncio event loop via
        :func:`asyncio.to_thread` so the existing async call sites
        (``async with asyncio.timeout(...)`` for bounded waits) keep
        their semantics.

        Race-free w.r.t. concurrent subscription frames: re-checks the dict
        while holding the condition's lock before suspending. Producers acquire
        the same lock around ``notify_all()``, so the change cannot land
        between the consumer's pre-check and its ``wait()``. Also wakes on
        :attr:`_stop_event` so callers exit promptly when the broker is
        torn down mid-wait.

        No timeout parameter: the function waits indefinitely on the
        observation condition. Callers MUST wrap the ``await`` in an
        ``asyncio.timeout(seconds)`` context manager if they need a
        bounded wait; the ``asyncio.TimeoutError`` raised by the outer
        context propagates cleanly.

        Args:
            topic_prefix: Byte prefix to match against observed subscription
                topics (e.g. ``b"market."``).

        Raises:
            RuntimeError: If broker was not started with ``xpub_verbose=True``.
        """
        if self._observed_subscriptions is None or self._observation_changed is None:
            raise RuntimeError("broker was not started with xpub_verbose=True")
        await asyncio.to_thread(self._wait_for_subscription_blocking, topic_prefix)

    def _wait_for_subscription_blocking(self, topic_prefix: bytes) -> None:
        """Block until a matching subscription is observed or broker stops.

        Args:
            topic_prefix: Byte prefix to match against observed subscription
                topics.
        """
        observed = self._observed_subscriptions
        condition = self._observation_changed
        if observed is None or condition is None:
            return
        with condition:
            while not self._stop_event.is_set():
                if any(topic.startswith(topic_prefix) for topic in observed):
                    return
                condition.wait()

    def _proxy_loop(self) -> None:
        """Blocking proxy loop forwarding XSUB <-> XPUB inside the broker thread.

        Polls both sockets with a short timeout (``_POLL_TIMEOUT_MS``) so
        cooperative shutdown via :attr:`_stop_event` stays responsive,
        then drains every ready socket of its pending frames before
        polling again. Subscription frames from XPUB are inspected via
        :meth:`_handle_subscription_frame` (which performs its own
        ``send_multipart`` to XSUB and updates the observed-set) when
        ``xpub_verbose=True``; otherwise they are forwarded blindly.
        """
        if self.xsub_socket is None or self.xpub_socket is None:
            return
        try:
            poller = zmq.Poller()
            poller.register(self.xsub_socket, zmq.POLLIN)
            poller.register(self.xpub_socket, zmq.POLLIN)
            while not self._stop_event.is_set():
                socks = dict(poller.poll(timeout=_POLL_TIMEOUT_MS))
                if self.xsub_socket in socks:
                    self._drain_xsub_to_xpub()
                if self.xpub_socket in socks:
                    self._drain_xpub_to_xsub()
        except zmq.ContextTerminated:
            return
        except Exception as exc:
            logger.error(f"Broker proxy error: {exc}")

    def _drain_xsub_to_xpub(self) -> None:
        """Forward every available XSUB frame to XPUB, exit on Again."""
        xsub = self.xsub_socket
        xpub = self.xpub_socket
        if xsub is None or xpub is None:
            return
        while True:
            try:
                message = xsub.recv_multipart(zmq.NOBLOCK)
            except zmq.Again:
                return
            xpub.send_multipart(message)

    def _drain_xpub_to_xsub(self) -> None:
        """Forward every available XPUB frame to XSUB, exit on Again.

        Subscription frames are routed through
        :meth:`_handle_subscription_frame` when ``xpub_verbose=True``
        so the observed-set update + ``notify_all`` happen on the same
        thread that did the forward.
        """
        xsub = self.xsub_socket
        xpub = self.xpub_socket
        if xsub is None or xpub is None:
            return
        while True:
            try:
                message = xpub.recv_multipart(zmq.NOBLOCK)
            except zmq.Again:
                return
            if self._handle_subscription_frame(message):
                continue
            xsub.send_multipart(message)

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
