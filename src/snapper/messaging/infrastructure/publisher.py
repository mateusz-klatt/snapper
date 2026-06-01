"""Message publisher for ZMQ transport with provenance tracking.

Provides SequenceTracker (session + per-stream counters) and MessagePublisher
(serializes and sends complete events). Every published payload item must
carry session_id and sequence_id set by the producer at construction time.

SequenceTracker owns one session_id and per-stream monotonic counters for
one component lifetime. MessagePublisher wraps ValidatedPublisher and
delegates routing to the caller via explicit stream_key.
"""

import contextlib
from time import perf_counter_ns
from typing import Any
from uuid import uuid7

import zmq
import zmq.asyncio

from snapper.api.schemas.base import StrictDataSchema
from snapper.messaging.infrastructure.tick_probe import get_probe
from snapper.messaging.infrastructure.validated_socket import HWM_AUDIT
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import apply_hwm


class SequenceTracker:
    """Per-component session identity and per-stream sequence counters.

    Created once at component start and shared across all MessagePublisher
    instances within that component. Generates a stable session_id (UUID7)
    and maintains monotonic counters keyed by stream name (ZMQ topic or
    logical channel).

    Counter keys are ZMQ topics (e.g. "market.kraken.BTC-USD.ticks",
    "orders.events.kraken.BTC-USD.executed") or logical channel names
    for non-ZMQ paths (e.g. "ws.control", "ws.telemetry"). Consumers
    use the same key for gap detection, ensuring accurate per-stream
    sequence tracking without false gaps.

    Invariant: session_id and counters are inseparable. If a component
    restarts, it creates a new SequenceTracker with a fresh session_id
    and zeroed counters.
    """

    def __init__(self) -> None:
        """Generate a fresh session_id and initialize empty counters."""
        self._session_id: str = str(uuid7())
        self._counters: dict[str, int] = {}

    @property
    def session_id(self) -> str:
        """UUID7 identifying this producer session.

        Returns:
            Session identifier string.
        """
        return self._session_id

    def next_sequence(self, stream: str) -> int:
        """Return the next monotonic sequence number for a stream.

        Args:
            stream: Stream identifier, typically a ZMQ topic (e.g.
                "market.kraken.BTC-USD.ticks") or logical channel
                (e.g. "ws.control", "ws.telemetry").

        Returns:
            Next sequence number (starts at 1, increments by 1).
        """
        seq = self._counters.get(stream, 0) + 1
        self._counters[stream] = seq
        return seq


class MessagePublisher:
    """Serialize and send complete event payloads via ZMQ.

    Wraps a ValidatedPublisher and sends already-complete events
    (with session_id and sequence_id set by the producer) to the
    specified stream_key. The ``send`` method routes serialization
    through :meth:`StrictDataSchema.publish_to`, which stamps the
    ``topic`` field onto the payload via ``model_copy`` before
    encoding — the producer-side instance stays unchanged.

    Exposes the tracker property so producers can obtain session_id
    and allocate sequence_id before constructing the event.
    """

    def __init__(self, publisher: ValidatedPublisher, tracker: SequenceTracker) -> None:
        """Initialize with a validated publisher and sequence tracker.

        Args:
            publisher: Underlying ZMQ PUB socket wrapper.
            tracker: Shared session/counter state from the owning component.
        """
        self._publisher = publisher
        self._tracker = tracker

    async def send(
        self,
        stream_key: str,
        data: StrictDataSchema[Any],
        *,
        flags: int = 0,
    ) -> None:
        """Serialize and send a complete event payload.

        The data object must already have session_id and sequence_id
        set by the producer. The ``topic`` field on the serialized
        payload is stamped here via the chokepoint helper, so every
        downstream consumer sees the routing key alongside the domain
        fields without reverse-engineering it.

        The tick probe (enabled by ``SNAPPER_TICK_PROBE``) records
        ``publish_to`` separately from ``socket_send`` so an operator
        can tell Pydantic-serialize CPU apart from ZMQ-send latency on
        a saturated publisher hot-path.

        Args:
            stream_key: Routing key (ZMQ topic or logical channel);
                also stamped onto the payload as the ``topic`` field.
            data: Complete payload item with provenance already set.
            flags: Optional ZMQ send flags (e.g. zmq.NOBLOCK).
        """
        probe = get_probe()
        t0 = perf_counter_ns()
        payload = data.publish_to(stream_key)
        t1 = perf_counter_ns()
        probe.record("publish_to", t1 - t0)
        await self._publisher.send_multipart(stream_key, payload, flags=flags)

    @property
    def tracker(self) -> SequenceTracker:
        """Access the shared sequence tracker.

        Producers use this to obtain session_id and allocate sequence_id
        before constructing events.

        Returns:
            The SequenceTracker instance.
        """
        return self._tracker

    @property
    def session_id(self) -> str:
        """UUID7 identifying this producer session.

        Returns:
            Session identifier string from the shared tracker.
        """
        return self._tracker.session_id

    def close(self) -> None:
        """Close the underlying validated publisher socket."""
        self._publisher.close()

    def setsockopt(self, option: int, value: int) -> None:
        """Delegate socket option to the underlying publisher.

        Args:
            option: ZMQ option constant (e.g., zmq.LINGER).
            value: Option value.
        """
        self._publisher.setsockopt(option, value)


def build_audit_publisher(zmq_broker_xsub: str) -> tuple[MessagePublisher, zmq.asyncio.Context]:
    """Open a fresh ZMQ PUB socket wired to the broker's XSUB endpoint.

    Shared construction helper for the audit/control fanout publisher
    used by both the API container (``admin.user_deactivated`` and
    related control events) and the dedicated feed container (per-process
    ``processes.events.summary.coord-<id>`` metrics snapshots). Centralising
    the build keeps the high-water mark, socket type and broker-connect
    contract identical across both containers so they never drift.

    Per the broker contract (publishers ──[connect]──> XSUB ── proxy ──
    XPUB ──[connect]──> subscribers), publishers connect to the broker's
    XSUB endpoint. ``PUB.connect()`` is non-blocking and tolerates an
    absent peer, so this never blocks even if the broker is momentarily
    unavailable.

    Args:
        zmq_broker_xsub: Address of the broker's XSUB endpoint
            (publishers connect here per the broker contract).

    Returns:
        Tuple of ``(MessagePublisher, zmq.asyncio.Context)``. The context
        is returned so the caller can ``term()`` it on shutdown via
        :func:`shutdown_audit_publisher`.

    Raises:
        Exception: Re-raises any error encountered while configuring or
            connecting the socket. Before re-raising, the partially-built
            socket and context are torn down with ``LINGER=0`` so a
            construction failure never leaks a socket or leaves a context
            that would later block ``term()``.
    """
    context = zmq.asyncio.Context()
    raw_socket: zmq.asyncio.Socket | None = None
    try:
        raw_socket = context.socket(zmq.PUB)
        apply_hwm(raw_socket, sndhwm=HWM_AUDIT)
        raw_socket.connect(zmq_broker_xsub)
    except Exception:
        if raw_socket is not None:
            with contextlib.suppress(Exception):
                raw_socket.setsockopt(zmq.LINGER, 0)
            with contextlib.suppress(Exception):
                raw_socket.close()
        with contextlib.suppress(Exception):
            context.term()
        raise
    publisher = MessagePublisher(ValidatedPublisher(raw_socket), SequenceTracker())
    return publisher, context


def shutdown_audit_publisher(
    publisher: MessagePublisher | None,
    context: zmq.asyncio.Context | None,
) -> None:
    """Safely tear down an audit publisher and its context.

    Mirrors the API container's proven shutdown ordering: set ``LINGER=0``
    so queued frames are dropped immediately, close the socket, then
    terminate the context. Without ``LINGER=0`` a vanished broker would
    leave queued frames that make ``term()`` block indefinitely, which is
    fatal for a crash-loop-sensitive feed container.

    Every cleanup step is wrapped in ``contextlib.suppress(Exception)``
    so a partially-constructed or already-closed publisher never raises
    during shutdown. Accepts ``None`` for either argument so callers can
    invoke it unconditionally on both the construction-failure path and
    the normal-shutdown path.

    Args:
        publisher: The publisher to close, or ``None`` if construction
            failed or no publisher was wired.
        context: The ZMQ context to terminate, or ``None``.
    """
    if publisher is not None:
        with contextlib.suppress(Exception):
            publisher.setsockopt(zmq.LINGER, 0)
        with contextlib.suppress(Exception):
            publisher.close()
    if context is not None:
        with contextlib.suppress(Exception):
            context.term()
