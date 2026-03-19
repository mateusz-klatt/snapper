"""Provenance-stamping message publisher for ZMQ transport.

Provides SequenceTracker (session + per-table counters) and MessagePublisher
(derives topic, stamps provenance, serializes, sends). Together they ensure
every published payload item carries session_id and sequence_id for downstream
gap detection.

SequenceTracker owns one session_id and per-table monotonic counters for
one component lifetime. MessagePublisher wraps ValidatedPublisher, delegates
session/counter state to the injected SequenceTracker, and stamps a copied
model before sending.
"""

from uuid import uuid7

from snapper.api.schemas.base import StrictDataSchema
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.topics.builders import table_for_message
from snapper.messaging.topics.builders import topic_for_message


class SequenceTracker:
    """Per-component session identity and per-table sequence counters.

    Created once at component start and shared across all MessagePublisher
    instances within that component. Generates a stable session_id (UUID7)
    and maintains monotonic counters keyed by destination table name.

    Counter keys are bare table names (e.g. "candles", "orders", "control",
    "telemetry") — not ZMQ topics. Multiple ZMQ topics writing to the same
    table share one counter. This guarantees that GROUP BY session_id
    ORDER BY sequence_id on any SQL table shows no gaps.

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

    def next_sequence(self, table: str) -> int:
        """Return the next monotonic sequence number for a destination table.

        Args:
            table: Destination table name (e.g. "candles", "orders",
                "control", "telemetry"). Must match the actual DB table
                that the payload item will be persisted into.

        Returns:
            Next sequence number (starts at 1, increments by 1).
        """
        seq = self._counters.get(table, 0) + 1
        self._counters[table] = seq
        return seq


class MessagePublisher:
    """Stamp provenance metadata and publish typed messages via ZMQ.

    Wraps a ValidatedPublisher and injects session_id / sequence_id
    into every outgoing message. Topic is derived automatically from
    the message payload via topic_for_message(), or can be overridden
    explicitly for cases where derivation is not possible (e.g. paper
    market data with source_exchange in the topic path).

    The original data object is never mutated; a model_copy is created
    with the provenance fields stamped before serialization.
    """

    def __init__(self, publisher: ValidatedPublisher, tracker: SequenceTracker) -> None:
        """Initialize with a validated publisher and sequence tracker.

        Args:
            publisher: Underlying ZMQ PUB socket wrapper.
            tracker: Shared session/counter state from the owning component.
        """
        self._publisher = publisher
        self._tracker = tracker

    async def publish(
        self,
        data: StrictDataSchema,
        *,
        topic: str | None = None,
        flags: int = 0,
    ) -> StrictDataSchema:
        """Stamp provenance and publish a payload item.

        Args:
            data: Payload item (any StrictDataSchema subclass).
            topic: Explicit ZMQ topic override. When None, topic is derived
                from the payload via topic_for_message().
            flags: Optional ZMQ send flags (e.g. zmq.NOBLOCK).

        Returns:
            The stamped copy with session_id and sequence_id set.
        """
        resolved_topic = topic or topic_for_message(data)
        dest_table = table_for_message(data)
        seq = self._tracker.next_sequence(dest_table)
        stamped = data.model_copy(
            update={
                "session_id": self._tracker.session_id,
                "sequence_id": seq,
            }
        )
        payload = stamped.to_json().encode("utf-8")
        await self._publisher.send_multipart(resolved_topic, payload, flags=flags)
        return stamped

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
