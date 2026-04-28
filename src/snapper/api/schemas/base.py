"""Base Pydantic schemas for all event payload items.

This module defines the foundational StrictDataSchema class used throughout
the application for REST API, WebSocket messages, and ZMQ data payloads.
All event payload items inherit from StrictDataSchema to ensure mandatory
provenance fields and strict type validation.

Schema hierarchy::

    BaseModel
    ├── PartialBody           → lenient partial parsing (extra="ignore", strict=True)
    └── StrictBody            → strict structural schemas / request bodies (no provenance)
        └── StrictDataSchema  → ALL event payload items (provenance required)
            ├── PayloadRequest[T]     → REST mutation request (payload: T)
            ├── PayloadResponse[T]    → singleton REST response (payload: T)
            └── PayloadListResponse[T] → list REST response (payload: list[T], count)
"""

from datetime import datetime
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

STRICT_BODY_CONFIG = ConfigDict(
    extra="forbid",
    strict=True,
    validate_default=True,
    populate_by_name=True,
)

PARTIAL_BODY_CONFIG = ConfigDict(
    extra="ignore",
    strict=True,
    validate_default=True,
    populate_by_name=True,
)


class StrictBody(BaseModel):
    """Strict base for structural schemas and request bodies without provenance.

    All request body classes and nested structural DTOs (stats, counts, info)
    should inherit from StrictBody instead of plain BaseModel. This ensures
    extra="forbid" (reject unknown fields) and strict=True (no type coercion)
    across the entire schema layer.

    For event payload items that carry provenance fields, use StrictDataSchema.
    """

    model_config = STRICT_BODY_CONFIG


class PartialBody(BaseModel):
    """Lenient base for schemas that parse a subset of fields from external data.

    Uses extra="ignore" so unknown fields are silently discarded rather than
    rejected. Intended for partial parsing of JWT payloads (WsTokenPayload)
    and best-effort provenance extraction (GapEnvelope) where the full shape
    is not controlled by Snapper.
    """

    model_config = PARTIAL_BODY_CONFIG


class StrictDataSchema[TypeT: str](StrictBody):
    """Base schema for all event payload items across ZMQ, WebSocket, and REST.

    Every event payload item inherits from this base, gaining a unique UUID7
    identifier, a type discriminator for routing/parsing, provenance fields
    for gap detection, and a bus timestamp recording when the item was created.

    Subclasses MUST override type with a Literal default
    (e.g. type: Literal["candle"] = "candle").

    Also provides to_json/from_json for ZMQ serialization.

    Provenance fields (session_id, sequence_id) are required at construction.
    Producers must obtain these from a SequenceTracker before creating the
    event. This ensures every event is complete and identifiable from birth.

    The optional ``topic`` field is populated at publish time by
    :meth:`publish_to` — the chokepoint every production ZMQ publish call
    site MUST use. Consumers receiving frames over WebSocket can therefore
    rely on ``topic`` being populated for data frames published to a topic;
    REST-only items and control frames leave it ``None``.

    Attributes:
        type: Payload item type discriminator for routing and deserialization.
        sequence_id: Per-table monotonic counter for gap detection.
        public_id: Unique identifier (UUID7), generated at creation time.
        timestamp: Bus arrival timestamp (UTC), generated once at creation.
        session_id: Producer session identifier for provenance tracking.
        topic: Routing key the payload is broadcast on. Stamped by
            :meth:`publish_to` at the publish call site; ``None`` on REST-only
            items and control frames that aren't broadcast.
    """

    type: TypeT
    sequence_id: int
    public_id: str
    timestamp: datetime
    session_id: str
    topic: str | None = None

    def to_json(self) -> str:
        """Serialize to JSON string for ZMQ transport.

        Returns:
            JSON string representation.
        """
        return self.model_dump_json(by_alias=True)

    @classmethod
    def from_json(cls, data: str) -> Self:
        """Deserialize from JSON string.

        Args:
            data: JSON string to parse.

        Returns:
            Typed instance.
        """
        return cls.model_validate_json(data)

    def publish_to(self, topic: str) -> bytes:
        """Return UTF-8 JSON bytes with the topic field stamped for ZMQ publish.

        Every production ZMQ publish call site MUST use this helper instead
        of ``self.to_json().encode("utf-8")`` or
        ``self.model_dump_json().encode()``. It is the single point where
        the published payload's ``topic`` field is populated, so consumers
        across the stack (frontend, iOS, bridge, future strategies) can
        rely on the routing key being available without reverse-engineering
        it from domain fields.

        Args:
            topic: The ZMQ topic the payload is being published to.
                Becomes the value of the serialized ``topic`` field; any
                topic preset on the producer-side instance is overwritten.

        Returns:
            UTF-8-encoded JSON bytes ready for ``send_multipart``.
        """
        return self.model_copy(update={"topic": topic}).to_json().encode("utf-8")


class PayloadRequest[TypeT: str, PayloadT](StrictDataSchema[TypeT]):
    """Generic REST request carrying a mutation payload.

    All POST/PUT mutation requests inherit from this base. The ``payload``
    field carries the domain command — the business intent without provenance.
    Provenance fields live on the envelope (stamped by the client).

    Attributes:
        payload: The request command body (StrictBody).
    """

    payload: PayloadT


class PayloadResponse[TypeT: str, PayloadT](StrictDataSchema[TypeT]):
    """Generic REST response carrying a single payload.

    All singleton REST responses inherit from this base. The ``payload``
    field carries the domain object — either a projection from DB (with
    its own provenance) or a minted result (sharing the envelope's provenance).

    Attributes:
        payload: The response data object.
    """

    payload: PayloadT


class PayloadListResponse[TypeT: str, PayloadT](StrictDataSchema[TypeT]):
    """Generic REST response carrying a list of payloads.

    All list REST responses inherit from this base. The ``payload``
    field carries a list of items and ``count`` equals ``len(payload)``.

    Attributes:
        payload: List of response data objects.
        count: Number of items in payload (== len(payload)).
    """

    payload: list[PayloadT]
    count: int = Field(description="Number of items in payload")


class MessageResponse(PayloadResponse[Literal["message"], str]):
    """Generic API response containing a single message.

    Used for simple acknowledgment responses (logout, delete, password change).

    Attributes:
        type: Payload item type discriminator.
        payload: Human-readable response message.
    """

    type: Literal["message"] = "message"


__all__ = [
    "PARTIAL_BODY_CONFIG",
    "STRICT_BODY_CONFIG",
    "PartialBody",
    "StrictBody",
    "StrictDataSchema",
    "PayloadRequest",
    "PayloadResponse",
    "PayloadListResponse",
    "MessageResponse",
]
