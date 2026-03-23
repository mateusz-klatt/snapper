"""Base Pydantic schemas for all event payload items.

This module defines the foundational StrictDataSchema class used throughout
the application for REST API, WebSocket messages, and ZMQ data payloads.
All event payload items inherit from StrictDataSchema to ensure mandatory
provenance fields and strict type validation.

Schema hierarchy::

    BaseModel
    └── StrictDataSchema → ALL event payload items (provenance required)
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

STRICT_CONFIG = ConfigDict(
    extra="forbid",
    strict=True,
    validate_default=True,
    populate_by_name=True,
)


class StrictDataSchema[TypeT: str](BaseModel):
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

    Attributes:
        type: Payload item type discriminator for routing and deserialization.
        sequence_id: Per-table monotonic counter for gap detection.
        public_id: Unique identifier (UUID7), generated at creation time.
        timestamp: Bus arrival timestamp (UTC), generated once at creation.
        session_id: Producer session identifier for provenance tracking.
    """

    model_config = STRICT_CONFIG

    type: TypeT
    sequence_id: int
    public_id: str
    timestamp: datetime
    session_id: str

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


class PayloadRequest[TypeT: str, PayloadT](StrictDataSchema[TypeT]):
    """Generic REST request carrying a mutation payload.

    All POST/PUT mutation requests inherit from this base. The ``payload``
    field carries the domain command — the business intent without provenance.
    Provenance fields live on the envelope (stamped by the client).

    Attributes:
        payload: The request command body (plain BaseModel).
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
    "STRICT_CONFIG",
    "StrictDataSchema",
    "PayloadRequest",
    "PayloadResponse",
    "PayloadListResponse",
    "MessageResponse",
]
