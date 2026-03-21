"""Base Pydantic schemas for all event payload items.

This module defines the foundational StrictDataSchema class used throughout
the application for REST API, WebSocket messages, and ZMQ data payloads.
All event payload items inherit from StrictDataSchema to ensure mandatory
provenance fields and strict type validation.

Schema hierarchy:
    BaseModel
    └── StrictDataSchema → ALL event payload items (provenance required)
"""

from datetime import UTC
from datetime import datetime
from typing import Literal
from typing import Self
from uuid import uuid7

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

STRICT_CONFIG = ConfigDict(
    extra="forbid",
    strict=True,
    validate_default=True,
    populate_by_name=True,
)


class StrictDataSchema(BaseModel):
    """Base schema for all event payload items across ZMQ, WebSocket, and REST.

    Every event payload item inherits from this base, gaining a unique UUID7
    identifier, a type discriminator for routing/parsing, provenance fields
    for gap detection, and a bus timestamp recording when the item was created.

    Subclasses MUST override type with a Literal default
    (e.g. type: Literal["candle"] = "candle").

    Also provides to_json/from_json for ZMQ serialization.

    Provenance fields (session_id, sequence_id) default to sentinel values
    for backward compatibility. ZMQ producers already provide real values
    at construction. REST/WS paths will be migrated to do the same, after
    which the defaults will be removed.

    Attributes:
        public_id: Unique identifier (UUID7), generated at creation time.
        type: Payload item type discriminator for routing and deserialization.
        timestamp: Bus arrival timestamp (UTC), generated once at creation.
        session_id: Producer session identifier for provenance tracking.
        sequence_id: Per-table monotonic counter for gap detection.
    """

    model_config = STRICT_CONFIG

    public_id: str = Field(default_factory=lambda: str(uuid7()))
    type: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    session_id: str = ""
    sequence_id: int = 0

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


class MessageResponse(StrictDataSchema):
    """Generic API response containing a single message.

    Used for simple acknowledgment responses.

    Attributes:
        type: Payload item type discriminator.
        message: Human-readable response message.
    """

    type: Literal["message"] = "message"
    message: str


__all__ = [
    "STRICT_CONFIG",
    "StrictDataSchema",
    "MessageResponse",
]
