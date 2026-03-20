"""Base Pydantic schemas for REST API, WebSocket messages, and ZMQ data payloads.

This module defines foundational schema classes and configuration objects used throughout
the API layer for request/response validation. All event payload items inherit from
StrictDataSchema to ensure mandatory provenance fields.

Configuration variants:
    - STRICT_API_CONFIG: Forbids extra fields, uses strict type coercion
    - STRICT_WS_CONFIG: Forbids extra fields but allows loose type coercion for WebSocket
    - STRICT_DATA_CONFIG: Forbids extra fields, loose coercion for ZMQ data payloads

Schema hierarchy:
    BaseModel
    └── StrictDataSchema       → ALL event payload items (provenance required)
        ├── WsMessageSchema    → WS control (strict=False for JS coercion)
        └── StrictApiSchema    → REST operational responses (strict=True)
"""

from datetime import UTC
from datetime import datetime
from typing import Literal
from typing import Self
from uuid import uuid7

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

STRICT_API_CONFIG = ConfigDict(
    extra="forbid",
    strict=True,
    validate_default=True,
    populate_by_name=True,
)

STRICT_WS_CONFIG = ConfigDict(
    extra="forbid",
    strict=False,
    validate_default=True,
    populate_by_name=True,
)

STRICT_DATA_CONFIG = ConfigDict(
    extra="forbid",
    strict=False,
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

    Provenance fields (session_id, sequence_id) are required at construction
    time with no defaults. For objects published via MessagePublisher, pass
    placeholder values (session_id="", sequence_id=0) since publish() stamps
    real values via model_copy(). For objects constructed in REST/WS handlers,
    pass the tracker values directly.

    Attributes:
        public_id: Unique identifier (UUID7), generated at creation time.
        type: Payload item type discriminator for routing and deserialization.
        timestamp: Bus arrival timestamp (UTC), generated once at creation.
        session_id: Producer session identifier for provenance tracking (required).
        sequence_id: Per-table monotonic counter for gap detection (required).
    """

    model_config = STRICT_DATA_CONFIG

    public_id: str = Field(default_factory=lambda: str(uuid7()))
    type: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    session_id: str
    sequence_id: int

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


class WsMessageSchema(StrictDataSchema):
    """Base schema for WebSocket protocol payload items.

    Inherits provenance fields (public_id, session_id, sequence_id, timestamp)
    from StrictDataSchema. Uses loose type coercion (strict=False) to handle
    JavaScript clients that may send numbers as strings.

    Provides sentinel defaults for provenance fields since WS control
    messages are stamped by a SequenceTracker before transmission, or
    carry informational payloads where provenance is not required.

    Subclasses MUST override type with a Literal default.
    """

    model_config = STRICT_WS_CONFIG

    session_id: str = ""
    sequence_id: int = 0


class StrictApiSchema(StrictDataSchema):
    """Base schema for REST API request/response models.

    Inherits StrictDataSchema to gain provenance fields (public_id,
    session_id, sequence_id, timestamp) on all REST operational responses.
    Uses strict type coercion (unlike WsMessageSchema which is loose).

    Provides sentinel defaults for provenance fields since REST responses
    are stamped by the ClientProvenanceMiddleware before delivery, or
    carry informational payloads where provenance is not required.

    Subclasses MUST override type with a Literal default
    (e.g. type: Literal["health_check"] = "health_check").
    """

    model_config = STRICT_API_CONFIG

    session_id: str = ""
    sequence_id: int = 0


class StrictWsSchema(BaseModel):
    """Legacy base schema for WebSocket message models.

    Kept for backward compatibility during migration. New code should use
    WsMessageSchema (which inherits from StrictDataSchema) instead.
    """

    model_config = STRICT_WS_CONFIG


class MessageResponse(StrictApiSchema):
    """Generic API response containing a single message.

    Used for simple acknowledgment responses.

    Attributes:
        type: Payload item type discriminator.
        message: Human-readable response message.
    """

    type: Literal["message"] = "message"
    message: str


__all__ = [
    "STRICT_API_CONFIG",
    "STRICT_WS_CONFIG",
    "STRICT_DATA_CONFIG",
    "StrictApiSchema",
    "StrictWsSchema",
    "StrictDataSchema",
    "WsMessageSchema",
    "MessageResponse",
]
