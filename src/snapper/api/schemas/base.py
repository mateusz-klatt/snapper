"""Base Pydantic schemas for REST API, WebSocket messages, and ZMQ data payloads.

This module defines foundational schema classes and configuration objects used throughout
the API layer for request/response validation. All API schemas inherit from these base
classes to ensure consistent validation behavior.

Configuration variants:
    - STRICT_API_CONFIG: Forbids extra fields, uses strict type coercion
    - STRICT_WS_CONFIG: Forbids extra fields but allows loose type coercion for WebSocket
    - STRICT_DATA_CONFIG: Forbids extra fields, loose coercion for ZMQ data payloads
"""

from datetime import UTC
from datetime import datetime
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


class StrictApiSchema(BaseModel):
    """Base schema for REST API request/response models.

    Inherits Pydantic BaseModel with strict validation:
    - Extra fields are forbidden (HTTP 422 on unknown fields)
    - Strict type coercion (no implicit conversions)
    - Default values are validated
    - Field aliases are populated by name
    """

    model_config = STRICT_API_CONFIG


STRICT_WS_CONFIG = ConfigDict(
    extra="forbid",
    strict=False,
    validate_default=True,
    populate_by_name=True,
)


class StrictWsSchema(BaseModel):
    """Base schema for WebSocket message models.

    Similar to StrictApiSchema but with relaxed type coercion (strict=False)
    to handle JavaScript clients that may send numbers as strings.
    Extra fields are still forbidden to catch protocol errors.
    """

    model_config = STRICT_WS_CONFIG


class WsMessageSchema(StrictWsSchema):
    """Base schema for WebSocket messages with timestamp.

    All WebSocket messages include a type discriminator and timestamp.
    The timestamp defaults to current UTC time if not provided.

    Attributes:
        type: Message type discriminator for routing.
        timestamp: When the message was created (UTC).
    """

    type: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


STRICT_DATA_CONFIG = ConfigDict(
    extra="forbid",
    strict=False,
    validate_default=True,
    populate_by_name=True,
)


class StrictDataSchema(BaseModel):
    """Base schema for all ZMQ data payloads and message entities.

    Every entity on the messaging bus inherits from this base, gaining a unique
    UUID7 identifier, a type discriminator for routing/parsing, and a bus timestamp
    recording when the entity was created. Subclasses MUST override type with a
    Literal default (e.g. type: Literal["candle"] = "candle").

    Also provides to_json/from_json for ZMQ serialization.

    Attributes:
        id: Unique identifier (UUID7), generated at creation time.
        type: Message type discriminator for routing and deserialization.
        timestamp: Bus arrival timestamp (UTC), generated once at creation.
    """

    model_config = STRICT_DATA_CONFIG

    id: str = Field(default_factory=lambda: str(uuid7()))
    type: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))

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


class MessageResponse(StrictApiSchema):
    """Generic API response containing a single message.

    Used for simple acknowledgment responses.

    Attributes:
        message: Human-readable response message.
    """

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
