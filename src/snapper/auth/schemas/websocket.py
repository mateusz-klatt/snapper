"""WebSocket authentication schemas module.

This module defines Pydantic schemas for WebSocket authentication
messages and responses.
"""

from typing import Literal

from snapper.api.schemas.base import StrictDataSchema
from snapper.auth.domain.roles import UserRole


class WebSocketAuthMessage(StrictDataSchema[Literal["auth"]]):
    """WebSocket authentication message schema.

    Sent by client to authenticate WebSocket connection.

    Attributes:
        type: Message type (always "auth").
        token: JWT access token.
    """

    type: Literal["auth"] = "auth"
    token: str


class WebSocketAuthResponse(StrictDataSchema[Literal["auth_response"]]):
    """WebSocket authentication response schema.

    Sent by server after authentication attempt.

    Attributes:
        type: Message type (always "auth_response").
        success: Whether authentication succeeded.
        user_id: User ID if successful.
        role: User role if successful.
        error: Error message if failed.
    """

    type: Literal["auth_response"] = "auth_response"
    success: bool
    user_id: str | None = None
    role: UserRole | None = None
    error: str | None = None
