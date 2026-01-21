"""WebSocket authentication schemas module.

This module defines Pydantic schemas for WebSocket authentication
messages and responses.
"""

from snapper.api.schemas.base import StrictApiSchema
from snapper.auth.domain.roles import UserRole


class WebSocketAuthMessage(StrictApiSchema):
    """WebSocket authentication message schema.

    Sent by client to authenticate WebSocket connection.

    Attributes:
        type: Message type (always "auth").
        token: JWT access token.
    """

    type: str = "auth"
    token: str


class WebSocketAuthResponse(StrictApiSchema):
    """WebSocket authentication response schema.

    Sent by server after authentication attempt.

    Attributes:
        type: Message type (always "auth_response").
        success: Whether authentication succeeded.
        user_id: User ID if successful.
        role: User role if successful.
        error: Error message if failed.
    """

    type: str = "auth_response"
    success: bool
    user_id: str | None = None
    role: UserRole | None = None
    error: str | None = None
