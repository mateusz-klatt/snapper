"""Authentication response schemas module.

This module defines Pydantic schemas for authentication-related
API responses.
"""

from datetime import datetime

from snapper.api.schemas.base import StrictApiSchema
from snapper.auth.schemas.user import UserProfile


class LoginResponse(StrictApiSchema):
    """Login response schema.

    Returned after successful authentication.

    Attributes:
        message: Success message.
        expires_in: Access token TTL in seconds.
        user: Authenticated user profile.
    """

    message: str
    expires_in: int
    user: UserProfile


class RefreshResponse(StrictApiSchema):
    """Token refresh response schema.

    Returned after successful token refresh.

    Attributes:
        message: Success message.
        ws_token: New WebSocket authentication token.
        ws_token_exp: WebSocket token expiration time.
        csrf_token: New CSRF token.
        user: User profile.
    """

    message: str
    ws_token: str
    ws_token_exp: datetime
    csrf_token: str
    user: UserProfile


class UserListResponse(StrictApiSchema):
    """User list response schema.

    Returned by user listing endpoints.

    Attributes:
        users: List of user profiles.
        total_count: Total number of users.
    """

    users: list[UserProfile]
    total_count: int
