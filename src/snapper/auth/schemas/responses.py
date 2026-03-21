"""Authentication response schemas module.

This module defines Pydantic schemas for authentication-related
API responses.
"""

from datetime import datetime
from typing import Literal

from snapper.api.schemas.base import StrictDataSchema
from snapper.auth.schemas.user import UserProfile


class LoginResponse(StrictDataSchema):
    """Login response schema.

    Returned after successful authentication.

    Attributes:
        type: Payload item type discriminator.
        message: Success message.
        expires_in: Access token TTL in seconds.
        user: Authenticated user profile.
    """

    type: Literal["login_response"] = "login_response"
    message: str
    expires_in: int
    user: UserProfile


class RefreshResponse(StrictDataSchema):
    """Token refresh response schema.

    Returned after successful token refresh.

    Attributes:
        type: Payload item type discriminator.
        message: Success message.
        ws_token: New WebSocket authentication token.
        ws_token_exp: WebSocket token expiration time.
        csrf_token: New CSRF token.
        user: User profile.
    """

    type: Literal["refresh_response"] = "refresh_response"
    message: str
    ws_token: str
    ws_token_exp: datetime
    csrf_token: str
    user: UserProfile


class UserResponse(StrictDataSchema):
    """Single user response wrapper.

    Wraps a UserProfile in a typed envelope for REST API consistency.

    Attributes:
        type: Payload item type discriminator.
        user: The user profile data.
    """

    type: Literal["user_response"] = "user_response"
    user: UserProfile


class UserListResponse(StrictDataSchema):
    """User list response schema.

    Returned by user listing endpoints.

    Attributes:
        type: Payload item type discriminator.
        items: List of user profiles.
        count: Total number of users.
    """

    type: Literal["user_list"] = "user_list"
    items: list[UserProfile]
    count: int
