"""Authentication response schemas module.

This module defines Pydantic schemas for authentication-related
API responses.
"""

from datetime import datetime
from typing import Literal

from snapper.api.schemas.base import PayloadListResponse
from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictDataSchema
from snapper.auth.schemas.user import UserProfile


class LoginData(StrictDataSchema[Literal["login"]]):
    """Login payload data.

    Attributes:
        type: Payload item type discriminator.
        message: Success message.
        expires_in: Access token TTL in seconds.
        user: Authenticated user profile.
        access_token: JWT returned only when ``?return_tokens=true``
            is set. ``None`` for the cookie-only browser flow.
        refresh_token: JWT returned only when ``?return_tokens=true``
            is set. ``None`` for the cookie-only browser flow.
    """

    type: Literal["login"] = "login"
    message: str
    expires_in: int
    user: UserProfile
    access_token: str | None = None
    refresh_token: str | None = None


class LoginResponse(PayloadResponse[Literal["login_response"], LoginData]):
    """Login REST response envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["login_response"] = "login_response"


class RefreshData(StrictDataSchema[Literal["refresh"]]):
    """Token refresh payload data.

    Attributes:
        type: Payload item type discriminator.
        message: Success message.
        ws_token: New WebSocket authentication token.
        ws_token_exp: WebSocket token expiration time.
        csrf_token: New CSRF token.
        user: User profile.
        access_token: JWT returned only when ``?return_tokens=true``
            is set. ``None`` for the cookie-only browser flow.
        refresh_token: JWT returned only when ``?return_tokens=true``
            is set. ``None`` for the cookie-only browser flow.
    """

    type: Literal["refresh"] = "refresh"
    message: str
    ws_token: str
    ws_token_exp: datetime
    csrf_token: str
    user: UserProfile
    access_token: str | None = None
    refresh_token: str | None = None


class RefreshResponse(PayloadResponse[Literal["refresh_response"], RefreshData]):
    """Token refresh REST response envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["refresh_response"] = "refresh_response"


class WsTokenData(StrictDataSchema[Literal["ws_token"]]):
    """WebSocket-token issuance payload.

    Returned by the dedicated ``POST /api/auth/ws_token`` route. The
    route authenticates an access bearer (no cookie path required for
    headless clients) and mints a one-shot WebSocket token from the
    access JWT's session.

    Attributes:
        type: Payload item type discriminator.
        message: Success message.
        ws_token: One-shot WebSocket authentication token.
        ws_token_exp: WebSocket token expiration time.
        expires_in: WebSocket token TTL in seconds (mirror of
            ``ws_token_exp``; convenience for clients that prefer
            relative-deadline math).
    """

    type: Literal["ws_token"] = "ws_token"
    message: str
    ws_token: str
    ws_token_exp: datetime
    expires_in: int


class WsTokenResponse(PayloadResponse[Literal["ws_token_response"], WsTokenData]):
    """WebSocket-token issuance REST response envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["ws_token_response"] = "ws_token_response"


class UserResponse(PayloadResponse[Literal["user_response"], UserProfile]):
    """Single user response wrapper.

    Wraps a UserProfile in a typed envelope for REST API consistency.

    Attributes:
        type: Payload item type discriminator.
        payload: The user profile data.
    """

    type: Literal["user_response"] = "user_response"


class UserListResponse(PayloadListResponse[Literal["user_list"], UserProfile]):
    """User list response schema.

    Returned by user listing endpoints.

    Attributes:
        type: Payload item type discriminator.
        payload: List of user profiles.
        count: Total number of users.
    """

    type: Literal["user_list"] = "user_list"
