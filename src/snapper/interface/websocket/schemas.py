"""WebSocket message schemas for real-time communication.

This module defines Pydantic schemas for all WebSocket protocol messages
including authentication, subscription management, and error handling.
All messages use a type discriminator field for routing.

Message categories:
    - Authentication: auth_required, auth_ok, auth_failed, auth_complete
    - Reauthentication: reauth_required, reauth_ok, auth_expired
    - Subscriptions: subscribe, unsubscribe, subscription_success, subscriptions_list
    - Utility: ping, pong, error
"""

from datetime import datetime
from typing import Literal

from pydantic import Field

from snapper.api.schemas.base import WsMessageSchema
from snapper.auth.domain.roles import UserRole
from snapper.core.types import ExecutionMode
from snapper.core.types import FillStatus
from snapper.core.types import HealthStatus
from snapper.core.types import OrderStatus
from snapper.core.types import OrderType
from snapper.core.types import SubscriptionAction
from snapper.core.types import SubscriptionStatus
from snapper.core.types import TradeSide

_TYPE_DESC = "Message type discriminator"

__all__ = [
    "ExecutionMode",
    "FillStatus",
    "HealthStatus",
    "OrderStatus",
    "OrderType",
    "TradeSide",
]


class WSErrorResponse(WsMessageSchema):
    """WebSocket error response message.

    Sent when an error occurs during message processing.

    Attributes:
        type: Message type discriminator ('error').
        message: Human-readable error description.
    """

    type: Literal["error"] = Field(default="error", description=_TYPE_DESC)
    message: str = Field(..., description="Error description")


class WSAuthOkResponse(WsMessageSchema):
    """Authentication success acknowledgment.

    Sent after successful ws_token verification.

    Attributes:
        type: Message type discriminator ('auth_ok').
        exp: Token expiration timestamp (ISO 8601).
    """

    type: Literal["auth_ok"] = Field(default="auth_ok", description=_TYPE_DESC)
    exp: datetime = Field(..., description="Token expiration (ISO 8601)")


class WSAuthRequiredResponse(WsMessageSchema):
    """Authentication request message.

    Sent immediately after WebSocket connection to request authentication.

    Attributes:
        type: Message type discriminator ('auth_required').
        timeout: Seconds until authentication timeout.
    """

    type: Literal["auth_required"] = Field("auth_required", description=_TYPE_DESC)
    timeout: int = Field(default=30, description="Authentication timeout in seconds")


class WSAuthFailedResponse(WsMessageSchema):
    """Authentication failure message.

    Sent when authentication fails for any reason.

    Attributes:
        type: Message type discriminator ('auth_failed').
        reason: Optional failure reason code.
    """

    type: Literal["auth_failed"] = Field(default="auth_failed", description=_TYPE_DESC)
    reason: str | None = Field(default=None, description="Failure reason")


class WSReauthRequiredResponse(WsMessageSchema):
    """Reauthentication warning message.

    Sent before token expiration to prompt client to refresh.

    Attributes:
        type: Message type discriminator ('reauth_required').
        deadline: Deadline for reauthentication (ISO 8601).
    """

    type: Literal["reauth_required"] = Field("reauth_required", description=_TYPE_DESC)
    deadline: datetime = Field(..., description="Deadline for reauthentication (ISO 8601)")


class WSReauthOkResponse(WsMessageSchema):
    """Reauthentication success acknowledgment.

    Sent after successful token refresh.

    Attributes:
        type: Message type discriminator ('reauth_ok').
        exp: New token expiration timestamp (ISO 8601).
    """

    type: Literal["reauth_ok"] = Field(default="reauth_ok", description=_TYPE_DESC)
    exp: datetime = Field(..., description="New token expiration (ISO 8601)")


class WSAuthExpiredResponse(WsMessageSchema):
    """Authentication expiration notification.

    Sent when token expires and grace period ends.

    Attributes:
        type: Message type discriminator ('auth_expired').
    """

    type: Literal["auth_expired"] = Field("auth_expired", description=_TYPE_DESC)


class WSAuthCompleteResponse(WsMessageSchema):
    """Authentication complete message with session info.

    Sent after successful authentication with available topics.

    Attributes:
        type: Message type discriminator ('auth_complete').
        available_topics: Topics available for subscription.
        user_role: Authenticated user's role.
        session_expires_at: Session expiration (ISO 8601).
        ws_token_exp: WebSocket token expiration (ISO 8601).
    """

    type: Literal["auth_complete"] = Field("auth_complete", description=_TYPE_DESC)
    available_topics: list[str] = Field(..., description="Topics available for subscription")
    user_role: UserRole = Field(..., description="Authenticated user role")
    session_expires_at: datetime | None = Field(
        default=None, description="Session expiration (ISO 8601)"
    )
    ws_token_exp: datetime = Field(..., description="WS token expiration (ISO 8601)")


class WSSubscribeRequest(WsMessageSchema):
    """Topic subscription request from client.

    Attributes:
        type: Message type discriminator ('subscribe').
        topics: List of topics to subscribe to.
    """

    type: Literal["subscribe"] = Field(default="subscribe", description=_TYPE_DESC)
    topics: list[str] = Field(..., description="Topics to subscribe to")


class WSUnsubscribeRequest(WsMessageSchema):
    """Topic unsubscription request from client.

    Attributes:
        type: Message type discriminator ('unsubscribe').
        topics: List of topics to unsubscribe from.
    """

    type: Literal["unsubscribe"] = Field(default="unsubscribe", description=_TYPE_DESC)
    topics: list[str] = Field(..., description="Topics to unsubscribe from")


class WSSubscriptionSuccessResponse(WsMessageSchema):
    """Subscription operation result message.

    Sent after subscribe/unsubscribe operations with detailed status.

    Attributes:
        type: Message type discriminator ('subscription_success').
        action: The action performed (subscribe/unsubscribe).
        status: Result status (subscribed, unsubscribed, partial, denied, no_topics).
        topics: Topics that were successfully processed.
        denied_topics: Topics denied due to permissions.
        active_subscriptions: Current list of active subscriptions.
        message: Optional additional details.
    """

    type: Literal["subscription_success"] = Field("subscription_success", description=_TYPE_DESC)
    action: SubscriptionAction = Field(..., description="The subscription action performed")
    status: SubscriptionStatus = Field(
        ..., description="Result status of the subscription operation"
    )
    topics: list[str] = Field(..., description="Topics that were successfully processed")
    denied_topics: list[str] = Field(
        default_factory=list, description="Topics that were denied due to permissions"
    )
    active_subscriptions: list[str] = Field(..., description="Current list of active subscriptions")
    message: str | None = Field(
        default=None, description="Optional message with additional details"
    )


class WSSubscriptionsListResponse(WsMessageSchema):
    """Active subscriptions list response.

    Sent in response to get_subscriptions request.

    Attributes:
        type: Message type discriminator ('subscriptions_list').
        subscriptions: Current active subscriptions.
        available_topics: Topics available for subscription.
        total_available: Total number of available topics.
    """

    type: Literal["subscriptions_list"] = Field("subscriptions_list", description=_TYPE_DESC)
    subscriptions: list[str] = Field(..., description="Current active subscriptions")
    available_topics: list[str] = Field(..., description="Topics available for subscription")
    total_available: int = Field(..., description="Total number of available topics")


class WSPongResponse(WsMessageSchema):
    """Pong response to ping request.

    Includes server timestamp and connection count.

    Attributes:
        type: Message type discriminator ('pong').
        timestamp: Server timestamp (ISO 8601).
        active_connections: Number of active WebSocket connections.
    """

    type: Literal["pong"] = Field(default="pong", description=_TYPE_DESC)
    timestamp: datetime = Field(..., description="Server timestamp (ISO 8601)")
    active_connections: int = Field(..., description="Number of active WebSocket connections")


class WSAuthenticateRequest(WsMessageSchema):
    """Initial authentication request from client.

    Sent in response to auth_required message.

    Attributes:
        type: Message type discriminator ('authenticate').
        ws_token: WebSocket authentication token.
    """

    type: Literal["authenticate"] = Field(default="authenticate", description=_TYPE_DESC)
    ws_token: str = Field(..., description="WebSocket authentication token")


class WSReauthRequest(WsMessageSchema):
    """Reauthentication request from client.

    Sent to refresh token before expiration.

    Attributes:
        type: Message type discriminator ('reauth').
        ws_token: New WebSocket authentication token.
    """

    type: Literal["reauth"] = Field(default="reauth", description=_TYPE_DESC)
    ws_token: str = Field(..., description="New WebSocket authentication token")


class WSPingRequest(WsMessageSchema):
    """Ping request from client.

    Used for connection health checks.

    Attributes:
        type: Message type discriminator ('ping').
    """

    type: Literal["ping"] = Field(default="ping", description=_TYPE_DESC)


class WSGetSubscriptionsRequest(WsMessageSchema):
    """Request current subscriptions list.

    Attributes:
        type: Message type discriminator ('get_subscriptions').
    """

    type: Literal["get_subscriptions"] = Field(default="get_subscriptions", description=_TYPE_DESC)
