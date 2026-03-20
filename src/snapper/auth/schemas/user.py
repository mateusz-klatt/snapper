"""User profile schema module.

This module defines the user profile schema used throughout
the authentication and authorization system.
"""

from datetime import UTC
from datetime import datetime
from typing import Literal

from pydantic import Field

from snapper.api.schemas.base import StrictApiSchema
from snapper.auth.domain.roles import UserRole


class UserProfile(StrictApiSchema):
    """User profile schema.

    Represents authenticated user information returned by API
    endpoints and stored in request state.

    Attributes:
        type: Payload item type discriminator.
        username: User's login name (also the primary key).
        email: Optional email address.
        role: User's role (VIEWER, OPERATOR, ADMIN).
        is_active: Whether user account is active.
        created_at: Account creation timestamp.
    """

    type: Literal["user_profile"] = "user_profile"
    username: str
    email: str | None = None
    role: UserRole
    is_active: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
