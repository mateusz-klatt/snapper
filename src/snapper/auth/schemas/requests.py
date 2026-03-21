"""Authentication request schemas module.

This module defines Pydantic schemas for authentication-related
API requests including login, user management, and password changes.
"""

from typing import Annotated
from typing import Literal

from pydantic import Field

from snapper.api.schemas.base import StrictDataSchema
from snapper.auth.domain.roles import UserRole


class LoginRequest(StrictDataSchema):
    """Login request schema.

    Attributes:
        type: Payload item type discriminator.
        username: User's login name.
        password: User's password.
        remember_me: If True, extends refresh token lifetime.
    """

    type: Literal["login_request"] = "login_request"
    username: str
    password: str
    remember_me: bool = False


class CreateUserRequest(StrictDataSchema):
    """Create user request schema.

    Attributes:
        type: Payload item type discriminator.
        username: Unique username (3-64 chars).
        email: Optional email address.
        password: Password (min 8 chars).
        role: User role to assign.
        is_active: Whether account is active.
    """

    type: Literal["create_user_request"] = "create_user_request"
    username: str = Field(min_length=3, max_length=64)
    email: str | None = Field(default=None, max_length=255)
    password: str = Field(min_length=8)
    role: Annotated[UserRole, Field(strict=False)]
    is_active: bool = True


class UpdateUserRequest(StrictDataSchema):
    """Update user request schema.

    All fields are optional - only provided fields are updated.

    Attributes:
        type: Payload item type discriminator.
        email: New email address.
        role: New role assignment.
        is_active: New active status.
    """

    type: Literal["update_user_request"] = "update_user_request"
    email: str | None = Field(default=None, max_length=255)
    role: Annotated[UserRole, Field(strict=False)] | None = None
    is_active: bool | None = None


class ChangePasswordRequest(StrictDataSchema):
    """Change password request schema.

    Used by authenticated users to change their own password.

    Attributes:
        type: Payload item type discriminator.
        current_password: Current password for verification.
        new_password: New password (min 8 chars).
    """

    type: Literal["change_password_request"] = "change_password_request"
    current_password: str
    new_password: str = Field(min_length=8)


class AdminResetPasswordRequest(StrictDataSchema):
    """Admin password reset request schema.

    Used by admins to reset another user's password
    without knowing the current password.

    Attributes:
        type: Payload item type discriminator.
        new_password: New password to set (min 8 chars).
    """

    type: Literal["admin_reset_password_request"] = "admin_reset_password_request"
    new_password: str = Field(min_length=8)
