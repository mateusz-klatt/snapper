"""Authentication request schemas module.

This module defines Pydantic schemas for authentication-related
API requests including login, user management, and password changes.
"""

from typing import Annotated

from pydantic import Field

from snapper.api.schemas.base import StrictApiSchema
from snapper.auth.domain.roles import UserRole


class LoginRequest(StrictApiSchema):
    """Login request schema.

    Attributes:
        username: User's login name.
        password: User's password.
        remember_me: If True, extends refresh token lifetime.
    """

    username: str
    password: str
    remember_me: bool = False


class CreateUserRequest(StrictApiSchema):
    """Create user request schema.

    Attributes:
        username: Unique username (3-64 chars).
        email: Optional email address.
        password: Password (min 8 chars).
        role: User role to assign.
        is_active: Whether account is active.
    """

    username: str = Field(min_length=3, max_length=64)
    email: str | None = Field(default=None, max_length=255)
    password: str = Field(min_length=8)
    role: Annotated[UserRole, Field(strict=False)]
    is_active: bool = True


class UpdateUserRequest(StrictApiSchema):
    """Update user request schema.

    All fields are optional - only provided fields are updated.

    Attributes:
        email: New email address.
        role: New role assignment.
        is_active: New active status.
    """

    email: str | None = Field(default=None, max_length=255)
    role: Annotated[UserRole, Field(strict=False)] | None = None
    is_active: bool | None = None


class ChangePasswordRequest(StrictApiSchema):
    """Change password request schema.

    Used by authenticated users to change their own password.

    Attributes:
        current_password: Current password for verification.
        new_password: New password (min 8 chars).
    """

    current_password: str
    new_password: str = Field(min_length=8)


class AdminResetPasswordRequest(StrictApiSchema):
    """Admin password reset request schema.

    Used by admins to reset another user's password
    without knowing the current password.

    Attributes:
        new_password: New password to set (min 8 chars).
    """

    new_password: str = Field(min_length=8)
