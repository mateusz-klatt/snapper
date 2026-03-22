"""Authentication request schemas module.

This module defines Pydantic schemas for authentication-related
API requests including login, user management, and password changes.

Request bodies are plain BaseModel (domain intent only). Each is wrapped
in a PayloadRequest envelope that carries provenance fields.
"""

from typing import Annotated
from typing import Literal

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

from snapper.api.schemas.base import PayloadRequest
from snapper.auth.domain.roles import UserRole


class LoginBody(BaseModel):
    """Login request body.

    Attributes:
        username: User's login name.
        password: User's password.
        remember_me: If True, extends refresh token lifetime.
    """

    model_config = ConfigDict(extra="forbid")

    username: str
    password: str
    remember_me: bool = False


class LoginRequest(PayloadRequest[Literal["login_request"], LoginBody]):
    """Login request envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["login_request"] = "login_request"


class CreateUserBody(BaseModel):
    """Create user request body.

    Attributes:
        username: Unique username (3-64 chars).
        email: Optional email address.
        password: Password (min 8 chars).
        role: User role to assign.
        is_active: Whether account is active.
    """

    model_config = ConfigDict(extra="forbid")

    username: str = Field(min_length=3, max_length=64)
    email: str | None = Field(default=None, max_length=255)
    password: str = Field(min_length=8)
    role: Annotated[UserRole, Field(strict=False)]
    is_active: bool = True


class CreateUserRequest(PayloadRequest[Literal["create_user_request"], CreateUserBody]):
    """Create user request envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["create_user_request"] = "create_user_request"


class UpdateUserBody(BaseModel):
    """Update user request body.

    All fields are optional - only provided fields are updated.

    Attributes:
        email: New email address.
        role: New role assignment.
        is_active: New active status.
    """

    model_config = ConfigDict(extra="forbid")

    email: str | None = Field(default=None, max_length=255)
    role: Annotated[UserRole, Field(strict=False)] | None = None
    is_active: bool | None = None


class UpdateUserRequest(PayloadRequest[Literal["update_user_request"], UpdateUserBody]):
    """Update user request envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["update_user_request"] = "update_user_request"


class ChangePasswordBody(BaseModel):
    """Change password request body.

    Used by authenticated users to change their own password.

    Attributes:
        current_password: Current password for verification.
        new_password: New password (min 8 chars).
    """

    model_config = ConfigDict(extra="forbid")

    current_password: str
    new_password: str = Field(min_length=8)


class ChangePasswordRequest(PayloadRequest[Literal["change_password_request"], ChangePasswordBody]):
    """Change password request envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["change_password_request"] = "change_password_request"


class AdminResetPasswordBody(BaseModel):
    """Admin password reset request body.

    Used by admins to reset another user's password
    without knowing the current password.

    Attributes:
        new_password: New password to set (min 8 chars).
    """

    model_config = ConfigDict(extra="forbid")

    new_password: str = Field(min_length=8)


class AdminResetPasswordRequest(
    PayloadRequest[Literal["admin_reset_password_request"], AdminResetPasswordBody]
):
    """Admin password reset request envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["admin_reset_password_request"] = "admin_reset_password_request"


class DeactivateUserBody(BaseModel):
    """Deactivate user command body (empty).

    Command-style endpoint: no domain fields needed, provenance
    is carried on the PayloadRequest envelope.

    Attributes:
        (none — empty body signals intent via URL path)
    """

    model_config = ConfigDict(extra="forbid")


class DeactivateUserRequest(PayloadRequest[Literal["deactivate_user_request"], DeactivateUserBody]):
    """Deactivate user request envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["deactivate_user_request"] = "deactivate_user_request"
