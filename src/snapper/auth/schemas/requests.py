"""Authentication request schemas module.

This module defines Pydantic schemas for authentication-related
API requests including login, user management, and password changes.

Request bodies inherit StrictBody (domain intent only, strict validation).
Each is wrapped in a PayloadRequest envelope that carries provenance fields.
"""

from typing import Literal
from typing import Self

from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator

from snapper.api.schemas.base import PayloadRequest
from snapper.api.schemas.base import StrictBody
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.core.ids import is_uuid7
from snapper.i18n.supported_languages import SUPPORTED_LANGUAGES


class LoginBody(StrictBody):
    """Login request body.

    Attributes:
        username: User's login name.
        password: User's password.
        remember_me: If True, extends refresh token lifetime.
        permissions: Optional narrower permission grant for this token pair.
            Omission retains the complete role grant.
    """

    username: str
    password: str
    remember_me: bool = False
    permissions: list[Permission] | None = None


class LoginRequest(PayloadRequest[Literal["login_request"], LoginBody]):
    """Login request envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["login_request"] = "login_request"


class CreateUserBody(StrictBody):
    """Create user request body.

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
    role: UserRole
    is_active: bool = True


class CreateUserRequest(PayloadRequest[Literal["create_user_request"], CreateUserBody]):
    """Create user request envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["create_user_request"] = "create_user_request"


class UpdateUserBody(StrictBody):
    """Update user request body.

    All fields are optional - only provided fields are updated.

    Attributes:
        email: New email address.
        role: New role assignment.
        is_active: New active status.
    """

    email: str | None = Field(default=None, max_length=255)
    role: UserRole | None = None
    is_active: bool | None = None


class UpdateUserRequest(PayloadRequest[Literal["update_user_request"], UpdateUserBody]):
    """Update user request envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["update_user_request"] = "update_user_request"


class UpdateAuthMeBody(StrictBody):
    """Self-service caller preferences request body.

    Used by the authenticated user to update their own preferences via
    ``POST /api/auth/me/update``. The body manages the caller's
    ``default_language`` preference. The admin-facing
    :class:`UpdateUserBody` deliberately stays separate so the
    ``MANAGE_USERS`` permission does not silently expand into
    self-service preference writes.

    Attributes:
        default_language: Catalog-language code (one of
            :data:`snapper.i18n.supported_languages.SUPPORTED_LANGUAGES`)
            or ``None`` to clear the preference and revert push
            notifications + alert history to English emission for this
            user.
    """

    default_language: str | None = Field(default=None, max_length=20)

    @field_validator("default_language")
    @classmethod
    def _validate_language_code(cls, value: str | None) -> str | None:
        """Reject unknown language codes at the schema boundary.

        Lets the service layer trust the value without a second check.
        """
        if value is None:
            return None
        if value not in SUPPORTED_LANGUAGES:
            raise ValueError(
                f"Unknown language code '{value}'. Must be one of the "
                f"{len(SUPPORTED_LANGUAGES)} catalog languages."
            )
        return value


class UpdateAuthMeRequest(PayloadRequest[Literal["update_auth_me_request"], UpdateAuthMeBody]):
    """Self-service caller preferences request envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["update_auth_me_request"] = "update_auth_me_request"


class ChangePasswordBody(StrictBody):
    """Change password request body.

    Used by authenticated users to change their own password.

    Attributes:
        current_password: Current password for verification.
        new_password: New password (min 8 chars).
    """

    current_password: str
    new_password: str = Field(min_length=8)


class ChangePasswordRequest(PayloadRequest[Literal["change_password_request"], ChangePasswordBody]):
    """Change password request envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["change_password_request"] = "change_password_request"


class AdminResetPasswordBody(StrictBody):
    """Admin password reset request body.

    Used by admins to reset another user's password
    without knowing the current password.

    Attributes:
        new_password: New password to set (min 8 chars).
    """

    new_password: str = Field(min_length=8)


class AdminResetPasswordRequest(
    PayloadRequest[Literal["admin_reset_password_request"], AdminResetPasswordBody]
):
    """Admin password reset request envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["admin_reset_password_request"] = "admin_reset_password_request"


class DeactivateUserBody(StrictBody):
    """Deactivate user command body.

    Command-style endpoint with optional human-readable rationale that
    is forwarded to the canonical `admin.user_deactivated` bus payload
    so subscribers (`AuthenticatedWebSocketManager`,
    cross-instance `TokenManager`, audit consumers) can record why the
    kill switch fired.

    Attributes:
        reason: Optional admin note explaining the deactivation. ``None``
            when the request body is omitted entirely.
    """

    reason: str | None = None


class DeactivateUserRequest(PayloadRequest[Literal["deactivate_user_request"], DeactivateUserBody]):
    """Deactivate user request envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["deactivate_user_request"] = "deactivate_user_request"


class RefreshTokenPayload(StrictBody):
    """Optional refresh-token body.

    Both fields are optional; when absent the caller inherits the
    existing JWT claims byte-identically. Set ``active_wallet_public_id``
    to mint new tokens scoped to that wallet (after server-side
    membership validation); set ``clear_active_wallet`` to explicitly
    clear the claim to ``None`` ("All wallets" UI option).
    Invariant: both fields MUST NOT be set simultaneously — enforced
    by the ``@model_validator`` below (422 at parse time). The
    ``active_wallet_public_id`` value must be a canonical UUID7
    a Pydantic field validator rejects malformed input with 422
    before any membership query fires.

    Attributes:
        active_wallet_public_id: Optional wallet to scope the new
            tokens to. None means "leave the claim unchanged".
        clear_active_wallet: Explicit clear signal. ``True`` mints
            new tokens with ``active_wallet_public_id=None``. Cannot
            co-exist with ``active_wallet_public_id``.
    """

    active_wallet_public_id: str | None = None
    clear_active_wallet: bool = False

    @field_validator("active_wallet_public_id")
    @classmethod
    def _validate_wallet_uuid7(cls, v: str | None) -> str | None:
        """Fail fast at parse time if the wallet hint is not a UUID7."""
        if v is None:
            return v
        if not is_uuid7(v):
            raise ValueError("active_wallet_public_id must be a UUID7")
        return v

    @model_validator(mode="after")
    def _validate_mutually_exclusive(self) -> Self:
        """active_wallet_public_id and clear_active_wallet are exclusive."""
        if self.active_wallet_public_id is not None and self.clear_active_wallet:
            raise ValueError(
                "active_wallet_public_id and clear_active_wallet are mutually exclusive"
            )
        return self


class RefreshTokenRequest(PayloadRequest[Literal["refresh_token_request"], RefreshTokenPayload]):
    """Refresh-token request envelope.

    Inherits the project's provenance envelope; ``payload`` carries
    the optional domain command. Registered on the refresh route with
    :func:`optional_json_body` so every existing zero-body caller
    (``apiClient.refreshAndRetry``, ``stores/auth.refreshToken``,
    WS ticket refresh) stays byte-identical.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["refresh_token_request"] = "refresh_token_request"
