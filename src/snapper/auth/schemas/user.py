"""User profile schema module.

This module defines the user profile schema used throughout
the authentication and authorization system.
"""

from datetime import datetime
from typing import Literal

from pydantic import Field

from snapper.api.schemas.base import StrictDataSchema
from snapper.auth.domain.roles import UserRole


class UserProfile(StrictDataSchema[Literal["user_profile"]]):
    """User profile schema.

    Represents authenticated user information returned by API
    endpoints and stored in request state.

    The multi-tenant operator fields (``operator_public_ids`` and
    ``primary_operator_public_id``) are populated by the ``/auth/me``
    endpoint from ``user_operator_memberships`` (with the ADMIN-wide
    expansion rule applied, mirroring ``UserService.build_auth_principal``).
    Write paths that project a raw DB ``User`` row return these fields
    empty; the response builder in ``auth/routes.py`` enriches them
    before returning to the client.

    Attributes:
        type: Payload item type discriminator.
        username: User's login name (also the primary key).
        email: Optional email address.
        role: User's role (VIEWER, OPERATOR, ADMIN).
        is_active: Whether user account is active.
        created_at: Account creation timestamp.
        operator_public_ids: Operators this user may act AS at the
            current ``as_of``. ADMIN receives every active operator;
            OPERATOR / VIEWER receive only their explicit memberships.
        primary_operator_public_id: The membership row marked
            ``is_primary=TRUE`` (``None`` when the user has no
            primary membership yet).
        active_wallet_public_id: Currently-selected wallet (UI state,
            round-tripped through token claims). ``None`` on the
            login path (user hasn't picked a wallet yet); populated
            on refresh and ``/me`` from the authenticated principal.
            The frontend reads this field to route WS subscriptions
            and REST requests against the same wallet scope the
            backend authorises.
        default_language: Optional catalog-language code (e.g.
            ``"pl"``, ``"de"``, ``"zh-Hans"``) that drives backend
            localization of push-notification titles/bodies + REST
            alert-history rendering. ``None`` for legacy users who
            have not yet picked a language via the frontend
            ``LocaleSwitcher``; the alert pipeline falls through to
            English emission in that case.
    """

    type: Literal["user_profile"] = "user_profile"
    username: str
    email: str | None = None
    role: UserRole
    is_active: bool = True
    created_at: datetime
    operator_public_ids: list[str] = Field(default=[])
    primary_operator_public_id: str | None = None
    active_wallet_public_id: str | None = None
    default_language: str | None = None
