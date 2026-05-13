"""Settings schemas for the REST API.

This module defines request/response schemas for the settings management
endpoints, supporting CRUD operations on application settings.

Request bodies inherit StrictBody (domain intent only, strict validation).
Each is wrapped in a PayloadRequest envelope that carries provenance fields.
"""

from datetime import datetime
from typing import Literal

from pydantic import Field

from snapper.api.schemas.base import PayloadListResponse
from snapper.api.schemas.base import PayloadRequest
from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictBody
from snapper.api.schemas.base import StrictDataSchema


class SettingRead(StrictDataSchema[Literal["setting_read"]]):
    """Setting read response schema.

    Returned when fetching a setting from the database.

    Attributes:
        type: Payload item type discriminator.
        key: Unique setting key identifier.
        value: Setting value as string.
        category: Setting category for grouping.
        description: Optional human-readable description.
        updated_at: Last modification timestamp.
        updated_by: User who last modified the setting.
    """

    type: Literal["setting_read"] = "setting_read"
    key: str
    value: str
    category: str
    description: str | None = None
    updated_at: datetime
    updated_by: str | None = None


class SettingUpdateBody(StrictBody):
    """Setting update request body.

    Attributes:
        value: New setting value as string.
        category: Setting category (defaults to 'system').
        description: Optional description.
    """

    value: str = Field(..., description="Setting value as string")
    category: str = Field(default="system", description="Setting category")
    description: str | None = Field(None, description="Setting description")


class SettingUpdate(PayloadRequest[Literal["setting_update"], SettingUpdateBody]):
    """Setting update request envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["setting_update"] = "setting_update"


class SettingCreateBody(StrictBody):
    """Setting creation request body.

    Attributes:
        key: Unique setting key identifier.
        value: Setting value as string.
        category: Setting category (defaults to 'system').
        description: Optional description.
    """

    key: str = Field(..., description="Setting key")
    value: str = Field(..., description="Setting value as string")
    category: str = Field(default="system", description="Setting category")
    description: str | None = Field(None, description="Setting description")


class SettingCreate(PayloadRequest[Literal["setting_create"], SettingCreateBody]):
    """Setting creation request envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["setting_create"] = "setting_create"


class SettingResponse(PayloadResponse[Literal["setting_response"], SettingRead]):
    """Single setting response wrapper.

    Wraps a SettingRead in a typed envelope for REST API consistency.

    Attributes:
        type: Payload item type discriminator.
        payload: The setting data.
    """

    type: Literal["setting_response"] = "setting_response"


class SettingListResponse(PayloadListResponse[Literal["setting_list"], SettingRead]):
    """Setting list response wrapper.

    Wraps a list of SettingRead items with a count for REST API consistency.

    Attributes:
        type: Payload item type discriminator.
        payload: List of setting data items.
        count: Total number of settings in the response.
    """

    type: Literal["setting_list"] = "setting_list"


class FeatureFlagsPayload(StrictBody):
    """Public feature-flag projection.

    Exposes ONLY the boolean feature flags that the frontend needs
    on mount to decide whether to render the ``/ai-integration``
    surface. No secrets, no per-user state, no setting values
    just the on/off state of feature gates that are safe to reveal
    to an unauthenticated caller.

    Attributes:
        ai_integration_enabled: Whether the MCP sub-app is
            activated. Defaults to ``True`` — operators must flip
            the setting to ``False`` to disable the feature. When
            disabled, the frontend hides the AI Integration
            navigation entry and the ``/api/mcp`` endpoint returns
            ``503 feature_disabled`` — always-mounted-but-gated
            semantics.
    """

    ai_integration_enabled: bool = Field(..., description="Whether the MCP sub-app is activated.")


class FeatureFlagsResponse(PayloadResponse[Literal["feature_flags_response"], FeatureFlagsPayload]):
    """Public feature-flag response envelope.

    Attributes:
        type: Payload item type discriminator.
        payload: Feature-flag booleans.
    """

    type: Literal["feature_flags_response"] = "feature_flags_response"


class RemoveSettingBody(StrictBody):
    """Remove setting command body (empty).

    Command-style endpoint: no domain fields needed, provenance
    is carried on the PayloadRequest envelope.

    Attributes:
        (none — empty body signals intent via URL path)
    """


class RemoveSettingRequest(PayloadRequest[Literal["remove_setting_request"], RemoveSettingBody]):
    """Remove setting request envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["remove_setting_request"] = "remove_setting_request"


class PushBetaConfigRead(StrictDataSchema[Literal["push_beta_config_read"]]):
    """Read projection of the active ``push_beta_config`` setting.

    Drives the rollout gate in ``application/notify/routing.py``:
    when ``enabled`` is true, only ``user_public_ids`` receive APNs
    pushes; everyone else is suppressed at the routing layer
    regardless of their per-device prefs. When ``enabled`` is false
    every authenticated user receives pushes (the legacy default
    pre-iOS-5).

    Attributes:
        type: Payload item type discriminator.
        enabled: Whether the beta gate is active.
        user_public_ids: Allowlist of UUID7 user identifiers when
            ``enabled`` is true. Empty list with ``enabled`` true
            silently drops every push — the admin contract.
    """

    type: Literal["push_beta_config_read"] = "push_beta_config_read"
    enabled: bool
    user_public_ids: list[str]


class PushBetaUsersBody(StrictBody):
    """Replace the entire push-beta allowlist + enabled flag.

    Attributes:
        enabled: New value of the gate flag.
        user_public_ids: Full replacement allowlist (the route does
            not merge — pass the complete intended list each call).
            Each id MUST be a non-empty string; the route validates
            without enforcing UUID7 format so the test fixture
            tooling is not coupled to the canonical generator.
    """

    enabled: bool
    user_public_ids: list[str] = Field(default=[])


class UpdatePushBetaUsersCommand(
    PayloadRequest[Literal["update_push_beta_users_command"], PushBetaUsersBody]
):
    """Request envelope for ``POST /api/settings/push-beta/users``."""

    type: Literal["update_push_beta_users_command"] = "update_push_beta_users_command"


class PushBetaConfigResponse(
    PayloadResponse[Literal["push_beta_config_response"], PushBetaConfigRead]
):
    """Singleton wrapper returned by GET + POST ``/push-beta/users``."""

    type: Literal["push_beta_config_response"] = "push_beta_config_response"


__all__ = [
    "FeatureFlagsPayload",
    "FeatureFlagsResponse",
    "PushBetaConfigRead",
    "PushBetaConfigResponse",
    "PushBetaUsersBody",
    "RemoveSettingBody",
    "RemoveSettingRequest",
    "SettingCreate",
    "SettingListResponse",
    "SettingRead",
    "SettingResponse",
    "SettingUpdate",
    "UpdatePushBetaUsersCommand",
]
