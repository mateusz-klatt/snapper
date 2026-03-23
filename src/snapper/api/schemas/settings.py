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


__all__ = [
    "RemoveSettingBody",
    "RemoveSettingRequest",
    "SettingRead",
    "SettingUpdate",
    "SettingCreate",
    "SettingResponse",
    "SettingListResponse",
]
