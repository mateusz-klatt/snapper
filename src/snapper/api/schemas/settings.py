"""Settings schemas for the REST API.

This module defines request/response schemas for the settings management
endpoints, supporting CRUD operations on application settings.
"""

from datetime import datetime
from typing import Literal

from pydantic import Field

from snapper.api.schemas.base import PayloadListResponse
from snapper.api.schemas.base import PayloadResponse
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


class SettingUpdate(StrictDataSchema[Literal["setting_update"]]):
    """Setting update request schema.

    Used when updating an existing setting.

    Attributes:
        type: Payload item type discriminator.
        value: New setting value as string.
        category: Setting category (defaults to 'system').
        description: Optional description.
    """

    type: Literal["setting_update"] = "setting_update"
    value: str = Field(..., description="Setting value as string")
    category: str = Field(default="system", description="Setting category")
    description: str | None = Field(None, description="Setting description")


class SettingCreate(StrictDataSchema[Literal["setting_create"]]):
    """Setting creation request schema.

    Used when creating a new setting.

    Attributes:
        type: Payload item type discriminator.
        key: Unique setting key identifier.
        value: Setting value as string.
        category: Setting category (defaults to 'system').
        description: Optional description.
    """

    type: Literal["setting_create"] = "setting_create"
    key: str = Field(..., description="Setting key")
    value: str = Field(..., description="Setting value as string")
    category: str = Field(default="system", description="Setting category")
    description: str | None = Field(None, description="Setting description")


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


__all__ = [
    "SettingRead",
    "SettingUpdate",
    "SettingCreate",
    "SettingResponse",
    "SettingListResponse",
]
