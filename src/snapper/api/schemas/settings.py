"""Settings schemas for the REST API.

This module defines request/response schemas for the settings management
endpoints, supporting CRUD operations on application settings.
"""

from datetime import datetime

from pydantic import Field

from snapper.api.schemas.base import StrictApiSchema


class SettingRead(StrictApiSchema):
    """Setting read response schema.

    Returned when fetching a setting from the database.

    Attributes:
        key: Unique setting key identifier.
        value: Setting value as string.
        category: Setting category for grouping.
        description: Optional human-readable description.
        updated_at: Last modification timestamp.
        updated_by: User who last modified the setting.
    """

    key: str
    value: str
    category: str
    description: str | None = None
    updated_at: datetime
    updated_by: str | None = None


class SettingUpdate(StrictApiSchema):
    """Setting update request schema.

    Used when updating an existing setting.

    Attributes:
        value: New setting value as string.
        category: Setting category (defaults to 'system').
        description: Optional description.
    """

    value: str = Field(..., description="Setting value as string")
    category: str = Field(default="system", description="Setting category")
    description: str | None = Field(None, description="Setting description")


class SettingCreate(StrictApiSchema):
    """Setting creation request schema.

    Used when creating a new setting.

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


__all__ = ["SettingRead", "SettingUpdate", "SettingCreate"]
