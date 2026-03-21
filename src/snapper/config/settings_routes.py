"""REST API routes for system settings management.

This module provides FastAPI routes for CRUD operations on application
settings stored in the database. All endpoints require CONFIGURE_SYSTEM
permission (admin role).

Endpoints:
    - ``GET /settings`` - List all settings, optionally filtered by category.
    - ``GET /settings/categories`` - List distinct setting categories.
    - ``PUT /settings/{key}`` - Update or create a setting.
    - ``DELETE /settings/{key}`` - Delete a setting.

Settings are stored in the ``settings`` table with encryption support
for sensitive values (API keys, secrets).

Example:
    List all settings::

        GET /api/settings
        Authorization: Bearer <token>

    Update a setting::

        PUT /api/settings/kraken_api_key
        {"value": "new-api-key", "category": "exchanges"}
"""

from datetime import UTC
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from sqlalchemy import select
from sqlalchemy import update

from snapper.api.schemas.base import MessageResponse
from snapper.api.schemas.health import SettingCategoriesResponse
from snapper.api.schemas.settings import SettingRead
from snapper.api.schemas.settings import SettingUpdate
from snapper.application.services.settings import get_settings_service
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.user import UserProfile
from snapper.config.settings import get_settings
from snapper.data.models import Setting
from snapper.data.repository import get_repository
from snapper.data.repository import where_active
from snapper.messaging.infrastructure.publisher import SequenceTracker

router = APIRouter(prefix="/settings", tags=["settings"])


@router.get("")
async def get_all_settings(
    user: Annotated[UserProfile, Depends(require_permission(Permission.CONFIGURE_SYSTEM))],
    category: str | None = None,
) -> list[SettingRead]:
    """Retrieve all application settings, optionally filtered by category.

    Args:
        category: Optional category name to filter settings.
        user: Authenticated user with CONFIGURE_SYSTEM permission.

    Returns:
        List of all settings matching the filter criteria.
    """
    settings = get_settings()
    repository = get_repository(settings.db_url)
    async with repository.session() as session:
        query = select(Setting).where(*where_active(Setting))
        if category:
            query = query.where(Setting.category == category)
        result = await session.execute(query)
        db_settings = result.scalars().all()
        return [
            SettingRead(
                key=setting.key,
                value=setting.value,
                category=setting.category,
                description=setting.description,
                updated_at=setting.timestamp,
                updated_by=setting.updated_by,
            )
            for setting in db_settings
        ]


@router.get("/categories")
async def get_setting_categories(
    user: Annotated[UserProfile, Depends(require_permission(Permission.CONFIGURE_SYSTEM))],
) -> SettingCategoriesResponse:
    """Retrieve all distinct setting category names.

    Args:
        user: Authenticated user with CONFIGURE_SYSTEM permission.

    Returns:
        Response containing sorted list of category names.
    """
    settings = get_settings()
    repository = get_repository(settings.db_url)
    async with repository.session() as session:
        result = await session.execute(
            select(Setting.category).where(*where_active(Setting)).distinct()
        )
        categories = [row[0] for row in result.fetchall()]
        return SettingCategoriesResponse(categories=sorted(categories))


@router.put("/{key}", responses={404: {"description": "Setting not found"}})
async def update_setting(
    key: str,
    request: SettingUpdate,
    user: Annotated[UserProfile, Depends(require_permission(Permission.CONFIGURE_SYSTEM))],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
) -> SettingRead:
    """Update or create a setting by key.

    Args:
        key: The setting key to update or create.
        request: Setting update payload with value and metadata.
        user: Authenticated user with CONFIGURE_SYSTEM permission.

    Returns:
        The updated setting.

    Raises:
        HTTPException: If setting not found after update.
    """
    settings = get_settings()
    settings_service = await get_settings_service(
        settings.db_url,
        settings.zmq_broker_xpub,
    )
    await settings_service.update_setting(
        key=key,
        value=request.value,
        category=request.category,
        description=request.description,
        updated_by=user.username,
    )
    repository = get_repository(settings.db_url)
    async with repository.session() as session:
        result = await session.execute(
            select(Setting).where(Setting.key == key, *where_active(Setting))
        )
        setting = result.scalar_one_or_none()
        if not setting:
            raise HTTPException(status_code=404, detail=f"Setting '{key}' not found")
        return SettingRead(
            key=setting.key,
            value=setting.value,
            category=setting.category,
            description=setting.description,
            updated_at=setting.timestamp,
            updated_by=setting.updated_by,
        )


@router.delete("/{key}", responses={404: {"description": "Setting not found"}})
async def delete_setting(
    request: Request,
    key: str,
    user: Annotated[UserProfile, Depends(require_permission(Permission.CONFIGURE_SYSTEM))],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
) -> MessageResponse:
    """Delete a setting by key.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        key: The setting key to delete.
        user: Authenticated user with CONFIGURE_SYSTEM permission.

    Returns:
        Success message confirming deletion.

    Raises:
        HTTPException: If setting not found.
    """
    settings = get_settings()
    repository = get_repository(settings.db_url)
    async with repository.session() as session:
        result = await session.execute(
            select(Setting).where(Setting.key == key, *where_active(Setting))
        )
        setting = result.scalar_one_or_none()
        if not setting:
            raise HTTPException(status_code=404, detail=f"Setting '{key}' not found")
        now = datetime.now(UTC)
        await session.execute(update(Setting).where(Setting.id == setting.id).values(known_to=now))
        await session.commit()
    tracker: SequenceTracker = request.app.state.rest_tracker
    return MessageResponse(
        message=f"Setting '{key}' deleted successfully",
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence("rest.control"),
    )
