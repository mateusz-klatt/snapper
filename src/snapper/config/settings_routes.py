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
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from sqlalchemy import select
from sqlalchemy import update

from snapper.api.schemas.base import MessageResponse
from snapper.api.schemas.health import SettingCategoriesResponse
from snapper.api.schemas.settings import SettingListResponse
from snapper.api.schemas.settings import SettingRead
from snapper.api.schemas.settings import SettingResponse
from snapper.api.schemas.settings import SettingUpdate
from snapper.application.services.settings import get_settings_service
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.config.settings import get_settings
from snapper.data.models import Setting
from snapper.data.repository import get_repository
from snapper.data.repository import where_active
from snapper.messaging.infrastructure.publisher import SequenceTracker

router = APIRouter(prefix="/settings", tags=["settings"])

_REST_STREAM = "rest.control"


@router.get("")
async def get_all_settings(
    request: Request,
    user: Annotated[AuthPrincipal, Depends(require_permission(Permission.CONFIGURE_SYSTEM))],
    category: str | None = None,
) -> SettingListResponse:
    """Retrieve all application settings, optionally filtered by category.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        category: Optional category name to filter settings.
        user: Authenticated user with CONFIGURE_SYSTEM permission.

    Returns:
        SettingListResponse wrapping all settings matching the filter criteria.
    """
    settings = get_settings()
    repository = get_repository(settings.db_url)
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = datetime.now(UTC)
    pid = str(uuid7())
    async with repository.session() as session:
        query = select(Setting).where(*where_active(Setting))
        if category:
            query = query.where(Setting.category == category)
        result = await session.execute(query)
        db_settings = result.scalars().all()
        items = [
            SettingRead(
                public_id=setting.public_id,
                timestamp=setting.timestamp,
                session_id=setting.session_id,
                sequence_id=setting.sequence_id,
                key=setting.key,
                value=setting.value,
                category=setting.category,
                description=setting.description,
                updated_at=setting.timestamp,
                updated_by=setting.updated_by,
            )
            for setting in db_settings
        ]
        return SettingListResponse(
            session_id=sid,
            sequence_id=seq,
            public_id=pid,
            timestamp=ts,
            payload=items,
            count=len(items),
        )


@router.get("/categories")
async def get_setting_categories(
    request: Request,
    user: Annotated[AuthPrincipal, Depends(require_permission(Permission.CONFIGURE_SYSTEM))],
) -> SettingCategoriesResponse:
    """Retrieve all distinct setting category names.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
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
        tracker: SequenceTracker = request.app.state.rest_tracker
        sid = tracker.session_id
        seq = tracker.next_sequence(_REST_STREAM)
        ts = datetime.now(UTC)
        pid = str(uuid7())
        return SettingCategoriesResponse(
            session_id=sid,
            sequence_id=seq,
            public_id=pid,
            timestamp=ts,
            payload=sorted(categories),
            count=len(categories),
        )


@router.put("/{key}", responses={404: {"description": "Setting not found"}})
async def update_setting(
    http_request: Request,
    key: str,
    body: SettingUpdate,
    user: Annotated[AuthPrincipal, Depends(require_permission(Permission.CONFIGURE_SYSTEM))],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
) -> SettingResponse:
    """Update or create a setting by key.

    Args:
        http_request: FastAPI request (provides REST tracker for provenance).
        key: The setting key to update or create.
        body: Setting update payload with value and metadata.
        user: Authenticated user with CONFIGURE_SYSTEM permission.

    Returns:
        SettingResponse wrapping the updated setting.

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
        value=body.value,
        category=body.category,
        description=body.description,
        updated_by=user.username,
    )
    repository = get_repository(settings.db_url)
    tracker: SequenceTracker = http_request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = datetime.now(UTC)
    pid = str(uuid7())
    async with repository.session() as session:
        result = await session.execute(
            select(Setting).where(Setting.key == key, *where_active(Setting))
        )
        setting = result.scalar_one_or_none()
        if not setting:
            raise HTTPException(status_code=404, detail=f"Setting '{key}' not found")
        setting_read = SettingRead(
            public_id=setting.public_id,
            timestamp=setting.timestamp,
            session_id=setting.session_id,
            sequence_id=setting.sequence_id,
            key=setting.key,
            value=setting.value,
            category=setting.category,
            description=setting.description,
            updated_at=setting.timestamp,
            updated_by=setting.updated_by,
        )
        return SettingResponse(
            session_id=sid,
            sequence_id=seq,
            public_id=pid,
            timestamp=ts,
            payload=setting_read,
        )


@router.delete("/{key}", responses={404: {"description": "Setting not found"}})
async def delete_setting(
    request: Request,
    key: str,
    user: Annotated[AuthPrincipal, Depends(require_permission(Permission.CONFIGURE_SYSTEM))],
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
        payload=f"Setting '{key}' deleted successfully",
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence(_REST_STREAM),
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
    )
