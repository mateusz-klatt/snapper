"""REST API routes for system settings management.

This module provides FastAPI routes for CRUD operations on application
settings stored in the database. All endpoints require CONFIGURE_SYSTEM
permission (admin role).

Endpoints:
    - ``GET /settings/features`` - Return public feature flags.
    - ``GET /settings`` - List all settings, optionally filtered by category.
    - ``GET /settings/categories`` - List distinct setting categories.
    - ``POST /settings/{key}/set`` - Set (update or create) a setting.
    - ``GET /settings/push-beta/users`` - Read push-beta gate configuration.
    - ``POST /settings/push-beta/users`` - Update push-beta gate configuration.
    - ``POST /settings/{key}/remove`` - Remove a setting.

Settings are stored in the ``settings`` table with encryption support
for sensitive values (API keys, secrets).

Example:
    List all settings::

        GET /api/settings
        Authorization: Bearer <token>

    Update a setting::

        POST /api/settings/polygon_api_key/set
        {
            "type": "setting_update",
            "payload": {
                "value": "new-api-key",
                "category": "api"
            }
        }
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
from snapper.api.schemas.settings import FeatureFlagsPayload
from snapper.api.schemas.settings import FeatureFlagsResponse
from snapper.api.schemas.settings import PushBetaConfigRead
from snapper.api.schemas.settings import PushBetaConfigResponse
from snapper.api.schemas.settings import RemoveSettingRequest
from snapper.api.schemas.settings import SettingListResponse
from snapper.api.schemas.settings import SettingRead
from snapper.api.schemas.settings import SettingResponse
from snapper.api.schemas.settings import SettingUpdate
from snapper.api.schemas.settings import UpdatePushBetaUsersCommand
from snapper.application.notify.push_beta import PUSH_BETA_SETTING_KEY
from snapper.application.notify.push_beta import PushBetaConfig
from snapper.application.notify.push_beta import parse_push_beta_config
from snapper.application.notify.push_beta import serialize_push_beta_config
from snapper.application.services.settings import get_settings_service
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.config.settings import get_settings
from snapper.data.models import Setting
from snapper.data.repository import get_repository
from snapper.data.repository import where_active_now
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.json_body import json_body
from snapper.server.json_body import openapi_schema

router = APIRouter(prefix="/settings", tags=["settings"])

_REST_STREAM = "rest.control"

_AI_INTEGRATION_FLAG_KEY = "ai_integration_enabled"
"""Key consulted by the MCP sub-app AND the public feature-flag endpoint."""


@router.get("/features")
async def get_public_feature_flags(
    request: Request,
) -> FeatureFlagsResponse:
    """Return the public feature-flag projection.

    The frontend reads this endpoint on mount to decide whether to
    render the ``/ai-integration`` navigation entry. No auth is
    required because the response only surfaces on/off state of
    feature gates that are already visible in the mount structure
    (``/api/mcp`` returns 503 when the same flag is off, regardless
    of credentials). Revealing the flag state to an unauthenticated
    caller is equivalent information to trying the disabled endpoint.

    Args:
        request: FastAPI request — used for the REST tracker that
            stamps provenance on the response envelope.

    Returns:
        :class:`FeatureFlagsResponse` with the current
        ``ai_integration_enabled`` public feature flag state.
    """
    settings_service = getattr(request.app.state, "settings_service", None)
    ai_integration_enabled = bool(
        settings_service.get_setting(_AI_INTEGRATION_FLAG_KEY, default=True)
        if settings_service is not None
        else True
    )
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = datetime.now(UTC)
    pid = str(uuid7())
    return FeatureFlagsResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=FeatureFlagsPayload(ai_integration_enabled=ai_integration_enabled),
    )


@router.get("")
async def get_all_settings(
    request: Request,
    user: Annotated[AuthPrincipal, Depends(require_permission(Permission.CONFIGURE_SYSTEM))],
    category: str | None = None,
    as_of: datetime | None = None,
) -> SettingListResponse:
    """Retrieve all application settings, optionally filtered by category.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        user: Authenticated user with CONFIGURE_SYSTEM permission.
        category: Optional category name to filter settings.
        as_of: Optional point-in-time query timestamp.

    Returns:
        SettingListResponse wrapping all settings matching the filter criteria.
    """
    settings = get_settings()
    repository = get_repository(settings.db_url)
    processing_date = as_of or datetime.now(UTC)
    rows = await repository.get_settings(as_of=processing_date, category=category)
    items = [
        SettingRead(
            public_id=r["public_id"],
            timestamp=r["timestamp"],
            session_id=r["session_id"],
            sequence_id=r["sequence_id"],
            key=r["key"],
            value=r["value"],
            category=r["category"],
            description=r["description"],
            updated_at=r["timestamp"],
            updated_by=r["updated_by"],
        )
        for r in rows
    ]
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = datetime.now(UTC)
    pid = str(uuid7())
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
    as_of: datetime | None = None,
) -> SettingCategoriesResponse:
    """Retrieve all distinct setting category names.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        user: Authenticated user with CONFIGURE_SYSTEM permission.
        as_of: Optional point-in-time query timestamp.

    Returns:
        Response containing sorted list of category names.
    """
    settings = get_settings()
    repository = get_repository(settings.db_url)
    processing_date = as_of or datetime.now(UTC)
    categories = await repository.get_setting_categories(as_of=processing_date)
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
        payload=categories,
        count=len(categories),
    )


@router.post(
    "/{key}/set",
    responses={404: {"description": "Setting not found"}},
    openapi_extra=openapi_schema(SettingUpdate),
)
async def set_setting(
    http_request: Request,
    key: str,
    user: Annotated[AuthPrincipal, Depends(require_permission(Permission.CONFIGURE_SYSTEM))],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    body: Annotated[SettingUpdate, Depends(json_body(SettingUpdate))],
) -> SettingResponse:
    """Set a setting value by key (update or create).

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
        settings.zmq_broker_xsub,
    )
    await settings_service.update_setting(
        key=key,
        value=body.payload.value,
        category=body.payload.category,
        description=body.payload.description,
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
            select(Setting).where(Setting.key == key, *where_active_now(Setting))
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


@router.get("/push-beta/users")
async def get_push_beta_users(
    request: Request,
    user: Annotated[AuthPrincipal, Depends(require_permission(Permission.CONFIGURE_SYSTEM))],
) -> PushBetaConfigResponse:
    """Return the active push-beta gate configuration.

    Admin-only (CONFIGURE_SYSTEM). Returns the decoded
    ``PushBetaConfig`` — when the underlying setting is absent or
    malformed, the default (gate disabled, empty allowlist) is
    surfaced so the admin can see exactly what the routing layer is
    enforcing.

    Args:
        request: FastAPI request (provides REST tracker).
        user: Authenticated admin caller.

    Returns:
        ``PushBetaConfigResponse`` with ``enabled`` + sorted
        ``user_public_ids``.
    """
    settings = get_settings()
    settings_service = await get_settings_service(
        settings.db_url,
        settings.zmq_broker_xsub,
    )
    raw = settings_service.get_setting(PUSH_BETA_SETTING_KEY)
    config = parse_push_beta_config(raw)
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = datetime.now(UTC)
    pid = str(uuid7())
    return PushBetaConfigResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=PushBetaConfigRead(
            session_id=sid,
            sequence_id=seq,
            public_id=pid,
            timestamp=ts,
            enabled=config.enabled,
            user_public_ids=sorted(set(config.user_public_ids)),
        ),
    )


@router.post(
    "/push-beta/users",
    openapi_extra=openapi_schema(UpdatePushBetaUsersCommand),
)
async def set_push_beta_users(
    request: Request,
    user: Annotated[AuthPrincipal, Depends(require_permission(Permission.CONFIGURE_SYSTEM))],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    body: Annotated[
        UpdatePushBetaUsersCommand,
        Depends(json_body(UpdatePushBetaUsersCommand)),
    ],
) -> PushBetaConfigResponse:
    """Replace the push-beta gate configuration in one call.

    The endpoint is REPLACEMENT-style (not merge): the body's
    ``user_public_ids`` becomes the complete allowlist after the
    write. Admins managing multi-step rollouts must read the current
    list (``GET``), edit locally, then ``POST`` the full intended set.

    The setting is stored as a JSON-encoded string under the
    ``push_beta_config`` key with category ``notifications``. The
    SCD2 close+insert in ``SettingsService.update_setting`` keeps
    history so the rollout timeline is auditable. The cached value
    propagates to the sidecar's routing layer on the next pub/sub
    refresh (settings publish on the bus).

    Args:
        request: FastAPI request (provides REST tracker).
        user: Authenticated admin caller — also stamps
            ``Setting.updated_by`` for audit.
        body: Typed request envelope with the replacement config.

    Returns:
        ``PushBetaConfigResponse`` echoing the now-active config.
    """
    payload = body.payload
    serialised = serialize_push_beta_config(
        PushBetaConfig(
            enabled=payload.enabled,
            user_public_ids=tuple(payload.user_public_ids),
        )
    )
    settings = get_settings()
    settings_service = await get_settings_service(
        settings.db_url,
        settings.zmq_broker_xsub,
    )
    await settings_service.update_setting(
        key=PUSH_BETA_SETTING_KEY,
        value=serialised,
        category="notifications",
        description="Push-beta rollout gate (allowlist + enabled flag).",
        updated_by=user.username,
    )
    config = parse_push_beta_config(serialised)
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = datetime.now(UTC)
    pid = str(uuid7())
    return PushBetaConfigResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=PushBetaConfigRead(
            session_id=sid,
            sequence_id=seq,
            public_id=pid,
            timestamp=ts,
            enabled=config.enabled,
            user_public_ids=sorted(set(config.user_public_ids)),
        ),
    )


@router.post(
    "/{key}/remove",
    responses={404: {"description": "Setting not found"}},
    openapi_extra=openapi_schema(RemoveSettingRequest),
)
async def remove_setting(
    request: Request,
    key: str,
    user: Annotated[AuthPrincipal, Depends(require_permission(Permission.CONFIGURE_SYSTEM))],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    _body: Annotated[RemoveSettingRequest, Depends(json_body(RemoveSettingRequest))],
) -> MessageResponse:
    """Remove a setting by key (soft-delete via bitemporal close).

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        key: The setting key to remove.
        _body: Request envelope with provenance (payload is empty).
        user: Authenticated user with CONFIGURE_SYSTEM permission.

    Returns:
        Success message confirming removal.

    Raises:
        HTTPException: If setting not found.
    """
    settings = get_settings()
    repository = get_repository(settings.db_url)
    async with repository.session() as session:
        result = await session.execute(
            select(Setting).where(Setting.key == key, *where_active_now(Setting))
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
        timestamp=now,
    )
