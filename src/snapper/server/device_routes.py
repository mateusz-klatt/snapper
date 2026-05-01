"""REST routes for iOS Push Foundation device management (BE-1c).

Four endpoints, all gated by the authenticated principal and
ownership-checked on ``user_public_id``:

- ``POST /api/devices`` — register / refresh APNs device token.
- ``GET /api/devices`` — list caller's own active devices.
- ``DELETE /api/devices/{public_id}`` — soft-delete via SCD2 close.
- ``PATCH /api/devices/{public_id}/prefs`` — per-(device, alert_type,
  scope) preference upsert.
- ``GET /api/devices/{public_id}/prefs`` — list active per-(alert_type,
  scope) preferences attached to the device.

All mutations use ``Repository.upsert_notification_device`` /
``upsert_device_alert_pref`` / ``deactivate_notification_device_scd2``,
which own the SCD2 close+insert semantics and idempotent retry on
same-key contention (see ``src/snapper/data/repository.py`` for the
atomic close + IntegrityError-retry pattern).
"""

import datetime as dt
from datetime import datetime
from typing import Annotated
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi import status

from snapper.api.schemas.base import MessageResponse
from snapper.api.schemas.devices import DeviceAlertPrefInfo
from snapper.api.schemas.devices import DeviceAlertPrefListResponse
from snapper.api.schemas.devices import DeviceAlertPrefResponse
from snapper.api.schemas.devices import NotificationDeviceInfo
from snapper.api.schemas.devices import NotificationDeviceListResponse
from snapper.api.schemas.devices import NotificationDeviceResponse
from snapper.api.schemas.devices import RegisterDeviceCommand
from snapper.api.schemas.devices import UpdateDevicePrefCommand
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import Repository
from snapper.data.repository_types import DeviceAlertPrefRow
from snapper.data.repository_types import DeviceAlertPrefUpsertRow
from snapper.data.repository_types import NotificationDeviceRow
from snapper.data.repository_types import NotificationDeviceUpsertRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency

router = APIRouter(prefix="/devices", tags=["devices"])

_REST_STREAM = "rest.devices"


def _next_provenance(request: Request) -> tuple[str, int, datetime, str]:
    """Mint a fresh ``(session_id, sequence_id, timestamp, public_id)`` tuple.

    Args:
        request: FastAPI request — used to pull the
            ``rest_tracker`` off app state.

    Returns:
        Tuple used by every SCD2 write from this router. Keeping the
        helper local avoids scattering ``app.state.rest_tracker``
        access across handlers.
    """
    tracker: SequenceTracker = request.app.state.rest_tracker
    sid = tracker.session_id
    seq = tracker.next_sequence(_REST_STREAM)
    ts = dt.datetime.now(dt.UTC)
    pid = str(uuid7())
    return sid, seq, ts, pid


def _device_alert_pref_info_from_row(row: DeviceAlertPrefRow) -> DeviceAlertPrefInfo:
    """Project a ``DeviceAlertPrefRow`` TypedDict into the wire schema.

    Drops SCD2-internal fields (``known_to``) so callers see only
    the active projection. Scope columns flow through as-is —
    ``None`` means the row applies device-globally for the
    ``alert_type``.
    """
    return DeviceAlertPrefInfo(
        session_id=row["session_id"],
        sequence_id=row["sequence_id"],
        public_id=row["public_id"],
        timestamp=row["timestamp"],
        device_public_id=row["device_public_id"],
        alert_type=row["alert_type"],
        operator_public_id=row["operator_public_id"],
        wallet_public_id=row["wallet_public_id"],
        enabled=row["enabled"],
        min_priority=row["min_priority"],
        quiet_hours_start_min=row["quiet_hours_start_min"],
        quiet_hours_end_min=row["quiet_hours_end_min"],
        mute_until=row["mute_until"],
        timezone=row["timezone"],
    )


def _device_info_from_row(row: NotificationDeviceRow) -> NotificationDeviceInfo:
    """Project a ``NotificationDeviceRow`` TypedDict into the wire schema."""
    return NotificationDeviceInfo(
        session_id=row["session_id"],
        sequence_id=row["sequence_id"],
        public_id=row["public_id"],
        timestamp=row["timestamp"],
        user_public_id=row["user_public_id"],
        device_token=row["device_token"],
        device_id=row["device_id"],
        platform=row["platform"],
        env=row["env"],
        app_version=row.get("app_version"),
        previews_mode=row["previews_mode"],
        registered_at=row["registered_at"],
        last_seen_at=row.get("last_seen_at"),
    )


@router.post("")
async def register_device(
    request: Request,
    command: RegisterDeviceCommand,
    principal: Annotated[AuthPrincipal, Depends(require_authentication)],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> NotificationDeviceResponse:
    """Register or refresh the caller's APNs device token.

    Same-token re-registrations collapse onto the existing SCD2 row
    via ``upsert_notification_device`` and return the stable
    ``public_id`` reused across versions.

    Args:
        request: FastAPI request (provides REST tracker).
        command: Typed request envelope with provenance + body.
        principal: Authenticated caller.
        repo: Repository dependency.

    Returns:
        ``NotificationDeviceResponse`` carrying the active row after
        upsert.
    """
    body = command.payload
    sid, seq, ts, _ = _next_provenance(request)
    device_public_id = await repo.upsert_notification_device(
        NotificationDeviceUpsertRow(
            session_id=sid,
            sequence_id=seq,
            timestamp=ts,
            user_public_id=principal.user_public_id,
            device_token=body.device_token,
            device_id=body.device_id,
            env=body.env,
            app_version=body.app_version,
            previews_mode=body.previews_mode,
            registered_at=ts,
        )
    )
    envelope_sid, envelope_seq, envelope_ts, envelope_pid = _next_provenance(request)
    return NotificationDeviceResponse(
        session_id=envelope_sid,
        sequence_id=envelope_seq,
        public_id=envelope_pid,
        timestamp=envelope_ts,
        payload=NotificationDeviceInfo(
            session_id=sid,
            sequence_id=seq,
            public_id=device_public_id,
            timestamp=ts,
            user_public_id=principal.user_public_id,
            device_token=body.device_token,
            device_id=body.device_id,
            platform="ios",
            env=body.env,
            app_version=body.app_version,
            previews_mode=body.previews_mode,
            registered_at=ts,
            last_seen_at=None,
        ),
    )


@router.get("")
async def list_devices(
    request: Request,
    principal: Annotated[AuthPrincipal, Depends(require_authentication)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> NotificationDeviceListResponse:
    """List the caller's active devices, newest registered first.

    Args:
        request: FastAPI request (provides REST tracker).
        principal: Authenticated caller.
        repo: Repository dependency.

    Returns:
        ``NotificationDeviceListResponse`` with zero-or-more active
        ``NotificationDeviceInfo`` rows scoped to the caller.
    """
    rows = await repo.list_active_notification_devices_for_user(principal.user_public_id)
    items = [_device_info_from_row(r) for r in rows]
    sid, seq, ts, pid = _next_provenance(request)
    return NotificationDeviceListResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=items,
        count=len(items),
    )


@router.delete("/{device_public_id}")
async def delete_device(
    request: Request,
    device_public_id: str,
    principal: Annotated[AuthPrincipal, Depends(require_authentication)],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> MessageResponse:
    """Soft-delete a device via SCD2 close + tombstone successor.

    Ownership is enforced by loading the caller's active devices and
    confirming the target ``public_id`` is among them. Marking a
    nonexistent or not-owned device inactive returns 404 so callers
    cannot probe foreign device ids. The tombstone successor row
    carries ``token_status = 'user_unregistered'`` so an ``as_of``
    query after the close sees an inactive row instead of a gap.

    Args:
        request: FastAPI request (provides REST tracker).
        device_public_id: Target device to soft-delete.
        principal: Authenticated caller.
        repo: Repository dependency.

    Returns:
        ``MessageResponse`` on success.

    Raises:
        HTTPException: 404 when the device does not belong to the
            caller or is already inactive.
    """
    owned = await repo.list_active_notification_devices_for_user(principal.user_public_id)
    if not any(d["public_id"] == device_public_id for d in owned):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Device {device_public_id} not found or not owned by caller.",
        )
    sid, seq, ts, _ = _next_provenance(request)
    await repo.deactivate_notification_device_scd2(
        device_public_id,
        reason="user_unregistered",
        timestamp=ts,
        session_id=sid,
        sequence_id=seq,
    )
    envelope_sid, envelope_seq, envelope_ts, envelope_pid = _next_provenance(request)
    return MessageResponse(
        session_id=envelope_sid,
        sequence_id=envelope_seq,
        public_id=envelope_pid,
        timestamp=envelope_ts,
        payload=f"Device {device_public_id} deactivated.",
    )


@router.patch("/{device_public_id}/prefs")
async def update_device_pref(
    request: Request,
    device_public_id: str,
    command: UpdateDevicePrefCommand,
    principal: Annotated[AuthPrincipal, Depends(require_authentication)],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> DeviceAlertPrefResponse:
    """Upsert a per-(device, alert_type, scope) preference for the caller.

    The composite scope key is
    ``(device_public_id, alert_type, operator_public_id, wallet_public_id)``
    — three null-permutations map to the three partial unique indexes
    on ``device_alert_prefs`` and the upsert-retry machinery in the
    repo collapses same-key races.

    Args:
        request: FastAPI request (provides REST tracker).
        device_public_id: Target device (must be owned by caller).
        command: Typed request envelope with provenance + pref body.
        principal: Authenticated caller.
        repo: Repository dependency.

    Returns:
        ``DeviceAlertPrefResponse`` with the now-active pref row.

    Raises:
        HTTPException: 404 when the device is not owned by caller.
    """
    owned = await repo.list_active_notification_devices_for_user(principal.user_public_id)
    if not any(d["public_id"] == device_public_id for d in owned):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Device {device_public_id} not found or not owned by caller.",
        )
    body = command.payload
    sid, seq, ts, _ = _next_provenance(request)
    pref_public_id = await repo.upsert_device_alert_pref(
        DeviceAlertPrefUpsertRow(
            session_id=sid,
            sequence_id=seq,
            timestamp=ts,
            device_public_id=device_public_id,
            alert_type=body.alert_type,
            operator_public_id=body.operator_public_id,
            wallet_public_id=body.wallet_public_id,
            enabled=body.enabled,
            min_priority=body.min_priority,
            quiet_hours_start_min=body.quiet_hours_start_min,
            quiet_hours_end_min=body.quiet_hours_end_min,
            mute_until=body.mute_until,
            timezone=body.timezone,
        )
    )
    envelope_sid, envelope_seq, envelope_ts, envelope_pid = _next_provenance(request)
    return DeviceAlertPrefResponse(
        session_id=envelope_sid,
        sequence_id=envelope_seq,
        public_id=envelope_pid,
        timestamp=envelope_ts,
        payload=DeviceAlertPrefInfo(
            session_id=sid,
            sequence_id=seq,
            public_id=pref_public_id,
            timestamp=ts,
            device_public_id=device_public_id,
            alert_type=body.alert_type,
            operator_public_id=body.operator_public_id,
            wallet_public_id=body.wallet_public_id,
            enabled=body.enabled,
            min_priority=body.min_priority,
            quiet_hours_start_min=body.quiet_hours_start_min,
            quiet_hours_end_min=body.quiet_hours_end_min,
            mute_until=body.mute_until,
            timezone=body.timezone,
        ),
    )


@router.get("/{device_public_id}/prefs")
async def list_device_prefs(
    request: Request,
    device_public_id: str,
    principal: Annotated[AuthPrincipal, Depends(require_authentication)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> DeviceAlertPrefListResponse:
    """List active per-(alert_type, scope) prefs for a caller-owned device.

    Reads ``Repository.list_device_alert_prefs_for_user`` (which
    server-side joins to ``notification_devices`` and filters by
    ``token_status = 'active'`` so tombstoned device prefs do not
    leak), then narrows the projection to ``device_public_id`` so the
    iOS notifications surface only sees prefs for the addressed
    device. Ownership is enforced separately by checking that the
    target device is among the caller's active devices — a foreign
    or unknown ``public_id`` returns 404 to prevent existence probing.

    Args:
        request: FastAPI request (provides REST tracker).
        device_public_id: Target device (must be owned by caller).
        principal: Authenticated caller.
        repo: Repository dependency.

    Returns:
        ``DeviceAlertPrefListResponse`` with zero-or-more active
        ``DeviceAlertPrefInfo`` rows tied to the device.

    Raises:
        HTTPException: 404 when the device is not owned by caller.
    """
    owned = await repo.list_active_notification_devices_for_user(principal.user_public_id)
    if not any(d["public_id"] == device_public_id for d in owned):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Device {device_public_id} not found or not owned by caller.",
        )
    rows = await repo.list_device_alert_prefs_for_user(principal.user_public_id)
    items = [
        _device_alert_pref_info_from_row(r)
        for r in rows
        if r["device_public_id"] == device_public_id
    ]
    sid, seq, ts, pid = _next_provenance(request)
    return DeviceAlertPrefListResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=items,
        count=len(items),
    )
