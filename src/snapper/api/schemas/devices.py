"""Pydantic schemas for iOS Push Foundation device-management routes.

Covers the four device endpoints: register/upsert, list caller's
active devices, soft-delete by public_id, and per-(device, alert_type)
preference updates. The five SCD2 tables behind these routes live in
``src/snapper/data/models.py``; the schemas here are the wire contract
exposed to iOS / TS clients and mirror the ``NotificationDeviceRow``
/ ``DeviceAlertPrefRow`` / ``UserAlertDefaultRow`` TypedDict projections
without leaking SCD2 plumbing (``known_to``) beyond provenance
(``session_id`` / ``sequence_id`` / ``timestamp``) that every Snapper
REST/WS payload carries.

Per the bitemporal invariant (every state table is SCD2-versioned)
every row lifecycle is SCD2 close-and-insert and the ``known_to == MAX``
active predicate is applied server-side; clients see only the currently
active projection through these schemas.
"""

from datetime import datetime
from typing import Literal

from pydantic import Field

from snapper.api.schemas.base import PayloadListResponse
from snapper.api.schemas.base import PayloadRequest
from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictBody
from snapper.api.schemas.base import StrictDataSchema


class RegisterDeviceBody(StrictBody):
    """Request body for ``POST /api/devices`` — register or refresh a device.

    A same-``device_token`` re-register collapses onto the existing
    SCD2 row via ``Repository.upsert_notification_device`` (close+insert
    atop the stable ``public_id``) so repeated app launches are
    idempotent. ``device_token`` is the 64-hex APNs token minted by
    iOS ``application:didRegisterForRemoteNotificationsWithDeviceToken:``
    — the backend does not mint it.

    Attributes:
        device_token: 64-hex APNs device token (lowercase hex).
        device_id: Stable device identifier from ``identifierForVendor``
            (UUID-like, 36 chars). Used for diagnostics; independent
            of ``device_token`` which can rotate.
        env: APNs environment the token was issued for
            (``sandbox`` for TestFlight / debug builds; ``prod`` for
            App Store builds). Determines which ``ApnsClientPool`` pool
            the sidecar uses.
        app_version: Optional app version string for server-side
            feature gating (e.g. retro-fixing schema mismatches).
        previews_mode: iOS lock-screen payload visibility
            (``private`` hides the body, ``public`` shows it). Server
            copies this into every APNs payload's
            ``aps.mutable-content`` / ``apns-priority`` logic.
    """

    device_token: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    device_id: str = Field(min_length=1, max_length=64)
    env: Literal["sandbox", "prod"]
    app_version: str | None = Field(default=None, max_length=32)
    previews_mode: Literal["private", "public"] = "private"


class RegisterDeviceCommand(PayloadRequest[Literal["register_device_command"], RegisterDeviceBody]):
    """Request envelope for ``POST /api/devices``."""

    type: Literal["register_device_command"] = "register_device_command"


class NotificationDeviceInfo(StrictDataSchema[Literal["notification_device_info"]]):
    """Read projection of a single active ``notification_devices`` SCD2 row.

    Attributes:
        type: Payload item type discriminator.
        user_public_id: Owner user UUID7 — the caller's own id on the
            list endpoint, scoped server-side.
        device_token: 64-hex APNs token (current active).
        device_id: Stable device identifier (see
            ``RegisterDeviceBody.device_id``).
        platform: Always ``ios`` for the current mobile API.
        env: APNs environment this token was issued for.
        app_version: Optional app version string.
        previews_mode: iOS lock-screen visibility
            (``private`` / ``public``).
        registered_at: When the token was first registered.
        last_seen_at: Most recent heartbeat (``None`` = never).
    """

    type: Literal["notification_device_info"] = "notification_device_info"
    user_public_id: str
    device_token: str
    device_id: str
    platform: str
    env: str
    app_version: str | None = None
    previews_mode: str
    registered_at: datetime
    last_seen_at: datetime | None = None


class NotificationDeviceResponse(
    PayloadResponse[Literal["notification_device_response"], NotificationDeviceInfo]
):
    """Singleton wrapper returned by ``POST /api/devices``."""

    type: Literal["notification_device_response"] = "notification_device_response"


class NotificationDeviceListResponse(
    PayloadListResponse[Literal["notification_device_list_response"], NotificationDeviceInfo]
):
    """List wrapper for ``GET /api/devices``."""

    type: Literal["notification_device_list_response"] = "notification_device_list_response"


class DeviceAlertPrefBody(StrictBody):
    """Per-(device, alert_type, scope) preference upsert body.

    Scope narrows a preference to either an operator or a specific
    wallet — when both ``operator_public_id`` and ``wallet_public_id``
    are None the pref applies at device-global scope for that
    alert_type. The composite ``(device, alert_type, operator, wallet)``
    tuple is enforced by three partial-unique indexes, so setting the
    same tuple twice collapses via SCD2 close+insert at the repo.

    Attributes:
        alert_type: One of the enumerated alert types
            (``order_fill_full``, ``order_rejected``,
            ``position_stop_loss_fired``, ``margin_warning``,
            ``critical_system_error``).
        operator_public_id: Optional operator scope.
        wallet_public_id: Optional wallet scope.
        enabled: Whether to deliver this alert_type at this scope.
        min_priority: Minimum priority required to deliver
            (``low``, ``medium``, ``high``).
        quiet_hours_start_min: Local-TZ quiet-hours start (minutes
            since midnight, 0-1439). ``None`` disables quiet hours.
        quiet_hours_end_min: Local-TZ quiet-hours end (minutes since
            midnight, 0-1439). ``None`` disables quiet hours.
        mute_until: Hard-mute until this UTC instant; ``None`` = no
            mute. Overrides quiet-hours when set.
        timezone: IANA TZ name for quiet-hours interpretation
            (default ``UTC``).
    """

    alert_type: Literal[
        "order_fill_full",
        "order_rejected",
        "position_stop_loss_fired",
        "margin_warning",
        "critical_system_error",
    ]
    operator_public_id: str | None = None
    wallet_public_id: str | None = None
    enabled: bool = True
    min_priority: Literal["low", "medium", "high"] = "medium"
    quiet_hours_start_min: int | None = Field(default=None, ge=0, le=1439)
    quiet_hours_end_min: int | None = Field(default=None, ge=0, le=1439)
    mute_until: datetime | None = None
    timezone: str = Field(default="UTC", max_length=64)


class UpdateDevicePrefCommand(
    PayloadRequest[Literal["update_device_pref_command"], DeviceAlertPrefBody]
):
    """Request envelope for ``PATCH /api/devices/{public_id}/prefs``."""

    type: Literal["update_device_pref_command"] = "update_device_pref_command"


class DeviceAlertPrefInfo(StrictDataSchema[Literal["device_alert_pref_info"]]):
    """Read projection of a single active ``device_alert_prefs`` SCD2 row."""

    type: Literal["device_alert_pref_info"] = "device_alert_pref_info"
    device_public_id: str
    alert_type: str
    operator_public_id: str | None = None
    wallet_public_id: str | None = None
    enabled: bool
    min_priority: str
    quiet_hours_start_min: int | None = None
    quiet_hours_end_min: int | None = None
    mute_until: datetime | None = None
    timezone: str


class DeviceAlertPrefResponse(
    PayloadResponse[Literal["device_alert_pref_response"], DeviceAlertPrefInfo]
):
    """Singleton wrapper returned by ``PATCH /api/devices/{public_id}/prefs``."""

    type: Literal["device_alert_pref_response"] = "device_alert_pref_response"


class DeviceAlertPrefListResponse(
    PayloadListResponse[Literal["device_alert_pref_list_response"], DeviceAlertPrefInfo]
):
    """List wrapper for ``GET /api/devices/{public_id}/prefs``.

    Returns every active per-(alert_type, scope) preference row tied
    to the addressed device. Wallet/operator-narrowed rows surface
    alongside device-global rows; the iOS UI is responsible for
    grouping by ``alert_type`` and rendering the scope picker.
    """

    type: Literal["device_alert_pref_list_response"] = "device_alert_pref_list_response"


class RevokeDevicePrefBody(StrictBody):
    """Request body for ``POST /api/devices/{public_id}/prefs/{pref_public_id}/revoke``.

    Mirrors ``RevokeScopeGrantBody``: the SCD2 close happens in place
    and no successor row carries the audit reason, so ``reason`` is
    a free-form note kept on the close transition log only. The route
    plumbs it straight to ``deactivate_device_alert_pref_scd2``'s
    ``reason`` argument and returns the closed projection so the iOS
    UI can drop the row from its local list without an extra GET.

    Attributes:
        reason: Optional audit note (e.g. ``"removed via Settings"``).
    """

    reason: str | None = Field(default=None, max_length=512)


class RevokeDevicePrefCommand(
    PayloadRequest[Literal["revoke_device_pref_command"], RevokeDevicePrefBody]
):
    """Request envelope for the per-(device, pref) revoke route.

    Backend convention is POST + envelope (not DELETE) so every
    write carries client-side provenance for the gap detector — same
    pattern as ``CancelOrderCommand`` / ``RevokeScopeGrantCommand``.
    The no-DELETE/use-POST convention keeps every revoke / cancel
    route consistent on this provenance-carrying write pattern.
    """

    type: Literal["revoke_device_pref_command"] = "revoke_device_pref_command"


class RevokeDevicePrefResponse(
    PayloadResponse[Literal["revoke_device_pref_response"], DeviceAlertPrefInfo]
):
    """Singleton wrapper returned by the device-pref revoke route.

    Payload is the pref row as it exists immediately after SCD2
    close — ``known_to`` is stamped at the revoke timestamp and the
    row is no longer surfaced by ``list_active_device_alert_prefs``,
    so the iOS UI uses the response purely as a confirmation echo
    (the row's content is unchanged from the pre-revoke active
    projection beyond the ``known_to`` stamp).
    """

    type: Literal["revoke_device_pref_response"] = "revoke_device_pref_response"


class UserAlertDefaultBody(StrictBody):
    """Per-(user, alert_type) fallback preference upsert body.

    Consulted by the alert-routing layer when no device-scoped
    override exists for the (alert_type, scope) tuple — the
    ``application/notify/routing`` rules pick the narrowest scope
    first (wallet → operator → device → user-default → built-in).

    Attributes:
        alert_type: One of the enumerated alert types
            (``order_fill_full``, ``order_rejected``,
            ``position_stop_loss_fired``, ``margin_warning``,
            ``critical_system_error``).
        enabled: Whether to deliver this alert_type at all when no
            device override matches.
        min_priority: Minimum priority required to deliver
            (``low``, ``medium``, ``high``).
    """

    alert_type: Literal[
        "order_fill_full",
        "order_rejected",
        "position_stop_loss_fired",
        "margin_warning",
        "critical_system_error",
    ]
    enabled: bool = True
    min_priority: Literal["low", "medium", "high"] = "medium"


class UpdateUserAlertDefaultCommand(
    PayloadRequest[Literal["update_user_alert_default_command"], UserAlertDefaultBody]
):
    """Request envelope for ``PATCH /api/alert_defaults``."""

    type: Literal["update_user_alert_default_command"] = "update_user_alert_default_command"


class UserAlertDefaultInfo(StrictDataSchema[Literal["user_alert_default_info"]]):
    """Read projection of a single active ``user_alert_defaults`` SCD2 row.

    Attributes:
        type: Payload item type discriminator.
        user_public_id: Owner — the caller's own UUID7 on these
            routes (scoped server-side).
        alert_type: One of the five enumerated alert types.
        enabled: Whether this fallback default delivers the alert.
        min_priority: Lower-bound priority filter applied when this
            fallback fires.
    """

    type: Literal["user_alert_default_info"] = "user_alert_default_info"
    user_public_id: str
    alert_type: str
    enabled: bool
    min_priority: str


class UserAlertDefaultResponse(
    PayloadResponse[Literal["user_alert_default_response"], UserAlertDefaultInfo]
):
    """Singleton wrapper returned by ``PATCH /api/alert_defaults``."""

    type: Literal["user_alert_default_response"] = "user_alert_default_response"


class UserAlertDefaultListResponse(
    PayloadListResponse[Literal["user_alert_default_list_response"], UserAlertDefaultInfo]
):
    """List wrapper for ``GET /api/alert_defaults``.

    Surfaces every active per-(user, alert_type) fallback row for the
    caller. Empty list is the legitimate "no overrides yet" state —
    the alert-routing layer falls through to the built-in defaults
    documented in ``application/notify/routing.py``.
    """

    type: Literal["user_alert_default_list_response"] = "user_alert_default_list_response"
