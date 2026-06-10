"""Per-(device, alert) routing with scoped preferences and default fallback.

Given an ``AlertEventRow`` (already persisted) and the caller's user +
device prefs + user-level defaults, decide which active devices
actually receive the push. Explicit preference precedence
(narrowest first), followed by the implicit default:

1. **Wallet scope** — pref where ``operator_public_id`` and
   ``wallet_public_id`` both match the alert's wallet triple.
2. **Operator scope** — pref where ``operator_public_id`` matches and
   ``wallet_public_id`` is NULL.
3. **Device-global scope** — pref with both scope columns NULL.
4. **User-level default** — ``UserAlertDefault`` for the user+alert_type.
5. **Default on for ``medium``+** — implicit safe fallback when no
   pref exists at any explicit layer.

Deny-wins at the narrowest matching depth (an ``enabled=False``
wallet pref beats an ``enabled=True`` operator pref). Safety-critical
alerts bypass quiet hours but still honour explicit disable + priority
thresholds — so users can opt out entirely by disabling the pref row,
we just refuse to silently shove them behind a quiet-hour wall.
"""

from datetime import datetime
from zoneinfo import ZoneInfo
from zoneinfo import ZoneInfoNotFoundError

from loguru import logger

from snapper.application.notify.push_beta import PushBetaConfig
from snapper.data.repository import Repository
from snapper.data.repository_types import AlertEventRow
from snapper.data.repository_types import DeviceAlertPrefRow
from snapper.data.repository_types import NotificationDeviceRow
from snapper.data.repository_types import UserAlertDefaultRow

_PRIORITY_RANK: dict[str, int] = {"low": 1, "medium": 2, "high": 3}

_DISABLED_PUSH_BETA = PushBetaConfig()
"""Default gate state when the sidecar has not loaded the setting
cache yet — open by default so a missing cache cannot silence the
entire push system on cold start.
"""


async def route_alert_to_devices(
    *,
    alert: AlertEventRow,
    repo: Repository,
    now: datetime,
    push_beta: PushBetaConfig | None = None,
) -> list[NotificationDeviceRow]:
    """Return the subset of the alert's user's active devices that receive it.

    Args:
        alert: Persisted ``AlertEventRow`` to route.
        repo: Repository handle for device + pref + user-default reads.
        now: Entry-boundary timestamp threaded from the sidecar.
        push_beta: Active push-beta gate config (rollout allowlist).
            ``None`` (the default — preserved for existing sidecar
            call sites that pre-date the gate) is treated as
            "gate disabled" so those callers behave exactly as
            before. The sidecar is expected to inject a freshly-read
            ``PushBetaConfig`` once per dispatch so the gate honours
            live admin updates.

    Returns:
        Active ``NotificationDeviceRow`` entries that pass the
        precedence cascade and the rollout gate. Empty list when
        the user has no active devices, every device was suppressed
        by prefs / defaults, or the rollout gate is enabled and the
        alert's user is NOT on the allowlist.
    """
    gate = push_beta or _DISABLED_PUSH_BETA
    if not gate.includes(alert["user_public_id"]):
        logger.info(
            "push_beta gate suppressed alert {pid} for user {uid}",
            pid=alert["public_id"],
            uid=alert["user_public_id"],
        )
        return []
    devices = await repo.list_active_notification_devices_for_user(alert["user_public_id"])
    if not devices:
        return []
    device_prefs = await repo.list_device_alert_prefs_for_user(alert["user_public_id"])
    user_defaults = await repo.list_user_alert_defaults(alert["user_public_id"])
    recipients: list[NotificationDeviceRow] = []
    for device in devices:
        if _policy_allows(
            device=device,
            alert=alert,
            device_prefs=device_prefs,
            user_defaults=user_defaults,
            now=now,
        ):
            recipients.append(device)
    return recipients


def _policy_allows(
    *,
    device: NotificationDeviceRow,
    alert: AlertEventRow,
    device_prefs: list[DeviceAlertPrefRow],
    user_defaults: list[UserAlertDefaultRow],
    now: datetime,
) -> bool:
    """Apply explicit preferences and the implicit fallback to one device."""
    matches = _narrowest_matches(device=device, alert=alert, prefs=device_prefs)
    if matches:
        narrowest = matches[0]
        if not narrowest["enabled"]:
            return False
        if _priority_rank(alert["priority"]) < _priority_rank(narrowest["min_priority"]):
            return False
        mute_until = narrowest.get("mute_until")
        if mute_until is not None and mute_until > now:
            return False
        return not (_in_quiet_hours(narrowest, now) and not alert["is_safety_critical"])
    user_default = next(
        (d for d in user_defaults if d["alert_type"] == alert["alert_type"]),
        None,
    )
    if user_default is not None:
        if not user_default["enabled"]:
            return False
        return _priority_rank(alert["priority"]) >= _priority_rank(user_default["min_priority"])
    return _priority_rank(alert["priority"]) >= _priority_rank("medium")


def _narrowest_matches(
    *,
    device: NotificationDeviceRow,
    alert: AlertEventRow,
    prefs: list[DeviceAlertPrefRow],
) -> list[DeviceAlertPrefRow]:
    """Return prefs matching (device, alert_type, scope) ordered narrowest-first.

    Wallet scope is only considered a match when the pref's
    ``(operator_public_id, wallet_public_id)`` pair equals the
    alert's pair AND the alert actually carries a wallet scope
    (i.e. ``alert.wallet_public_id is not None``). Operator scope
    follows similarly. The device-global (NULL, NULL) pref always
    matches — it's the per-device default when no narrower scope is
    configured.
    """
    device_id = device["public_id"]
    alert_type = alert["alert_type"]
    wallet_scope: list[DeviceAlertPrefRow] = []
    operator_scope: list[DeviceAlertPrefRow] = []
    device_scope: list[DeviceAlertPrefRow] = []
    for pref in prefs:
        if pref["device_public_id"] != device_id or pref["alert_type"] != alert_type:
            continue
        p_op = pref.get("operator_public_id")
        p_wal = pref.get("wallet_public_id")
        if (
            p_op is not None
            and p_wal is not None
            and alert.get("wallet_public_id") == p_wal
            and alert.get("operator_public_id") == p_op
        ):
            wallet_scope.append(pref)
        elif p_op is not None and p_wal is None and alert.get("operator_public_id") == p_op:
            operator_scope.append(pref)
        elif p_op is None and p_wal is None:
            device_scope.append(pref)
    return wallet_scope + operator_scope + device_scope


def _priority_rank(priority: str) -> int:
    """Map priority string to ordinal for comparison (unknown → 0, always blocked)."""
    return _PRIORITY_RANK.get(priority, 0)


def _in_quiet_hours(pref: DeviceAlertPrefRow, now: datetime) -> bool:
    """Return True when ``now`` falls inside the pref's quiet-hours window.

    The window is a minutes-of-day interval in the pref's ``timezone``
    — quiet hours are a user-facing local-time feature, so the
    comparison happens after converting ``now`` to that zone. The
    default zone on fresh preference rows is ``"UTC"``, which is a
    no-op conversion; users who configure ``"America/New_York"`` or
    similar get suppression tied to local clock minutes.

    ``quiet_hours_start_min`` / ``quiet_hours_end_min`` are both None
    when the feature is disabled. A wrap-around window (start > end)
    is interpreted as overnight, e.g. 22:00–07:00.
    """
    start = pref.get("quiet_hours_start_min")
    end = pref.get("quiet_hours_end_min")
    if start is None or end is None:
        return False
    if start == end:
        return False
    tz_name = pref.get("timezone") or "UTC"
    try:
        zone = ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        logger.warning(
            "routing: unknown device-alert pref timezone={tz} — falling back to UTC",
            tz=tz_name,
        )
        zone = ZoneInfo("UTC")
    local_now = now.astimezone(zone)
    current_min = local_now.hour * 60 + local_now.minute
    if start < end:
        return start <= current_min < end
    return current_min >= start or current_min < end
