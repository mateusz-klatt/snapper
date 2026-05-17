"""``critical_system_error`` rule — 3-consecutive WARNING heartbeat."""

from datetime import datetime
from datetime import timedelta

from loguru import logger

from snapper.application.notify.rules.base import AlertRule
from snapper.application.notify.rules.dedup import check_dedup_window
from snapper.core.json_types import JsonValue
from snapper.core.types import HealthStatusEnum
from snapper.data.repository import Repository
from snapper.data.repository_types import AlertEventInsertRow
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.schemas.messages import MessageParseError
from snapper.messaging.schemas.messages import parse_message

_CONSECUTIVE_WARNING_THRESHOLD = 3
_ROLLING_WINDOW_SECONDS = 600
_PERMISSION_FOR_FAN_OUT = "read:system_status"


class CriticalSystemErrorRule(AlertRule):
    """Fan-out system-degradation alert to every admin with READ_SYSTEM_STATUS.

    Heartbeat publishers emit ``HEALTHY | WARNING`` only —
    ``ERROR`` lives at the REST layer and never hits the bus. A
    single WARNING is noisy (transient spikes), so the rule gates on
    3 consecutive WARNING heartbeats inside a 10-minute rolling
    window per ``(component, name)`` pair. The rolling window is
    **in-memory on the rule instance** — restart resets state so the
    first 3 ticks after a restart will re-trigger if degradation is
    ongoing, which is the intentional trade-off (minor alert spam on
    deploy cycles vs the complexity of persisted window state).

    Deduplication is hour-bucketed: ``dedup_key`` =
    ``f"sys_error.{component}.{name}.{hour_bucket}"`` so at most one
    alert fires per ``(component, name)`` per hour even if the
    3-warning gate keeps firing.

    Fan-out: one ``AlertEventInsertRow`` per active user whose role
    grants ``Permission.READ_SYSTEM_STATUS`` (per
    ``list_users_with_permission``).
    """

    alert_type = "critical_system_error"
    subscribe_topic_prefixes = ("system.heartbeats.",)
    priority = "high"
    is_safety_critical = False
    thread_key_prefix = "snapper.system"
    suppression_window_seconds = 3600

    def __init__(self) -> None:
        """Start with an empty per-``(component, name)`` rolling window."""
        self._rolling_window: dict[tuple[str, str], list[tuple[datetime, str]]] = {}

    async def evaluate(
        self,
        topic: str,
        payload: bytes,
        repo: Repository,
        now: datetime,
    ) -> list[AlertEventInsertRow]:
        """Return per-admin rows when the 3-consecutive-WARNING gate opens.

        Args:
            topic: ZMQ topic string —
                ``system.heartbeats.{component}.{name}``.
            payload: ``HeartbeatData`` JSON bytes.
            repo: Repository handle for admin fan-out + dedup lookups.
            now: Entry-boundary timestamp threaded from the sidecar.

        Returns:
            One ``AlertEventInsertRow`` per admin user with
            ``READ_SYSTEM_STATUS`` when the rolling window shows 3
            consecutive non-HEALTHY heartbeats. Empty list on HEALTHY
            (window reset), under-threshold counts, topic / payload
            malformation, empty admin set, or hour-bucket dedup hits.
        """
        parts = topic.split(".")
        if len(parts) != 4 or parts[0] != "system" or parts[1] != "heartbeats":
            return []
        component = parts[2]
        name = parts[3]
        try:
            data = parse_message(payload.decode("utf-8"))
        except (UnicodeDecodeError, MessageParseError):
            return []
        if not isinstance(data, HeartbeatData):
            return []
        state_key = (component, name)
        if data.status == HealthStatusEnum.HEALTHY:
            self._rolling_window.pop(state_key, None)
            return []
        window = self._rolling_window.setdefault(state_key, [])
        cutoff = now - timedelta(seconds=_ROLLING_WINDOW_SECONDS)
        window[:] = [(t, s) for t, s in window if t >= cutoff]
        window.append((now, data.status))
        if len(window) < _CONSECUTIVE_WARNING_THRESHOLD:
            return []
        recent = window[-_CONSECUTIVE_WARNING_THRESHOLD:]
        if not all(s != HealthStatusEnum.HEALTHY for _, s in recent):
            return []
        hour_bucket = now.replace(minute=0, second=0, microsecond=0).isoformat()
        dedup_key = f"sys_error.{component}.{name}.{hour_bucket}"
        admin_user_ids = await repo.list_users_with_permission(_PERMISSION_FOR_FAN_OUT)
        if not admin_user_ids:
            logger.info(
                "critical_system_error: no users hold READ_SYSTEM_STATUS — dropping alert"
                " (component={comp}, name={nm})",
                comp=component,
                nm=name,
            )
            return []
        rows: list[AlertEventInsertRow] = []
        for admin_user_id in admin_user_ids:
            if await check_dedup_window(
                repo=repo,
                user_public_id=admin_user_id,
                dedup_key=dedup_key,
                window_seconds=self.suppression_window_seconds,
                now=now,
            ):
                continue
            body_args: list[JsonValue] = [
                component,
                name,
                data.status,
                _CONSECUTIVE_WARNING_THRESHOLD,
            ]
            rows.append(
                AlertEventInsertRow(
                    user_public_id=admin_user_id,
                    operator_public_id=None,
                    wallet_public_id=None,
                    alert_type=self.alert_type,
                    priority=self.priority,
                    is_safety_critical=self.is_safety_critical,
                    title=f"System degraded: {component}",
                    body=(
                        f"{body_args[0]}/{body_args[1]} reported {body_args[2]} for"
                        f" {body_args[3]} consecutive heartbeats"
                    ),
                    payload={
                        "deep_link_path": "/system",
                        "component": component,
                        "name": name,
                        "status": data.status,
                        "body_suppressed": False,
                        "title_loc_key": "alerts.title.critical_system_error",
                        "title_loc_args": [component],
                        "body_loc_key": "alerts.body.critical_system_error",
                        "body_loc_args": body_args,
                    },
                    dedup_key=dedup_key,
                    thread_key=f"{self.thread_key_prefix}.{component}.{name}",
                    source_topic=topic,
                )
            )
        return rows
