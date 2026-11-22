"""``critical_system_error`` rule — 3-consecutive WARNING heartbeat."""

from datetime import datetime
from datetime import timedelta

from loguru import logger

from snapper.application.notify.rules.base import AlertRule
from snapper.application.notify.rules.dedup import check_dedup_since
from snapper.core.json_types import JsonValue
from snapper.core.types import HealthStatusEnum
from snapper.data.repository import Repository
from snapper.data.repository_types import AlertEventInsertRow
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.schemas.messages import MessageParseError
from snapper.messaging.schemas.messages import parse_message

_CONSECUTIVE_WARNING_THRESHOLD = 3
_ROLLING_WINDOW_SECONDS = 600

_EPISODE_REMINDER_SECONDS = 86400
"""Minimum spacing between pages for one still-degraded component instance.

A degradation episode pages once when it opens and, while it persists, at
most once per this interval. The in-memory gate is cheap; the per-user
``alert_events`` lookup with the stable episode key enforces the same bound
across a sidecar restart that wipes the in-memory state."""

_EPISODE_CLOSE_HEALTHY_STREAK = 3
"""Consecutive HEALTHY heartbeats required before a paged episode closes.

Mirrors the three-WARNING open gate, so one HEALTHY blip in a flapping
component does not end the episode."""

_REOPEN_FLOOR_SECONDS = 3600
"""Minimum spacing between pages for one instance even across a recovery.

A closed episode may page again sooner than the 24 h reminder, but never
within an hour of the previous page, so oscillation cannot page more often
than the former hourly cap."""
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

    Deduplication is per degradation episode: ``dedup_key`` =
    ``f"sys_error.{component}.{name}"``. A chronic WARNING pages once and
    then at most once per ``_EPISODE_REMINDER_SECONDS`` (24 h), instead of
    every hour. Three consecutive HEALTHY heartbeats after a page close the
    episode in memory, so a later degradation pages again, though never
    within an hour of the previous page; after a restart, with that memory
    gone, the 24 h bound from ``alert_events`` applies.

    Fan-out: one ``AlertEventInsertRow`` per active user whose role
    grants ``Permission.READ_SYSTEM_STATUS`` (per
    ``list_users_with_permission``).
    """

    alert_type = "critical_system_error"
    subscribe_topic_prefixes = ("system.heartbeats.",)
    priority = "high"
    is_safety_critical = False
    thread_key_prefix = "snapper.system"
    suppression_window_seconds = _EPISODE_REMINDER_SECONDS

    def __init__(self) -> None:
        """Start with an empty per-``(component, name)`` rolling window."""
        self._rolling_window: dict[tuple[str, str], list[tuple[datetime, str]]] = {}
        self._last_fired: dict[tuple[str, str], datetime] = {}
        self._episode_closed_at: dict[tuple[str, str], datetime] = {}
        self._healthy_streak: dict[tuple[str, str], int] = {}

    def _close_episode(self, state_key: tuple[str, str], now: datetime) -> None:
        """Reset the rolling window and close a paged episode on HEALTHY.

        The episode closes only after ``_EPISODE_CLOSE_HEALTHY_STREAK``
        consecutive HEALTHY heartbeats following a page.

        Args:
            state_key: ``(component, name)`` identity of the instance.
            now: Timestamp of the HEALTHY heartbeat.
        """
        self._rolling_window.pop(state_key, None)
        streak = self._healthy_streak.get(state_key, 0) + 1
        self._healthy_streak[state_key] = streak
        last_fired = self._last_fired.get(state_key)
        if last_fired is None or streak < _EPISODE_CLOSE_HEALTHY_STREAK:
            return
        closed_at = self._episode_closed_at.get(state_key)
        if closed_at is None or closed_at < last_fired:
            self._episode_closed_at[state_key] = now

    def _episode_since(self, state_key: tuple[str, str], now: datetime) -> datetime:
        """Return the lower bound for the per-user episode dedup lookup.

        Args:
            state_key: ``(component, name)`` identity of the instance.
            now: Timestamp of the heartbeat that opened the gate.

        Returns:
            ``now - 24 h`` while the episode is open; after a close, the later
            of that and ``min(closed_at, now - 1 h)``, so a recovery reopens
            paging but never within the hourly floor.
        """
        reminder_since = now - timedelta(seconds=_EPISODE_REMINDER_SECONDS)
        if not self._episode_closed(state_key):
            return reminder_since
        closed_at = self._episode_closed_at[state_key]
        floor_since = now - timedelta(seconds=_REOPEN_FLOOR_SECONDS)
        return max(reminder_since, min(closed_at, floor_since))

    def _episode_closed(self, state_key: tuple[str, str]) -> bool:
        """Return whether the instance's last page was followed by a close.

        Args:
            state_key: ``(component, name)`` identity of the instance.

        Returns:
            True when an in-memory close is newer than the last page.
        """
        last_fired = self._last_fired.get(state_key)
        closed_at = self._episode_closed_at.get(state_key)
        return last_fired is not None and closed_at is not None and closed_at > last_fired

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

            Both 4-segment (``system.heartbeats.{component}.{name}``)
            and 5-segment per-wallet executor topics
            (``system.heartbeats.executor.{exchange}.{wallet_short}``)
            are accepted; the per-wallet name joins to
            ``{exchange}.{wallet_short}`` so windows, dedup buckets, and
            alert args stay per-instance. The old 4-only parse silently
            discarded EVERY per-wallet executor heartbeat — non-HEALTHY
            executor frames could never alert.
        """
        parts = topic.split(".")
        if len(parts) not in (4, 5) or parts[0] != "system" or parts[1] != "heartbeats":
            return []
        component = parts[2]
        name = ".".join(parts[3:])
        try:
            data = parse_message(payload.decode("utf-8"))
        except (UnicodeDecodeError, MessageParseError):
            return []
        if not isinstance(data, HeartbeatData):
            return []
        state_key = (component, name)
        if data.status == HealthStatusEnum.HEALTHY:
            self._close_episode(state_key, now)
            return []
        self._healthy_streak.pop(state_key, None)
        window = self._rolling_window.setdefault(state_key, [])
        cutoff = now - timedelta(seconds=_ROLLING_WINDOW_SECONDS)
        window[:] = [(t, s) for t, s in window if t >= cutoff]
        window.append((now, data.status))
        if len(window) < _CONSECUTIVE_WARNING_THRESHOLD:
            return []
        recent = window[-_CONSECUTIVE_WARNING_THRESHOLD:]
        if not all(s != HealthStatusEnum.HEALTHY for _, s in recent):
            return []
        last_fired = self._last_fired.get(state_key)
        spacing = (
            _REOPEN_FLOOR_SECONDS if self._episode_closed(state_key) else _EPISODE_REMINDER_SECONDS
        )
        if last_fired is not None and (now - last_fired) < timedelta(seconds=spacing):
            return []
        dedup_key = f"sys_error.{component}.{name}"
        since = self._episode_since(state_key, now)
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
            if await check_dedup_since(
                repo=repo,
                user_public_id=admin_user_id,
                dedup_key=dedup_key,
                since=since,
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
        if rows:
            self._last_fired[state_key] = now
        return rows
