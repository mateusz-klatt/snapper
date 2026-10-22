"""Bound Android outbox delivery alongside the unchanged APNs transport."""

from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from uuid import uuid7

from snapper.application.notify.fcm_client import FcmClient
from snapper.application.notify.fcm_client import FcmHint
from snapper.application.notify.fcm_client import FcmSendResult
from snapper.application.notify.push_beta import PushBetaConfig
from snapper.application.notify.routing import allows_device_alert
from snapper.application.notify.scope_revalidation import ScopeRevalidator
from snapper.data.android_devices import AndroidDeliveryResult
from snapper.data.android_devices import RegistrationWrite
from snapper.data.repository import AndroidConfigurationStore
from snapper.data.repository import AndroidDeviceStore
from snapper.data.repository import Repository
from snapper.data.repository_types import AlertDeliveryInsertRow
from snapper.data.repository_types import AlertDeliveryRow
from snapper.data.repository_types import AlertEventRow
from snapper.messaging.infrastructure.publisher import SequenceTracker


class FcmDeliveryService:
    """Never retarget a persisted hint onto a replacement account, FID or project."""

    def __init__(
        self, repo: Repository, sender: FcmClient, tracker: SequenceTracker, scope: ScopeRevalidator
    ) -> None:
        """Share the sidecar's repository, provenance and authority revocation cache."""
        self.repo = repo
        self.sender = sender
        self.tracker = tracker
        self.scope = scope
        self.push_beta_provider: Callable[[], PushBetaConfig] | None = None
        self.clock: Callable[[], datetime] = lambda: datetime.now(UTC)

    def _write(self, now: datetime) -> RegistrationWrite:
        """Allocate one provenance sequence per durable transition."""
        return RegistrationWrite(
            "", self.tracker.session_id, self.tracker.next_sequence("sidecar.notify.fcm"), now
        )

    async def fanout(
        self, event: AlertEventRow, now: datetime, beta: PushBetaConfig | None
    ) -> None:
        """Allocate immutable 24-hour hints after the existing preference/rollout cascade.

        Args:
            event: Persisted alert with its captured user and authority scope.
            now: UTC allocation boundary for the immutable hint expiry.
            beta: Current rollout policy, or None when no gate is supplied.
        """
        if beta is not None and not beta.includes(event["user_public_id"]):
            return
        if not await AndroidConfigurationStore(self.repo).matches(self.sender.configuration):
            return
        devices = await AndroidDeviceStore(self.repo).list_owned(event["user_public_id"])
        prefs = await self.repo.list_device_alert_prefs_for_user(event["user_public_id"])
        defaults = await self.repo.list_user_alert_defaults(event["user_public_id"])
        for device in devices:
            if device.configuration_id != self.sender.configuration.configuration_id:
                continue
            if not allows_device_alert(device.public_id, event, prefs, defaults, now):
                continue
            write = self._write(now)
            public_id = await self.repo.insert_alert_delivery(
                AlertDeliveryInsertRow(
                    public_id=str(uuid7()),
                    session_id=write.session_id,
                    sequence_id=write.sequence_id,
                    timestamp=now,
                    alert_event_public_id=event["public_id"],
                    device_public_id=device.public_id,
                    user_public_id=event["user_public_id"],
                    operator_public_id=event.get("operator_public_id"),
                    wallet_public_id=event.get("wallet_public_id"),
                    status="queued",
                    attempt_count=0,
                    created_at=now,
                    provider="fcm",
                    configuration_id=device.configuration_id,
                    binding_id=device.binding_id,
                    expires_at=now + timedelta(hours=24),
                )
            )
            row = await self.repo.get_delivery_by_public_id(public_id)
            if row is not None:
                await self.attempt(row, now)

    async def attempt(self, row: AlertDeliveryRow, now: datetime) -> None:
        """Claim a durable attempt, then repeat binding/scope checks after OAuth acquisition.

        Args:
            row: Queued outbox snapshot retaining its original binding and expiry.
            now: UTC boundary for checking expiry and charging the attempt.
        """
        if row["attempt_count"] >= 3:
            await self._failed(row, "retry_exhausted_or_expired", now)
            return
        expiry = row.get("expires_at")
        if expiry is None or expiry <= now:
            await self._failed(row, "expired", now)
            return
        event = await self.repo.get_alert_event_by_public_id(row["alert_event_public_id"])
        if event is None or not await self._admitted(row, event):
            await self._cancelled(row, now)
            return
        write = self._write(now)
        count = await self.repo.begin_delivery_attempt(
            row["public_id"],
            transition_at=now,
            session_id=write.session_id,
            sequence_id=write.sequence_id,
        )
        if count is None:
            return
        row = row.copy()
        row["attempt_count"] = count
        if count > 3:
            await self._failed(row, "retry_exhausted_or_expired", now)
            return
        target = await AndroidDeviceStore(self.repo).delivery_target(row)
        if target is None or target.firebase_installation_id is None:
            await self._cancelled(row, now)
            return
        hint = FcmHint(
            row["device_public_id"],
            row.get("binding_id") or "",
            row["alert_event_public_id"],
            expiry,
        )

        async def admitted() -> bool:
            """Reject a late OAuth completion after retirement or a terminal transition."""
            current = await self.repo.get_delivery_by_public_id(row["public_id"])
            fresh_event = await self.repo.get_alert_event_by_public_id(row["alert_event_public_id"])
            return (
                current is not None
                and current["status"] == "queued"
                and current["attempt_count"] == count
                and fresh_event is not None
                and await self._admitted(current, fresh_event)
            )

        result = await self.sender.send(target.firebase_installation_id, hint, admitted)
        await self._apply(row, count, result, self.clock())

    async def _admitted(self, row: AlertDeliveryRow, event: AlertEventRow) -> bool:
        """Use current server authority and exact persisted identities before each send."""
        if not self._scope_matches(row, event):
            return False
        if row.get("configuration_id") != self.sender.configuration.configuration_id:
            return False
        if not await AndroidConfigurationStore(self.repo).matches(self.sender.configuration):
            return False
        if await AndroidDeviceStore(self.repo).delivery_target(row) is None:
            return False
        if not await self._policy_admitted(row, event):
            return False
        authoritative = event.copy()
        authoritative["is_safety_critical"] = True
        return not await self.scope.should_skip_send(authoritative, self.repo, self.clock())

    @staticmethod
    def _scope_matches(row: AlertDeliveryRow, event: AlertEventRow) -> bool:
        """Never let corrected alert history retarget an existing queued scope."""
        return (
            row["user_public_id"] == event["user_public_id"]
            and row.get("operator_public_id") == event.get("operator_public_id")
            and row.get("wallet_public_id") == event.get("wallet_public_id")
        )

    async def _policy_admitted(self, row: AlertDeliveryRow, event: AlertEventRow) -> bool:
        """Re-read quiet hours, priorities and current rollout before every network send."""
        if self.push_beta_provider is not None:
            if not self.push_beta_provider().includes(row["user_public_id"]):
                return False
        prefs = await self.repo.list_device_alert_prefs_for_user(row["user_public_id"])
        defaults = await self.repo.list_user_alert_defaults(row["user_public_id"])
        return allows_device_alert(row["device_public_id"], event, prefs, defaults, self.clock())

    async def _apply(
        self, row: AlertDeliveryRow, count: int, result: FcmSendResult, now: datetime
    ) -> None:
        """Map provider outcomes without changing APNs result interpretation."""
        write = self._write(now)
        if result.outcome == "accepted" and result.receipt is not None:
            await self.repo.finish_android_delivery(
                row["public_id"],
                AndroidDeliveryResult("sent", count, receipt=result.receipt),
                write,
            )
        elif result.outcome == "unregistered":
            committed = await self.repo.finish_android_delivery(
                row["public_id"],
                AndroidDeliveryResult("unregistered", count, reason="unregistered"),
                write,
            )
            if committed:
                await self.repo.deactivate_notification_device_scd2(
                    row["device_public_id"],
                    reason="unregistered",
                    timestamp=now,
                    session_id=write.session_id,
                    sequence_id=write.sequence_id,
                )
        elif result.outcome == "cancelled":
            await self._cancelled(row, now)
        elif result.outcome == "retry":
            await self._retry(row, count, result, now)
        else:
            await self._failed(row, result.reason, now)

    async def _retry(
        self, row: AlertDeliveryRow, count: int, result: FcmSendResult, now: datetime
    ) -> None:
        """Honor provider Retry-After without extending the original hint lifetime."""
        next_at = max(
            now + timedelta(seconds=min(300, 30 * 2 ** min(count, 4))), result.retry_at or now
        )
        expiry = row.get("expires_at")
        if count >= 3 or expiry is None or next_at >= expiry:
            await self._failed(row, "retry_exhausted_or_expired", now)
            return
        write = self._write(now)
        await self.repo.update_delivery_retry_schedule(
            row["public_id"],
            attempt_count=count,
            next_attempt_at=next_at,
            error_reason=result.reason,
            transition_at=now,
            session_id=write.session_id,
            sequence_id=write.sequence_id,
        )

    async def _failed(self, row: AlertDeliveryRow, reason: str, now: datetime) -> None:
        """Record a sanitized terminal reason only if this attempt is still current."""
        await self.repo.finish_android_delivery(
            row["public_id"],
            AndroidDeliveryResult("failed", row["attempt_count"], reason=reason),
            self._write(now),
        )

    async def _cancelled(self, row: AlertDeliveryRow, now: datetime) -> None:
        """An obsolete admission result cannot cancel another worker's newer attempt."""
        await self.repo.finish_android_delivery(
            row["public_id"],
            AndroidDeliveryResult(
                "cancelled_scope", row["attempt_count"], reason="binding_or_scope_changed"
            ),
            self._write(now),
        )
