"""``NotifySidecar`` — ZMQ ``alerts.`` consumer with APNs fanout and outbox retry.

The sidecar owns three co-routines: the ZMQ receive loop, the startup
outbox drain, and a background retry loop that wakes every 30s and
picks up queued deliveries whose ``next_attempt_at`` has arrived.

Flow per received ``alerts.`` message:

1. Parse bus payload via ``parse_message`` -> ``AlertEventData``.
2. Cross-check the topic's ``user_public_id`` segment vs. the
   payload's ``user_public_id`` — mismatch is a producer bug and
   drops the message with a warning (never sends to a token owned
   by a different user).
3. ``insert_alert_event`` (SCD2 row for history +
   ``GET /api/alerts/history`` reads).
4. Fan out: for each active device owned by the target user, insert
   one ``alert_deliveries`` row with ``status='queued'`` and
   denormalised scope columns (avoids the scope-join race against
   SCD2 corrections on ``alert_events``).
5. Immediately attempt to send each queued row (happy path). Any
   non-terminal response (``throttled`` / ``server_error``) leaves
   the row ``queued`` and schedules a retry via ``next_attempt_at``.

Retry semantics (attempt-number-before-attempt rule): every
attempt bumps ``attempt_count`` *before* the APNs call via
``update_delivery_retry_schedule`` — so a crash between the DB write
and the APNs call leaves ``attempt_count=N`` and the retry loop will
pick it up again as the Nth attempt. Terminal outcomes use the
``mark_delivery_*`` close-and-insert methods (SCD2 atomic transition
under concurrent retry workers).
"""

import asyncio
import contextlib
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from uuid import uuid7

from loguru import logger

from snapper.application.notify.apns_client import ApnsClientPool
from snapper.application.notify.apns_client import ApnsSendResult
from snapper.application.notify.push_beta import PushBetaConfig
from snapper.application.notify.routing import route_alert_to_devices
from snapper.application.notify.rules.base import RuleRegistry
from snapper.application.notify.rules.registry_factory import load_default_registry
from snapper.application.notify.scope_revalidation import ScopeRevalidator
from snapper.application.process_manager.models import RegisterableProcess
from snapper.core.json_types import JsonObject
from snapper.data.repository import Repository
from snapper.data.repository_types import AlertDeliveryInsertRow
from snapper.data.repository_types import AlertDeliveryRow
from snapper.data.repository_types import AlertEventInsertRow
from snapper.data.repository_types import AlertEventRow
from snapper.data.repository_types import NotificationDeviceRow
from snapper.i18n import catalog
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.schemas.data import AlertEventData

_RETRY_LOOP_INTERVAL_S = 30.0
_RETRY_GIVE_UP_AFTER_ATTEMPTS = 3
_BACKOFF_BASE_S = 30.0
_BACKOFF_CAP_S = 300.0
_THROTTLED_RETRY_DEFAULT_S = 60.0
_ZMQ_STREAM = "sidecar.notify"


def _backoff_seconds(attempt_number: int) -> float:
    """Compute the exponential backoff for the N-th attempt.

    Schedule: ``min(30s * 2^(N-1), 5min)``.
    N=1 -> 30s, N=2 -> 60s, N=3 -> 120s; capped at 300s.

    Args:
        attempt_number: 1-based attempt number that just returned a
            non-terminal error (retryable server_error / throttled).

    Returns:
        Backoff duration in seconds (float).
    """
    exp = max(attempt_number - 1, 0)
    scaled: float = _BACKOFF_BASE_S * (2**exp)
    return min(scaled, _BACKOFF_CAP_S)


class NotifySidecar(RegisterableProcess):
    """ZMQ ``alerts.`` consumer + APNs fanout + outbox retry loop.

    Wires ``ValidatedSubscriber`` (bus receive), ``Repository`` (SCD2
    outbox) and ``ApnsClientPool`` (HTTP/2 sends) into a single
    long-running process. Instantiable from the
    ``snapper notify`` CLI subcommand.

    Attributes:
        _subscriber: Validated ZMQ SUB socket subscribed to
            ``alerts.`` prefix.
        _repo: Repository for SCD2 reads/writes on the five iOS-push
            tables.
        _apns: APNs client pool (sandbox + prod, keyed by device env).
        _apns_topic: APNs topic header — the app bundle id.
        _tracker: Sequence tracker for provenance on sidecar-minted
            rows.
        _stop_event: Set by ``stop()`` to tear down the main loop.
        _retry_task: Background retry loop (started in ``start``).
    """

    def __init__(
        self,
        subscriber: ValidatedSubscriber,
        repo: Repository,
        apns: ApnsClientPool,
        apns_topic: str,
        tracker: SequenceTracker,
        publisher: MessagePublisher,
        registry: RuleRegistry | None = None,
        scope_revalidator: ScopeRevalidator | None = None,
        push_beta_provider: Callable[[], PushBetaConfig] | None = None,
    ) -> None:
        """Wire the sidecar with its collaborators + rule registry.

        Args:
            subscriber: Pre-configured validated ZMQ SUB socket.
            repo: Repository instance.
            apns: Ready ``ApnsClientPool``.
            apns_topic: APNs topic header (= bundle id, e.g.
                ``ie.klatt.snapper``). Propagated to every
                ``ApnsClientPool.send`` call — isolating the sidecar
                from the config-loader at send time.
            tracker: ``SequenceTracker`` used to stamp provenance on
                every ``alert_events`` / ``alert_deliveries`` row
                the sidecar writes.
            publisher: ``MessagePublisher`` used to fan out one
                ``AlertEventData`` frame per persisted alert onto the
                ZMQ bus topic ``alerts.{user_public_id}.{alert_type}``.
                Powers the web live-refresh path: the bridge
                forwards the frame to subscribed WebSocket clients
                with per-user scope enforcement (mirrors REST
                ``/api/alerts/history`` scoping). Publish happens
                BEFORE APNs fanout so web clients see updates in
                <50ms; APNs round-trip is the slower path.
            registry: Alert rule registry — defaults to
                ``load_default_registry()`` (the core rule set
                + ``margin_warning``). Injected for tests that want a
                narrower rule set.
            scope_revalidator: Scope-revocation helper — defaults to
                a fresh ``ScopeRevalidator`` seeded with the shared
                ``SequenceTracker`` so provenance on cancel writes is
                consistent with the rest of the sidecar's SCD2 writes.
            push_beta_provider: Callable returning the current
                ``PushBetaConfig`` rollout gate. ``None`` (the
                default) is treated as "gate disabled" so legacy /
                test call sites that pre-date the gate behave as
                before. The CLI wire-up resolves the callable to a
                ``SettingsService.get_setting`` read so the gate
                honours live admin POSTs to
                ``/api/settings/push-beta/users``.
        """
        self._subscriber = subscriber
        self._repo = repo
        self._apns = apns
        self._apns_topic = apns_topic
        self._tracker = tracker
        self._publisher = publisher
        self._stop_event = asyncio.Event()
        self._retry_task: asyncio.Task[None] | None = None
        self._registry = registry or load_default_registry()
        self._scope_revalidator = scope_revalidator or ScopeRevalidator(tracker=tracker)
        self._push_beta_provider = push_beta_provider

    async def start(self) -> None:
        """Run the sidecar main loop until ``stop()`` is signalled.

        Subscribes to every prefix the rule registry aggregates
        (``orders.events.`` + ``plans.decisions.`` + ``system.heartbeats.``
        for the default set), drains the outbox (crash recovery),
        spawns the background retry loop, and consumes the receive loop
        until ``_stop_event`` is set. Each entry boundary mints one
        ``now`` timestamp (single timestamp per entry boundary)
        which is threaded through every helper and repository call.
        """
        for prefix in self._registry.all_subscribe_prefixes():
            self._subscriber.subscribe(prefix)
        self._subscriber.subscribe("admin.scope_revoked")
        self._subscriber.subscribe("admin.user_deactivated")
        await self._drain_outbox(datetime.now(UTC))
        self._retry_task = asyncio.create_task(self._process_retry_queue_loop())
        try:
            while not self._stop_event.is_set():
                recv_task = asyncio.create_task(self._subscriber.recv_multipart())
                stop_task = asyncio.create_task(self._stop_event.wait())
                done, pending = await asyncio.wait(
                    {recv_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()
                if recv_task not in done:
                    break
                topic, payload = recv_task.result()
                await self._dispatch(topic, payload, datetime.now(UTC))
        finally:
            if self._retry_task is not None and not self._retry_task.done():
                self._retry_task.cancel()

    async def stop(self) -> None:
        """Signal the main loop + retry loop to exit on the next tick."""
        self._stop_event.set()

    async def _dispatch(self, topic: str, payload: bytes, now: datetime) -> None:
        """Route one received bus frame through the rule registry.

        Matches ``topic`` against the registry's longest-prefix
        dispatch, runs every matching rule's ``evaluate``, and persists
        + fans out each ``AlertEventInsertRow`` the rules produce.
        Rule exceptions are caught per-rule so a misbehaving rule
        does not poison the whole receive loop; the sidecar keeps
        consuming.

        Args:
            topic: ZMQ topic string the frame arrived on.
            payload: Raw JSON payload bytes.
            now: Entry-boundary timestamp (one ``now`` per received
                frame, reused for every SCD2 write performed while
                handling it).
        """
        if topic == "admin.scope_revoked":
            try:
                await self._scope_revalidator.handle_scope_revoked(payload, self._repo, now)
            except Exception as exc:
                logger.warning(
                    "sidecar: admin.scope_revoked handler raised: {err}",
                    err=exc,
                )
            return
        if topic == "admin.user_deactivated":
            try:
                await self._scope_revalidator.handle_user_deactivated(payload, self._repo, now)
            except Exception as exc:
                logger.warning(
                    "sidecar: admin.user_deactivated handler raised: {err}",
                    err=exc,
                )
            return
        matching = self._registry.get_longest_match(topic)
        if not matching:
            return
        for rule in matching:
            try:
                alert_rows = await rule.evaluate(topic, payload, self._repo, now)
            except Exception as exc:
                logger.warning(
                    "sidecar: rule {rule} raised on topic={topic}: {err}",
                    rule=type(rule).__name__,
                    topic=topic,
                    err=exc,
                )
                continue
            for alert_row in alert_rows:
                await self._persist_and_fanout_row(alert_row, now)

    async def _persist_and_fanout_row(
        self,
        alert_row: AlertEventInsertRow,
        now: datetime,
    ) -> None:
        """Insert the alert row + fan out via policy routing.

        Args:
            alert_row: Row minted by an ``AlertRule.evaluate`` call.
            now: Entry-boundary timestamp (threaded from ``_dispatch``).
        """
        event_public_id = await self._insert_alert_event(alert_row, now)
        event = await self._repo.get_alert_event_by_public_id(event_public_id)
        if event is None:
            logger.warning(
                "sidecar: freshly inserted alert_event {pid} missing on read-back —"
                " skipping fanout",
                pid=event_public_id,
            )
            return
        await self._publish_alert_event_frame(event, now)
        push_beta = self._push_beta_provider() if self._push_beta_provider is not None else None
        recipients = await route_alert_to_devices(
            alert=event,
            repo=self._repo,
            now=now,
            push_beta=push_beta,
        )
        if not recipients:
            logger.info(
                "sidecar: routing cascade suppressed all devices for alert_event {pid}"
                " (user={user}, alert_type={at})",
                pid=event_public_id,
                user=alert_row.get("user_public_id"),
                at=alert_row.get("alert_type"),
            )
            return
        recipient_user_pid = event["user_public_id"]
        languages = await self._repo.get_default_languages_for_users([recipient_user_pid])
        user_language = languages.get(recipient_user_pid)
        for device in recipients:
            delivery_pid = await self._insert_delivery_for_event(
                event=event, device=device, now=now
            )
            await self._attempt_once(delivery_pid, device, event, now, user_language=user_language)

    async def _publish_alert_event_frame(self, event: AlertEventRow, now: datetime) -> None:
        """Emit an ``AlertEventData`` frame onto the ZMQ bus.

        Powers the web live-refresh path. The bridge subscribes
        to the ``alerts.`` prefix and forwards the frame to authenticated
        WebSocket clients with per-user scope enforcement. Sequence is
        allocated against the actual topic so consumer-side gap detection
        is per-topic — matching the publisher convention used elsewhere
        in the codebase (``executors/base.py``).

        Args:
            event: Active SCD2 row read back after ``_insert_alert_event``;
                the publish frame mirrors its provenance, scope, and
                resolved title/body.
            now: Entry-boundary timestamp (threaded from
                ``_persist_and_fanout_row``).
        """
        topic = f"alerts.{event['user_public_id']}.{event['alert_type']}"
        tracker = self._publisher.tracker
        seq = tracker.next_sequence(topic)
        frame = AlertEventData.model_validate(
            {
                "type": "alert_event",
                "sequence_id": seq,
                "public_id": event["public_id"],
                "timestamp": now,
                "session_id": tracker.session_id,
                "user_public_id": event["user_public_id"],
                "operator_public_id": event["operator_public_id"],
                "wallet_public_id": event["wallet_public_id"],
                "alert_type": event["alert_type"],
                "priority": event["priority"],
                "is_safety_critical": event["is_safety_critical"],
                "title": event["title"],
                "body": event["body"],
                "payload": event["payload"],
                "dedup_key": event["dedup_key"],
                "thread_key": event["thread_key"],
                "source_topic": event["source_topic"],
            }
        )
        await self._publisher.send(topic, frame)

    async def _insert_alert_event(self, alert_row: AlertEventInsertRow, now: datetime) -> str:
        """Write the SCD2 ``alert_events`` row and return its public_id."""
        sid = self._tracker.session_id
        seq = self._tracker.next_sequence(_ZMQ_STREAM)
        enriched = AlertEventInsertRow(
            public_id=alert_row.get("public_id") or str(uuid7()),
            session_id=sid,
            sequence_id=seq,
            timestamp=now,
            user_public_id=alert_row["user_public_id"],
            operator_public_id=alert_row.get("operator_public_id"),
            wallet_public_id=alert_row.get("wallet_public_id"),
            alert_type=alert_row["alert_type"],
            priority=alert_row["priority"],
            is_safety_critical=alert_row.get("is_safety_critical", False),
            title=alert_row["title"],
            body=alert_row["body"],
            payload=alert_row.get("payload"),
            dedup_key=alert_row.get("dedup_key"),
            thread_key=alert_row.get("thread_key"),
            source_topic=alert_row.get("source_topic"),
        )
        return await self._repo.insert_alert_event(enriched)

    async def _insert_delivery_for_event(
        self,
        *,
        event: AlertEventRow,
        device: NotificationDeviceRow,
        now: datetime,
    ) -> str:
        """Insert one queued ``alert_deliveries`` row with denormalised scope."""
        sid = self._tracker.session_id
        seq = self._tracker.next_sequence(_ZMQ_STREAM)
        return await self._repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                public_id=str(uuid7()),
                session_id=sid,
                sequence_id=seq,
                timestamp=now,
                alert_event_public_id=event["public_id"],
                device_public_id=device["public_id"],
                user_public_id=event["user_public_id"],
                operator_public_id=event.get("operator_public_id"),
                wallet_public_id=event.get("wallet_public_id"),
                status="queued",
                attempt_count=0,
                last_attempt_at=None,
                next_attempt_at=None,
                apns_id=None,
                error_reason=None,
                created_at=now,
            )
        )

    async def _attempt_once(
        self,
        delivery_public_id: str,
        device: NotificationDeviceRow,
        event: AlertEventRow,
        now: datetime,
        *,
        user_language: str | None = None,
    ) -> None:
        """Run exactly one APNs send attempt on a queued delivery row.

        Before the APNs call, ``ScopeRevalidator.should_skip_send``
        decides whether the event's scope is still active (safety-
        critical always re-checks; non-critical uses the TTL-gated
        stale-scope cache). Scope-stale deliveries transition to
        ``cancelled_scope`` in the outbox and this attempt ends
        without an APNs round-trip.

        Otherwise, bumps ``attempt_count`` first (attempt-number-before-
        attempt rule), builds the APNs payload, calls through the
        pool, and maps the result to the SCD2 terminal / retry schedule.

        ``user_language`` is the recipient's ``User.default_language``
        preference (or ``None`` when never set). When non-null AND the
        event payload carries ``title_loc_key``/``body_loc_key``, the
        APNs ``aps.alert.title``/``body`` fields are resolved through
        the backend catalog so the push notification renders in the
        user's chosen language. Falls back to the EN ``event.title``/
        ``event.body`` otherwise.
        """
        if await self._scope_revalidator.should_skip_send(event, self._repo, now):
            sid = self._tracker.session_id
            seq = self._tracker.next_sequence(_ZMQ_STREAM)
            await self._repo.mark_delivery_cancelled(
                delivery_public_id,
                reason="scope_revoked",
                transition_at=now,
                session_id=sid,
                sequence_id=seq,
            )
            logger.info(
                "sidecar: scope revoked mid-send — cancelled delivery={pid}"
                " user={user} operator={op} wallet={wal}",
                pid=delivery_public_id,
                user=event["user_public_id"],
                op=event.get("operator_public_id"),
                wal=event.get("wallet_public_id"),
            )
            return
        current_attempt = await self._bump_attempt(delivery_public_id, now)
        if current_attempt is None:
            logger.info(
                "sidecar: delivery {pid} no longer queued at bump time —"
                " concurrent cancel / terminal transition wins",
                pid=delivery_public_id,
            )
            return
        payload = _build_apns_payload(event, user_language=user_language)
        try:
            result = await self._apns.send(
                env=device["env"],
                device_token=device["device_token"],
                payload=payload,
                apns_topic=self._apns_topic,
                priority=_priority_to_apns(event["priority"]),
                push_type="alert",
                collapse_id=event.get("thread_key"),
            )
        except Exception as exc:
            logger.warning(
                "sidecar: APNs send raised — scheduling retry"
                " delivery={pid}, attempt={n}, err={err}",
                pid=delivery_public_id,
                n=current_attempt,
                err=exc,
            )
            await self._schedule_retry_or_fail(
                delivery_public_id, current_attempt, f"exception: {exc}", now
            )
            return
        await self._apply_result(delivery_public_id, current_attempt, result, device, now)

    async def _bump_attempt(self, delivery_public_id: str, now: datetime) -> int | None:
        """Increment ``attempt_count`` via SCD2 close+insert.

        Reads the active row to learn the current attempt_count, then
        delegates to ``update_delivery_retry_schedule`` which emits a
        successor SCD2 row with the incremented counter. The
        new attempt number is returned when the transition succeeded;
        ``None`` when the row is no longer queued (a concurrent admin
        handler cancelled it between our ``should_skip_send`` check
        and this call — race guard). Callers treating ``None`` as
        "abort this attempt" keep the sidecar from emitting a send
        for a row that was just cancelled.
        """
        sid = self._tracker.session_id
        seq = self._tracker.next_sequence(_ZMQ_STREAM)
        row = await self._load_delivery(delivery_public_id)
        next_attempt = (row["attempt_count"] if row is not None else 0) + 1
        transitioned = await self._repo.update_delivery_retry_schedule(
            delivery_public_id,
            attempt_count=next_attempt,
            next_attempt_at=None,
            error_reason=None,
            transition_at=now,
            session_id=sid,
            sequence_id=seq,
        )
        if not transitioned:
            return None
        return next_attempt

    async def _apply_result(
        self,
        delivery_public_id: str,
        attempt_number: int,
        result: ApnsSendResult,
        device: NotificationDeviceRow,
        now: datetime,
    ) -> None:
        """Route an APNs result to the correct SCD2 terminal / retry transition."""
        sid = self._tracker.session_id
        seq = self._tracker.next_sequence(_ZMQ_STREAM)
        if result.status == "success":
            await self._repo.mark_delivery_sent(
                delivery_public_id,
                apns_id=result.apns_id,
                transition_at=now,
                session_id=sid,
                sequence_id=seq,
            )
            return
        if result.status == "unregistered":
            await self._repo.mark_delivery_unregistered(
                delivery_public_id,
                transition_at=now,
                session_id=sid,
                sequence_id=seq,
            )
            device_sid = self._tracker.session_id
            device_seq = self._tracker.next_sequence(_ZMQ_STREAM)
            await self._repo.deactivate_notification_device_scd2(
                device["public_id"],
                reason="unregistered",
                timestamp=now,
                session_id=device_sid,
                sequence_id=device_seq,
            )
            return
        await self._schedule_retry_or_fail(
            delivery_public_id,
            attempt_number,
            result.description or result.status,
            now,
        )

    async def _schedule_retry_or_fail(
        self,
        delivery_public_id: str,
        attempt_number: int,
        error_reason: str,
        now: datetime,
    ) -> None:
        """Either give up (attempt >= 3) or schedule the next retry.

        Throttled responses use a conservative 60s delay (aioapns 4.0
        does not expose Retry-After); all other server/network errors
        use the exponential backoff. Retry exhaustion
        emits a ``logger.warning`` before the terminal ``failed``
        transition so ops alerting has a single log record to pivot
        on.
        """
        sid = self._tracker.session_id
        seq = self._tracker.next_sequence(_ZMQ_STREAM)
        if attempt_number >= _RETRY_GIVE_UP_AFTER_ATTEMPTS:
            logger.warning(
                "sidecar: alert delivery failed after retries"
                " — delivery_public_id={pid}, attempts={n}, error_reason={err}",
                pid=delivery_public_id,
                n=attempt_number,
                err=error_reason,
            )
            await self._repo.mark_delivery_failed(
                delivery_public_id,
                error_reason=error_reason,
                transition_at=now,
                session_id=sid,
                sequence_id=seq,
            )
            return
        backoff_s = _backoff_seconds(attempt_number)
        next_attempt_at = now + timedelta(seconds=backoff_s)
        await self._repo.update_delivery_retry_schedule(
            delivery_public_id,
            attempt_count=attempt_number,
            next_attempt_at=next_attempt_at,
            error_reason=error_reason,
            transition_at=now,
            session_id=sid,
            sequence_id=seq,
        )

    async def _load_delivery(self, delivery_public_id: str) -> AlertDeliveryRow | None:
        """Load an active delivery row by public_id, None when closed / unknown.

        Indexed lookup via
        :meth:`~snapper.data.repository.SQLAlchemyRepository.get_delivery_by_public_id`
        — replaces the legacy linear scan over
        :meth:`list_queued_deliveries_all` that paid O(n_queued) per
        retry attempt on the sidecar hot path.
        """
        return await self._repo.get_delivery_by_public_id(delivery_public_id)

    async def _drain_outbox(self, now: datetime) -> None:
        """Process every ``status='queued'`` row on startup (crash recovery).

        Ordered ``last_attempt_at ASC NULLS FIRST`` (never-
        attempted rows first, then longest-waiting retries). Each
        row gets one fresh attempt; a second-or-later failure stays
        inside the retry loop's normal scheduling.

        Bulk-prefetches the alert_event + device-list for every row in
        the batch with two ``IN (...)`` SELECTs — replaces the per-row
        N+1 fetches that previously dominated drain/retry latency at
        large backlogs.
        """
        queued = await self._repo.list_queued_deliveries_all()
        queued.sort(
            key=lambda r: (
                r.get("last_attempt_at") or datetime(1970, 1, 1, tzinfo=UTC),
                r["created_at"],
            )
        )
        events_cache, devices_cache, languages_cache = await self._prefetch_caches_for_rows(queued)
        for row in queued:
            await self._attempt_on_queued_row(
                row,
                now,
                events_cache=events_cache,
                devices_cache=devices_cache,
                languages_cache=languages_cache,
            )

    async def _prefetch_caches_for_rows(self, rows: list[AlertDeliveryRow]) -> tuple[
        dict[str, AlertEventRow],
        dict[str, list[NotificationDeviceRow]],
        dict[str, str | None],
    ]:
        """Bulk-load alert_event + device-list + default_language maps.

        Returns one ``{event_public_id: AlertEventRow}``, one
        ``{user_public_id: list[NotificationDeviceRow]}`` and one
        ``{user_public_id: default_language | None}`` cache,
        deduplicating IDs so the underlying SELECTs touch each row
        at most once even when many deliveries share an alert_event
        or user. The third cache feeds the APNs sidecar's
        catalog-resolution path so push titles/bodies render in the
        recipient's preferred language.
        """
        if not rows:
            return {}, {}, {}
        event_pids = [row["alert_event_public_id"] for row in rows]
        user_pids = [row["user_public_id"] for row in rows]
        events_cache = await self._repo.get_alert_events_by_public_ids(event_pids)
        devices_cache = await self._repo.list_active_notification_devices_for_users(user_pids)
        languages_cache = await self._repo.get_default_languages_for_users(user_pids)
        return events_cache, devices_cache, languages_cache

    async def _attempt_on_queued_row(
        self,
        row: AlertDeliveryRow,
        now: datetime,
        *,
        events_cache: dict[str, AlertEventRow] | None = None,
        devices_cache: dict[str, list[NotificationDeviceRow]] | None = None,
        languages_cache: dict[str, str | None] | None = None,
    ) -> None:
        """Load the source alert_event + device for one queued row and send.

        ``events_cache`` and ``devices_cache`` are optional pre-loaded
        bulk results produced by :meth:`_prefetch_caches_for_rows`. When
        a cache miss falls through (e.g. row referenced outside any
        prefetched batch), the per-row repository fetch is reused —
        keeping the path correct for cancellation-event handlers that
        process a single delivery without a surrounding batch.
        """
        event_pid = row["alert_event_public_id"]
        if events_cache is not None and event_pid in events_cache:
            event: AlertEventRow | None = events_cache[event_pid]
        else:
            event = await self._repo.get_alert_event_by_public_id(event_pid)
        if event is None:
            logger.warning(
                "sidecar: delivery={pid} references missing alert_event {evt} — marking failed",
                pid=row["public_id"],
                evt=event_pid,
            )
            await self._mark_failed(row["public_id"], "alert_event missing", now)
            return
        user_pid = row["user_public_id"]
        if devices_cache is not None and user_pid in devices_cache:
            devices = devices_cache[user_pid]
        else:
            devices = await self._repo.list_active_notification_devices_for_user(user_pid)
        device = next((d for d in devices if d["public_id"] == row["device_public_id"]), None)
        if device is None:
            logger.info(
                "sidecar: delivery={pid} target device inactive —"
                " marking unregistered (no APNs call)",
                pid=row["public_id"],
            )
            await self._mark_unregistered_no_device(row["public_id"], now)
            return
        if languages_cache is not None and user_pid in languages_cache:
            user_language: str | None = languages_cache[user_pid]
        else:
            language_map = await self._repo.get_default_languages_for_users([user_pid])
            user_language = language_map.get(user_pid)
        await self._attempt_once(
            row["public_id"],
            device,
            event,
            now,
            user_language=user_language,
        )

    async def _mark_failed(self, delivery_public_id: str, error_reason: str, now: datetime) -> None:
        """Terminal failure with provenance stamped from this sidecar."""
        sid = self._tracker.session_id
        seq = self._tracker.next_sequence(_ZMQ_STREAM)
        await self._repo.mark_delivery_failed(
            delivery_public_id,
            error_reason=error_reason,
            transition_at=now,
            session_id=sid,
            sequence_id=seq,
        )

    async def _mark_unregistered_no_device(self, delivery_public_id: str, now: datetime) -> None:
        """Mark a delivery unregistered when its target device is already gone."""
        sid = self._tracker.session_id
        seq = self._tracker.next_sequence(_ZMQ_STREAM)
        await self._repo.mark_delivery_unregistered(
            delivery_public_id,
            transition_at=now,
            session_id=sid,
            sequence_id=seq,
        )

    async def _process_retry_queue_loop(self) -> None:
        """Wake every 30s and retry deliveries whose ``next_attempt_at`` arrived.

        Background task spawned in ``start()``; tears down when
        ``stop()`` sets the stop event. A cancelled retry is just an
        interrupted sleep — the next startup drains any rows left
        behind. Each tick mints one ``now`` at the entry boundary
        (single timestamp per tick) that drives both
        the retry-eligibility SELECT and every SCD2 write performed
        while processing the batch returned by that query.
        """
        while not self._stop_event.is_set():
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop_event.wait(), timeout=_RETRY_LOOP_INTERVAL_S)
            if self._stop_event.is_set():
                return
            now = datetime.now(UTC)
            rows = await self._repo.list_deliveries_ready_for_retry(now)
            events_cache, devices_cache, languages_cache = await self._prefetch_caches_for_rows(
                rows
            )
            for row in rows:
                await self._attempt_on_queued_row(
                    row,
                    now,
                    events_cache=events_cache,
                    devices_cache=devices_cache,
                    languages_cache=languages_cache,
                )


def _priority_to_apns(priority: str) -> int:
    """Map ``AlertPriority`` to the APNs ``apns-priority`` header value.

    Args:
        priority: ``low`` / ``medium`` / ``high``.

    Returns:
        ``10`` for ``high``; ``5`` for ``low`` / ``medium``
        (throttleable).
    """
    return 10 if priority == "high" else 5


def _build_apns_payload(event: AlertEventRow, *, user_language: str | None = None) -> JsonObject:
    """Assemble the APNs payload dict from a persisted ``AlertEventRow``.

    Keeps the ``aps.alert.title`` / ``aps.alert.body`` mapping
    explicit rather than leaking row shape through.
    Optional ``thread-id`` and custom context only appear when the
    source row actually carried them, minimising the 4KB APNs
    payload budget.

    When ``user_language`` is non-null AND the event payload carries
    ``title_loc_key``/``body_loc_key`` (emitted by every notify rule),
    the title/body are resolved through ``snapper.i18n.catalog.render``
    so the APNs push renders in the recipient's chosen language.
    Falls back to the EN ``event.title``/``event.body`` columns for
    legacy rows / users with no preference. ``loc_args`` lists are
    coerced to ``str`` per element so integer args like
    ``%lld`` placeholders work without callers having to pre-stringify.

    Args:
        event: Persisted ``AlertEventRow`` as returned by
            ``get_alert_event_by_public_id``.
        user_language: Recipient's ``User.default_language``
            preference, or ``None`` when never set.

    Returns:
        JSON-ready dict to hand to ``ApnsClientPool.send``.
    """
    title, body = _resolve_localized_title_body(event, user_language)
    aps: JsonObject = {
        "alert": {"title": title, "body": body},
    }
    thread_key = event.get("thread_key")
    if thread_key is not None:
        aps["thread-id"] = thread_key
    payload: JsonObject = {"aps": aps}
    extra = event.get("payload")
    if extra is not None:
        for key, value in extra.items():
            if key in _APNS_PAYLOAD_SKIP_KEYS:
                continue
            payload[key] = value
    payload["alert_type"] = event["alert_type"]
    payload["priority"] = event["priority"]
    return payload


_APNS_PAYLOAD_SKIP_KEYS: frozenset[str] = frozenset(
    {
        "aps",
        "title_loc_key",
        "title_loc_args",
        "body_loc_key",
        "body_loc_args",
    }
)
"""Keys excluded from the APNs custom payload.

``aps`` is rewritten by ``_build_apns_payload`` directly. The four
``*_loc_key``/``*_loc_args`` keys are an internal contract between
notify rules and the backend resolver — iOS does not consume them
(the backend resolves the title/body server-side), so we strip them
from the wire payload to (a) save the 4 KB APNs budget and (b)
avoid leaking exchange/instrument/reason values in a second place.
"""


def _resolve_localized_title_body(
    event: AlertEventRow, user_language: str | None
) -> tuple[str, str]:
    """Return localized ``(title, body)`` for the APNs alert dict.

    Thin wrapper around ``snapper.i18n.catalog.resolve_alert_strings``
    that supplies the AlertEventRow shape — the same helper is used by
    the REST alert-history endpoints so the two surfaces share a
    single fallback funnel (no risk of divergent legacy-row behavior
    between APNs and REST).
    """
    return catalog.resolve_alert_strings(
        payload=event.get("payload"),
        fallback_title=event["title"],
        fallback_body=event["body"],
        user_language=user_language,
        log_context=f"alert_event={event['public_id']}",
    )
