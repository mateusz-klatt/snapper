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
``begin_delivery_attempt``. A crash after this commit leaves the counter
charged; the next retry allocates a new attempt number. Retry scheduling
preserves the counter and refuses results from an older attempt. Terminal
outcomes use the ``mark_delivery_*`` close-and-insert methods. These database
transitions do not provide exclusive or exactly-once APNs delivery.
"""

import asyncio
import contextlib
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from uuid import uuid7

from loguru import logger

from snapper.application.notify.apns_client import ApnsClientPool
from snapper.application.notify.apns_client import ApnsSendResult
from snapper.application.notify.apns_client import is_network_delivery_error
from snapper.application.notify.fcm_client import FcmClient
from snapper.application.notify.fcm_delivery import FcmDeliveryService
from snapper.application.notify.portfolio_drift_paging import PORTFOLIO_DRIFT_ALERT_TYPE
from snapper.application.notify.portfolio_drift_recovery import PortfolioDriftRecoveryScanner
from snapper.application.notify.push_beta import PushBetaConfig
from snapper.application.notify.routing import allows_device_alert
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
from snapper.data.repository_types import DeviceAlertPrefRow
from snapper.data.repository_types import NotificationDeviceRow
from snapper.data.repository_types import UserAlertDefaultRow
from snapper.i18n import catalog
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.schemas.data import AlertEventData

_RETRY_LOOP_INTERVAL_S = 30.0
_RETRY_GIVE_UP_AFTER_ATTEMPTS = 3
_BACKOFF_BASE_S = 30.0
_BACKOFF_CAP_S = 300.0
_NETWORK_BACKOFF_CAP_S = 900.0
_DEFAULT_DELIVERY_TTL = timedelta(hours=6)
_APNS_SEND_TIMEOUT_S = 30.0
_MAX_ERROR_REASON_LEN = 200
_THROTTLED_RETRY_DEFAULT_S = 60.0
_ZMQ_STREAM = "sidecar.notify"


@dataclass(frozen=True, slots=True)
class _DeliveryTarget:
    """Immutable context for one APNs delivery attempt."""

    delivery_public_id: str
    device: NotificationDeviceRow
    event: AlertEventRow
    user_language: str | None = None
    expires_at: datetime | None = None
    created_at: datetime | None = None
    prior_error_reason: str | None = None


@dataclass(frozen=True, slots=True)
class _PrefetchedBatchCaches:
    """Bulk-loaded lookup tables for one drain or retry batch."""

    events: dict[str, AlertEventRow]
    devices: dict[str, list[NotificationDeviceRow]]
    languages: dict[str, str | None]
    prefs: dict[str, list[DeviceAlertPrefRow]] = field(default_factory=dict)
    defaults: dict[str, list[UserAlertDefaultRow]] = field(default_factory=dict)


def _format_exception_reason(exc: BaseException) -> str:
    """Format a bounded persistence reason from a raised send exception.

    Args:
        exc: Exception caught around ``ApnsClientPool.send``.

    Returns:
        Truncated ``exception: <Type>[: <msg>]`` string fitting ``VARCHAR(200)``.
    """
    detail = str(exc)
    raw = (
        f"exception: {type(exc).__name__}: {detail}"
        if detail
        else f"exception: {type(exc).__name__}"
    )
    return raw[:_MAX_ERROR_REASON_LEN]


def _expired_error_reason(prior_reason: str | None) -> str:
    """Build a bounded terminal reason when a delivery exceeds its outage TTL.

    Args:
        prior_reason: Last recorded error reason before expiry, if any.

    Returns:
        Clear expiry reason fitting ``AlertDelivery.error_reason`` (200 chars).
    """
    if prior_reason:
        return f"expired during outage: {prior_reason}"[:_MAX_ERROR_REASON_LEN]
    return "expired during outage"


def _deadline_for_delivery_row(row: AlertDeliveryRow) -> datetime:
    """Resolve the effective outage TTL deadline for one queued delivery row.

    Args:
        row: Queued ``AlertDeliveryRow`` snapshot.

    Returns:
        ``expires_at`` when populated, else ``created_at + _DEFAULT_DELIVERY_TTL``.
    """
    explicit_expiry = row.get("expires_at")
    if explicit_expiry is not None:
        return explicit_expiry
    return row["created_at"] + _DEFAULT_DELIVERY_TTL


def _is_drain_eligible(row: AlertDeliveryRow, now: datetime) -> bool:
    """Return whether startup drain should evaluate ``row`` at ``now``.

    Honours persisted ``next_attempt_at`` backoff for rows still within their
    outage TTL while admitting rows whose TTL already expired so cleanup runs
    independently of sender availability.
    """
    next_attempt = row.get("next_attempt_at")
    if next_attempt is None or next_attempt <= now:
        return True
    return now >= _deadline_for_delivery_row(row)


def _backoff_seconds(attempt_number: int, *, cap_s: float = _BACKOFF_CAP_S) -> float:
    """Compute the exponential backoff for the N-th attempt.

    Schedule: ``min(30s * 2^(N-1), cap_s)``.
    With default ``cap_s=300s``: N=1 -> 30s, N=2 -> 60s, N=3 -> 120s, capped
    at 300s. Network-outage retries pass ``cap_s=900s`` (15 min).

    Args:
        attempt_number: 1-based attempt number that just returned a
            non-terminal error (retryable server_error / throttled / network).
        cap_s: Maximum backoff delay in seconds.

    Returns:
        Backoff duration in seconds (float).
    """
    exp = max(attempt_number - 1, 0)
    scaled: float = _BACKOFF_BASE_S * (2**exp)
    return min(scaled, cap_s)


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
        apns: ApnsClientPool | None,
        apns_topic: str,
        tracker: SequenceTracker,
        publisher: MessagePublisher,
        registry: RuleRegistry | None = None,
        scope_revalidator: ScopeRevalidator | None = None,
        push_beta_provider: Callable[[], PushBetaConfig] | None = None,
        portfolio_drift_recovery_scanner: PortfolioDriftRecoveryScanner | None = None,
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
                ``load_default_registry()``. Injected for tests that want a
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
            portfolio_drift_recovery_scanner: Scanner override for tests.
                ``None`` builds the production scanner over this sidecar's
                normal persistence and fanout sink.
        """
        self._subscriber = subscriber
        self._repo = repo
        self._android: FcmDeliveryService | None = None
        self._apns = apns
        self._apns_topic = apns_topic
        self._tracker = tracker
        self._publisher = publisher
        self._stop_event = asyncio.Event()
        self._retry_task: asyncio.Task[None] | None = None
        self._workers: list[asyncio.Task[None] | asyncio.Task[bool]] = []
        self._running = False
        self._cleanup_complete = False
        self._registry = registry or load_default_registry()
        self._scope_revalidator = scope_revalidator or ScopeRevalidator(tracker=tracker)
        self._push_beta_provider = push_beta_provider
        self._clock: Callable[[], datetime] = lambda: datetime.now(UTC)
        self._outage_started_at: datetime | None = None
        self._outage_delayed_delivery_ids: set[str] = set()
        self._portfolio_drift_recovery_scanner = (
            portfolio_drift_recovery_scanner
            if portfolio_drift_recovery_scanner is not None
            else PortfolioDriftRecoveryScanner(
                repo=repo,
                emit_alert_row=self._persist_and_fanout_row,
            )
        )

    def configure_android(self, sender: FcmClient) -> None:
        """Attach an independently configured sender while preserving APNs defaults.

        Args:
            sender: Validated sender for one immutable Android project configuration.
        """
        self._android = FcmDeliveryService(
            self._repo, sender, self._tracker, self._scope_revalidator
        )
        self._android.push_beta_provider = self._push_beta_provider

    async def start(self) -> None:
        """Own startup, supervised workers and awaited teardown.

        Drain the outbox, start recovery, then supervise receive, retry and
        stop together. Retry failure wins over simultaneous receive failure
        or normal stop. Every exit awaits owned workers and scanner cleanup
        without replacing the original failure or external cancellation.
        """
        if self._stop_event.is_set():
            return
        self._running = True
        primary: BaseException | None = None
        try:
            for prefix in self._registry.all_subscribe_prefixes():
                self._subscriber.subscribe(prefix)
            self._subscriber.subscribe("admin.scope_revoked")
            self._subscriber.subscribe("admin.user_deactivated")
            self._subscriber.subscribe("admin.membership_revoked")
            await self._drain_outbox(self._clock())
            await self._portfolio_drift_recovery_scanner.start()
            await self._run_workers()
        except BaseException as exc:
            primary = exc
            raise
        finally:
            try:
                failure = await self._finish_run(primary)
                if primary is None and failure is not None:
                    raise failure
            finally:
                if primary is not None:
                    raise primary

    async def _run_workers(self) -> None:
        """Observe retry before receive when multiple workers finish together."""
        self._retry_task = asyncio.create_task(self._process_retry_queue_loop())
        self._workers.append(self._retry_task)
        receive_task = asyncio.create_task(self._receive_loop())
        self._workers.append(receive_task)
        stop_task = asyncio.create_task(self._stop_event.wait())
        self._workers.append(stop_task)
        await asyncio.wait(self._workers, return_when=asyncio.FIRST_COMPLETED)
        for task, name in ((self._retry_task, "retry"), (receive_task, "receive")):
            if task.done():
                task.result()
                if not self._stop_event.is_set():
                    raise RuntimeError(f"Notify sidecar {name} worker exited before stop")

    async def _receive_loop(self) -> None:
        """Receive and dispatch frames with one timestamp per received message."""
        while not self._stop_event.is_set():
            topic, payload = await self._subscriber.recv_multipart()
            if self._stop_event.is_set():
                return
            await self._dispatch(topic, payload, self._clock())

    async def _join_cleanup(
        self, cleanup_task: asyncio.Task[BaseException | None]
    ) -> BaseException | None:
        """Join teardown and propagate the first observed cancellation.

        Each cancellation while teardown remains pending adds one stack frame;
        this handles ordinary repeated cancellation within Python's recursion limit.
        """
        try:
            return await asyncio.shield(cleanup_task)
        except asyncio.CancelledError as cancellation:
            try:
                if not cleanup_task.done():
                    await self._join_cleanup(cleanup_task)
            finally:
                raise cancellation

    async def _finish_run(self, primary: BaseException | None) -> BaseException | None:
        """Finish cleanup even when the owner is cancelled while awaiting it."""
        cleanup_task = asyncio.create_task(self._cleanup_workers(primary))
        try:
            return await self._join_cleanup(cleanup_task)
        except asyncio.CancelledError as exc:
            failure = None if cleanup_task.cancelled() else cleanup_task.result()
            if failure is not None:
                self._retain_failure(primary if primary is not None else exc, failure, "cleanup")
            raise
        finally:
            self._running = False
            self._cleanup_complete = (
                cleanup_task.done()
                and not cleanup_task.cancelled()
                and cleanup_task.exception() is None
            )

    async def _cleanup_workers(self, primary: BaseException | None) -> BaseException | None:
        """Cancel and retrieve all workers, then always attempt scanner cleanup."""
        self._stop_event.set()
        for task in self._workers:
            if not task.done():
                task.cancel()
        results = await asyncio.gather(*self._workers, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
                primary = self._retain_failure(primary, result, "worker")
        scanner_results = await asyncio.gather(
            self._portfolio_drift_recovery_scanner.stop(), return_exceptions=True
        )
        scanner_failure = scanner_results[0]
        if isinstance(scanner_failure, BaseException):
            primary = self._retain_failure(primary, scanner_failure, "scanner")
        return primary

    @staticmethod
    def _retain_failure(
        primary: BaseException | None, failure: BaseException, stage: str
    ) -> BaseException:
        """Keep the first failure and report secondary failure types safely."""
        if primary is None:
            return failure
        if failure is not primary:
            logger.error(
                "sidecar: secondary cleanup failure stage={stage} type={error_type}",
                stage=stage,
                error_type=type(failure).__name__,
            )
        return primary

    async def stop(self) -> None:
        """Signal the owner to stop; completed owner teardown is idempotent.

        While start is active it owns and awaits all cleanup. Before start,
        stop the scanner once; a later start observes the stop event and
        creates no workers. Repeated CLI-finally calls cannot repeat a
        scanner failure already observed during teardown.
        """
        self._stop_event.set()
        if self._running or self._cleanup_complete:
            return
        try:
            await self._portfolio_drift_recovery_scanner.stop()
        finally:
            self._cleanup_complete = True

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
        if await self._dispatch_admin_revocation(topic, payload, now):
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

    async def _dispatch_admin_revocation(
        self,
        topic: str,
        payload: bytes,
        now: datetime,
    ) -> bool:
        """Dispatch one administrative authority-reduction event when matched."""
        handlers = {
            "admin.scope_revoked": self._scope_revalidator.handle_scope_revoked,
            "admin.user_deactivated": self._scope_revalidator.handle_user_deactivated,
            "admin.membership_revoked": self._scope_revalidator.handle_membership_revoked,
        }
        handler = handlers.get(topic)
        if handler is None:
            return False
        try:
            await handler(payload, self._repo, now)
        except Exception as exc:
            logger.warning("sidecar: {topic} handler raised: {err}", topic=topic, err=exc)
        return True

    async def _persist_and_fanout_row(
        self,
        alert_row: AlertEventInsertRow,
        now: datetime,
    ) -> None:
        """Insert the alert row + fan out via policy routing.

        ``route_alert_to_devices`` runs before any ``alert_deliveries`` insert,
        and ``AlertDelivery.device_public_id`` is non-null. When routing
        returns no eligible devices, no delivery row is inserted and nothing
        is left ``queued``.

        Args:
            alert_row: Row minted by an ``AlertRule.evaluate`` call.
            now: Entry-boundary timestamp (threaded from ``_dispatch``).
        """
        event_public_id = await self._insert_alert_event(alert_row, now)
        if event_public_id is None:
            return
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
        if self._android is not None:
            await self._android.fanout(event, now, push_beta)
        if self._apns is None:
            return
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
        expires_at = now + _DEFAULT_DELIVERY_TTL
        for device in recipients:
            delivery_pid = await self._insert_delivery_for_event(
                event=event, device=device, now=now
            )
            target = _DeliveryTarget(
                delivery_public_id=delivery_pid,
                device=device,
                event=event,
                user_language=user_language,
                expires_at=expires_at,
                created_at=now,
            )
            await self._attempt_delivery(target, now)

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

    async def _insert_alert_event(
        self,
        alert_row: AlertEventInsertRow,
        now: datetime,
    ) -> str | None:
        """Write one event, atomically claiming drift dedup identities."""
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
        if enriched["alert_type"] == PORTFOLIO_DRIFT_ALERT_TYPE:
            return await self._repo.insert_alert_event_if_dedup_absent(enriched)
        return await self._repo.insert_alert_event(enriched)

    async def _insert_delivery_for_event(
        self,
        *,
        event: AlertEventRow,
        device: NotificationDeviceRow,
        now: datetime,
    ) -> str:
        """Insert one queued ``alert_deliveries`` row with denormalised scope and 6 h TTL."""
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
                expires_at=now + _DEFAULT_DELIVERY_TTL,
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
        """Run one APNs send attempt from raw identifiers (test and fanout helper)."""
        target = _DeliveryTarget(
            delivery_public_id=delivery_public_id,
            device=device,
            event=event,
            user_language=user_language,
        )
        await self._attempt_delivery(target, now)

    async def _expire_if_past_deadline(
        self,
        delivery_public_id: str,
        prior_reason: str | None,
        deadline: datetime,
        now: datetime,
    ) -> bool:
        """Transition a delivery to ``failed`` without sending when ``now >= deadline``."""
        if now < deadline:
            return False
        await self._fail_expired_delivery(delivery_public_id, prior_reason, now)
        return True

    async def _attempt_delivery(self, target: _DeliveryTarget, now: datetime) -> None:
        """Run exactly one APNs send attempt on a queued delivery row.

        Before reserving an attempt and again immediately before the APNs
        call, ``ScopeRevalidator.should_skip_send`` decides whether the
        event's scope is still active and a fresh UTC check enforces the
        resolved delivery deadline (``expires_at`` or ``created_at + 6 h``).
        Deliveries past their deadline transition to ``failed`` without
        sending. Otherwise, bumps ``attempt_count`` first, bounds
        ``ApnsClientPool.send`` with ``_APNS_SEND_TIMEOUT_S``, and computes
        retry backoff and SCD2 transition times from fresh UTC timestamps.

        Note:
            If ``ApnsClientPool.send`` hits the 30s timeout after APNs already
            accepted the push before the HTTP/2 response returned, the row
            stays ``queued`` and a subsequent retry can duplicate the push
            (collapsed per ``thread_key`` via ``apns-collapse-id`` when set).
        """
        _ = now
        if self._apns is None:
            return
        admission_now = self._clock()
        if await self._cancel_if_scope_revoked(
            target.delivery_public_id, target.event, admission_now
        ):
            return
        deadline = await self._resolve_delivery_deadline(target, admission_now)
        if await self._expire_if_past_deadline(
            target.delivery_public_id,
            target.prior_error_reason,
            deadline,
            admission_now,
        ):
            return
        current_attempt = await self._bump_attempt(target.delivery_public_id, self._clock())
        if current_attempt is None:
            logger.info(
                "sidecar: delivery {pid} no longer queued at bump time —"
                " concurrent cancel / terminal transition wins",
                pid=target.delivery_public_id,
            )
            return
        send_now = self._clock()
        if await self._cancel_if_scope_revoked(target.delivery_public_id, target.event, send_now):
            return
        if await self._expire_if_past_deadline(
            target.delivery_public_id,
            target.prior_error_reason,
            deadline,
            send_now,
        ):
            return
        payload = _build_apns_payload(target.event, user_language=target.user_language)
        try:
            result = await asyncio.wait_for(
                self._apns.send(
                    env=target.device["env"],
                    device_token=target.device["device_token"],
                    payload=payload,
                    apns_topic=self._apns_topic,
                    priority=_priority_to_apns(target.event["priority"]),
                    push_type="alert",
                    collapse_id=target.event.get("thread_key"),
                ),
                timeout=_APNS_SEND_TIMEOUT_S,
            )
        except Exception as exc:
            await self._handle_send_exception(target, current_attempt, exc, self._clock())
            return
        await self._apply_result(target, current_attempt, result, self._clock())

    async def _handle_send_exception(
        self,
        target: _DeliveryTarget,
        attempt_number: int,
        exc: Exception,
        now: datetime,
    ) -> None:
        """Classify a raised APNs send exception and schedule retry or failure."""
        is_network = is_network_delivery_error(exc)
        error_reason = _format_exception_reason(exc)
        logger.warning(
            "sidecar: APNs send raised — scheduling retry"
            " delivery={pid}, attempt={n}, network_error={net}, err={err}",
            pid=target.delivery_public_id,
            n=attempt_number,
            net=is_network,
            err=error_reason,
        )
        if is_network:
            self._record_outage_delay(target.delivery_public_id, now)
            await self._schedule_network_retry_or_fail(target, attempt_number, error_reason, now)
            return
        await self._schedule_retry_or_fail(target, attempt_number, error_reason, now)

    async def _cancel_if_scope_revoked(
        self,
        delivery_public_id: str,
        event: AlertEventRow,
        now: datetime,
    ) -> bool:
        """Cancel one delivery when its authority is stale at an admission check.

        Args:
            delivery_public_id: Delivery whose APNs attempt is being admitted.
            event: Alert event carrying the recipient and desk scope.
            now: Dispatch timestamp used for the cancellation transition.

        Returns:
            Whether the delivery was cancelled and the caller must stop.
        """
        if not await self._scope_revalidator.should_skip_send(event, self._repo, now):
            return False
        await self._mark_cancelled(delivery_public_id, "scope_revoked", now)
        logger.info(
            "sidecar: scope revoked mid-send — cancelled delivery={pid}"
            " user={user} operator={op} wallet={wal}",
            pid=delivery_public_id,
            user=event["user_public_id"],
            op=event.get("operator_public_id"),
            wal=event.get("wallet_public_id"),
        )
        return True

    async def _bump_attempt(self, delivery_public_id: str, now: datetime) -> int | None:
        """Return the attempt number committed by the repository, or refuse.

        The repository increments the selected queued version in its guarded
        transaction. Missing, terminal or concurrently closed versions return
        None so the caller does not send an unallocated attempt.
        """
        return await self._repo.begin_delivery_attempt(
            delivery_public_id,
            transition_at=now,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence(_ZMQ_STREAM),
        )

    async def _apply_result(
        self,
        target: _DeliveryTarget,
        attempt_number: int,
        result: ApnsSendResult,
        now: datetime,
    ) -> None:
        """Route an APNs result to the correct SCD2 terminal / retry transition."""
        sid = self._tracker.session_id
        seq = self._tracker.next_sequence(_ZMQ_STREAM)
        if result.status == "success":
            await self._repo.mark_delivery_sent(
                target.delivery_public_id,
                apns_id=result.apns_id,
                transition_at=now,
                session_id=sid,
                sequence_id=seq,
            )
            await self._maybe_log_outage_recovery(target.delivery_public_id, now)
            return
        if result.status == "unregistered":
            await self._repo.mark_delivery_unregistered(
                target.delivery_public_id,
                transition_at=now,
                session_id=sid,
                sequence_id=seq,
            )
            device_sid = self._tracker.session_id
            device_seq = self._tracker.next_sequence(_ZMQ_STREAM)
            await self._repo.deactivate_notification_device_scd2(
                target.device["public_id"],
                reason="unregistered",
                timestamp=now,
                session_id=device_sid,
                sequence_id=device_seq,
            )
            return
        if result.status == "config_error":
            error_reason = f"config_error:{result.description}"[:_MAX_ERROR_REASON_LEN]
            logger.error(
                "sidecar: APNs configuration error — leaving device active"
                " delivery_public_id={pid}, device_public_id={dev}, env={env},"
                " apns_topic={topic}, status_code={code}, reason={reason}",
                pid=target.delivery_public_id,
                dev=target.device["public_id"],
                env=target.device["env"],
                topic=self._apns_topic,
                code=result.status_code,
                reason=result.description,
            )
            await self._repo.mark_delivery_failed(
                target.delivery_public_id,
                error_reason=error_reason,
                transition_at=now,
                session_id=sid,
                sequence_id=seq,
            )
            return
        await self._schedule_retry_or_fail(
            target,
            attempt_number,
            (result.description or result.status)[:_MAX_ERROR_REASON_LEN],
            now,
        )

    async def _schedule_retry_or_fail(
        self,
        target: _DeliveryTarget,
        attempt_number: int,
        error_reason: str,
        now: datetime,
    ) -> None:
        """Give up at 3 attempts for rejects/HTTP errors or schedule next retry."""
        if attempt_number >= _RETRY_GIVE_UP_AFTER_ATTEMPTS:
            logger.warning(
                "sidecar: alert delivery failed after retries"
                " — delivery_public_id={pid}, attempts={n}, error_reason={err}",
                pid=target.delivery_public_id,
                n=attempt_number,
                err=error_reason,
            )
            await self._mark_failed(target.delivery_public_id, error_reason, now)
            return
        backoff_s = _backoff_seconds(attempt_number)
        next_attempt_at = now + timedelta(seconds=backoff_s)
        await self._persist_retry_schedule(
            target.delivery_public_id,
            attempt_number,
            next_attempt_at,
            error_reason,
            now,
        )

    async def _schedule_network_retry_or_fail(
        self,
        target: _DeliveryTarget,
        attempt_number: int,
        error_reason: str,
        now: datetime,
    ) -> None:
        """Retry a network-class outage failure with 15 min cap until TTL expires."""
        deadline = await self._resolve_delivery_deadline(target, now)
        if now >= deadline:
            await self._fail_expired_delivery(target.delivery_public_id, error_reason, now)
            return
        backoff_s = _backoff_seconds(attempt_number, cap_s=_NETWORK_BACKOFF_CAP_S)
        next_attempt_at = min(now + timedelta(seconds=backoff_s), deadline)
        await self._persist_retry_schedule(
            target.delivery_public_id,
            attempt_number,
            next_attempt_at,
            error_reason,
            now,
        )

    async def _resolve_delivery_deadline(self, target: _DeliveryTarget, now: datetime) -> datetime:
        """Return the outage-retry expiration deadline for ``target``.

        Uses ``target.expires_at`` when set, otherwise ``created_at + 6 h``.
        Falls back to reading the active delivery row when ``_attempt_once`` was
        invoked directly without pre-loaded row timestamps.
        """
        if target.expires_at is not None:
            return target.expires_at
        if target.created_at is not None:
            return target.created_at + _DEFAULT_DELIVERY_TTL
        row = await self._repo.get_delivery_by_public_id(target.delivery_public_id)
        if row is not None:
            return _deadline_for_delivery_row(row)
        return now

    async def _fail_expired_delivery(
        self, delivery_public_id: str, prior_reason: str | None, now: datetime
    ) -> None:
        """Transition a delivery whose outage TTL elapsed to terminal ``failed``."""
        reason = _expired_error_reason(prior_reason)
        logger.warning(
            "sidecar: alert delivery expired during outage"
            " — delivery_public_id={pid}, error_reason={err}",
            pid=delivery_public_id,
            err=reason,
        )
        await self._mark_failed(delivery_public_id, reason, now)

    async def _persist_retry_schedule(
        self,
        delivery_public_id: str,
        attempt_number: int,
        next_attempt_at: datetime,
        error_reason: str | None,
        now: datetime,
    ) -> None:
        """Commit an updated ``next_attempt_at`` on the queued delivery row."""
        bounded_reason = error_reason[:_MAX_ERROR_REASON_LEN] if error_reason is not None else None
        await self._repo.update_delivery_retry_schedule(
            delivery_public_id,
            attempt_count=attempt_number,
            next_attempt_at=next_attempt_at,
            error_reason=bounded_reason,
            transition_at=now,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence(_ZMQ_STREAM),
        )

    def _record_outage_delay(self, delivery_public_id: str, now: datetime) -> None:
        """Record one delivery delayed by an active network outage.

        Outage state is tracked process-wide across APNs ``sandbox`` and
        ``prod`` environments rather than per environment.
        """
        if self._outage_started_at is None:
            self._outage_started_at = now
        self._outage_delayed_delivery_ids.add(delivery_public_id)

    async def _maybe_log_outage_recovery(
        self, recovered_delivery_public_id: str, now: datetime
    ) -> None:
        """Log one recovery summary and reschedule delayed backlog for the next tick.

        Because outage tracking is shared process-wide across APNs
        environments, the first successful send in either ``sandbox`` or
        ``prod`` closes the shared outage window and expedites all queued
        deliveries in the delayed set.
        """
        if self._outage_started_at is None:
            return
        delayed_ids = sorted(self._outage_delayed_delivery_ids)
        started_at = self._outage_started_at
        self._outage_started_at = None
        self._outage_delayed_delivery_ids.clear()
        logger.warning(
            "sidecar: {count} deliveries delayed by an outage between {t1} and {t2}",
            count=len(delayed_ids),
            t1=started_at.isoformat(),
            t2=now.isoformat(),
        )
        await self._expedite_outage_backlog(delayed_ids, recovered_delivery_public_id, now)

    async def _expedite_outage_backlog(
        self,
        delayed_ids: list[str],
        recovered_delivery_public_id: str,
        now: datetime,
    ) -> None:
        """Set ``next_attempt_at`` to a fresh UTC time on still-queued outage rows."""
        _ = now
        for pid in delayed_ids:
            if pid == recovered_delivery_public_id:
                continue
            row = await self._repo.get_delivery_by_public_id(pid)
            if row is None or row["status"] != "queued":
                continue
            write_now = self._clock()
            next_attempt = row.get("next_attempt_at")
            if next_attempt is None or next_attempt <= write_now:
                continue
            await self._persist_retry_schedule(
                pid,
                row["attempt_count"],
                write_now,
                row.get("error_reason"),
                write_now,
            )

    async def _drain_outbox(self, now: datetime) -> None:
        """Process ready or expired ``status='queued'`` rows on startup (crash recovery).

        Ordered ``last_attempt_at ASC NULLS FIRST`` (never-attempted rows
        first, then longest-waiting retries). Honours persisted
        ``next_attempt_at`` backoff while still expiring rows already past
        their outage TTL regardless of APNs sender availability.

        Bulk-prefetches the alert_event, active devices, user languages,
        and routing preferences once for the eligible batch so drain avoids
        N+1 reads; prefetched devices and prefs carry bounded staleness up to
        the drain duration. Each eligible row uses a fresh UTC timestamp.
        """
        _ = now
        queued = await self._repo.list_queued_deliveries_all()
        queued.sort(
            key=lambda r: (
                r.get("last_attempt_at") or datetime(1970, 1, 1, tzinfo=UTC),
                r["created_at"],
            )
        )
        drain_now = self._clock()
        eligible = [row for row in queued if _is_drain_eligible(row, drain_now)]
        caches = await self._prefetch_caches_for_rows(eligible)
        for row in eligible:
            await self._safe_attempt_on_queued_row(row, self._clock(), caches=caches)

    async def _prefetch_caches_for_rows(
        self, rows: list[AlertDeliveryRow]
    ) -> _PrefetchedBatchCaches:
        """Bulk-load alert_event, device, language, and routing pref maps.

        Returns a :class:`_PrefetchedBatchCaches` bundle, deduplicating IDs
        so underlying SELECTs touch each event or user at most once per batch.
        Device and preference snapshots can exhibit bounded staleness over a
        long drain or retry tick (up to batch length * ``_APNS_SEND_TIMEOUT_S``).
        """
        if not rows:
            return _PrefetchedBatchCaches(events={}, devices={}, languages={})
        event_pids = [row["alert_event_public_id"] for row in rows]
        user_pids = list(dict.fromkeys(row["user_public_id"] for row in rows))
        events_cache = await self._repo.get_alert_events_by_public_ids(event_pids)
        devices_cache = await self._repo.list_active_notification_devices_for_users(user_pids)
        languages_cache = await self._repo.get_default_languages_for_users(user_pids)
        prefs_cache = {
            uid: await self._repo.list_device_alert_prefs_for_user(uid) for uid in user_pids
        }
        defaults_cache = {uid: await self._repo.list_user_alert_defaults(uid) for uid in user_pids}
        return _PrefetchedBatchCaches(
            events=events_cache,
            devices=devices_cache,
            languages=languages_cache,
            prefs=prefs_cache,
            defaults=defaults_cache,
        )

    async def _safe_attempt_on_queued_row(
        self,
        row: AlertDeliveryRow,
        now: datetime,
        *,
        caches: _PrefetchedBatchCaches | None = None,
    ) -> None:
        """Execute ``_attempt_on_queued_row`` isolating per-row errors from the batch."""
        try:
            await self._attempt_on_queued_row(row, now, caches=caches)
        except Exception as exc:
            logger.warning(
                "sidecar: retry attempt failed unexpectedly for delivery={pid}: {err}",
                pid=row["public_id"],
                err=exc,
            )

    async def _resolve_queued_event(
        self, event_pid: str, caches: _PrefetchedBatchCaches | None
    ) -> AlertEventRow | None:
        """Resolve an alert event from batch prefetch caches or fall back to DB."""
        if caches is not None and event_pid in caches.events:
            return caches.events[event_pid]
        return await self._repo.get_alert_event_by_public_id(event_pid)

    async def _resolve_queued_device(
        self,
        user_pid: str,
        device_pid: str,
        caches: _PrefetchedBatchCaches | None,
    ) -> NotificationDeviceRow | None:
        """Resolve an active APNs device from batch prefetch caches or DB."""
        devices = (
            caches.devices[user_pid]
            if caches is not None and user_pid in caches.devices
            else await self._repo.list_active_notification_devices_for_user(user_pid)
        )
        return next((d for d in devices if d["public_id"] == device_pid), None)

    async def _resolve_user_language(
        self, user_pid: str, caches: _PrefetchedBatchCaches | None
    ) -> str | None:
        """Resolve recipient language preference from batch prefetch caches or DB."""
        if caches is not None and user_pid in caches.languages:
            return caches.languages[user_pid]
        language_map = await self._repo.get_default_languages_for_users([user_pid])
        return language_map.get(user_pid)

    async def _cancel_if_routing_suppressed(
        self,
        row: AlertDeliveryRow,
        event: AlertEventRow,
        now: datetime,
        *,
        caches: _PrefetchedBatchCaches | None = None,
    ) -> bool:
        """Cancel a queued APNs row if rollout gate or alert prefs now suppress it."""
        user_pid = row["user_public_id"]
        if self._push_beta_provider is not None and not self._push_beta_provider().includes(
            user_pid
        ):
            await self._mark_cancelled(row["public_id"], "routing_suppressed", now)
            return True
        prefs = (
            caches.prefs[user_pid]
            if caches is not None and user_pid in caches.prefs
            else await self._repo.list_device_alert_prefs_for_user(user_pid)
        )
        defaults = (
            caches.defaults[user_pid]
            if caches is not None and user_pid in caches.defaults
            else await self._repo.list_user_alert_defaults(user_pid)
        )
        if not allows_device_alert(row["device_public_id"], event, prefs, defaults, now):
            await self._mark_cancelled(row["public_id"], "routing_suppressed", now)
            return True
        return False

    async def _attempt_on_queued_row(
        self,
        row: AlertDeliveryRow,
        now: datetime,
        *,
        caches: _PrefetchedBatchCaches | None = None,
    ) -> None:
        """Load the source alert_event + device for one queued row and send.

        Enforces the resolved delivery TTL before routing and before checking
        APNs sender availability so expired APNs rows transition to ``failed``
        even when APNs is disabled or routing data is malformed. Missing
        events, inactive devices, and rows suppressed by the routing cascade
        transition to terminal ``failed``, ``unregistered``, or
        ``cancelled_scope`` instead of remaining ``queued``.
        """
        if row.get("provider", "apns") == "fcm":
            if self._android is not None:
                await self._android.attempt(row, now)
            return
        deadline = _deadline_for_delivery_row(row)
        if await self._expire_if_past_deadline(
            row["public_id"], row.get("error_reason"), deadline, self._clock()
        ):
            return
        if self._apns is None:
            return
        event = await self._resolve_queued_event(row["alert_event_public_id"], caches)
        if event is None:
            logger.warning(
                "sidecar: delivery={pid} references missing alert_event {evt} — marking failed",
                pid=row["public_id"],
                evt=row["alert_event_public_id"],
            )
            await self._mark_failed(row["public_id"], "alert_event missing", self._clock())
            return
        device = await self._resolve_queued_device(
            row["user_public_id"], row["device_public_id"], caches
        )
        if device is None:
            logger.info(
                "sidecar: delivery={pid} target device inactive —"
                " marking unregistered (no APNs call)",
                pid=row["public_id"],
            )
            await self._mark_unregistered_no_device(row["public_id"], self._clock())
            return
        if await self._cancel_if_routing_suppressed(row, event, self._clock(), caches=caches):
            return
        user_language = await self._resolve_user_language(row["user_public_id"], caches)
        target = _DeliveryTarget(
            delivery_public_id=row["public_id"],
            device=device,
            event=event,
            user_language=user_language,
            expires_at=row.get("expires_at"),
            created_at=row["created_at"],
            prior_error_reason=row.get("error_reason"),
        )
        await self._attempt_delivery(target, now)

    async def _mark_failed(self, delivery_public_id: str, error_reason: str, now: datetime) -> None:
        """Terminal failure with provenance stamped from this sidecar."""
        sid = self._tracker.session_id
        seq = self._tracker.next_sequence(_ZMQ_STREAM)
        await self._repo.mark_delivery_failed(
            delivery_public_id,
            error_reason=error_reason[:_MAX_ERROR_REASON_LEN],
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

    async def _mark_cancelled(self, delivery_public_id: str, reason: str, now: datetime) -> None:
        """Terminal scope/routing cancellation with provenance from this sidecar."""
        await self._repo.mark_delivery_cancelled(
            delivery_public_id,
            reason=reason[:_MAX_ERROR_REASON_LEN],
            transition_at=now,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence(_ZMQ_STREAM),
        )

    async def _process_retry_queue_loop(self) -> None:
        """Wake every 30s and retry deliveries whose ``next_attempt_at`` arrived.

        Background task spawned in ``start()``; tears down when
        ``stop()`` sets the stop event. Each tick selects ready rows at tick
        entry, prefetches batch caches (whose device and preference snapshots
        carry bounded staleness over the tick duration), and passes a fresh
        UTC timestamp for each row so earlier slow/timed-out sends do not
        leave later rows with stale expiry or retry timestamps.

        Why every send is bounded: both retry selections
        (``list_deliveries_ready_for_retry`` and ``list_queued_deliveries_all``)
        already include rows whose ``next_attempt_at`` is in the past, but this
        loop processes rows sequentially, and an unbounded
        ``self._apns.send(...)`` can block indefinitely inside ``aioapns``
        (pool acquire, a stalled socket, or an unresolved response future)
        during a network outage. One hung send would stall this task without
        raising to ``_run_workers`` and strand every due row even after the
        network recovers. Bounding each send with ``_APNS_SEND_TIMEOUT_S`` and
        isolating per-row errors keeps the retry scheduler advancing. FCM rows
        are intentionally left untouched when ``_android is None``.
        """
        while not self._stop_event.is_set():
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop_event.wait(), timeout=_RETRY_LOOP_INTERVAL_S)
            if self._stop_event.is_set():
                return
            tick_now = self._clock()
            rows = await self._repo.list_deliveries_ready_for_retry(tick_now)
            caches = await self._prefetch_caches_for_rows(rows)
            for row in rows:
                await self._safe_attempt_on_queued_row(row, self._clock(), caches=caches)


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
    ``title_loc_key``/``body_loc_key`` (emitted by localized notify rules),
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
