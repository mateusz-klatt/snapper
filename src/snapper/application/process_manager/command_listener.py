"""Coordinator-side listener for HMAC-signed process-command nudges (control plane P2.3).

Each coordinator engine (feed / strategy) runs one listener subscribed to its own
``processes.commands.{own_slug}`` topic. On a well-formed, signature-valid, fresh,
non-duplicate command addressed to this coordinator it triggers an immediate
:meth:`ProcessLauncherService.reconcile_desired_state` and publishes a SIGNED
:class:`ProcessCommandAckData` on ``processes.events.command_ack.{own_slug}`` so the
API's blocking PATCH resolves in milliseconds instead of waiting for the periodic
tick.

The nudge is NEVER authoritative: a dropped, duplicated, or forged command is
harmless because the periodic reconcile (P0.3) converges from the DB desired-state
regardless. The HMAC (P2.1) blocks unauthenticated bus injection; the signed
``coordinator`` field (not the unsigned topic) is the authoritative slug check; a
bounded seen-cache plus a freshness window bound replay.
"""

import asyncio
import contextlib
import json
from collections import OrderedDict
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from uuid import uuid7

import zmq
import zmq.asyncio
from loguru import logger

from snapper.application.process_manager.launcher import ProcessLauncherService
from snapper.messaging.infrastructure.validated_socket import HWM_AUDIT
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.data import ProcessCommandAckData
from snapper.messaging.schemas.data import ProcessCommandData
from snapper.messaging.security.command_signing import command_signing_key
from snapper.messaging.security.command_signing import sign_command_payload
from snapper.messaging.security.command_signing import verify_command_payload

_COMMANDS_STREAM = "processes.commands"
_ACK_STREAM = "processes.events.command_ack"
_FRESHNESS_WINDOW = timedelta(seconds=30)
_SEEN_MAX = 1024
_RECV_BACKOFF_S = 0.1
_ACK_APPLIED = "applied"
_ACK_REJECTED = "rejected"


class ProcessCommandListener:
    """Subscribes to this coordinator's command topic; verifies, reconciles, acks."""

    def __init__(self, launcher: ProcessLauncherService) -> None:
        """Bind the listener to a launcher and derive its signing key + slug.

        Args:
            launcher: The owning coordinator's launcher — supplies the slug,
                the wired publisher, the master password, and the reconcile.
        """
        self._launcher = launcher
        self._own_slug = launcher.coordinator_topic_slug()
        self._command_topic = f"{_COMMANDS_STREAM}.{self._own_slug}"
        self._ack_topic = f"{_ACK_STREAM}.{self._own_slug}"
        self._key = command_signing_key(launcher.settings.master_password)
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._lock = asyncio.Lock()
        self._context: zmq.asyncio.Context | None = None
        self._subscriber: ValidatedSubscriber | None = None
        self._task: asyncio.Task[None] | None = None
        self._running = False

    async def start(self, zmq_broker_xpub: str) -> None:
        """Subscribe to the command topic and spawn the listen loop.

        Idempotent and restart-safe via an internal lock (mirrors
        ``AiReviewService.start_bus_listener``). An empty XPUB endpoint (e.g.
        an embedded broker / test harness with no bus) skips the listener.

        Args:
            zmq_broker_xpub: The broker XPUB endpoint to connect the SUB socket to.
        """
        async with self._lock:
            if self._task is not None and not self._task.done():
                return
            if self._task is not None:
                await self._reap_unlocked()
            if not zmq_broker_xpub:
                logger.info("ProcessCommandListener: empty XPUB, skipping listener")
                return
            self._context = zmq.asyncio.Context()
            raw_socket = self._context.socket(zmq.SUB)
            apply_hwm(raw_socket, rcvhwm=HWM_AUDIT)
            raw_socket.connect(zmq_broker_xpub)
            self._subscriber = ValidatedSubscriber(raw_socket)
            self._subscriber.subscribe(self._command_topic)
            self._running = True
            self._task = asyncio.create_task(self._listen_loop())
            logger.info(
                "ProcessCommandListener subscribed to {} on {}",
                self._command_topic,
                zmq_broker_xpub,
            )

    async def stop(self) -> None:
        """Cancel the loop, close the subscriber, terminate the context (idempotent)."""
        async with self._lock:
            await self._reap_unlocked()

    async def _reap_unlocked(self) -> None:
        """Tear down listener resources; caller MUST hold the lock.

        Captures resource references into locals BEFORE nulling the attributes
        so an overlapping start sees a clean slate.
        """
        self._running = False
        task = self._task
        subscriber = self._subscriber
        context = self._context
        self._task = None
        self._subscriber = None
        self._context = None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        if subscriber is not None:
            with contextlib.suppress(Exception):
                subscriber.close()
        if context is not None:
            with contextlib.suppress(Exception):
                context.term()

    async def _listen_loop(self) -> None:
        """Receive command frames and handle each; only cancellation unwinds it."""
        subscriber = self._subscriber
        if subscriber is None:
            return
        try:
            while self._running:
                payload = await self._recv_one(subscriber)
                if payload is None:
                    continue
                await self._handle_frame(payload)
        except asyncio.CancelledError:
            logger.info("ProcessCommandListener loop cancelled")
            raise

    async def _recv_one(self, subscriber: ValidatedSubscriber) -> str | None:
        """Receive and decode one command payload; None (after backoff) on error."""
        try:
            _topic, payload_bytes = await subscriber.recv_multipart()

            return payload_bytes.decode()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("ProcessCommandListener recv failed: {}", exc)
            await asyncio.sleep(_RECV_BACKOFF_S)

            return None

    async def _handle_frame(self, payload: str) -> None:
        """Verify, reconcile, and ack one command frame (all failures logged, not raised).

        Order: JSON shape -> HMAC signature (over the raw dict, so it matches the
        signer's ``model_dump(mode="json")`` byte-for-byte) -> schema -> signed
        ``coordinator`` slug -> freshness -> dedup. A forged / mis-addressed /
        stale command is dropped silently (no ack), so the API's blocking PATCH
        falls back to reconcile-pending. A duplicate is re-acked (idempotent) so
        a retried nudge still resolves without re-reconciling.

        Args:
            payload: The decoded JSON command payload from the bus.
        """
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError as exc:
            logger.warning("ProcessCommandListener: malformed JSON dropped: {}", exc)

            return
        if not isinstance(raw, dict):
            logger.warning("ProcessCommandListener: non-object payload dropped")

            return
        if not verify_command_payload(raw, self._key):
            logger.warning("ProcessCommandListener: signature verification failed, dropped")

            return
        try:
            command = ProcessCommandData.model_validate_json(payload)
        except Exception as exc:
            logger.warning("ProcessCommandListener: schema validation failed, dropped: {}", exc)

            return
        if command.coordinator != self._own_slug:
            logger.warning(
                "ProcessCommandListener: command for {} not us ({}), dropped",
                command.coordinator,
                self._own_slug,
            )

            return
        if not self._is_fresh(command.issued_at):
            logger.warning("ProcessCommandListener: stale command {} dropped", command.command_id)

            return
        if command.command_id in self._seen:
            await self._publish_ack(command, _ACK_APPLIED, "duplicate")

            return
        try:
            await self._launcher.reconcile_desired_state()
        except Exception as exc:
            logger.error(
                "ProcessCommandListener: reconcile failed for {}: {}", command.command_id, exc
            )
            await self._publish_ack(command, _ACK_REJECTED, str(exc))

            return
        self._remember(command.command_id)
        await self._publish_ack(command, _ACK_APPLIED, None)

    def _is_fresh(self, issued_at: datetime) -> bool:
        """Return whether ``issued_at`` is within the freshness window of now.

        A naive datetime is treated as UTC (the API always signs an aware UTC
        ISO string, but this keeps the comparison robust rather than raising).

        Args:
            issued_at: The command's issue time.

        Returns:
            True when within the freshness window.
        """
        moment = issued_at if issued_at.tzinfo is not None else issued_at.replace(tzinfo=UTC)

        return abs(datetime.now(UTC) - moment) <= _FRESHNESS_WINDOW

    def _remember(self, command_id: str) -> None:
        """Record a handled command id, evicting the oldest past the bound.

        Args:
            command_id: The command id to remember for dedup.
        """
        self._seen[command_id] = None
        while len(self._seen) > _SEEN_MAX:
            self._seen.popitem(last=False)

    async def _publish_ack(
        self, command: ProcessCommandData, status: str, detail: str | None
    ) -> None:
        """Publish a SIGNED ack for a handled command; best-effort (missing publisher no-ops).

        Args:
            command: The command being acknowledged.
            status: Outcome (``applied`` / ``rejected``).
            detail: Optional human-readable detail.
        """
        publisher = self._launcher.message_publisher
        if publisher is None:
            return
        try:
            tracker = publisher.tracker
            ack = ProcessCommandAckData(
                session_id=tracker.session_id,
                sequence_id=tracker.next_sequence(self._ack_topic),
                public_id=str(uuid7()),
                timestamp=datetime.now(UTC),
                command_id=command.command_id,
                coordinator=self._own_slug,
                process_name=command.process_name,
                status=status,
                detail=detail,
                signature="",
            )
            signed = ack.model_copy(
                update={"signature": sign_command_payload(ack.model_dump(mode="json"), self._key)}
            )
            await publisher.send(self._ack_topic, signed)
        except Exception as exc:
            logger.exception("ProcessCommandListener: ack publish failed: {}", exc)
