"""API-side registry that resolves signed command acks to pending PATCH futures (P2.4).

The API coordinator publishes an HMAC-signed :class:`ProcessCommandData` nudge after a
desired-state PATCH and blocks on the owning coordinator's ack. This registry runs one
listener subscribed to ``processes.events.command_ack.*`` (acks from every coordinator);
the PATCH handler registers a future keyed by ``command_id`` before publishing, awaits
it to a short timeout, then unregisters. Each received ack is signature-verified before
its future is resolved, so the blocking PATCH cannot be resolved by a spoofed success
frame. A missing/late ack simply times out and the PATCH falls back to reconcile-pending
— the periodic reconcile (P0.3) converges regardless, so the ack is a latency optimizer,
never a correctness dependency.
"""

import asyncio
import contextlib
import json
from datetime import UTC
from datetime import datetime
from uuid import uuid7

import zmq
import zmq.asyncio
from loguru import logger

from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.validated_socket import HWM_AUDIT
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.data import ProcessCommandAckData
from snapper.messaging.schemas.data import ProcessCommandData
from snapper.messaging.security.command_signing import sign_command_payload
from snapper.messaging.security.command_signing import verify_command_payload

_ACK_TOPIC_PREFIX = "processes.events.command_ack."
_COMMANDS_STREAM = "processes.commands"
_RECV_BACKOFF_S = 0.1
_MAX_PENDING = 1024
_ACK_TIMEOUT_S = 4.0


class ProcessCommandAckRegistry:
    """Listens for signed command acks and resolves the matching pending future."""

    def __init__(self, signing_key: bytes, *, ack_timeout_s: float = _ACK_TIMEOUT_S) -> None:
        """Initialize an empty registry.

        Args:
            signing_key: The control-plane HMAC key (from
                :func:`command_signing_key`), used to verify each ack before
                resolving its future.
            ack_timeout_s: Seconds to wait for a coordinator ack before the
                caller falls back to reconcile-pending.
        """
        self._key = signing_key
        self._ack_timeout_s = ack_timeout_s
        self._pending: dict[str, tuple[asyncio.Future[ProcessCommandAckData], str, str]] = {}
        self._zmq_context: zmq.asyncio.Context | None = None
        self._subscriber: ValidatedSubscriber | None = None
        self._listen_task: asyncio.Task[None] | None = None
        self._running = False
        self._lock = asyncio.Lock()

    def register(
        self, command_id: str, *, coordinator: str, process_name: str
    ) -> asyncio.Future[ProcessCommandAckData] | None:
        """Register a future to be resolved by ``command_id``'s ack.

        The expected coordinator and process name are pinned alongside the
        future so :meth:`_resolve_ack` can reject an ack whose identity
        fields do not match the nudge that minted the id — the registry
        subscribes to EVERY coordinator's ack topic, and a bare
        ``command_id`` match would let any signed ack frame resolve any
        pending PATCH.

        Args:
            command_id: The id of the command about to be published.
            coordinator: The slug of the coordinator the nudge targets.
            process_name: The process the nudge controls.

        Returns:
            A future the caller awaits, or ``None`` when the registry is
            saturated (the caller then skips the nudge and reconcile-pends).
        """
        if len(self._pending) >= _MAX_PENDING:
            logger.warning(
                "ProcessCommandAckRegistry saturated ({} pending); nudge will not be acked",
                _MAX_PENDING,
            )

            return None
        future: asyncio.Future[ProcessCommandAckData] = asyncio.get_running_loop().create_future()
        self._pending[command_id] = (future, coordinator, process_name)

        return future

    def unregister(self, command_id: str) -> None:
        """Drop a pending future (call in a finally after awaiting it).

        Args:
            command_id: The command id whose future to discard.
        """
        self._pending.pop(command_id, None)

    async def nudge(
        self,
        publisher: MessagePublisher | None,
        *,
        coordinator: str,
        process_name: str,
        action: str,
        issued_by: str,
    ) -> ProcessCommandAckData | None:
        """Publish a signed nudge to a coordinator and await its ack to a timeout.

        Registers a future, publishes a signed :class:`ProcessCommandData` on
        ``processes.commands.{coordinator}``, and blocks on the coordinator's
        signed ack up to the configured ack timeout. Returns the ack, or
        ``None`` when there is no publisher, the registry is saturated, the
        publish fails, or the ack does not arrive in time — in EVERY ``None``
        case the caller falls back to reconcile-pending (the periodic reconcile
        converges regardless, so the nudge is a latency optimizer, never a
        correctness dependency).

        Args:
            publisher: The wired bus publisher, or ``None``.
            coordinator: The owning coordinator's slug.
            process_name: The process being controlled.
            action: The desired-state action (advisory; the coordinator
                reconciles from the DB, not from this field).
            issued_by: Issuer label for audit.

        Returns:
            The coordinator's ack, or ``None`` on any failure / timeout.
        """
        if publisher is None:
            return None
        command_id = str(uuid7())
        future = self.register(command_id, coordinator=coordinator, process_name=process_name)
        if future is None:
            return None
        try:
            await self._publish_command(
                publisher, command_id, coordinator, process_name, action, issued_by
            )

            async with asyncio.timeout(self._ack_timeout_s):
                return await future
        except TimeoutError:
            return None
        except Exception as exc:
            logger.exception("ProcessCommandAckRegistry: nudge publish failed: {}", exc)

            return None
        finally:
            self.unregister(command_id)

    async def _publish_command(
        self,
        publisher: MessagePublisher,
        command_id: str,
        coordinator: str,
        process_name: str,
        action: str,
        issued_by: str,
    ) -> None:
        """Build, sign, and publish a ``ProcessCommandData`` nudge.

        Args:
            publisher: The wired bus publisher.
            command_id: The unique command id (also the future key).
            coordinator: The owning coordinator's slug (topic + signed field).
            process_name: The process being controlled.
            action: The desired-state action.
            issued_by: Issuer label.
        """
        topic = f"{_COMMANDS_STREAM}.{coordinator}"
        tracker = publisher.tracker
        command = ProcessCommandData(
            session_id=tracker.session_id,
            sequence_id=tracker.next_sequence(topic),
            public_id=str(uuid7()),
            timestamp=datetime.now(UTC),
            command_id=command_id,
            coordinator=coordinator,
            process_name=process_name,
            action=action,
            issued_by=issued_by,
            issued_at=datetime.now(UTC),
            signature="",
        )
        signed = command.model_copy(
            update={"signature": sign_command_payload(command.model_dump(mode="json"), self._key)}
        )
        await publisher.send(topic, signed)

    async def start(self, zmq_broker_xpub: str) -> None:
        """Subscribe to the ack topics and spawn the listen loop (idempotent).

        Args:
            zmq_broker_xpub: The broker XPUB endpoint; an empty string skips the
                listener (the registry then never resolves, so PATCHes always
                time out to reconcile-pending — the safe degrade).
        """
        async with self._lock:
            if self._listen_task is not None and not self._listen_task.done():
                return
            if self._listen_task is not None:
                await self._reap_unlocked()
            if not zmq_broker_xpub:
                logger.info("ProcessCommandAckRegistry: empty XPUB, listener skipped")
                return
            try:
                self._zmq_context = zmq.asyncio.Context()
                raw_socket = self._zmq_context.socket(zmq.SUB)
                self._subscriber = ValidatedSubscriber(raw_socket)
                apply_hwm(raw_socket, rcvhwm=HWM_AUDIT)
                raw_socket.connect(zmq_broker_xpub)
                self._subscriber.subscribe(_ACK_TOPIC_PREFIX)
            except Exception:
                await self._reap_unlocked()
                raise
            self._running = True
            self._listen_task = asyncio.create_task(self._listen_loop())
            logger.info(
                "ProcessCommandAckRegistry subscribed to {}* on {}",
                _ACK_TOPIC_PREFIX,
                zmq_broker_xpub,
            )

    async def stop(self) -> None:
        """Cancel the loop, close the subscriber, terminate the context (idempotent)."""
        async with self._lock:
            await self._reap_unlocked()

    async def _reap_unlocked(self) -> None:
        """Tear down listener resources; caller MUST hold the lock."""
        self._running = False
        task = self._listen_task
        subscriber = self._subscriber
        context = self._zmq_context
        self._listen_task = None
        self._subscriber = None
        self._zmq_context = None
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
        """Consume ack frames and resolve pending futures until cancelled."""
        subscriber = self._subscriber
        if subscriber is None:
            return
        try:
            while self._running:
                payload = await self._recv_one(subscriber)
                if payload is None:
                    continue
                self._resolve_ack(payload)
        except asyncio.CancelledError:
            logger.info("ProcessCommandAckRegistry loop cancelled")
            raise

    async def _recv_one(self, subscriber: ValidatedSubscriber) -> str | None:
        """Receive and decode one ack payload; None (after backoff) on error."""
        try:
            _topic, payload_bytes = await subscriber.recv_multipart()

            return payload_bytes.decode()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("ProcessCommandAckRegistry recv failed: {}", exc)
            await asyncio.sleep(_RECV_BACKOFF_S)

            return None

    def _resolve_ack(self, payload: str) -> None:
        """Verify one ack and resolve its pending future, if any.

        A malformed / non-object / bad-signature / schema-invalid ack is
        dropped (the PATCH times out to reconcile-pending). An ack for an
        unknown or already-resolved command id is ignored. An ack whose
        ``coordinator`` / ``process_name`` do not match the values pinned
        at :meth:`register` time is dropped with a warning — a signed ack
        can only resolve the exact nudge that minted its command id.

        Args:
            payload: The decoded ack JSON payload.
        """
        try:
            raw = json.loads(payload)
        except json.JSONDecodeError:
            return
        if not isinstance(raw, dict):
            return
        if not verify_command_payload(raw, self._key):
            logger.warning("ProcessCommandAckRegistry: ack signature verification failed, dropped")

            return
        try:
            ack = ProcessCommandAckData.model_validate_json(payload)
        except Exception:
            return
        entry = self._pending.get(ack.command_id)
        if entry is None:
            return
        future, coordinator, process_name = entry
        if ack.coordinator != coordinator or ack.process_name != process_name:
            logger.warning(
                "ProcessCommandAckRegistry: ack '{}' identity mismatch "
                "({}/{} != expected {}/{}), dropped",
                ack.command_id,
                ack.coordinator,
                ack.process_name,
                coordinator,
                process_name,
            )

            return
        if not future.done():
            future.set_result(ack)
