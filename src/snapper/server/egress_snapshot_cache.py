"""API-side cache for cross-process egress pool snapshots.

Feed publishers own process-local egress pools, so their active
reservations never appear in the API process's singleton. This consumer
subscribes to ``system.egress.snapshot`` and keeps the latest read-only
snapshot per reporting process. Freshness is based on local receive time
so container clock skew cannot make a live publisher look stale or fresh
incorrectly.
"""

import asyncio
import contextlib
import time
from collections.abc import Callable
from dataclasses import dataclass

import zmq
import zmq.asyncio
from loguru import logger

from snapper.infrastructure.network.egress_models import EgressPoolStatusSnapshot
from snapper.infrastructure.network.egress_observability import EGRESS_SNAPSHOT_TOPIC
from snapper.messaging.infrastructure.validated_socket import HWM_AUDIT
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.data import EgressPoolSnapshotEventData
from snapper.messaging.topics.validation import validate_topic

_RECV_BACKOFF_SECONDS = 1.0


class _StoredEgressSnapshot:
    """One container's latest snapshot and local receipt timestamp."""

    __slots__ = ("received_at", "snapshot")

    def __init__(self, received_at: float, snapshot: EgressPoolStatusSnapshot) -> None:
        """Store a snapshot with its monotonic receive timestamp.

        Args:
            received_at: Monotonic clock value at receipt.
            snapshot: Parsed egress pool status snapshot.
        """
        self.received_at = received_at
        self.snapshot = snapshot


@dataclass(frozen=True, slots=True)
class EgressCachedSnapshot:
    """Public read model for a cached remote egress snapshot.

    Attributes:
        container: Reporting process/container id.
        snapshot: Latest egress pool snapshot from that source.
        age_seconds: Seconds since this API process received the frame.
        stale: True when age exceeds the configured stale threshold.
    """

    container: str
    snapshot: EgressPoolStatusSnapshot
    age_seconds: float
    stale: bool


class EgressSnapshotCache:
    """Receive and cache egress pool snapshots from other processes."""

    def __init__(
        self,
        *,
        own_container: str,
        stale_after_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Initialize an empty egress snapshot cache.

        Args:
            own_container: Local source id to ignore on ingest.
            stale_after_seconds: Age after which a cached snapshot is stale.
            clock: Monotonic clock source for deterministic tests.
        """
        self._own_container = own_container
        self._stale_after_seconds = stale_after_seconds
        self._clock = clock
        self._snapshots: dict[str, _StoredEgressSnapshot] = {}
        self._zmq_context: zmq.asyncio.Context | None = None
        self._subscriber: ValidatedSubscriber | None = None
        self._listen_task: asyncio.Task[None] | None = None
        self._running = False
        self._listener_lock = asyncio.Lock()

    @property
    def stale_after_seconds(self) -> float:
        """Return the configured stale threshold in seconds.

        Returns:
            Age threshold after which cached remote snapshots are stale.
        """
        return self._stale_after_seconds

    async def start(self, zmq_broker_xpub: str) -> None:
        """Open the SUB socket and spawn the listener task.

        Args:
            zmq_broker_xpub: Broker XPUB endpoint. Empty string skips
                socket setup and leaves the cache empty.
        """
        async with self._listener_lock:
            if self._listen_task is not None and not self._listen_task.done():
                return
            if self._listen_task is not None:
                await self._reap_unlocked()
            if not zmq_broker_xpub:
                logger.info("EgressSnapshotCache: empty broker XPUB, listener skipped")
                return
            try:
                self._zmq_context = zmq.asyncio.Context()
                raw_sub_socket = self._zmq_context.socket(zmq.SUB)
                self._subscriber = ValidatedSubscriber(raw_sub_socket)
                apply_hwm(raw_sub_socket, rcvhwm=HWM_AUDIT)
                raw_sub_socket.connect(zmq_broker_xpub)
                self._subscriber.subscribe(EGRESS_SNAPSHOT_TOPIC)
            except Exception:
                await self._reap_unlocked()
                raise
            self._running = True
            self._listen_task = asyncio.create_task(self._listen_loop())
            logger.info(
                "EgressSnapshotCache: subscribed to {} on {} (own={})",
                EGRESS_SNAPSHOT_TOPIC,
                zmq_broker_xpub,
                self._own_container,
            )

    async def stop(self) -> None:
        """Cancel the listener task and dispose ZMQ resources."""
        async with self._listener_lock:
            await self._reap_unlocked()

    async def _reap_unlocked(self) -> None:
        """Tear down listener resources. Caller holds the listener lock."""
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
        """Consume egress snapshot frames until cancelled."""
        subscriber = self._subscriber
        if subscriber is None:
            return
        try:
            while self._running:
                frame = await self._recv_one_frame(subscriber)
                if frame is None:
                    continue
                topic, payload = frame
                self._ingest(topic, payload)
        except asyncio.CancelledError:
            logger.info("EgressSnapshotCache: listener cancelled")
            raise

    async def _recv_one_frame(self, subscriber: ValidatedSubscriber) -> tuple[str, bytes] | None:
        """Receive one ZMQ frame, returning ``None`` on transient failure.

        Args:
            subscriber: Active validated subscriber.

        Returns:
            Decoded topic and payload bytes, or ``None`` after a logged
            transient receive failure.
        """
        try:
            topic, payload = await subscriber.recv_multipart()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("EgressSnapshotCache: recv failed: {}", exc)
            await asyncio.sleep(_RECV_BACKOFF_SECONDS)
            return None
        return topic, payload

    def _ingest(self, topic: str, payload: bytes) -> None:
        """Parse and store one egress snapshot frame.

        Args:
            topic: ZMQ topic, expected to be ``system.egress.snapshot``.
            payload: Raw JSON payload bytes.
        """
        if topic != EGRESS_SNAPSHOT_TOPIC:
            return
        is_valid, _error = validate_topic(topic)
        if not is_valid:
            return
        try:
            event = EgressPoolSnapshotEventData.from_json(payload.decode("utf-8"))
        except Exception as exc:
            logger.error("EgressSnapshotCache: payload parse failed for {}: {}", topic, exc)
            return
        if event.container == self._own_container:
            return
        self._snapshots[event.container] = _StoredEgressSnapshot(
            received_at=self._clock(),
            snapshot=event.snapshot,
        )

    def latest_snapshots(self) -> list[EgressCachedSnapshot]:
        """Return cached snapshots sorted by container id.

        Returns:
            Cached snapshots with receive-age and stale flag computed
            from the current local monotonic clock.
        """
        now = self._clock()
        entries: list[EgressCachedSnapshot] = []
        for container, stored in self._snapshots.items():
            age_seconds = max(0.0, now - stored.received_at)
            entries.append(
                EgressCachedSnapshot(
                    container=container,
                    snapshot=stored.snapshot,
                    age_seconds=age_seconds,
                    stale=age_seconds > self._stale_after_seconds,
                )
            )
        return sorted(entries, key=lambda item: item.container)
