"""API-side consumer of cross-coordinator process-summary snapshots.

The dedicated feed container publishes ``processes.events.summary.coord-N``
snapshots roughly every five seconds. The API container runs the ``API``
autostart profile, so it never launches market-data publishers and its
local ``started_processes`` view reports every feed publisher as stopped
(the "Feeds Running 0/5" defect). This consumer subscribes to those
snapshots and keeps a TTL-bounded last-write-wins cache so the REST
process handlers can union remote running-state into ``/processes/summary``
and ``/processes/configured`` without owning a control plane.

The owning coordinator is taken from the fully validated ZMQ topic, not
the JSON ``coordinator`` field (which defaults to ``coord-0`` for older
producers and could otherwise be spoofed or misattributed). Each
coordinator publishes from a single socket, so ZMQ delivers its snapshots
in order and a plain last-received-wins update is correct — a producer
restart's fresh frames take effect immediately (no wall-clock ordering
guard that a backward clock step could strand). Freshness is measured
from local receipt, a liveness signal robust to cross-container clock
skew. The cache degrades cleanly: it is lost on API restart and
repopulates within one summary tick (~5s); a stale coordinator (no
snapshot within the TTL) is treated as fully stopped; and an absent cache
(startup failure) leaves the handlers reporting the local-only view they
reported before.
"""

import asyncio
import contextlib
import time
from collections.abc import Callable

import zmq
import zmq.asyncio
from loguru import logger

from snapper.messaging.infrastructure.validated_socket import HWM_AUDIT
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.data import ProcessSummaryEventData
from snapper.messaging.schemas.data import ProcessSummaryItem
from snapper.messaging.topics.validation import validate_topic

_SUMMARY_TOPIC_PREFIX = "processes.events.summary."
"""Prefix subscription matching every coordinator's summary stream."""

_DEFAULT_TTL_SECONDS = 15.0
"""Snapshots older than this are treated as stale (three missed ~5s ticks)."""

_RECV_BACKOFF_SECONDS = 1.0
"""Backoff after a transient ``recv_multipart`` failure."""


class _CoordinatorSnapshot:
    """One coordinator's most-recent summary, stamped with receipt time.

    Attributes:
        received_at: Monotonic clock reading when the snapshot arrived.
        processes: Per-process rows keyed by process name.
    """

    __slots__ = ("processes", "received_at")

    def __init__(self, received_at: float, processes: dict[str, ProcessSummaryItem]) -> None:
        """Store the snapshot rows and their receipt timestamp.

        Args:
            received_at: Monotonic clock reading at receipt.
            processes: Per-process rows keyed by process name.
        """
        self.received_at = received_at
        self.processes = processes


class RemoteSummaryCache:
    """TTL-bounded cache of other coordinators' process-summary snapshots.

    Subscribes to ``processes.events.summary.*`` on the broker XPUB and
    records each remote coordinator's latest snapshot. The API container
    queries :meth:`lookup` to union the feed container's live state into
    its REST responses.
    """

    def __init__(
        self,
        own_coordinator: str,
        ttl_seconds: float = _DEFAULT_TTL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        """Initialize an empty cache.

        Args:
            own_coordinator: This node's coordinator slug (e.g. ``coord-0``);
                snapshots whose topic carries this slug are ignored so the
                node never shadows its own authoritative local view.
            ttl_seconds: Age beyond which a coordinator's snapshot is stale.
            clock: Monotonic clock source, injectable for deterministic tests.
        """
        self._own_coordinator = own_coordinator
        self._ttl_seconds = ttl_seconds
        self._clock = clock
        self._snapshots: dict[str, _CoordinatorSnapshot] = {}
        self._zmq_context: zmq.asyncio.Context | None = None
        self._subscriber: ValidatedSubscriber | None = None
        self._listen_task: asyncio.Task[None] | None = None
        self._running = False
        self._listener_lock = asyncio.Lock()

    async def start(self, zmq_broker_xpub: str) -> None:
        """Open the SUB socket and spawn the listener task.

        Idempotent + restart-safe via :attr:`_listener_lock`: a second
        call while the listener is alive is a no-op, and a stale listener
        is reaped before a fresh one is built. An empty ``zmq_broker_xpub``
        skips socket setup entirely (test mode); the cache still answers
        queries against an empty snapshot set, degrading to the local-only
        view.

        Args:
            zmq_broker_xpub: Address of the broker's XPUB endpoint. Empty
                string skips the listener.
        """
        async with self._listener_lock:
            if self._listen_task is not None and not self._listen_task.done():
                return
            if self._listen_task is not None:
                await self._reap_unlocked()
            if not zmq_broker_xpub:
                logger.info("RemoteSummaryCache: empty broker XPUB, listener skipped")
                return
            try:
                self._zmq_context = zmq.asyncio.Context()
                raw_sub_socket = self._zmq_context.socket(zmq.SUB)
                self._subscriber = ValidatedSubscriber(raw_sub_socket)
                apply_hwm(raw_sub_socket, rcvhwm=HWM_AUDIT)
                raw_sub_socket.connect(zmq_broker_xpub)
                self._subscriber.subscribe(_SUMMARY_TOPIC_PREFIX)
            except Exception:
                await self._reap_unlocked()
                raise
            self._running = True
            self._listen_task = asyncio.create_task(self._listen_loop())
            logger.info(
                "RemoteSummaryCache: subscribed to {}* on {} (own={})",
                _SUMMARY_TOPIC_PREFIX,
                zmq_broker_xpub,
                self._own_coordinator,
            )

    async def stop(self) -> None:
        """Cancel the listener task and dispose ZMQ resources. Idempotent."""
        async with self._listener_lock:
            await self._reap_unlocked()

    async def _reap_unlocked(self) -> None:
        """Tear down listener resources. Caller MUST hold the listener lock."""
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
        """Consume summary frames and fold them into the cache until cancelled."""
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
            logger.info("RemoteSummaryCache: listener cancelled")
            raise

    async def _recv_one_frame(self, subscriber: ValidatedSubscriber) -> tuple[str, bytes] | None:
        """Receive one ``(topic, payload)`` frame; ``None`` on transient failure.

        Args:
            subscriber: The active validated subscriber.

        Returns:
            The decoded frame, or ``None`` after a logged transient error.

        Raises:
            asyncio.CancelledError: Propagated so shutdown can reap the task.
        """
        try:
            topic, payload = await subscriber.recv_multipart()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("RemoteSummaryCache: recv failed: {}", exc)
            await asyncio.sleep(_RECV_BACKOFF_SECONDS)
            return None
        return topic, payload

    def _ingest(self, topic: str, payload: bytes) -> None:
        """Parse one summary payload and store it under its topic coordinator.

        The coordinator is taken from the validated topic suffix, not the
        JSON ``coordinator`` field, so an older payload that omitted the
        field (defaulting to ``coord-0``) is still attributed correctly
        and a spoofed payload cannot claim a different owner. The full
        topic is validated (rejecting malformed multi-segment suffixes)
        before the coordinator is derived, so a forged
        ``...summary.coord-0.extra`` cannot bypass the self-skip and shadow
        the local view. Snapshots whose topic carries this node's own slug
        are dropped so the local authoritative view always wins for
        self-owned processes. Storage is last-received-wins: a single
        coordinator publishes in order, so the newest frame (including a
        producer restart's) simply replaces the prior one.

        Args:
            topic: ZMQ topic of the form ``processes.events.summary.<slug>``.
            payload: Raw JSON bytes of a ``ProcessSummaryEventData`` frame.
        """
        if not topic.startswith(_SUMMARY_TOPIC_PREFIX):
            return
        is_valid, _error = validate_topic(topic)
        if not is_valid:
            return
        coordinator = topic[len(_SUMMARY_TOPIC_PREFIX) :]
        if coordinator == self._own_coordinator:
            return
        try:
            event = ProcessSummaryEventData.from_json(payload.decode("utf-8"))
        except Exception as exc:
            logger.error("RemoteSummaryCache: payload parse failed for {}: {}", topic, exc)
            return
        rows = {item.name: item for item in event.processes}
        self._snapshots[coordinator] = _CoordinatorSnapshot(
            received_at=self._clock(),
            processes=rows,
        )

    def _is_fresh(self, snapshot: _CoordinatorSnapshot) -> bool:
        """Return whether ``snapshot`` is within the freshness TTL.

        Args:
            snapshot: A stored coordinator snapshot.

        Returns:
            True when the snapshot arrived within ``ttl_seconds``.
        """
        return self._clock() - snapshot.received_at <= self._ttl_seconds

    def lookup(self, name: str) -> tuple[bool, str | None]:
        """Return ``(running, coordinator)`` for ``name`` from one fresh snapshot.

        Resolving both facts in a single scan keeps them consistent when
        several coordinators happen to list the same process: a fresh
        snapshot reporting it running wins; otherwise the most recently
        received fresh coordinator that merely lists it is returned as the
        owner (deterministic regardless of dict iteration order).

        Args:
            name: Process name to look up.

        Returns:
            ``(running, coordinator)`` where ``running`` is True when a fresh
            remote snapshot reports the process running, and ``coordinator``
            is the owning slug (``None`` when no fresh snapshot lists it).
        """
        owner: str | None = None
        owner_received_at = float("-inf")
        for coordinator, snapshot in self._snapshots.items():
            if not self._is_fresh(snapshot):
                continue
            item = snapshot.processes.get(name)
            if item is None:
                continue
            if item.running:
                return True, coordinator
            if snapshot.received_at > owner_received_at:
                owner = coordinator
                owner_received_at = snapshot.received_at
        return False, owner

    def is_running(self, name: str) -> bool:
        """Return whether a fresh remote snapshot reports ``name`` running.

        Args:
            name: Process name to look up.

        Returns:
            True when any non-stale remote coordinator reports the process
            with ``running=True``.
        """
        return self.lookup(name)[0]

    def coordinator_for(self, name: str) -> str | None:
        """Return the slug of a fresh remote coordinator tracking ``name``.

        Args:
            name: Process name to look up.

        Returns:
            The coordinator slug owning a fresh snapshot that lists the
            process, or ``None`` when no fresh coordinator tracks it.
        """
        return self.lookup(name)[1]

    def has_fresh_snapshot(self) -> bool:
        """Return whether any remote coordinator snapshot is currently fresh.

        A fresh snapshot from ANY coordinator proves the broker is
        forwarding: every ``processes.events.summary`` frame physically
        transits the broker's XSUB/XPUB proxy. The dedicated broker
        container publishes no summary of its own, so this transitive
        signal is the API's only liveness evidence for it short of the
        docker TCP healthcheck, which the API cannot observe. The
        false-negative window is narrow: only when EVERY remote
        coordinator is simultaneously silent (itself an outage) while the
        broker stays up.

        Returns:
            True when at least one non-stale coordinator snapshot exists.
        """
        return any(self._is_fresh(snapshot) for snapshot in self._snapshots.values())
