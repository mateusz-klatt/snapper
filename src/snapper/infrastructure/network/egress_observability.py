"""ZMQ egress pool snapshot publisher.

The egress pool is process-local, so route reservations owned by a feed
publisher subprocess are invisible to the API process unless each
pool-bearing process broadcasts a read-only snapshot. This module owns
the shared topic constant and the publisher loop used by both feed
publishers and the API process. It reuses the existing
``MessagePublisher`` PUB socket and heartbeat cadence rather than
opening a new transport path.
"""

import asyncio
import contextlib
import socket
from datetime import UTC
from datetime import datetime
from uuid import uuid7

from loguru import logger

from snapper.infrastructure.network.egress_pool import get_egress_pool
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.schemas.data import EgressPoolSnapshotEventData

EGRESS_SNAPSHOT_TOPIC = "system.egress.snapshot"
"""System topic carrying cross-process egress pool status snapshots."""

_MIN_INTERVAL_SECONDS = 0.001


def resolve_egress_container_id(role: str, hostname: str | None = None) -> str:
    """Build a stable human-readable egress snapshot source id.

    Args:
        role: Process role or launcher profile label.
        hostname: Optional host/container name override for tests.

    Returns:
        Source id in ``<role>@<hostname>`` form.
    """
    clean_role = role.strip() or "unknown"
    clean_host = (hostname or socket.gethostname()).strip() or "unknown"
    return f"{clean_role}@{clean_host}"


class EgressSnapshotPublisher:
    """Periodically publish this process's local egress pool snapshot."""

    def __init__(
        self,
        *,
        container: str,
        publisher: MessagePublisher,
        interval_seconds: float,
    ) -> None:
        """Create a publisher loop wrapper.

        Args:
            container: Stable process/container identity included in every payload.
            publisher: Existing ZMQ ``MessagePublisher`` for the process.
            interval_seconds: Cadence in seconds, normally the heartbeat interval.
        """
        self._container = container
        self._publisher = publisher
        self._interval_seconds = max(interval_seconds, _MIN_INTERVAL_SECONDS)
        self._task: asyncio.Task[None] | None = None
        self._running = False

    @property
    def container(self) -> str:
        """Return the source identity carried by this publisher.

        Returns:
            Stable process/container identity included in egress snapshots.
        """
        return self._container

    def start(self) -> None:
        """Start the background publishing task if it is not already alive."""
        if self._task is not None and not self._task.done():
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        """Cancel and await the background publishing task."""
        self._running = False
        task = self._task
        self._task = None
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task

    async def _run_loop(self) -> None:
        """Publish snapshots on the configured cadence until stopped."""
        try:
            while self._running:
                await asyncio.sleep(self._interval_seconds)
                if not self._running:
                    break
                try:
                    await self.publish_once()
                except Exception as exc:
                    logger.error("egress_snapshot: publish failed for {}: {}", self._container, exc)
        except asyncio.CancelledError:
            raise

    async def publish_once(self) -> bool:
        """Publish one snapshot if this process has an egress pool.

        Returns:
            True when a snapshot was sent, False when no local pool exists.
        """
        pool = get_egress_pool()
        if pool is None:
            return False
        topic = EGRESS_SNAPSHOT_TOPIC
        event = EgressPoolSnapshotEventData(
            public_id=str(uuid7()),
            timestamp=datetime.now(UTC),
            session_id=self._publisher.session_id,
            sequence_id=self._publisher.tracker.next_sequence(topic),
            container=self._container,
            snapshot=pool.status_snapshot(),
        )
        await self._publisher.send(topic, event)
        return True
