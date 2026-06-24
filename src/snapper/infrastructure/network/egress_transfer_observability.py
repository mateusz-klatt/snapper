"""ZMQ WireGuard transfer publisher for snapper-egress.

The sidecar can read tunnel interface counters through ``wg show`` and
publishes those read-only samples over the existing audit PUB socket.
The API process consumes the samples and joins them to egress routes by
SOCKS5 listener port.
"""

import asyncio
import contextlib
from datetime import UTC
from datetime import datetime
from uuid import uuid7

from loguru import logger

from snapper.infrastructure.network.egress_transfer_stats import EgressTransferSampler
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.schemas.data import EgressTransferEventData

EGRESS_TRANSFER_TOPIC = "system.egress.transfer"
"""System topic carrying sidecar WireGuard transfer samples."""

_MIN_INTERVAL_SECONDS = 0.001


class EgressTransferPublisher:
    """Periodically publish sidecar WireGuard transfer samples."""

    def __init__(
        self,
        *,
        sampler: EgressTransferSampler,
        publisher: MessagePublisher,
        interval_seconds: float,
    ) -> None:
        """Create a publisher loop wrapper.

        Args:
            sampler: Transfer sampler backed by ``wg show``.
            publisher: Existing ZMQ ``MessagePublisher`` for the sidecar.
            interval_seconds: Cadence in seconds, normally the heartbeat interval.
        """
        self._sampler = sampler
        self._publisher = publisher
        self._interval_seconds = max(interval_seconds, _MIN_INTERVAL_SECONDS)
        self._task: asyncio.Task[None] | None = None
        self._running = False

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
        """Publish transfer samples on the configured cadence until stopped."""
        while self._running:
            await asyncio.sleep(self._interval_seconds)
            if not self._running:
                break
            try:
                await self.publish_once()
            except Exception as exc:
                logger.error("egress_transfer: publish failed: {}", exc)

    async def publish_once(self) -> bool:
        """Publish one transfer event if the sampler has tunnel rows.

        Returns:
            True when a transfer event was sent, False when no tunnel
            rows are configured.
        """
        interfaces = self._sampler.sample()
        if not interfaces:
            return False
        topic = EGRESS_TRANSFER_TOPIC
        event = EgressTransferEventData(
            public_id=str(uuid7()),
            timestamp=datetime.now(UTC),
            session_id=self._publisher.session_id,
            sequence_id=self._publisher.tracker.next_sequence(topic),
            interfaces=interfaces,
        )
        await self._publisher.send(topic, event)
        return True
