"""AI-delegate liveness and response watchdog.

The API lifespan polls durable delegate and review state, then publishes
synthetic heartbeats on ``system.heartbeats.ai_delegate.global``. A
WARNING burst reuses the existing ``CriticalSystemErrorRule`` gate,
hourly deduplication, permission-based fan-out, localization, and APNs
delivery path. No alert type or wire-payload variant is needed.

Two independent signals close the incident blind spots:

* no active AI-delegate user with an active operator membership and
  scope grant has a heartbeat inside the same strict liveness window
  used by ``AiReviewService.create_review``;
* at least one review reached ``timeout_no_response`` inside the recent
  timeout window, which catches a connected transport whose delegate is
  occupied or otherwise fails to answer.

The scope is deliberately ``global`` because Snapper is a
single-operator self-hosted system and there is no durable registry of
strategy review tuples to monitor before a review row exists. The check
starts after one poll interval so a healthy delegate can reconnect after
an API restart. It is then level-triggered: WARNING ticks publish three
frames so the existing critical rule opens immediately, while HEALTHY
ticks publish one frame that clears its rolling warning state.
"""

import asyncio
import contextlib
import logging
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Final
from typing import Protocol
from uuid import uuid7

from snapper.application.ai_review.service import DEFAULT_HEARTBEAT_WINDOW_SECONDS
from snapper.core.json_types import JsonObject
from snapper.core.types import HealthStatusEnum
from snapper.data.repository import Repository
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.topics.builders import heartbeat_topic

logger = logging.getLogger(__name__)

DEFAULT_INTERVAL_SECONDS: Final = 60.0
"""One DB check per minute keeps the idle cost negligible while still
paging within one operational minute of a detectable failure."""

DEFAULT_STARTUP_GRACE_SECONDS: Final = DEFAULT_INTERVAL_SECONDS
"""One poll interval lets a healthy delegate reconnect after API startup
before the first strict liveness evaluation can page the operator."""

LIVENESS_WINDOW_SECONDS: Final = DEFAULT_HEARTBEAT_WINDOW_SECONDS
"""The 15-second admission-control window imported from the service so
watchdog liveness cannot drift from ``create_review`` liveness."""

DEFAULT_TIMEOUT_WINDOW_SECONDS: Final = 3600
"""Keep a missed review visible for one hour so restarts cannot erase
the signal before the existing rule's rolling hourly page gate sees it."""

DEFAULT_TIMEOUT_COUNT_THRESHOLD: Final = 1
"""One unanswered review is alert-worthy because a LIVE timeout vetoes
an entire trading window; the existing hourly cooldown controls noise."""

WATCHDOG_HEARTBEAT_COMPONENT: Final = "ai_delegate"
WATCHDOG_SCOPE: Final = "global"

_BURST_FRAME_COUNT: Final = 3
"""The existing critical-system-error rule opens after three warnings."""

_BURST_FRAME_SPACING_SECONDS: Final = 2.0
"""Spacing preserves each warning frame for WebSocket observers while
the notify sidecar receives all three directly from ZMQ."""


class _HeartbeatSequenceTracker(Protocol):
    """Subset of ``SequenceTracker`` needed for heartbeat provenance."""

    @property
    def session_id(self) -> str:
        """Return the publisher session identifier."""

    def next_sequence(self, stream: str) -> int:
        """Return the next transport sequence for a topic.

        Args:
            stream: Topic whose transport sequence advances.

        Returns:
            Next monotonically increasing sequence.
        """


class _HeartbeatPublisher(Protocol):
    """Subset of ``MessagePublisher`` needed by the watchdog."""

    @property
    def tracker(self) -> _HeartbeatSequenceTracker:
        """Return the publisher's shared sequence tracker."""

    async def send(self, stream_key: str, data: HeartbeatData) -> None:
        """Send one complete heartbeat frame.

        Args:
            stream_key: ZMQ heartbeat topic.
            data: Complete heartbeat envelope.
        """


class AiDelegateWatchdog:
    """Publish AI-delegate health from liveness and timeout DB reads.

    ``start`` spawns one owned task that waits through a reconnect grace,
    performs a guarded initial tick, and enters the periodic loop. API
    readiness therefore never waits for either the grace or warning-burst
    spacing. Every tick is defensive so a transient DB or publisher error
    is logged and retried rather than silently killing the monitor.
    """

    def __init__(
        self,
        *,
        repo: Repository,
        msg_publisher: _HeartbeatPublisher | None = None,
        interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
        startup_grace_seconds: float = DEFAULT_STARTUP_GRACE_SECONDS,
        timeout_window_seconds: int = DEFAULT_TIMEOUT_WINDOW_SECONDS,
        timeout_count_threshold: int = DEFAULT_TIMEOUT_COUNT_THRESHOLD,
    ) -> None:
        """Wire dependencies and thresholds.

        Args:
            repo: Repository used for delegate-liveness and timeout reads.
            msg_publisher: Shared ZMQ publisher for synthetic heartbeats;
                ``None`` retains detection and logging without transport.
            interval_seconds: Poll cadence override for tests.
            startup_grace_seconds: Delay before the initial liveness read.
            timeout_window_seconds: Recent timeout lookback in seconds.
            timeout_count_threshold: Count that changes status to WARNING.
        """
        self._repo = repo
        self._msg_publisher = msg_publisher
        self._interval_seconds = interval_seconds
        self._startup_grace_seconds = startup_grace_seconds
        self._timeout_window_seconds = timeout_window_seconds
        self._timeout_count_threshold = timeout_count_threshold
        self._sequence = 0
        self._stopping = asyncio.Event()
        self._loop_task: asyncio.Task[None] | None = None

    @property
    def interval_seconds(self) -> float:
        """Return the configured poll cadence.

        Returns:
            Seconds between completed loop ticks.
        """
        return self._interval_seconds

    def start(self) -> None:
        """Spawn the owned task that performs the delayed initial tick."""
        self._stopping.clear()
        self._loop_task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Signal, cancel, and await the poll loop when it exists."""
        self._stopping.set()
        task = self._loop_task
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._loop_task = None

    async def _run(self) -> None:
        """Allow startup reconnection, evaluate, then enter the poll loop."""
        await asyncio.sleep(self._startup_grace_seconds)
        await self._guarded_tick()
        await self._loop()

    async def _loop(self) -> None:
        """Sleep, tick, and repeat until shutdown is signalled."""
        while not self._stopping.is_set():
            await asyncio.sleep(self._interval_seconds)
            if self._stopping.is_set():
                return
            await self._guarded_tick()

    async def _guarded_tick(self) -> None:
        """Run one tick and keep the watchdog alive after failures."""
        try:
            await self._tick()
        except Exception:
            logger.exception("AiDelegateWatchdog tick failed; continuing on next tick")

    async def _tick(self, now: datetime | None = None) -> None:
        """Evaluate both failure signals and publish one scope status.

        Args:
            now: Reference instant override for deterministic tests;
                ``None`` uses ``datetime.now(UTC)``.
        """
        reference_now = now if now is not None else datetime.now(UTC)
        has_live_delegate = await self._repo.has_live_ai_delegate(
            heartbeat_window_seconds=LIVENESS_WINDOW_SECONDS,
            as_of=reference_now,
        )
        timeout_since = reference_now - timedelta(seconds=self._timeout_window_seconds)
        timeout_count = await self._repo.count_ai_review_timeouts_since(
            since=timeout_since,
            as_of=reference_now,
        )
        no_live_delegate = not has_live_delegate
        timeout_threshold_reached = timeout_count >= self._timeout_count_threshold
        if no_live_delegate and timeout_threshold_reached:
            reason = "no_live_delegate_and_timeout_no_response"
        elif no_live_delegate:
            reason = "no_live_delegate"
        elif timeout_threshold_reached:
            reason = "timeout_no_response"
        else:
            reason = "healthy"
        status = (
            HealthStatusEnum.WARNING
            if no_live_delegate or timeout_threshold_reached
            else HealthStatusEnum.HEALTHY
        )
        if status == HealthStatusEnum.WARNING:
            logger.warning(
                "AiDelegateWatchdog: %s (recent timeouts %d, threshold %d)",
                reason,
                timeout_count,
                self._timeout_count_threshold,
            )
        await self._publish_status(
            status=status,
            reason=reason,
            has_live_delegate=has_live_delegate,
            timeout_count=timeout_count,
            evaluated_at=reference_now,
        )

    async def _publish_status(
        self,
        *,
        status: HealthStatusEnum,
        reason: str,
        has_live_delegate: bool,
        timeout_count: int,
        evaluated_at: datetime,
    ) -> None:
        """Publish one HEALTHY frame or a three-frame WARNING burst.

        Per-frame failures are logged and swallowed. A later frame or
        tick can therefore still reach the existing critical alert rule.

        Args:
            status: Synthetic health status selected by the DB checks.
            reason: Stable reason identifier stored in heartbeat metadata.
            has_live_delegate: Result of the strict liveness query.
            timeout_count: Matching timeout rows in the lookback window.
            evaluated_at: Reference instant used for both DB reads.
        """
        publisher = self._msg_publisher
        if publisher is None:
            return
        topic = heartbeat_topic(WATCHDOG_HEARTBEAT_COMPONENT, WATCHDOG_SCOPE)
        frame_count = _BURST_FRAME_COUNT if status == HealthStatusEnum.WARNING else 1
        for frame_index in range(frame_count):
            if frame_index:
                await asyncio.sleep(_BURST_FRAME_SPACING_SECONDS)
            try:
                tracker = publisher.tracker
                self._sequence += 1
                meta: JsonObject = {
                    "synthetic": True,
                    "origin": "ai_delegate_watchdog",
                    "scope": WATCHDOG_SCOPE,
                    "reason": reason,
                    "has_live_delegate": has_live_delegate,
                    "heartbeat_window_seconds": LIVENESS_WINDOW_SECONDS,
                    "timeout_count": timeout_count,
                    "timeout_count_threshold": self._timeout_count_threshold,
                    "timeout_window_seconds": self._timeout_window_seconds,
                    "evaluated_at": evaluated_at.isoformat(),
                }
                frame = HeartbeatData(
                    public_id=str(uuid7()),
                    timestamp=datetime.now(UTC),
                    session_id=tracker.session_id,
                    sequence_id=tracker.next_sequence(topic),
                    component=f"{WATCHDOG_HEARTBEAT_COMPONENT}.{WATCHDOG_SCOPE}",
                    sequence=self._sequence,
                    status=status,
                    lag_ms=0,
                    meta=meta,
                )
                await publisher.send(topic, frame)
            except Exception:
                logger.exception("AiDelegateWatchdog heartbeat publish failed")
