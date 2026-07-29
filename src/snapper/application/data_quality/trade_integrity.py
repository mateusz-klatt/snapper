"""Scheduled alert bridge for incremental trade-integrity monitors.

The repository owns cursor durability, bounded sweep and worklog
draining, and finding latching. This watchdog owns only recurrence,
per-monitor timeout isolation, and publication through the existing
critical-system-error heartbeat path.

M1 always runs before M2. Each repository call has its own timeout and
failure guard, so one unavailable invariant cannot suppress the other.
A clean result emits one HEALTHY frame to reset the alert rule's
rolling state. Findings or excessive completed-coverage lag emit three
WARNING frames spaced two seconds apart, satisfying the existing
three-consecutive-warning gate.
"""

import asyncio
import contextlib
import logging
import os
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from math import isfinite
from typing import Final
from typing import Protocol
from uuid import uuid7

from snapper.core.json_types import JsonArray
from snapper.core.json_types import JsonObject
from snapper.core.types import HealthStatusEnum
from snapper.data.repository_types import TradeIntegrityMonitor
from snapper.data.repository_types import TradeIntegrityRunResult
from snapper.data.trade_integrity import TRADE_INTEGRITY_SWEEP_LIMIT
from snapper.data.trade_integrity import TRADE_INTEGRITY_WORKLOG_LIMIT
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.topics.builders import heartbeat_topic

logger = logging.getLogger(__name__)

DEFAULT_INTERVAL_SECONDS: Final = 60.0
MIN_INTERVAL_SECONDS: Final = 30.0
MAX_INTERVAL_SECONDS: Final = 60.0
DEFAULT_STARTUP_GRACE_SECONDS: Final = 0.0
DEFAULT_SWEEP_LIMIT: Final = TRADE_INTEGRITY_SWEEP_LIMIT
DEFAULT_WORKLOG_LIMIT: Final = TRADE_INTEGRITY_WORKLOG_LIMIT
DEFAULT_MONITOR_TIMEOUT_SECONDS: Final = 30.0

WATCHDOG_HEARTBEAT_COMPONENT: Final = "trade_integrity"

_BURST_FRAME_COUNT: Final = 3
_BURST_FRAME_SPACING_SECONDS: Final = 2.0
_INTERVAL_ENV_VAR: Final = "TRADE_INTEGRITY_MONITOR_INTERVAL_SECONDS"
_MONITORS: Final[tuple[TradeIntegrityMonitor, ...]] = ("m1", "m2")

ENV_VARS: Final[frozenset[str]] = frozenset({_INTERVAL_ENV_VAR})
"""Public allowlist of environment variables read by this module."""


class _HeartbeatSequenceTracker(Protocol):
    """Subset of the transport tracker needed for heartbeat provenance."""

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
    """Subset of the shared message publisher used by the watchdog."""

    @property
    def tracker(self) -> _HeartbeatSequenceTracker:
        """Return the publisher's sequence tracker."""

    async def send(self, stream_key: str, data: HeartbeatData) -> None:
        """Publish one complete heartbeat.

        Args:
            stream_key: Validated heartbeat topic.
            data: Complete heartbeat payload.
        """


class TradeIntegrityRepository(Protocol):
    """Repository operation required by the scheduling layer."""

    async def run_trade_integrity_monitor(
        self,
        *,
        monitor: TradeIntegrityMonitor,
        now: datetime,
        sweep_limit: int,
        worklog_limit: int,
    ) -> TradeIntegrityRunResult:
        """Run one bounded monitor pass.

        Args:
            monitor: M1 or M2 invariant selector.
            now: Shared evaluation timestamp.
            sweep_limit: Maximum recent-window rows requested.
            worklog_limit: Maximum durable worklog rows requested.

        Returns:
            Durable cursor, coverage, cost, and latched finding state.
        """


@dataclass(frozen=True, slots=True)
class TradeIntegrityMonitorConfig:
    """Fixed operational bounds for the watchdog.

    Attributes:
        interval_seconds: Delay between completed recurring ticks.
        startup_grace_seconds: Delay before the initial guarded tick.
        sweep_limit: Maximum recent-window rows requested per monitor.
        worklog_limit: Maximum worklog rows requested per monitor.
        monitor_timeout_seconds: Wall-clock bound for one repository call.
    """

    interval_seconds: float = DEFAULT_INTERVAL_SECONDS
    startup_grace_seconds: float = DEFAULT_STARTUP_GRACE_SECONDS
    sweep_limit: int = DEFAULT_SWEEP_LIMIT
    worklog_limit: int = DEFAULT_WORKLOG_LIMIT
    monitor_timeout_seconds: float = DEFAULT_MONITOR_TIMEOUT_SECONDS


def _resolve_interval(env_value: str | None) -> float:
    """Parse the cadence with a safe default and floor.

    Args:
        env_value: Raw environment value, or ``None`` when unset.

    Returns:
        Positive parsed seconds clamped to the 30–60 second range, or
        the one-minute default for missing, invalid, and non-positive
        values.
    """
    if env_value is None:
        return DEFAULT_INTERVAL_SECONDS
    try:
        parsed = float(env_value)
    except ValueError:
        return DEFAULT_INTERVAL_SECONDS
    if not isfinite(parsed) or parsed <= 0:
        return DEFAULT_INTERVAL_SECONDS
    return min(max(parsed, MIN_INTERVAL_SECONDS), MAX_INTERVAL_SECONDS)


class TradeIntegrityWatchdog:
    """Run M1 and M2 on a bounded cadence and publish their health.

    ``start`` is non-blocking and owns one task. The task performs its
    first guarded tick after the configured startup grace, which is zero
    by default, then repeats every minute. ``stop`` cancels and
    awaits that task.
    """

    def __init__(
        self,
        *,
        repo: TradeIntegrityRepository,
        msg_publisher: _HeartbeatPublisher | None = None,
        config: TradeIntegrityMonitorConfig | None = None,
    ) -> None:
        """Wire the durable repository, publisher, and operational bounds.

        Args:
            repo: Repository implementing the bounded monitor operation.
            msg_publisher: Shared heartbeat publisher; ``None`` retains
                monitoring and logging without bus publication.
            config: Explicit bounds, or defaults with cadence read from
                ``TRADE_INTEGRITY_MONITOR_INTERVAL_SECONDS``.
        """
        if config is None:
            config = TradeIntegrityMonitorConfig(
                interval_seconds=_resolve_interval(os.environ.get(_INTERVAL_ENV_VAR))
            )
        self._repo = repo
        self._msg_publisher = msg_publisher
        self._config = config
        self._sequences: dict[TradeIntegrityMonitor, int] = {}
        self._stopping = asyncio.Event()
        self._loop_task: asyncio.Task[None] | None = None

    @property
    def config(self) -> TradeIntegrityMonitorConfig:
        """Return the resolved immutable operating bounds.

        Returns:
            Watchdog configuration.
        """
        return self._config

    def start(self) -> None:
        """Spawn the task that performs the initial and recurring ticks."""
        self._stopping.clear()
        self._loop_task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Signal, cancel, and await the owned task when present."""
        self._stopping.set()
        task = self._loop_task
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._loop_task = None

    async def _run(self) -> None:
        """Apply startup grace, take one guarded tick, and enter the loop."""
        if self._config.startup_grace_seconds > 0:
            await asyncio.sleep(self._config.startup_grace_seconds)
        await self._guarded_tick()
        await self._loop()

    async def _loop(self) -> None:
        """Sleep, tick, and repeat until shutdown is signalled."""
        while not self._stopping.is_set():
            await asyncio.sleep(self._config.interval_seconds)
            if self._stopping.is_set():
                return
            await self._guarded_tick()

    async def _guarded_tick(self) -> None:
        """Keep an unexpected scheduling or publication failure non-fatal."""
        try:
            await self._tick()
        except Exception:
            logger.exception("TradeIntegrityWatchdog tick failed; continuing on next tick")

    async def _tick(self, now: datetime | None = None) -> None:
        """Run M1 then M2 with independent failure and timeout guards.

        Args:
            now: Shared reference instant, or current UTC time when omitted.
        """
        reference_now = now if now is not None else datetime.now(UTC)
        for monitor in _MONITORS:
            result = await self._run_monitor(monitor, reference_now)
            if result is not None:
                await self._publish_result(result, reference_now)

    async def _run_monitor(
        self,
        monitor: TradeIntegrityMonitor,
        now: datetime,
    ) -> TradeIntegrityRunResult | None:
        """Run one repository pass without suppressing the other invariant.

        Args:
            monitor: M1 or M2.
            now: Shared tick timestamp.

        Returns:
            Repository result, or ``None`` after a timeout or failure.
        """
        try:
            return await asyncio.wait_for(
                self._repo.run_trade_integrity_monitor(
                    monitor=monitor,
                    now=now,
                    sweep_limit=self._config.sweep_limit,
                    worklog_limit=self._config.worklog_limit,
                ),
                timeout=self._config.monitor_timeout_seconds,
            )
        except TimeoutError:
            logger.error(
                "TradeIntegrityWatchdog %s timed out after %.1fs",
                monitor,
                self._config.monitor_timeout_seconds,
            )
        except Exception:
            logger.exception(
                "TradeIntegrityWatchdog %s failed; continuing with next monitor", monitor
            )
        return None

    async def _publish_result(
        self,
        result: TradeIntegrityRunResult,
        evaluated_at: datetime,
    ) -> None:
        """Publish a clean reset or a three-frame warning burst.

        Args:
            result: Durable repository outcome.
            evaluated_at: Shared timestamp passed to both monitors.
        """
        finding_details = self._finding_details(result)
        if finding_details:
            logger.error(
                "Trade integrity %s violation detected: %s",
                result.monitor,
                finding_details,
            )
        elif result.lagged:
            logger.error(
                "Trade integrity %s coverage lagged by %ss",
                result.monitor,
                result.lag_seconds,
            )
        publisher = self._msg_publisher
        if publisher is None:
            return
        has_findings = bool(result.findings)
        status = (
            HealthStatusEnum.WARNING if has_findings or result.lagged else HealthStatusEnum.HEALTHY
        )
        topic = heartbeat_topic(WATCHDOG_HEARTBEAT_COMPONENT, result.monitor)
        frame_count = _BURST_FRAME_COUNT if status == HealthStatusEnum.WARNING else 1
        for frame_index in range(frame_count):
            if frame_index:
                await asyncio.sleep(_BURST_FRAME_SPACING_SECONDS)
            await self._publish_frame(
                publisher=publisher,
                topic=topic,
                result=result,
                status=status,
                evaluated_at=evaluated_at,
            )

    @staticmethod
    def _reason(*, has_findings: bool, lagged: bool) -> str:
        """Select a stable status reason.

        Args:
            has_findings: Whether latched invariant findings exist.
            lagged: Whether completed coverage breached its lag budget.

        Returns:
            Stable reason identifier for heartbeat metadata.
        """
        if has_findings and lagged:
            return "integrity_findings_and_monitor_lagged"
        if has_findings:
            return "integrity_findings"
        if lagged:
            return "monitor_lagged"
        return "healthy"

    async def _publish_frame(
        self,
        *,
        publisher: _HeartbeatPublisher,
        topic: str,
        result: TradeIntegrityRunResult,
        status: HealthStatusEnum,
        evaluated_at: datetime,
    ) -> None:
        """Publish one frame while containing transport failures.

        Args:
            publisher: Shared bus publisher.
            topic: Monitor-specific heartbeat topic.
            result: Repository outcome represented by the frame.
            status: HEALTHY or WARNING.
            evaluated_at: Shared monitor evaluation timestamp.
        """
        try:
            tracker = publisher.tracker
            sequence = self._sequences.get(result.monitor, 0) + 1
            self._sequences[result.monitor] = sequence
            reason = self._reason(
                has_findings=bool(result.findings),
                lagged=result.lagged,
            )
            meta: JsonObject = {
                "synthetic": True,
                "origin": "trade_integrity_watchdog",
                "monitor": result.monitor,
                "reason": reason,
                "finding_count": len(result.findings),
                "findings": self._finding_details(result),
                "sweep_rows": result.sweep_rows,
                "worklog_rows": result.worklog_rows,
                "cursor_timestamp": result.cursor_timestamp.isoformat(),
                "cursor_id": result.cursor_id,
                "covered_through": result.covered_through.isoformat(),
                "lag_seconds": result.lag_seconds,
                "lagged": result.lagged,
                "pass_completed": result.pass_completed,
                "sweep_limit": self._config.sweep_limit,
                "worklog_limit": self._config.worklog_limit,
                "monitor_timeout_seconds": self._config.monitor_timeout_seconds,
                "evaluated_at": evaluated_at.isoformat(),
            }
            frame = HeartbeatData(
                public_id=str(uuid7()),
                timestamp=datetime.now(UTC),
                session_id=tracker.session_id,
                sequence_id=tracker.next_sequence(topic),
                component=f"{WATCHDOG_HEARTBEAT_COMPONENT}.{result.monitor}",
                sequence=sequence,
                status=status,
                lag_ms=max(result.lag_seconds, 0) * 1000,
                meta=meta,
            )
            await publisher.send(topic, frame)
        except Exception:
            logger.exception(
                "TradeIntegrityWatchdog %s heartbeat publish failed",
                result.monitor,
            )

    @staticmethod
    def _finding_details(result: TradeIntegrityRunResult) -> JsonArray:
        """Serialize the bounded finding set for logs and heartbeat evidence.

        Args:
            result: Repository outcome whose findings are already capped.

        Returns:
            JSON objects identifying every bounded finding.
        """
        details: JsonArray = []
        for finding in result.findings:
            detail: JsonObject = {"monitor": finding.monitor}
            if finding.public_id is not None:
                detail["public_id"] = finding.public_id
            if finding.instrument_public_id is not None:
                detail["instrument_public_id"] = finding.instrument_public_id
            if finding.trade_id is not None:
                detail["trade_id"] = finding.trade_id
            if finding.expected_executed_at is not None:
                detail["expected_executed_at"] = finding.expected_executed_at.isoformat()
            if finding.conflicting_executed_at is not None:
                detail["conflicting_executed_at"] = finding.conflicting_executed_at.isoformat()
            if finding.active_count is not None:
                detail["active_count"] = finding.active_count
            details.append(detail)
        return details
