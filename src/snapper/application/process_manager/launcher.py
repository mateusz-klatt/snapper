"""Process launcher service module.

This module provides the main process lifecycle management service.
It handles:
- Starting processes as threads or subprocesses
- Tracking process runs in database
- Monitoring and cleanup of completed processes
- Graceful shutdown of all processes

Configuration resolution is delegated to config_resolver module.
Registry synchronization is delegated to registry_syncer module.
Run record persistence is delegated to run_recorder module.
"""

import asyncio
import contextlib
import functools
import inspect
import json
import re
import time
import zlib
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from enum import StrEnum
from typing import Any
from typing import Final
from typing import cast
from uuid import uuid7

import psutil
from loguru import logger
from sqlalchemy import select

from snapper.application.process_manager.config_resolver import VALID_PROCESS_MODES
from snapper.application.process_manager.config_resolver import get_process_configs
from snapper.application.process_manager.config_resolver import import_process_class
from snapper.application.process_manager.config_resolver import resolve_lifecycle
from snapper.application.process_manager.config_resolver import resolve_mode
from snapper.application.process_manager.config_resolver import resolve_parameters_schema
from snapper.application.process_manager.config_resolver import resolve_restart_policy
from snapper.application.process_manager.config_resolver import resolve_role
from snapper.application.process_manager.config_resolver import resolve_tags
from snapper.application.process_manager.executor_naming import is_executor_instance
from snapper.application.process_manager.executor_naming import is_executor_template
from snapper.application.process_manager.executor_naming import parse_executor_instance
from snapper.application.process_manager.models import ProcessConfigModel
from snapper.application.process_manager.models import ProcessInstanceInfo
from snapper.application.process_manager.models import ProcessRegistryEntry
from snapper.application.process_manager.models import ProcessStartResult
from snapper.application.process_manager.models import ProcessStatusResult
from snapper.application.process_manager.models import ProcessStopResult
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.registry import get_registered_processes
from snapper.application.process_manager.registry_syncer import ProcessRegistrySyncer
from snapper.application.process_manager.run_recorder import ProcessRunRecorder
from snapper.application.process_manager.spawner import ProcessSpawnerService
from snapper.application.process_manager.strategy_scope import StrategyScopeError
from snapper.application.process_manager.strategy_scope import classify_strategy_process
from snapper.application.process_manager.strategy_scope import (
    enforce_classified_strategy_scope_complete,
)
from snapper.application.services.market_persist_policy import MarketPersistPolicy
from snapper.config.settings import AppSettings
from snapper.core.json_types import JsonObject
from snapper.core.types import HealthStatus
from snapper.core.types import HealthStatusEnum
from snapper.core.types import ProcessAutostartProfileEnum
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessMode
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRestartPolicyEnum
from snapper.core.types import ProcessRoleEnum
from snapper.core.types import ProcessRunStatusEnum
from snapper.core.types import StartProcessStatusEnum
from snapper.core.types import StopProcessStatusEnum
from snapper.core.wallet_resolution import WalletAmbiguousError
from snapper.core.wallet_resolution import WalletUnresolvedError
from snapper.core.wallet_short import compute_wallet_short
from snapper.data.models import Setting
from snapper.data.repository import get_repository
from snapper.data.repository import where_active_now
from snapper.data.repository_types import WalletCredentialRow
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.schemas.data import ProcessConfiguredEventData
from snapper.messaging.schemas.data import ProcessRunEventData
from snapper.messaging.schemas.data import ProcessSummaryEventData
from snapper.messaging.schemas.data import ProcessSummaryItem
from snapper.messaging.schemas.data import StrategyListEventData
from snapper.messaging.topics.builders import heartbeat_topic

_PROCESSES_SUMMARY_STREAM = "processes.events.summary"
_PROCESSES_CONFIGURED_STREAM = "processes.events.configured"
_PROCESSES_RUNS_STREAM = "processes.events.runs"
_STRATEGIES_LIST_STREAM = "strategies.events.list"
_BROADCAST_FAILURE_TEMPLATE = "Failed to broadcast {}: {}"
_CORE_HEALTH_CACHE_TTL_S: Final[float] = 5.0
_MARKET_DATA_PUBLISHER_TAGS: Final[frozenset[str]] = frozenset({"market-data", "publisher"})
_ZMQ_BROKER_TAGS: Final[frozenset[str]] = frozenset({"zmq", "broker"})

_RESTART_BASE_DELAY_S: Final[float] = 1.0
_RESTART_FACTOR: Final[float] = 2.0
_RESTART_MAX_DELAY_S: Final[float] = 60.0
_RESTART_JITTER_FRACTION: Final[float] = 0.25
_RESTART_HEALTHY_UPTIME_S: Final[float] = 120.0
_TOTAL_RESET_UPTIME_S: Final[float] = 1200.0
_MAX_RESTART_ATTEMPTS: Final[int] = 6

_PARK_REBURST_PERIOD_S: Final[float] = 3600.0
"""Pause between repeated park bursts while a name stays parked.

A one-shot burst could be lost forever: the notify sidecar holds its
3-consecutive window only in memory, so a restart that swallows even one
burst frame would silently drop the only page a parked executor ever
gets. Re-bursting hourly makes parked alerting level-triggered (it
survives sidecar restarts) while the rule's dedup caps pages at about
one per hour; an accepted parking stops the loop via the per-frame gate
and the unpark cancellation."""

_PARK_HEARTBEAT_SPACING_S: Final[float] = 2.0
"""Spacing between synthetic park-heartbeat frames.

Must exceed the bridge-side 1s throttle on ``system.heartbeats.`` —
tighter spacing would drop frames 2-3 and the critical-system-error
rule's 3-consecutive gate would never trip for a parked executor."""

_EXECUTOR_INSTANCE_RE: Final[re.Pattern[str]] = re.compile(
    r"^executor_(?P<exchange>[a-z0-9_]+)_w(?P<wallet_short>[0-9a-f]+)$"
)
"""Per-wallet executor instance names as minted by spawn_per_wallet_executors."""
_MAX_TOTAL_FAILED_RESTARTS: Final[int] = 20


class _DesiredState(StrEnum):
    """Operator-intended state for a managed process.

    The watchdog reconciles the actual process state toward this
    single source of truth on every death. ``RUNNING`` means the
    process should be (re)spawned on an unexpected death per its
    restart policy; ``STOPPED`` means a deliberate stop owns the
    process and the watchdog must not respawn it.
    """

    RUNNING = "running"
    STOPPED = "stopped"


def _monotonic() -> float:
    """Return a monotonic clock reading for uptime/backoff accounting.

    A module-level indirection so tests can patch the clock and drive
    deterministic uptime/escalation scenarios without real sleeps.

    Returns:
        The current monotonic time in seconds.
    """
    return time.monotonic()


def _jitter(name: str, base: float) -> float:
    """Return an additive, per-name de-correlated jitter for a backoff.

    The jitter is ADDITIVE-only (never shrinks the base delay) and is
    de-correlated across process names via ``zlib.crc32`` so a fleet of
    publishers dying together does not stampede with identical backoffs.
    A given name always yields the same fraction, which keeps tests
    deterministic.

    Args:
        name: Process name driving the de-correlation.
        base: Base backoff delay the jitter is computed against.

    Returns:
        A non-negative jitter in ``[0, base * _RESTART_JITTER_FRACTION)``.
    """
    fraction = zlib.crc32(name.encode()) % 1000 / 1000.0
    return base * _RESTART_JITTER_FRACTION * fraction


def _compute_backoff_delay(name: str, attempts: int) -> float:
    """Return the backoff delay for the n-th consecutive restart attempt.

    Exponential backoff (``base * factor ** attempts``) capped at
    ``_RESTART_MAX_DELAY_S``, plus the additive per-name jitter so the
    final delay never drops below the capped exponential term.

    Args:
        name: Process name (drives the de-correlated jitter).
        attempts: Consecutive restart-triggering deaths so far.

    Returns:
        The backoff delay in seconds.
    """
    exponential = _RESTART_BASE_DELAY_S * _RESTART_FACTOR**attempts
    capped = min(exponential, _RESTART_MAX_DELAY_S)
    return capped + _jitter(name, capped)


def is_market_data_publisher(tags: Iterable[str]) -> bool:
    """Return whether a process's tags mark it as a market-data publisher.

    A market-data publisher carries BOTH the ``market-data`` and
    ``publisher`` tags (the venue tag, e.g. ``kraken_equities``, is
    additional). This is the predicate the ``feed``/``api`` autostart
    profiles split on so the ingest tier can run in its own container.

    Args:
        tags: The registered tags of a process.

    Returns:
        True when the tag set is a superset of ``market-data`` +
        ``publisher``, False otherwise.
    """
    return _MARKET_DATA_PUBLISHER_TAGS.issubset(set(tags))


def is_zmq_broker(tags: Iterable[str]) -> bool:
    """Return whether a process's tags mark it as the ZMQ broker.

    The broker registration carries BOTH the ``zmq`` and ``broker``
    tags (``messaging/infrastructure/broker.py``). This is the
    predicate the ``zmq_broker_embedded=False`` opt-out excludes from
    autostart so a dedicated broker container can own the bus without
    the backend starting a duplicate.

    Args:
        tags: The registered tags of a process.

    Returns:
        True when the tag set is a superset of ``zmq`` + ``broker``,
        False otherwise.
    """
    return _ZMQ_BROKER_TAGS.issubset(set(tags))


def _copy_process_config_with_mode(
    config: ProcessConfigModel, mode: ProcessMode
) -> ProcessConfigModel:
    """Return a process config copy with a different execution mode.

    Args:
        config: Process configuration to copy.
        mode: Execution mode for the returned configuration.

    Returns:
        ProcessConfigModel with all fields preserved except ``mode``.
    """
    return ProcessConfigModel(
        name=config.name,
        enabled=config.enabled,
        mode=mode,
        class_path=config.class_path,
        method=config.method,
        parameters=config.parameters,
        note=config.note,
        lifecycle=config.lifecycle,
        role=config.role,
        restart_policy=config.restart_policy,
        tags=config.tags,
        parameters_schema=config.parameters_schema,
        template=config.template,
    )


def _copy_process_config_with_parameters(
    config: ProcessConfigModel, parameters: JsonObject
) -> ProcessConfigModel:
    """Return a process config copy with different constructor parameters.

    Args:
        config: Process configuration to copy.
        parameters: Constructor parameters for the returned configuration.

    Returns:
        ProcessConfigModel with all fields preserved except ``parameters``.
    """
    return ProcessConfigModel(
        name=config.name,
        enabled=config.enabled,
        mode=config.mode,
        class_path=config.class_path,
        method=config.method,
        parameters=parameters,
        note=config.note,
        lifecycle=config.lifecycle,
        role=config.role,
        restart_policy=config.restart_policy,
        tags=config.tags,
        parameters_schema=config.parameters_schema,
        template=config.template,
    )


class CoreProcessStartupError(RuntimeError):
    """Raised when one or more enabled CORE processes fail to start."""

    def __init__(self, failed_processes: list[str]) -> None:
        """Initialize with list of failed CORE process names."""
        self.failed_processes = failed_processes
        names = ", ".join(failed_processes)
        super().__init__(f"CORE process startup failed: {names}")


@dataclass
class _PerWalletSpawnOutcome:
    """Per-credential outcome from :meth:`_spawn_one_per_wallet_instance`.

    Carries the resolved instance metadata so the spawn-loop can
    decide whether a failure escalates to
    :class:`CoreProcessStartupError`. ``error=None`` means the spawn
    succeeded.
    """

    instance_name: str
    entry: ProcessRegistryEntry
    error: Exception | None


@dataclass
class _PerWalletStartTarget:
    """Resolved registry target for a per-wallet manual start."""

    exchange: str
    wallet_short: str
    template_name: str
    entry: ProcessRegistryEntry


class ProcessLauncherService:
    """Service for launching and managing application processes.

    Provides comprehensive process lifecycle management:
    - Starts processes based on priority order
    - Monitors process completion and handles failures
    - Coordinates graceful shutdown

    Delegates to specialized services:
    - ProcessRunRecorder for run record persistence
    - ProcessRegistrySyncer for registry-database synchronization
    - config_resolver module for configuration parsing

    Watchdog concurrency model (desired-state reconciler):
        Each managed process carries an explicit desired state
        (``RUNNING`` | ``STOPPED``) in :attr:`_desired_state` — the
        single source of truth. On every death the watchdog reconciles
        the actual state toward the desired one under a per-name
        ``asyncio.Lock`` obtained from :meth:`_restart_lock_for`.
        Single-loop asyncio makes a synchronous check-and-set atomic.

        NO-REENTRANCY RULE: only :meth:`_maybe_schedule_restart`,
        :meth:`_delayed_restart`, :meth:`start_process_by_name`,
        :meth:`stop_process_by_name`, and :meth:`stop_all_processes`
        acquire ``_restart_lock_for``. :meth:`start_process` and the
        internal stop primitive used INSIDE a locked region MUST NOT
        acquire it (the lock is not reentrant). The backoff
        ``asyncio.sleep`` in :meth:`_delayed_restart` stays OUTSIDE the
        lock so a stop can cancel it cleanly. No path acquires two
        different name-locks, so there is no cross-name deadlock.

    Attributes:
        settings: Application settings.
        started_processes: Dict of running process instances.
        process_tasks: Dict of asyncio tasks for async processes.
        process_lifecycles: Dict tracking lifecycle type per process.
        process_roles: Dict tracking role per process.
        active_runs: Dict mapping process name to public_id.
        spawner: ProcessSpawnerService for subprocess management.
        expected_terminations: Set of processes expected to stop.
    """

    def __init__(self, settings: AppSettings) -> None:
        """Initialize the launcher service.

        Args:
            settings: Application settings for database URL etc.
        """
        self.settings = settings
        self.started_processes: dict[str, RegisterableProcess] = {}
        self.process_tasks: dict[str, asyncio.Task[object]] = {}
        self.process_lifecycles: dict[str, ProcessLifecycleEnum] = {}
        self.process_roles: dict[str, ProcessRoleEnum] = {}
        self.instance_configs: dict[str, ProcessConfigModel] = {}
        self.active_runs: dict[str, str] = {}
        self.active_run_started_at: dict[str, datetime] = {}
        self.spawner = ProcessSpawnerService()
        self.expected_terminations: set[str] = set()
        self._feed_failed_publisher: str = ""
        self._feed_failure_event = asyncio.Event()
        self._desired_state: dict[str, _DesiredState] = {}
        self._restart_attempts: dict[str, int] = {}
        self._total_failed_restarts: dict[str, int] = {}
        self._restart_uptime_start: dict[str, float] = {}
        self._restart_configs: dict[str, ProcessConfigModel] = {}
        self._restart_tasks: dict[str, asyncio.Task[None]] = {}
        self._restart_locks: dict[str, asyncio.Lock] = {}
        self._psutil_handles: dict[str, psutil.Process] = {}
        self._process_metrics: dict[str, tuple[int | None, float | None]] = {}
        self._run_recorder = ProcessRunRecorder(settings)
        self._registry_syncer = ProcessRegistrySyncer(settings)
        self._market_persist_policy: MarketPersistPolicy | None = None
        self._msg_publisher: MessagePublisher | None = None
        self._parked_processes: set[str] = set()
        self._park_heartbeat_tasks: dict[str, asyncio.Task[None]] = {}
        self._core_health_cache: tuple[float, HealthStatus] | None = None

    def set_msg_publisher(self, publisher: MessagePublisher | None) -> None:
        """Inject the bus publisher used for processes/strategies fanout.

        Mirrors :class:`ScopeGrantService.set_msg_publisher` exactly so
        the FastAPI lifespan can share the existing
        ``user_service_publisher`` socket — no second PUB socket is
        opened. Called once after launcher construction and BEFORE
        :meth:`start_all_processes` so autostart emits go out on the
        first start cycle.

        Args:
            publisher: Configured ``MessagePublisher`` or ``None`` to clear.
        """
        self._msg_publisher = publisher

    def coordinator_topic_slug(self) -> str:
        """Return a topic-safe slug for ``coordinator_instance_id``.

        Wraps the int ``coordinator_instance_id`` (zero-based, default ``0``)
        with a literal ``coord-`` prefix so the result satisfies the
        ``processes.events.*`` and ``strategies.events.list.*`` validator
        pattern ``[A-Za-z][A-Za-z0-9_-]*``. Without the prefix a bare
        ``"0"`` suffix fails the validator's leading-letter constraint
        and the emit-site send raises ``TopicValidationError``. Public so
        the REST ``/processes/summary`` handler can stamp the same node
        slug onto its response that the launcher emits on the bus.

        Returns:
            The ``coord-<id>`` slug for this node, safe as a topic suffix.
        """
        return f"coord-{self.settings.coordinator_instance_id}"

    async def build_process_summary_items(self) -> list[ProcessSummaryItem]:
        """Compose a snapshot of every tracked process row.

        Joins persisted configs from :meth:`get_process_configs` (the
        source of truth for ``enabled``) with runtime per-wallet
        instances held in ``instance_configs``. The result is the
        launcher's authoritative "what exists right now" view —
        consumers invalidate their cache against this signal and
        re-fetch via REST for full detail. Public because the REST
        ``/processes/summary`` handler reuses it to surface this node's
        per-process RSS/CPU rows in its response payload.

        Returns:
            Per-process status rows joining persisted configs with runtime
            per-wallet instances, each carrying any sampled RSS/CPU.
        """
        configs = await self.get_process_configs()
        items: list[ProcessSummaryItem] = []
        seen: set[str] = set()
        for config in configs:
            name = config.name
            rss, cpu = self._process_metrics.get(name, (None, None))
            items.append(
                ProcessSummaryItem(
                    name=name,
                    running=name in self.started_processes,
                    enabled=config.enabled,
                    role=config.role.value,
                    lifecycle=config.lifecycle.value,
                    active_public_id=self.active_runs.get(name),
                    rss_bytes=rss,
                    cpu_percent=cpu,
                )
            )
            seen.add(name)
        for name, instance_config in self.instance_configs.items():
            if name in seen:
                continue
            rss, cpu = self._process_metrics.get(name, (None, None))
            items.append(
                ProcessSummaryItem(
                    name=name,
                    running=name in self.started_processes,
                    enabled=instance_config.enabled,
                    role=instance_config.role.value,
                    lifecycle=instance_config.lifecycle.value,
                    active_public_id=self.active_runs.get(name),
                    rss_bytes=rss,
                    cpu_percent=cpu,
                )
            )
        return items

    async def emit_summary_snapshot(self) -> None:
        """Emit one cross-coordinator summary snapshot now (public wrapper).

        Container entrypoints without PROCESS children (the strategies
        engine runs THREAD-mode tasks) drive a periodic loop through
        this wrapper so the API-side ``RemoteSummaryCache`` (15s TTL)
        keeps seeing fresh coord snapshots; the native-process monitor
        only emits when subprocesses exist.
        """
        await self._emit_summary_snapshot()

    async def _emit_summary_snapshot(self) -> None:
        """Publish ``processes.events.summary.{instance_id}`` snapshot.

        Best-effort: a missing publisher silently no-ops (the launcher
        may be used in test harnesses or process-only modes that don't
        wire one), and a send failure logs the exception at exception
        level. Matches the :class:`ScopeGrantService` resilience
        contract — emit failures must not propagate into the launcher's
        start / stop control flow.
        """
        if self._msg_publisher is None:
            return
        slug = self.coordinator_topic_slug()
        topic = f"{_PROCESSES_SUMMARY_STREAM}.{slug}"
        try:
            items = await self.build_process_summary_items()
            tracker = self._msg_publisher.tracker
            payload = ProcessSummaryEventData(
                session_id=tracker.session_id,
                sequence_id=tracker.next_sequence(topic),
                public_id=str(uuid7()),
                timestamp=datetime.now(UTC),
                coordinator=slug,
                processes=items,
                snapshot_at=datetime.now(UTC),
            )
            await self._msg_publisher.send(topic, payload)
        except Exception as exc:
            logger.exception(_BROADCAST_FAILURE_TEMPLATE, topic, exc)

    async def _emit_configured_snapshot(self) -> None:
        """Publish ``processes.events.configured.{instance_id}`` snapshot.

        Fired when the persisted configuration set mutates
        (``create_process_config``) or when runtime per-wallet executor
        instances appear after :meth:`spawn_per_wallet_executors`.
        """
        if self._msg_publisher is None:
            return
        topic = f"{_PROCESSES_CONFIGURED_STREAM}.{self.coordinator_topic_slug()}"
        try:
            configs = await self.get_process_configs()
            names = sorted({config.name for config in configs} | set(self.instance_configs.keys()))
            tracker = self._msg_publisher.tracker
            payload = ProcessConfiguredEventData(
                session_id=tracker.session_id,
                sequence_id=tracker.next_sequence(topic),
                public_id=str(uuid7()),
                timestamp=datetime.now(UTC),
                process_names=names,
                snapshot_at=datetime.now(UTC),
            )
            await self._msg_publisher.send(topic, payload)
        except Exception as exc:
            logger.exception(_BROADCAST_FAILURE_TEMPLATE, topic, exc)

    async def _emit_strategy_list_snapshot(self) -> None:
        """Publish ``strategies.events.list.{instance_id}`` snapshot.

        Carries the canonical class paths of every persisted
        strategy-role process. Frontend invalidates against this
        signal — payload identity fields are informational only.
        """
        if self._msg_publisher is None:
            return
        topic = f"{_STRATEGIES_LIST_STREAM}.{self.coordinator_topic_slug()}"
        try:
            configs = await self.get_process_configs()
            class_paths = sorted(
                {config.class_path for config in configs if config.role is ProcessRoleEnum.STRATEGY}
            )
            tracker = self._msg_publisher.tracker
            payload = StrategyListEventData(
                session_id=tracker.session_id,
                sequence_id=tracker.next_sequence(topic),
                public_id=str(uuid7()),
                timestamp=datetime.now(UTC),
                strategy_classes=class_paths,
                snapshot_at=datetime.now(UTC),
            )
            await self._msg_publisher.send(topic, payload)
        except Exception as exc:
            logger.exception(_BROADCAST_FAILURE_TEMPLATE, topic, exc)

    async def _emit_run_event(
        self,
        *,
        process_name: str,
        run_id: str,
        status: ProcessRunStatusEnum,
        started_at: datetime,
        completed_at: datetime | None,
        error: str | None,
        exit_code: int | None = None,
    ) -> None:
        """Publish ``processes.events.runs.{process_name}`` lifecycle event.

        Unlike the summary topic, run events are per-run (not full
        snapshots) so consumers can append to their run-history view
        without re-fetching. ``exit_code`` is populated for native
        subprocess termination (folded out of the ``error`` string by
        :meth:`_finalize_process_run` callers) and left ``None`` for
        asyncio-task processes which have no equivalent exit code.
        """
        if self._msg_publisher is None:
            return
        topic = f"{_PROCESSES_RUNS_STREAM}.{process_name}"
        try:
            tracker = self._msg_publisher.tracker
            payload = ProcessRunEventData(
                session_id=tracker.session_id,
                sequence_id=tracker.next_sequence(topic),
                public_id=str(uuid7()),
                timestamp=datetime.now(UTC),
                process_name=process_name,
                run_id=run_id,
                status=status.value,
                started_at=started_at,
                completed_at=completed_at,
                exit_code=exit_code,
            )
            if error is not None:
                logger.debug("run-event error for {} run {}: {}", process_name, run_id, error)
            await self._msg_publisher.send(topic, payload)
        except Exception as exc:
            logger.exception(_BROADCAST_FAILURE_TEMPLATE, topic, exc)

    def set_market_persist_policy(self, policy: MarketPersistPolicy | None) -> None:
        """Inject the :class:`MarketPersistPolicy` for publisher gating.

        Called from the FastAPI lifespan after the policy has been
        rebuilt + the admin listener has started, but BEFORE
        :meth:`start_all_processes` so every publisher launched
        in-process receives the same policy reference. Subprocess
        publishers reconstruct the policy from settings independently;
        the launcher-side injection only covers thread / in-process mode
        per the deployment contract.

        Args:
            policy: Configured policy singleton or ``None`` to clear.
        """
        self._market_persist_policy = policy

    def _inject_market_persist_policy(
        self, process_instance: RegisterableProcess, config_name: str
    ) -> None:
        """Wire ``self._market_persist_policy`` into a publisher instance.

        Walks the duck-type contract instead of importing
        :class:`MarketDataPublisherService` so the launcher does not
        introduce a circular dependency (``base.py`` already imports
        the policy module). A publisher exposes ``set_persist_policy``;
        any other process type silently no-ops.

        Args:
            process_instance: Just-instantiated process object.
            config_name: Process name (for log readability).
        """
        if self._market_persist_policy is None:
            return
        setter = getattr(process_instance, "set_persist_policy", None)
        if not callable(setter):
            return
        setter(self._market_persist_policy)
        logger.debug(
            "MarketPersistPolicy injected into {} (in-process)",
            config_name,
        )

    async def _create_process_run_record(
        self,
        config: ProcessConfigModel,
        parameters: JsonObject | None,
    ) -> str:
        """Delegate to run_recorder.create_run_record."""
        return await self._run_recorder.create_run_record(config, parameters)

    async def _update_process_run_record(
        self,
        public_id: str,
        status: ProcessRunStatusEnum,
        *,
        result: JsonObject | None = None,
        error: str | None = None,
    ) -> None:
        """Delegate to run_recorder.update_run_record."""
        await self._run_recorder.update_run_record(public_id, status, result=result, error=error)

    async def _finalize_process_run(
        self,
        name: str,
        status: ProcessRunStatusEnum,
        *,
        result: JsonObject | None = None,
        error: str | None = None,
        exit_code: int | None = None,
    ) -> None:
        """Finalize a process run by updating its record.

        Removes run from active_runs, delegates DB update to
        run_recorder, then emits a terminal ``processes.events.runs``
        frame so consumers can drop the run from their in-flight view.

        Args:
            name: Process name.
            status: Final status.
            result: Optional result data.
            error: Optional error message.
            exit_code: Native subprocess exit code when applicable;
                forwarded onto the run event so direct event consumers
                can reconstruct termination details without joining the
                stringified ``error`` field.
        """
        public_id = self.active_runs.pop(name, None)
        started_at = self.active_run_started_at.pop(name, None)
        if public_id is None:
            return
        await self._run_recorder.update_run_record(public_id, status, result=result, error=error)
        completed_at = datetime.now(UTC)
        await self._emit_run_event(
            process_name=name,
            run_id=public_id,
            status=status,
            started_at=started_at if started_at is not None else completed_at,
            completed_at=completed_at,
            error=error,
            exit_code=exit_code,
        )

    @staticmethod
    def _resolve_lifecycle(
        raw: Any,
        process_name: str,
    ) -> ProcessLifecycleEnum:
        """Delegate to config_resolver.resolve_lifecycle."""
        return resolve_lifecycle(raw, process_name)

    @staticmethod
    def _resolve_role(
        raw: Any,
        process_name: str,
    ) -> ProcessRoleEnum:
        """Delegate to config_resolver.resolve_role."""
        return resolve_role(raw, process_name)

    @staticmethod
    def _resolve_tags(raw: Any) -> tuple[str, ...]:
        """Delegate to config_resolver.resolve_tags."""
        return resolve_tags(raw)

    @staticmethod
    def _resolve_parameters_schema(
        config_dict: dict[str, Any],
        entry: ProcessRegistryEntry | None,
    ) -> JsonObject | None:
        """Delegate to config_resolver.resolve_parameters_schema."""
        return resolve_parameters_schema(config_dict, entry)

    async def get_process_configs(self) -> list[ProcessConfigModel]:
        """Load process configurations from database.

        Executor templates have ``enabled`` forced to ``False`` at read
        time and any ``wallet_public_id`` leak is stripped from
        ``parameters``. Templates are config-only — never directly
        runnable — so the persisted ``enabled=True`` flag from the
        legacy single-wallet era must not flow back into
        :meth:`start_all_processes` (which would try to launch the
        template directly, defeating the per-wallet design). Stripping
        the wallet leak keeps the template a clean source of operator
        defaults shared across all wallets on that exchange.

        Returns:
            List of ProcessConfigModel instances with executor
            templates normalized.
        """
        configs = await get_process_configs(self.settings)
        for config in configs:
            if not is_executor_template(config.name):
                continue
            config.enabled = False
            if "wallet_public_id" in config.parameters:
                config.parameters = {
                    key: value
                    for key, value in config.parameters.items()
                    if key != "wallet_public_id"
                }
        return configs

    def import_class(
        self,
        class_path: str,
        process_name: str | None = None,
        template_name: str | None = None,
    ) -> type:
        """Import a class by its fully qualified path.

        Args:
            class_path: Fully qualified class path.
            process_name: Optional process name to check registry first.
            template_name: Optional source-template registry name checked
                when the process name is not registered.

        Returns:
            The imported class type.
        """
        return import_process_class(class_path, process_name, template_name)

    async def _start_as_async_task(self, config: ProcessConfigModel, method: Any) -> None:
        """Start an async method as an asyncio task.

        Args:
            config: Process configuration.
            method: Async method to run.

        Raises:
            Exception: Re-raised if task fails within startup grace period.
        """
        task = asyncio.create_task(method())
        self.process_tasks[config.name] = task
        self._register_task_completion(config.name, task)
        logger.info(f"Process '{config.name}' started as async task")
        await asyncio.sleep(0.1)
        if not task.done():
            return
        if task.cancelled():
            raise asyncio.CancelledError(
                f"Process '{config.name}' cancelled during startup grace window"
            )
        exception = task.exception()
        if exception is not None:
            raise exception

    async def _start_as_sync_executor(self, config: ProcessConfigModel, method: Any) -> None:
        """Start a sync method in a thread executor.

        Args:
            config: Process configuration.
            method: Sync method to run.
        """
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, method)
        logger.info(f"Process '{config.name}' started in thread executor")

    def _cleanup_failed_start(self, config_name: str) -> None:
        """Remove process from all tracking dicts after a failed start.

        Args:
            config_name: Process name to clean up.
        """
        self.process_lifecycles.pop(config_name, None)
        self.process_tasks.pop(config_name, None)
        self.started_processes.pop(config_name, None)
        self.process_roles.pop(config_name, None)
        self.instance_configs.pop(config_name, None)

    def _validate_parameters(self, config: ProcessConfigModel) -> dict[str, Any]:
        """Validate process parameters against the registered model.

        If a parameters_model is registered for this process, validates
        the parameters dict against it. Otherwise returns parameters as-is.

        Args:
            config: Process configuration with parameters to validate.

        Returns:
            Validated parameters dict ready for process_class instantiation.
        """
        registry = get_registered_processes()
        entry = registry.get(config.name)
        parameters_dict: dict[str, Any] = dict(config.parameters)
        if entry and entry.parameters_model:
            validated = entry.parameters_model.model_validate(parameters_dict)
            parameters_dict = validated.model_dump()
        return parameters_dict

    def _start_as_subprocess(self, config: ProcessConfigModel) -> None:
        """Spawn a native subprocess and register it.

        Args:
            config: Process configuration.
        """
        validated_params = self._validate_parameters(config)
        process_info = self.spawner.spawn(
            name=config.name,
            class_path=config.class_path,
            method=config.method,
            parameters=validated_params,
            template_name=config.template,
        )
        logger.info(f"Process '{config.name}' started with PID {process_info.pid}")
        self.started_processes[config.name] = process_info

    async def _start_in_process(self, config: ProcessConfigModel) -> None:
        """Instantiate the class and launch as async task or thread executor.

        Args:
            config: Process configuration.
        """
        process_class = self.import_class(config.class_path, config.name, config.template)
        validated_params = self._validate_parameters(config)
        process_instance = process_class(**validated_params)
        self._inject_market_persist_policy(process_instance, config.name)
        method = getattr(process_instance, config.method)
        if inspect.iscoroutinefunction(method):
            await self._start_as_async_task(config, method)
        else:
            await self._start_as_sync_executor(config, method)
        self.started_processes[config.name] = process_instance
        if config.name not in self.process_tasks:
            await self._finalize_process_run(config.name, ProcessRunStatusEnum.SUCCEEDED)

    def _is_one_shot_completed(self, config: ProcessConfigModel) -> bool:
        """Check whether a one-shot process has already completed.

        Args:
            config: Process configuration.

        Returns:
            True if the process is one-shot, not tracked as a task,
            and not running as a subprocess.
        """
        return (
            config.lifecycle is ProcessLifecycleEnum.ONE_SHOT
            and config.name not in self.process_tasks
            and config.mode != ProcessModeEnum.PROCESS
        )

    async def _try_create_run_record(self, config: ProcessConfigModel) -> str | None:
        """Attempt to create a database run record.

        On success the started_at clock is stamped into
        ``active_run_started_at`` so the matching ``processes.events.runs``
        emit on finalize carries the correct duration. A ``started``
        run event is emitted best-effort right after the DB insert so
        consumers see the run before its terminal status arrives.

        Args:
            config: Process configuration.

        Returns:
            The public_id string, or None if persistence failed.
        """
        run_parameters: JsonObject = {
            "mode": config.mode,
            "parameters": config.parameters,
        }
        try:
            public_id = await self._create_process_run_record(config, run_parameters)
            started_at = datetime.now(UTC)
            self.active_runs[config.name] = public_id
            self.active_run_started_at[config.name] = started_at
            await self._emit_run_event(
                process_name=config.name,
                run_id=public_id,
                status=ProcessRunStatusEnum.RUNNING,
                started_at=started_at,
                completed_at=None,
                error=None,
            )
            return public_id
        except Exception as run_error:
            logger.warning(
                "Unable to persist run record for process '{}': {}",
                config.name,
                run_error,
            )
            return None

    async def _handle_start_failure(
        self, config_name: str, public_id: str | None, exc: Exception
    ) -> None:
        """Handle process startup failure by cleaning up and recording.

        Emits a terminal ``processes.events.runs`` frame so consumers
        see the failed run alongside the started frame published by
        :meth:`_try_create_run_record`.

        Args:
            config_name: Name of the failed process.
            public_id: Database run record public ID, or None if not persisted.
            exc: The exception that caused the failure.
        """
        self._cleanup_failed_start(config_name)
        started_at = self.active_run_started_at.pop(config_name, None)
        if public_id is not None:
            await self._update_process_run_record(
                public_id,
                ProcessRunStatusEnum.FAILED,
                error=str(exc),
            )
            self.active_runs.pop(config_name, None)
            completed_at = datetime.now(UTC)
            await self._emit_run_event(
                process_name=config_name,
                run_id=public_id,
                status=ProcessRunStatusEnum.FAILED,
                started_at=started_at if started_at is not None else completed_at,
                completed_at=completed_at,
                error=str(exc),
            )

    async def _finalize_one_shot(self, config: ProcessConfigModel) -> None:
        """Finalize a one-shot process that has already completed.

        Clears the watchdog markers :meth:`start_process` armed before
        spawning so a completed non-native ONE_SHOT never leaks
        desired-state/config/uptime entries (it must never be
        watchdog-restarted). Native ONE_SHOT subprocesses do not reach
        this branch (the :meth:`_is_one_shot_completed` guard excludes
        PROCESS mode) and are cleared via their completion handler.

        Args:
            config: Process configuration.
        """
        if not self._is_one_shot_completed(config):
            return
        await self._finalize_process_run(config.name, ProcessRunStatusEnum.SUCCEEDED)
        self.started_processes.pop(config.name, None)
        self.process_lifecycles.pop(config.name, None)
        self.process_roles.pop(config.name, None)
        self._clear_watchdog_state(config.name)

    async def start_process(self, config: ProcessConfigModel) -> None:
        """Start a single process from configuration.

        Handles both thread and subprocess modes:
        - Thread mode: Instantiates class, calls method as task
        - Process mode: Spawns subprocess via ProcessSpawnerService

        Creates database run record and handles completion tracking.
        On success the launcher emits a ``processes.events.summary``
        snapshot — and a ``strategies.events.list`` snapshot when the
        config carries the STRATEGY role — so subscribers can refresh
        their cached views without polling.

        This is the LOCK-FREE start primitive. It synchronously records
        the respawn config snapshot and the uptime origin BEFORE its
        first await so a death that arrives later can be reconciled. It
        does NOT own the desired-state RUNNING transition: the
        ``desired=RUNNING`` write belongs to the lock-holding callers
        (:meth:`start_process_by_name`,
        :meth:`start_per_wallet_instance_by_name`,
        :meth:`_delayed_restart`) and to the boot-time spawners, which
        all set it via :meth:`_arm_desired_running` before invoking this
        primitive. Starting a name also clears it from
        ``_parked_processes`` — a successful (re)start ends the parked
        episode, recovering the ``/health`` flip. Were ``start_process`` to set RUNNING itself, a manual
        start racing a stop's pre-lock window could clobber the stop's
        ``STOPPED`` marker. It NEVER clears the watchdog
        desired-state/config/uptime on a startup failure: ownership of
        those markers belongs to the watchdog and to a deliberate stop,
        so a respawn failure cannot clobber a concurrent stop's
        ``STOPPED`` marker. The lock-taking manual callers
        (:meth:`start_process_by_name`,
        :meth:`start_per_wallet_instance_by_name`) own the first-start
        leak cleanup for names they themselves armed. The ``try`` opens
        BEFORE the run-record creation await so its ``finally`` covers
        ``_try_create_run_record`` too. On ANY non-clean exit (including
        ``CancelledError``, which is a ``BaseException`` that bypasses
        the ``except Exception`` cleanup), the ``finally`` finalizes a
        still-dangling active run record so a cancel landing inside
        run-record creation, or between it and the spawn, never leaks a
        RUNNING run. The ``finally`` guards on
        ``config.name in self.active_runs`` so the normal ``except``
        path (which already pops ``active_runs`` via
        :meth:`_handle_start_failure`) is never double-finalized. Per
        the NO-REENTRANCY RULE it never acquires
        ``_restart_lock_for``; callers that need serialization
        (:meth:`start_process_by_name`, :meth:`_delayed_restart`) hold
        the lock around their call.

        Args:
            config: Process configuration to start.

        Raises:
            Exception: Re-raised from process startup failures.
        """
        self.process_lifecycles[config.name] = config.lifecycle
        self.process_roles[config.name] = config.role
        self._restart_configs[config.name] = config
        self._restart_uptime_start[config.name] = _monotonic()
        started = False
        public_id: str | None = None
        try:
            public_id = await self._try_create_run_record(config)
            logger.info(
                f"Starting process '{config.name}' in {config.mode} mode "
                f"(class: {config.class_path}, method: {config.method})"
            )
            if config.mode == ProcessModeEnum.PROCESS:
                self._start_as_subprocess(config)
            elif config.mode == ProcessModeEnum.THREAD:
                await self._start_in_process(config)
            else:
                raise ValueError(
                    f"Invalid mode '{config.mode}' for process '{config.name}'. "
                    f"Valid modes: {sorted(VALID_PROCESS_MODES)}"
                )
            if config.note:
                logger.info(f"Note for '{config.name}': {config.note}")
            started = True
            self._unpark(config.name)
        except Exception as exc:
            await self._handle_start_failure(config.name, public_id, exc)
            raise
        finally:
            if not started and config.name in self.active_runs:
                self._cleanup_failed_start(config.name)
                await self._finalize_process_run(
                    config.name,
                    ProcessRunStatusEnum.FAILED,
                    error="start cancelled before spawn",
                )
        await self._finalize_one_shot(config)
        await self._emit_summary_snapshot()
        if config.role is ProcessRoleEnum.STRATEGY:
            await self._emit_strategy_list_snapshot()

    def autostart_includes(self, config: ProcessConfigModel) -> bool:
        """Return whether ``config`` belongs to this node's autostart profile.

        Splits the ingest tier off the FastAPI loop via
        :attr:`AppSettings.process_autostart_profile`:

        - ``ALL``: every process is included (single-container / dev).
        - ``API``: every process EXCEPT market-data publishers (backend).
        - ``FEED``: ONLY market-data publishers (dedicated feed container).

        Identity uses :func:`is_market_data_publisher` on the config tags.
        Comparison is by ``is`` against the enum singletons so a mocked
        settings object (whose attribute is not an enum member) falls
        through to the permissive ``ALL`` behaviour.

        ``STRATEGY`` selects ONLY role-STRATEGY processes — the
        dedicated strategies container's profile.

        Independently of the profile, ``zmq_broker_embedded=False``
        excludes the ``zmq_broker`` process (:func:`is_zmq_broker`) and
        ``strategies_embedded=False`` excludes role-STRATEGY processes
        on the API and ALL branches: a dedicated container then owns
        them, this node must never start duplicates, and ownership
        resolution treats them as remotely managed. Both checks use
        ``is False`` so mocked settings fall through to the embedded
        (permissive) behaviour.

        Args:
            config: The process configuration under consideration.

        Returns:
            True when the process should autostart on this node.
        """
        profile = self.settings.process_autostart_profile
        if profile is ProcessAutostartProfileEnum.FEED:
            return is_market_data_publisher(config.tags)
        if profile is ProcessAutostartProfileEnum.STRATEGY:
            return config.role is ProcessRoleEnum.STRATEGY
        if getattr(self.settings, "zmq_broker_embedded", True) is False and is_zmq_broker(
            config.tags
        ):
            return False
        if (
            getattr(self.settings, "strategies_embedded", True) is False
            and config.role is ProcessRoleEnum.STRATEGY
        ):
            return False
        if profile is ProcessAutostartProfileEnum.API:
            return not is_market_data_publisher(config.tags)
        return True

    async def _resolve_autostart_strategy_scope(
        self, config: ProcessConfigModel
    ) -> ProcessConfigModel:
        """Return ``config`` with boot-time strategy wallet scope resolved.

        Non-strategy configs return unchanged. Strategy configs use their
        persisted operator when present; otherwise boot resolves against
        the admin active-wallet catalogue. Explicit persisted wallet IDs
        pass through unchanged.

        Args:
            config: Enabled process configuration selected for autostart.

        Returns:
            Original or copied config with resolved strategy wallet params.

        Raises:
            StrategyScopeError: Strategy classification or validation failed.
            WalletUnresolvedError: No wallet matched the boot lookup scope.
            WalletAmbiguousError: More than one wallet matched the boot lookup scope.
        """
        classification = classify_strategy_process(
            raw_role=config.role,
            class_path=config.class_path,
            raw_parameters=config.parameters,
        )
        if not classification.treat_as_strategy:
            return config
        repository = get_repository(self.settings.db_url)
        scope = await enforce_classified_strategy_scope_complete(
            repository,
            classification=classification,
            principal_operator_public_ids=None,
            allow_admin_lookup_without_operator=True,
            allow_unscoped_paper=False,
            require_operator_for_explicit_wallet=True,
        )
        parameters = cast(JsonObject, dict(cast(dict[str, object], scope.parameters)))
        return _copy_process_config_with_parameters(
            config,
            parameters,
        )

    async def start_all_processes(self) -> None:
        """Start all enabled processes in priority order.

        Loads configurations, sorts by priority (lower first), and starts
        each enabled process that the current autostart profile selects
        (see :meth:`autostart_includes`). Tracks success/failure counts.

        Raises:
            CoreProcessStartupError: If any enabled CORE process fails to start.
        """
        configs = await self.get_process_configs()
        registry = get_registered_processes()
        configs_with_priority = [
            (config, registry[config.name].priority if config.name in registry else 50)
            for config in configs
        ]
        configs_with_priority.sort(key=lambda x: x[1])
        sorted_configs = [c[0] for c in configs_with_priority]
        logger.info(f"Found {len(sorted_configs)} process configurations")
        started_count = 0
        failed_count = 0
        disabled_count = 0
        filtered_count = 0
        failed_core_names: list[str] = []
        for config in sorted_configs:
            if not config.enabled:
                disabled_count += 1
                continue
            if not self.autostart_includes(config):
                filtered_count += 1
                continue
            try:
                process_config = await self._resolve_autostart_strategy_scope(config)
                self._arm_desired_running(process_config.name)
                await self.start_process(process_config)
                started_count += 1
            except (StrategyScopeError, WalletAmbiguousError, WalletUnresolvedError):
                logger.warning(
                    "strategy {} not started: wallet unresolved/ambiguous; set wallet_public_id",
                    config.name,
                )
                failed_count += 1
            except Exception as e:
                logger.error(f"Failed to start process '{config.name}': {e}")
                failed_count += 1
                if (
                    config.role is ProcessRoleEnum.CORE
                    and config.lifecycle is ProcessLifecycleEnum.LONG_RUNNING
                ):
                    failed_core_names.append(config.name)
        logger.info(
            f"Process startup complete ({self.settings.process_autostart_profile} profile): "
            f"{started_count} started, {failed_count} failed, {disabled_count} disabled, "
            f"{filtered_count} filtered by profile"
        )
        self._start_native_process_monitoring()
        if failed_core_names:
            raise CoreProcessStartupError(failed_core_names)

    async def start_feed_publishers(self) -> None:
        """Start every enabled market-data publisher as its own OS subprocess.

        The dedicated feed-container entrypoint. Loads the synced process
        configs, keeps only enabled market-data publishers (identified by
        :func:`is_market_data_publisher`), and starts each one with its
        mode forced to ``PROCESS`` so it runs in a separate interpreter
        with its own GIL and uvloop event loop — and therefore its own
        CPU core. This is what lifts the single event-loop ceiling: the
        publishers no longer share one loop with each other or the API.

        Unlike :meth:`start_all_processes` this ignores the autostart
        profile (the feed container always wants publishers) and ignores
        each config's registered mode (always ``PROCESS``). Native
        subprocess monitoring is started so a publisher that exits is
        detected, and :meth:`start_process` records the desired-state
        ``RUNNING`` marker so the watchdog supervises each publisher.
        When a publisher later dies unexpectedly the watchdog restarts
        it per its ``restart_policy`` with exponential backoff; only
        after the restart budget is exhausted does a CORE market-data
        publisher escalate to :meth:`wait_for_feed_publisher_failure`
        (the last-resort container trip). A clean exit-0 under an
        ALWAYS policy is restarted but never escalates, sparing the
        paper publisher's benign live-mode idle-exit. CORE startup
        failures escalate the same way as :meth:`start_all_processes`.

        Raises:
            CoreProcessStartupError: If any market-data publisher (all
                CORE, long-running) fails to spawn.
        """
        configs = await self.get_process_configs()
        publishers = [
            config for config in configs if config.enabled and is_market_data_publisher(config.tags)
        ]
        logger.info(
            f"Feed container: starting {len(publishers)} market-data publishers as processes"
        )
        started_count = 0
        failed_core_names: list[str] = []
        for config in publishers:
            process_config = _copy_process_config_with_mode(config, ProcessModeEnum.PROCESS)
            try:
                self._arm_desired_running(process_config.name)
                await self.start_process(process_config)
                started_count += 1
            except Exception as e:
                logger.error(f"Failed to start publisher '{config.name}': {e}")
                if (
                    config.role is ProcessRoleEnum.CORE
                    and config.lifecycle is ProcessLifecycleEnum.LONG_RUNNING
                ):
                    failed_core_names.append(config.name)
        logger.info(
            f"Feed publisher startup complete: {started_count} started, "
            f"{len(failed_core_names)} core failures"
        )
        self._start_native_process_monitoring()
        if failed_core_names:
            raise CoreProcessStartupError(failed_core_names)

    async def wait_for_feed_publisher_failure(self) -> str:
        """Block until the watchdog escalates a feed publisher, then name it.

        Resolves only when the watchdog exhausts the restart budget for a
        CORE market-data publisher (see :meth:`_escalate_restart`) — i.e.
        a publisher that keeps dying (non-zero exit / unhandled exception,
        e.g. kraken_equities crashing during the NYSE-open burst) and
        cannot be healed by per-process restarts. A clean exit-0 under an
        ALWAYS policy is restarted without ever touching the escalation
        counters, so the paper publisher's benign live-mode idle-exit can
        never wake this method. The feed entrypoint treats the returned
        name as fatal and exits non-zero so the orchestrator restarts the
        whole container as a last resort.

        Returns:
            The name of the publisher whose restart budget was exhausted.
        """
        await self._feed_failure_event.wait()
        return self._feed_failed_publisher

    PER_WALLET_REGISTRY_FIXED_FIELDS: frozenset[str] = frozenset(
        {"class", "class_path", "method", "role", "lifecycle", "tags", "restart_policy"}
    )
    """Fields whose values come exclusively from the registry decorator.

    Setting attempts to override these are logged and ignored when
    building per-wallet instance configs. The Setting captures
    operator-tunable runtime config; class identity / role / lifecycle
    / tags / restart_policy are wired by ``@register_process`` and must
    not drift across instances of the same exchange.
    """

    async def _load_template_setting(self, template_name: str) -> dict[str, Any]:
        """Read the active ``process_<template_name>`` Setting value.

        Returns the parsed JSON dict on success. Returns an empty dict
        when the row is absent, the JSON cannot be parsed, or the
        top-level value is not an object. Caller treats empty dict as
        "fall back to registry defaults".

        Args:
            template_name: Process template name (e.g. ``executor_kraken``).

        Returns:
            Parsed Setting JSON dict, or empty dict on absence/parse error.
        """
        repository = get_repository(self.settings.db_url)
        config_key = f"process_{template_name}"
        async with repository.session() as session:
            result = await session.execute(
                select(Setting).where(Setting.key == config_key, *where_active_now(Setting))
            )
            setting = result.scalar_one_or_none()
        if setting is None:
            return {}
        try:
            parsed = json.loads(setting.value)
        except json.JSONDecodeError as exc:
            logger.warning(
                "Per-wallet instance build: failed to parse Setting '{}': {}; "
                "falling back to registry defaults",
                config_key,
                exc,
            )
            return {}
        if not isinstance(parsed, dict):
            logger.warning(
                "Per-wallet instance build: Setting '{}' is not a JSON object "
                "(got {}); falling back to registry defaults",
                config_key,
                type(parsed).__name__,
            )
            return {}
        return parsed

    def _resolve_per_wallet_mode(
        self,
        template_name: str,
        template_config: dict[str, Any],
        entry: ProcessRegistryEntry,
    ) -> ProcessMode:
        """Resolve the execution mode for a per-wallet instance.

        Template Setting may override mode. Invalid values fall back to
        the registry default with a warning rather than aborting the
        instance build.
        """
        template_mode = template_config.get("mode")
        if template_mode is None:
            return entry.mode
        try:
            return resolve_mode(template_mode, template_name)
        except ValueError as exc:
            logger.warning(
                "Per-wallet instance build for '{}': invalid Setting mode '{}' ({}); "
                "falling back to registry mode '{}'",
                template_name,
                template_mode,
                exc,
                entry.mode,
            )
            return entry.mode

    def _build_per_wallet_instance_config(
        self,
        exchange: str,
        wallet_public_id: str,
        entry: ProcessRegistryEntry,
        template_config: dict[str, Any],
    ) -> ProcessConfigModel:
        """Merge registry + template Setting + per-wallet overlay.

        Precedence (lowest → highest):

        1. Registry entry: ``class_path``, ``method``, ``role``,
           ``lifecycle``, ``tags`` are immutable. ``mode`` and
           ``parameters_schema`` provide defaults.
        2. Template ``process_executor_<exchange>`` Setting: may override
           ``parameters``, ``note``, ``parameters_schema``, ``mode``.
           Setting attempts to override registry-fixed fields are logged
           and dropped (registry wins).
        3. Per-instance overlay: ``name = executor_<exchange>_w<short>``,
           ``enabled = True`` (templates are config-only, never runnable),
           ``parameters["wallet_public_id"] = wallet_public_id`` (any
           template-side leak of the same key is stripped first).

        Args:
            exchange: Exchange identifier (e.g. ``kraken``, ``paper``).
            wallet_public_id: Wallet UUID7. First 12 hex chars become
                the instance name suffix.
            entry: Registry entry for ``executor_<exchange>``. Mandatory.
            template_config: Parsed ``process_executor_<exchange>``
                Setting JSON dict. Empty dict when no Setting row is
                active.

        Returns:
            Fully resolved per-wallet ``ProcessConfigModel``.
        """
        template_name = f"executor_{exchange}"
        rejected = sorted(self.PER_WALLET_REGISTRY_FIXED_FIELDS & template_config.keys())
        if rejected:
            logger.warning(
                "Per-wallet instance build for '{}': ignoring Setting overrides for {} "
                "(registry values are authoritative)",
                template_name,
                rejected,
            )
        template_parameters_raw = template_config.get("parameters", {})
        if isinstance(template_parameters_raw, dict):
            template_parameters: dict[str, Any] = {
                key: value
                for key, value in template_parameters_raw.items()
                if key != "wallet_public_id"
            }
        else:
            template_parameters = {}
        parameters: JsonObject = {
            **template_parameters,
            "wallet_public_id": wallet_public_id,
        }
        template_note = template_config.get("note")
        if isinstance(template_note, str) and template_note:
            note: str | None = template_note
        else:
            note = f"Per-wallet executor for exchange={exchange} wallet={wallet_public_id}"
        mode = self._resolve_per_wallet_mode(template_name, template_config, entry)
        template_schema = template_config.get("parameters_schema")
        if isinstance(template_schema, dict):
            parameters_schema: JsonObject | None = template_schema
        else:
            parameters_schema = entry.parameters_schema
        wallet_short = compute_wallet_short(wallet_public_id)
        instance_name = f"executor_{exchange}_w{wallet_short}"
        return ProcessConfigModel(
            name=instance_name,
            enabled=True,
            mode=mode,
            class_path=entry.class_path,
            method=entry.method,
            parameters=parameters,
            note=note,
            lifecycle=entry.lifecycle,
            role=entry.role,
            restart_policy=entry.restart_policy,
            tags=entry.tags,
            parameters_schema=parameters_schema,
        )

    async def _spawn_one_per_wallet_instance(
        self,
        credential: WalletCredentialRow,
        template_configs: dict[str, dict[str, Any]],
    ) -> _PerWalletSpawnOutcome | None:
        """Attempt one per-wallet spawn; return ``None`` when skipped.

        The outcome captures the resolved ``instance_name`` and registry
        ``entry`` plus the optional ``error`` from
        :meth:`start_process`. The caller uses ``error`` + ``entry``
        to decide whether a CORE/LONG_RUNNING failure should escalate
        to :class:`CoreProcessStartupError`.

        This boot-time spawn path is LOCK-FREE by design: it runs once
        during startup, sequentially per credential, BEFORE the
        native-process monitor is armed, so it cannot race a watchdog
        respawn. The operator-triggered manual path
        (:meth:`start_per_wallet_instance_by_name`) takes the per-name
        lock instead.

        Returns:
            ``None`` when the credential is skipped (template missing
            or instance already running). Otherwise a populated
            outcome — ``error=None`` on success, exception otherwise.
        """
        exchange = credential["exchange"]
        wallet_public_id = credential["wallet_public_id"]
        template_name = f"executor_{exchange}"
        registry = get_registered_processes()
        entry = registry.get(template_name)
        if entry is None:
            logger.warning(
                f"Per-wallet spawner: template '{template_name}' not "
                f"registered, skipping wallet={wallet_public_id}"
            )
            return None
        wallet_short = compute_wallet_short(wallet_public_id)
        instance_name = f"executor_{exchange}_w{wallet_short}"
        if instance_name in self.started_processes:
            logger.info(f"Per-wallet spawner: instance '{instance_name}' already running, skipping")
            return None
        if exchange not in template_configs:
            template_configs[exchange] = await self._load_template_setting(template_name)
        instance_config = self._build_per_wallet_instance_config(
            exchange=exchange,
            wallet_public_id=wallet_public_id,
            entry=entry,
            template_config=template_configs[exchange],
        )
        self.instance_configs[instance_name] = instance_config
        try:
            self._arm_desired_running(instance_name)
            await self.start_process(instance_config)
        except Exception as exc:
            self.instance_configs.pop(instance_name, None)
            return _PerWalletSpawnOutcome(instance_name=instance_name, entry=entry, error=exc)
        logger.info(f"Per-wallet spawner: started '{instance_name}' for wallet={wallet_public_id}")
        return _PerWalletSpawnOutcome(instance_name=instance_name, entry=entry, error=None)

    async def spawn_per_wallet_executors(self) -> int:
        """Spawn one executor instance per active ``wallet_credentials`` row.

        Queries ``wallet_credentials`` for every active row and starts a
        per-wallet executor instance for each ``(exchange,
        wallet_public_id)`` pair via :meth:`start_process`. The dynamic
        process name is ``executor_{exchange}_w{wallet_short}`` where
        ``wallet_short`` is the last 12 hex characters of the wallet
        UUID7 (the random portion — see
        :mod:`snapper.core.wallet_short`).

        Each per-wallet ``ProcessConfigModel`` is built by
        :meth:`_build_per_wallet_instance_config`, which merges the
        registry decorator metadata with the active template Setting
        ``process_executor_<exchange>``. The Setting may tune
        ``parameters``, ``note``, ``parameters_schema``, ``mode`` for
        all wallets sharing an exchange; ``class_path``, ``method``,
        ``role``, ``lifecycle``, ``tags`` are fixed by the registry.

        Templates themselves are config-only and never run directly:
        :meth:`get_process_configs` forces ``enabled=False`` on every
        executor template at read time, and both
        :meth:`start_process_by_name` and the REST start endpoint
        reject bare template names. Executors therefore only ever run
        as per-wallet instances — spawned here at boot or manually via
        :meth:`start_per_wallet_instance_by_name`. Each per-wallet
        instance joins the same exchange-prefix topic subscription and
        the wallet filter on
        :meth:`ExchangeExecutorService._is_for_my_wallet` keeps
        cross-wallet messages from spilling into the wrong instance.

        The spawner skips any exchange whose template class is not
        registered (e.g. an exchange-specific build that omits
        ``executor_paper`` from registry). It also catches
        per-instance startup failures and continues with the next
        wallet so a misconfigured credential row never aborts the
        entire boot — failures are logged with full context for the
        operator.

        Returns:
            Number of executor instances successfully started. Zero
            when no credentials exist (e.g. fresh DB before
            ``seed_default_multi_tenant`` runs) or when no exchange
            templates are registered.

        Raises:
            CoreProcessStartupError: If any LONG_RUNNING CORE per-wallet
                executor instance fails to start. Mirrors
                :meth:`start_all_processes` semantics so a missing
                credential row or boot-time CORE failure does not
                silently leave the system without a critical executor.
        """
        repository = get_repository(self.settings.db_url)
        try:
            credentials = await repository.list_active_wallet_credentials(as_of=datetime.now(UTC))
        except Exception as exc:
            logger.error(f"Failed to query wallet_credentials for spawner: {exc}")
            return 0
        if not credentials:
            logger.info("Per-wallet spawner: no wallet credentials, skipping")
            return 0
        template_configs: dict[str, dict[str, Any]] = {}
        spawned = 0
        failed_core_names: list[str] = []
        for credential in credentials:
            outcome = await self._spawn_one_per_wallet_instance(credential, template_configs)
            if outcome is None:
                continue
            if outcome.error is None:
                spawned += 1
                continue
            logger.error(
                f"Per-wallet spawner: failed to start '{outcome.instance_name}' "
                f"for wallet={credential['wallet_public_id']}: {outcome.error}"
            )
            if (
                outcome.entry.role is ProcessRoleEnum.CORE
                and outcome.entry.lifecycle is ProcessLifecycleEnum.LONG_RUNNING
            ):
                failed_core_names.append(outcome.instance_name)
        logger.info(f"Per-wallet spawner: started {spawned} per-wallet executor(s)")
        if spawned > 0:
            await self._emit_configured_snapshot()
        if failed_core_names:
            raise CoreProcessStartupError(failed_core_names)
        return spawned

    async def stop_all_processes(self) -> None:
        """Stop all running processes in reverse priority order.

        Cancels asyncio tasks and stops subprocess instances.
        Cleans up state and clears tracking dictionaries.
        """
        for parked in tuple(self._parked_processes):
            self._unpark(parked)
        logger.info("Stopping all processes")
        registry = get_registered_processes()
        tracked_processes = set(self.process_tasks.keys()) | set(self.started_processes.keys())
        if tracked_processes:
            self.expected_terminations.update(tracked_processes)
        for name in set(self._desired_state):
            self._desired_state[name] = _DesiredState.STOPPED
        await self._cancel_restart_tasks_for_shutdown()
        await self._cancel_tracked_tasks_for_shutdown(registry)
        await self._stop_started_instances_for_shutdown(registry)
        self._clear_process_tracking_state()
        logger.info("All processes stopped")
        await self._emit_summary_snapshot()

    async def _cancel_restart_tasks_for_shutdown(self) -> None:
        """Cancel every pending watchdog restart task during full shutdown.

        Cancels each not-yet-done task in :attr:`_restart_tasks` and
        awaits it with ``asyncio.CancelledError`` suppressed so a
        mid-backoff restart never resurrects a process after
        :meth:`stop_all_processes` has marked it STOPPED.
        """
        for restart_task in self._restart_tasks.values():
            if not restart_task.done():
                restart_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await restart_task

    async def _cancel_tracked_tasks_for_shutdown(
        self, registry: dict[str, ProcessRegistryEntry]
    ) -> None:
        """Cancel tracked asyncio tasks in reverse priority order.

        Sorts :attr:`process_tasks` by registry priority (descending,
        defaulting to 50 for unregistered names such as per-wallet
        instances) and cancels each not-yet-done task, awaiting it with
        ``asyncio.CancelledError`` suppressed. Each cancelled name is
        added to :attr:`expected_terminations` so the completion handler
        treats the cancellation as intentional.

        Args:
            registry: Registered process entries keyed by name, used to
                resolve teardown priority.
        """
        tasks_with_priority = [
            (name, task, registry[name].priority if name in registry else 50)
            for name, task in self.process_tasks.items()
        ]
        tasks_with_priority.sort(key=lambda x: x[2], reverse=True)
        for name, task, _priority in tasks_with_priority:
            if not task.done():
                self.expected_terminations.add(name)
                logger.info(f"Cancelling task '{name}'")
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def _stop_started_instances_for_shutdown(
        self, registry: dict[str, ProcessRegistryEntry]
    ) -> None:
        """Stop started process instances in reverse priority order.

        Sorts :attr:`started_processes` by registry priority (descending,
        defaulting to 50 for unregistered names such as per-wallet
        instances) and stops each instance. A failure to stop one
        instance is logged and never blocks the remaining instances.
        Native subprocess children (:class:`ProcessInstanceInfo`)
        additionally get their spawner bookkeeping cleaned up with all
        exceptions suppressed. Each name is added to
        :attr:`expected_terminations` before its stop so the completion
        handler treats the termination as intentional.

        Args:
            registry: Registered process entries keyed by name, used to
                resolve teardown priority.
        """
        processes_with_priority = [
            (name, instance, registry[name].priority if name in registry else 50)
            for name, instance in self.started_processes.items()
        ]
        processes_with_priority.sort(key=lambda x: x[2], reverse=True)
        for name, instance, _priority in processes_with_priority:
            self.expected_terminations.add(name)
            try:
                logger.info(f"Stopping process '{name}'")
                await instance.stop()
            except Exception as e:
                logger.error(f"Error stopping process '{name}': {e}")
            if isinstance(instance, ProcessInstanceInfo):
                with contextlib.suppress(Exception):
                    self.spawner.cleanup(name)

    def _clear_process_tracking_state(self) -> None:
        """Clear every per-process tracking dictionary after full shutdown.

        Resets task/instance maps, lifecycle and config caches,
        expected-termination markers, run tracking, desired-state flags,
        watchdog restart bookkeeping, and process metrics handles so the
        launcher returns to a pristine state after
        :meth:`stop_all_processes`.
        """
        self.started_processes.clear()
        self.process_tasks.clear()
        self.process_lifecycles.clear()
        self.instance_configs.clear()
        self.expected_terminations.clear()
        self.active_runs.clear()
        self.active_run_started_at.clear()
        self._desired_state.clear()
        self._restart_attempts.clear()
        self._total_failed_restarts.clear()
        self._restart_uptime_start.clear()
        self._restart_configs.clear()
        self._restart_tasks.clear()
        self._restart_locks.clear()
        self._process_metrics.clear()
        self._psutil_handles.clear()

    def _try_chain_result(self, name: str, result: Any) -> bool:
        """Chain a task or coroutine result into a new tracked task.

        Args:
            name: Process name for tracking.
            result: The result from a completed task.

        Returns:
            True if the result was chained, False otherwise.
        """
        if isinstance(result, asyncio.Task):
            result_task: asyncio.Task[object] = result
            self.process_tasks[name] = result_task
            self._register_task_completion(name, result_task)
            return True
        if inspect.iscoroutine(result):
            chained_task = asyncio.create_task(result)
            self.process_tasks[name] = chained_task
            self._register_task_completion(name, chained_task)
            return True
        return False

    def _register_task_completion(self, name: str, task: asyncio.Task[object]) -> None:
        """Register a done-callback that handles task completion or chains results.

        Args:
            name: Process name for tracking.
            task: The asyncio task to monitor.
        """

        def _callback(completed_task: asyncio.Task[Any]) -> None:
            """Schedule async completion handling from the task callback."""
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                logger.warning(
                    "Event loop closed before handling completion for process '{}'", name
                )
                return

            async def _handle_completion() -> None:
                """Process completion and chain coroutine/task results."""
                if not completed_task.cancelled():
                    try:
                        result = completed_task.result()
                    except Exception:
                        await self._handle_task_completion(name, completed_task)
                        return
                    if self._try_chain_result(name, result):
                        return
                await self._handle_task_completion(name, completed_task)

            loop.create_task(_handle_completion())

        task.add_done_callback(_callback)

    def _sample_process_metrics(self) -> None:
        """Sample RSS + CPU% for every native subprocess child.

        Walks ``started_processes`` and, for each
        :class:`ProcessInstanceInfo` (PROCESS-mode child with a live
        pid), maintains a persistent :class:`psutil.Process` handle keyed
        by process NAME. The handle is (re)created when missing or when
        the live ``ProcessInstanceInfo.pid`` differs from the stored
        handle's pid (a watchdog respawn gives the same name a new pid).
        A persistent handle is required because
        ``cpu_percent(interval=None)`` is non-blocking and returns the
        delta since the previous call on the SAME handle: the first call
        after a (re)start reads ``0.0`` and later calls read the real
        utilisation, mirroring
        :meth:`SystemMetricsSnapshotter._sample_cpu_metrics`.

        Results are stored in :attr:`_process_metrics` as
        ``(rss_bytes, cpu_percent)`` for
        :meth:`build_process_summary_items` to surface on the ZMQ
        summary event. A child that has vanished or become inaccessible
        between ticks (``NoSuchProcess`` / ``AccessDenied`` /
        ``ZombieProcess``) records ``(None, None)`` and its handle is
        pruned so a later respawn mints a fresh one. Thread-mode
        processes are skipped entirely (no entry), so their summary rows
        carry ``None`` metrics. Synchronous: every psutil read is a cheap
        /proc access, and the monitor loop owns the only call site, so
        there is no concurrency to coordinate.
        """
        for name, proc in self.started_processes.items():
            if not isinstance(proc, ProcessInstanceInfo):
                continue
            try:
                handle = self._psutil_handles.get(name)
                if handle is None or handle.pid != proc.pid:
                    handle = psutil.Process(proc.pid)
                    self._psutil_handles[name] = handle
                cpu = handle.cpu_percent(interval=None)
                rss = handle.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                self._process_metrics[name] = (None, None)
                self._psutil_handles.pop(name, None)
                continue
            self._process_metrics[name] = (rss, cpu)

    def _start_native_process_monitoring(self) -> None:
        """Start the native-process monitor task when PROCESS children exist."""
        existing_monitor = self.process_tasks.get("_native_monitor")
        if existing_monitor and not existing_monitor.done():
            return
        native_processes = {
            name: proc
            for name, proc in self.started_processes.items()
            if isinstance(proc, ProcessInstanceInfo)
        }
        if not native_processes:
            return
        monitor_task = asyncio.create_task(self._monitor_native_processes())
        self.process_tasks["_native_monitor"] = monitor_task
        logger.info("Started native process monitoring")

    async def _monitor_native_processes(self) -> None:
        """Poll native subprocesses for exit, metrics, and summary events."""
        try:
            while True:
                try:
                    await asyncio.sleep(5)
                    native_processes = {
                        name: proc
                        for name, proc in self.started_processes.items()
                        if isinstance(proc, ProcessInstanceInfo)
                    }
                    if not native_processes:
                        logger.info("No more native processes to monitor, stopping monitor")
                        break
                    for name, proc_info in native_processes.items():
                        status = self.spawner.get_status(name)
                        if not status.running:
                            logger.info(f"Native process '{name}' has completed")
                            await self._handle_process_completion(name, proc_info)
                    self._sample_process_metrics()
                    await self._emit_summary_snapshot()
                except asyncio.CancelledError:
                    logger.info("Native process monitoring cancelled")
                    raise
                except Exception as e:
                    logger.error(f"Error in native process monitoring: {e}")
                    await asyncio.sleep(10)
        finally:
            self.process_tasks.pop("_native_monitor", None)

    def _resolve_native_exit_status(
        self,
        name: str,
        exit_code: int | None,
        lifecycle: ProcessLifecycleEnum,
        expected: bool,
    ) -> tuple[ProcessRunStatusEnum, str | None]:
        """Determine run status and error message from native process exit code.

        Args:
            name: Process name for logging.
            exit_code: Process return code.
            lifecycle: Process lifecycle type.
            expected: Whether termination was expected.

        Returns:
            Tuple of (run_status, error_message).
        """
        if exit_code == 0:
            logger.info(f"Native process '{name}' completed successfully")
            if lifecycle is ProcessLifecycleEnum.LONG_RUNNING and not expected:
                logger.warning(
                    "Long-running native process '{}' exited; autostart unchanged",
                    name,
                )
            if lifecycle is ProcessLifecycleEnum.LONG_RUNNING and expected:
                return ProcessRunStatusEnum.CANCELLED, None
            return ProcessRunStatusEnum.SUCCEEDED, None
        if expected:
            logger.info(
                "Native process '{}' stopped gracefully with exit code {}",
                name,
                exit_code,
            )
            return ProcessRunStatusEnum.CANCELLED, None
        logger.error(
            "Native process '{}' failed with exit code {}; autostart unchanged",
            name,
            exit_code,
        )
        return ProcessRunStatusEnum.FAILED, f"exit_code={exit_code}"

    def _restart_lock_for(self, name: str) -> asyncio.Lock:
        """Return the per-name restart lock, creating it on first use.

        Synchronous (no await) so a caller can acquire the lock without a
        suspension point between lookup and ``async with``. The lock
        serializes all restart decisions and fire-time spawn/stop for a
        single name. Per the NO-REENTRANCY RULE only the documented
        callers acquire it, and no caller acquires two different
        name-locks, so there is no cross-name deadlock.

        Args:
            name: Process name the lock guards.

        Returns:
            The (possibly freshly created) ``asyncio.Lock`` for ``name``.
        """
        lock = self._restart_locks.get(name)
        if lock is None:
            lock = asyncio.Lock()
            self._restart_locks[name] = lock
        return lock

    def _clear_watchdog_state(self, name: str) -> None:
        """Drop every watchdog bookkeeping entry for a process.

        The single terminal-cleanup helper. Called on each terminal
        decision (NEVER policy, ON_FAILURE clean exit, ONE_SHOT,
        give-up/escalation, deliberate stop) so a later deliberate
        re-start gets a fresh restart/backstop budget. Never called while
        an in-flight backoff still needs the state. Also drops the
        per-child resource metrics and the persistent psutil handle so a
        dead process never carries stale RSS/CPU into the summary event
        and a respawn under the same name mints a fresh handle.

        The per-name ``_restart_locks`` entry is deliberately NOT popped
        here: popping a lock object while another caller is queued on it
        would let :meth:`_restart_lock_for` mint a SECOND live lock for
        the same name, losing mutual exclusion and permitting a
        double-spawn. The lock is cheap and per-name; it is garbage
        collected only in the fully-drained context of
        :meth:`stop_all_processes`, after every per-name task has been
        cancelled and joined.

        Args:
            name: Process name to forget.
        """
        self._desired_state.pop(name, None)
        self._restart_attempts.pop(name, None)
        self._restart_uptime_start.pop(name, None)
        self._restart_tasks.pop(name, None)
        self._restart_configs.pop(name, None)
        self._total_failed_restarts.pop(name, None)
        self._process_metrics.pop(name, None)
        self._psutil_handles.pop(name, None)

    def _arm_desired_running(self, name: str) -> None:
        """Declare ``name`` desired-RUNNING (the lock-owned transition).

        :meth:`start_process` no longer writes ``desired=RUNNING`` itself;
        the transition belongs here so only deliberate starters
        own it. The lock-holding manual callers
        (:meth:`start_process_by_name`,
        :meth:`start_per_wallet_instance_by_name`) call this UNDER
        ``_restart_lock_for(name)`` so a start cannot clobber a
        concurrent :meth:`stop_process_by_name`'s ``STOPPED`` marker that
        was set during the stop's pre-lock window: the stop and the start
        serialize on the same per-name lock. The boot-time spawners and
        :meth:`_delayed_restart` also call this; they cannot race an
        operator stop (boot runs once, sequentially, before the
        native-process monitor is armed; the respawn already holds the
        lock and has re-checked desired before arming).

        Args:
            name: Logical process name to mark desired-RUNNING.
        """
        self._desired_state[name] = _DesiredState.RUNNING

    async def _cancel_restart_tasks_locked(self, name: str) -> None:
        """Cancel every pending restart task for ``name``, under the lock.

        A defense-in-depth step: a :meth:`stop_process_by_name` runs
        ``await _cancel_pending_restart`` BEFORE acquiring the per-name
        lock, so a manual start that fails and re-arms recovery during
        that pre-lock window can create a fresh :meth:`_delayed_restart`
        task that the stop's :meth:`_clear_watchdog_state` would merely
        POP (not cancel), leaving an orphan sleeper that could later
        respawn against an already-running name. This helper, called AFTER
        the stop has acquired ``_restart_lock_for(name)``, cancels and
        joins any such task so none survives the stop.

        Unlike :meth:`_cancel_pending_restart`, this is safe to run while
        HOLDING the lock: every task it cancels is either sleeping its
        backoff (outside the lock) or queued on the lock ``acquire``, and
        a cancel lands at that suspension point and unwinds WITHOUT ever
        entering the locked body, so the awaited task never contends for
        the lock the caller holds. Because the caller holds the lock, no
        watchdog path can schedule a NEW task while this runs, so the loop
        is bounded and terminates. A single pop-and-cancel suffices (no
        loop): because the caller holds the lock, no watchdog path can
        schedule a SUCCESSOR while this runs — unlike the pre-lock
        :meth:`_cancel_pending_restart`, whose loop must chase a successor
        that a failed respawn schedules during its own await. The caller
        (the stop) is never itself a restart task, so no current-task
        self-cancel can arise.

        Args:
            name: Logical process name whose restart task to cancel.
        """
        pending = self._restart_tasks.pop(name, None)
        if pending is not None and not pending.done():
            pending.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await pending

    def _has_live_restart_task(self, name: str) -> bool:
        """Return whether a delayed restart task is already pending."""
        existing = self._restart_tasks.get(name)
        return existing is not None and not existing.done()

    def _restart_config_for_status(
        self,
        name: str,
        run_status: ProcessRunStatusEnum,
    ) -> ProcessConfigModel | None:
        """Return the restart config when policy allows a restart."""
        if self._desired_state.get(name) is not _DesiredState.RUNNING:
            return None
        if name in self.expected_terminations:
            return None
        config = self._restart_configs.get(name)
        if config is None:
            return None
        if config.lifecycle is ProcessLifecycleEnum.ONE_SHOT:
            self._clear_watchdog_state(name)
            return None
        if config.restart_policy is ProcessRestartPolicyEnum.NEVER:
            logger.info(f"Process '{name}' died; restart_policy=NEVER, not restarting")
            self._clear_watchdog_state(name)
            return None
        if (
            config.restart_policy is ProcessRestartPolicyEnum.ON_FAILURE
            and run_status is not ProcessRunStatusEnum.FAILED
        ):
            logger.info(f"Process '{name}' exited cleanly under ON_FAILURE; not restarting")
            self._clear_watchdog_state(name)
            return None
        return config

    def _record_failed_restart_attempt(
        self,
        name: str,
        healthy: bool,
        long_healthy: bool,
    ) -> None:
        """Update restart counters for a failed process death."""
        if long_healthy:
            self._total_failed_restarts[name] = 0
        self._total_failed_restarts[name] = self._total_failed_restarts.get(name, 0) + 1
        if healthy:
            self._restart_attempts[name] = 0
            return
        self._restart_attempts[name] = self._restart_attempts.get(name, 0) + 1

    def _record_non_failed_restart_attempt(
        self,
        name: str,
        healthy: bool,
        long_healthy: bool,
    ) -> None:
        """Reset restart counters after a non-failed process death."""
        if long_healthy:
            self._total_failed_restarts[name] = 0
        if healthy:
            self._restart_attempts[name] = 0

    def _record_restart_attempt(
        self,
        name: str,
        run_status: ProcessRunStatusEnum,
    ) -> None:
        """Update restart accounting for the just-observed process death."""
        now = _monotonic()
        uptime = now - self._restart_uptime_start.get(name, now)
        healthy = uptime > _RESTART_HEALTHY_UPTIME_S
        long_healthy = uptime > _TOTAL_RESET_UPTIME_S
        if run_status is ProcessRunStatusEnum.FAILED:
            self._record_failed_restart_attempt(name, healthy, long_healthy)
            return
        self._record_non_failed_restart_attempt(name, healthy, long_healthy)

    def _restart_budget_exhausted(self, name: str) -> bool:
        """Return whether the process exhausted its restart budget."""
        return (
            self._restart_attempts.get(name, 0) >= _MAX_RESTART_ATTEMPTS
            or self._total_failed_restarts.get(name, 0) >= _MAX_TOTAL_FAILED_RESTARTS
        )

    def _schedule_delayed_restart(self, name: str) -> None:
        """Schedule the next delayed restart task for a process."""
        attempts = self._restart_attempts.get(name, 0)
        delay = _compute_backoff_delay(name, attempts)
        logger.info(f"Scheduling restart of '{name}' in {delay:.2f}s (attempt {attempts})")
        self._restart_tasks[name] = asyncio.create_task(self._delayed_restart(name, delay))

    async def _maybe_schedule_restart(self, name: str, run_status: ProcessRunStatusEnum) -> None:
        """Reconcile a dead process toward its desired state (watchdog core).

        Acquires the per-name lock, then immediately bails out as a pure
        no-op when a restart task is already live for the name (the
        live-task guard runs BEFORE any counter/escalation mutation, so a
        re-entrant FAILED completion arriving while a restart sleeps can
        neither burn restart budget nor escalate without a real new
        attempt). Otherwise it decides whether the death
        warrants a restart based on the desired state, the registered
        ``restart_policy``, and the run status. Escalation is driven by
        FAILED deaths ONLY, via two counters: :attr:`_restart_attempts`
        (consecutive short FAILED deaths; reset on a healthy uptime;
        escalate at ``_MAX_RESTART_ATTEMPTS``) and
        :attr:`_total_failed_restarts` (lifetime backstop; reset only on a
        long healthy uptime; escalate at ``_MAX_TOTAL_FAILED_RESTARTS``).
        A clean exit-0 under an ALWAYS policy is restarted but touches
        NEITHER counter, so a healthy cleanly-exiting publisher can never
        trip escalation. When a restart is warranted a delayed respawn
        task is scheduled with the computed backoff.

        Args:
            name: Process name that died.
            run_status: The resolved terminal run status.
        """
        async with self._restart_lock_for(name):
            if self._has_live_restart_task(name):
                logger.info(f"Restart of '{name}' already pending; not scheduling a second")
                return
            config = self._restart_config_for_status(name, run_status)
            if config is None:
                return
            self._record_restart_attempt(name, run_status)
            if self._restart_budget_exhausted(name):
                self._escalate_restart(name, config)
                self._clear_watchdog_state(name)
                return
            self._schedule_delayed_restart(name)

    def _park(self, name: str) -> None:
        """Record a give-up parking and invalidate the health cache.

        The cache invalidation makes ``/health`` flip to ERROR on the
        next probe instead of serving a stale HEALTHY for up to the
        cache TTL.

        Args:
            name: Parked process name.
        """
        self._parked_processes.add(name)
        self._core_health_cache = None

    def _unpark(self, name: str) -> None:
        """End a parked episode and invalidate the health cache.

        Called from the SUCCESS tail of ``start_process`` (an attempted
        start that fails must keep the parked marker — discarding it
        up-front made a failed manual restart of a parked executor
        report HEALTHY while nothing ran) and from the deliberate-stop
        paths (an operator stopping a parked name accepts its state; a
        lingering marker would pin ``/health`` to ERROR forever).

        Args:
            name: Process name whose parked marker to drop.
        """
        if name in self._parked_processes:
            self._parked_processes.discard(name)
            self._core_health_cache = None
        burst = self._park_heartbeat_tasks.pop(name, None)
        if burst is not None:
            burst.cancel()

    def _discard_park_task(self, name: str, done: asyncio.Task[None]) -> None:
        """Drop a finished park-burst task from per-name tracking.

        Identity-checked so a finished old burst can never evict a newer
        one registered for a re-parked name.

        Args:
            name: Process name the burst belonged to.
            done: The completed (or cancelled) burst task.
        """
        if self._park_heartbeat_tasks.get(name) is done:
            self._park_heartbeat_tasks.pop(name, None)

    def _escalate_restart(self, name: str, config: ProcessConfigModel) -> None:
        """Take the last-resort escalation action for an exhausted process.

        A CORE market-data publisher trips the feed-engine container exit
        (reusing the unchanged ``wait_for_feed_publisher_failure``
        contract); anything else is logged as a give-up with no container
        kill. The caller is responsible for the subsequent
        :meth:`_clear_watchdog_state`.

        Args:
            name: Process whose restart budget was exhausted.
            config: The process config (role + tags drive the action).
        """
        if config.role is ProcessRoleEnum.CORE and is_market_data_publisher(config.tags):
            logger.error(
                f"Publisher '{name}' exhausted its restart budget; "
                f"escalating to container restart"
            )
            self._feed_failed_publisher = name
            self._feed_failure_event.set()
            return
        logger.error(
            f"Process '{name}' exhausted its restart budget; giving up (no container restart)"
        )
        self._park(name)
        match = _EXECUTOR_INSTANCE_RE.match(name)
        if match is not None:
            failures = self._restart_attempts.get(name, 0)
            total = self._total_failed_restarts.get(name, 0)
            task = asyncio.create_task(
                self._publish_park_heartbeats(
                    name, match.group("exchange"), match.group("wallet_short"), failures, total
                )
            )
            self._park_heartbeat_tasks[name] = task
            task.add_done_callback(functools.partial(self._discard_park_task, name))

    async def _publish_park_heartbeats(
        self,
        name: str,
        exchange: str,
        wallet_short: str,
        consecutive_failures: int,
        total_failed_restarts: int,
    ) -> None:
        """Publish a 3-frame synthetic ERROR heartbeat burst for a parked executor.

        The parked executor itself can emit nothing — the launcher speaks
        for it ON ITS OWN per-wallet topic, so the entire existing
        pipeline (critical-system-error rule, dedup, fan-out, routing,
        i18n) alerts without any new AlertType, topic family, or wire
        contract member — deliberately sidestepping the regen trap.
        Three frames because the rule gates on 3 consecutive non-HEALTHY
        beats; spacing must exceed the bridge's 1s heartbeat throttle or
        frames 2-3 are dropped and the gate never trips. Once-per-parking
        by construction: the burst fires exactly on the give-up branch,
        then re-bursts every ``_PARK_REBURST_PERIOD_S`` while the name
        stays parked — level-triggered, so a sidecar restart that missed
        a burst still pages on the next one, with the rule's rolling
        cooldown capping pages at about one per hour. Every frame is
        gated on the name still being parked AND ``_unpark`` cancels the
        burst task outright, which also kills a frame whose send is
        SUSPENDED on socket backpressure at the moment the parking is
        accepted; an accepted parking can never complete the rule's
        3-consecutive gate. Send failures are swallowed — the give-up path must
        never raise; the /health flip remains as the level-triggered
        backstop.

        Args:
            name: Parked process instance name (forensics).
            exchange: Executor exchange segment of the heartbeat topic.
            wallet_short: Wallet short hash segment of the topic.
            consecutive_failures: Watchdog failure counter at give-up.
            total_failed_restarts: Lifetime failed-restart counter.
        """
        publisher = self._msg_publisher
        if publisher is None:
            return
        topic = heartbeat_topic("executor", exchange, wallet_short=wallet_short)
        sequence = 0
        while True:
            for _ in range(3):
                if name not in self._parked_processes:
                    logger.info(
                        f"Park heartbeat burst for '{name}' ended - no longer parked "
                        f"(operator stop or successful restart)"
                    )
                    return
                sequence += 1
                try:
                    tracker = publisher.tracker
                    frame = HeartbeatData(
                        public_id=str(uuid7()),
                        timestamp=datetime.now(UTC),
                        session_id=tracker.session_id,
                        sequence_id=tracker.next_sequence(topic),
                        component=f"executor.{exchange}.{wallet_short}",
                        sequence=sequence,
                        status=HealthStatusEnum.ERROR,
                        lag_ms=0,
                        meta={
                            "synthetic": True,
                            "origin": "launcher",
                            "reason": "restart_budget_exhausted",
                            "process_name": name,
                            "consecutive_failures": consecutive_failures,
                            "total_failed_restarts": total_failed_restarts,
                        },
                    )
                    await publisher.send(topic, frame)
                except Exception as exc:
                    logger.error(f"Park heartbeat for '{name}' failed: {exc!r}")
                await asyncio.sleep(_PARK_HEARTBEAT_SPACING_S)
            if name not in self._parked_processes:
                logger.info(
                    f"Park heartbeat burst for '{name}' ended - no longer parked "
                    f"(operator stop or successful restart)"
                )
                return
            await asyncio.sleep(_PARK_REBURST_PERIOD_S)

    def _rearm_recovery_after_manual_start_failure(self, name: str) -> None:
        """Re-arm watchdog recovery after a failed manual start.

        A manual start of a watchdog-managed name cancels the pending
        delayed-restart task (via :meth:`_cancel_pending_restart`) before
        calling :meth:`start_process`. When that start raises, the
        managed-failure branch RETAINS desired=RUNNING and the respawn
        config but would otherwise leave ``_restart_tasks`` empty,
        so nothing would ever re-fire — :meth:`_maybe_schedule_restart`
        only runs from a (now-spent) completion event. This schedules a
        fresh :meth:`_delayed_restart` so the watchdog resumes its backoff
        loop. The backoff delay uses the current consecutive-attempt count
        so the operator's failed retry does not reset the budget.

        The caller's per-name restart lock has already been released by the
        time its ``except`` branch runs, but scheduling a task never
        ACQUIRES that lock (the task acquires it only when its backoff
        elapses), so this respects the no-reentrancy rule. The preceding
        :meth:`_cancel_pending_restart` leaves ``_restart_tasks`` empty;
        the live-task guard here additionally prevents stacking a second
        task if one is somehow still pending.

        The re-arm is gated on ``desired==RUNNING``: a concurrent
        :meth:`stop_process_by_name` that set desired=STOPPED while the
        manual start was in flight owns the lifecycle now, so no successor
        restart task may be created (it would only orphan-sleep and bail at
        its own desired re-check anyway, but creating it means the stop did
        not cleanly win).

        Args:
            name: Logical process name whose recovery must be re-armed.
        """
        if self._desired_state.get(name) is not _DesiredState.RUNNING:
            return
        existing = self._restart_tasks.get(name)
        if existing is not None and not existing.done():
            return
        attempts = self._restart_attempts.get(name, 0)
        delay = _compute_backoff_delay(name, attempts)
        self._restart_tasks[name] = asyncio.create_task(self._delayed_restart(name, delay))

    async def _delayed_restart(self, name: str, delay: float) -> None:
        """Respawn a process after a backoff, honouring a concurrent stop.

        Sleeps the backoff OUTSIDE the lock (so a stop can cancel it
        cleanly), then acquires the per-name lock and respawns only if the
        desired state is still RUNNING. Because ``spawner.spawn`` is
        synchronous (no suspension point), a stop cannot land mid-spawn;
        the post-spawn re-check under the lock tears down the just-spawned
        process if a stop set desired=STOPPED while we were spawning. A
        respawn that itself raises counts as a FAILED death: the
        failed-respawn branch FIRST re-checks the desired state — if a
        concurrent stop flipped it away from RUNNING (or cleared it), it
        clears the watchdog state and returns WITHOUT re-arming or
        rescheduling, so a stop that raced the failing respawn can never
        be resurrected and never orphans a successor task.
        Only when desired is still RUNNING does it bump both escalation
        counters and either escalate or schedule the next backoff — it
        never abandons a still-wanted process. Because
        :meth:`start_process` no longer clears the watchdog
        desired-state/config on a startup exception, those markers
        survive a failed respawn, so the next ``_delayed_restart`` does
        not exit at its desired/config guard.

        Args:
            name: Process name to respawn.
            delay: Backoff delay in seconds.
        """
        await asyncio.sleep(delay)
        async with self._restart_lock_for(name):
            try:
                if self._desired_state.get(name) is not _DesiredState.RUNNING:
                    return
                config = self._restart_configs.get(name)
                if config is None or not config.enabled:
                    self._clear_watchdog_state(name)
                    return
                try:
                    await self.start_process(config)
                except Exception as exc:
                    logger.error(f"Respawn of '{name}' failed: {exc}")
                    if self._desired_state.get(name) is not _DesiredState.RUNNING:
                        logger.info(
                            f"Respawn of '{name}' failed but desired state is no longer "
                            f"RUNNING; a stop won, not re-arming"
                        )
                        self._clear_watchdog_state(name)
                        return
                    self._total_failed_restarts[name] = self._total_failed_restarts.get(name, 0) + 1
                    self._restart_attempts[name] = self._restart_attempts.get(name, 0) + 1
                    if (
                        self._restart_attempts[name] >= _MAX_RESTART_ATTEMPTS
                        or self._total_failed_restarts[name] >= _MAX_TOTAL_FAILED_RESTARTS
                    ):
                        self._escalate_restart(name, config)
                        self._clear_watchdog_state(name)
                        return
                    self._restart_uptime_start[name] = _monotonic()
                    next_delay = _compute_backoff_delay(name, self._restart_attempts[name])
                    self._restart_tasks[name] = asyncio.create_task(
                        self._delayed_restart(name, next_delay)
                    )
                    return
                if self._desired_state.get(name) is _DesiredState.STOPPED:
                    logger.info(
                        f"Stop won during respawn of '{name}'; tearing down just-spawned process"
                    )
                    await self._teardown_started_process(name)
                    self._clear_watchdog_state(name)
            finally:
                if self._restart_tasks.get(name) is asyncio.current_task():
                    self._restart_tasks.pop(name, None)

    async def _teardown_started_process(self, name: str) -> None:
        """Stop and clean up a just-spawned process (lock-free primitive).

        The internal stop primitive used INSIDE a locked region. Per the
        NO-REENTRANCY RULE it MUST NOT acquire ``_restart_lock_for``. Stops
        the tracked instance (which terminates and cleans the subprocess)
        and drops it from the live-process tracking dicts.

        Args:
            name: Process name to tear down.
        """
        instance = self.started_processes.get(name)
        if instance is not None:
            with contextlib.suppress(Exception):
                await instance.stop()
        self.started_processes.pop(name, None)
        self.process_lifecycles.pop(name, None)
        self.process_roles.pop(name, None)

    async def _handle_process_completion(self, name: str, proc_info: ProcessInstanceInfo) -> None:
        """Finalize a dead native subprocess and drive the watchdog.

        Resolves the exit status, finalizes the run record, and schedules
        a watchdog restart per the process's policy. Regardless of whether
        a restart is scheduled, it drops the dead child's per-process
        metrics (set to ``(None, None)``) and prunes its stale psutil
        handle so a ``running=False`` backoff row never reports the old
        pid's RSS/CPU and a respawn mints a fresh handle on the next
        sample. Per-tick fanout is the monitor loop's responsibility: the
        loop emits exactly one summary snapshot after this handler runs
        (see :meth:`_monitor_native_processes`), so this method does NOT
        publish a summary itself, avoiding a double-emit on a death tick.
        A STRATEGY-role process still emits a strategy-list snapshot here
        because the monitor loop does not.

        Args:
            name: Logical process name of the dead subprocess.
            proc_info: The tracked instance info carrying the OS process.
        """
        was_strategy = self.process_roles.get(name) is ProcessRoleEnum.STRATEGY
        try:
            lifecycle = self.process_lifecycles.get(name, ProcessLifecycleEnum.LONG_RUNNING)
            expected = name in self.expected_terminations
            exit_code = proc_info.process.returncode
            run_status, error_message = self._resolve_native_exit_status(
                name, exit_code, lifecycle, expected
            )
            try:
                self.spawner.cleanup(name)
            except Exception as cleanup_error:
                logger.warning(f"Failed to cleanup process '{name}': {cleanup_error}")
            await self._finalize_process_run(
                name, run_status, error=error_message, exit_code=exit_code
            )
            await self._maybe_schedule_restart(name, run_status)
        except Exception as e:
            logger.error(f"Error handling completion of native process '{name}': {e}")
        finally:
            self.process_lifecycles.pop(name, None)
            self.process_roles.pop(name, None)
            self.started_processes.pop(name, None)
            self.expected_terminations.discard(name)
            self._process_metrics[name] = (None, None)
            self._psutil_handles.pop(name, None)
            if was_strategy:
                await self._emit_strategy_list_snapshot()

    def _resolve_task_exception_status(
        self,
        name: str,
        task: asyncio.Task[Any],
    ) -> tuple[ProcessRunStatusEnum, str | None]:
        """Resolve run status when a task completed with an exception.

        Args:
            name: Process name for logging.
            task: The completed asyncio task.

        Returns:
            Tuple of (run_status, error_message).
        """
        exc = task.exception()
        if isinstance(exc, (GeneratorExit, StopAsyncIteration, asyncio.CancelledError)):
            logger.debug(f"Process '{name}' stopped during shutdown: {type(exc).__name__}")
            return ProcessRunStatusEnum.CANCELLED, None
        logger.error(
            "Process '{}' failed with exception: {}; autostart unchanged",
            name,
            exc,
        )
        return ProcessRunStatusEnum.FAILED, str(exc)

    def _resolve_task_success_status(
        self,
        name: str,
        lifecycle: ProcessLifecycleEnum,
        expected: bool,
    ) -> ProcessRunStatusEnum:
        """Resolve run status when a task completed without exception.

        Args:
            name: Process name for logging.
            lifecycle: Process lifecycle type.
            expected: Whether termination was expected.

        Returns:
            Resolved ProcessRunStatusEnum.
        """
        if expected:
            logger.info(f"Process '{name}' stopped gracefully")
        else:
            logger.info(f"Process '{name}' completed successfully")
        if lifecycle is not ProcessLifecycleEnum.LONG_RUNNING:
            return ProcessRunStatusEnum.SUCCEEDED
        if expected:
            return ProcessRunStatusEnum.CANCELLED
        logger.warning(
            "Long-running process '{}' completed unexpectedly; autostart unchanged",
            name,
        )
        return ProcessRunStatusEnum.SUCCEEDED

    def _cleanup_task_tracking(self, name: str, task: asyncio.Task[Any]) -> None:
        """Remove process from all tracking dictionaries.

        Note: ``instance_configs`` is intentionally NOT popped here.
        Per-wallet instances stay tracked across stop/restart cycles
        so the API can render stopped instances with a working Start
        button. ``instance_configs`` is cleared only on full
        :meth:`stop_all_processes` (full reset) and on
        :meth:`_cleanup_failed_start` (the entry was never legitimate).

        Args:
            name: Process name to clean up.
            task: Task to remove if it matches the stored task.
        """
        stored_task = self.process_tasks.get(name)
        if stored_task is task:
            del self.process_tasks[name]
        self.started_processes.pop(name, None)
        self.process_lifecycles.pop(name, None)
        self.process_roles.pop(name, None)
        self.expected_terminations.discard(name)

    async def _handle_task_completion(self, name: str, task: asyncio.Task[Any]) -> None:
        """Finalize an asyncio-task process and drive the restart watchdog.

        Mirrors the native-subprocess completion handler
        (:meth:`_handle_process_completion`): the resolved run status feeds
        :meth:`_maybe_schedule_restart`, so a died THREAD/async-task
        process (executors foremost) is respawned by the same backoff,
        budget, and stop-race machinery as a native feed — previously a
        died executor task was finalized FAILED and silently never
        restarted, a permanent invisible order-execution outage. The
        restart decision runs BEFORE :meth:`_cleanup_task_tracking`
        because the deliberate-stop guard reads ``expected_terminations``,
        which cleanup discards. Watchdog markers are cleared by the
        restart machinery itself (terminal lifecycles, clean exits, stop
        paths), never unconditionally here.

        Args:
            name: Process name whose task completed.
            task: The completed asyncio task.
        """
        try:
            lifecycle = self.process_lifecycles.get(name, ProcessLifecycleEnum.LONG_RUNNING)
            expected = name in self.expected_terminations
            was_strategy = self.process_roles.get(name) is ProcessRoleEnum.STRATEGY
            run_status: ProcessRunStatusEnum = ProcessRunStatusEnum.CANCELLED
            error_message: str | None = None
            if task.cancelled():
                logger.info(f"Process '{name}' was cancelled")
            elif task.exception():
                run_status, error_message = self._resolve_task_exception_status(name, task)
            else:
                run_status = self._resolve_task_success_status(name, lifecycle, expected)
            should_finalize = task.cancelled() or not isinstance(
                task.exception(), (GeneratorExit, StopAsyncIteration)
            )
            try:
                if should_finalize:
                    await self._finalize_process_run(name, run_status, error=error_message)
            finally:
                await self._maybe_schedule_restart(name, run_status)
                self._cleanup_task_tracking(name, task)
            await self._emit_summary_snapshot()
            if was_strategy:
                await self._emit_strategy_list_snapshot()
        except Exception as e:
            if not isinstance(e, (GeneratorExit, StopAsyncIteration, asyncio.CancelledError)):
                logger.error(f"Error handling completion of process '{name}': {e}")

    def _apply_overrides_to_config_dict(
        self,
        config_dict: dict[str, Any],
        mode: ProcessMode | None,
        parameters: dict[str, Any] | None,
    ) -> bool:
        """Apply runtime overrides to a config dictionary.

        Mutates config_dict in place with any non-None overrides and
        sets defaults for missing keys. Overrides are runtime-only and
        do not persist to the database.

        Args:
            config_dict: Mutable config dictionary.
            mode: Optional execution mode override.
            parameters: Optional constructor parameters override.

        Returns:
            The resolved autostart_enabled value.
        """
        autostart_enabled = bool(config_dict.get("enabled", False))
        if mode is not None:
            config_dict["mode"] = mode
        config_dict.setdefault("mode", ProcessModeEnum.THREAD)
        if parameters is not None:
            config_dict["parameters"] = parameters
        config_dict.setdefault("parameters", {})
        return autostart_enabled

    def _build_config_for_start_by_name(
        self,
        name: str,
        config_dict: dict[str, Any],
        autostart_enabled: bool,
    ) -> ProcessConfigModel:
        """Build ProcessConfigModel for start_process_by_name.

        Resolves metadata from the registry and constructs the
        config model. Tags are cleared when parameters_schema is None
        (preserving original behavior).

        Args:
            name: Process name.
            config_dict: Parsed and overridden config dictionary.
            autostart_enabled: Resolved autostart value.

        Returns:
            Fully resolved ProcessConfigModel.
        """
        registry = get_registered_processes()
        entry = registry.get(name)
        lifecycle_raw = config_dict.get("lifecycle")
        if lifecycle_raw is None:
            lifecycle_raw = entry.lifecycle if entry else ProcessLifecycleEnum.LONG_RUNNING
        role_raw = config_dict.get("role")
        if role_raw is None:
            role_raw = entry.role if entry else ProcessRoleEnum.CORE
        restart_policy_raw = config_dict.get("restart_policy")
        if restart_policy_raw is None:
            restart_policy_raw = (
                entry.restart_policy if entry else ProcessRestartPolicyEnum.ON_FAILURE
            )
        tags_raw = entry.tags if entry else None
        if tags_raw is None:
            tags_raw = config_dict.get("tags", ())
        tags_tuple = self._resolve_tags(tags_raw)
        parameters_schema = self._resolve_parameters_schema(config_dict, entry)
        if parameters_schema is None:
            tags_tuple = ()
        return ProcessConfigModel(
            name=name,
            enabled=autostart_enabled,
            mode=resolve_mode(config_dict.get("mode", ProcessModeEnum.THREAD), name),
            class_path=config_dict["class"],
            method=config_dict.get("method", "start"),
            parameters=config_dict.get("parameters", {}),
            note=config_dict.get("note"),
            lifecycle=self._resolve_lifecycle(lifecycle_raw, name),
            role=self._resolve_role(role_raw, name),
            restart_policy=resolve_restart_policy(restart_policy_raw, name),
            tags=tags_tuple,
            parameters_schema=parameters_schema,
            template=config_dict.get("template"),
        )

    async def _handle_manual_start_stop_race(self, name: str) -> ProcessStartResult | None:
        """Tear down a just-started process when a stop won the race.

        The manual lock-takers (:meth:`start_process_by_name`,
        :meth:`start_per_wallet_instance_by_name`) call this immediately
        after :meth:`start_process` returns, still INSIDE their per-name
        restart lock. ``spawner.spawn`` is synchronous (no suspension
        point), so a concurrent :meth:`stop_process_by_name` cannot land
        mid-spawn; instead it set desired=STOPPED synchronously and then
        BLOCKS on the same per-name lock. This post-spawn re-check observes
        that STOPPED marker, tears the just-started process down with the
        same teardown the watchdog respawn uses
        (:meth:`_teardown_started_process`), clears the watchdog state, and
        returns an ERROR result so the racing stop wins: the operator sees
        the process did not stay up, and the subsequent stop finds nothing
        to do. Returns ``None`` when desired is still RUNNING (the start
        won the race and should report success normally). Per the
        NO-REENTRANCY RULE this never acquires ``_restart_lock_for``; the
        caller already holds it.

        Args:
            name: Process name whose start may have been raced by a stop.

        Returns:
            A not-running ``ProcessStartResult`` when a stop won and the
            just-started process was torn down, else ``None``.
        """
        if self._desired_state.get(name) is not _DesiredState.STOPPED:
            return None
        logger.info(f"Stop won during manual start of '{name}'; tearing down just-started process")
        await self._teardown_started_process(name)
        self._clear_watchdog_state(name)
        return ProcessStartResult(
            status=StartProcessStatusEnum.ERROR,
            message=f"Process '{name}' was stopped during start",
        )

    def _already_running_start_result(self, name: str) -> ProcessStartResult | None:
        """Return an ALREADY_RUNNING start result when a process is live."""
        if name not in self.started_processes:
            return None
        logger.warning(f"Process '{name}' is already running")
        return ProcessStartResult(
            status=StartProcessStatusEnum.ALREADY_RUNNING,
            message=f"Process '{name}' is already running",
        )

    def _resolve_per_wallet_start_target(
        self,
        name: str,
    ) -> _PerWalletStartTarget | ProcessStartResult:
        """Resolve a per-wallet instance name to its registry template."""
        parsed = parse_executor_instance(name)
        if parsed is None:
            return ProcessStartResult(
                status=StartProcessStatusEnum.ERROR,
                message=f"'{name}' is not a per-wallet executor instance name",
            )
        exchange, wallet_short = parsed
        template_name = f"executor_{exchange}"
        entry = get_registered_processes().get(template_name)
        if entry is None:
            return ProcessStartResult(
                status=StartProcessStatusEnum.ERROR,
                message=f"Template '{template_name}' is not registered",
            )
        return _PerWalletStartTarget(
            exchange=exchange,
            wallet_short=wallet_short,
            template_name=template_name,
            entry=entry,
        )

    async def _resolve_per_wallet_credential(
        self,
        name: str,
        target: _PerWalletStartTarget,
    ) -> WalletCredentialRow | ProcessStartResult:
        """Return the active wallet credential matching a per-wallet target."""
        repository = get_repository(self.settings.db_url)
        try:
            credentials = await repository.list_active_wallet_credentials(as_of=datetime.now(UTC))
        except Exception as exc:
            logger.error(f"Per-wallet start: failed to query wallet_credentials: {exc}")
            return ProcessStartResult(
                status=StartProcessStatusEnum.ERROR,
                message=f"Cannot query wallet credentials: {exc}",
            )
        match = next(
            (
                credential
                for credential in credentials
                if credential["exchange"] == target.exchange
                and compute_wallet_short(credential["wallet_public_id"]) == target.wallet_short
            ),
            None,
        )
        if match is None:
            return ProcessStartResult(
                status=StartProcessStatusEnum.ERROR,
                message=(
                    f"No active wallet credential for '{name}' "
                    f"(exchange={target.exchange}, wallet prefix={target.wallet_short}); "
                    f"create the credential or use a different instance name"
                ),
            )
        return match

    def _restore_prior_instance_config(
        self,
        name: str,
        prior_instance_config: ProcessConfigModel | None,
    ) -> None:
        """Restore or remove a per-wallet instance config after a failed start."""
        if prior_instance_config is None:
            self.instance_configs.pop(name, None)
            return
        self.instance_configs[name] = prior_instance_config

    def _handle_manual_start_failure(
        self,
        name: str,
        was_watchdog_managed: bool,
    ) -> None:
        """Restore watchdog state after a failed manual start attempt."""
        if was_watchdog_managed:
            self._rearm_recovery_after_manual_start_failure(name)
            return
        self._clear_watchdog_state(name)

    async def _start_prepared_per_wallet_instance(
        self,
        name: str,
        instance_config: ProcessConfigModel,
    ) -> ProcessStartResult:
        """Start a resolved per-wallet instance under the per-name lock."""
        prior_instance_config = self.instance_configs.get(name)
        self.instance_configs[name] = instance_config
        was_watchdog_managed = name in self._desired_state
        await self._cancel_pending_restart(name)
        try:
            async with self._restart_lock_for(name):
                running_result = self._already_running_start_result(name)
                if running_result is not None:
                    if prior_instance_config is not None:
                        self.instance_configs[name] = prior_instance_config
                    return running_result
                self._arm_desired_running(name)
                await self.start_process(instance_config)
                stopped_result = await self._handle_manual_start_stop_race(name)
                if stopped_result is not None:
                    self.instance_configs.pop(name, None)
                    return stopped_result
        except Exception as exc:
            self._restore_prior_instance_config(name, prior_instance_config)
            self._handle_manual_start_failure(name, was_watchdog_managed)
            logger.error(f"Per-wallet start: failed to start '{name}': {exc}")
            return ProcessStartResult(
                status=StartProcessStatusEnum.ERROR,
                message=f"Failed to start '{name}': {exc}",
            )
        self._start_native_process_monitoring()
        public_id = self.active_runs.get(name)
        logger.info(f"Per-wallet start: '{name}' started successfully")
        return ProcessStartResult(
            status=StartProcessStatusEnum.SUCCESS,
            message=f"Process '{name}' started successfully",
            public_id=public_id,
        )

    async def start_per_wallet_instance_by_name(
        self,
        name: str,
        mode: ProcessMode | None = None,
    ) -> ProcessStartResult:
        """Resolve a per-wallet executor instance name and start it.

        For ``name`` matching ``executor_<exchange>_w<wallet_short>``:

        1. Parse the exchange + 12-hex wallet prefix from the name.
        2. Look up the active ``wallet_credentials`` row matching both
           the exchange AND the wallet prefix.
        3. Build the instance config via
           :meth:`_build_per_wallet_instance_config` — same merge logic
           the boot-time spawner uses, so a manual restart picks up
           any Setting edits made since boot.
        4. Apply the optional ``mode`` override on the resolved config
           so the operator's selection in the execution-mode modal
           (Thread vs Process) actually takes effect at start time.
        5. Cancel any pending watchdog restart for the name, then call
           :meth:`start_process` UNDER ``_restart_lock_for(name)`` with an
           in-lock re-check of ``started_processes`` (returning
           ``ALREADY_RUNNING`` if a watchdog respawn won the race), and
           register the live config in ``instance_configs`` so the API
           surface keeps mirroring it. The boot-time per-wallet spawner
           (:meth:`_spawn_one_per_wallet_instance`) does NOT take the lock
           because it runs once at startup, sequentially, before the
           native-process monitor is armed, so it cannot race a watchdog
           respawn.

        Like :meth:`start_process_by_name`, this owns the first-start
        leak cleanup for the markers it caused: a failed start clears the
        watchdog markers only when the instance was NOT already
        watchdog-managed before the start began, so a failed manual start
        of an already-managed instance leaves the watchdog able to
        recover it.

        Args:
            name: Per-wallet instance name in the form
                ``executor_<exchange>_w<wallet_short>``.
            mode: Optional execution mode override (``thread`` or
                ``process``). When set, replaces the mode merged from
                registry + template Setting on the resolved instance
                config. The override is runtime-only and does not
                persist back to the template Setting.

        Returns:
            ``ProcessStartResult`` with status ``SUCCESS`` on a clean
            start, ``ALREADY_RUNNING`` when the instance is already in
            ``started_processes``, or ``ERROR`` when the name does not
            parse, the template is not registered, no matching active
            credential exists, or the underlying ``start_process``
            raises.
        """
        running_result = self._already_running_start_result(name)
        if running_result is not None:
            return running_result
        target = self._resolve_per_wallet_start_target(name)
        if isinstance(target, ProcessStartResult):
            return target
        match = await self._resolve_per_wallet_credential(name, target)
        if isinstance(match, ProcessStartResult):
            return match
        template_config = await self._load_template_setting(target.template_name)
        instance_config = self._build_per_wallet_instance_config(
            exchange=target.exchange,
            wallet_public_id=match["wallet_public_id"],
            entry=target.entry,
            template_config=template_config,
        )
        if mode is not None:
            instance_config.mode = mode
        return await self._start_prepared_per_wallet_instance(name, instance_config)

    async def start_process_by_name(
        self,
        name: str,
        mode: ProcessMode | None = None,
        parameters: dict[str, Any] | None = None,
    ) -> ProcessStartResult:
        """Start a process by its registered name.

        Overrides (mode, parameters) are applied at runtime only and
        are not persisted back to the database.

        Per-wallet executor instance names (``executor_<exchange>_w<short>``)
        are routed to :meth:`start_per_wallet_instance_by_name` which
        resolves the matching ``wallet_credentials`` row and rebuilds
        the instance config. Bare executor template names
        (``executor_<exchange>``) are rejected as ERROR — templates
        are config-only and never directly runnable.

        This manual lock-taker owns the first-start-leak cleanup for the
        markers it caused: if the name was NOT already watchdog-managed
        when the start began and :meth:`start_process` then raises, it
        clears the watchdog markers so a failed manual start of a
        brand-new name leaks nothing. Conversely, a failed manual start
        of an already-managed name (e.g. a publisher mid-backoff) does
        NOT clear the markers, so the watchdog can still recover it.

        Args:
            name: Process name from registry.
            mode: Execution mode override (thread/process).
            parameters: Constructor parameters override for the process.

        Returns:
            Typed result with operation status, message, and optional public_id.
        """
        if is_executor_instance(name):
            return await self.start_per_wallet_instance_by_name(name, mode=mode)
        if is_executor_template(name):
            return ProcessStartResult(
                status=StartProcessStatusEnum.ERROR,
                message=(
                    f"'{name}' is an executor template — start "
                    f"'{name}_w<wallet_short>' for a specific wallet"
                ),
            )
        if name in self.started_processes:
            logger.warning(f"Process '{name}' is already running")
            return ProcessStartResult(
                status=StartProcessStatusEnum.ALREADY_RUNNING,
                message=f"Process '{name}' is already running",
            )
        repository = get_repository(self.settings.db_url)
        config_key = f"process_{name}"
        config_dict: dict[str, Any] = {}
        async with repository.session() as session:
            result = await session.execute(
                select(Setting).where(Setting.key == config_key, *where_active_now(Setting))
            )
            setting = result.scalar_one_or_none()
            if not setting:
                return ProcessStartResult(
                    status=StartProcessStatusEnum.ERROR,
                    message=f"Process '{name}' not found in configuration",
                )
            config_dict = json.loads(setting.value)
            autostart_enabled = self._apply_overrides_to_config_dict(config_dict, mode, parameters)
            config = self._build_config_for_start_by_name(name, config_dict, autostart_enabled)
        raw_tags = tuple(config_dict.get("tags") or ())
        if getattr(self.settings, "zmq_broker_embedded", True) is False and (
            is_zmq_broker(config.tags) or is_zmq_broker(raw_tags)
        ):
            return ProcessStartResult(
                status=StartProcessStatusEnum.ERROR,
                message=(
                    f"Process '{name}' is owned by the dedicated broker container "
                    "(ZMQ_BROKER_EMBEDDED=false) — starting a local duplicate would "
                    "bind a second bus; manage it via docker instead"
                ),
            )
        raw_role = str(config_dict.get("role") or "")
        if getattr(self.settings, "strategies_embedded", True) is False and (
            config.role is ProcessRoleEnum.STRATEGY or raw_role == ProcessRoleEnum.STRATEGY.value
        ):
            return ProcessStartResult(
                status=StartProcessStatusEnum.ERROR,
                message=(
                    f"Process '{name}' is owned by the strategies container "
                    "(STRATEGIES_EMBEDDED=false) — starting a local duplicate would "
                    "run the strategy twice; manage it via the strategies container"
                ),
            )
        was_watchdog_managed = name in self._desired_state
        await self._cancel_pending_restart(name)
        try:
            async with self._restart_lock_for(name):
                if name in self.started_processes:
                    logger.warning(f"Process '{name}' is already running")
                    return ProcessStartResult(
                        status=StartProcessStatusEnum.ALREADY_RUNNING,
                        message=f"Process '{name}' is already running",
                    )
                self._arm_desired_running(name)
                await self.start_process(config)
                stopped_result = await self._handle_manual_start_stop_race(name)
                if stopped_result is not None:
                    return stopped_result
        except Exception as e:
            if was_watchdog_managed:
                self._rearm_recovery_after_manual_start_failure(name)
            else:
                self._clear_watchdog_state(name)
            logger.error(f"Failed to start process '{name}': {e}")
            return ProcessStartResult(
                status=StartProcessStatusEnum.ERROR,
                message=f"Failed to start process '{name}': {str(e)}",
            )
        self._start_native_process_monitoring()
        public_id = self.active_runs.get(name)
        if config.lifecycle is ProcessLifecycleEnum.ONE_SHOT:
            return ProcessStartResult(
                status=StartProcessStatusEnum.SUCCESS,
                message=f"Process '{name}' executed successfully",
                public_id=public_id,
            )
        logger.info(f"Process '{name}' started successfully")
        return ProcessStartResult(
            status=StartProcessStatusEnum.SUCCESS,
            message=f"Process '{name}' started successfully",
            public_id=public_id,
        )

    async def _cancel_pending_restart(self, name: str) -> None:
        """Cancel and join every pending delayed-restart task for ``name``.

        Pops the ``_restart_tasks`` entry and cancels it if it is still
        live. The cancel lands cleanly because the only cancellable point
        in :meth:`_delayed_restart` is the backoff ``asyncio.sleep``,
        which runs OUTSIDE the per-name lock; a task still queued on the
        lock is interrupted at its ``acquire`` await. MUST be called
        WITHOUT holding ``_restart_lock_for(name)`` so the awaited task
        can never deadlock against the caller.

        Loops until no live task remains for the name: a failed-respawn
        task can schedule a SUCCESSOR ``_delayed_restart`` into
        ``_restart_tasks[name]`` while it runs, so cancelling a single
        entry is not enough — awaiting the cancelled task lets that
        successor be observed and cancelled in turn. The
        bound is the escalation ceiling, so the loop always terminates.

        Args:
            name: Process name whose pending restarts should be dropped.
        """
        while True:
            pending = self._restart_tasks.pop(name, None)
            if pending is None:
                return
            if not pending.done():
                pending.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await pending

    async def _cancel_process_task(self, name: str) -> None:
        """Cancel and await the asyncio task for a process.

        Args:
            name: Process name whose task should be cancelled.
        """
        if name not in self.process_tasks:
            return
        task = self.process_tasks[name]
        if not task.done():
            logger.info(f"Cancelling task '{name}'")
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        del self.process_tasks[name]

    async def stop_process_by_name(self, name: str) -> ProcessStopResult:
        """Stop a running process by name.

        Emits a ``processes.events.summary`` snapshot — and a
        ``strategies.events.list`` snapshot when the stopped process
        carried the STRATEGY role — after the spawner releases so
        subscribers refresh without polling.

        A stop always wins, even when no live process entry exists:
        desired=STOPPED is set and any pending delayed-restart task is
        cancelled FIRST (synchronously, OUTSIDE the lock — the cancel must
        await the cancelled task, which only re-acquires the lock after its
        backoff sleep and is already cancelled, so there is no contention
        and no self-deadlock). The not-running check, the real stop, and
        ``_clear_watchdog_state`` then ALL run INSIDE the per-name restart
        lock so the stop serializes against an in-flight manual
        start or a watchdog respawn that holds the same lock: the stop
        waits, then either sees the now-started process and stops it, or
        the in-flight start sees desired=STOPPED in its post-spawn re-check
        and tears the just-started process down. The not-running path
        clears the watchdog state so the STOPPED marker never leaks.

        Once the lock is held the stop re-cancels every restart task for
        the name via :meth:`_cancel_restart_tasks_locked`: a manual
        start that failed and re-armed recovery during the pre-lock
        ``_cancel_pending_restart`` window could have created a fresh
        delayed-restart task that ``_clear_watchdog_state`` would only POP
        (not cancel), leaving an orphan sleeper that could later respawn
        against an already-running name. Re-cancelling under the lock
        guarantees no restart task survives the stop, regardless of the
        pre-lock window.

        Args:
            name: Process name to stop.

        Returns:
            Typed result with operation status and message.
        """
        self._desired_state[name] = _DesiredState.STOPPED
        await self._cancel_pending_restart(name)
        was_strategy = self.process_roles.get(name) is ProcessRoleEnum.STRATEGY
        try:
            async with self._restart_lock_for(name):
                await self._cancel_restart_tasks_locked(name)
                if name not in self.started_processes:
                    logger.warning(f"Process '{name}' is not running")
                    self._clear_watchdog_state(name)
                    self._unpark(name)
                    return ProcessStopResult(
                        status=StopProcessStatusEnum.NOT_RUNNING,
                        message=f"Process '{name}' is not running",
                    )
                self.expected_terminations.add(name)
                await self._cancel_process_task(name)
                instance = self.started_processes.get(name)
                if instance is not None:
                    logger.info(f"Stopping process '{name}'")
                    await instance.stop()
                self.started_processes.pop(name, None)
                self.process_lifecycles.pop(name, None)
                self.process_roles.pop(name, None)
                logger.info(f"Process '{name}' stopped successfully")
                await self._finalize_process_run(name, ProcessRunStatusEnum.CANCELLED)
                self._clear_watchdog_state(name)
                self._unpark(name)
            await self._emit_summary_snapshot()
            if was_strategy:
                await self._emit_strategy_list_snapshot()
            return ProcessStopResult(
                status=StopProcessStatusEnum.SUCCESS,
                message=f"Process '{name}' stopped successfully",
            )
        except Exception as e:
            logger.error(f"Failed to stop process '{name}': {e}")
            return ProcessStopResult(status=StopProcessStatusEnum.ERROR, message=str(e))
        finally:
            self.expected_terminations.discard(name)

    async def get_process_status(self, name: str) -> ProcessStatusResult:
        """Get current status of a process.

        Args:
            name: Process name to query.

        Returns:
            Typed status with running state, role, lifecycle and details.
        """
        is_running = name in self.started_processes
        details: JsonObject | None = None
        instance = self.started_processes.get(name)
        if instance:
            try:
                details = instance.get_status()
            except Exception as e:
                logger.warning(f"Failed to get status from process '{name}': {e}")
        await asyncio.sleep(0)
        return ProcessStatusResult(
            name=name,
            running=is_running,
            role=(self.process_roles.get(name) or ProcessRoleEnum.CORE),
            lifecycle=self.process_lifecycles.get(name, ProcessLifecycleEnum.LONG_RUNNING),
            active_public_id=self.active_runs.get(name),
            details=details,
        )

    async def get_recent_runs(
        self,
        *,
        limit: int = 50,
        name: str | None = None,
    ) -> list[dict[str, Any]]:
        """Retrieve recent process run records.

        Args:
            limit: Maximum number of records to return.
            name: Optional process name filter.

        Returns:
            List of run record dictionaries.
        """
        return await self._run_recorder.get_recent_runs(limit=limit, name=name)

    async def get_core_health(self) -> HealthStatus:
        """Check health of enabled long-running CORE processes.

        Returns "healthy" when all enabled long-running CORE processes are
        running, or "error" when any are missing. Disabled CORE processes
        and completed one-shot CORE processes are ignored.

        Bare executor templates (``executor_<exchange>``) are skipped:
        they are config-only entries expanded into per-wallet instances
        by :meth:`spawn_per_wallet_executors`. Instance-level CORE
        startup failures already escalate via
        :class:`CoreProcessStartupError` at boot, so health checks do
        not need to re-validate each per-wallet instance individually.

        In API-only mode (no autostart), returns "healthy" unless a
        manually started process has PARKED (restart budget exhausted) —
        the parked check runs before every other branch, including this
        one and the TTL cache, because it is the only level-triggered
        backstop for a parked executor whose alert burst was lost.

        Returns:
            "healthy" or "error" as HealthStatus string.

        The result of the configs scan is cached for
        ``_CORE_HEALTH_CACHE_TTL_S`` seconds on a monotonic clock to
        avoid a Postgres roundtrip on every probe — the endpoint is
        hit by the frontend market-data poll, the Docker healthcheck,
        and operator dashboards, and ``process_configs`` shares the
        connection pool with the kraken tick-writer which can stall
        the query up to ~1.5s under prod load. Cache staleness of up
        to ``_CORE_HEALTH_CACHE_TTL_S`` is acceptable: container
        orchestrators probe every 30s and operator dashboards refresh
        every 5-10s, both within tolerance for noticing a freshly-
        died CORE process.
        """
        if self._parked_processes:
            self._core_health_cache = (time.monotonic(), HealthStatusEnum.ERROR)
            return HealthStatusEnum.ERROR
        if self.settings.server_api_only:
            return HealthStatusEnum.HEALTHY
        now = time.monotonic()
        cached = self._core_health_cache
        if cached is not None and (now - cached[0]) < _CORE_HEALTH_CACHE_TTL_S:
            return cached[1]
        configs = await self.get_process_configs()
        status: HealthStatus = HealthStatusEnum.HEALTHY
        for config in configs:
            if is_executor_template(config.name):
                continue
            if (
                config.enabled
                and self.autostart_includes(config)
                and config.role is ProcessRoleEnum.CORE
                and config.lifecycle is ProcessLifecycleEnum.LONG_RUNNING
                and config.name not in self.started_processes
            ):
                status = HealthStatusEnum.ERROR
                break
        self._core_health_cache = (now, status)
        return status

    async def sync_registry_to_database(self) -> None:
        """Delegate to registry_syncer.sync_registry_to_database."""
        await self._registry_syncer.sync_registry_to_database()

    async def create_process_config(
        self,
        *,
        name: str,
        class_path: str,
        method: str,
        enabled: bool,
        mode: ProcessMode,
        parameters: dict[str, Any],
        lifecycle: ProcessLifecycleEnum,
        role: ProcessRoleEnum,
        tags: Iterable[str],
        parameters_schema: JsonObject | None = None,
        note: str | None = None,
        template: str | None = None,
    ) -> None:
        """Create a new process configuration in the database.

        Emits a ``processes.events.configured`` snapshot after the
        insert commits — and a ``strategies.events.list`` snapshot
        when the new config carries the STRATEGY role — so subscribers
        refresh without polling.

        Args:
            name: Process name.
            class_path: Fully qualified class path.
            method: Method to invoke on the class.
            enabled: Whether the process is enabled.
            mode: Execution mode (thread/process).
            parameters: Constructor parameters dict.
            lifecycle: Process lifecycle type.
            role: Process role classification.
            tags: Process tags for categorization.
            parameters_schema: Optional JSON schema for parameters.
            note: Optional descriptive note.
            template: Source-template registry name persisted on the row.
        """
        await self._registry_syncer.create_process_config(
            name=name,
            class_path=class_path,
            method=method,
            enabled=enabled,
            mode=mode,
            parameters=parameters,
            lifecycle=lifecycle,
            role=role,
            tags=tags,
            parameters_schema=parameters_schema,
            note=note,
            template=template,
        )
        await self._emit_configured_snapshot()
        await self._emit_summary_snapshot()
        if role is ProcessRoleEnum.STRATEGY:
            await self._emit_strategy_list_snapshot()
