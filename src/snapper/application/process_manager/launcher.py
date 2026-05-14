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
import inspect
import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from typing import Any
from uuid import uuid7

from loguru import logger
from sqlalchemy import select

from snapper.application.process_manager.config_resolver import VALID_PROCESS_MODES
from snapper.application.process_manager.config_resolver import get_process_configs
from snapper.application.process_manager.config_resolver import import_process_class
from snapper.application.process_manager.config_resolver import resolve_lifecycle
from snapper.application.process_manager.config_resolver import resolve_mode
from snapper.application.process_manager.config_resolver import resolve_parameters_schema
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
from snapper.application.services.market_persist_policy import MarketPersistPolicy
from snapper.config.settings import AppSettings
from snapper.core.json_types import JsonObject
from snapper.core.types import HealthStatus
from snapper.core.types import HealthStatusEnum
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessMode
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.core.types import ProcessRunStatusEnum
from snapper.core.types import StartProcessStatusEnum
from snapper.core.types import StopProcessStatusEnum
from snapper.core.wallet_short import compute_wallet_short
from snapper.data.models import Setting
from snapper.data.repository import get_repository
from snapper.data.repository import where_active_now
from snapper.data.repository_types import WalletCredentialRow
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.schemas.data import ProcessConfiguredEventData
from snapper.messaging.schemas.data import ProcessRunEventData
from snapper.messaging.schemas.data import ProcessSummaryEventData
from snapper.messaging.schemas.data import ProcessSummaryItem
from snapper.messaging.schemas.data import StrategyListEventData

_PROCESSES_SUMMARY_STREAM = "processes.events.summary"
_PROCESSES_CONFIGURED_STREAM = "processes.events.configured"
_PROCESSES_RUNS_STREAM = "processes.events.runs"
_STRATEGIES_LIST_STREAM = "strategies.events.list"
_BROADCAST_FAILURE_TEMPLATE = "Failed to broadcast {}: {}"


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
        self._run_recorder = ProcessRunRecorder(settings)
        self._registry_syncer = ProcessRegistrySyncer(settings)
        self._market_persist_policy: MarketPersistPolicy | None = None
        self._msg_publisher: MessagePublisher | None = None

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

    async def _build_process_summary_items(self) -> list[ProcessSummaryItem]:
        """Compose a snapshot of every tracked process row.

        Joins persisted configs from :meth:`get_process_configs` (the
        source of truth for ``enabled``) with runtime per-wallet
        instances held in ``instance_configs``. The result is the
        launcher's authoritative "what exists right now" view —
        consumers invalidate their cache against this signal and
        re-fetch via REST for full detail.
        """
        configs = await self.get_process_configs()
        items: list[ProcessSummaryItem] = []
        seen: set[str] = set()
        for config in configs:
            name = config.name
            items.append(
                ProcessSummaryItem(
                    name=name,
                    running=name in self.started_processes,
                    enabled=config.enabled,
                    role=config.role.value,
                    lifecycle=config.lifecycle.value,
                    active_public_id=self.active_runs.get(name),
                )
            )
            seen.add(name)
        for name, instance_config in self.instance_configs.items():
            if name in seen:
                continue
            items.append(
                ProcessSummaryItem(
                    name=name,
                    running=name in self.started_processes,
                    enabled=instance_config.enabled,
                    role=instance_config.role.value,
                    lifecycle=instance_config.lifecycle.value,
                    active_public_id=self.active_runs.get(name),
                )
            )
        return items

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
        topic = f"{_PROCESSES_SUMMARY_STREAM}.{self.settings.coordinator_instance_id}"
        try:
            items = await self._build_process_summary_items()
            tracker = self._msg_publisher.tracker
            payload = ProcessSummaryEventData(
                session_id=tracker.session_id,
                sequence_id=tracker.next_sequence(topic),
                public_id=str(uuid7()),
                timestamp=datetime.now(UTC),
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
        topic = f"{_PROCESSES_CONFIGURED_STREAM}.{self.settings.coordinator_instance_id}"
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
        topic = f"{_STRATEGIES_LIST_STREAM}.{self.settings.coordinator_instance_id}"
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
        per the v1 deployment contract documented in plan v6 step 8.

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

    def import_class(self, class_path: str, process_name: str | None = None) -> type:
        """Import a class by its fully qualified path.

        Args:
            class_path: Fully qualified class path.
            process_name: Optional process name to check registry first.

        Returns:
            The imported class type.
        """
        return import_process_class(class_path, process_name)

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
        )
        logger.info(f"Process '{config.name}' started with PID {process_info.pid}")
        self.started_processes[config.name] = process_info

    async def _start_in_process(self, config: ProcessConfigModel) -> None:
        """Instantiate the class and launch as async task or thread executor.

        Args:
            config: Process configuration.
        """
        process_class = self.import_class(config.class_path, config.name)
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

        Args:
            config: Process configuration.
        """
        if not self._is_one_shot_completed(config):
            return
        await self._finalize_process_run(config.name, ProcessRunStatusEnum.SUCCEEDED)
        self.started_processes.pop(config.name, None)
        self.process_lifecycles.pop(config.name, None)
        self.process_roles.pop(config.name, None)

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

        Args:
            config: Process configuration to start.

        Raises:
            Exception: Re-raised from process startup failures.
        """
        self.process_lifecycles[config.name] = config.lifecycle
        self.process_roles[config.name] = config.role
        public_id = await self._try_create_run_record(config)
        try:
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
        except Exception as exc:
            await self._handle_start_failure(config.name, public_id, exc)
            raise
        await self._finalize_one_shot(config)
        await self._emit_summary_snapshot()
        if config.role is ProcessRoleEnum.STRATEGY:
            await self._emit_strategy_list_snapshot()

    async def start_all_processes(self) -> None:
        """Start all enabled processes in priority order.

        Loads configurations, sorts by priority (lower first),
        and starts each enabled process. Tracks success/failure counts.

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
        failed_core_names: list[str] = []
        for config in sorted_configs:
            if config.enabled:
                try:
                    await self.start_process(config)
                    started_count += 1
                except Exception as e:
                    logger.error(f"Failed to start process '{config.name}': {e}")
                    failed_count += 1
                    if (
                        config.role is ProcessRoleEnum.CORE
                        and config.lifecycle is ProcessLifecycleEnum.LONG_RUNNING
                    ):
                        failed_core_names.append(config.name)
            else:
                disabled_count += 1
        logger.info(
            f"Process startup complete: {started_count} started, "
            f"{failed_count} failed, {disabled_count} disabled"
        )
        self._start_native_process_monitoring()
        if failed_core_names:
            raise CoreProcessStartupError(failed_core_names)

    PER_WALLET_REGISTRY_FIXED_FIELDS: frozenset[str] = frozenset(
        {"class", "class_path", "method", "role", "lifecycle", "tags"}
    )
    """Fields whose values come exclusively from the registry decorator.

    Setting attempts to override these are logged and ignored when
    building per-wallet instance configs. The Setting captures
    operator-tunable runtime config; class identity / role / lifecycle
    / tags are wired by ``@register_process`` and must not drift across
    instances of the same exchange.
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

        The spawn loop is intentionally **additive**: executor templates
        registered with ``enabled=True`` continue to run as the legacy
        single-wallet path. Each per-wallet instance joins the same
        exchange-prefix topic subscription and the wallet filter on
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
        logger.info("Stopping all processes")
        registry = get_registered_processes()
        tracked_processes = set(self.process_tasks.keys()) | set(self.started_processes.keys())
        if tracked_processes:
            self.expected_terminations.update(tracked_processes)
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
        self.started_processes.clear()
        self.process_tasks.clear()
        self.process_lifecycles.clear()
        self.instance_configs.clear()
        self.expected_terminations.clear()
        self.active_runs.clear()
        self.active_run_started_at.clear()
        logger.info("All processes stopped")
        await self._emit_summary_snapshot()

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
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                logger.warning(
                    "Event loop closed before handling completion for process '{}'", name
                )
                return

            async def _handle_completion() -> None:
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

    def _start_native_process_monitoring(self) -> None:
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

    async def _handle_process_completion(self, name: str, proc_info: ProcessInstanceInfo) -> None:
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
        except Exception as e:
            logger.error(f"Error handling completion of native process '{name}': {e}")
        finally:
            self.process_lifecycles.pop(name, None)
            self.process_roles.pop(name, None)
            self.started_processes.pop(name, None)
            self.expected_terminations.discard(name)
            await self._emit_summary_snapshot()
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
            self._cleanup_task_tracking(name, task)
            should_finalize = task.cancelled() or not isinstance(
                task.exception(), (GeneratorExit, StopAsyncIteration)
            )
            if should_finalize:
                await self._finalize_process_run(name, run_status, error=error_message)
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
            tags=tags_tuple,
            parameters_schema=parameters_schema,
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
        5. Call :meth:`start_process` and register the live config in
           ``instance_configs`` so the API surface keeps mirroring it.

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
        if name in self.started_processes:
            logger.warning(f"Process '{name}' is already running")
            return ProcessStartResult(
                status=StartProcessStatusEnum.ALREADY_RUNNING,
                message=f"Process '{name}' is already running",
            )
        parsed = parse_executor_instance(name)
        if parsed is None:
            return ProcessStartResult(
                status=StartProcessStatusEnum.ERROR,
                message=f"'{name}' is not a per-wallet executor instance name",
            )
        exchange, wallet_short = parsed
        registry = get_registered_processes()
        template_name = f"executor_{exchange}"
        entry = registry.get(template_name)
        if entry is None:
            return ProcessStartResult(
                status=StartProcessStatusEnum.ERROR,
                message=f"Template '{template_name}' is not registered",
            )
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
                cred
                for cred in credentials
                if cred["exchange"] == exchange
                and compute_wallet_short(cred["wallet_public_id"]) == wallet_short
            ),
            None,
        )
        if match is None:
            return ProcessStartResult(
                status=StartProcessStatusEnum.ERROR,
                message=(
                    f"No active wallet credential for '{name}' "
                    f"(exchange={exchange}, wallet prefix={wallet_short}); "
                    f"create the credential or use a different instance name"
                ),
            )
        template_config = await self._load_template_setting(template_name)
        instance_config = self._build_per_wallet_instance_config(
            exchange=exchange,
            wallet_public_id=match["wallet_public_id"],
            entry=entry,
            template_config=template_config,
        )
        if mode is not None:
            instance_config.mode = mode
        prior_instance_config = self.instance_configs.get(name)
        self.instance_configs[name] = instance_config
        try:
            await self.start_process(instance_config)
        except Exception as exc:
            if prior_instance_config is not None:
                self.instance_configs[name] = prior_instance_config
            else:
                self.instance_configs.pop(name, None)
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
        try:
            await self.start_process(config)
        except Exception as e:
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

        Args:
            name: Process name to stop.

        Returns:
            Typed result with operation status and message.
        """
        if name not in self.started_processes:
            logger.warning(f"Process '{name}' is not running")
            return ProcessStopResult(
                status=StopProcessStatusEnum.NOT_RUNNING,
                message=f"Process '{name}' is not running",
            )
        was_strategy = self.process_roles.get(name) is ProcessRoleEnum.STRATEGY
        try:
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

        In API-only mode (no autostart), returns "healthy" unconditionally
        since processes are intentionally not started.

        Returns:
            "healthy" or "error" as HealthStatus string.
        """
        if self.settings.server_api_only:
            return HealthStatusEnum.HEALTHY
        configs = await self.get_process_configs()
        for config in configs:
            if is_executor_template(config.name):
                continue
            if (
                config.enabled
                and config.role is ProcessRoleEnum.CORE
                and config.lifecycle is ProcessLifecycleEnum.LONG_RUNNING
                and config.name not in self.started_processes
            ):
                return HealthStatusEnum.ERROR
        return HealthStatusEnum.HEALTHY

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
        )
        await self._emit_configured_snapshot()
        await self._emit_summary_snapshot()
        if role is ProcessRoleEnum.STRATEGY:
            await self._emit_strategy_list_snapshot()
