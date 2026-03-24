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
from datetime import UTC
from datetime import datetime
from typing import Any

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
from snapper.application.process_manager.enums import ProcessLifecycleEnum
from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.enums import ProcessRunStatusEnum
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
from snapper.config.settings import AppSettings
from snapper.core.types import ProcessMode
from snapper.data.models import Setting
from snapper.data.repository import Repository
from snapper.data.repository import close_and_insert
from snapper.data.repository import get_repository
from snapper.data.repository import where_active_now
from snapper.messaging.infrastructure.publisher import SequenceTracker

_SETTINGS_TOPIC = "settings"


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
        self.active_runs: dict[str, str] = {}
        self.spawner = ProcessSpawnerService()
        self.expected_terminations: set[str] = set()
        self._run_recorder = ProcessRunRecorder(settings)
        self._registry_syncer = ProcessRegistrySyncer(settings)
        self._tracker = SequenceTracker()

    async def _create_process_run_record(
        self,
        config: ProcessConfigModel,
        parameters: dict[str, Any] | None,
    ) -> str:
        """Delegate to run_recorder.create_run_record."""
        return await self._run_recorder.create_run_record(config, parameters)

    async def _update_process_run_record(
        self,
        public_id: str,
        status: ProcessRunStatusEnum,
        *,
        result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        """Delegate to run_recorder.update_run_record."""
        await self._run_recorder.update_run_record(public_id, status, result=result, error=error)

    async def _finalize_process_run(
        self,
        name: str,
        status: ProcessRunStatusEnum,
        *,
        result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        """Finalize a process run by updating its record.

        Removes run from active_runs and delegates DB update
        to run_recorder.

        Args:
            name: Process name.
            status: Final status.
            result: Optional result data.
            error: Optional error message.
        """
        public_id = self.active_runs.pop(name, None)
        if public_id is None:
            return
        await self._run_recorder.update_run_record(public_id, status, result=result, error=error)

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
    ) -> dict[str, Any] | None:
        """Delegate to config_resolver.resolve_parameters_schema."""
        return resolve_parameters_schema(config_dict, entry)

    async def get_process_configs(self) -> list[ProcessConfigModel]:
        """Load process configurations from database.

        Returns:
            List of ProcessConfigModel instances.
        """
        return await get_process_configs(self.settings)

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
        if task.done():
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

    def _start_as_subprocess(self, config: ProcessConfigModel) -> None:
        """Spawn a native subprocess and register it.

        Args:
            config: Process configuration.
        """
        process_info = self.spawner.spawn(
            name=config.name,
            class_path=config.class_path,
            method=config.method,
            args=config.args,
            kwargs=config.kwargs,
        )
        logger.info(f"Process '{config.name}' started with PID {process_info.pid}")
        self.started_processes[config.name] = process_info

    async def _start_in_process(self, config: ProcessConfigModel) -> None:
        """Instantiate the class and launch as async task or thread executor.

        Args:
            config: Process configuration.
        """
        process_class = self.import_class(config.class_path, config.name)
        process_instance = process_class(*config.args, **config.kwargs)
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
            and config.mode != "process"
        )

    async def _try_create_run_record(self, config: ProcessConfigModel) -> str | None:
        """Attempt to create a database run record.

        Args:
            config: Process configuration.

        Returns:
            The public_id string, or None if persistence failed.
        """
        run_parameters: dict[str, Any] = {
            "mode": config.mode,
            "args": config.args,
            "kwargs": config.kwargs,
        }
        try:
            public_id = await self._create_process_run_record(config, run_parameters)
            self.active_runs[config.name] = public_id
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

        Args:
            config_name: Name of the failed process.
            public_id: Database run record public ID, or None if not persisted.
            exc: The exception that caused the failure.
        """
        self._cleanup_failed_start(config_name)
        if public_id is not None:
            await self._update_process_run_record(
                public_id,
                ProcessRunStatusEnum.FAILED,
                error=str(exc),
            )
            self.active_runs.pop(config_name, None)

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
            if config.mode == "process":
                self._start_as_subprocess(config)
            elif config.mode == "thread":
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

    async def start_all_processes(self) -> None:
        """Start all enabled processes in priority order.

        Loads configurations, sorts by priority (lower first),
        and starts each enabled process. Tracks success/failure counts.
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
        for config in sorted_configs:
            if config.enabled:
                try:
                    await self.start_process(config)
                    started_count += 1
                except Exception as e:
                    logger.error(f"Failed to start process '{config.name}': {e}")
                    failed_count += 1
            else:
                disabled_count += 1
        logger.info(
            f"Process startup complete: {started_count} started, "
            f"{failed_count} failed, {disabled_count} disabled"
        )
        self._start_native_process_monitoring()

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
        self.expected_terminations.clear()
        logger.info("All processes stopped")

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
            await self._finalize_process_run(name, run_status, error=error_message)
        except Exception as e:
            logger.error(f"Error handling completion of native process '{name}': {e}")
        finally:
            self.process_lifecycles.pop(name, None)
            self.process_roles.pop(name, None)
            self.started_processes.pop(name, None)
            self.expected_terminations.discard(name)

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
            run_status: ProcessRunStatusEnum = ProcessRunStatusEnum.CANCELLED
            error_message: str | None = None
            if task.cancelled():
                logger.info(f"Process '{name}' was cancelled")
            elif task.exception():
                run_status, error_message = self._resolve_task_exception_status(name, task)
            else:
                run_status = self._resolve_task_success_status(name, lifecycle, expected)
            self._cleanup_task_tracking(name, task)
            if not isinstance(task.exception(), (GeneratorExit, StopAsyncIteration)):
                await self._finalize_process_run(name, run_status, error=error_message)
        except Exception as e:
            if not isinstance(e, (GeneratorExit, StopAsyncIteration, asyncio.CancelledError)):
                logger.error(f"Error handling completion of process '{name}': {e}")

    def _apply_overrides_to_config_dict(
        self,
        config_dict: dict[str, Any],
        mode: ProcessMode | None,
        args: list[Any] | None,
        kwargs: dict[str, Any] | None,
        autostart: bool | None,
    ) -> bool:
        """Apply runtime overrides to a config dictionary.

        Mutates config_dict in place with any non-None overrides and
        sets defaults for missing keys.

        Args:
            config_dict: Mutable config dictionary.
            mode: Optional execution mode override.
            args: Optional positional arguments override.
            kwargs: Optional keyword arguments override.
            autostart: Optional autostart override.

        Returns:
            The resolved autostart_enabled value.
        """
        autostart_enabled = bool(config_dict.get("enabled", False))
        if autostart is not None:
            autostart_enabled = bool(autostart)
        config_dict["enabled"] = autostart_enabled
        if mode is not None:
            config_dict["mode"] = mode
        config_dict.setdefault("mode", "thread")
        if args is not None:
            config_dict["args"] = args
        config_dict.setdefault("args", [])
        if kwargs is not None:
            config_dict["kwargs"] = kwargs
        config_dict.setdefault("kwargs", {})
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
            mode=resolve_mode(config_dict.get("mode", "thread"), name),
            class_path=config_dict["class"],
            method=config_dict.get("method", "start"),
            args=config_dict.get("args", []),
            kwargs=config_dict.get("kwargs", {}),
            note=config_dict.get("note"),
            lifecycle=self._resolve_lifecycle(lifecycle_raw, name),
            role=self._resolve_role(role_raw, name),
            tags=tags_tuple,
            parameters_schema=parameters_schema,
        )

    async def _persist_config_after_start(
        self,
        repository: Repository,
        config_key: str,
        config_dict: dict[str, Any],
        config: ProcessConfigModel,
    ) -> None:
        """Persist updated config to database after successful start.

        Args:
            repository: Database repository instance.
            config_key: Setting key (e.g. "process_myproc").
            config_dict: Original config dictionary for base values.
            config: Resolved ProcessConfigModel with final values.
        """
        persisted_config = dict(config_dict)
        persisted_config["lifecycle"] = config.lifecycle.value
        persisted_config["mode"] = config.mode
        persisted_config["args"] = config.args
        persisted_config["kwargs"] = config.kwargs
        persisted_config["role"] = config.role.value
        if config.tags:
            persisted_config["tags"] = list(config.tags)
        elif "tags" in persisted_config:
            persisted_config.pop("tags", None)
        if config.parameters_schema is not None:
            persisted_config["parameters_schema"] = config.parameters_schema
        async with repository.session() as session:
            now = datetime.now(UTC)
            await close_and_insert(
                session=session,
                model=Setting,
                match_filters=[Setting.key == config_key],
                new_values={
                    "key": config_key,
                    "value": json.dumps(persisted_config),
                    "category": "process",
                    "session_id": self._tracker.session_id,
                    "sequence_id": self._tracker.next_sequence(_SETTINGS_TOPIC),
                },
                bus_time=now,
            )
            await session.commit()

    async def start_process_by_name(
        self,
        name: str,
        mode: ProcessMode | None = None,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        autostart: bool | None = None,
    ) -> ProcessStartResult:
        """Start a process by its registered name.

        Args:
            name: Process name from registry.
            mode: Execution mode (thread/process).
            args: Positional arguments for the process.
            kwargs: Keyword arguments for the process.
            autostart: Whether to enable autostart on boot.

        Returns:
            Typed result with operation status, message, and optional public_id.
        """
        if name in self.started_processes:
            logger.warning(f"Process '{name}' is already running")
            return ProcessStartResult(
                status="already_running",
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
                    status="error",
                    message=f"Process '{name}' not found in configuration",
                )
            config_dict = json.loads(setting.value)
            autostart_enabled = self._apply_overrides_to_config_dict(
                config_dict, mode, args, kwargs, autostart
            )
            config = self._build_config_for_start_by_name(name, config_dict, autostart_enabled)
        try:
            await self.start_process(config)
        except Exception as e:
            logger.error(f"Failed to start process '{name}': {e}")
            return ProcessStartResult(
                status="error",
                message=f"Failed to start process '{name}': {str(e)}",
            )
        self._start_native_process_monitoring()
        await self._persist_config_after_start(repository, config_key, config_dict, config)
        public_id = self.active_runs.get(name)
        if config.lifecycle is ProcessLifecycleEnum.ONE_SHOT:
            return ProcessStartResult(
                status="success",
                message=f"Process '{name}' executed successfully",
                public_id=public_id,
            )
        logger.info(f"Process '{name}' started successfully")
        return ProcessStartResult(
            status="success",
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

    async def _disable_process_in_db(self, name: str) -> None:
        """Mark a process as disabled in the database.

        Args:
            name: Process name to disable.
        """
        repository = get_repository(self.settings.db_url)
        config_key = f"process_{name}"
        async with repository.session() as session:
            result = await session.execute(
                select(Setting).where(Setting.key == config_key, *where_active_now(Setting))
            )
            setting = result.scalar_one_or_none()
            if setting:
                config_dict = json.loads(setting.value)
                config_dict["enabled"] = False
                now = datetime.now(UTC)
                await close_and_insert(
                    session=session,
                    model=Setting,
                    match_filters=[Setting.key == config_key],
                    new_values={
                        "key": config_key,
                        "value": json.dumps(config_dict),
                        "category": setting.category,
                        "description": setting.description,
                        "is_encrypted": setting.is_encrypted,
                        "updated_by": setting.updated_by,
                        "session_id": self._tracker.session_id,
                        "sequence_id": self._tracker.next_sequence(_SETTINGS_TOPIC),
                    },
                    bus_time=now,
                )
                await session.commit()

    async def stop_process_by_name(self, name: str) -> ProcessStopResult:
        """Stop a running process by name.

        Args:
            name: Process name to stop.

        Returns:
            Typed result with operation status and message.
        """
        if name not in self.started_processes:
            logger.warning(f"Process '{name}' is not running")
            return ProcessStopResult(
                status="not_running",
                message=f"Process '{name}' is not running",
            )
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
            await self._disable_process_in_db(name)
            logger.info(f"Process '{name}' stopped and marked as disabled in database")
            await self._finalize_process_run(name, ProcessRunStatusEnum.CANCELLED)
            return ProcessStopResult(
                status="success",
                message=f"Process '{name}' stopped and marked as disabled in database",
            )
        except Exception as e:
            logger.error(f"Failed to stop process '{name}': {e}")
            return ProcessStopResult(status="error", message=str(e))
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
        details: dict[str, Any] | None = None
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
            role=(self.process_roles.get(name) or ProcessRoleEnum.CORE).value,
            lifecycle=self.process_lifecycles.get(name, ProcessLifecycleEnum.LONG_RUNNING).value,
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
        args: list[Any],
        kwargs: dict[str, Any],
        lifecycle: ProcessLifecycleEnum,
        role: ProcessRoleEnum,
        tags: Iterable[str],
        parameters_schema: dict[str, Any] | None = None,
        note: str | None = None,
    ) -> None:
        """Create a new process configuration in the database.

        Args:
            name: Process name.
            class_path: Fully qualified class path.
            method: Method to invoke on the class.
            enabled: Whether the process is enabled.
            mode: Execution mode (thread/process).
            args: Positional arguments for the method.
            kwargs: Keyword arguments for the method.
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
            args=args,
            kwargs=kwargs,
            lifecycle=lifecycle,
            role=role,
            tags=tags,
            parameters_schema=parameters_schema,
            note=note,
        )
