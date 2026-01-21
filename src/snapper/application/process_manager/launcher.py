"""Process launcher service module.

This module provides the main process lifecycle management service.
It handles:
- Loading process configurations from database
- Starting processes as threads or subprocesses
- Tracking process runs in database
- Monitoring and cleanup of completed processes
- Graceful shutdown of all processes
"""

import asyncio
import contextlib
import importlib
import inspect
import json
from collections.abc import Iterable
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast
from uuid import uuid4

from loguru import logger
from sqlalchemy import desc
from sqlalchemy import select

from snapper.application.process_manager.enums import ProcessLifecycleEnum
from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.enums import ProcessRunStatusEnum
from snapper.application.process_manager.models import ProcessConfigModel
from snapper.application.process_manager.models import ProcessInstanceInfo
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.registry import get_registered_processes
from snapper.application.process_manager.spawner import ProcessSpawnerService
from snapper.config.settings import AppSettings
from snapper.data.models import ProcessRun
from snapper.data.models import Setting
from snapper.data.repository import get_repository


class ProcessLauncherService:
    """Service for launching and managing application processes.

    Provides comprehensive process lifecycle management:
    - Loads configurations from database settings
    - Starts processes based on priority order
    - Tracks run history in database
    - Monitors process completion and handles failures
    - Coordinates graceful shutdown

    Attributes:
        settings: Application settings.
        started_processes: Dict of running process instances.
        process_tasks: Dict of asyncio tasks for async processes.
        process_lifecycles: Dict tracking lifecycle type per process.
        process_roles: Dict tracking role per process.
        active_runs: Dict mapping process name to run_id.
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

    async def _create_process_run_record(
        self,
        config: ProcessConfigModel,
        parameters: dict[str, Any] | None,
    ) -> str:
        """Create a new process run record in database.

        Args:
            config: Process configuration.
            parameters: Runtime parameters for this run.

        Returns:
            Generated run_id (UUID string).
        """
        repository = get_repository(self.settings.db_url)
        run_id = str(uuid4())
        async with repository.session() as session:
            run = ProcessRun(
                run_id=run_id,
                process_name=config.name,
                role=config.role.value,
                lifecycle=config.lifecycle.value,
                status=ProcessRunStatusEnum.RUNNING.value,
                parameters=parameters,
                tags=list(config.tags),
                started_at=datetime.now(UTC),
            )
            session.add(run)
            await session.commit()
        return run_id

    async def _update_process_run_record(
        self,
        run_id: str,
        status: ProcessRunStatusEnum,
        *,
        result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        """Update an existing process run record.

        Args:
            run_id: Run ID to update.
            status: New status to set.
            result: Optional result data dict.
            error: Optional error message (truncated to 1024 chars).
        """
        repository = get_repository(self.settings.db_url)
        async with repository.session() as session:
            result_row = await session.execute(
                select(ProcessRun).where(ProcessRun.run_id == run_id)
            )
            process_run = result_row.scalar_one_or_none()
            if process_run is None:
                logger.warning("Process run '{}' not found for status update", run_id)
                return
            process_run.status = status.value
            process_run.completed_at = datetime.now(UTC)
            if result is not None:
                process_run.result = result
            if error is not None:
                process_run.error = error[:1024]
            await session.commit()

    async def _finalize_process_run(
        self,
        name: str,
        status: ProcessRunStatusEnum,
        *,
        result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        """Finalize a process run by updating its record.

        Removes run from active_runs and updates database record.

        Args:
            name: Process name.
            status: Final status.
            result: Optional result data.
            error: Optional error message.
        """
        run_id = self.active_runs.pop(name, None)
        if run_id is None:
            return
        await self._update_process_run_record(run_id, status, result=result, error=error)

    async def get_process_configs(self) -> list[ProcessConfigModel]:
        """Load process configurations from database.

        Reads settings with key prefix "process_" and merges with
        registered process metadata.

        Returns:
            List of ProcessConfigModel instances.
        """
        repository = get_repository(self.settings.db_url)
        registry = get_registered_processes()
        async with repository.session() as session:
            result = await session.execute(select(Setting).where(Setting.key.like("process_%")))
            settings_rows = result.scalars().all()
            configs: list[ProcessConfigModel] = []
            for setting in settings_rows:
                try:
                    config_dict = json.loads(setting.value)
                    process_name = setting.key.replace("process_", "")
                    metadata = registry.get(process_name, {})
                    lifecycle_raw = config_dict.get("lifecycle")
                    if lifecycle_raw is None:
                        lifecycle_raw = metadata.get("lifecycle", ProcessLifecycleEnum.LONG_RUNNING)
                    try:
                        lifecycle = (
                            lifecycle_raw
                            if isinstance(lifecycle_raw, ProcessLifecycleEnum)
                            else ProcessLifecycleEnum(str(lifecycle_raw))
                        )
                    except ValueError:
                        logger.warning(
                            "Unknown lifecycle '{}' for process '{}', defaulting to long-running",
                            lifecycle_raw,
                            process_name,
                        )
                        lifecycle = ProcessLifecycleEnum.LONG_RUNNING
                    role_raw = config_dict.get("role")
                    if role_raw is None:
                        role_raw = metadata.get("role", ProcessRoleEnum.CORE)
                    try:
                        role = (
                            role_raw
                            if isinstance(role_raw, ProcessRoleEnum)
                            else ProcessRoleEnum(str(role_raw))
                        )
                    except ValueError:
                        logger.warning(
                            "Unknown role '{}' for process '{}', defaulting to core",
                            role_raw,
                            process_name,
                        )
                        role = ProcessRoleEnum.CORE
                    tags_raw = config_dict.get("tags")
                    if tags_raw is None:
                        tags_raw = metadata.get("tags", ())
                    tags_tuple: tuple[str, ...]
                    if isinstance(tags_raw, (list, tuple, set)):
                        tags_tuple = tuple(str(tag) for tag in cast(Iterable[Any], tags_raw))
                    else:
                        tags_tuple = ()
                    parameters_schema = config_dict.get("parameters_schema")
                    if parameters_schema is None:
                        parameters_schema = metadata.get("parameters_schema")
                    config = ProcessConfigModel(
                        name=process_name,
                        enabled=config_dict.get("enabled", False),
                        mode=config_dict.get("mode", "thread"),
                        class_path=config_dict["class"],
                        method=config_dict.get("method", "start"),
                        args=config_dict.get("args", []),
                        kwargs=config_dict.get("kwargs", {}),
                        note=config_dict.get("note"),
                        lifecycle=lifecycle,
                        role=role,
                        tags=tags_tuple,
                        parameters_schema=parameters_schema,
                    )
                    configs.append(config)
                except (json.JSONDecodeError, KeyError) as e:
                    logger.error(f"Failed to parse process config '{setting.key}': {e}")
        return configs

    def import_class(self, class_path: str, process_name: str | None = None) -> type:
        """Import a class by its fully qualified path.

        First checks the process registry, then falls back to
        dynamic import.

        Args:
            class_path: Fully qualified class path (e.g., "snapper.app.MyClass").
            process_name: Optional process name to check registry first.

        Returns:
            The imported class type.

        Raises:
            TypeError: If the imported object is not a class.
            ImportError: If the class cannot be imported.
        """
        if process_name:
            registry = get_registered_processes()
            if process_name in registry:
                cls = registry[process_name]["class_ref"]
                if not isinstance(cls, type):
                    raise TypeError(f"{class_path} is not a class")
                return cls
        try:
            module_path, class_name = class_path.rsplit(".", 1)
            module = importlib.import_module(module_path)
            cls = getattr(module, class_name)
            if not isinstance(cls, type):
                raise TypeError(f"{class_path} is not a class")
            return cls
        except (ValueError, ModuleNotFoundError, AttributeError) as e:
            raise ImportError(
                f"Failed to import class '{class_path}' (process_name='{process_name}'): {e}"
            ) from e

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
        run_parameters: dict[str, Any] = {
            "mode": config.mode,
            "args": config.args,
            "kwargs": config.kwargs,
        }
        run_id: str | None = None
        try:
            run_id = await self._create_process_run_record(config, run_parameters)
            self.active_runs[config.name] = run_id
        except Exception as run_error:
            logger.warning(
                "Unable to persist run record for process '{}': {}",
                config.name,
                run_error,
            )
        try:
            logger.info(
                f"Starting process '{config.name}' in {config.mode} mode "
                f"(class: {config.class_path}, method: {config.method})"
            )
            if config.mode == "process":
                process_info = self.spawner.spawn(
                    name=config.name,
                    class_path=config.class_path,
                    method=config.method,
                    args=config.args,
                    kwargs=config.kwargs,
                )
                logger.info(f"Process '{config.name}' started with PID {process_info.pid}")
                self.started_processes[config.name] = process_info
                if config.note:
                    logger.info(f"Note for '{config.name}': {config.note}")
                return
            process_class = self.import_class(config.class_path, config.name)
            process_instance = process_class(*config.args, **config.kwargs)
            stored_reference: Any = process_instance
            method = getattr(process_instance, config.method)
            if inspect.iscoroutinefunction(method):
                if config.mode != "thread":
                    logger.warning(
                        "Async process '{}' requested non-thread mode '{}'; running as thread",
                        config.name,
                        config.mode,
                    )
                task = asyncio.create_task(method())
                self.process_tasks[config.name] = task
                self._register_task_completion(config.name, task)
                logger.info(f"Process '{config.name}' started as async task")
                await asyncio.sleep(0.1)
                if task.done():
                    exception = task.exception()
                    if exception is not None:
                        raise exception
            else:
                if config.mode != "thread":
                    logger.warning(
                        "Sync process '{}' requested non-thread mode '{}'; running in executor",
                        config.name,
                        config.mode,
                    )
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, method)
                logger.info(f"Process '{config.name}' started in thread executor")
            self.started_processes[config.name] = stored_reference
            if config.name not in self.process_tasks and config.mode != "process":
                await self._finalize_process_run(config.name, ProcessRunStatusEnum.SUCCEEDED)
            if config.note:
                logger.info(f"Note for '{config.name}': {config.note}")
        except Exception as exc:
            self.process_lifecycles.pop(config.name, None)
            self.process_tasks.pop(config.name, None)
            self.started_processes.pop(config.name, None)
            self.process_roles.pop(config.name, None)
            if run_id is not None:
                await self._update_process_run_record(
                    run_id,
                    ProcessRunStatusEnum.FAILED,
                    error=str(exc),
                )
                self.active_runs.pop(config.name, None)
            raise
        if (
            config.lifecycle is ProcessLifecycleEnum.ONE_SHOT
            and config.name not in self.process_tasks
            and config.mode != "process"
        ):
            await self._finalize_process_run(config.name, ProcessRunStatusEnum.SUCCEEDED)
            self.started_processes.pop(config.name, None)
            self.process_lifecycles.pop(config.name, None)
            self.process_roles.pop(config.name, None)

    async def start_all_processes(self) -> None:
        """Start all enabled processes in priority order.

        Loads configurations, sorts by priority (lower first),
        and starts each enabled process. Tracks success/failure counts.
        """
        configs = await self.get_process_configs()
        registry = get_registered_processes()
        configs_with_priority = [
            (config, registry.get(config.name, {}).get("priority", 50)) for config in configs
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
            (name, task, registry.get(name, {}).get("priority", 50))
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
            (name, instance, registry.get(name, {}).get("priority", 50))
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

    def _register_task_completion(self, name: str, task: asyncio.Task[object]) -> None:
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
                    if isinstance(result, asyncio.Task):
                        result_task: asyncio.Task[object] = result
                        self.process_tasks[name] = result_task
                        self._register_task_completion(name, result_task)
                        return
                    if inspect.iscoroutine(result):
                        chained_task = asyncio.create_task(result)
                        self.process_tasks[name] = chained_task
                        self._register_task_completion(name, chained_task)
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
                        if not status.get("running", True):
                            logger.info(f"Native process '{name}' has completed")
                            await self._handle_process_completion(name, proc_info)
                except asyncio.CancelledError:
                    logger.info("Native process monitoring cancelled")
                    break
                except Exception as e:
                    logger.error(f"Error in native process monitoring: {e}")
                    await asyncio.sleep(10)
        finally:
            self.process_tasks.pop("_native_monitor", None)

    async def _handle_process_completion(self, name: str, proc_info: ProcessInstanceInfo) -> None:
        try:
            lifecycle = self.process_lifecycles.get(name, ProcessLifecycleEnum.LONG_RUNNING)
            expected = name in self.expected_terminations
            run_status = ProcessRunStatusEnum.CANCELLED
            error_message: str | None = None
            exit_code = proc_info.process.returncode
            if exit_code == 0:
                logger.info(f"Native process '{name}' completed successfully")
                if lifecycle is ProcessLifecycleEnum.LONG_RUNNING and not expected:
                    logger.warning(
                        "Long-running native process '{}' exited; autostart unchanged",
                        name,
                    )
                run_status = (
                    ProcessRunStatusEnum.CANCELLED
                    if lifecycle is ProcessLifecycleEnum.LONG_RUNNING and expected
                    else ProcessRunStatusEnum.SUCCEEDED
                )
            else:
                if expected:
                    logger.info(
                        "Native process '{}' stopped gracefully with exit code {}",
                        name,
                        exit_code,
                    )
                    run_status = ProcessRunStatusEnum.CANCELLED
                else:
                    logger.error(
                        "Native process '{}' failed with exit code {}; autostart unchanged",
                        name,
                        exit_code,
                    )
                    run_status = ProcessRunStatusEnum.FAILED
                    error_message = f"exit_code={exit_code}"
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

    async def _handle_task_completion(self, name: str, task: asyncio.Task[Any]) -> None:
        try:
            lifecycle = self.process_lifecycles.get(name, ProcessLifecycleEnum.LONG_RUNNING)
            expected = name in self.expected_terminations
            run_status: ProcessRunStatusEnum = ProcessRunStatusEnum.CANCELLED
            error_message: str | None = None
            if task.cancelled():
                logger.info(f"Process '{name}' was cancelled")
            elif task.exception():
                exc = task.exception()
                if isinstance(exc, (GeneratorExit, StopAsyncIteration, asyncio.CancelledError)):
                    logger.debug(f"Process '{name}' stopped during shutdown: {type(exc).__name__}")
                else:
                    logger.error(
                        "Process '{}' failed with exception: {}; autostart unchanged",
                        name,
                        exc,
                    )
                    run_status = ProcessRunStatusEnum.FAILED
                    error_message = str(exc)
            else:
                if expected:
                    logger.info(f"Process '{name}' stopped gracefully")
                else:
                    logger.info(f"Process '{name}' completed successfully")
                if lifecycle is ProcessLifecycleEnum.LONG_RUNNING:
                    if expected:
                        run_status = ProcessRunStatusEnum.CANCELLED
                    else:
                        logger.warning(
                            "Long-running process '{}' completed unexpectedly; autostart unchanged",
                            name,
                        )
                        run_status = ProcessRunStatusEnum.SUCCEEDED
                else:
                    run_status = ProcessRunStatusEnum.SUCCEEDED
            stored_task = self.process_tasks.get(name)
            if stored_task is task:
                del self.process_tasks[name]
            self.started_processes.pop(name, None)
            self.process_lifecycles.pop(name, None)
            self.process_roles.pop(name, None)
            self.expected_terminations.discard(name)
            if not isinstance(task.exception(), (GeneratorExit, StopAsyncIteration)):
                await self._finalize_process_run(name, run_status, error=error_message)
        except Exception as e:
            if not isinstance(e, (GeneratorExit, StopAsyncIteration, asyncio.CancelledError)):
                logger.error(f"Error handling completion of process '{name}': {e}")

    async def start_process_by_name(
        self,
        name: str,
        mode: str | None = None,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        autostart: bool | None = None,
    ) -> dict[str, Any]:
        """Start a process by its registered name.

        Args:
            name: Process name from registry.
            mode: Execution mode (thread/process/async).
            args: Positional arguments for the process.
            kwargs: Keyword arguments for the process.
            autostart: Whether to enable autostart on boot.

        Returns:
            Status dict with operation result.
        """
        if name in self.started_processes:
            logger.warning(f"Process '{name}' is already running")
            return {"status": "already_running", "message": f"Process '{name}' is already running"}
        repository = get_repository(self.settings.db_url)
        config_key = f"process_{name}"
        config_dict: dict[str, Any] = {}
        async with repository.session() as session:
            result = await session.execute(select(Setting).where(Setting.key == config_key))
            setting = result.scalar_one_or_none()
            if not setting:
                return {
                    "status": "error",
                    "message": f"Process '{name}' not found in configuration",
                }
            config_dict = json.loads(setting.value)
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
            registry = get_registered_processes()
            metadata = registry.get(name, {})
            lifecycle_raw = config_dict.get("lifecycle")
            if lifecycle_raw is None:
                lifecycle_raw = metadata.get("lifecycle", ProcessLifecycleEnum.LONG_RUNNING)
            try:
                lifecycle = (
                    lifecycle_raw
                    if isinstance(lifecycle_raw, ProcessLifecycleEnum)
                    else ProcessLifecycleEnum(str(lifecycle_raw))
                )
            except ValueError:
                logger.warning(
                    "Unknown lifecycle '{}' for process '{}', defaulting to long-running",
                    lifecycle_raw,
                    name,
                )
                lifecycle = ProcessLifecycleEnum.LONG_RUNNING
            role_raw = config_dict.get("role")
            if role_raw is None:
                role_raw = metadata.get("role", ProcessRoleEnum.CORE)
            try:
                role = (
                    role_raw
                    if isinstance(role_raw, ProcessRoleEnum)
                    else ProcessRoleEnum(str(role_raw))
                )
            except ValueError:
                logger.warning(
                    "Unknown role '{}' for process '{}', defaulting to core",
                    role_raw,
                    name,
                )
                role = ProcessRoleEnum.CORE
            tags_raw = metadata.get("tags")
            if tags_raw is None:
                tags_raw = config_dict.get("tags", ())
            tags_tuple: tuple[str, ...]
            if isinstance(tags_raw, (list, tuple, set)):
                tags_tuple = tuple(str(tag) for tag in cast(Iterable[Any], tags_raw))
            else:
                tags_tuple = ()
            parameters_schema = config_dict.get("parameters_schema")
            if parameters_schema is None:
                parameters_schema = metadata.get("parameters_schema")
            if parameters_schema is None:
                tags_tuple = ()
            config = ProcessConfigModel(
                name=name,
                enabled=autostart_enabled,
                mode=config_dict.get("mode", "thread"),
                class_path=config_dict["class"],
                method=config_dict.get("method", "start"),
                args=config_dict.get("args", []),
                kwargs=config_dict.get("kwargs", {}),
                note=config_dict.get("note"),
                lifecycle=lifecycle,
                role=role,
                tags=tags_tuple,
                parameters_schema=parameters_schema,
            )
        try:
            await self.start_process(config)
        except Exception as e:
            logger.error(f"Failed to start process '{name}': {e}")
            return {
                "status": "error",
                "message": f"Failed to start process '{name}': {str(e)}",
            }
        self._start_native_process_monitoring()
        async with repository.session() as session:
            result = await session.execute(select(Setting).where(Setting.key == config_key))
            setting = result.scalar_one_or_none()
            if setting:
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
                setting.value = json.dumps(persisted_config)
                setting.updated_at = datetime.now(UTC)
                await session.commit()
        if config.lifecycle is ProcessLifecycleEnum.ONE_SHOT:
            return {
                "status": "success",
                "message": f"Process '{name}' executed successfully",
            }
        logger.info(f"Process '{name}' started successfully")
        return {"status": "success", "message": f"Process '{name}' started successfully"}

    async def stop_process_by_name(self, name: str) -> dict[str, Any]:
        """Stop a running process by name.

        Args:
            name: Process name to stop.

        Returns:
            Status dict with operation result.
        """
        if name not in self.started_processes:
            logger.warning(f"Process '{name}' is not running")
            return {"status": "not_running", "message": f"Process '{name}' is not running"}
        try:
            self.expected_terminations.add(name)
            if name in self.process_tasks:
                task = self.process_tasks[name]
                if not task.done():
                    logger.info(f"Cancelling task '{name}'")
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
                del self.process_tasks[name]
            instance = self.started_processes.get(name)
            if instance is not None:
                logger.info(f"Stopping process '{name}'")
                await instance.stop()
            self.started_processes.pop(name, None)
            self.process_lifecycles.pop(name, None)
            self.process_roles.pop(name, None)
            repository = get_repository(self.settings.db_url)
            async with repository.session() as session:
                config_key = f"process_{name}"
                result = await session.execute(select(Setting).where(Setting.key == config_key))
                setting = result.scalar_one_or_none()
                if setting:
                    config_dict = json.loads(setting.value)
                    config_dict["enabled"] = False
                    setting.value = json.dumps(config_dict)
                    setting.updated_at = datetime.now(UTC)
                    await session.commit()
            logger.info(f"Process '{name}' stopped and marked as disabled in database")
            await self._finalize_process_run(name, ProcessRunStatusEnum.CANCELLED)
            return {
                "status": "success",
                "message": f"Process '{name}' stopped and marked as disabled in database",
            }
        except Exception as e:
            logger.error(f"Failed to stop process '{name}': {e}")
            return {"status": "error", "message": str(e)}
        finally:
            self.expected_terminations.discard(name)

    async def get_process_status(self, name: str) -> dict[str, Any]:
        """Get current status of a process.

        Args:
            name: Process name to query.

        Returns:
            Status dict with running state, role, lifecycle and details.
        """
        is_running = name in self.started_processes
        status: dict[str, Any] = {
            "name": name,
            "running": is_running,
            "role": (self.process_roles.get(name) or ProcessRoleEnum.CORE).value,
            "lifecycle": self.process_lifecycles.get(name, ProcessLifecycleEnum.LONG_RUNNING).value,
        }
        active_run_id = self.active_runs.get(name)
        if active_run_id is not None:
            status["active_run_id"] = active_run_id
        instance = self.started_processes.get(name)
        if instance:
            try:
                status["details"] = instance.get_status()
            except Exception as e:
                logger.warning(f"Failed to get status from process '{name}': {e}")
        return status

    async def get_recent_runs(
        self,
        *,
        limit: int = 50,
        name: str | None = None,
    ) -> list[dict[str, Any]]:
        """Retrieve recent process run history.

        Args:
            limit: Maximum number of runs to return.
            name: Filter by process name.

        Returns:
            List of run records with status, timestamps, and results.
        """
        repository = get_repository(self.settings.db_url)
        async with repository.session() as session:
            stmt = select(ProcessRun).order_by(desc(ProcessRun.started_at)).limit(limit)
            if name:
                stmt = stmt.where(ProcessRun.process_name == name)
            result = await session.execute(stmt)
            runs = result.scalars().all()
        return [
            {
                "run_id": run.run_id,
                "process_name": run.process_name,
                "status": run.status,
                "role": run.role,
                "lifecycle": run.lifecycle,
                "parameters": run.parameters,
                "result": run.result,
                "error": run.error,
                "tags": run.tags or [],
                "started_at": run.started_at.isoformat(),
                "completed_at": run.completed_at.isoformat() if run.completed_at else None,
            }
            for run in runs
        ]

    def _get_defaults_from_metadata(self, metadata: dict[str, Any]) -> dict[str, Any]:
        return {
            "enabled": metadata.get("enabled", False),
            "mode": metadata.get("mode", "thread"),
            "args": metadata.get("args", []),
            "kwargs": {},
            "lifecycle": metadata.get("lifecycle", ProcessLifecycleEnum.LONG_RUNNING),
            "role": metadata.get("role", ProcessRoleEnum.CORE),
            "tags": metadata.get("tags", ()),
            "parameters_schema": metadata.get("parameters_schema"),
        }

    async def _create_process_config_in_db(
        self, name: str, class_path: str, method: str, defaults: dict[str, Any]
    ) -> None:
        repository = get_repository(self.settings.db_url)
        config_dict: dict[str, Any] = {
            "enabled": defaults["enabled"],
            "mode": defaults["mode"],
            "class": class_path,
            "method": method,
            "args": defaults["args"],
            "kwargs": defaults["kwargs"],
            "lifecycle": (
                defaults["lifecycle"].value
                if isinstance(defaults["lifecycle"], ProcessLifecycleEnum)
                else str(defaults["lifecycle"])
            ),
            "role": (
                defaults["role"].value
                if isinstance(defaults["role"], ProcessRoleEnum)
                else str(defaults["role"])
            ),
        }
        tags_default = defaults.get("tags")
        if isinstance(tags_default, (list, tuple, set)):
            config_dict["tags"] = [str(tag) for tag in cast(Iterable[Any], tags_default)]
        parameters_schema_default = defaults.get("parameters_schema")
        if parameters_schema_default is not None:
            config_dict["parameters_schema"] = parameters_schema_default
        async with repository.session() as session:
            setting = Setting(
                key=f"process_{name}",
                value=json.dumps(config_dict),
                category="process",
                updated_at=datetime.now(UTC),
            )
            session.add(setting)
            await session.commit()
        logger.info(f"Created database config for process '{name}'")

    async def sync_registry_to_database(self) -> None:
        """Synchronize process registry with database configurations.

        Creates missing database entries and updates existing ones
        with current metadata from the registry.
        """
        registry = get_registered_processes()
        logger.info(f"Syncing {len(registry)} registered processes to database")
        repository = get_repository(self.settings.db_url)
        for name, metadata in registry.items():
            config_key = f"process_{name}"
            async with repository.session() as session:
                result = await session.execute(select(Setting).where(Setting.key == config_key))
                existing = result.scalar_one_or_none()
            if existing is None:
                cls: type[RegisterableProcess] = metadata["class_ref"]
                defaults = self._get_defaults_from_metadata(metadata)
                try:
                    defaults["kwargs"] = cls.get_default_kwargs(self.settings)
                except Exception as e:
                    logger.warning(
                        f"Failed to get default kwargs for '{name}': {e}, using empty dict"
                    )
                    defaults["kwargs"] = {}
                await self._create_process_config_in_db(
                    name=name,
                    class_path=metadata["class_path"],
                    method=metadata["method"],
                    defaults=defaults,
                )
            else:
                try:
                    config_dict = json.loads(existing.value)
                    update_needed = False
                    kwargs = config_dict.get("kwargs", {})
                    if not kwargs:
                        cls = metadata["class_ref"]
                        try:
                            default_kwargs = cls.get_default_kwargs(self.settings)
                        except Exception as e:
                            logger.debug(f"Process '{name}' get_default_kwargs failed: {e}")
                            default_kwargs = {}
                        if default_kwargs:
                            config_dict["kwargs"] = default_kwargs
                            update_needed = True
                        else:
                            logger.debug(f"Process '{name}' has empty default kwargs")
                    else:
                        logger.debug(f"Process '{name}' already has database config with kwargs")
                    lifecycle_meta = metadata.get(
                        "lifecycle",
                        ProcessLifecycleEnum.LONG_RUNNING,
                    )
                    new_lifecycle = (
                        lifecycle_meta.value
                        if isinstance(lifecycle_meta, ProcessLifecycleEnum)
                        else str(lifecycle_meta)
                    )
                    if config_dict.get("lifecycle") != new_lifecycle:
                        config_dict["lifecycle"] = new_lifecycle
                        update_needed = True
                    role_meta = metadata.get("role", ProcessRoleEnum.CORE)
                    new_role = (
                        role_meta.value
                        if isinstance(role_meta, ProcessRoleEnum)
                        else str(role_meta)
                    )
                    if config_dict.get("role") != new_role:
                        config_dict["role"] = new_role
                        update_needed = True
                    if "tags" not in config_dict and metadata.get("tags"):
                        tags_meta = metadata.get("tags", ())
                        if isinstance(tags_meta, (list, tuple, set)):
                            config_dict["tags"] = [
                                str(tag) for tag in cast(Iterable[Any], tags_meta)
                            ]
                            update_needed = True
                    if (
                        "parameters_schema" not in config_dict
                        and metadata.get("parameters_schema") is not None
                    ):
                        config_dict["parameters_schema"] = metadata.get("parameters_schema")
                        update_needed = True
                    if update_needed:
                        async with repository.session() as update_session:
                            result = await update_session.execute(
                                select(Setting).where(Setting.key == config_key)
                            )
                            existing_record = result.scalar_one_or_none()
                            if existing_record:
                                existing_record.value = json.dumps(config_dict, indent=4)
                                existing_record.updated_at = datetime.now(UTC)
                                existing_record.updated_by = "sync_registry"
                                await update_session.commit()
                                logger.info(
                                    "Updated process '{}' metadata in database",
                                    name,
                                )
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse config for '{name}': {e}")

    async def create_process_config(
        self,
        *,
        name: str,
        class_path: str,
        method: str,
        enabled: bool,
        mode: str,
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
            name: Unique process name.
            class_path: Fully qualified class path.
            method: Entry method name.
            enabled: Whether process is enabled for autostart.
            mode: Execution mode (thread/process/async).
            args: Positional arguments.
            kwargs: Keyword arguments.
            lifecycle: Process lifecycle type.
            role: Process role category.
            tags: Process tags for grouping.
            parameters_schema: Optional JSON schema for parameters.
            note: Optional description note.

        Raises:
            ValueError: If process name already exists.
        """
        repository = get_repository(self.settings.db_url)
        config_key = f"process_{name}"
        config_dict: dict[str, Any] = {
            "enabled": enabled,
            "mode": mode,
            "class": class_path,
            "method": method,
            "args": args,
            "kwargs": kwargs,
            "lifecycle": lifecycle.value,
            "role": role.value,
        }
        tags_list = [str(tag) for tag in tags]
        if tags_list:
            config_dict["tags"] = tags_list
        if parameters_schema is not None:
            config_dict["parameters_schema"] = parameters_schema
        if note is not None:
            config_dict["note"] = note
        async with repository.session() as session:
            existing = await session.execute(select(Setting).where(Setting.key == config_key))
            if existing.scalar_one_or_none() is not None:
                raise ValueError(f"Process '{name}' is already configured")
            setting = Setting(
                key=config_key,
                value=json.dumps(config_dict, indent=4),
                category="process",
                updated_at=datetime.now(UTC),
            )
            session.add(setting)
            await session.commit()
