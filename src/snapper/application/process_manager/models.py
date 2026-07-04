"""Process manager data models module.

This module defines the core data structures for process management:
- RegisterableProcess: Abstract base class for manageable processes
- ProcessInstanceInfo: Runtime information for spawned subprocesses
- ProcessConfigModel: Configuration model for process definitions
- ProcessRegistryEntry: Typed metadata for registered processes
- ProcessStartResult: Typed result for start operations
- ProcessStopResult: Typed result for stop operations
- ProcessStatusResult: Typed runtime status snapshot
- SpawnerStatusSnapshot: Point-in-time status from spawner
"""

import asyncio
import subprocess
from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from typing import Any

from snapper.config.settings import AppSettings
from snapper.core.json_types import JsonObject
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessLifecycleType
from snapper.core.types import ProcessMode
from snapper.core.types import ProcessRestartPolicyEnum
from snapper.core.types import ProcessRoleEnum
from snapper.core.types import ProcessRoleType
from snapper.core.types import StartProcessStatus
from snapper.core.types import StopProcessStatus


class RegisterableProcess(ABC):
    """Abstract base class for processes that can be registered and managed.

    All managed processes must inherit from this class and implement
    the start() method. The class provides hooks for:
    - Default parameters generation from settings
    - Status reporting
    - Graceful shutdown

    Subclasses are registered via the @register_process decorator.
    """

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Get default constructor parameters from application settings.

        Override in subclasses to provide settings-based defaults.
        Returns a dict (or Pydantic model instance that supports
        model_dump) with default parameters for the process constructor.

        Args:
            settings: Application settings instance.

        Returns:
            Dict with default parameters for the process constructor.
        """
        return {}

    def get_status(self) -> dict[str, Any]:
        """Get current process status.

        Override in subclasses to provide process-specific status.

        Returns:
            Dict with status information.
        """
        return {}

    @abstractmethod
    async def start(self) -> None:
        """Start the process.

        Must be implemented by all subclasses.
        """
        ...

    async def stop(self) -> None:
        """Stop the process gracefully.

        Override in subclasses that need cleanup on shutdown.
        Default implementation does nothing.
        """
        await asyncio.sleep(0)
        return None


@dataclass
class ProcessInstanceInfo(RegisterableProcess):
    """Runtime information for a spawned subprocess.

    Tracks a subprocess created via ProcessSpawnerService,
    including its PID, start time, and exit status.

    Attributes:
        name: Process name (unique identifier).
        pid: Operating system process ID.
        started_at: Timestamp when process was started.
        config: Configuration dict used to spawn the process.
        process: Subprocess Popen object.
        spawner: Reference to ProcessSpawnerService for cleanup.
        exit_code: Exit code if process has terminated.
        last_heartbeat: Last known heartbeat timestamp.
        started_monotonic: Monotonic process start timestamp for durations.
        last_heartbeat_monotonic: Monotonic heartbeat timestamp for durations.
    """

    name: str
    pid: int
    started_at: datetime
    config: JsonObject
    process: subprocess.Popen[bytes]
    spawner: Any = None
    exit_code: int | None = None
    last_heartbeat: datetime | None = None
    started_monotonic: float | None = None
    last_heartbeat_monotonic: float | None = None
    _stopped: bool = field(default=False, repr=False)

    async def start(self) -> None:
        """No-op start for already-spawned processes."""
        pass

    async def stop(self) -> None:
        """Stop the subprocess via spawner.

        Uses the spawner reference to terminate and cleanup
        the subprocess resources.
        """
        if self._stopped:
            return
        self._stopped = True
        if self.spawner is not None:
            self.spawner.terminate(self.name)
            self.spawner.cleanup(self.name)

    def get_status(self) -> dict[str, Any]:
        """Get subprocess status information.

        Returns:
            Dict with pid, started_at, and exit_code.
        """
        return {
            "pid": self.pid,
            "started_at": self.started_at.isoformat(),
            "exit_code": self.exit_code,
        }


@dataclass
class ProcessConfigModel:
    """Configuration model for a managed process.

    Defines all parameters needed to instantiate and run a process.

    Attributes:
        name: Unique process identifier.
        enabled: Whether process should auto-start.
        mode: Execution mode ("thread" or "process").
        class_path: Fully qualified class path (e.g., "snapper.app.MyProcess").
        method: Method to call on instantiated class.
        parameters: Constructor parameters dict.
        note: Optional human-readable description.
        lifecycle: LONG_RUNNING or ONE_SHOT.
        role: Process role (CORE, TASK, STRATEGY, BACKTEST).
        restart_policy: Auto-restart policy for the launcher watchdog.
        tags: Tuple of string tags for filtering.
        parameters_schema: Optional JSON schema for parameters.
        template: Registered process name this config was created from
            (None for registry-native configs). Lets class resolution
            fall back to the template's registry class when the stored
            class_path is a function-local (unimportable) wrapper.
        restart_nonce: Operator restart generation token (a client-minted
            uuid) persisted in the config JSON. The desired-state reconcile
            loop bounces (stop+start) a process when this differs from the
            nonce it last applied, so an operator restart survives retries
            and coordinator restarts idempotently. None for a config that
            has never been restarted through the control plane.
    """

    name: str
    enabled: bool
    mode: ProcessMode
    class_path: str
    method: str
    parameters: JsonObject
    note: str | None = None
    lifecycle: ProcessLifecycleEnum = ProcessLifecycleEnum.LONG_RUNNING
    role: ProcessRoleEnum = ProcessRoleEnum.CORE
    restart_policy: ProcessRestartPolicyEnum = ProcessRestartPolicyEnum.ON_FAILURE
    tags: tuple[str, ...] = ()
    parameters_schema: JsonObject | None = None
    template: str | None = None
    restart_nonce: str | None = None


@dataclass
class ProcessStartResult:
    """Result of a process start operation.

    Attributes:
        status: Operation outcome (success, already_running, error).
        message: Human-readable description of the result.
        public_id: Database run record public ID if the process was started.
    """

    status: StartProcessStatus
    message: str
    public_id: str | None = None


@dataclass
class ProcessStopResult:
    """Result of a process stop operation.

    Attributes:
        status: Operation outcome (success, not_running, error).
        message: Human-readable description of the result.
    """

    status: StopProcessStatus
    message: str


@dataclass
class ProcessStatusResult:
    """Current runtime status of a managed process.

    Attributes:
        name: Process name.
        running: Whether the process is currently running.
        role: Process role category (enum string value).
        lifecycle: Process lifecycle type (enum string value).
        active_public_id: Active run record public ID if currently running.
        details: Additional process-specific status information.
    """

    name: str
    running: bool
    role: ProcessRoleType
    lifecycle: ProcessLifecycleType
    active_public_id: str | None = None
    details: dict[str, Any] | None = None


@dataclass
class ProcessRegistryEntry:
    """Metadata for a registered process in the global registry.

    Populated by the @register_process decorator and used by
    launcher, syncer, and process routes for process discovery.

    Attributes:
        class_ref: Reference to the process class.
        class_path: Fully qualified class path string.
        method: Entry point method name.
        description: Human-readable description.
        priority: Startup priority (lower starts first).
        lifecycle: Process lifecycle type enum.
        role: Process role category enum.
        tags: Categorization tags tuple.
        parameters_model: Optional Pydantic model type for parameter validation.
        parameters_schema: Derived JSON Schema from parameters_model (API metadata).
        enabled: Default enabled state.
        mode: Default execution mode.
        restart_policy: Default auto-restart policy for the launcher watchdog.
    """

    class_ref: type[RegisterableProcess]
    class_path: str
    method: str
    description: str
    priority: int
    lifecycle: ProcessLifecycleEnum
    role: ProcessRoleEnum
    tags: tuple[str, ...]
    parameters_model: type[Any] | None
    parameters_schema: JsonObject | None
    enabled: bool
    mode: ProcessMode
    restart_policy: ProcessRestartPolicyEnum = ProcessRestartPolicyEnum.ON_FAILURE


@dataclass
class SpawnerStatusSnapshot:
    """Point-in-time status snapshot for a spawned process.

    Attributes:
        name: Process name.
        running: Whether the process is currently alive.
        pid: OS process ID (None if not found).
        started_at: Start timestamp in ISO format (None if not found).
        uptime_seconds: Seconds since process started (None if not found).
        exit_code: Exit code if process has terminated.
        stopped_at: Stop timestamp in ISO format (None if still running).
        last_heartbeat: Last heartbeat ISO timestamp (None if no heartbeats).
        heartbeat_age_seconds: Seconds since last heartbeat (None if no heartbeats).
        error: Error message (e.g., 'Process not found').
    """

    name: str
    running: bool
    pid: int | None = None
    started_at: str | None = None
    uptime_seconds: float | None = None
    exit_code: int | None = None
    stopped_at: str | None = None
    last_heartbeat: str | None = None
    heartbeat_age_seconds: float | None = None
    error: str | None = None
