"""Process manager data models module.

This module defines the core data structures for process management:
- RegisterableProcess: Abstract base class for manageable processes
- ProcessInstanceInfo: Runtime information for spawned subprocesses
- ProcessConfigModel: Configuration model for process definitions
"""

import asyncio
import subprocess
from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from typing import Any

from snapper.application.process_manager.enums import ProcessLifecycleEnum
from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.config.settings import AppSettings


class RegisterableProcess(ABC):
    """Abstract base class for processes that can be registered and managed.

    All managed processes must inherit from this class and implement
    the start() method. The class provides hooks for:
    - Default kwargs generation from settings
    - Status reporting
    - Graceful shutdown

    Subclasses are registered via the @register_process decorator.
    """

    @staticmethod
    def get_default_kwargs(settings: AppSettings) -> dict[str, Any]:
        """Get default constructor kwargs from application settings.

        Override in subclasses to provide settings-based defaults.

        Args:
            settings: Application settings instance.

        Returns:
            Dict with default kwargs for the process constructor.
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
    """

    name: str
    pid: int
    started_at: datetime
    config: dict[str, Any]
    process: subprocess.Popen[bytes]
    spawner: Any = None
    exit_code: int | None = None
    last_heartbeat: datetime | None = None
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
        args: Positional arguments for constructor.
        kwargs: Keyword arguments for constructor.
        note: Optional human-readable description.
        lifecycle: LONG_RUNNING or ONE_SHOT.
        role: Process role (CORE, TASK, STRATEGY, BACKTEST).
        tags: Tuple of string tags for filtering.
        parameters_schema: Optional JSON schema for parameters.
    """

    name: str
    enabled: bool
    mode: str
    class_path: str
    method: str
    args: list[Any]
    kwargs: dict[str, Any]
    note: str | None = None
    lifecycle: ProcessLifecycleEnum = ProcessLifecycleEnum.LONG_RUNNING
    role: ProcessRoleEnum = ProcessRoleEnum.CORE
    tags: tuple[str, ...] = ()
    parameters_schema: dict[str, Any] | None = None
