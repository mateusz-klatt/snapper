"""Process manager enumerations module.

This module defines enumerations for process lifecycle management,
including process types, roles, and run statuses.
"""

from enum import StrEnum


class ProcessLifecycleEnum(StrEnum):
    """Process lifecycle type enumeration.

    Defines whether a process is designed to run continuously
    or execute once and terminate.

    Attributes:
        LONG_RUNNING: Process runs continuously until stopped
            (e.g., data feeds, trading coordinators).
        ONE_SHOT: Process executes once and terminates
            (e.g., backfill tasks, symbol updates).
    """

    LONG_RUNNING = "long_running"
    ONE_SHOT = "one_shot"


class ProcessRoleEnum(StrEnum):
    """Process role enumeration.

    Categorizes processes by their function in the system.

    Attributes:
        CORE: Essential system processes (broker, coordinator).
        TASK: Maintenance and utility tasks (backfill, symbol updates).
        STRATEGY: Trading strategy processes.
        BACKTEST: Historical backtesting processes.
    """

    CORE = "core"
    TASK = "task"
    STRATEGY = "strategy"
    BACKTEST = "backtest"


class ProcessRunStatusEnum(StrEnum):
    """Process run status enumeration.

    Tracks the current state of a process execution.

    Attributes:
        RUNNING: Process is currently executing.
        SUCCEEDED: Process completed successfully.
        FAILED: Process terminated with an error.
        CANCELLED: Process was manually stopped.
    """

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
