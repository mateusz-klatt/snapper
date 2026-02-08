"""Process management schemas for the REST API.

This module defines request/response schemas for the process management
endpoints, supporting process lifecycle operations (start, stop, create).
"""

from typing import Any
from typing import Literal

from pydantic import Field

from snapper.api.schemas.base import StrictApiSchema
from snapper.core.types import ProcessLifecycleType
from snapper.core.types import ProcessMode
from snapper.core.types import ProcessRoleType
from snapper.core.types import ProcessRunStatusType
from snapper.core.types import SpawnerProcessStatus
from snapper.core.types import StartProcessStatus
from snapper.core.types import StopProcessStatus

_UNIQUE_PROCESS_NAME_DESC = "Unique process name"
_CLASS_PATH_DESC = "Full Python class path"
_METHOD_DESC = "Entry point method name"
_LIFECYCLE_DESC = "Process lifecycle type"
_ROLE_DESC = "Process role category"
_PROCESS_NAME_DESC = "Process name"

__all__ = [
    "ProcessStartRequest",
    "ProcessCreateRequest",
    "TradeStartRequest",
    "BacktestRequest",
    "ProcessStatus",
    "SystemStatus",
    "BacktestStatus",
    "BacktestOutput",
    "AvailableProcess",
    "AvailableProcessesResponse",
    "ConfiguredProcess",
    "ConfiguredProcessesResponse",
    "ProcessCreatedInfo",
    "ProcessCreateResponse",
    "ProcessSchemaResponse",
    "ProcessRun",
    "ProcessRunsResponse",
    "ProcessRuntimeStatus",
    "ProcessStartResponse",
    "ProcessStopResponse",
    "StrategyStatusPayload",
    "ProcessLifecycleType",
    "ProcessRoleType",
    "ProcessRunStatusType",
]


class ProcessStartRequest(StrictApiSchema):
    """Process start request schema.

    Used to start a configured process with optional parameter overrides.

    Attributes:
        mode: Execution mode (thread/process) override.
        args: Constructor positional arguments override.
        kwargs: Constructor keyword arguments override.
        autostart: Toggle autostart flag (None keeps stored value).
    """

    mode: ProcessMode | None = Field(
        None,
        description="Execution mode (thread/process) - for ProcessLauncherService, not constructor",
        examples=["thread", "process"],
    )
    args: list[Any] | None = Field(
        None,
        description="Constructor positional arguments override",
        examples=[["arg1", 2, 3.0]],
    )
    kwargs: dict[str, Any] | None = Field(
        None,
        description="Constructor keyword arguments override",
        examples=[{"endpoint": "tcp://0.0.0.0:5555"}],
    )
    autostart: bool | None = Field(
        None,
        description="Toggle autostart flag; None keeps stored value",
    )


class ProcessCreateRequest(StrictApiSchema):
    """Process creation request schema.

    Used to create a new process configuration from a template.

    Attributes:
        name: Unique process name (lowercase alphanumeric with underscores).
        template: Registered process identifier used as template.
        enabled: Whether process should autostart on boot.
        mode: Execution mode override (thread/process).
        args: Constructor positional arguments.
        kwargs: Constructor keyword arguments.
        note: Optional note stored alongside configuration.
    """

    name: str = Field(
        ...,
        description=_UNIQUE_PROCESS_NAME_DESC,
        pattern=r"^[a-z0-9_]+$",
        min_length=3,
        max_length=64,
    )
    template: str = Field(
        ...,
        description="Registered process identifier used as template",
    )
    enabled: bool | None = Field(
        None,
        description="Whether process should autostart on boot",
    )
    mode: ProcessMode | None = Field(
        None,
        description="Execution mode override (thread/process)",
    )
    args: list[Any] | None = Field(
        None,
        description="Constructor positional arguments",
    )
    kwargs: dict[str, Any] | None = Field(
        None,
        description="Constructor keyword arguments",
    )
    note: str | None = Field(
        None,
        description="Optional note stored alongside configuration",
        max_length=512,
    )


class TradeStartRequest(StrictApiSchema):
    """Trade execution start request schema.

    Used to start live trading with a specific strategy.

    Attributes:
        strategy: Strategy name to trade with.
        paper: Enable paper trading mode.
    """

    strategy: str = Field(description="Strategy name to trade with")
    paper: bool = Field(default=False, description="Enable paper trading mode")


class BacktestRequest(StrictApiSchema):
    """Backtest request schema.

    Used to start a backtest run.

    Attributes:
        strategy: Strategy name to backtest.
        start: Start date in YYYY-MM-DD format.
        end: End date in YYYY-MM-DD format.
    """

    strategy: str = Field(description="Strategy name to backtest")
    start: str = Field(description="Start date in YYYY-MM-DD format")
    end: str = Field(description="End date in YYYY-MM-DD format")


class ProcessStatus(StrictApiSchema):
    """Process status response schema.

    Represents the current status of a single process.

    Attributes:
        status: Process status (not_running, running, stopped, completed, error).
        pid: Process ID if running.
        started_at: Start time in ISO format.
        command: Command that was executed.
        exit_code: Exit code if stopped.
        error: Error message if failed.
    """

    status: SpawnerProcessStatus = Field(
        description="Process status: not_running, running, stopped, completed, error"
    )
    pid: int | None = Field(default=None, description="Process ID if running")
    started_at: str | None = Field(default=None, description="Start time in ISO format")
    command: str | None = Field(default=None, description="Command that was executed")
    exit_code: int | None = Field(default=None, description="Exit code if stopped")
    error: str | None = Field(default=None, description="Error message if failed")


class StrategyStatusPayload(StrictApiSchema):
    """Strategy process status payload for the system status endpoint.

    Attributes:
        strategy_name: Name of the strategy.
        status: Current strategy status string.
        details: Full raw status dictionary from the process.
        signals_generated: Number of signals generated.
        trades_executed: Number of trades executed.
        last_signal: Last signal description.
        last_signal_time: Timestamp of last signal.
        pnl: Current profit and loss.
        pid: Process ID.
        uptime: Process uptime string.
    """

    strategy_name: str = Field(description="Strategy name")
    status: str = Field(description="Current strategy status")
    details: dict[str, Any] = Field(default_factory=dict, description="Full raw status")
    signals_generated: int | None = Field(None, description="Signals generated count")
    trades_executed: int | None = Field(None, description="Trades executed count")
    last_signal: str | None = Field(None, description="Last signal description")
    last_signal_time: str | None = Field(None, description="Last signal timestamp")
    pnl: float | None = Field(None, description="Current PnL")
    pid: int | None = Field(None, description="Process ID")
    uptime: str | None = Field(None, description="Process uptime")


class SystemStatus(StrictApiSchema):
    """System-wide status response schema.

    Provides status of the trader process, backtests, and active strategies.

    Attributes:
        trader: Trader process status.
        backtests: Status of backtest processes by ID.
        strategies: List of active strategies from strategy_runner.
    """

    trader: ProcessStatus
    backtests: dict[str, ProcessStatus]
    strategies: list[StrategyStatusPayload] = Field(
        default_factory=list, description="List of active strategies from strategy_runner"
    )


class BacktestStatus(StrictApiSchema):
    """Backtest status response schema.

    Represents the current status of a backtest run.

    Attributes:
        status: Backtest status.
        backtest_id: Unique backtest identifier.
        pid: Process ID if running.
        strategy: Strategy being backtested.
        start_date: Backtest start date.
        end_date: Backtest end date.
        error: Error message if failed.
    """

    status: SpawnerProcessStatus
    backtest_id: str | None = None
    pid: int | None = None
    strategy: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    error: str | None = None


class BacktestOutput(StrictApiSchema):
    """Backtest output response schema.

    Contains output lines from a backtest run.

    Attributes:
        backtest_id: Unique backtest identifier.
        output_lines: List of output lines.
        status: Current backtest status.
    """

    backtest_id: str
    output_lines: list[str]
    status: SpawnerProcessStatus


class AvailableProcess(StrictApiSchema):
    """Available process template response schema.

    Describes a registered process that can be instantiated.

    Attributes:
        name: Process identifier.
        class_path: Full Python class path.
        method: Entry point method name.
        description: Human-readable description.
        lifecycle: Process lifecycle type (long_running/one_shot).
        role: Process role category.
        tags: Categorization tags.
        parameters_schema: JSON Schema for parameters.
    """

    name: str = Field(description="Process identifier")
    class_path: str = Field(description=_CLASS_PATH_DESC)
    method: str = Field(description=_METHOD_DESC)
    description: str = Field(description="Human-readable description")
    lifecycle: ProcessLifecycleType = Field(description=_LIFECYCLE_DESC)
    role: ProcessRoleType = Field(description=_ROLE_DESC)
    tags: list[str] = Field(default_factory=list, description="Categorization tags")
    parameters_schema: dict[str, Any] | None = Field(None, description="JSON Schema for parameters")


class AvailableProcessesResponse(StrictApiSchema):
    """Available processes list response schema.

    Attributes:
        processes: List of available process templates.
        count: Total number of available processes.
    """

    processes: list[AvailableProcess]
    count: int


class ConfiguredProcess(StrictApiSchema):
    """Configured process response schema.

    Describes a process configuration with its current runtime state.

    Attributes:
        name: Unique process name.
        enabled: Whether process autostarts on boot.
        running: Whether process is currently running.
        mode: Execution mode (thread/process).
        class_path: Full Python class path.
        method: Entry point method name.
        args: Constructor arguments.
        kwargs: Constructor keyword arguments.
        note: Optional note.
        lifecycle: Process lifecycle type.
        role: Process role category.
        tags: Categorization tags.
        parameters_schema: JSON Schema for parameters.
        is_one_shot: Whether process is one-shot task.
        active_run_id: Active run ID if running.
    """

    name: str = Field(description=_UNIQUE_PROCESS_NAME_DESC)
    enabled: bool = Field(description="Whether process autostarts on boot")
    running: bool = Field(description="Whether process is currently running")
    mode: ProcessMode = Field(description="Execution mode (thread/process)")
    class_path: str = Field(description=_CLASS_PATH_DESC)
    method: str = Field(description=_METHOD_DESC)
    args: list[Any] = Field(default_factory=list, description="Constructor arguments")
    kwargs: dict[str, Any] = Field(default_factory=dict, description="Constructor kwargs")
    note: str | None = Field(None, description="Optional note")
    lifecycle: ProcessLifecycleType = Field(description=_LIFECYCLE_DESC)
    role: ProcessRoleType = Field(description=_ROLE_DESC)
    tags: list[str] = Field(default_factory=list, description="Categorization tags")
    parameters_schema: dict[str, Any] | None = Field(None, description="JSON Schema for parameters")
    is_one_shot: bool = Field(description="Whether process is one-shot task")
    active_run_id: str | None = Field(None, description="Active run ID if running")


class ConfiguredProcessesResponse(StrictApiSchema):
    """Configured processes list response schema.

    Attributes:
        processes: List of configured processes.
        count: Total number of configured processes.
    """

    processes: list[ConfiguredProcess]
    count: int


class ProcessCreatedInfo(StrictApiSchema):
    """Process creation info schema.

    Attributes:
        name: Unique process name.
        template: Template used for creation.
    """

    name: str = Field(description=_UNIQUE_PROCESS_NAME_DESC)
    template: str = Field(description="Template used for creation")


class ProcessCreateResponse(StrictApiSchema):
    """Process creation response schema.

    Attributes:
        status: Operation status ('created').
        process: Created process info.
    """

    status: Literal["created"] = Field(description="Operation status")
    process: ProcessCreatedInfo = Field(description="Created process info")


class ProcessSchemaResponse(StrictApiSchema):
    """Process schema response.

    Describes a process template schema with default values.

    Attributes:
        name: Process identifier.
        description: Human-readable description.
        class_path: Full Python class path.
        method: Entry point method name.
        default_enabled: Default autostart setting.
        default_mode: Default execution mode.
        default_args: Default arguments.
        default_kwargs: Default keyword arguments.
        lifecycle: Process lifecycle type.
    """

    name: str = Field(description="Process identifier")
    description: str = Field(description="Human-readable description")
    class_path: str = Field(description=_CLASS_PATH_DESC)
    method: str = Field(description=_METHOD_DESC)
    default_enabled: bool = Field(description="Default autostart setting")
    default_mode: ProcessMode = Field(description="Default execution mode")
    default_args: list[Any] = Field(default_factory=list, description="Default arguments")
    default_kwargs: dict[str, Any] = Field(default_factory=dict, description="Default kwargs")
    lifecycle: ProcessLifecycleType = Field(description=_LIFECYCLE_DESC)


class ProcessRun(StrictApiSchema):
    """Process run record response schema.

    Represents a single execution run of a process.

    Attributes:
        run_id: Unique run identifier.
        process_name: Process name.
        status: Run status.
        role: Process role.
        lifecycle: Process lifecycle.
        parameters: Run parameters.
        result: Run result if completed.
        error: Error message if failed.
        tags: Process tags.
        started_at: Start time in ISO format.
        completed_at: Completion time if finished.
    """

    run_id: str = Field(description="Unique run identifier")
    process_name: str = Field(description=_PROCESS_NAME_DESC)
    status: ProcessRunStatusType = Field(description="Run status")
    role: ProcessRoleType = Field(description="Process role")
    lifecycle: ProcessLifecycleType = Field(description="Process lifecycle")
    parameters: dict[str, Any] | None = Field(None, description="Run parameters")
    result: dict[str, Any] | None = Field(None, description="Run result if completed")
    error: str | None = Field(None, description="Error message if failed")
    tags: list[str] = Field(default_factory=list, description="Process tags")
    started_at: str = Field(description="Start time in ISO format")
    completed_at: str | None = Field(None, description="Completion time if finished")


class ProcessRunsResponse(StrictApiSchema):
    """Process runs list response schema.

    Attributes:
        runs: List of process runs.
        count: Total number of runs.
    """

    runs: list[ProcessRun]
    count: int


class ProcessRuntimeStatus(StrictApiSchema):
    """Process runtime status response schema.

    Represents the current runtime state of a process.

    Attributes:
        name: Process name.
        running: Whether process is currently running.
        role: Process role category.
        lifecycle: Process lifecycle type.
        active_run_id: Active run ID if running.
        details: Additional process details.
    """

    name: str = Field(description=_PROCESS_NAME_DESC)
    running: bool = Field(description="Whether process is currently running")
    role: ProcessRoleType = Field(description=_ROLE_DESC)
    lifecycle: ProcessLifecycleType = Field(description=_LIFECYCLE_DESC)
    active_run_id: str | None = Field(None, description="Active run ID if running")
    details: dict[str, Any] | None = Field(None, description="Additional process details")


class ProcessStartResponse(StrictApiSchema):
    """Process start response schema.

    Attributes:
        status: Operation status (success, already_running, error).
        name: Process name.
        run_id: Run ID if started.
        message: Additional message.
    """

    status: StartProcessStatus = Field(
        description="Operation status (success, already_running, error)"
    )
    name: str = Field(description=_PROCESS_NAME_DESC)
    run_id: str | None = Field(None, description="Run ID if started")
    message: str | None = Field(None, description="Additional message")


class ProcessStopResponse(StrictApiSchema):
    """Process stop response schema.

    Attributes:
        status: Operation status (success, not_running, error).
        name: Process name.
        message: Additional message.
    """

    status: StopProcessStatus = Field(description="Operation status (success, not_running, error)")
    name: str = Field(description=_PROCESS_NAME_DESC)
    message: str | None = Field(None, description="Additional message")
