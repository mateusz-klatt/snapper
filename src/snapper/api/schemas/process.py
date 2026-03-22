"""Process management schemas for the REST API.

This module defines request/response schemas for the process management
endpoints, supporting process lifecycle operations (start, stop, create).

Schema hierarchy follows these rules:

- Nested structural schemas (fields inside other schemas) are plain BaseModel
  with ``extra="forbid"`` and no provenance fields.
- Top-level response schemas use ``PayloadResponse[..., XxxData]`` wrapping
  a new ``XxxData(StrictDataSchema)`` that carries the domain fields.
- Schemas that are payloads in ``PayloadListResponse`` stay as
  ``StrictDataSchema`` (first-class objects with their own provenance).
- Request schemas stay as ``StrictDataSchema``.
"""

from typing import Any
from typing import Literal

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

from snapper.api.schemas.base import PayloadListResponse
from snapper.api.schemas.base import PayloadRequest
from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictDataSchema
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
_RUNNING_DESC = "Whether process is currently running"
_ENABLED_DESC = "Whether process autostarts on boot"

__all__ = [
    "ProcessStartRequest",
    "ProcessCreateRequest",
    "TradeStartRequest",
    "BacktestRequest",
    "ProcessStatus",
    "StrategyStatusPayload",
    "SystemStatusData",
    "SystemStatusResponse",
    "BacktestStatusData",
    "BacktestStatusResponse",
    "BacktestOutputData",
    "BacktestOutputResponse",
    "AvailableProcess",
    "AvailableProcessesResponse",
    "ConfiguredProcess",
    "ConfiguredProcessesResponse",
    "ProcessCreatedInfo",
    "ProcessCreateData",
    "ProcessCreateResponse",
    "ProcessSchemaData",
    "ProcessSchemaResponse",
    "ProcessCategoryCount",
    "ProcessSummaryData",
    "ProcessSummaryResponse",
    "ProcessRun",
    "ProcessRunsResponse",
    "ProcessRuntimeStatusData",
    "ProcessRuntimeStatusResponse",
    "ProcessStartData",
    "ProcessStartResponse",
    "ProcessStopData",
    "ProcessStopResponse",
    "StrategyProcess",
    "StrategyListResponse",
    "ProcessLifecycleType",
    "ProcessRoleType",
    "ProcessRunStatusType",
]


class ProcessStartBody(BaseModel):
    """Process start request body.

    Attributes:
        mode: Execution mode (thread/process) override.
        args: Constructor positional arguments override.
        kwargs: Constructor keyword arguments override.
        autostart: Toggle autostart flag (None keeps stored value).
    """

    model_config = ConfigDict(extra="forbid")

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


class ProcessStartRequest(PayloadRequest[Literal["process_start_request"], ProcessStartBody]):
    """Process start request envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["process_start_request"] = "process_start_request"


class ProcessCreateBody(BaseModel):
    """Process creation request body.

    Attributes:
        name: Unique process name (lowercase alphanumeric with underscores).
        template: Registered process identifier used as template.
        enabled: Whether process should autostart on boot.
        mode: Execution mode override (thread/process).
        args: Constructor positional arguments.
        kwargs: Constructor keyword arguments.
        note: Optional note stored alongside configuration.
    """

    model_config = ConfigDict(extra="forbid")

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


class ProcessCreateRequest(PayloadRequest[Literal["process_create_request"], ProcessCreateBody]):
    """Process creation request envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["process_create_request"] = "process_create_request"


class TradeStartBody(BaseModel):
    """Trade execution start request body.

    Attributes:
        strategy: Strategy name to trade with.
        paper: Enable paper trading mode.
    """

    model_config = ConfigDict(extra="forbid")

    strategy: str = Field(description="Strategy name to trade with")
    paper: bool = Field(default=False, description="Enable paper trading mode")


class TradeStartRequest(PayloadRequest[Literal["trade_start_request"], TradeStartBody]):
    """Trade execution start request envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["trade_start_request"] = "trade_start_request"


class BacktestBody(BaseModel):
    """Backtest request body.

    Attributes:
        strategy: Strategy name to backtest.
        start: Start date in YYYY-MM-DD format.
        end: End date in YYYY-MM-DD format.
    """

    model_config = ConfigDict(extra="forbid")

    strategy: str = Field(description="Strategy name to backtest")
    start: str = Field(description="Start date in YYYY-MM-DD format")
    end: str = Field(description="End date in YYYY-MM-DD format")


class BacktestRequest(PayloadRequest[Literal["backtest_request"], BacktestBody]):
    """Backtest request envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["backtest_request"] = "backtest_request"


class ProcessStatus(BaseModel):
    """Process status nested structural schema.

    Represents the current status of a single process. Used as a nested
    field inside other schemas (e.g. SystemStatusData).

    Attributes:
        status: Process status (not_running, running, stopped, completed, error).
        pid: Process ID if running.
        started_at: Start time in ISO format.
        command: Command that was executed.
        exit_code: Exit code if stopped.
        error: Error message if failed.
    """

    model_config = ConfigDict(extra="forbid")

    status: SpawnerProcessStatus = Field(
        description="Process status: not_running, running, stopped, completed, error"
    )
    pid: int | None = Field(default=None, description="Process ID if running")
    started_at: str | None = Field(default=None, description="Start time in ISO format")
    command: str | None = Field(default=None, description="Command that was executed")
    exit_code: int | None = Field(default=None, description="Exit code if stopped")
    error: str | None = Field(default=None, description="Error message if failed")


class StrategyStatusPayload(BaseModel):
    """Strategy process status payload for the system status endpoint.

    Nested structural schema used inside SystemStatusData.

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

    model_config = ConfigDict(extra="forbid")

    strategy_name: str = Field(description="Strategy name")
    status: str = Field(description="Current strategy status")
    details: dict[str, Any] = Field(default={}, description="Full raw status")
    signals_generated: int | None = Field(None, description="Signals generated count")
    trades_executed: int | None = Field(None, description="Trades executed count")
    last_signal: str | None = Field(None, description="Last signal description")
    last_signal_time: str | None = Field(None, description="Last signal timestamp")
    pnl: float | None = Field(None, description="Current PnL")
    pid: int | None = Field(None, description="Process ID")
    uptime: str | None = Field(None, description="Process uptime")


class ProcessCategoryCount(BaseModel):
    """Running/total count for a process category.

    Nested structural schema used inside ProcessSummaryData.

    Attributes:
        running: Number of currently running processes.
        total: Total number of configured processes.
    """

    model_config = ConfigDict(extra="forbid")

    running: int = Field(description="Number of currently running processes")
    total: int = Field(description="Total number of configured processes")


class ProcessCreatedInfo(BaseModel):
    """Process creation info nested structural schema.

    Used inside ProcessCreateData to describe the created process.

    Attributes:
        name: Unique process name.
        template: Template used for creation.
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(description=_UNIQUE_PROCESS_NAME_DESC)
    template: str = Field(description="Template used for creation")


class SystemStatusData(StrictDataSchema[Literal["system_status"]]):
    """System-wide status data schema.

    Provides status of the trader process, backtests, and active strategies.

    Attributes:
        type: Payload item type discriminator.
        trader: Trader process status.
        backtests: Status of backtest processes by ID.
        strategies: List of active strategies from strategy_runner.
    """

    type: Literal["system_status"] = "system_status"
    trader: ProcessStatus
    backtests: dict[str, ProcessStatus]
    strategies: list[StrategyStatusPayload] = Field(
        default=[], description="List of active strategies from strategy_runner"
    )


class SystemStatusResponse(PayloadResponse[Literal["system_status_response"], SystemStatusData]):
    """System-wide status REST response envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["system_status_response"] = "system_status_response"


class BacktestStatusData(StrictDataSchema[Literal["backtest_status"]]):
    """Backtest status data schema.

    Represents the current status of a backtest run.

    Attributes:
        type: Payload item type discriminator.
        status: Backtest status.
        backtest_id: Unique backtest identifier.
        pid: Process ID if running.
        strategy: Strategy being backtested.
        start_date: Backtest start date.
        end_date: Backtest end date.
        error: Error message if failed.
    """

    type: Literal["backtest_status"] = "backtest_status"
    status: SpawnerProcessStatus
    backtest_id: str | None = None
    pid: int | None = None
    strategy: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    error: str | None = None


class BacktestStatusResponse(
    PayloadResponse[Literal["backtest_status_response"], BacktestStatusData]
):
    """Backtest status REST response envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["backtest_status_response"] = "backtest_status_response"


class BacktestOutputData(StrictDataSchema[Literal["backtest_output"]]):
    """Backtest output data schema.

    Contains output lines from a backtest run.

    Attributes:
        type: Payload item type discriminator.
        backtest_id: Unique backtest identifier.
        output_lines: List of output lines.
        status: Current backtest status.
    """

    type: Literal["backtest_output"] = "backtest_output"
    backtest_id: str
    output_lines: list[str]
    status: SpawnerProcessStatus


class BacktestOutputResponse(
    PayloadResponse[Literal["backtest_output_response"], BacktestOutputData]
):
    """Backtest output REST response envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["backtest_output_response"] = "backtest_output_response"


class AvailableProcess(StrictDataSchema[Literal["available_process"]]):
    """Available process template response schema.

    Describes a registered process that can be instantiated.
    First-class payload in AvailableProcessesResponse.

    Attributes:
        type: Payload item type discriminator.
        name: Process identifier.
        class_path: Full Python class path.
        method: Entry point method name.
        description: Human-readable description.
        lifecycle: Process lifecycle type (long_running/one_shot).
        role: Process role category.
        tags: Categorization tags.
        parameters_schema: JSON Schema for parameters.
    """

    type: Literal["available_process"] = "available_process"
    name: str = Field(description="Process identifier")
    class_path: str = Field(description=_CLASS_PATH_DESC)
    method: str = Field(description=_METHOD_DESC)
    description: str = Field(description="Human-readable description")
    lifecycle: ProcessLifecycleType = Field(description=_LIFECYCLE_DESC)
    role: ProcessRoleType = Field(description=_ROLE_DESC)
    tags: list[str] = Field(default=[], description="Categorization tags")
    parameters_schema: dict[str, Any] | None = Field(None, description="JSON Schema for parameters")


class AvailableProcessesResponse(
    PayloadListResponse[Literal["available_processes"], AvailableProcess]
):
    """Available processes list response schema.

    Attributes:
        type: Payload item type discriminator.
        payload: List of available process templates.
        count: Total number of available processes.
    """

    type: Literal["available_processes"] = "available_processes"


class ConfiguredProcess(StrictDataSchema[Literal["configured_process"]]):
    """Configured process response schema.

    Describes a process configuration with its current runtime state.
    First-class payload in ConfiguredProcessesResponse.

    Attributes:
        type: Payload item type discriminator.
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
        active_public_id: Active public ID if running.
    """

    type: Literal["configured_process"] = "configured_process"
    name: str = Field(description=_UNIQUE_PROCESS_NAME_DESC)
    enabled: bool = Field(description=_ENABLED_DESC)
    running: bool = Field(description=_RUNNING_DESC)
    mode: ProcessMode = Field(description="Execution mode (thread/process)")
    class_path: str = Field(description=_CLASS_PATH_DESC)
    method: str = Field(description=_METHOD_DESC)
    args: list[Any] = Field(default=[], description="Constructor arguments")
    kwargs: dict[str, Any] = Field(default={}, description="Constructor kwargs")
    note: str | None = Field(None, description="Optional note")
    lifecycle: ProcessLifecycleType = Field(description=_LIFECYCLE_DESC)
    role: ProcessRoleType = Field(description=_ROLE_DESC)
    tags: list[str] = Field(default=[], description="Categorization tags")
    parameters_schema: dict[str, Any] | None = Field(None, description="JSON Schema for parameters")
    is_one_shot: bool = Field(description="Whether process is one-shot task")
    active_public_id: str | None = Field(None, description="Active public ID if running")


class ConfiguredProcessesResponse(
    PayloadListResponse[Literal["configured_processes"], ConfiguredProcess]
):
    """Configured processes list response schema.

    Attributes:
        type: Payload item type discriminator.
        payload: List of configured processes.
        count: Total number of configured processes.
    """

    type: Literal["configured_processes"] = "configured_processes"


class ProcessSummaryData(StrictDataSchema[Literal["process_summary"]]):
    """Lightweight process summary data for the overview dashboard.

    Attributes:
        type: Payload item type discriminator.
        feeds: Count of feed publisher processes.
        strategies: Count of strategy processes.
        executors: Count of executor processes.
        brokers: Count of broker processes.
    """

    type: Literal["process_summary"] = "process_summary"
    feeds: ProcessCategoryCount = Field(description="Feed publisher process counts")
    strategies: ProcessCategoryCount = Field(description="Strategy process counts")
    executors: ProcessCategoryCount = Field(description="Executor process counts")
    brokers: ProcessCategoryCount = Field(description="Broker process counts")


class ProcessSummaryResponse(
    PayloadResponse[Literal["process_summary_response"], ProcessSummaryData]
):
    """Process summary REST response envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["process_summary_response"] = "process_summary_response"


class StrategyProcess(StrictDataSchema[Literal["strategy_process"]]):
    """Lightweight strategy process info for read-only views.

    First-class payload in StrategyListResponse.

    Attributes:
        type: Payload item type discriminator.
        name: Unique process name.
        running: Whether process is currently running.
        enabled: Whether process autostarts on boot.
        mode: Execution mode (thread/process).
    """

    type: Literal["strategy_process"] = "strategy_process"
    name: str = Field(description=_UNIQUE_PROCESS_NAME_DESC)
    running: bool = Field(description=_RUNNING_DESC)
    enabled: bool = Field(description=_ENABLED_DESC)
    mode: ProcessMode = Field(description="Execution mode (thread/process)")


class StrategyListResponse(PayloadListResponse[Literal["strategy_list"], StrategyProcess]):
    """Strategy processes list response.

    Attributes:
        type: Payload item type discriminator.
        payload: List of strategy processes.
        count: Total number of strategies.
    """

    type: Literal["strategy_list"] = "strategy_list"


class ProcessCreateData(StrictDataSchema[Literal["process_create"]]):
    """Process creation data schema.

    Attributes:
        type: Payload item type discriminator.
        status: Operation status ('created').
        process: Created process info.
    """

    type: Literal["process_create"] = "process_create"
    status: Literal["created"] = Field(description="Operation status")
    process: ProcessCreatedInfo = Field(description="Created process info")


class ProcessCreateResponse(PayloadResponse[Literal["process_create_response"], ProcessCreateData]):
    """Process creation REST response envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["process_create_response"] = "process_create_response"


class ProcessSchemaData(StrictDataSchema[Literal["process_schema"]]):
    """Process schema data.

    Describes a process template schema with default values.

    Attributes:
        type: Payload item type discriminator.
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

    type: Literal["process_schema"] = "process_schema"
    name: str = Field(description="Process identifier")
    description: str = Field(description="Human-readable description")
    class_path: str = Field(description=_CLASS_PATH_DESC)
    method: str = Field(description=_METHOD_DESC)
    default_enabled: bool = Field(description="Default autostart setting")
    default_mode: ProcessMode = Field(description="Default execution mode")
    default_args: list[Any] = Field(default=[], description="Default arguments")
    default_kwargs: dict[str, Any] = Field(default={}, description="Default kwargs")
    lifecycle: ProcessLifecycleType = Field(description=_LIFECYCLE_DESC)


class ProcessSchemaResponse(PayloadResponse[Literal["process_schema_response"], ProcessSchemaData]):
    """Process schema REST response envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["process_schema_response"] = "process_schema_response"


class ProcessRun(StrictDataSchema[Literal["process_run"]]):
    """Process run record response schema.

    Represents a single execution run of a process.
    First-class payload in ProcessRunsResponse.

    Attributes:
        type: Payload item type discriminator.
        public_id: Unique run identifier.
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

    type: Literal["process_run"] = "process_run"
    public_id: str = Field(description="Unique run identifier")
    process_name: str = Field(description=_PROCESS_NAME_DESC)
    status: ProcessRunStatusType = Field(description="Run status")
    role: ProcessRoleType = Field(description="Process role")
    lifecycle: ProcessLifecycleType = Field(description="Process lifecycle")
    parameters: dict[str, Any] | None = Field(None, description="Run parameters")
    result: dict[str, Any] | None = Field(None, description="Run result if completed")
    error: str | None = Field(None, description="Error message if failed")
    tags: list[str] = Field(default=[], description="Process tags")
    started_at: str = Field(description="Start time in ISO format")
    completed_at: str | None = Field(None, description="Completion time if finished")


class ProcessRunsResponse(PayloadListResponse[Literal["process_runs"], ProcessRun]):
    """Process runs list response schema.

    Attributes:
        type: Payload item type discriminator.
        payload: List of process runs.
        count: Total number of runs.
    """

    type: Literal["process_runs"] = "process_runs"


class ProcessRuntimeStatusData(StrictDataSchema[Literal["process_runtime_status"]]):
    """Process runtime status data schema.

    Represents the current runtime state of a process.

    Attributes:
        type: Payload item type discriminator.
        name: Process name.
        running: Whether process is currently running.
        role: Process role category.
        lifecycle: Process lifecycle type.
        active_public_id: Active public ID if running.
        details: Additional process details.
    """

    type: Literal["process_runtime_status"] = "process_runtime_status"
    name: str = Field(description=_PROCESS_NAME_DESC)
    running: bool = Field(description=_RUNNING_DESC)
    role: ProcessRoleType = Field(description=_ROLE_DESC)
    lifecycle: ProcessLifecycleType = Field(description=_LIFECYCLE_DESC)
    active_public_id: str | None = Field(None, description="Active public ID if running")
    details: dict[str, Any] | None = Field(None, description="Additional process details")


class ProcessRuntimeStatusResponse(
    PayloadResponse[Literal["process_runtime_status_response"], ProcessRuntimeStatusData]
):
    """Process runtime status REST response envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["process_runtime_status_response"] = "process_runtime_status_response"


class ProcessStartData(StrictDataSchema[Literal["process_start"]]):
    """Process start data schema.

    Attributes:
        type: Payload item type discriminator.
        status: Operation status (success, already_running, error).
        name: Process name.
        process_public_id: Public ID of the started process run (if started).
        message: Additional message.
    """

    type: Literal["process_start"] = "process_start"
    status: StartProcessStatus = Field(
        description="Operation status (success, already_running, error)"
    )
    name: str = Field(description=_PROCESS_NAME_DESC)
    process_public_id: str | None = Field(None, description="Public ID if started")
    message: str | None = Field(None, description="Additional message")


class ProcessStartResponse(PayloadResponse[Literal["process_start_response"], ProcessStartData]):
    """Process start REST response envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["process_start_response"] = "process_start_response"


class ProcessStopData(StrictDataSchema[Literal["process_stop"]]):
    """Process stop data schema.

    Attributes:
        type: Payload item type discriminator.
        status: Operation status (success, not_running, error).
        name: Process name.
        message: Additional message.
    """

    type: Literal["process_stop"] = "process_stop"
    status: StopProcessStatus = Field(description="Operation status (success, not_running, error)")
    name: str = Field(description=_PROCESS_NAME_DESC)
    message: str | None = Field(None, description="Additional message")


class ProcessStopResponse(PayloadResponse[Literal["process_stop_response"], ProcessStopData]):
    """Process stop REST response envelope.

    Attributes:
        type: Payload item type discriminator.
    """

    type: Literal["process_stop_response"] = "process_stop_response"
