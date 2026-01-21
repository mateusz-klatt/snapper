"""REST API routes for process lifecycle management.

This module provides FastAPI routes for managing background processes
in the Snapper trading platform. Processes include market data feeds,
trading strategies, executors, and other long-running services.

Endpoints:
    - ``GET /processes/available`` - List registered process templates.
    - ``GET /processes/configured`` - List configured process instances.
    - ``POST /processes`` - Create new process configuration.
    - ``GET /processes/schema/{name}`` - Get process parameter schema.
    - ``POST /processes/{name}/start`` - Start a configured process.
    - ``POST /processes/{name}/stop`` - Stop a running process.
    - ``GET /processes/runs`` - List historical process runs.

Process Types:
    - **Long-running**: Continuous services (feeds, executors)
    - **One-shot**: Tasks that complete (backfill, sync)

All endpoints require MANAGE_PROCESSES permission (operator/admin role).

Example:
    Start a process::

        POST /snapper/api/processes/my-strategy/start
        {"mode": "subprocess", "autostart": true}
"""

from collections.abc import Iterable
from typing import Any
from typing import cast

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request

from snapper.api.schemas.process import AvailableProcess
from snapper.api.schemas.process import AvailableProcessesResponse
from snapper.api.schemas.process import ConfiguredProcess
from snapper.api.schemas.process import ConfiguredProcessesResponse
from snapper.api.schemas.process import ProcessCreatedInfo
from snapper.api.schemas.process import ProcessCreateRequest
from snapper.api.schemas.process import ProcessCreateResponse
from snapper.api.schemas.process import ProcessLifecycleType
from snapper.api.schemas.process import ProcessRoleType
from snapper.api.schemas.process import ProcessRun
from snapper.api.schemas.process import ProcessRunsResponse
from snapper.api.schemas.process import ProcessSchemaResponse
from snapper.api.schemas.process import ProcessStartRequest
from snapper.api.schemas.process import ProcessStartResponse
from snapper.api.schemas.process import ProcessStopResponse
from snapper.application.process_manager.enums import ProcessLifecycleEnum
from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.launcher import ProcessLauncherService
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.registry import get_registered_processes
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.user import UserProfile
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings

__all__ = [
    "router",
    "get_process_factory",
    "get_process_schema",
    "create_process_configuration",
]
router = APIRouter(prefix="/processes", tags=["processes"])


def get_process_factory(request: Request) -> ProcessLauncherService:
    """FastAPI dependency to get the process launcher service.

    Args:
        request: FastAPI request containing app state.

    Returns:
        ProcessLauncherService instance from app state.
    """
    factory: ProcessLauncherService = request.app.state.process_factory
    return factory


@router.get("/available")
async def list_available_processes(
    _user: UserProfile = Depends(require_permission(Permission.MANAGE_PROCESSES)),
) -> AvailableProcessesResponse:
    registry = get_registered_processes()
    processes: list[AvailableProcess] = []
    for name, metadata in registry.items():
        lifecycle_meta = metadata.get("lifecycle", ProcessLifecycleEnum.LONG_RUNNING)
        lifecycle_value = (
            lifecycle_meta.value
            if isinstance(lifecycle_meta, ProcessLifecycleEnum)
            else str(lifecycle_meta)
        )
        role_meta = metadata.get("role", ProcessRoleEnum.CORE)
        role_value = role_meta.value if isinstance(role_meta, ProcessRoleEnum) else str(role_meta)
        tags_meta = metadata.get("tags", ())
        tags_list: list[str] = []
        if isinstance(tags_meta, (list, tuple, set)):
            tags_list = [str(tag) for tag in cast(Iterable[Any], tags_meta)]
        processes.append(
            AvailableProcess(
                name=name,
                class_path=metadata["class_path"],
                method=metadata["method"],
                description=metadata["description"],
                lifecycle=cast(ProcessLifecycleType, lifecycle_value),
                role=cast(ProcessRoleType, role_value),
                tags=tags_list,
                parameters_schema=metadata.get("parameters_schema"),
            )
        )
    return AvailableProcessesResponse(processes=processes, count=len(processes))


@router.get("/configured")
async def list_configured_processes(
    factory: ProcessLauncherService = Depends(get_process_factory),
    _user: UserProfile = Depends(require_permission(Permission.MANAGE_PROCESSES)),
) -> ConfiguredProcessesResponse:
    configs = await factory.get_process_configs()
    processes: list[ConfiguredProcess] = [
        ConfiguredProcess(
            name=config.name,
            enabled=config.enabled,
            mode=config.mode,
            class_path=config.class_path,
            method=config.method,
            args=config.args,
            kwargs=config.kwargs,
            note=config.note,
            lifecycle=config.lifecycle.value,
            role=config.role.value,
            tags=list(config.tags),
            parameters_schema=config.parameters_schema,
            running=config.name in factory.started_processes,
            is_one_shot=config.lifecycle is ProcessLifecycleEnum.ONE_SHOT,
            active_run_id=factory.active_runs.get(config.name),
        )
        for config in configs
    ]
    return ConfiguredProcessesResponse(processes=processes, count=len(processes))


@router.post("", status_code=201)
async def create_process_configuration(
    request: ProcessCreateRequest,
    factory: ProcessLauncherService = Depends(get_process_factory),
    settings: AppSettings = Depends(get_settings),
    _user: UserProfile = Depends(require_permission(Permission.MANAGE_PROCESSES)),
    _csrf: None = Depends(validate_csrf_token),
) -> ProcessCreateResponse:
    """Create a new process configuration from a template.

    Args:
        request: Process creation request with template name and config.
        factory: Process launcher service.
        settings: Application settings.
        _user: Authenticated user with MANAGE_PROCESSES permission.
        _csrf: CSRF token validation.

    Returns:
        Process creation response with status.

    Raises:
        HTTPException: If template not found or name already exists.
    """
    registry = get_registered_processes()
    metadata = registry.get(request.template)
    if metadata is None:
        raise HTTPException(status_code=404, detail=f"Template '{request.template}' not found")
    cls: type[RegisterableProcess] = metadata["class_ref"]
    try:
        base_kwargs = cls.get_default_kwargs(settings)
    except Exception:
        base_kwargs = {}
    if request.kwargs:
        base_kwargs.update(request.kwargs)
    final_args = request.args if request.args is not None else list(metadata.get("args", []))
    final_mode = request.mode or str(metadata.get("mode", "thread"))
    final_enabled = metadata.get("enabled", False) if request.enabled is None else request.enabled
    lifecycle_meta = metadata.get("lifecycle", ProcessLifecycleEnum.LONG_RUNNING)
    lifecycle = (
        lifecycle_meta
        if isinstance(lifecycle_meta, ProcessLifecycleEnum)
        else ProcessLifecycleEnum(str(lifecycle_meta))
    )
    role_meta = metadata.get("role", ProcessRoleEnum.CORE)
    role = role_meta if isinstance(role_meta, ProcessRoleEnum) else ProcessRoleEnum(str(role_meta))
    tags_meta = metadata.get("tags", ())
    tags: tuple[str, ...]
    if isinstance(tags_meta, (list, tuple, set)):
        tags = tuple(str(tag) for tag in cast(Iterable[Any], tags_meta))
    else:
        tags = ()
    try:
        await factory.create_process_config(
            name=request.name,
            class_path=metadata["class_path"],
            method=metadata.get("method", "start"),
            enabled=bool(final_enabled),
            mode=final_mode,
            args=final_args,
            kwargs=base_kwargs,
            lifecycle=lifecycle,
            role=role,
            tags=tags,
            parameters_schema=metadata.get("parameters_schema"),
            note=request.note,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return ProcessCreateResponse(
        status="created",
        process=ProcessCreatedInfo(
            name=request.name,
            template=request.template,
        ),
    )


@router.get("/schema/{name}")
async def get_process_schema(
    name: str,
    settings: AppSettings = Depends(get_settings),
    _user: UserProfile = Depends(require_permission(Permission.MANAGE_PROCESSES)),
) -> ProcessSchemaResponse:
    """Get the configuration schema for a registered process.

    Args:
        name: Process name from registry.
        settings: Application settings.
        _user: Authenticated user with MANAGE_PROCESSES permission.

    Returns:
        Schema response with defaults and configuration options.

    Raises:
        HTTPException: If process not found in registry.
    """
    registry = get_registered_processes()
    if name not in registry:
        raise HTTPException(status_code=404, detail=f"Process '{name}' not found in registry")
    metadata = registry[name]
    cls: type[RegisterableProcess] = metadata["class_ref"]
    try:
        default_kwargs = cls.get_default_kwargs(settings)
    except Exception:
        default_kwargs = {}
    lifecycle_meta = metadata.get("lifecycle", ProcessLifecycleEnum.LONG_RUNNING)
    lifecycle_value = (
        lifecycle_meta.value
        if isinstance(lifecycle_meta, ProcessLifecycleEnum)
        else str(lifecycle_meta)
    )
    return ProcessSchemaResponse(
        name=name,
        description=metadata["description"],
        class_path=metadata["class_path"],
        method=metadata["method"],
        default_enabled=metadata.get("enabled", False),
        default_mode=metadata.get("mode", "thread"),
        default_args=metadata.get("args", []),
        default_kwargs=default_kwargs,
        lifecycle=cast(ProcessLifecycleType, lifecycle_value),
    )


@router.post("/{name}/start")
async def start_process(
    name: str,
    request: ProcessStartRequest,
    factory: ProcessLauncherService = Depends(get_process_factory),
    _user: UserProfile = Depends(require_permission(Permission.MANAGE_PROCESSES)),
    _csrf: None = Depends(validate_csrf_token),
) -> ProcessStartResponse:
    result = await factory.start_process_by_name(
        name=name,
        mode=request.mode,
        args=request.args,
        kwargs=request.kwargs,
        autostart=request.autostart,
    )
    return ProcessStartResponse(
        status=result.get("status", "unknown"),
        name=name,
        run_id=result.get("run_id"),
        message=result.get("message"),
    )


@router.post("/{name}/stop")
async def stop_process(
    name: str,
    factory: ProcessLauncherService = Depends(get_process_factory),
    _user: UserProfile = Depends(require_permission(Permission.MANAGE_PROCESSES)),
    _csrf: None = Depends(validate_csrf_token),
) -> ProcessStopResponse:
    result = await factory.stop_process_by_name(name)
    return ProcessStopResponse(
        status=result.get("status", "unknown"),
        name=name,
        message=result.get("message"),
    )


@router.get("/runs")
async def list_process_runs(
    limit: int = 50,
    name: str | None = None,
    factory: ProcessLauncherService = Depends(get_process_factory),
    _user: UserProfile = Depends(require_permission(Permission.MANAGE_PROCESSES)),
) -> ProcessRunsResponse:
    runs_data = await factory.get_recent_runs(limit=limit, name=name)
    runs = [ProcessRun(**run) for run in runs_data]
    return ProcessRunsResponse(runs=runs, count=len(runs))
