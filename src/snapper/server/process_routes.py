"""REST API routes for process lifecycle management.

This module provides FastAPI routes for managing background processes
in the Snapper trading platform. Processes include market data feeds,
trading strategies, executors, and other long-running services.

Endpoints:
    - ``GET /processes/available`` - List registered process templates.
    - ``GET /processes/configured`` - List configured process instances.
    - ``GET /processes/summary`` - Lightweight process category counts.
    - ``POST /processes`` - Create new process configuration.
    - ``GET /processes/schema/{name}`` - Get process parameter schema.
    - ``POST /processes/{name}/start`` - Start a configured process.
    - ``POST /processes/{name}/stop`` - Stop a running process.
    - ``GET /processes/runs`` - List historical process runs.

Process Types:
    - **Long-running**: Continuous services (feeds, executors)
    - **One-shot**: Tasks that complete (backfill, sync)

Most endpoints require MANAGE_PROCESSES permission (operator/admin role).
The summary endpoint requires only READ_SYSTEM_STATUS (viewer+).

Example:
    Start a process::

        POST /api/processes/my-strategy/start
        {"mode": "process"}
"""

from datetime import UTC
from datetime import datetime
from typing import Annotated
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request

from snapper.api.schemas.process import AvailableProcess
from snapper.api.schemas.process import AvailableProcessesResponse
from snapper.api.schemas.process import ConfiguredProcess
from snapper.api.schemas.process import ConfiguredProcessesResponse
from snapper.api.schemas.process import ProcessCategoryCount
from snapper.api.schemas.process import ProcessCreateData
from snapper.api.schemas.process import ProcessCreatedInfo
from snapper.api.schemas.process import ProcessCreateRequest
from snapper.api.schemas.process import ProcessCreateResponse
from snapper.api.schemas.process import ProcessRun
from snapper.api.schemas.process import ProcessRunsResponse
from snapper.api.schemas.process import ProcessSchemaData
from snapper.api.schemas.process import ProcessSchemaResponse
from snapper.api.schemas.process import ProcessStartData
from snapper.api.schemas.process import ProcessStartRequest
from snapper.api.schemas.process import ProcessStartResponse
from snapper.api.schemas.process import ProcessStopData
from snapper.api.schemas.process import ProcessStopResponse
from snapper.api.schemas.process import ProcessSummaryData
from snapper.api.schemas.process import ProcessSummaryResponse
from snapper.application.process_manager.config_resolver import resolve_mode
from snapper.application.process_manager.launcher import ProcessLauncherService
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.registry import get_registered_processes
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessRoleEnum
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.json_body import json_body
from snapper.server.json_body import openapi_schema

_REST_STREAM = "rest.control"


def _mint_provenance(request: Request) -> tuple[str, int, str, datetime]:
    """Extract one sid/seq/pid/ts quad from the REST tracker.

    Called once per handler. All nested minted DTOs in the response
    tree share the same provenance quad.

    Args:
        request: FastAPI request with app.state.rest_tracker.

    Returns:
        Tuple of (session_id, sequence_id, public_id, timestamp).
    """
    tracker: SequenceTracker = request.app.state.rest_tracker
    return tracker.session_id, tracker.next_sequence(_REST_STREAM), str(uuid7()), datetime.now(UTC)


__all__ = [
    "router",
    "get_process_factory",
    "get_process_schema",
    "get_process_summary",
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
    request: Request,
    _user: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_PROCESSES))],
) -> AvailableProcessesResponse:
    sid, seq, pid, ts = _mint_provenance(request)
    registry = get_registered_processes()
    processes: list[AvailableProcess] = []
    for name, entry in registry.items():
        processes.append(
            AvailableProcess(
                session_id=sid,
                sequence_id=seq,
                public_id=str(uuid7()),
                timestamp=ts,
                name=name,
                class_path=entry.class_path,
                method=entry.method,
                description=entry.description,
                lifecycle=entry.lifecycle,
                role=entry.role,
                tags=list(entry.tags),
                parameters_schema=entry.parameters_schema,
            )
        )
    return AvailableProcessesResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=processes,
        count=len(processes),
    )


@router.get("/configured")
async def list_configured_processes(
    request: Request,
    factory: Annotated[ProcessLauncherService, Depends(get_process_factory)],
    _user: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_PROCESSES))],
) -> ConfiguredProcessesResponse:
    sid, seq, pid, ts = _mint_provenance(request)
    configs = await factory.get_process_configs()
    processes: list[ConfiguredProcess] = [
        ConfiguredProcess(
            session_id=sid,
            sequence_id=seq,
            public_id=str(uuid7()),
            timestamp=ts,
            name=config.name,
            enabled=config.enabled,
            mode=config.mode,
            class_path=config.class_path,
            method=config.method,
            parameters=config.parameters,
            note=config.note,
            lifecycle=config.lifecycle,
            role=config.role,
            tags=list(config.tags),
            parameters_schema=config.parameters_schema,
            running=config.name in factory.started_processes,
            is_one_shot=config.lifecycle is ProcessLifecycleEnum.ONE_SHOT,
            active_public_id=factory.active_runs.get(config.name),
        )
        for config in configs
    ]
    return ConfiguredProcessesResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=processes,
        count=len(processes),
    )


@router.get("/summary")
async def get_process_summary(
    request: Request,
    factory: Annotated[ProcessLauncherService, Depends(get_process_factory)],
    _user: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_SYSTEM_STATUS))],
) -> ProcessSummaryResponse:
    """Lightweight process summary returning category counts.

    Returns running/total counts per category (feeds, strategies,
    executors, brokers) for the overview dashboard. Requires only
    READ_SYSTEM_STATUS permission so viewers can see process health.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        factory: Process launcher service.
        _user: Authenticated user with READ_SYSTEM_STATUS permission.

    Returns:
        Process summary with counts per category.
    """
    configs = await factory.get_process_configs()
    running = factory.started_processes

    feeds_total = 0
    feeds_running = 0
    strategies_total = 0
    strategies_running = 0
    executors_total = 0
    executors_running = 0
    brokers_total = 0
    brokers_running = 0

    for config in configs:
        is_running = config.name in running
        if "feed_publisher" in config.name:
            feeds_total += 1
            feeds_running += int(is_running)
        elif config.role is ProcessRoleEnum.STRATEGY:
            strategies_total += 1
            strategies_running += int(is_running)
        elif config.name.startswith("executor_"):
            executors_total += 1
            executors_running += int(is_running)
        elif config.name == "zmq_broker":
            brokers_total += 1
            brokers_running += int(is_running)

    sid, seq, pid, ts = _mint_provenance(request)
    data = ProcessSummaryData(
        session_id=sid,
        sequence_id=seq,
        public_id=str(uuid7()),
        timestamp=ts,
        feeds=ProcessCategoryCount(
            running=feeds_running,
            total=feeds_total,
        ),
        strategies=ProcessCategoryCount(
            running=strategies_running,
            total=strategies_total,
        ),
        executors=ProcessCategoryCount(
            running=executors_running,
            total=executors_total,
        ),
        brokers=ProcessCategoryCount(
            running=brokers_running,
            total=brokers_total,
        ),
    )
    return ProcessSummaryResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=data,
    )


@router.post(
    "",
    status_code=201,
    responses={
        404: {"description": "Template not found"},
        409: {"description": "Process name already exists"},
    },
    openapi_extra=openapi_schema(ProcessCreateRequest),
)
async def create_process_configuration(
    http_request: Request,
    factory: Annotated[ProcessLauncherService, Depends(get_process_factory)],
    settings: Annotated[AppSettings, Depends(get_settings)],
    _user: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_PROCESSES))],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    body: Annotated[ProcessCreateRequest, Depends(json_body(ProcessCreateRequest))],
) -> ProcessCreateResponse:
    """Create a new process configuration from a template.

    Args:
        http_request: FastAPI request (provides REST tracker for provenance).
        body: Process creation request with template name and config.
        factory: Process launcher service.
        settings: Application settings.
        _user: Authenticated user with MANAGE_PROCESSES permission.
        _csrf: CSRF token validation.

    Returns:
        Process creation response with status.

    Raises:
        HTTPException: If template not found or name already exists.
    """
    payload = body.payload
    registry = get_registered_processes()
    entry = registry.get(payload.template)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Template '{payload.template}' not found")
    cls: type[RegisterableProcess] = entry.class_ref
    try:
        base_parameters = cls.get_default_parameters(settings)
    except Exception:
        base_parameters = {}
    if payload.parameters:
        base_parameters.update(payload.parameters)
    final_mode = payload.mode or resolve_mode(entry.mode, payload.name)
    final_enabled = entry.enabled if payload.enabled is None else payload.enabled
    try:
        await factory.create_process_config(
            name=payload.name,
            class_path=entry.class_path,
            method=entry.method,
            enabled=bool(final_enabled),
            mode=final_mode,
            parameters=base_parameters,
            lifecycle=entry.lifecycle,
            role=entry.role,
            tags=entry.tags,
            parameters_schema=entry.parameters_schema,
            note=payload.note,
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    sid, seq, pid, ts = _mint_provenance(http_request)
    data = ProcessCreateData(
        session_id=sid,
        sequence_id=seq,
        public_id=str(uuid7()),
        timestamp=ts,
        status="created",
        process=ProcessCreatedInfo(
            name=payload.name,
            template=payload.template,
        ),
    )
    return ProcessCreateResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=data,
    )


@router.get(
    "/schema/{name}",
    responses={404: {"description": "Process not found in registry"}},
)
async def get_process_schema(
    request: Request,
    name: str,
    settings: Annotated[AppSettings, Depends(get_settings)],
    _user: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_PROCESSES))],
) -> ProcessSchemaResponse:
    """Get the configuration schema for a registered process.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
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
    entry = registry[name]
    cls: type[RegisterableProcess] = entry.class_ref
    try:
        default_parameters = cls.get_default_parameters(settings)
    except Exception:
        default_parameters = {}
    sid, seq, pid, ts = _mint_provenance(request)
    data = ProcessSchemaData(
        session_id=sid,
        sequence_id=seq,
        public_id=str(uuid7()),
        timestamp=ts,
        name=name,
        description=entry.description,
        class_path=entry.class_path,
        method=entry.method,
        default_enabled=entry.enabled,
        default_mode=resolve_mode(entry.mode, name),
        default_parameters=default_parameters,
        lifecycle=entry.lifecycle,
    )
    return ProcessSchemaResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=data,
    )


@router.post("/{name}/start", openapi_extra=openapi_schema(ProcessStartRequest))
async def start_process(
    http_request: Request,
    name: str,
    factory: Annotated[ProcessLauncherService, Depends(get_process_factory)],
    _user: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_PROCESSES))],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    body: Annotated[ProcessStartRequest, Depends(json_body(ProcessStartRequest))],
) -> ProcessStartResponse:
    payload = body.payload
    result = await factory.start_process_by_name(
        name=name,
        mode=payload.mode,
        parameters=payload.parameters,
    )
    sid, seq, pid, ts = _mint_provenance(http_request)
    data = ProcessStartData(
        session_id=sid,
        sequence_id=seq,
        public_id=str(uuid7()),
        timestamp=ts,
        status=result.status,
        name=name,
        process_public_id=result.public_id,
        message=result.message,
    )
    return ProcessStartResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=data,
    )


@router.post("/{name}/stop")
async def stop_process(
    request: Request,
    name: str,
    factory: Annotated[ProcessLauncherService, Depends(get_process_factory)],
    _user: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_PROCESSES))],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
) -> ProcessStopResponse:
    result = await factory.stop_process_by_name(name)
    sid, seq, pid, ts = _mint_provenance(request)
    data = ProcessStopData(
        session_id=sid,
        sequence_id=seq,
        public_id=str(uuid7()),
        timestamp=ts,
        status=result.status,
        name=name,
        message=result.message,
    )
    return ProcessStopResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=data,
    )


@router.get("/runs")
async def list_process_runs(
    request: Request,
    factory: Annotated[ProcessLauncherService, Depends(get_process_factory)],
    _user: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_PROCESSES))],
    limit: int = 50,
    name: str | None = None,
) -> ProcessRunsResponse:
    runs_data = await factory.get_recent_runs(limit=limit, name=name)
    runs = [ProcessRun(**run) for run in runs_data]
    sid, seq, pid, ts = _mint_provenance(request)
    return ProcessRunsResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=runs,
        count=len(runs),
    )
