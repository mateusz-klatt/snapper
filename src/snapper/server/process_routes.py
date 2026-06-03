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

import json
from datetime import UTC
from datetime import datetime
from typing import Annotated
from typing import Any
from typing import Literal
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from sqlalchemy import select

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
from snapper.application.process_manager.executor_naming import is_executor_instance
from snapper.application.process_manager.executor_naming import is_executor_template
from snapper.application.process_manager.executor_naming import parent_template_for_instance
from snapper.application.process_manager.launcher import ProcessLauncherService
from snapper.application.process_manager.models import ProcessConfigModel
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
from snapper.data.models import Setting
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import get_repository
from snapper.data.repository import where_active_now
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import ProcessSummaryItem
from snapper.server.json_body import json_body
from snapper.server.json_body import openapi_schema
from snapper.server.remote_summary_cache import RemoteSummaryCache

_REST_STREAM = "rest.control"
_PROCESS_BAD_REQUEST_RESPONSE: dict[int | str, dict[str, Any]] = {
    400: {"description": "Invalid process request"}
}
_PROCESS_FORBIDDEN_RESPONSE: dict[int | str, dict[str, Any]] = {
    403: {"description": "Process scope denied"}
}
_PROCESS_START_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"description": "Invalid process request"},
    403: {"description": "Process scope denied"},
    422: {"description": "Bare executor template — start a per-wallet instance instead"},
}


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


def get_remote_summary_cache(request: Request) -> RemoteSummaryCache | None:
    """FastAPI dependency to get the cross-coordinator summary cache.

    Returns ``None`` when the cache failed to start (or was never wired,
    as in some tests), in which case callers fall back to the local-only
    running view.

    Args:
        request: FastAPI request containing app state.

    Returns:
        The :class:`RemoteSummaryCache` if attached, else ``None``.
    """
    cache: RemoteSummaryCache | None = getattr(request.app.state, "remote_summary_cache", None)
    return cache


def _resolve_ownership(
    factory: ProcessLauncherService,
    cache: RemoteSummaryCache | None,
    config: ProcessConfigModel,
    *,
    local_running: bool,
) -> tuple[bool, str | None, bool]:
    """Union local and remote running-state for one process config.

    Processes the local autostart profile does not select (market-data
    publishers, when running in the ``API`` profile) are owned by a
    dedicated feed container; their running-state lives in the
    cross-coordinator summary cache, not this node's
    ``started_processes``.

    Args:
        factory: The local process launcher service.
        cache: Cross-coordinator summary cache, or ``None`` when the
            consumer failed to start (degrade to the local-only view).
        config: The process configuration under consideration.
        local_running: Whether this node's launcher tracks the process.

    A process that is actually running locally is always treated as
    locally owned, even when this node's autostart profile would not
    select it: that is the duplicate-publisher footgun state, and the UI
    MUST keep Start/Stop enabled so an operator can kill the rogue copy
    rather than see it frozen behind a "managed remotely" badge.

    Returns:
        Tuple ``(running, coordinator, managed_remotely)``:
        ``managed_remotely`` is True when this node neither runs nor owns
        the process; ``coordinator`` is the owning node slug (``None``
        when a remote owner has not yet been observed); ``running`` unions
        the local view with any fresh remote snapshot.
    """
    if local_running or factory.autostart_includes(config):
        return local_running, factory.coordinator_topic_slug(), False
    if cache is None:
        return False, None, True
    remote_running, remote_coordinator = cache.lookup(config.name)
    return remote_running, remote_coordinator, True


def get_repository_for_processes() -> Repository:
    """FastAPI dependency for repository access in strategy permission checks.

    Defined locally to avoid an import cycle with ``snapper.server.app``
    (which imports this module to mount the process router). Returns the
    same repository instance as ``get_repository_dependency`` in
    ``snapper.server.app`` because both call ``get_repository`` against
    the bootstrap settings db_url.
    """
    settings = get_settings()
    return get_repository(settings.db_url)


async def _read_persisted_strategy_parameters(
    repo: Repository, name: str
) -> dict[str, object] | None:
    """Read the persisted process configuration for ``name``.

    Returns a dict with ``template`` and ``parameters`` keys when the
    persisted ``process_<name>`` settings row exists, otherwise None.
    Mirrors the launcher's ``start_process_by_name`` DB read so the
    start endpoint can re-validate the same effective parameters the
    launcher will hand to the process constructor.
    """
    if not isinstance(repo, SQLAlchemyRepository):
        return None

    config_key = f"process_{name}"
    async with repo.session() as session:
        result = await session.execute(
            select(Setting).where(Setting.key == config_key, *where_active_now(Setting))
        )
        setting = result.scalar_one_or_none()
        if setting is None:
            return None
        try:
            config_dict = json.loads(setting.value)
        except json.JSONDecodeError:
            return None
    if not isinstance(config_dict, dict):
        return None
    parameters = config_dict.get("parameters") or {}
    class_path = config_dict.get("class") or config_dict.get("class_path") or ""
    return {"class_path": class_path, "parameters": parameters}


def _resolve_role_for_class_path(class_path: str) -> ProcessRoleEnum | None:
    """Look up a registered process entry by its class_path.

    The launcher persists ``class_path`` in the process settings row
    but not the template name, so the start endpoint maps class_path
    back to a registry entry to read the role for the scope check.
    Returns None when no matching entry exists (e.g. the template was
    deregistered between create and start).
    """
    registry = get_registered_processes()
    for entry in registry.values():
        if entry.class_path == class_path:
            return entry.role
    return None


async def _enforce_wallet_grant_exists(
    repo: SQLAlchemyRepository,
    operator_public_id: str,
    wallet_public_id: str,
    as_of: datetime,
) -> None:
    """Verify the operator holds at least one active grant on the wallet.

    Raises 403 when the operator has zero matching grants. Coarse check
    that runs before the per-output instrument coverage check.
    """
    grants = await repo.list_active_scope_grants_for_wallet(
        wallet_public_id=wallet_public_id,
        as_of=as_of,
    )
    matching = [g for g in grants if g["operator_public_id"] == operator_public_id]
    if matching:
        return
    raise HTTPException(
        status_code=403,
        detail=(
            f"Operator '{operator_public_id}' has no active scope grant on "
            f"wallet '{wallet_public_id}'"
        ),
    )


async def _enforce_strategy_outputs_covered(
    repo: SQLAlchemyRepository,
    parameters: dict[str, object],
    operator_public_id: str,
    wallet_public_id: str,
    as_of: datetime,
) -> None:
    """Verify every output instrument is covered by an active grant.

    Maps each ``outputs`` symbol on the strategy's exchange to its
    instrument_public_id and checks membership against the union of
    instruments covered by all active grants for the operator on the
    wallet. Skipped for the paper exchange (no Instrument rows) and
    when ``outputs`` / ``exchange`` are missing or non-string. Raises
    403 listing every uncovered symbol when the check fails.
    """
    raw_outputs = parameters.get("outputs", [])
    raw_exchange = parameters.get("exchange", "")
    if not isinstance(raw_outputs, list) or not isinstance(raw_exchange, str):
        return
    outputs = [o for o in raw_outputs if isinstance(o, str)]
    if not outputs or not raw_exchange or raw_exchange == "paper":
        return
    covered = await repo.list_grant_covered_instrument_public_ids(
        operator_public_id=operator_public_id,
        wallet_public_id=wallet_public_id,
        as_of=as_of,
    )
    uncovered = await _find_uncovered_outputs(repo, outputs, raw_exchange, covered, as_of)
    if not uncovered:
        return
    raise HTTPException(
        status_code=403,
        detail=(
            f"Operator '{operator_public_id}' has no active grant covering "
            f"instruments {sorted(uncovered)} on wallet '{wallet_public_id}'"
        ),
    )


async def _find_uncovered_outputs(
    repo: SQLAlchemyRepository,
    outputs: list[str],
    exchange: str,
    covered: set[str],
    as_of: datetime,
) -> list[str]:
    """Return the subset of output symbols whose instrument is not covered."""
    instrument_public_ids = await repo.get_instrument_public_ids_by_symbols(
        native_symbols=set(outputs),
        exchange=exchange,
        as_of=as_of,
    )
    return [symbol for symbol in outputs if instrument_public_ids.get(symbol) not in covered]


async def _enforce_strategy_scope(
    parameters: dict[str, object],
    role: ProcessRoleEnum,
    principal: AuthPrincipal,
    repo: Repository,
) -> None:
    """Validate operator/wallet scope for a strategy process launch.

    Transitional rules:

    - If ``role`` is not ``STRATEGY`` the check is skipped — non-strategy
      process templates (feeds, executors, services) do not yet carry
      operator/wallet scope.
    - ``operator_public_id`` and ``wallet_public_id`` are *both* optional
      during the transition. If neither is set, the launch is
      allowed for backwards compatibility (matches the empty-string
      defaults on ``StrategyProcessParameters``).
    - If ``operator_public_id`` is set, it must be in
      ``principal.operator_public_ids``. ADMIN principals see every
      active operator (resolved at login) so this naturally allows
      admins on any operator.
    - If both ``operator_public_id`` AND ``wallet_public_id`` are set,
      an active scope grant for that pair must exist in
      ``wallet_operator_scope_grants``.

    NOT NULL tightening will eventually make both fields
    required at the schema layer, at which point this helper will reject
    the empty-defaults path.

    Raises:
        HTTPException: 403 on operator mismatch or missing grant; 400
            when a wallet is supplied without an operator.
    """
    if role is not ProcessRoleEnum.STRATEGY:
        return
    raw_operator = parameters.get("operator_public_id", "")
    raw_wallet = parameters.get("wallet_public_id", "")
    operator_public_id = raw_operator if isinstance(raw_operator, str) else ""
    wallet_public_id = raw_wallet if isinstance(raw_wallet, str) else ""
    if not operator_public_id and not wallet_public_id:
        return
    if not operator_public_id:
        raise HTTPException(
            status_code=400,
            detail="wallet_public_id supplied without operator_public_id",
        )
    if operator_public_id not in principal.operator_public_ids:
        raise HTTPException(
            status_code=403,
            detail=(
                f"User '{principal.username}' has no membership on operator "
                f"'{operator_public_id}'"
            ),
        )
    if not wallet_public_id:
        return
    if not isinstance(repo, SQLAlchemyRepository):
        return
    as_of = datetime.now(UTC)
    await _enforce_wallet_grant_exists(repo, operator_public_id, wallet_public_id, as_of)
    await _enforce_strategy_outputs_covered(
        repo, parameters, operator_public_id, wallet_public_id, as_of
    )


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
    cache: Annotated[RemoteSummaryCache | None, Depends(get_remote_summary_cache)],
    _user: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_PROCESSES))],
) -> ConfiguredProcessesResponse:
    """List configured processes — DB templates plus runtime per-wallet instances.

    Two row shapes share the response:

    - **Template rows** come from the ``Setting`` table and represent
      executor templates (``executor_<exchange>``) plus regular
      processes (broker, feeds, strategies). Executor templates have
      ``kind="template"`` with ``running=False`` (they are config-only
      and never run directly). All other DB rows have ``kind="instance"``.
    - **Synthetic per-wallet rows** are pulled from
      ``factory.instance_configs`` and represent live per-wallet
      executor instances (``executor_<exchange>_w<wallet_short>``).
      They carry ``kind="instance"`` plus the ``wallet_public_id`` and
      ``parent_template`` discriminators so the UI can render them
      grouped under their template.

    Each row carries ``coordinator`` + ``managed_remotely`` so the UI can
    render feed-container-owned publishers as running (via the
    cross-coordinator summary cache) and disable Start/Stop on them
    instead of spawning a duplicate publisher in the API container.
    """
    sid, seq, pid, ts = _mint_provenance(request)
    configs = await factory.get_process_configs()
    processes: list[ConfiguredProcess] = []
    for config in configs:
        is_template_row = is_executor_template(config.name)
        kind: Literal["template", "instance"] = "template" if is_template_row else "instance"
        local_running = False if is_template_row else config.name in factory.started_processes
        running, coordinator, managed_remotely = _resolve_ownership(
            factory, cache, config, local_running=local_running
        )
        active_public_id = None if is_template_row else factory.active_runs.get(config.name)
        processes.append(
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
                running=running,
                is_one_shot=config.lifecycle is ProcessLifecycleEnum.ONE_SHOT,
                active_public_id=active_public_id,
                kind=kind,
                wallet_public_id=None,
                parent_template=None,
                coordinator=coordinator,
                managed_remotely=managed_remotely,
            )
        )
    for instance_name, instance_config in factory.instance_configs.items():
        if not is_executor_instance(instance_name):
            continue
        wallet_param = instance_config.parameters.get("wallet_public_id")
        wallet_id = wallet_param if isinstance(wallet_param, str) else None
        local_running = instance_name in factory.started_processes
        running, coordinator, managed_remotely = _resolve_ownership(
            factory, cache, instance_config, local_running=local_running
        )
        processes.append(
            ConfiguredProcess(
                session_id=sid,
                sequence_id=seq,
                public_id=str(uuid7()),
                timestamp=ts,
                name=instance_name,
                enabled=instance_config.enabled,
                mode=instance_config.mode,
                class_path=instance_config.class_path,
                method=instance_config.method,
                parameters=instance_config.parameters,
                note=instance_config.note,
                lifecycle=instance_config.lifecycle,
                role=instance_config.role,
                tags=list(instance_config.tags),
                parameters_schema=instance_config.parameters_schema,
                running=running,
                is_one_shot=instance_config.lifecycle is ProcessLifecycleEnum.ONE_SHOT,
                active_public_id=factory.active_runs.get(instance_name),
                kind="instance",
                wallet_public_id=wallet_id,
                parent_template=parent_template_for_instance(instance_name),
                coordinator=coordinator,
                managed_remotely=managed_remotely,
            )
        )
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
    cache: Annotated[RemoteSummaryCache | None, Depends(get_remote_summary_cache)],
    _user: Annotated[AuthPrincipal, Depends(require_permission(Permission.READ_SYSTEM_STATUS))],
) -> ProcessSummaryResponse:
    """Lightweight process summary returning category counts.

    Returns running/total counts per category (feeds, strategies,
    executors, brokers) for the overview dashboard. Requires only
    READ_SYSTEM_STATUS permission so viewers can see process health.

    Feed publishers run in a dedicated container, so their running-state
    is unioned from the cross-coordinator summary cache; without it the
    API's local view would always report ``feeds_running = 0``.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        factory: Process launcher service.
        cache: Cross-coordinator summary cache (``None`` degrades to local).
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
        is_running, _coordinator, _managed_remotely = _resolve_ownership(
            factory, cache, config, local_running=config.name in running
        )
        if "feed_publisher" in config.name:
            feeds_total += 1
            feeds_running += int(is_running)
        elif config.role is ProcessRoleEnum.STRATEGY:
            strategies_total += 1
            strategies_running += int(is_running)
        elif is_executor_template(config.name):
            continue
        elif config.name == "zmq_broker":
            brokers_total += 1
            brokers_running += int(is_running)
    for instance_name in factory.instance_configs:
        if not is_executor_instance(instance_name):
            continue
        executors_total += 1
        executors_running += int(instance_name in running)

    local_items = await factory.build_process_summary_items()
    config_by_name: dict[str, ProcessConfigModel] = {config.name: config for config in configs}
    for instance_name, instance_config in factory.instance_configs.items():
        config_by_name.setdefault(instance_name, instance_config)
    items: list[ProcessSummaryItem] = []
    for item in local_items:
        row_config = config_by_name.get(item.name)
        if row_config is None:
            items.append(item)
            continue
        unioned_running, _coordinator, _managed_remotely = _resolve_ownership(
            factory, cache, row_config, local_running=item.running
        )
        items.append(
            item
            if unioned_running == item.running
            else item.model_copy(update={"running": unioned_running})
        )
    sid, seq, pid, ts = _mint_provenance(request)
    data = ProcessSummaryData(
        session_id=sid,
        sequence_id=seq,
        public_id=str(uuid7()),
        timestamp=ts,
        coordinator=factory.coordinator_topic_slug(),
        processes=items,
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
        400: {"description": "Invalid process request"},
        403: {"description": "Process scope denied"},
        404: {"description": "Template not found"},
        409: {"description": "Process name already exists"},
    },
    openapi_extra=openapi_schema(ProcessCreateRequest),
)
async def create_process_configuration(
    http_request: Request,
    factory: Annotated[ProcessLauncherService, Depends(get_process_factory)],
    settings: Annotated[AppSettings, Depends(get_settings)],
    user: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_PROCESSES))],
    repo: Annotated[Repository, Depends(get_repository_for_processes)],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    body: Annotated[ProcessCreateRequest, Depends(json_body(ProcessCreateRequest))],
) -> ProcessCreateResponse:
    """Create a new process configuration from a template.

    Args:
        http_request: FastAPI request (provides REST tracker for provenance).
        body: Process creation request with template name and config.
        factory: Process launcher service.
        settings: Application settings.
        user: Authenticated user with MANAGE_PROCESSES permission, used
            for the strategy scope check on operator/wallet.
        repo: Repository used to verify active scope grants for the
            requested operator/wallet pair.
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
    await _enforce_strategy_scope(base_parameters, entry.role, user, repo)
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


@router.post(
    "/{name}/start",
    openapi_extra=openapi_schema(ProcessStartRequest),
    responses=_PROCESS_START_RESPONSES,
)
async def start_process(
    http_request: Request,
    name: str,
    factory: Annotated[ProcessLauncherService, Depends(get_process_factory)],
    user: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_PROCESSES))],
    repo: Annotated[Repository, Depends(get_repository_for_processes)],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    body: Annotated[ProcessStartRequest, Depends(json_body(ProcessStartRequest))],
) -> ProcessStartResponse:
    """Start a previously created process configuration.

    ``payload.parameters`` cannot
    override ``operator_public_id`` or ``wallet_public_id`` at start
    time — those fields are pinned to whatever ``_enforce_strategy_scope``
    validated at create time. If the caller wants to switch wallets or
    operators they must update the persisted process configuration
    through the create / configure path so the scope check runs again.

    For strategy templates, this handler ALSO re-runs
    ``_enforce_strategy_scope`` against the persisted parameters before
    starting the process, so a strategy whose grant has been revoked
    between create-time and start-time fails closed instead of running
    on a wallet the caller no longer controls.
    """
    if is_executor_template(name):
        raise HTTPException(
            status_code=422,
            detail=(
                f"'{name}' is an executor template — start "
                f"'{name}_w<wallet_short>' for a specific wallet"
            ),
        )
    payload = body.payload
    overrides = payload.parameters or {}
    persisted = await _read_persisted_strategy_parameters(repo, name)
    persisted_role: ProcessRoleEnum | None = None
    persisted_params: dict[str, object] | None = None
    if persisted is not None:
        class_path = persisted.get("class_path")
        if isinstance(class_path, str) and class_path:
            persisted_role = _resolve_role_for_class_path(class_path)
            raw_params = persisted.get("parameters")
            if isinstance(raw_params, dict):
                persisted_params = raw_params
    if persisted_role is ProcessRoleEnum.STRATEGY and overrides:
        raise HTTPException(
            status_code=400,
            detail=(
                "Strategy processes do not accept start-time parameter overrides; "
                "update the persisted process configuration via the create endpoint "
                "instead so the operator/wallet/output scope check runs against the "
                "exact parameters that will launch."
            ),
        )
    forbidden = {"operator_public_id", "wallet_public_id"}.intersection(overrides.keys())
    if forbidden:
        raise HTTPException(
            status_code=400,
            detail=(
                "Cannot override "
                f"{sorted(forbidden)} at start time; update the persisted "
                "process configuration via the create endpoint instead."
            ),
        )
    if persisted_role is ProcessRoleEnum.STRATEGY and persisted_params is not None:
        await _enforce_strategy_scope(persisted_params, persisted_role, user, repo)
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
