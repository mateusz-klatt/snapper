"""Process registration and discovery module.

This module provides the decorator-based process registration system.
Processes decorated with @register_process are added to the global registry
and can be discovered and launched by the ProcessLauncherService.
"""

from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
from typing import Any

from loguru import logger

from snapper.application.process_manager.models import ProcessRegistryEntry
from snapper.application.process_manager.models import RegisterableProcess
from snapper.config.settings import get_bootstrap_settings
from snapper.core.json_types import JsonObject
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessMode
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRestartPolicyEnum
from snapper.core.types import ProcessRoleEnum
from snapper.utils.autoload import import_all_under

__all__ = [
    "register_process",
    "get_registered_processes",
    "discover_processes",
]
_PROCESS_REGISTRY: dict[str, ProcessRegistryEntry] = {}


def _derive_parameters_schema(parameters_model: type[Any] | None) -> JsonObject | None:
    """Derive JSON Schema from a Pydantic parameters model.

    Args:
        parameters_model: Pydantic model type, or None.

    Returns:
        JSON Schema dict, or None if no model provided.
    """
    if parameters_model is None:
        return None
    schema: JsonObject = parameters_model.model_json_schema()
    return schema


def register_process[T: type[RegisterableProcess]](
    name: str,
    method: str = "start",
    description: str = "",
    priority: int = 50,
    lifecycle: ProcessLifecycleEnum | str = ProcessLifecycleEnum.LONG_RUNNING,
    role: ProcessRoleEnum | str = ProcessRoleEnum.CORE,
    tags: Iterable[str] | None = None,
    parameters_model: type[Any] | None = None,
    enabled: bool = False,
    mode: ProcessMode = ProcessModeEnum.THREAD,
    restart_policy: ProcessRestartPolicyEnum | str = ProcessRestartPolicyEnum.ON_FAILURE,
    reference_identity_params: Mapping[str, str] | None = None,
    seeded_identity_params: tuple[str, ...] = (),
) -> Callable[[T], T]:
    """Decorator to register a process class in the global registry.

    Registers the decorated class with metadata for discovery and launching.
    Lower priority values start first.

    Args:
        name: Unique process identifier used in settings.
        method: Method name to call on the instance. Defaults to "start".
        description: Human-readable description of the process.
        priority: Startup priority (lower = starts earlier). Defaults to 50.
        lifecycle: LONG_RUNNING or ONE_SHOT.
        role: Process role (CORE, TASK, STRATEGY, BACKTEST).
        tags: Iterable of string tags for filtering.
        parameters_model: Optional Pydantic model for parameter validation.
            When provided, parameters_schema is derived automatically.
        enabled: Default enabled state. Defaults to False.
        mode: Execution mode ("thread" or "process"). Defaults to "thread".
        restart_policy: Watchdog auto-restart policy. Defaults to on_failure.
        reference_identity_params: Scoped-strategy nested identity references
            (name -> kind) forwarded from the strategy class, empty otherwise.
        seeded_identity_params: Scoped-strategy nested seeded-identity keys
            forwarded from the strategy class, empty otherwise.

    Returns:
        Decorator function that registers and returns the class unchanged.

    Example:
        >>> @register_process(
        "my_service"
        description="My custom service"
        priority=10
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING
        enabled=True
        class MyService(RegisterableProcess)
        async def start(self) -> None
        pass
    """

    def decorator(cls: T) -> T:
        """Inner decorator that performs the registration."""
        module = cls.__module__
        class_name = cls.__qualname__
        class_path = f"{module}.{class_name}"
        lifecycle_value = (
            lifecycle
            if isinstance(lifecycle, ProcessLifecycleEnum)
            else ProcessLifecycleEnum(str(lifecycle))
        )
        role_value = role if isinstance(role, ProcessRoleEnum) else ProcessRoleEnum(str(role))
        restart_policy_value = (
            restart_policy
            if isinstance(restart_policy, ProcessRestartPolicyEnum)
            else ProcessRestartPolicyEnum(str(restart_policy))
        )
        tags_value: tuple[str, ...] = tuple(str(tag) for tag in tags) if tags is not None else ()
        _PROCESS_REGISTRY[name] = ProcessRegistryEntry(
            class_ref=cls,
            class_path=class_path,
            method=method,
            description=description,
            priority=priority,
            lifecycle=lifecycle_value,
            role=role_value,
            tags=tags_value,
            parameters_model=parameters_model,
            parameters_schema=_derive_parameters_schema(parameters_model),
            enabled=enabled,
            mode=mode,
            restart_policy=restart_policy_value,
            reference_identity_params=dict(reference_identity_params or {}),
            seeded_identity_params=tuple(seeded_identity_params),
        )
        return cls

    return decorator


def get_registered_processes() -> dict[str, ProcessRegistryEntry]:
    """Get a copy of all registered processes.

    Returns:
        Dict mapping process name to ProcessRegistryEntry.
    """
    return _PROCESS_REGISTRY.copy()


def discover_processes() -> None:
    """Discover and register all processes in the snapper package.

    Imports all modules under snapper to trigger @register_process
    decorators, then every extra top-level package listed in the
    ``STRATEGY_EXTRA_PACKAGES`` bootstrap setting (comma-separated) —
    fail-soft per package, so a missing or broken out-of-tree strategy
    mount (e.g. proprietary code on PYTHONPATH) logs a warning instead
    of aborting discovery for OSS deployments.
    """
    import_all_under("snapper")
    extra = get_bootstrap_settings().strategy_extra_packages
    for package in (part.strip() for part in extra.split(",")):
        if not package:
            continue
        try:
            imported = import_all_under(package)
            logger.info(f"Discovered {imported} extra strategy modules under '{package}'")
        except Exception as exc:
            logger.warning(f"Extra strategy package '{package}' not imported: {exc}")
