"""Process registration and discovery module.

This module provides the decorator-based process registration system.
Processes decorated with @register_process are added to the global registry
and can be discovered and launched by the ProcessLauncherService.
"""

from collections.abc import Callable
from collections.abc import Iterable
from typing import Any
from typing import TypeVar

from snapper.application.process_manager.enums import ProcessLifecycleEnum
from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.models import RegisterableProcess
from snapper.utils.autoload import import_all_under

__all__ = [
    "register_process",
    "get_registered_processes",
    "get_process_metadata",
    "discover_processes",
]
_PROCESS_REGISTRY: dict[str, dict[str, Any]] = {}
T = TypeVar("T", bound=type[RegisterableProcess])


def register_process(
    name: str,
    method: str = "start",
    description: str = "",
    priority: int = 50,
    lifecycle: ProcessLifecycleEnum | str = ProcessLifecycleEnum.LONG_RUNNING,
    role: ProcessRoleEnum | str = ProcessRoleEnum.CORE,
    tags: Iterable[str] | None = None,
    parameters_schema: dict[str, Any] | None = None,
    enabled: bool = False,
    mode: str = "thread",
    args: list[Any] | None = None,
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
        parameters_schema: Optional JSON schema for constructor parameters.
        enabled: Default enabled state. Defaults to False.
        mode: Execution mode ("thread" or "process"). Defaults to "thread".
        args: Default positional arguments for constructor.

    Returns:
        Decorator function that registers and returns the class unchanged.

    Example:
        >>> @register_process(
        ...     "my_service",
        ...     description="My custom service",
        ...     priority=10,
        ...     lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        ...     enabled=True,
        ... )
        ... class MyService(RegisterableProcess):
        ...     async def start(self) -> None:
        ...         pass
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
        tags_value: tuple[str, ...] = tuple(str(tag) for tag in tags) if tags is not None else ()
        args_value: list[Any] = args if args is not None else []
        _PROCESS_REGISTRY[name] = {
            "class_ref": cls,
            "class_path": class_path,
            "method": method,
            "description": description,
            "priority": priority,
            "lifecycle": lifecycle_value,
            "role": role_value,
            "tags": tags_value,
            "parameters_schema": parameters_schema,
            "enabled": enabled,
            "mode": mode,
            "args": args_value,
        }
        return cls

    return decorator


def get_registered_processes() -> dict[str, dict[str, Any]]:
    """Get a copy of all registered processes.

    Returns:
        Dict mapping process name to registration metadata.
    """
    return _PROCESS_REGISTRY.copy()


def get_process_metadata(process_name: str) -> dict[str, Any] | None:
    """Get metadata for a specific registered process.

    Args:
        process_name: Name of the process to look up.

    Returns:
        Registration metadata dict, or None if not found.
    """
    return _PROCESS_REGISTRY.get(process_name)


def discover_processes() -> None:
    """Discover and register all processes in the snapper package.

    Imports all modules under snapper to trigger @register_process decorators.
    """
    import_all_under("snapper")
