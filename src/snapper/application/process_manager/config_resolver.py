"""Process configuration resolution and class import utilities.

Provides stateless functions for resolving process configuration
values (lifecycle, role, tags, parameters_schema) and building
ProcessConfigModel instances from parsed database settings.
"""

import importlib
import json
from collections.abc import Iterable
from typing import Any
from typing import cast

from loguru import logger
from sqlalchemy import select

from snapper.application.process_manager.models import ProcessConfigModel
from snapper.application.process_manager.models import ProcessRegistryEntry
from snapper.application.process_manager.registry import get_registered_processes
from snapper.config.settings import AppSettings
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessMode
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRestartPolicyEnum
from snapper.core.types import ProcessRoleEnum
from snapper.data.models import Setting
from snapper.data.repository import get_repository
from snapper.data.repository import where_active_now

VALID_PROCESS_MODES: frozenset[str] = frozenset(m.value for m in ProcessModeEnum)
"""Valid process mode values matching the ProcessMode Literal."""


def resolve_lifecycle(
    raw: Any,
    process_name: str,
) -> ProcessLifecycleEnum:
    """Resolve lifecycle value from config or metadata.

    Args:
        raw: Raw lifecycle value (enum, string, or None).
        process_name: Name used in warning messages.

    Returns:
        Resolved ProcessLifecycleEnum value.
    """
    if raw is None:
        return ProcessLifecycleEnum.LONG_RUNNING
    if isinstance(raw, ProcessLifecycleEnum):
        return raw
    try:
        return ProcessLifecycleEnum(str(raw))
    except ValueError:
        logger.warning(
            "Unknown lifecycle '{}' for process '{}', defaulting to long-running",
            raw,
            process_name,
        )
        return ProcessLifecycleEnum.LONG_RUNNING


def resolve_role(
    raw: Any,
    process_name: str,
) -> ProcessRoleEnum:
    """Resolve role value from config or metadata.

    Args:
        raw: Raw role value (enum, string, or None).
        process_name: Name used in warning messages.

    Returns:
        Resolved ProcessRoleEnum value.
    """
    if raw is None:
        return ProcessRoleEnum.CORE
    if isinstance(raw, ProcessRoleEnum):
        return raw
    try:
        return ProcessRoleEnum(str(raw))
    except ValueError:
        logger.warning(
            "Unknown role '{}' for process '{}', defaulting to core",
            raw,
            process_name,
        )
        return ProcessRoleEnum.CORE


def resolve_restart_policy(
    raw: Any,
    process_name: str,
) -> ProcessRestartPolicyEnum:
    """Resolve restart policy value from config or metadata.

    Args:
        raw: Raw restart policy value (enum, string, or None).
        process_name: Name used in warning messages.

    Returns:
        Resolved ProcessRestartPolicyEnum value.
    """
    if raw is None:
        return ProcessRestartPolicyEnum.ON_FAILURE
    if isinstance(raw, ProcessRestartPolicyEnum):
        return raw
    try:
        return ProcessRestartPolicyEnum(str(raw))
    except ValueError:
        logger.warning(
            "Unknown restart policy '{}' for process '{}', defaulting to on-failure",
            raw,
            process_name,
        )
        return ProcessRestartPolicyEnum.ON_FAILURE


def resolve_tags(raw: Any) -> tuple[str, ...]:
    """Resolve tags from config or metadata.

    Args:
        raw: Raw tags value (list, tuple, set, or other).

    Returns:
        Tuple of tag strings, empty tuple if not iterable.
    """
    if isinstance(raw, (list, tuple, set)):
        return tuple(str(tag) for tag in cast(Iterable[Any], raw))
    return ()


def resolve_mode(
    raw: Any,
    process_name: str,
) -> ProcessMode:
    """Resolve and validate process execution mode.

    Unlike lifecycle/role resolvers which fall back to defaults,
    this function raises ValueError for unknown mode values to prevent
    silent fallthrough to thread execution.

    Args:
        raw: Raw mode value from config or API.
        process_name: Process name for error messages.

    Returns:
        Validated ProcessMode value.

    Raises:
        ValueError: If mode is not a valid ProcessMode.
    """
    if raw is None:
        return ProcessModeEnum.THREAD
    mode_str = str(raw)
    if mode_str not in VALID_PROCESS_MODES:
        raise ValueError(
            f"Invalid mode '{mode_str}' for process '{process_name}'. "
            f"Valid modes: {sorted(VALID_PROCESS_MODES)}"
        )
    return cast(ProcessMode, mode_str)


def resolve_parameters_schema(
    config_dict: dict[str, Any],
    entry: ProcessRegistryEntry | None,
) -> dict[str, Any] | None:
    """Resolve parameters_schema from config or registry entry.

    Args:
        config_dict: Parsed config dictionary.
        entry: Registry entry, or None if not registered.

    Returns:
        Parameters schema dict or None.
    """
    schema = config_dict.get("parameters_schema")
    if schema is None and entry is not None:
        schema = entry.parameters_schema
    return schema


def build_process_config_from_dict(
    process_name: str,
    config_dict: dict[str, Any],
    entry: ProcessRegistryEntry | None,
) -> ProcessConfigModel:
    """Build ProcessConfigModel from parsed config dict and registry entry.

    Args:
        process_name: The process name.
        config_dict: Parsed JSON config dictionary.
        entry: Registry entry, or None if not registered.

    Returns:
        Fully resolved ProcessConfigModel.
    """
    lifecycle_raw = config_dict.get("lifecycle")
    if lifecycle_raw is None:
        lifecycle_raw = entry.lifecycle if entry else ProcessLifecycleEnum.LONG_RUNNING
    role_raw = config_dict.get("role")
    if role_raw is None:
        role_raw = entry.role if entry else ProcessRoleEnum.CORE
    tags_raw = config_dict.get("tags")
    if tags_raw is None:
        tags_raw = entry.tags if entry else ()
    restart_policy_raw = config_dict.get("restart_policy")
    if restart_policy_raw is None:
        restart_policy_raw = entry.restart_policy if entry else ProcessRestartPolicyEnum.ON_FAILURE
    return ProcessConfigModel(
        name=process_name,
        enabled=config_dict.get("enabled", False),
        mode=resolve_mode(config_dict.get("mode", ProcessModeEnum.THREAD), process_name),
        class_path=config_dict["class"],
        method=config_dict.get("method", "start"),
        parameters=config_dict.get("parameters", {}),
        note=config_dict.get("note"),
        lifecycle=resolve_lifecycle(lifecycle_raw, process_name),
        role=resolve_role(role_raw, process_name),
        restart_policy=resolve_restart_policy(restart_policy_raw, process_name),
        tags=resolve_tags(tags_raw),
        parameters_schema=resolve_parameters_schema(config_dict, entry),
    )


def import_process_class(class_path: str, process_name: str | None = None) -> type:
    """Import a class by its fully qualified path.

    First checks the process registry, then falls back to
    dynamic import.

    Args:
        class_path: Fully qualified class path (e.g., "snapper.app.MyClass").
        process_name: Optional process name to check registry first.

    Returns:
        The imported class type.

    Raises:
        TypeError: If the imported object is not a class.
        ImportError: If the class cannot be imported.
    """
    if process_name:
        registry = get_registered_processes()
        if process_name in registry:
            cls = registry[process_name].class_ref
            if not isinstance(cls, type):
                raise TypeError(f"{class_path} is not a class")
            return cls
    try:
        module_path, class_name = class_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
        if not isinstance(cls, type):
            raise TypeError(f"{class_path} is not a class")
        return cls
    except (ValueError, ModuleNotFoundError, AttributeError) as e:
        raise ImportError(
            f"Failed to import class '{class_path}' (process_name='{process_name}'): {e}"
        ) from e


async def get_process_configs(settings: AppSettings) -> list[ProcessConfigModel]:
    """Load process configurations from database.

    Reads settings with key prefix "process_" and merges with
    registered process metadata.

    Args:
        settings: Application settings for database URL.

    Returns:
        List of ProcessConfigModel instances.
    """
    repository = get_repository(settings.db_url)
    registry = get_registered_processes()
    async with repository.session() as session:
        result = await session.execute(
            select(Setting).where(Setting.key.like("process_%"), *where_active_now(Setting))
        )
        settings_rows = result.scalars().all()
        configs: list[ProcessConfigModel] = []
        for setting in settings_rows:
            try:
                config_dict = json.loads(setting.value)
                process_name = setting.key.replace("process_", "")
                entry = registry.get(process_name)
                config = build_process_config_from_dict(process_name, config_dict, entry)
                configs.append(config)
            except (json.JSONDecodeError, KeyError) as e:
                logger.error(f"Failed to parse process config '{setting.key}': {e}")
    return configs
