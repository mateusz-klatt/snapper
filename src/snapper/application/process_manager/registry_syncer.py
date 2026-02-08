"""Process registry synchronization service.

Synchronizes the in-memory process registry with database
configurations, creating new entries and updating existing
ones with current metadata.
"""

import json
from collections.abc import Iterable
from datetime import UTC
from datetime import datetime
from enum import Enum
from typing import Any
from typing import cast

from loguru import logger
from sqlalchemy import select

from snapper.application.process_manager.enums import ProcessLifecycleEnum
from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.registry import get_registered_processes
from snapper.config.settings import AppSettings
from snapper.core.types import ProcessMode
from snapper.data.models import Setting
from snapper.data.repository import get_repository


class ProcessRegistrySyncer:
    """Synchronizes process registry with database configurations.

    Handles creating new database entries for newly registered processes
    and updating existing entries when metadata changes.

    Attributes:
        settings: Application settings for database access.
    """

    def __init__(self, settings: AppSettings) -> None:
        """Initialize the registry syncer.

        Args:
            settings: Application settings for database URL.
        """
        self.settings = settings

    def _get_defaults_from_metadata(self, metadata: dict[str, Any]) -> dict[str, Any]:
        """Extract default configuration values from registry metadata.

        Args:
            metadata: Registry metadata dictionary.

        Returns:
            Dictionary of default configuration values.
        """
        return {
            "enabled": metadata.get("enabled", False),
            "mode": metadata.get("mode", "thread"),
            "args": metadata.get("args", []),
            "kwargs": {},
            "lifecycle": metadata.get("lifecycle", ProcessLifecycleEnum.LONG_RUNNING),
            "role": metadata.get("role", ProcessRoleEnum.CORE),
            "tags": metadata.get("tags", ()),
            "parameters_schema": metadata.get("parameters_schema"),
        }

    async def _create_process_config_in_db(
        self, name: str, class_path: str, method: str, defaults: dict[str, Any]
    ) -> None:
        """Create a new process configuration Setting row in the database.

        Args:
            name: Process name.
            class_path: Fully qualified class path.
            method: Entry method name.
            defaults: Default configuration values.
        """
        repository = get_repository(self.settings.db_url)
        config_dict: dict[str, Any] = {
            "enabled": defaults["enabled"],
            "mode": defaults["mode"],
            "class": class_path,
            "method": method,
            "args": defaults["args"],
            "kwargs": defaults["kwargs"],
            "lifecycle": (
                defaults["lifecycle"].value
                if isinstance(defaults["lifecycle"], ProcessLifecycleEnum)
                else str(defaults["lifecycle"])
            ),
            "role": (
                defaults["role"].value
                if isinstance(defaults["role"], ProcessRoleEnum)
                else str(defaults["role"])
            ),
        }
        tags_default = defaults.get("tags")
        if isinstance(tags_default, (list, tuple, set)):
            config_dict["tags"] = [str(tag) for tag in cast(Iterable[Any], tags_default)]
        parameters_schema_default = defaults.get("parameters_schema")
        if parameters_schema_default is not None:
            config_dict["parameters_schema"] = parameters_schema_default
        async with repository.session() as session:
            setting = Setting(
                key=f"process_{name}",
                value=json.dumps(config_dict),
                category="process",
                updated_at=datetime.now(UTC),
            )
            session.add(setting)
            await session.commit()
        logger.info(f"Created database config for process '{name}'")

    def _sync_kwargs_from_metadata(
        self,
        name: str,
        config_dict: dict[str, Any],
        metadata: dict[str, Any],
    ) -> bool:
        """Sync kwargs from metadata into config_dict if missing.

        Args:
            name: Process name for logging.
            config_dict: Mutable config dictionary.
            metadata: Registry metadata dictionary.

        Returns:
            True if config_dict was updated.
        """
        kwargs = config_dict.get("kwargs", {})
        if kwargs:
            logger.debug(f"Process '{name}' already has database config with kwargs")
            return False
        cls = metadata["class_ref"]
        try:
            default_kwargs = cls.get_default_kwargs(self.settings)
        except Exception as e:
            logger.debug(f"Process '{name}' get_default_kwargs failed: {e}")
            default_kwargs = {}
        if not default_kwargs:
            logger.debug(f"Process '{name}' has empty default kwargs")
            return False
        config_dict["kwargs"] = default_kwargs
        return True

    @staticmethod
    def _sync_enum_field(
        config_dict: dict[str, Any],
        field_key: str,
        meta_value: Any,
    ) -> bool:
        """Sync an enum field from metadata to config_dict.

        Args:
            config_dict: Mutable config dictionary.
            field_key: Key in config_dict to check/update.
            meta_value: Metadata value (enum or string).

        Returns:
            True if config_dict was updated.
        """
        new_value = meta_value.value if isinstance(meta_value, Enum) else str(meta_value)
        if config_dict.get(field_key) == new_value:
            return False
        config_dict[field_key] = new_value
        return True

    @staticmethod
    def _sync_tags_from_metadata(
        config_dict: dict[str, Any],
        metadata: dict[str, Any],
    ) -> bool:
        """Sync tags from metadata to config_dict if not present.

        Args:
            config_dict: Mutable config dictionary.
            metadata: Registry metadata dictionary.

        Returns:
            True if config_dict was updated.
        """
        if "tags" in config_dict or not metadata.get("tags"):
            return False
        tags_meta = metadata.get("tags", ())
        if not isinstance(tags_meta, (list, tuple, set)):
            return False
        config_dict["tags"] = [str(tag) for tag in cast(Iterable[Any], tags_meta)]
        return True

    def _apply_metadata_updates(
        self,
        name: str,
        config_dict: dict[str, Any],
        metadata: dict[str, Any],
    ) -> bool:
        """Apply all metadata updates to an existing config_dict.

        Args:
            name: Process name for logging.
            config_dict: Mutable config dictionary.
            metadata: Registry metadata dictionary.

        Returns:
            True if any field was updated.
        """
        updated = self._sync_kwargs_from_metadata(name, config_dict, metadata)
        lifecycle_meta = metadata.get("lifecycle", ProcessLifecycleEnum.LONG_RUNNING)
        updated = self._sync_enum_field(config_dict, "lifecycle", lifecycle_meta) or updated
        role_meta = metadata.get("role", ProcessRoleEnum.CORE)
        updated = self._sync_enum_field(config_dict, "role", role_meta) or updated
        updated = self._sync_tags_from_metadata(config_dict, metadata) or updated
        if "parameters_schema" not in config_dict and metadata.get("parameters_schema") is not None:
            config_dict["parameters_schema"] = metadata.get("parameters_schema")
            updated = True
        return updated

    async def _sync_new_process(self, name: str, metadata: dict[str, Any]) -> None:
        """Create database config for a newly registered process.

        Args:
            name: Process name.
            metadata: Registry metadata for this process.
        """
        cls: type[RegisterableProcess] = metadata["class_ref"]
        defaults = self._get_defaults_from_metadata(metadata)
        try:
            defaults["kwargs"] = cls.get_default_kwargs(self.settings)
        except Exception as e:
            logger.warning(f"Failed to get default kwargs for '{name}': {e}, using empty dict")
            defaults["kwargs"] = {}
        await self._create_process_config_in_db(
            name=name,
            class_path=metadata["class_path"],
            method=metadata["method"],
            defaults=defaults,
        )

    async def _sync_existing_process(
        self,
        name: str,
        metadata: dict[str, Any],
        existing: Any,
        repository: Any,
    ) -> None:
        """Update database config for an existing registered process.

        Args:
            name: Process name.
            metadata: Registry metadata for this process.
            existing: Existing Setting row from database.
            repository: Database repository for update operations.
        """
        try:
            config_dict = json.loads(existing.value)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse config for '{name}': {e}")
            return
        if not self._apply_metadata_updates(name, config_dict, metadata):
            return
        config_key = f"process_{name}"
        async with repository.session() as update_session:
            result = await update_session.execute(select(Setting).where(Setting.key == config_key))
            existing_record = result.scalar_one_or_none()
            if existing_record:
                existing_record.value = json.dumps(config_dict, indent=4)
                existing_record.updated_at = datetime.now(UTC)
                existing_record.updated_by = "sync_registry"
                await update_session.commit()
                logger.info(
                    "Updated process '{}' metadata in database",
                    name,
                )

    async def sync_registry_to_database(self) -> None:
        """Synchronize process registry with database configurations.

        Creates missing database entries and updates existing ones
        with current metadata from the registry.
        """
        registry = get_registered_processes()
        logger.info(f"Syncing {len(registry)} registered processes to database")
        repository = get_repository(self.settings.db_url)
        for name, metadata in registry.items():
            config_key = f"process_{name}"
            async with repository.session() as session:
                result = await session.execute(select(Setting).where(Setting.key == config_key))
                existing = result.scalar_one_or_none()
            if existing is None:
                await self._sync_new_process(name, metadata)
            else:
                await self._sync_existing_process(name, metadata, existing, repository)

    async def create_process_config(
        self,
        *,
        name: str,
        class_path: str,
        method: str,
        enabled: bool,
        mode: ProcessMode,
        args: list[Any],
        kwargs: dict[str, Any],
        lifecycle: ProcessLifecycleEnum,
        role: ProcessRoleEnum,
        tags: Iterable[str],
        parameters_schema: dict[str, Any] | None = None,
        note: str | None = None,
    ) -> None:
        """Create a new process configuration in the database.

        Args:
            name: Unique process name.
            class_path: Fully qualified class path.
            method: Entry method name.
            enabled: Whether process is enabled for autostart.
            mode: Execution mode (thread/process).
            args: Positional arguments.
            kwargs: Keyword arguments.
            lifecycle: Process lifecycle type.
            role: Process role category.
            tags: Process tags for grouping.
            parameters_schema: Optional JSON schema for parameters.
            note: Optional description note.

        Raises:
            ValueError: If process name already exists.
        """
        repository = get_repository(self.settings.db_url)
        config_key = f"process_{name}"
        config_dict: dict[str, Any] = {
            "enabled": enabled,
            "mode": mode,
            "class": class_path,
            "method": method,
            "args": args,
            "kwargs": kwargs,
            "lifecycle": lifecycle.value,
            "role": role.value,
        }
        tags_list = [str(tag) for tag in tags]
        if tags_list:
            config_dict["tags"] = tags_list
        if parameters_schema is not None:
            config_dict["parameters_schema"] = parameters_schema
        if note is not None:
            config_dict["note"] = note
        async with repository.session() as session:
            existing = await session.execute(select(Setting).where(Setting.key == config_key))
            if existing.scalar_one_or_none() is not None:
                raise ValueError(f"Process '{name}' is already configured")
            setting = Setting(
                key=config_key,
                value=json.dumps(config_dict, indent=4),
                category="process",
                updated_at=datetime.now(UTC),
            )
            session.add(setting)
            await session.commit()
