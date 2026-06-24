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

from snapper.application.process_manager.models import ProcessRegistryEntry
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.registry import get_registered_processes
from snapper.config.settings import AppSettings
from snapper.core.json_types import JsonObject
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessMode
from snapper.core.types import ProcessRoleEnum
from snapper.data.models import Setting
from snapper.data.repository import Repository
from snapper.data.repository import close_and_insert
from snapper.data.repository import get_repository
from snapper.data.repository import where_active_now
from snapper.messaging.infrastructure.publisher import SequenceTracker

_SETTINGS_TOPIC = "settings"


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
        self._tracker = SequenceTracker()

    def _get_defaults_from_entry(self, entry: ProcessRegistryEntry) -> JsonObject:
        """Extract default configuration values from registry entry.

        Args:
            entry: ProcessRegistryEntry from the registry.

        Returns:
            Dictionary of default configuration values.
        """
        return {
            "enabled": entry.enabled,
            "mode": entry.mode,
            "parameters": {},
            "lifecycle": entry.lifecycle,
            "role": entry.role,
            "tags": list(entry.tags),
            "parameters_schema": entry.parameters_schema,
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
            "parameters": defaults["parameters"],
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
                timestamp=datetime.now(UTC),
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(_SETTINGS_TOPIC),
            )
            session.add(setting)
            await session.commit()
        logger.info(f"Created database config for process '{name}'")

    def _sync_parameters_from_entry(
        self,
        name: str,
        config_dict: dict[str, Any],
        entry: ProcessRegistryEntry,
    ) -> bool:
        """Sync parameters from registry entry into config_dict if missing.

        Args:
            name: Process name for logging.
            config_dict: Mutable config dictionary.
            entry: ProcessRegistryEntry from the registry.

        Returns:
            True if config_dict was updated.
        """
        parameters = config_dict.get("parameters", {})
        if parameters:
            logger.debug(f"Process '{name}' already has database config with parameters")
            return False
        cls = entry.class_ref
        try:
            default_parameters = cls.get_default_parameters(self.settings)
        except Exception as e:
            logger.debug(f"Process '{name}' get_default_parameters failed: {e}")
            default_parameters = {}
        if not default_parameters:
            logger.debug(f"Process '{name}' has empty default parameters")
            return False
        config_dict["parameters"] = default_parameters
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
    def _sync_tags_from_entry(
        config_dict: dict[str, Any],
        entry: ProcessRegistryEntry,
    ) -> bool:
        """Sync tags from registry entry to config_dict if not present.

        Args:
            config_dict: Mutable config dictionary.
            entry: ProcessRegistryEntry from the registry.

        Returns:
            True if config_dict was updated.
        """
        if "tags" in config_dict or not entry.tags:
            return False
        config_dict["tags"] = [str(tag) for tag in entry.tags]
        return True

    def _apply_entry_updates(
        self,
        name: str,
        config_dict: dict[str, Any],
        entry: ProcessRegistryEntry,
    ) -> bool:
        """Apply all registry entry updates to an existing config_dict.

        Args:
            name: Process name for logging.
            config_dict: Mutable config dictionary.
            entry: ProcessRegistryEntry from the registry.

        Returns:
            True if any field was updated.
        """
        updated = self._sync_parameters_from_entry(name, config_dict, entry)
        updated = self._sync_enum_field(config_dict, "lifecycle", entry.lifecycle) or updated
        updated = self._sync_enum_field(config_dict, "role", entry.role) or updated
        updated = self._sync_tags_from_entry(config_dict, entry) or updated
        if "parameters_schema" not in config_dict and entry.parameters_schema is not None:
            config_dict["parameters_schema"] = entry.parameters_schema
            updated = True
        return updated

    async def _sync_new_process(self, name: str, entry: ProcessRegistryEntry) -> None:
        """Create database config for a newly registered process.

        Args:
            name: Process name.
            entry: ProcessRegistryEntry from the registry.
        """
        cls: type[RegisterableProcess] = entry.class_ref
        defaults = self._get_defaults_from_entry(entry)
        try:
            defaults["parameters"] = cls.get_default_parameters(self.settings)
        except Exception as e:
            logger.warning(f"Failed to get default parameters for '{name}': {e}, using empty dict")
            defaults["parameters"] = {}
        await self._create_process_config_in_db(
            name=name,
            class_path=entry.class_path,
            method=entry.method,
            defaults=defaults,
        )

    async def _sync_existing_process(
        self,
        name: str,
        entry: ProcessRegistryEntry,
        existing: Setting,
        repository: Repository,
    ) -> None:
        """Update database config for an existing registered process.

        Args:
            name: Process name.
            entry: ProcessRegistryEntry from the registry.
            existing: Existing Setting row from database.
            repository: Database repository for update operations.
        """
        try:
            config_dict = json.loads(existing.value)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse config for '{name}': {e}")
            return
        if not self._apply_entry_updates(name, config_dict, entry):
            return
        config_key = f"process_{name}"
        async with repository.session() as update_session:
            now = datetime.now(UTC)
            await close_and_insert(
                session=update_session,
                model=Setting,
                match_filters=[Setting.key == config_key],
                new_values={
                    "key": config_key,
                    "value": json.dumps(config_dict, indent=4),
                    "category": existing.category,
                    "description": existing.description,
                    "is_encrypted": existing.is_encrypted,
                    "updated_by": "sync_registry",
                    "session_id": self._tracker.session_id,
                    "sequence_id": self._tracker.next_sequence(_SETTINGS_TOPIC),
                },
                bus_time=now,
            )
            await update_session.commit()
            logger.info(
                "Updated process '{}' metadata in database",
                name,
            )

    async def sync_registry_to_database(self) -> None:
        """Synchronize process registry with database configurations.

        Creates missing database entries and updates existing ones
        with current metadata from the registry. Each process is synced
        inside its own fault boundary: a failure on one process (for
        example a non-JSON-serializable default parameter) is logged and
        skipped so a single malformed registration cannot abort server
        startup and cascade to services that wait on its healthcheck.
        """
        registry = get_registered_processes()
        logger.info(f"Syncing {len(registry)} registered processes to database")
        repository = get_repository(self.settings.db_url)
        for name, entry in registry.items():
            try:
                config_key = f"process_{name}"
                async with repository.session() as session:
                    result = await session.execute(
                        select(Setting).where(Setting.key == config_key, *where_active_now(Setting))
                    )
                    existing = result.scalar_one_or_none()
                if existing is None:
                    await self._sync_new_process(name, entry)
                else:
                    await self._sync_existing_process(name, entry, existing, repository)
            except Exception as exc:
                logger.exception(
                    f"Failed to sync process '{name}' to database; skipping it so server "
                    f"startup is not blocked: {exc!r}"
                )

    async def create_process_config(
        self,
        *,
        name: str,
        class_path: str,
        method: str,
        enabled: bool,
        mode: ProcessMode,
        parameters: dict[str, Any],
        lifecycle: ProcessLifecycleEnum,
        role: ProcessRoleEnum,
        tags: Iterable[str],
        parameters_schema: JsonObject | None = None,
        note: str | None = None,
    ) -> None:
        """Create a new process configuration in the database.

        Args:
            name: Unique process name.
            class_path: Fully qualified class path.
            method: Entry method name.
            enabled: Whether process is enabled for autostart.
            mode: Execution mode (thread/process).
            parameters: Constructor parameters dict.
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
            "parameters": parameters,
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
            existing = await session.execute(
                select(Setting).where(Setting.key == config_key, *where_active_now(Setting))
            )
            if existing.scalar_one_or_none() is not None:
                raise ValueError(f"Process '{name}' is already configured")
            setting = Setting(
                key=config_key,
                value=json.dumps(config_dict, indent=4),
                category="process",
                timestamp=datetime.now(UTC),
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(_SETTINGS_TOPIC),
            )
            session.add(setting)
            await session.commit()
