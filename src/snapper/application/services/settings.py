"""Settings service module.

This module provides a centralized settings management service.
It handles:
- Loading settings from database with caching
- Encryption of sensitive settings
- Real-time settings updates via ZMQ broadcast
- Settings retrieval by category
"""

import asyncio
import contextlib
import json
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from typing import Any

import zmq
import zmq.asyncio
from loguru import logger
from sqlalchemy import select
from sqlalchemy import update

from snapper.data.models import Setting
from snapper.data.repository import get_repository
from snapper.infrastructure.security.encryption import decrypt_if_encrypted
from snapper.infrastructure.security.encryption import encrypt_if_sensitive
from snapper.infrastructure.security.encryption import force_encrypt_if_cleartext
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.schemas.data import SettingChangedData


@dataclass
class SettingChangeEvent:
    """Event data for a setting change.

    Attributes:
        key: Setting key that was changed.
        value: New setting value.
        category: Setting category.
        timestamp: When the change occurred.
        updated_by: Optional user who made the change.
    """

    key: str
    value: str
    category: str
    timestamp: datetime
    updated_by: str | None = None


class SettingsService:
    """Singleton service for managing application settings.

    Provides centralized settings management with:
    - Database-backed persistence
    - In-memory caching for fast reads
    - Encryption support for sensitive values
    - ZMQ broadcast on changes for distributed sync

    Uses singleton pattern - same instance returned for same parameters.

    Attributes:
        db_url: Database connection URL.
        zmq_broker_xpub: ZMQ broker XPUB address for publishing changes.
    """

    _instance: SettingsService | None = None
    _init_params: tuple[str, str] | None = None
    _initialized: bool = False

    def __new__(
        cls,
        db_url: str,
        zmq_broker_xpub: str,
    ) -> SettingsService:
        """Create or return existing singleton instance.

        Returns same instance if called with identical parameters.

        Args:
            db_url: Database connection URL.
            zmq_broker_xpub: ZMQ broker XPUB address.

        Returns:
            SettingsService singleton instance.
        """
        current_params = (db_url, zmq_broker_xpub)
        if cls._instance is not None and cls._init_params == current_params:
            return cls._instance
        instance = super().__new__(cls)
        cls._instance = instance
        cls._init_params = current_params
        return instance

    def __init__(
        self,
        db_url: str,
        zmq_broker_xpub: str,
    ) -> None:
        """Initialize the settings service.

        Only runs once per singleton instance.

        Args:
            db_url: Database connection URL.
            zmq_broker_xpub: ZMQ broker XPUB address.
        """
        if self._initialized:
            return
        self.db_url = db_url
        self.zmq_broker_xpub = zmq_broker_xpub
        self._cache: dict[str, Any] = {}
        self._loaded = False
        self._zmq_context: zmq.asyncio.Context | None = None
        self._publisher: ValidatedPublisher | None = None
        self._initialized = True

    async def initialize(self) -> None:
        """Initialize the service by loading settings and setting up ZMQ."""
        await self._load_all_settings()
        await self._setup_zmq_publisher()

    async def shutdown(self) -> None:
        """Shutdown the service and cleanup resources."""
        if self._publisher:
            with contextlib.suppress(Exception):
                self._publisher.setsockopt(zmq.LINGER, 0)
            with contextlib.suppress(Exception):
                self._publisher.close()
            self._publisher = None
        if self._zmq_context:
            with contextlib.suppress(Exception):
                self._zmq_context.term()
            self._zmq_context = None
        logger.info("SettingsService shutdown complete")
        await asyncio.sleep(0)

    async def _load_all_settings(self) -> None:
        """Load all settings from database into cache.

        Decrypts encrypted values and parses JSON/primitive types.
        """
        repository = get_repository(self.db_url)
        async with repository.session() as session:
            now = datetime.now(UTC)
            result = await session.execute(
                select(Setting).where(Setting.timestamp <= now, Setting.known_to > now)
            )
            settings = result.scalars().all()
            self._cache = {}
            for setting in settings:
                raw_value = decrypt_if_encrypted(setting.value, setting.is_encrypted)
                self._cache[setting.key] = self._parse_value(raw_value)
            self._loaded = True
            logger.info(f"Loaded {len(settings)} settings from database")

    def _parse_value(self, value: str) -> Any:
        """Parse string value to appropriate Python type.

        Attempts parsing in order: JSON, bool, int, float, string.

        Args:
            value: Raw string value from database.

        Returns:
            Parsed value as appropriate Python type.
        """
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            pass
        if value.lower() in ("true", "false"):
            return value.lower() == "true"
        try:
            return int(value)
        except ValueError:
            pass
        try:
            return float(value)
        except ValueError:
            pass
        return value

    def _serialize_value(self, value: Any) -> str:
        """Serialize Python value to string for storage.

        Args:
            value: Value to serialize.

        Returns:
            String representation for database storage.
        """
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        elif isinstance(value, bool):
            return "true" if value else "false"
        else:
            return str(value)

    async def _setup_zmq_publisher(self) -> None:
        """Set up ZMQ publisher for broadcasting changes."""
        self._zmq_context = zmq.asyncio.Context()
        raw_pub_socket = self._zmq_context.socket(zmq.PUB)
        raw_pub_socket.connect(self.zmq_broker_xpub)
        self._publisher = ValidatedPublisher(raw_pub_socket)
        logger.info(f"Settings service connected to ZMQ broker: {self.zmq_broker_xpub}")
        await asyncio.sleep(0)

    def get_setting(self, key: str, default: Any = None) -> Any:
        """Get a setting value from cache.

        Args:
            key: Setting key to retrieve.
            default: Default value if not found.

        Returns:
            Setting value or default.
        """
        if not self._loaded:
            logger.warning(f"Settings not loaded yet, returning default for {key}")
            return default
        return self._cache.get(key, default)

    async def update_setting(
        self,
        key: str,
        value: Any,
        category: str = "system",
        description: str | None = None,
        updated_by: str | None = None,
        force_encrypt_cleartext: bool = True,
    ) -> None:
        """Update or create a setting.

        Updates database, cache, and broadcasts change via ZMQ.

        Args:
            key: Setting key.
            value: New value (any serializable type).
            category: Setting category. Defaults to "system".
            description: Optional description.
            updated_by: Optional user identifier.
            force_encrypt_cleartext: Whether to encrypt sensitive values.
        """
        str_value = self._serialize_value(value)
        if force_encrypt_cleartext:
            encrypted_value, is_encrypted = force_encrypt_if_cleartext(key, str_value)
        else:
            encrypted_value, is_encrypted = encrypt_if_sensitive(key, str_value)
        repository = get_repository(self.db_url)
        async with repository.session() as session:
            now = datetime.now(UTC)
            existing = (
                (
                    await session.execute(
                        select(Setting)
                        .where(
                            Setting.key == key,
                            Setting.timestamp <= now,
                            Setting.known_to > now,
                        )
                        .with_for_update()
                    )
                )
                .scalars()
                .first()
            )
            if existing:
                await session.execute(
                    update(Setting).where(Setting.id == existing.id).values(known_to=now)
                )
                new_setting = Setting(
                    public_id=existing.public_id,
                    key=key,
                    value=encrypted_value,
                    category=category,
                    description=description,
                    is_encrypted=is_encrypted,
                    timestamp=now,
                    updated_by=updated_by,
                )
            else:
                new_setting = Setting(
                    key=key,
                    value=encrypted_value,
                    category=category,
                    description=description,
                    is_encrypted=is_encrypted,
                    timestamp=now,
                    updated_by=updated_by,
                )
            session.add(new_setting)
            await session.commit()
        self._cache[key] = value
        await self._broadcast_change(key, encrypted_value, category, updated_by)
        logger.info(
            f"Setting {key} updated to: {encrypted_value}"
            + (" (encrypted)" if is_encrypted else "")
        )

    async def _broadcast_change(
        self, key: str, value: str, category: str, updated_by: str | None = None
    ) -> None:
        """Broadcast setting change via ZMQ.

        Args:
            key: Changed setting key.
            value: New value (encrypted if applicable).
            category: Setting category.
            updated_by: Optional user identifier.
        """
        if not self._publisher:
            logger.warning("ZMQ publisher not available, skipping broadcast")
            return
        envelope = SettingChangedData(
            key=key,
            value=value,
            category=category,
            updated_by=updated_by,
        )
        try:
            await self._publisher.send_multipart(
                "system.settings", envelope.to_json().encode("utf-8")
            )
            logger.debug(f"Broadcasted setting change: {key}")
        except Exception as e:
            logger.error(f"Failed to broadcast setting change: {e}")

    async def get_all_settings(self) -> dict[str, Any]:
        """Get all settings from cache.

        Returns:
            Copy of settings cache dict.
        """
        await asyncio.sleep(0)
        return self._cache.copy()

    async def get_settings_by_category(self, category: str) -> dict[str, Any]:
        """Get settings filtered by category.

        Loads directly from database to ensure accuracy.

        Args:
            category: Category to filter by.

        Returns:
            Dict of settings in the specified category.
        """
        repository = get_repository(self.db_url)
        async with repository.session() as session:
            result = await session.execute(select(Setting).where(Setting.category == category))
            settings = result.scalars().all()
            decrypted_settings = {}
            for setting in settings:
                raw_value = decrypt_if_encrypted(setting.value, setting.is_encrypted)
                decrypted_settings[setting.key] = self._parse_value(raw_value)
            return decrypted_settings

    @classmethod
    def get_instance(
        cls,
        db_url: str | None = None,
        zmq_broker_xpub: str | None = None,
    ) -> SettingsService | None:
        """Get existing singleton or create new one.

        Args:
            db_url: Database URL (required for new instance).
            zmq_broker_xpub: ZMQ broker address (required for new instance).

        Returns:
            SettingsService instance or None if parameters missing.
        """
        if cls._instance is None:
            if db_url is None or zmq_broker_xpub is None:
                return None
            cls._instance = SettingsService(db_url, zmq_broker_xpub)
        return cls._instance

    @classmethod
    def clear_instance(cls) -> None:
        """Clear the singleton instance.

        Useful for testing or reinitializing with different parameters.
        """
        cls._instance = None
        cls._init_params = None


async def get_settings_service(
    db_url: str,
    zmq_broker_xpub: str,
) -> SettingsService:
    """Get or create an initialized SettingsService.

    Convenience function that gets the singleton and initializes
    it if not already loaded.

    Args:
        db_url: Database connection URL.
        zmq_broker_xpub: ZMQ broker XPUB address.

    Returns:
        Initialized SettingsService instance.
    """
    instance = SettingsService.get_instance(db_url, zmq_broker_xpub)
    assert instance is not None, "get_instance should never return None with required params"
    if not instance._loaded:
        await instance.initialize()
    return instance
