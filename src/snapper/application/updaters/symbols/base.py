"""Base class for symbol mapping updater services.

Provides common functionality for fetching and persisting exchange symbol
mappings to the database.
"""

from abc import ABC
from abc import abstractmethod
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any

import zmq
import zmq.asyncio
from loguru import logger
from sqlalchemy import select

from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.services.settings import get_settings_service
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_with_service
from snapper.data.models import Setting
from snapper.data.repository import DatabaseRepository
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.schemas.messages import SymbolMappingUpdateEnvelope
from snapper.utils.logging import set_log_context


class SymbolMappingUpdaterService[T: ExchangeClientBase](RegisterableProcess, ABC):
    """Base service for updating exchange symbol mappings in the database."""

    def __init__(self, update_threshold_hours: int, force: bool = False) -> None:
        """Initialize the instance.

        Args:
            update_threshold_hours: Minimum hours between automatic updates.
            force: If True, bypass the update threshold check.
        """
        self.settings: AppSettings = get_settings()
        self.repository: DatabaseRepository | None = None
        self.context: zmq.asyncio.Context | None = None
        self.publisher: ValidatedPublisher | None = None
        self.update_threshold_hours = update_threshold_hours
        self.force = force

    @abstractmethod
    def _create_exchange_client(self) -> T:
        """Create and return the exchange client instance.

        Returns:
            Configured exchange client for fetching symbol data.
        """
        ...

    @abstractmethod
    def _get_setting_key(self) -> str:
        """Return the database setting key for storing last update timestamp.

        Returns:
            Setting key string unique to this exchange updater.
        """
        ...

    @abstractmethod
    async def _update_database(self, symbols: list[dict[str, Any]]) -> None:
        """Persist fetched symbol mappings to the database.

        Args:
            symbols: List of symbol dictionaries from the exchange.
        """
        ...

    async def _setup_zmq(self) -> None:
        """Initialize ZMQ context and publisher socket for cache invalidation."""
        if self.context is not None:
            return
        self.context = zmq.asyncio.Context()
        raw_pub_socket = self.context.socket(zmq.PUB)
        raw_pub_socket.connect(self.settings.zmq_broker_xsub)
        self.publisher = ValidatedPublisher(raw_pub_socket)
        logger.info(f"SymbolUpdater: Connected to broker {self.settings.zmq_broker_xsub}")

    async def _cleanup_zmq(self) -> None:
        """Close ZMQ publisher socket and terminate context."""
        if self.publisher:
            self.publisher.setsockopt(zmq.LINGER, 0)
            self.publisher.close()
            self.publisher = None
        if self.context:
            self.context.term()
            self.context = None
        logger.info("SymbolUpdater: ZMQ resources cleaned up")

    async def broadcast_cache_invalidation(self) -> None:
        """Broadcast cache invalidation message via ZMQ to all subscribers."""
        if not self.publisher:
            await self._setup_zmq()
        if self.publisher:
            envelope = SymbolMappingUpdateEnvelope()
            topic = "system.symbol_mappings"
            await self.publisher.send_multipart(topic, envelope.to_json().encode())
            logger.info(f"Broadcasted cache invalidation: {topic}")
        else:
            logger.warning("ZMQ publisher not available, skipping cache invalidation broadcast")

    def _get_last_update_timestamp(self) -> datetime | None:
        """Retrieve the timestamp of the last symbol mapping update from database.

        Returns:
            Datetime of last update, or None if no previous update exists.
        """
        try:
            assert self.repository is not None, "Repository not initialized"
            with self.repository.get_session() as session:
                stmt = select(Setting).where(Setting.key == self._get_setting_key())
                setting = session.execute(stmt).scalar_one_or_none()
                if setting is None or setting.value == "null" or not setting.value:
                    return None
                return datetime.fromisoformat(setting.value.replace("Z", "+00:00"))
        except Exception as e:
            logger.warning(f"Error getting last update timestamp: {e}")
            return None

    def _set_last_update_timestamp(self, timestamp: datetime) -> None:
        """Store the timestamp of the current symbol mapping update to database.

        Args:
            timestamp: Datetime to record as the last update time.
        """
        try:
            assert self.repository is not None, "Repository not initialized"
            with self.repository.get_session() as session:
                stmt = select(Setting).where(Setting.key == self._get_setting_key())
                setting = session.execute(stmt).scalar_one_or_none()
                iso_timestamp = timestamp.isoformat()
                if setting is None:
                    setting = Setting(
                        key=self._get_setting_key(),
                        value=iso_timestamp,
                        category="system",
                        description=f"Timestamp of last {self._get_setting_key()} update",
                        updated_at=timestamp,
                    )
                    session.add(setting)
                else:
                    setting.value = iso_timestamp
                    setting.updated_at = timestamp
                session.commit()
                logger.info(f"Updated timestamp: {self._get_setting_key()} = {iso_timestamp}")
        except Exception as e:
            logger.error(f"Error setting last update timestamp: {e}")
            raise

    def should_update(self) -> bool:
        """Check if symbol mappings should be updated based on threshold and force flag.

        Returns:
            True if update should proceed, False otherwise.
        """
        if self.force:
            logger.info("Force mode enabled - update will proceed")
            return True
        last_update = self._get_last_update_timestamp()
        if last_update is None:
            logger.info("No previous update found - update will proceed")
            return True
        time_since_update = datetime.now(UTC) - last_update
        threshold = timedelta(hours=self.update_threshold_hours)
        if time_since_update < threshold:
            remaining = threshold - time_since_update
            hours_ago = time_since_update.total_seconds() / 3600
            hours_remaining = remaining.total_seconds() / 3600
            logger.info(
                f"Update not needed - last update {hours_ago:.1f}h ago "
                f"(threshold: {self.update_threshold_hours}h, remaining: {hours_remaining:.1f}h)"
            )
            return False
        logger.info(
            f"Update needed - last update {time_since_update.total_seconds() / 3600:.1f}h ago "
            f"(threshold: {self.update_threshold_hours}h)"
        )
        return True

    async def _fetch_symbols(self, client: T) -> list[dict[str, Any]]:
        """Fetch symbol data from the exchange client.

        Args:
            client: Exchange client instance to fetch symbols from.

        Returns:
            List of symbol dictionaries containing exchange instrument data.
        """
        logger.info("Fetching symbols via subscribe_instruments()...")
        symbols: list[dict[str, Any]] = []
        async for symbol_data in client.subscribe_instruments():
            symbols.append(symbol_data)
        logger.info(f"Fetched {len(symbols)} symbols from exchange")
        return symbols

    async def start(self) -> None:
        """Execute the symbol mapping update process."""
        setting_key = self._get_setting_key()
        exchange = setting_key.split("_")[0]
        set_log_context(f"sym:{exchange}")
        try:
            self.repository = DatabaseRepository()
            logger.info("Repository initialized")
            settings_service = await get_settings_service(
                self.settings.db_url,
                self.settings.zmq_broker_xpub,
                self.settings.master_password,
                self.settings.encryption_salt,
            )
            self.settings = get_settings_with_service(settings_service)
            logger.info("AppSettings service initialized with database access")
            if not self.should_update():
                logger.info("Skipping update")
                return
            await self._setup_zmq()
            client = self._create_exchange_client()
            await client.connect()
            logger.info(f"Exchange client connected: {type(client).__name__}")
            try:
                symbols = await self._fetch_symbols(client)
                await self._update_database(symbols)
                await self.broadcast_cache_invalidation()
                self._set_last_update_timestamp(datetime.now(UTC))
                logger.info("Symbol mapping update completed successfully")
            finally:
                await client.disconnect()
                logger.info("Exchange client disconnected")
        except Exception as e:
            logger.error(f"Symbol mapping update failed: {e}")
            raise
        finally:
            await self._cleanup_zmq()
            if self.repository is not None:
                self.repository = None
