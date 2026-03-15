"""Base class for symbol updater services.

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
from sqlalchemy import or_
from sqlalchemy import select

from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.services.settings import get_settings_service
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_with_service
from snapper.core.types import AliasChannel
from snapper.core.types import AssetType
from snapper.core.types import UpsertResult
from snapper.data.models import Setting
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolExchangeCapability
from snapper.data.models import SymbolVersion
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import close_and_insert
from snapper.data.repository import close_and_insert_sync
from snapper.data.repository import get_repository
from snapper.data.repository import where_active
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.schemas.data import SymbolAliasUpdateData
from snapper.utils.logging import set_log_context


class SymbolUpdaterService[T: ExchangeClientBase](RegisterableProcess, ABC):
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

    def _setup_zmq(self) -> None:
        """Initialize ZMQ context and publisher socket for cache invalidation."""
        if self.context is not None:
            return
        self.context = zmq.asyncio.Context()
        raw_pub_socket = self.context.socket(zmq.PUB)
        raw_pub_socket.connect(self.settings.zmq_broker_xsub)
        self.publisher = ValidatedPublisher(raw_pub_socket)
        logger.info(f"SymbolUpdater: Connected to broker {self.settings.zmq_broker_xsub}")

    def _cleanup_zmq(self) -> None:
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
            self._setup_zmq()
        if self.publisher:
            envelope = SymbolAliasUpdateData()
            topic = "system.symbol_aliases"
            await self.publisher.send_multipart(topic, envelope.to_json().encode())
            logger.info(f"Broadcasted cache invalidation: {topic}")
        else:
            logger.warning("ZMQ publisher not available, skipping cache invalidation broadcast")

    @staticmethod
    def _upsert_catalog(
        session: Any,
        native_symbol: str,
        base: str,
        quote: str | None,
        asset_type: AssetType,
        now: datetime,
    ) -> bool:
        """Upsert a Symbol identity row and a SymbolVersion versioned row.

        Ensures the Symbol identity row exists (INSERT if missing), then
        upserts the SymbolVersion using SCD Type 2 close+insert when
        payload attributes (base, quote, asset_type) have changed.

        Args:
            session: SQLAlchemy session.
            native_symbol: Native symbol (PK).
            base: Base currency code.
            quote: Quote currency code, or None for equity/index.
            asset_type: One of crypto, forex, equity, index.
            now: Current UTC timestamp.

        Returns:
            True if a new Symbol identity row was created, False if it
            already existed.
        """
        existing_symbol = session.execute(
            select(Symbol).where(Symbol.native_symbol == native_symbol)
        ).scalar_one_or_none()
        created = existing_symbol is None
        if created:
            session.add(Symbol(native_symbol=native_symbol, created_at=now, timestamp=now))
            session.flush()

        existing_version = session.execute(
            select(SymbolVersion).where(
                SymbolVersion.native_symbol == native_symbol,
                SymbolVersion.timestamp <= now,
                SymbolVersion.known_to > now,
            )
        ).scalar_one_or_none()
        if existing_version is None:
            session.add(
                SymbolVersion(
                    native_symbol=native_symbol,
                    base=base,
                    quote=quote,
                    asset_type=asset_type,
                    timestamp=now,
                )
            )
        else:
            changed = (
                existing_version.base != base
                or existing_version.quote != quote
                or existing_version.asset_type != asset_type
            )
            if changed:
                close_and_insert_sync(
                    session=session,
                    model=SymbolVersion,
                    match_filters=[SymbolVersion.native_symbol == native_symbol],
                    new_values={
                        "native_symbol": native_symbol,
                        "base": base,
                        "quote": quote,
                        "asset_type": asset_type,
                    },
                    bus_time=now,
                )
        return created

    @staticmethod
    def _upsert_alias(
        session: Any,
        native_symbol: str,
        exchange: str,
        channel: AliasChannel,
        exchange_symbol: str,
        now: datetime,
    ) -> UpsertResult:
        """Upsert a SymbolAlias row using SCD Type 2 close+insert.

        Args:
            session: SQLAlchemy session.
            native_symbol: Native symbol (FK to symbols).
            exchange: Exchange name (lowercase).
            channel: Channel type (ws, rest, or ccxt).
            exchange_symbol: Exchange-specific symbol string.
            now: Current UTC timestamp.

        Returns:
            One of ``created``, ``updated``, or ``unchanged``.
        """
        existing = session.execute(
            select(SymbolAlias).where(
                SymbolAlias.native_symbol == native_symbol,
                SymbolAlias.exchange == exchange,
                SymbolAlias.channel == channel,
                SymbolAlias.timestamp <= now,
                SymbolAlias.known_to > now,
            )
        ).scalar_one_or_none()
        if existing is None:
            session.add(
                SymbolAlias(
                    native_symbol=native_symbol,
                    exchange=exchange,
                    channel=channel,
                    exchange_symbol=exchange_symbol,
                    created_at=now,
                    timestamp=now,
                )
            )
            return "created"
        if existing.exchange_symbol != exchange_symbol:
            close_and_insert_sync(
                session=session,
                model=SymbolAlias,
                match_filters=[
                    SymbolAlias.native_symbol == native_symbol,
                    SymbolAlias.exchange == exchange,
                    SymbolAlias.channel == channel,
                ],
                new_values={
                    "native_symbol": native_symbol,
                    "exchange": exchange,
                    "channel": channel,
                    "exchange_symbol": exchange_symbol,
                    "created_at": existing.created_at,
                },
                bus_time=now,
            )
            return "updated"
        return "unchanged"

    @staticmethod
    def _upsert_capability(
        session: Any,
        native_symbol: str,
        exchange: str,
        can_market_data: bool,
        can_trade: bool,
        source: str | None,
        reason: str | None,
        now: datetime,
    ) -> UpsertResult:
        """Upsert a SymbolExchangeCapability row using SCD Type 2 close+insert.

        Args:
            session: SQLAlchemy session.
            native_symbol: Native symbol (FK to symbols).
            exchange: Exchange name (lowercase).
            can_market_data: Whether exchange provides market data for this symbol.
            can_trade: Whether exchange supports trading this symbol.
            source: Origin of the capability information (e.g., updater name).
            reason: Human-readable explanation for the capability values.
            now: Current UTC timestamp.

        Returns:
            One of ``created``, ``updated``, or ``unchanged``.
        """
        existing = session.execute(
            select(SymbolExchangeCapability).where(
                SymbolExchangeCapability.native_symbol == native_symbol,
                SymbolExchangeCapability.exchange == exchange,
                SymbolExchangeCapability.timestamp <= now,
                SymbolExchangeCapability.known_to > now,
            )
        ).scalar_one_or_none()
        if existing is None:
            session.add(
                SymbolExchangeCapability(
                    native_symbol=native_symbol,
                    exchange=exchange,
                    can_market_data=can_market_data,
                    can_trade=can_trade,
                    source=source,
                    reason=reason,
                    created_at=now,
                    timestamp=now,
                )
            )
            return "created"
        changed = (
            existing.can_market_data != can_market_data
            or existing.can_trade != can_trade
            or existing.source != source
            or existing.reason != reason
        )
        if changed:
            close_and_insert_sync(
                session=session,
                model=SymbolExchangeCapability,
                match_filters=[
                    SymbolExchangeCapability.native_symbol == native_symbol,
                    SymbolExchangeCapability.exchange == exchange,
                ],
                new_values={
                    "native_symbol": native_symbol,
                    "exchange": exchange,
                    "can_market_data": can_market_data,
                    "can_trade": can_trade,
                    "source": source,
                    "reason": reason,
                    "created_at": existing.created_at,
                },
                bus_time=now,
            )
            return "updated"
        return "unchanged"

    @staticmethod
    def _deactivate_stale_capabilities(
        session: Any,
        exchange: str,
        active_symbols: set[str],
        source: str,
        now: datetime,
    ) -> int:
        """Deactivate capabilities for symbols no longer seen on the exchange.

        Uses SCD Type 2 close+insert to set ``can_trade=False`` and
        ``can_market_data=False`` for capability rows belonging to
        ``exchange`` whose ``native_symbol`` is not in ``active_symbols``.

        Args:
            session: SQLAlchemy session.
            exchange: Exchange name (lowercase).
            active_symbols: Set of native symbols still active on the exchange.
            source: Updater source tag for the deactivation record.
            now: Current UTC timestamp.

        Returns:
            Number of deactivated capability rows.
        """
        stmt = select(SymbolExchangeCapability).where(
            SymbolExchangeCapability.exchange == exchange,
            SymbolExchangeCapability.timestamp <= now,
            SymbolExchangeCapability.known_to > now,
            or_(
                SymbolExchangeCapability.can_trade.is_(True),
                SymbolExchangeCapability.can_market_data.is_(True),
            ),
        )
        active_caps = session.execute(stmt).scalars().all()
        deactivated = 0
        for cap in active_caps:
            if cap.native_symbol not in active_symbols:
                close_and_insert_sync(
                    session=session,
                    model=SymbolExchangeCapability,
                    match_filters=[
                        SymbolExchangeCapability.native_symbol == cap.native_symbol,
                        SymbolExchangeCapability.exchange == exchange,
                    ],
                    new_values={
                        "native_symbol": cap.native_symbol,
                        "exchange": exchange,
                        "can_market_data": False,
                        "can_trade": False,
                        "source": source,
                        "reason": "Delisted: not seen in updater run",
                        "created_at": cap.created_at,
                    },
                    bus_time=now,
                )
                deactivated += 1
        return deactivated

    @staticmethod
    def _reconcile_capabilities(
        session: Any,
        exchange: str,
        active_symbols: set[str],
        source: str,
        now: datetime,
        min_active_ratio: float = 0.5,
    ) -> int:
        """Reconcile capabilities: deactivate stale rows with safety threshold.

        Skips deactivation when the ratio of active symbols to previously
        known active capabilities is below ``min_active_ratio``, guarding
        against mass deactivation caused by partial API responses.

        Args:
            session: SQLAlchemy session.
            exchange: Exchange name (lowercase).
            active_symbols: Set of native symbols seen in the current updater run.
            source: Updater source tag.
            now: Current UTC timestamp.
            min_active_ratio: Minimum ratio of active / existing to proceed.

        Returns:
            Number of deactivated rows (0 if skipped due to safety threshold).
        """
        existing_count_stmt = select(SymbolExchangeCapability).where(
            SymbolExchangeCapability.exchange == exchange,
            SymbolExchangeCapability.timestamp <= now,
            SymbolExchangeCapability.known_to > now,
            or_(
                SymbolExchangeCapability.can_trade.is_(True),
                SymbolExchangeCapability.can_market_data.is_(True),
            ),
        )
        existing_count = len(session.execute(existing_count_stmt).scalars().all())
        if existing_count > 0:
            ratio = len(active_symbols) / existing_count
            if ratio < min_active_ratio:
                logger.warning(
                    f"Skipping capability reconciliation for {exchange}: "
                    f"active/existing ratio {ratio:.1%} < threshold {min_active_ratio:.0%} "
                    f"({len(active_symbols)} active vs {existing_count} existing)"
                )
                return 0
        deactivated = SymbolUpdaterService._deactivate_stale_capabilities(
            session, exchange, active_symbols, source, now
        )
        if deactivated > 0:
            logger.info(
                f"Reconciled {exchange} capabilities: deactivated {deactivated} stale symbols"
            )
        return deactivated

    def _get_last_update_timestamp(self) -> datetime | None:
        """Retrieve the timestamp of the last symbol mapping update from database.

        Returns:
            Datetime of last update, or None if no previous update exists.
        """
        try:
            assert self.repository is not None, "Repository not initialized"
            with self.repository.get_session() as session:
                stmt = select(Setting).where(
                    Setting.key == self._get_setting_key(), *where_active(Setting)
                )
                setting = session.execute(stmt).scalar_one_or_none()
                if setting is None or setting.value == "null" or not setting.value:
                    return None
                return datetime.fromisoformat(setting.value.replace("Z", "+00:00"))
        except Exception as e:
            logger.warning(f"Error getting last update timestamp: {e}")
            return None

    async def _set_last_update_timestamp(self, timestamp: datetime) -> None:
        """Store the timestamp of the current symbol mapping update to database.

        Args:
            timestamp: Datetime to record as the last update time.
        """
        try:
            repository = get_repository(self.settings.db_url)
            async with repository.session() as session:
                iso_timestamp = timestamp.isoformat()
                await close_and_insert(
                    session=session,
                    model=Setting,
                    match_filters=[Setting.key == self._get_setting_key()],
                    new_values={
                        "key": self._get_setting_key(),
                        "value": iso_timestamp,
                        "category": "system",
                        "description": f"Timestamp of last {self._get_setting_key()} update",
                    },
                    bus_time=timestamp,
                )
                await session.commit()
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
            self.repository = DatabaseRepository(self.settings.db_url)
            logger.info("Repository initialized")
            settings_service = await get_settings_service(
                self.settings.db_url,
                self.settings.zmq_broker_xpub,
            )
            self.settings = get_settings_with_service(settings_service)
            logger.info("AppSettings service initialized with database access")
            if not self.should_update():
                logger.info("Skipping update")
                return
            self._setup_zmq()
            client = self._create_exchange_client()
            await client.connect()
            logger.info(f"Exchange client connected: {type(client).__name__}")
            try:
                symbols = await self._fetch_symbols(client)
                await self._update_database(symbols)
                await self.broadcast_cache_invalidation()
                await self._set_last_update_timestamp(datetime.now(UTC))
                logger.info("Symbol mapping update completed successfully")
            finally:
                await client.disconnect()
                logger.info("Exchange client disconnected")
        except Exception as e:
            logger.error(f"Symbol mapping update failed: {e}")
            raise
        finally:
            self._cleanup_zmq()
            self.repository = None
