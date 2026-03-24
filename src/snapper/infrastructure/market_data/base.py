"""Abstract base class for market snapshot updater services.

This module defines the interface for exchange-specific market data collectors.
Each exchange implementation inherits from MarketSnapshotUpdaterService and
implements the update_market_snapshots method.

The updater services are responsible for:
    - Fetching current market data from exchange APIs
    - Transforming data to internal format
    - Resolving native symbols to instrument_public_id (2-hop via Symbol/Instrument)
    - Persisting snapshots via SCD2 close+insert
"""

from abc import ABC
from abc import abstractmethod
from datetime import datetime
from typing import Any

from loguru import logger
from sqlalchemy import select

from snapper.data.models import Instrument
from snapper.data.models import MarketSnapshot
from snapper.data.models import Symbol
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import close_and_insert_sync
from snapper.messaging.infrastructure.publisher import SequenceTracker


class MarketSnapshotUpdaterService(ABC):
    """Abstract base class for market snapshot collection services.

    Provides common infrastructure for exchange-specific snapshot updaters
    including instrument resolution and SCD2 persistence.

    Subclasses must implement the update_market_snapshots method to fetch
    and persist market data.

    Attributes:
        exchange_client: Exchange-specific client for API access.
        repository: Database repository for persisting snapshots.
    """

    def __init__(self, exchange_client: object, repository: DatabaseRepository) -> None:
        """Initialize the updater service.

        Args:
            exchange_client: Exchange-specific API client instance.
            repository: Database repository for storing snapshots.
        """
        self.exchange_client = exchange_client
        self.repository = repository
        self._tracker: SequenceTracker = SequenceTracker()

    @abstractmethod
    async def update_market_snapshots(self, **kwargs: object) -> int:
        """Fetch and persist market snapshots.

        Must be implemented by subclasses to handle exchange-specific
        data fetching and transformation.

        Args:
            **kwargs: Exchange-specific parameters (e.g., symbols, timeframes).

        Returns:
            Number of snapshots successfully updated.
        """
        ...

    def _resolve_instrument_public_id(
        self, native_symbol: str, exchange: str, as_of: datetime
    ) -> str | None:
        """Resolve native_symbol to instrument_public_id via 2-hop lookup.

        Performs Symbol(native_symbol) -> Symbol.public_id, then
        Instrument(symbol_public_id, exchange) -> Instrument.public_id.

        Args:
            native_symbol: Native symbol string (e.g. 'BTC-USD').
            exchange: Exchange identifier (lowercase, e.g. 'kraken').
            as_of: Point-in-time for temporal query.

        Returns:
            Instrument public_id string, or None if resolution fails.
        """
        now = as_of
        with self.repository.session_factory() as session:
            sym_row = (
                session.execute(
                    select(Symbol.public_id).where(
                        Symbol.native_symbol == native_symbol,
                        Symbol.timestamp <= now,
                        Symbol.known_to > now,
                    )
                )
                .scalars()
                .first()
            )
            if sym_row is None:
                return None
            inst_row = (
                session.execute(
                    select(Instrument.public_id).where(
                        Instrument.symbol_public_id == sym_row,
                        Instrument.exchange == exchange,
                        Instrument.timestamp <= now,
                        Instrument.known_to > now,
                    )
                )
                .scalars()
                .first()
            )
            return inst_row

    def _resolve_batch_instrument_ids(
        self, native_symbols: set[str], exchange: str, as_of: datetime
    ) -> dict[str, str]:
        """Resolve a batch of native symbols to instrument_public_id.

        Args:
            native_symbols: Set of native symbol strings.
            exchange: Exchange identifier (lowercase).
            as_of: Point-in-time for temporal query.

        Returns:
            Mapping of native_symbol -> instrument_public_id for successful lookups.
        """
        now = as_of
        result: dict[str, str] = {}
        with self.repository.session_factory() as session:
            for ns in native_symbols:
                sym_pid = (
                    session.execute(
                        select(Symbol.public_id).where(
                            Symbol.native_symbol == ns,
                            Symbol.timestamp <= now,
                            Symbol.known_to > now,
                        )
                    )
                    .scalars()
                    .first()
                )
                if sym_pid is None:
                    logger.warning(f"No active Symbol found for native_symbol={ns}")
                    continue
                inst_pid = (
                    session.execute(
                        select(Instrument.public_id).where(
                            Instrument.symbol_public_id == sym_pid,
                            Instrument.exchange == exchange,
                            Instrument.timestamp <= now,
                            Instrument.known_to > now,
                        )
                    )
                    .scalars()
                    .first()
                )
                if inst_pid is None:
                    logger.warning(
                        f"No active Instrument found for symbol_public_id={sym_pid}, "
                        f"exchange={exchange}"
                    )
                    continue
                result[ns] = inst_pid
        return result

    def _persist_snapshots_scd2(self, snapshots: list[MarketSnapshot]) -> int:
        """Persist snapshots using SCD2 close+insert pattern.

        For each snapshot, closes the existing active row for the same
        instrument_public_id and inserts a new version.

        Args:
            snapshots: List of MarketSnapshot instances with instrument_public_id set.

        Returns:
            Number of snapshots persisted.
        """
        if not snapshots:
            return 0
        with self.repository.session_factory() as session:
            count = 0
            for snap in snapshots:
                bus_time = snap.timestamp
                new_values: dict[str, Any] = {
                    "instrument_public_id": snap.instrument_public_id,
                    "bid": snap.bid,
                    "bid_volume": snap.bid_volume,
                    "ask": snap.ask,
                    "ask_volume": snap.ask_volume,
                    "last_price": snap.last_price,
                    "volume_24h": snap.volume_24h,
                    "vwap_24h": snap.vwap_24h,
                    "low_24h": snap.low_24h,
                    "high_24h": snap.high_24h,
                    "change_24h": snap.change_24h,
                    "spread": snap.spread,
                    "spread_pct": snap.spread_pct,
                    "session_id": snap.session_id,
                    "sequence_id": snap.sequence_id,
                }
                close_and_insert_sync(
                    session=session,
                    model=MarketSnapshot,
                    match_filters=[
                        MarketSnapshot.instrument_public_id == snap.instrument_public_id,
                    ],
                    new_values=new_values,
                    bus_time=bus_time,
                )
                count += 1
            session.commit()
        return count

    async def start(self) -> None:
        """Execute a single snapshot update cycle.

        Convenience method that logs the update process and reports
        the number of snapshots collected.
        """
        logger.info(f"Starting {self.__class__.__name__}...")
        count = await self.update_market_snapshots()
        logger.info(f"{self.__class__.__name__} completed - updated {count} snapshots")
