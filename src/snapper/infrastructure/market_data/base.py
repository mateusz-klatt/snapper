"""Abstract base class for market snapshot updater services.

This module defines the interface for exchange-specific market data collectors.
Each exchange implementation inherits from MarketSnapshotUpdaterService and
implements the update_market_snapshots method.

The updater services are responsible for:
    - Fetching current market data from exchange APIs
    - Transforming data to internal format
    - Persisting snapshots to the database
"""

from abc import ABC
from abc import abstractmethod

from loguru import logger

from snapper.data.repository import DatabaseRepository
from snapper.messaging.infrastructure.publisher import SequenceTracker


class MarketSnapshotUpdaterService(ABC):
    """Abstract base class for market snapshot collection services.

    Provides common infrastructure for exchange-specific snapshot updaters.
    Subclasses must implement the update_market_snapshots method to fetch
    and persist market data.

    Attributes:
        exchange_client: Exchange-specific client for API access.
        repository: Database repository for persisting snapshots.

    Example:
        >>> class MyExchangeUpdater(MarketSnapshotUpdaterService):
        ...     async def update_market_snapshots(self, **kwargs) -> int:
        ...         # Fetch and save snapshots
        ...         return count
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

    async def start(self) -> None:
        """Execute a single snapshot update cycle.

        Convenience method that logs the update process and reports
        the number of snapshots collected.
        """
        logger.info(f"Starting {self.__class__.__name__}...")
        count = await self.update_market_snapshots()
        logger.info(f"{self.__class__.__name__} completed - updated {count} snapshots")
