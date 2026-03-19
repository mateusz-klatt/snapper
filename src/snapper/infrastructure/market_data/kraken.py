"""Kraken market snapshot updater service.

This module provides the KrakenSnapshotUpdaterService for collecting real-time
market data from Kraken exchange via WebSocket subscription. It subscribes to
all available tickers and persists snapshots to the database.

Features:
    - WebSocket-based real-time ticker subscription
    - Automatic deduplication (keeps latest snapshot per symbol)
    - Batch persistence for efficiency
    - Configurable snapshot collection limit

Example:
    >>> from snapper.infrastructure.market_data.kraken import run_snapshot_update
    >>> run_snapshot_update()  # Collects and saves market snapshots
"""

import asyncio
from datetime import UTC
from datetime import datetime

from loguru import logger

from snapper.config.settings import get_settings
from snapper.data.models import MarketSnapshot
from snapper.data.repository import DatabaseRepository
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.market_data.base import MarketSnapshotUpdaterService
from snapper.utils.logging import set_log_context


class KrakenSnapshotUpdaterService(MarketSnapshotUpdaterService):
    """Market snapshot updater for Kraken exchange.

    Subscribes to Kraken WebSocket ticker feed for all trading pairs and
    collects market snapshots including bid/ask prices, volumes, spread,
    and 24h statistics.

    The service collects up to 2000 ticker updates, deduplicates by symbol
    (keeping the latest), and bulk-saves to the database.

    Attributes:
        exchange_client: Kraken exchange client for WebSocket access.
        repository: Database repository for persisting snapshots.
    """

    def __init__(
        self, exchange_client: KrakenExchangeClient, repository: DatabaseRepository
    ) -> None:
        """Initialize the Kraken snapshot updater.

        Args:
            exchange_client: Kraken exchange client instance.
            repository: Database repository for snapshot storage.
        """
        super().__init__(exchange_client, repository)
        self.exchange_client: KrakenExchangeClient = exchange_client

    @staticmethod
    def _build_kraken_snapshot(
        ticker_data: TickerUpdate,
        session_id: str,
        sequence_id: int,
    ) -> MarketSnapshot:
        """Build a MarketSnapshot from Kraken ticker data.

        Args:
            ticker_data: Parsed ticker update from exchange.
            session_id: Session identifier for provenance stamping.
            sequence_id: Sequence number for provenance stamping.

        Returns:
            MarketSnapshot instance populated with ticker values.
        """
        bid = ticker_data.bid
        ask = ticker_data.ask
        spread = ask - bid
        mid = (bid + ask) / 2
        spread_pct = (ask - bid) / mid * 100 if mid > 0 else 0.0
        return MarketSnapshot(
            exchange="kraken",
            symbol=ticker_data.symbol,
            bid=bid,
            bid_volume=ticker_data.bid_qty,
            ask=ask,
            ask_volume=ticker_data.ask_qty,
            last_price=ticker_data.last,
            volume_24h=ticker_data.volume,
            vwap_24h=ticker_data.vwap,
            low_24h=ticker_data.low,
            high_24h=ticker_data.high,
            change_24h=ticker_data.change,
            spread=spread,
            spread_pct=spread_pct,
            timestamp=datetime.now(UTC),
            session_id=session_id,
            sequence_id=sequence_id,
        )

    _COLLECTION_TIMEOUT_SECONDS = 120.0

    async def _collect_ticker_snapshots(self) -> tuple[list[MarketSnapshot], int]:
        """Collect ticker snapshots from Kraken WebSocket feed.

        Subscribes to all tickers and collects up to 2000 updates.
        Enforces a timeout so the process exits even when unmapped
        symbols prevent the counter from reaching the target.

        Returns:
            Tuple of (collected snapshots list, total count).
        """
        snapshots_batch: list[MarketSnapshot] = []
        count = 0
        try:
            async with asyncio.timeout(self._COLLECTION_TIMEOUT_SECONDS):
                async for ticker_data in self.exchange_client.subscribe_ticks(["*"]):
                    if not ticker_data.symbol:
                        continue
                    snapshots_batch.append(
                        self._build_kraken_snapshot(
                            ticker_data,
                            session_id=self._tracker.session_id,
                            sequence_id=self._tracker.next_sequence("snapshots"),
                        )
                    )
                    count += 1
                    if count % 100 == 0:
                        logger.debug(f"Collected {count} market snapshots...")
                    if count >= 2000:
                        logger.info(f"Collected {count} market snapshots - stopping")
                        break
        except TimeoutError:
            logger.warning(
                f"Snapshot collection timed out after {self._COLLECTION_TIMEOUT_SECONDS}s "
                f"with {count} snapshots collected"
            )
        finally:
            await self.exchange_client.disconnect_websocket()
        return snapshots_batch, count

    def _deduplicate_and_persist(self, snapshots_batch: list[MarketSnapshot]) -> None:
        """Deduplicate snapshots by symbol and persist to database.

        Args:
            snapshots_batch: List of collected snapshots (may contain duplicates).
        """
        unique_snapshots: dict[str, MarketSnapshot] = {}
        for snapshot in snapshots_batch:
            unique_snapshots[snapshot.symbol] = snapshot
        final_snapshots = list(unique_snapshots.values())
        logger.info(
            f"Collected {len(snapshots_batch)} snapshots, {len(final_snapshots)} unique symbols"
        )
        with self.repository.session_factory() as session:
            session.bulk_save_objects(final_snapshots)
            session.commit()
        logger.info(f"Successfully saved {len(final_snapshots)} market snapshots to database")

    async def update_market_snapshots(self, **kwargs: object) -> int:
        """Fetch and persist market snapshots from Kraken.

        Subscribes to all Kraken tickers via WebSocket, collects up to 2000
        updates, deduplicates by symbol, and saves to database.

        Args:
            **kwargs: Unused, present for interface compatibility.

        Returns:
            Total number of ticker updates received.

        Raises:
            Exception: If WebSocket connection or database operation fails.
        """
        logger.info("Starting market snapshots update from WebSocket ticker ['*']...")
        try:
            snapshots_batch, count = await self._collect_ticker_snapshots()
            if snapshots_batch:
                self._deduplicate_and_persist(snapshots_batch)
            return count
        except Exception as e:
            logger.error(f"Error updating market snapshots: {e}")
            raise


def run_snapshot_update() -> None:
    """Run Kraken market snapshot collection.

    Entry point for CLI command. Sets up logging context and runs the
    async snapshot collection.
    """
    set_log_context("snap:kraken")
    asyncio.run(_async_update_snapshots())


async def _async_update_snapshots() -> None:
    """Execute async market snapshot update.

    Creates exchange client and service, runs update, and ensures
    proper cleanup of connections.
    """
    logger.info("Starting market snapshot update...")
    settings = get_settings()
    repository = DatabaseRepository(settings.db_url)
    exchange_client = KrakenExchangeClient()
    try:
        service = KrakenSnapshotUpdaterService(exchange_client, repository)
        await service.start()
        logger.info("Market snapshot update complete!")
    finally:
        await exchange_client.disconnect()
        logger.debug("Exchange client disconnected")
