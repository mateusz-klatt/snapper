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
        count = 0
        logger.info("Starting market snapshots update from WebSocket ticker ['*']...")
        snapshots_batch: list[MarketSnapshot] = []
        try:

            async def collect_snapshots() -> None:
                nonlocal count
                try:
                    async for ticker_data in self.exchange_client.subscribe_ticks(["*"]):
                        native_symbol = ticker_data.symbol
                        if not native_symbol:
                            continue
                        bid = ticker_data.bid
                        ask = ticker_data.ask
                        last_price = ticker_data.last
                        volume_24h = ticker_data.volume
                        vwap_24h = ticker_data.vwap
                        low_24h = ticker_data.low
                        high_24h = ticker_data.high
                        change_24h = ticker_data.change
                        bid_volume = ticker_data.bid_qty
                        ask_volume = ticker_data.ask_qty
                        spread = ask - bid
                        mid = (bid + ask) / 2
                        spread_pct = (ask - bid) / mid * 100 if mid > 0 else 0.0
                        snapshot = MarketSnapshot(
                            exchange="kraken",
                            symbol=native_symbol,
                            bid=bid,
                            bid_volume=bid_volume,
                            ask=ask,
                            ask_volume=ask_volume,
                            last_price=last_price,
                            volume_24h=volume_24h,
                            vwap_24h=vwap_24h,
                            low_24h=low_24h,
                            high_24h=high_24h,
                            change_24h=change_24h,
                            spread=spread,
                            spread_pct=spread_pct,
                            updated_at=datetime.now(UTC),
                        )
                        snapshots_batch.append(snapshot)
                        count += 1
                        if count % 100 == 0:
                            logger.debug(f"Collected {count} market snapshots...")
                        if count >= 2000:
                            logger.info(f"Collected {count} market snapshots - stopping")
                            break
                finally:
                    await self.exchange_client.disconnect_websocket()

            await collect_snapshots()
            if snapshots_batch:
                unique_snapshots: dict[str, MarketSnapshot] = {}
                for snapshot in snapshots_batch:
                    unique_snapshots[snapshot.symbol] = snapshot
                final_snapshots = list(unique_snapshots.values())
                logger.info(
                    f"Collected {len(snapshots_batch)} snapshots, "
                    f"{len(final_snapshots)} unique symbols"
                )
                with self.repository.session_factory() as session:
                    session.bulk_save_objects(final_snapshots)
                    session.commit()
                logger.info(
                    f"Successfully saved {len(final_snapshots)} market snapshots to database"
                )
        except Exception as e:
            logger.error(f"Error updating market snapshots: {e}")
            raise
        return count


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
