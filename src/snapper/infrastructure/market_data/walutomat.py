"""Walutomat market snapshot updater service.

This module provides the WalutomatSnapshotUpdaterService for collecting market
data from Walutomat exchange via REST API polling. It iterates through all
supported forex pairs and persists snapshots to the database.

Features:
    - REST API-based ticker polling
    - Configurable collection timeout (default: 12 seconds)
    - Automatic symbol discovery from exchange API
    - Batch persistence for efficiency

Example:
    >>> from snapper.infrastructure.market_data.walutomat import (
    ...     run_walutomat_snapshot_update,
    ... )
    >>> run_walutomat_snapshot_update()  # Collects and saves Walutomat snapshots
"""

import asyncio
from datetime import UTC
from datetime import datetime
from typing import cast

from loguru import logger

from snapper.config.settings import get_settings
from snapper.data.models import MarketSnapshot
from snapper.data.repository import DatabaseRepository
from snapper.infrastructure.exchanges.implementations.walutomat import WalutomatExchangeClient
from snapper.infrastructure.market_data.base import MarketSnapshotUpdaterService
from snapper.utils.logging import set_log_context


class WalutomatSnapshotUpdaterService(MarketSnapshotUpdaterService):
    """Market snapshot updater for Walutomat exchange.

    Collects forex market data from Walutomat using REST API polling.
    Unlike WebSocket-based updaters, this polls the ticker endpoint
    sequentially for each supported currency pair.

    Attributes:
        exchange_client: Walutomat exchange client for API access.
        repository: Database repository for persisting snapshots.
    """

    def __init__(
        self,
        exchange_client: WalutomatExchangeClient,
        repository: DatabaseRepository,
    ) -> None:
        """Initialize the Walutomat snapshot updater.

        Args:
            exchange_client: Walutomat exchange client instance.
            repository: Database repository for snapshot storage.
        """
        super().__init__(exchange_client, repository)
        self.exchange_client: WalutomatExchangeClient = exchange_client

    async def load_all_symbols(self) -> list[str]:
        """Load all supported Walutomat symbols.

        Returns:
            Sorted list of native symbol strings.
        """
        symbols = self.exchange_client.get_supported_pairs()
        logger.info(f"Loaded {len(symbols)} Walutomat symbols from API (sorted alphabetically)")
        return sorted(symbols)

    async def update_market_snapshots(self, **kwargs: object) -> int:
        """Fetch and persist market snapshots from Walutomat.

        Polls Walutomat API for ticker data and collects snapshots
        until timeout or all symbols are collected.

        Args:
            **kwargs: Optional parameters.
                - timeout_seconds (int): Collection timeout in seconds (default: 12).

        Returns:
            Number of unique snapshots collected and saved.

        Raises:
            Exception: If API connection or database operation fails.
        """
        timeout_seconds = cast(int, kwargs.get("timeout_seconds", 12))
        count = 0
        logger.info(
            f"Starting Walutomat market snapshots update via polling "
            f"(timeout: {timeout_seconds}s)..."
        )
        try:
            snapshots = await self._collect_snapshots_with_timeout(timeout_seconds)
            if snapshots:
                count = len(snapshots)
                logger.info(f"Collected {count} Walutomat market snapshots")
                with self.repository.session_factory() as session:
                    session.bulk_save_objects(snapshots)
                    session.commit()
                logger.info(f"Successfully saved {count} Walutomat market snapshots to database")
        except Exception as e:
            logger.error(f"Error updating Walutomat market snapshots: {e}")
            raise
        return count

    async def _collect_snapshots_with_timeout(self, timeout_seconds: int) -> list[MarketSnapshot]:
        """Collect snapshots with a timeout limit.

        Args:
            timeout_seconds: Maximum time to spend collecting snapshots.

        Returns:
            List of collected MarketSnapshot objects.
        """
        snapshots: dict[str, MarketSnapshot] = {}
        all_symbols = await self.load_all_symbols()
        logger.info(f"Will subscribe to {len(all_symbols)} Walutomat symbols")
        try:
            async with asyncio.timeout(timeout_seconds):
                await self._collect_snapshots_loop(all_symbols, snapshots)
        except TimeoutError:
            logger.warning(
                f"Polling collection timed out after {timeout_seconds}s - "
                f"collected {len(snapshots)}/{len(all_symbols)} symbols"
            )
        return list(snapshots.values())

    async def _collect_snapshots_loop(
        self, all_symbols: list[str], snapshots: dict[str, MarketSnapshot]
    ) -> None:
        """Main collection loop for Walutomat ticker data.

        Processes incoming ticker messages and builds snapshot dictionary.
        Continues until all symbols are collected or caller cancels.

        Args:
            all_symbols: List of symbols to collect.
            snapshots: Dictionary to populate with snapshots (keyed by native symbol).
        """
        async for ticker_data in self.exchange_client.subscribe_ticks(all_symbols):
            try:
                native_symbol = ticker_data.symbol
                bid = ticker_data.bid
                ask = ticker_data.ask
                last_price = ticker_data.last
                high_24h = ticker_data.high
                low_24h = ticker_data.low
                volume_24h = ticker_data.volume
                change_24h = ticker_data.change
                vwap_24h = ticker_data.vwap
                bid_volume = ticker_data.bid_qty
                ask_volume = ticker_data.ask_qty
                spread = ask - bid if (bid > 0 and ask > 0) else 0.0
                mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else 0.0
                spread_pct = (spread / mid * 100) if mid > 0 else 0.0
                snapshot = MarketSnapshot(
                    exchange="walutomat",
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
                snapshots[native_symbol] = snapshot
                if len(snapshots) % 10 == 0:
                    logger.debug(f"Collected {len(snapshots)} unique Walutomat snapshots...")
                if len(snapshots) >= len(all_symbols):
                    logger.info(f"Collected all {len(snapshots)} Walutomat symbols - stopping")
                    break
            except Exception as e:
                logger.warning(f"Error processing Walutomat ticker: {e}")
                continue


def run_walutomat_snapshot_update() -> None:
    """Run Walutomat market snapshot collection.

    Entry point for CLI command. Sets up logging context and runs the
    async snapshot collection.
    """
    set_log_context("snap:walutomat")
    asyncio.run(_async_update_snapshots())


async def _async_update_snapshots() -> None:
    """Execute async Walutomat market snapshot update.

    Creates exchange client and service, runs update, and ensures
    proper cleanup of connections.
    """
    logger.info("Starting Walutomat market snapshot update...")
    settings = get_settings()
    repository = DatabaseRepository(settings.db_url)
    exchange_client = WalutomatExchangeClient()
    try:
        await exchange_client.connect()
        service = WalutomatSnapshotUpdaterService(exchange_client, repository)
        await service.start()
        logger.info("Walutomat market snapshot update complete!")
    finally:
        await exchange_client.disconnect()
        logger.debug("Walutomat exchange client disconnected")
