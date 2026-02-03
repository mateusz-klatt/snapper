"""Zonda market snapshot updater service.

This module provides the ZondaSnapshotUpdaterService for collecting market data
from Zonda exchange via WebSocket subscription. It subscribes to all configured
trading pairs and persists snapshots to the database.

Features:
    - WebSocket-based ticker subscription with snapshot mode
    - Configurable collection timeout
    - Automatic symbol mapping from Zonda format to native format
    - Batch persistence for efficiency

Example:
    >>> from snapper.infrastructure.market_data.zonda import run_zonda_snapshot_update
    >>> run_zonda_snapshot_update()  # Collects and saves Zonda market snapshots
"""

import asyncio
from datetime import UTC
from datetime import datetime
from typing import cast

from loguru import logger

from snapper.config.settings import get_settings
from snapper.data.models import MarketSnapshot
from snapper.data.repository import DatabaseRepository
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.implementations.zonda import ZondaExchangeClient
from snapper.infrastructure.market_data.base import MarketSnapshotUpdaterService
from snapper.infrastructure.symbols.functions import get_available_zonda_symbols
from snapper.infrastructure.symbols.functions import zonda_to_native
from snapper.utils.logging import set_log_context


class ZondaSnapshotUpdaterService(MarketSnapshotUpdaterService):
    """Market snapshot updater for Zonda exchange.

    Subscribes to Zonda WebSocket ticker feed for all configured trading pairs
    and collects market snapshots including bid/ask prices, volumes, spread,
    and 24h statistics.

    The service uses a timeout-based collection approach, gathering as many
    symbols as possible within the configured time limit.

    Attributes:
        exchange_client: Zonda exchange client for WebSocket access.
        repository: Database repository for persisting snapshots.
    """

    def __init__(
        self,
        exchange_client: ZondaExchangeClient,
        repository: DatabaseRepository,
    ) -> None:
        """Initialize the Zonda snapshot updater.

        Args:
            exchange_client: Zonda exchange client instance.
            repository: Database repository for snapshot storage.
        """
        super().__init__(exchange_client, repository)
        self.exchange_client: ZondaExchangeClient = exchange_client

    async def load_all_symbols(self) -> list[str]:
        """Load all available Zonda symbols from the mapper cache.

        Returns:
            Sorted list of Zonda symbol strings.
        """
        symbols = get_available_zonda_symbols()
        logger.info(
            f"Loaded {len(symbols)} Zonda symbols from mapper cache (sorted alphabetically)"
        )
        await asyncio.sleep(0)
        return symbols

    async def update_market_snapshots(self, **kwargs: object) -> int:
        """Fetch and persist market snapshots from Zonda.

        Subscribes to Zonda WebSocket ticker feed and collects snapshots
        until timeout or all symbols are collected.

        Args:
            **kwargs: Optional parameters.
                - timeout_seconds (int): Collection timeout in seconds (default: 30).

        Returns:
            Number of unique snapshots collected and saved.

        Raises:
            Exception: If WebSocket connection or database operation fails.
        """
        timeout_seconds = cast(int, kwargs.get("timeout_seconds", 30))
        count = 0
        logger.info(
            f"Starting Zonda market snapshots update via WebSocket (timeout: {timeout_seconds}s)..."
        )
        try:
            snapshots = await self._collect_snapshots_with_timeout(timeout_seconds)
            if snapshots:
                count = len(snapshots)
                logger.info(f"Collected {count} Zonda market snapshots")
                with self.repository.session_factory() as session:
                    session.bulk_save_objects(snapshots)
                    session.commit()
                logger.info(f"Successfully saved {count} Zonda market snapshots to database")
        except Exception as e:
            logger.error(f"Error updating Zonda market snapshots: {e}")
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
        logger.info(f"Will subscribe to {len(all_symbols)} Zonda symbols")
        try:
            async with asyncio.timeout(timeout_seconds):
                await self._collect_snapshots_loop(all_symbols, snapshots)
        except TimeoutError:
            logger.warning(
                f"WebSocket collection timed out after {timeout_seconds}s - "
                f"collected {len(snapshots)}/{len(all_symbols)} symbols"
            )
        return list(snapshots.values())

    def _resolve_native_symbol(self, zonda_symbol: str) -> str | None:
        """Resolve a Zonda symbol to its native format.

        Args:
            zonda_symbol: Exchange symbol in Zonda format.

        Returns:
            Native symbol string, or None if symbol is unknown.
        """
        try:
            return zonda_to_native(zonda_symbol)
        except ValueError:
            logger.warning(f"Unknown Zonda symbol: {zonda_symbol}")
            return None

    @staticmethod
    def _build_zonda_snapshot(native_symbol: str, ticker_data: TickerUpdate) -> MarketSnapshot:
        """Build a MarketSnapshot from Zonda ticker data.

        Args:
            native_symbol: Native symbol string.
            ticker_data: Parsed ticker update from exchange.

        Returns:
            MarketSnapshot instance populated with ticker values.
        """
        bid = ticker_data.bid
        ask = ticker_data.ask
        has_valid_prices = bid > 0 and ask > 0
        spread = ask - bid if has_valid_prices else 0.0
        mid = (bid + ask) / 2 if has_valid_prices else 0.0
        spread_pct = (spread / mid * 100) if mid > 0 else 0.0
        return MarketSnapshot(
            exchange="zonda",
            symbol=native_symbol,
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
            updated_at=datetime.now(UTC),
        )

    async def _collect_snapshots_loop(
        self, all_symbols: list[str], snapshots: dict[str, MarketSnapshot]
    ) -> None:
        """Main collection loop for Zonda ticker data.

        Processes incoming ticker messages and builds snapshot dictionary.
        Continues until all symbols are collected or caller cancels.

        Args:
            all_symbols: List of symbols to collect.
            snapshots: Dictionary to populate with snapshots (keyed by native symbol).
        """
        async for ticker_data in self.exchange_client.subscribe_ticks(all_symbols, snapshot=True):
            try:
                native_symbol = self._resolve_native_symbol(ticker_data.symbol)
                if native_symbol is None:
                    continue
                snapshots[native_symbol] = self._build_zonda_snapshot(native_symbol, ticker_data)
                if len(snapshots) % 10 == 0:
                    logger.debug(f"Collected {len(snapshots)} unique Zonda snapshots...")
                if len(snapshots) >= len(all_symbols):
                    logger.info(f"Collected all {len(snapshots)} Zonda symbols - stopping")
                    break
            except Exception as e:
                logger.warning(
                    f"Failed to process ticker for {ticker_data.symbol}: {e}",
                    exc_info=True,
                )
                continue


def run_zonda_snapshot_update() -> None:
    """Run Zonda market snapshot collection.

    Entry point for CLI command. Sets up logging context and runs the
    async snapshot collection.
    """
    set_log_context("snap:zonda")
    asyncio.run(_async_update_snapshots())


async def _async_update_snapshots() -> None:
    """Execute async Zonda market snapshot update.

    Creates exchange client and service, runs update, and ensures
    proper cleanup of connections.
    """
    logger.info("Starting Zonda market snapshot update...")
    settings = get_settings()
    repository = DatabaseRepository(settings.db_url)
    exchange_client = ZondaExchangeClient()
    try:
        await exchange_client.connect()
        service = ZondaSnapshotUpdaterService(exchange_client, repository)
        await service.start()
        logger.info("Zonda market snapshot update complete!")
    finally:
        await exchange_client.disconnect()
        logger.debug("Zonda exchange client disconnected")
