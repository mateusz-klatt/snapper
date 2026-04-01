"""Kraken Futures market snapshot updater service.

This module provides the KrakenFuturesSnapshotUpdaterService for collecting
market data from Kraken Futures exchange via WebSocket subscription. It
subscribes to all configured crypto futures pairs and persists snapshots
to the database.

Features:
    - WebSocket-based ticker subscription
    - Configurable collection timeout (default 60s for fewer symbols)
    - Automatic symbol resolution (adapter converts internally)
    - Instrument resolution via Symbol/Instrument 2-hop lookup
    - SCD2 close+insert persistence
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
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.market_data.base import MarketSnapshotUpdaterService
from snapper.infrastructure.symbols.functions import get_available_kraken_futures_symbols
from snapper.utils.logging import set_log_context

EXCHANGE_NAME = "kraken_futures"


class KrakenFuturesSnapshotUpdaterService(MarketSnapshotUpdaterService):
    """Market snapshot updater for Kraken Futures exchange.

    Subscribes to Kraken Futures WebSocket ticker feed for all configured
    crypto futures pairs and collects market snapshots including bid/ask
    prices, volumes, spread, and 24h statistics.

    The service uses a timeout-based collection approach, gathering as many
    symbols as possible within the configured time limit. Instrument resolution
    is performed after collection, mapping native symbols to instrument_public_id.

    Attributes:
        exchange_client: Kraken Futures exchange client for WebSocket access.
        repository: Database repository for persisting snapshots.
    """

    def __init__(
        self,
        exchange_client: KrakenFuturesExchangeClient,
        repository: DatabaseRepository,
    ) -> None:
        """Initialize the Kraken Futures snapshot updater.

        Args:
            exchange_client: Kraken Futures exchange client instance.
            repository: Database repository for snapshot storage.
        """
        super().__init__(exchange_client, repository)
        self.exchange_client: KrakenFuturesExchangeClient = exchange_client

    async def load_all_symbols(self) -> list[str]:
        """Load all available Kraken Futures symbols from the mapper cache.

        Returns:
            Sorted list of Kraken Futures symbol strings.
        """
        symbols = get_available_kraken_futures_symbols()
        logger.info(
            f"Loaded {len(symbols)} Kraken Futures symbols from mapper cache (sorted alphabetically)"
        )
        await asyncio.sleep(0)
        return symbols

    async def update_market_snapshots(self, **kwargs: object) -> int:
        """Fetch and persist market snapshots from Kraken Futures.

        Subscribes to Kraken Futures WebSocket ticker feed, collects snapshots,
        resolves instrument_public_id, and persists via SCD2 close+insert.

        Args:
            **kwargs: Optional parameters.
                - timeout_seconds (int): Collection timeout in seconds (default: 60).

        Returns:
            Number of unique snapshots collected and saved.

        Raises:
            Exception: If WebSocket connection or database operation fails.
        """
        timeout_seconds = cast(int, kwargs.get("timeout_seconds", 60))
        count = 0
        logger.info(
            f"Starting Kraken Futures market snapshots update via WebSocket "
            f"(timeout: {timeout_seconds}s)..."
        )
        try:
            raw_snapshots = await self._collect_snapshots_with_timeout(timeout_seconds)
            if raw_snapshots:
                native_symbols = set(raw_snapshots.keys())
                now = datetime.now(UTC)
                symbol_to_inst = self._resolve_batch_instrument_ids(
                    native_symbols, EXCHANGE_NAME, as_of=now
                )
                snapshots: list[MarketSnapshot] = []
                skipped = 0
                for symbol, snap in raw_snapshots.items():
                    inst_pid = symbol_to_inst.get(symbol)
                    if inst_pid is None:
                        skipped += 1
                        continue
                    snap.instrument_public_id = inst_pid
                    snapshots.append(snap)
                if skipped > 0:
                    logger.warning(f"Skipped {skipped} symbols with no instrument resolution")
                count = self._persist_snapshots_scd2(snapshots)
                logger.info(
                    f"Successfully saved {count} Kraken Futures market snapshots to database"
                )
        except Exception as e:
            logger.error(f"Error updating Kraken Futures market snapshots: {e}")
            raise
        return count

    async def _collect_snapshots_with_timeout(
        self, timeout_seconds: int
    ) -> dict[str, MarketSnapshot]:
        """Collect snapshots with a timeout limit.

        Snapshots are keyed by native_symbol. The instrument_public_id
        field is set to a placeholder and must be resolved after collection.

        Args:
            timeout_seconds: Maximum time to spend collecting snapshots.

        Returns:
            Dict mapping native_symbol to MarketSnapshot (unresolved).
        """
        snapshots: dict[str, MarketSnapshot] = {}
        all_symbols = await self.load_all_symbols()
        logger.info(f"Will subscribe to {len(all_symbols)} Kraken Futures symbols")
        try:
            async with asyncio.timeout(timeout_seconds):
                await self._collect_snapshots_loop(all_symbols, snapshots)
        except TimeoutError:
            logger.warning(
                f"WebSocket collection timed out after {timeout_seconds}s - "
                f"collected {len(snapshots)}/{len(all_symbols)} symbols"
            )
        return snapshots

    def _resolve_native_symbol(self, symbol: str) -> str | None:
        """Resolve a Kraken Futures symbol to its native format.

        The Kraken Futures adapter converts symbols internally, so
        ticker_data.symbol is already in native format.

        Args:
            symbol: Symbol string from ticker data (already native).

        Returns:
            The symbol string unchanged (already native format).
        """
        return symbol

    @staticmethod
    def _build_snapshot(
        native_symbol: str,
        ticker_data: TickerUpdate,
        session_id: str,
        sequence_id: int,
    ) -> MarketSnapshot:
        """Build a MarketSnapshot from Kraken Futures ticker data.

        The instrument_public_id is set to a placeholder value that must
        be resolved after collection via _resolve_batch_instrument_ids.

        Args:
            native_symbol: Native symbol string (used as temporary key).
            ticker_data: Parsed ticker update from exchange.
            session_id: Session identifier for provenance stamping.
            sequence_id: Sequence number for provenance stamping.

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
            instrument_public_id=native_symbol,
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

    async def _collect_snapshots_loop(
        self, all_symbols: list[str], snapshots: dict[str, MarketSnapshot]
    ) -> None:
        """Main collection loop for Kraken Futures ticker data.

        Processes incoming ticker messages and builds snapshot dictionary
        keyed by native symbol. The instrument_public_id is set to the
        native_symbol as a placeholder and resolved later in batch.

        Args:
            all_symbols: List of symbols to collect.
            snapshots: Dictionary to populate with snapshots (keyed by native symbol).
        """
        async for ticker_data in self.exchange_client.subscribe_ticks(all_symbols):
            try:
                native_symbol = self._resolve_native_symbol(ticker_data.symbol)
                if native_symbol is None:
                    continue
                snapshots[native_symbol] = self._build_snapshot(
                    native_symbol,
                    ticker_data,
                    session_id=self._tracker.session_id,
                    sequence_id=self._tracker.next_sequence("snapshots"),
                )
                if len(snapshots) % 10 == 0:
                    logger.debug(f"Collected {len(snapshots)} unique Kraken Futures snapshots...")
                if len(snapshots) >= len(all_symbols):
                    logger.info(f"Collected all {len(snapshots)} Kraken Futures symbols - stopping")
                    break
            except Exception as e:
                logger.warning(
                    f"Failed to process ticker for {ticker_data.symbol}: {e}",
                    exc_info=True,
                )
                continue


def run_kraken_futures_snapshot_update() -> None:
    """Run Kraken Futures market snapshot collection.

    Entry point for CLI command. Sets up logging context and runs the
    async snapshot collection.
    """
    set_log_context("snap:kraken_futures")
    asyncio.run(_async_update_snapshots())


async def _async_update_snapshots() -> None:
    """Execute async Kraken Futures market snapshot update.

    Creates exchange client and service, runs update, and ensures
    proper cleanup of connections.
    """
    logger.info("Starting Kraken Futures market snapshot update...")
    settings = get_settings()
    repository = DatabaseRepository(settings.db_url)
    exchange_client = KrakenFuturesExchangeClient()
    try:
        await exchange_client.connect()
        service = KrakenFuturesSnapshotUpdaterService(exchange_client, repository)
        await service.start()
        logger.info("Kraken Futures market snapshot update complete!")
    finally:
        await exchange_client.disconnect()
        logger.debug("Kraken Futures exchange client disconnected")
