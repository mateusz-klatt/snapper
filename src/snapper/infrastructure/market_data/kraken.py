"""Kraken market snapshot updater service.

This module provides the KrakenSnapshotUpdaterService for collecting real-time
market data from Kraken exchange via WebSocket subscription. It subscribes to
all available tickers and persists snapshots to the database.

Features:
    - WebSocket-based real-time ticker subscription
    - Automatic deduplication (keeps latest snapshot per instrument)
    - Instrument resolution via Symbol/Instrument 2-hop lookup
    - SCD2 close+insert persistence
    - Configurable snapshot collection limit
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

EXCHANGE_NAME = "kraken"


class KrakenSnapshotUpdaterService(MarketSnapshotUpdaterService):
    """Market snapshot updater for Kraken exchange.

    Subscribes to Kraken WebSocket ticker feed for all trading pairs and
    collects market snapshots including bid/ask prices, volumes, spread,
    and 24h statistics.

    The service collects up to 2000 ticker updates, deduplicates by
    instrument_public_id (keeping the latest), and persists via SCD2
    close+insert.

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
        instrument_public_id: str,
        session_id: str,
        sequence_id: int,
    ) -> MarketSnapshot:
        """Build a MarketSnapshot from Kraken ticker data.

        Args:
            ticker_data: Parsed ticker update from exchange.
            instrument_public_id: Resolved instrument public identifier.
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
            instrument_public_id=instrument_public_id,
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

    async def _collect_ticker_snapshots(
        self,
    ) -> tuple[list[tuple[str, TickerUpdate]], int]:
        """Collect ticker snapshots from Kraken WebSocket feed.

        Subscribes to all tickers and collects up to 2000 updates.
        Enforces a timeout so the process exits even when unmapped
        symbols prevent the counter from reaching the target.

        Returns:
            Tuple of (collected (symbol, ticker) pairs, total count).
        """
        raw_batch: list[tuple[str, TickerUpdate]] = []
        count = 0
        try:
            async with asyncio.timeout(self._COLLECTION_TIMEOUT_SECONDS):
                async for ticker_data in self.exchange_client.subscribe_ticks(["*"]):
                    if not ticker_data.symbol:
                        continue
                    raw_batch.append((ticker_data.symbol, ticker_data))
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
        return raw_batch, count

    def _deduplicate_and_persist(self, raw_batch: list[tuple[str, TickerUpdate]]) -> None:
        """Resolve instruments, deduplicate by instrument_public_id, and persist.

        Args:
            raw_batch: List of (native_symbol, ticker_data) pairs.
        """
        unique_by_symbol: dict[str, TickerUpdate] = dict(raw_batch)

        native_symbols = set(unique_by_symbol.keys())
        now = datetime.now(UTC)
        symbol_to_inst = self._resolve_batch_instrument_ids(
            native_symbols, EXCHANGE_NAME, as_of=now
        )

        snapshots: list[MarketSnapshot] = []
        skipped = 0
        for symbol, ticker in unique_by_symbol.items():
            inst_pid = symbol_to_inst.get(symbol)
            if inst_pid is None:
                skipped += 1
                continue
            snapshots.append(
                self._build_kraken_snapshot(
                    ticker,
                    instrument_public_id=inst_pid,
                    session_id=self._tracker.session_id,
                    sequence_id=self._tracker.next_sequence("snapshots"),
                )
            )

        if skipped > 0:
            logger.warning(f"Skipped {skipped} symbols with no instrument resolution")

        logger.info(
            f"Collected {len(raw_batch)} snapshots, "
            f"{len(unique_by_symbol)} unique symbols, "
            f"{len(snapshots)} resolved instruments"
        )
        count = self._persist_snapshots_scd2(snapshots)
        logger.info(f"Successfully saved {count} market snapshots to database")

    async def update_market_snapshots(self, **kwargs: object) -> int:
        """Fetch and persist market snapshots from Kraken.

        Subscribes to all Kraken tickers via WebSocket, collects up to 2000
        updates, resolves instruments, deduplicates, and saves via SCD2.

        Args:
            **kwargs: Unused, present for interface compatibility.

        Returns:
            Total number of ticker updates received.

        Raises:
            Exception: If WebSocket connection or database operation fails.
        """
        logger.info("Starting market snapshots update from WebSocket ticker ['*']...")
        try:
            raw_batch, count = await self._collect_ticker_snapshots()
            if raw_batch:
                self._deduplicate_and_persist(raw_batch)
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
