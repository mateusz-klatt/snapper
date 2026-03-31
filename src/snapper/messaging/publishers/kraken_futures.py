"""Kraken Futures exchange market data publisher.

This module provides a market data feed publisher for the Kraken Futures
exchange. It streams real-time ticks and trades via Kraken's Futures
WebSocket API and publishes normalized data to the ZMQ messaging bus.

Candles are not available via WebSocket for Kraken Futures.
The publisher's candle loop should be overridden to use REST OHLCV polling
or disabled entirely.

Configuration
-------------
Symbols are configured via settings.instruments["kraken_futures"].
The publisher uses public (anonymous) WebSocket connections.
"""

from typing import Any

from loguru import logger

from snapper.application.process_manager.process_parameters import PublisherSymbolsParameters
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.core.types import ExchangeEnum
from snapper.core.types import MarketDataExchange
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.symbols.functions import native_to_kraken_futures_ws
from snapper.messaging.publishers.base import MarketDataPublisherService


@register_process(
    "kraken_futures_feed_publisher",
    description="Kraken Futures market data feed publisher",
    priority=20,
    role=ProcessRoleEnum.CORE,
    tags=("market-data", "publisher", "kraken_futures"),
    parameters_model=PublisherSymbolsParameters,
    enabled=False,
    mode=ProcessModeEnum.THREAD,
)
class KrakenFuturesMarketDataPublisher(MarketDataPublisherService[KrakenFuturesExchangeClient]):
    """Kraken Futures exchange market data publisher.

    Streams real-time market data from Kraken Futures WebSocket API and
    publishes normalized messages to ZMQ. Handles symbol conversion between
    native format (BTC-USD-PERP) and Kraken Futures product IDs (PF_XBTUSD).

    Topics Published:
        - market.kraken_futures.{instrument}.ticks
        - market.kraken_futures.{instrument}.trades
        - system.heartbeats.feed.kraken_futures

    Attributes:
        Inherits all attributes from MarketDataPublisherService.
    """

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Get default parameters from settings.

        Args:
            settings: Application settings with instruments config.

        Returns:
            Dictionary with symbols list for Kraken Futures.
        """
        instruments = settings.instruments
        return {
            "symbols": instruments.get(ExchangeEnum.KRAKEN_FUTURES, []),
        }

    def _create_exchange_client(self) -> KrakenFuturesExchangeClient:
        """Create anonymous Kraken Futures client for public data.

        Returns:
            Configured KrakenFuturesExchangeClient (no API keys needed).
        """
        return KrakenFuturesExchangeClient()

    def _get_exchange_name(self) -> MarketDataExchange:
        """Get exchange identifier.

        Returns:
            "kraken_futures" exchange name.
        """
        return ExchangeEnum.KRAKEN_FUTURES

    def _validate_symbols(self, symbols: list[str]) -> list[str]:
        """Validate and filter symbols for Kraken Futures.

        Converts symbols to Kraken Futures WS format to validate them.
        Invalid symbols are logged and skipped.

        Args:
            symbols: Input symbols in native format.

        Returns:
            Valid symbols that can be streamed from Kraken Futures.
        """
        native_symbols: list[str] = []
        seen_symbols: set[str] = set()
        for symbol in symbols:
            try:
                native_to_kraken_futures_ws(symbol)
            except ValueError:
                logger.warning(
                    f"KrakenFuturesMarketDataPublisher: Skipping unknown native symbol {symbol}"
                )
                continue
            if symbol in seen_symbols:
                continue
            seen_symbols.add(symbol)
            native_symbols.append(symbol)
        return native_symbols

    def _get_max_symbols_per_connection(self) -> int:
        """Get Kraken Futures WebSocket symbol limit.

        Returns:
            0 (unlimited) — Kraken Futures WS does not document a per-connection limit.
        """
        return 0

    async def _candle_loop(self, symbols: list[str], timeframe: str) -> None:
        """No-op candle loop.

        Kraken Futures has no WebSocket candle feed. Candle data should
        be fetched via REST get_ohlcv() if needed.

        Args:
            symbols: Product symbols (unused).
            timeframe: Candle interval (unused).
        """
        logger.info(
            f"KrakenFuturesMarketDataPublisher: Candle loop disabled "
            f"(no WS candle feed for kraken_futures, timeframe={timeframe})"
        )
