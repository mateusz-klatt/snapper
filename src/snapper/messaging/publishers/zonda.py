"""Zonda exchange market data publisher.

This module provides a market data feed publisher for the Zonda (formerly BitBay)
exchange. It streams real-time ticks, trades, and candles via Zonda's WebSocket
API and publishes normalized data to the ZMQ messaging bus.

The publisher handles Zonda-specific symbol conversion (BTC-USD -> BTC-USD format
on Zonda).

Classes
-------
ZondaMarketDataPublisher
    RegisterableProcess for Zonda market data streaming.

Configuration
-------------
Symbols are configured via settings.instruments["zonda"].
The publisher uses public (anonymous) WebSocket connections.

Example:
-------
Register and run via process manager::

    publisher = ZondaMarketDataPublisher(symbols=["BTC-PLN", "ETH-PLN"])
    await publisher.start()
"""

from typing import Any

from loguru import logger

from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.core.types import MarketDataExchange
from snapper.infrastructure.exchanges.implementations.zonda import ZondaExchangeClient
from snapper.infrastructure.symbols.functions import native_to_zonda_ws
from snapper.messaging.publishers.base import MarketDataPublisherService


@register_process(
    "zonda_feed_publisher",
    description="Zonda market data feed publisher",
    priority=21,
    role=ProcessRoleEnum.CORE,
    tags=("market-data", "publisher", "zonda"),
    enabled=True,
    mode="thread",
    args=[],
)
class ZondaMarketDataPublisher(MarketDataPublisherService[ZondaExchangeClient]):
    """Zonda exchange market data publisher.

    Streams real-time market data from Zonda's WebSocket API and publishes
    normalized messages to ZMQ. Handles symbol conversion for Zonda's format.

    Topics Published:
        - market.zonda.{instrument}.ticks
        - market.zonda.{instrument}.trades
        - market.zonda.{instrument}.candles.{timeframe}
        - system.heartbeats.feed.zonda

    Attributes:
        Inherits all attributes from MarketDataPublisherService.

    Example:
        ::

            publisher = ZondaMarketDataPublisher(symbols=["BTC-PLN"])
            await publisher.start()
    """

    @staticmethod
    def get_default_kwargs(settings: AppSettings) -> dict[str, Any]:
        """Get default kwargs from settings.

        Args:
            settings: Application settings with instruments config.

        Returns:
            Dictionary with symbols list for Zonda.
        """
        instruments = settings.instruments
        zonda_symbols = instruments.get("zonda", [])
        return {
            "symbols": zonda_symbols,
        }

    def _create_exchange_client(self) -> ZondaExchangeClient:
        """Create anonymous Zonda WebSocket client.

        Returns:
            Configured ZondaExchangeClient for public data.
        """
        return ZondaExchangeClient()

    def _get_exchange_name(self) -> MarketDataExchange:
        """Get exchange identifier.

        Returns:
            "zonda" exchange name.
        """
        return "zonda"

    def _validate_symbols(self, symbols: list[str]) -> list[str]:
        """Validate and filter symbols for Zonda.

        Converts symbols to Zonda format to validate them.
        Invalid symbols are logged and skipped.

        Args:
            symbols: Input symbols in native format.

        Returns:
            Valid symbols that can be streamed from Zonda.
        """
        native_symbols: list[str] = []
        seen_symbols: set[str] = set()
        for symbol in symbols:
            try:
                native_to_zonda_ws(symbol)
            except ValueError:
                logger.warning(f"ZondaMarketDataPublisher: Skipping unknown native symbol {symbol}")
                continue
            if symbol in seen_symbols:
                continue
            seen_symbols.add(symbol)
            native_symbols.append(symbol)
        return native_symbols
