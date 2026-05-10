"""Kraken exchange market data publisher.

This module provides a market data feed publisher for the Kraken exchange.
It streams real-time ticks, trades, and candles via Kraken's WebSocket API
and publishes normalized data to the ZMQ messaging bus.

The publisher handles Kraken-specific symbol conversion and respects the
exchange's WebSocket connection limit of 20 symbols per connection.

Classes
-------
KrakenMarketDataPublisher
    RegisterableProcess for Kraken market data streaming.

Configuration
-------------
Symbols are configured via settings.instruments["kraken"].
The publisher uses public (anonymous) WebSocket connections.

Example:
-------
Register and run via process manager::

    # Configured automatically via @register_process decorator
    # or manually:
    publisher = KrakenMarketDataPublisher(symbols=["BTC-USD", "ETH-USD"])
    await publisher.start()
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
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.symbols.functions import native_to_kraken_websocket
from snapper.messaging.publishers.base import MarketDataPublisherService


@register_process(
    "kraken_feed_publisher",
    description="Kraken market data feed publisher",
    priority=20,
    role=ProcessRoleEnum.CORE,
    tags=("market-data", "publisher", "kraken"),
    parameters_model=PublisherSymbolsParameters,
    enabled=True,
    mode=ProcessModeEnum.THREAD,
)
class KrakenMarketDataPublisher(MarketDataPublisherService[KrakenExchangeClient]):
    """Kraken exchange market data publisher.

    Streams real-time market data from Kraken's WebSocket API and publishes
    normalized messages to ZMQ. Handles symbol conversion between native
    format (BTC-USD) and Kraken WebSocket format (XBT/USD).

    Respects Kraken's limit of 20 symbols per WebSocket connection.

    Topics Published:
        - market.kraken.{instrument}.ticks
        - market.kraken.{instrument}.trades
        - market.kraken.{instrument}.candles.{timeframe}
        - system.heartbeats.feed.kraken

    Attributes:
        Inherits all attributes from MarketDataPublisherService.

    Example:
        ::

            publisher = KrakenMarketDataPublisher(symbols=["BTC-USD"])
            await publisher.start()  # Streams until stopped
    """

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Get default parameters from settings.

        Args:
            settings: Application settings with instruments config.

        Returns:
            Dictionary with symbols list for Kraken.
        """
        instruments = settings.instruments
        kraken_symbols = instruments.get(ExchangeEnum.KRAKEN, [])
        return {
            "symbols": kraken_symbols,
        }

    def _create_exchange_client(self) -> KrakenExchangeClient:
        """Create anonymous Kraken WebSocket client.

        Returns:
            Configured KrakenExchangeClient for public data.
        """
        return KrakenExchangeClient()

    def _get_exchange_name(self) -> MarketDataExchange:
        """Get exchange identifier.

        Returns:
            "kraken" exchange name.
        """
        return ExchangeEnum.KRAKEN

    def _validate_symbols(self, symbols: list[str]) -> list[str]:
        """Validate and filter symbols for Kraken.

        Converts symbols to Kraken WebSocket format to validate them.
        Invalid symbols are logged and skipped. The wildcard ``["*"]``
        passes through untouched — Kraken's WebSocket accepts ``"*"``
        as a subscribe-all sentinel (already used by
        :class:`KrakenSnapshotUpdaterService`) so the publisher
        forwards it verbatim and bypasses both per-symbol mapping and
        the 20-symbol-per-connection limit.

        Args:
            symbols: Input symbols in native format, or ``["*"]``.

        Returns:
            Valid symbols that can be streamed from Kraken, or
            ``["*"]`` when the caller requested subscribe-all.
        """
        if symbols == ["*"]:
            return ["*"]
        native_symbols: list[str] = []
        seen_symbols: set[str] = set()
        for symbol in symbols:
            try:
                native_to_kraken_websocket(symbol)
            except ValueError:
                logger.warning(
                    f"KrakenMarketDataPublisher: Skipping unknown native symbol {symbol}"
                )
                continue
            if symbol in seen_symbols:
                continue
            seen_symbols.add(symbol)
            native_symbols.append(symbol)
        return native_symbols

    def _get_max_symbols_per_connection(self) -> int:
        """Get Kraken's WebSocket symbol limit.

        Returns:
            ``0`` when ``self.symbols == ["*"]`` (wildcard subscribe-all
            uses a single connection unconstrained by the per-symbol
            limit) or ``20`` for the explicit-symbol path.
        """
        if self.symbols == ["*"]:
            return 0
        return 20
