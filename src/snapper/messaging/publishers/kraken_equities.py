"""Kraken Equities (FCM Futures) market data publisher.

This module provides a market data feed publisher for FCM commodity/index
futures on Kraken's equities platform. It streams real-time ticks and trades
via the Kraken Equities WebSocket (``wss://ws-equities.kraken.com``) and
publishes normalized data to the ZMQ messaging bus.

Configuration
-------------
Symbols are configured via settings.instruments["kraken_equities"].
The publisher uses public (anonymous) WebSocket connections.
Data is delayed (~10 minutes).
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
from snapper.infrastructure.exchanges.implementations.kraken_equities import (
    KrakenEquitiesExchangeClient,
)
from snapper.infrastructure.symbols.functions import get_available_kraken_equities_symbols
from snapper.infrastructure.symbols.functions import native_to_kraken_equities_ws
from snapper.messaging.publishers.base import MarketDataPublisherService


@register_process(
    "kraken_equities_feed_publisher",
    description="Kraken Equities (FCM Futures) market data feed publisher",
    priority=20,
    role=ProcessRoleEnum.CORE,
    tags=("market-data", "publisher", "kraken_equities"),
    parameters_model=PublisherSymbolsParameters,
    enabled=False,
    mode=ProcessModeEnum.THREAD,
)
class KrakenEquitiesMarketDataPublisher(
    MarketDataPublisherService[KrakenEquitiesExchangeClient],
):
    """Kraken Equities market data publisher.

    Streams real-time market data from Kraken Equities WebSocket and
    publishes normalized messages to ZMQ. Handles symbol conversion between
    native format (``CLM6-NYMEX``) and WS format (``CLM6.NYMEX``).

    Topics Published:
        - market.kraken_equities.{instrument}.ticks
        - market.kraken_equities.{instrument}.trades
        - system.heartbeats.feed.kraken_equities
    """

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Get default parameters from settings.

        Args:
            settings: Application settings with instruments config.

        Returns:
            Dictionary with symbols list for Kraken Equities.
        """
        instruments = settings.instruments
        return {
            "symbols": instruments.get(ExchangeEnum.KRAKEN_EQUITIES, []),
        }

    def _create_exchange_client(self) -> KrakenEquitiesExchangeClient:
        """Create anonymous Kraken Equities client for public data.

        Returns:
            Configured KrakenEquitiesExchangeClient (no API keys needed).
        """
        return KrakenEquitiesExchangeClient()

    def _get_exchange_name(self) -> MarketDataExchange:
        """Get exchange identifier.

        Returns:
            "kraken_equities" exchange name.
        """
        return ExchangeEnum.KRAKEN_EQUITIES

    def _validate_symbols(self, symbols: list[str]) -> list[str]:
        """Validate and filter symbols for Kraken Equities.

        Converts symbols to Kraken Equities WS format to validate them.
        Invalid symbols are logged and skipped.

        Wildcard handling: ``["*"]`` expands client-side to every
        native Kraken Equities symbol currently loaded by the symbol
        mapper. The Kraken Equities WS has no server-side wildcard
        token, so expansion happens here and the underlying SDK
        subscribes per-product.

        Args:
            symbols: Input symbols in native format, or ``["*"]`` for
                subscribe-all.

        Returns:
            Valid symbols that can be streamed from Kraken Equities.
        """
        if symbols == ["*"]:
            expanded = get_available_kraken_equities_symbols()
            logger.info(
                f"KrakenEquitiesMarketDataPublisher: wildcard expansion -> "
                f"{len(expanded)} equities symbols"
            )
            return expanded
        native_symbols: list[str] = []
        seen_symbols: set[str] = set()
        for symbol in symbols:
            try:
                native_to_kraken_equities_ws(symbol)
            except ValueError:
                logger.warning(
                    f"KrakenEquitiesMarketDataPublisher: Skipping unknown native symbol {symbol}"
                )
                continue
            if symbol in seen_symbols:
                continue
            seen_symbols.add(symbol)
            native_symbols.append(symbol)
        return native_symbols

    def _get_max_symbols_per_connection(self) -> int:
        """Get WebSocket symbol limit.

        Returns:
            0 (unlimited).
        """
        return 0

    async def _candle_loop(self, symbols: list[str], timeframe: str) -> None:
        """No-op candle loop.

        Kraken Equities candle data should be fetched via REST if needed.

        Args:
            symbols: Contract symbols (unused).
            timeframe: Candle interval (unused).
        """
        logger.info(
            f"KrakenEquitiesMarketDataPublisher: Candle loop disabled "
            f"(no WS candle feed for kraken_equities, timeframe={timeframe})"
        )
