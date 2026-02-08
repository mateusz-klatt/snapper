"""Walutomat market data publisher service.

Streams real-time FX quotes from Walutomat exchange via ZeroMQ.
"""

from typing import Any

from loguru import logger

from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.core.types import MarketDataExchange
from snapper.infrastructure.exchanges.implementations.walutomat import WalutomatExchangeClient
from snapper.infrastructure.symbols.functions import native_to_walutomat
from snapper.messaging.publishers.base import MarketDataPublisherService


@register_process(
    "walutomat_feed_publisher",
    description="Walutomat market data feed publisher",
    priority=22,
    role=ProcessRoleEnum.CORE,
    tags=("market-data", "publisher", "walutomat"),
    enabled=True,
    mode="thread",
    args=[],
)
class WalutomatMarketDataPublisher(MarketDataPublisherService[WalutomatExchangeClient]):
    """Market data publisher for Walutomat exchange."""

    @staticmethod
    def get_default_kwargs(settings: AppSettings) -> dict[str, Any]:
        """Return default keyword arguments for publisher initialization.

        Args:
            settings: Application settings instance.

        Returns:
            Dictionary with symbols from Walutomat instruments configuration.
        """
        instruments = settings.instruments
        walutomat_symbols = instruments.get("walutomat", [])
        return {
            "symbols": walutomat_symbols,
        }

    def _create_exchange_client(self) -> WalutomatExchangeClient:
        return WalutomatExchangeClient()

    def _get_exchange_name(self) -> MarketDataExchange:
        return "walutomat"

    def _validate_symbols(self, symbols: list[str]) -> list[str]:
        native_symbols: list[str] = []
        seen_symbols: set[str] = set()
        for symbol in symbols:
            try:
                native_to_walutomat(symbol)
            except ValueError:
                logger.warning(
                    f"WalutomatMarketDataPublisher: Skipping unknown native symbol {symbol}"
                )
                continue
            if symbol in seen_symbols:
                continue
            seen_symbols.add(symbol)
            native_symbols.append(symbol)
        return native_symbols
