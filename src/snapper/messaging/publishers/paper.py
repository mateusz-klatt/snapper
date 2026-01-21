"""Paper trading market data publisher.

Replays historical market data for backtesting and paper trading simulation.
"""

from typing import Any

from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.infrastructure.exchanges.implementations.paper import PaperExchangeClient
from snapper.messaging.publishers.base import MarketDataPublisherService


@register_process(
    "paper_feed_publisher",
    description="Paper trading feed publisher (historical data replay)",
    priority=30,
    role=ProcessRoleEnum.CORE,
    tags=("market-data", "publisher", "paper"),
    enabled=True,
    mode="thread",
    args=[],
)
class PaperMarketDataPublisher(MarketDataPublisherService[PaperExchangeClient]):
    """Market data publisher for paper trading with historical data replay."""

    def __init__(
        self,
        symbols: list[str],
        start_time: float | None = None,
        end_time: float | None = None,
    ) -> None:
        """Initialize the instance."""
        self.start_time = start_time
        self.end_time = end_time
        super().__init__(symbols)

    @staticmethod
    def get_default_kwargs(settings: AppSettings) -> dict[str, Any]:
        """Return default keyword arguments for publisher initialization.

        Args:
            settings: Application settings instance.

        Returns:
            Dictionary with symbols and time range configuration.
        """
        instruments = settings.instruments
        paper_symbols = instruments.get("paper", ["BTC-USD", "ETH-USD"])
        return {
            "symbols": paper_symbols,
            "start_time": None,
            "end_time": None,
        }

    def _create_exchange_client(self) -> PaperExchangeClient:
        return PaperExchangeClient(
            repository=None,
            start_time=self.start_time,
            end_time=self.end_time,
        )

    def _get_exchange_name(self) -> str:
        return "paper"

    def _validate_symbols(self, symbols: list[str]) -> list[str]:
        return list(set(symbols))
