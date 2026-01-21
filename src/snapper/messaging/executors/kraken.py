"""Kraken exchange order executor.

This module provides an order execution service for the Kraken exchange.
It receives order requests via ZMQ, executes them through Kraken's
authenticated API, and publishes execution results.

The executor supports WebSocket execution streaming for real-time fill
notifications.

Classes
-------
KrakenOrderExecutor
    RegisterableProcess for Kraken order execution.

Configuration
-------------
Requires API credentials configured in settings:
- kraken_api_key
- kraken_api_secret

Example:
-------
Register and run via process manager::

    executor = KrakenOrderExecutor()
    await executor.start()  # Listens for orders until stopped
"""

from typing import Any

from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.data.repository import get_repository
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.symbols.functions import TradingExchange
from snapper.messaging.executors.base import ExchangeExecutorService


@register_process(
    "executor_kraken",
    description="Kraken execution service for processing orders",
    priority=30,
    role=ProcessRoleEnum.CORE,
    tags=("execution", "orders", "kraken"),
    enabled=True,
    mode="thread",
    args=[],
)
class KrakenOrderExecutor(ExchangeExecutorService[KrakenExchangeClient]):
    """Kraken exchange order execution service.

    Executes orders on Kraken via authenticated API. Supports WebSocket
    execution streaming for real-time fill notifications.

    Topics Subscribed:
        - orders.kraken.requests
        - orders.kraken.{instrument}.new
        - system.symbol_mappings
        - system.settings

    Topics Published:
        - executions.kraken.{instrument}.fill
        - orders.kraken.{instrument}.status
        - system.heartbeats.executor.kraken

    Attributes:
        Inherits all attributes from ExchangeExecutorService.

    Example:
        ::

            executor = KrakenOrderExecutor()
            await executor.start()
    """

    @staticmethod
    def get_default_kwargs(settings: AppSettings) -> dict[str, Any]:
        """Get default kwargs from settings.

        Args:
            settings: Application settings (not used).

        Returns:
            Empty dictionary.
        """
        return {}

    def _create_exchange_client(self) -> KrakenExchangeClient:
        """Create authenticated Kraken client.

        Returns:
            KrakenExchangeClient with API credentials from settings.
        """
        repository = get_repository(self.settings.db_url)
        return KrakenExchangeClient(
            api_key=self.settings.kraken_api_key,
            api_secret=self.settings.kraken_api_secret,
            repository=repository,
        )

    def _get_exchange_name(self) -> TradingExchange:
        """Get exchange identifier.

        Returns:
            "kraken" as TradingExchange literal.
        """
        return "kraken"
