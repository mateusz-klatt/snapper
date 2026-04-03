"""Kraken Futures exchange order executor.

This module provides an order execution service for the Kraken Futures
exchange. It receives order requests via ZMQ, executes them through
Kraken Futures authenticated API, and publishes execution results.

The executor supports WebSocket execution streaming for real-time fill
notifications via authenticated ``fills`` and ``open_orders`` channels.

Classes
-------
KrakenFuturesOrderExecutor
    RegisterableProcess for Kraken Futures order execution.

Configuration
-------------
Requires API credentials configured in settings:
- kraken_futures_api_key
- kraken_futures_api_secret
"""

from snapper.application.process_manager.registry import register_process
from snapper.core.types import ExchangeEnum
from snapper.core.types import OrderExchange
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.data.repository import get_repository
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.messaging.executors.base import ExchangeExecutorService


@register_process(
    "executor_kraken_futures",
    description="Kraken Futures order executor",
    priority=30,
    role=ProcessRoleEnum.CORE,
    tags=("execution", "orders", "kraken_futures"),
    enabled=False,
    mode=ProcessModeEnum.THREAD,
)
class KrakenFuturesOrderExecutor(ExchangeExecutorService[KrakenFuturesExchangeClient]):
    """Kraken Futures exchange order execution service.

    Executes orders on Kraken Futures via authenticated API. Supports
    WebSocket execution streaming for real-time fill notifications.

    Topics Subscribed:
        - orders.commands.kraken_futures.{instrument}.submit
        - orders.commands.kraken_futures.{instrument}.cancel
        - system.symbol_aliases
        - system.settings

    Topics Published:
        - orders.events.kraken_futures.{instrument}.submitted
        - orders.events.kraken_futures.{instrument}.executed
        - orders.events.kraken_futures.{instrument}.rejected
        - system.heartbeats.executor.kraken_futures

    Attributes:
        Inherits all attributes from ExchangeExecutorService.
    """

    def _create_exchange_client(self) -> KrakenFuturesExchangeClient:
        """Create authenticated Kraken Futures client.

        Returns:
            KrakenFuturesExchangeClient with API credentials from settings.
        """
        repository = get_repository(self.settings.db_url)
        return KrakenFuturesExchangeClient(
            api_key=self.settings.kraken_futures_api_key,
            api_secret=self.settings.kraken_futures_api_secret,
            repository=repository,
        )

    def _get_exchange_name(self) -> OrderExchange:
        """Get exchange identifier.

        Returns:
            "kraken_futures" exchange name.
        """
        return ExchangeEnum.KRAKEN_FUTURES
