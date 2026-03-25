"""Paper trading order executor.

This module provides a simulated order execution service for paper trading.
It receives order requests via ZMQ and simulates execution with configurable
fill delays, providing a safe environment for strategy testing.

The paper executor maintains a simulated balance and tracks positions without
connecting to real exchange APIs.

Classes
-------
PaperOrderExecutor
    RegisterableProcess for paper trading execution.

Configuration
-------------
Configurable via constructor:
- fill_delay: Simulated execution delay (default: 0.1s)
- initial_balance: Starting paper balance (default: $10,000)

Example:
-------
Register and run via process manager::

    executor = PaperOrderExecutor()
    await executor.start()
"""

from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.registry import register_process
from snapper.core.types import OrderExchange
from snapper.data.repository import get_repository
from snapper.infrastructure.exchanges.implementations.paper import PaperExchangeClient
from snapper.messaging.executors.base import ExchangeExecutorService


@register_process(
    "executor_paper",
    description="Paper order executor",
    priority=30,
    role=ProcessRoleEnum.CORE,
    tags=("execution", "orders", "paper", "simulation"),
    enabled=True,
    mode="thread",
)
class PaperOrderExecutor(ExchangeExecutorService[PaperExchangeClient]):
    """Paper trading order execution service.

    Simulates order execution without connecting to real exchanges.
    Useful for strategy backtesting and development. Supports immediate
    simulated fills with configurable delay.

    Topics Subscribed:
        - orders.commands.paper.{instrument}.submit
        - orders.commands.paper.{instrument}.cancel
        - system.symbol_aliases
        - system.settings

    Topics Published:
        - orders.events.paper.{instrument}.accepted
        - orders.events.paper.{instrument}.executed
        - orders.events.paper.{instrument}.rejected
        - system.heartbeats.executor.paper

    Attributes:
        Inherits all attributes from ExchangeExecutorService.

    Example:
        ::

            executor = PaperOrderExecutor()
            await executor.start()  # Simulates order execution
    """

    def _create_exchange_client(self) -> PaperExchangeClient:
        """Create paper trading client.

        Returns:
            PaperExchangeClient with simulated balance.
        """
        repository = get_repository(self.settings.db_url)
        return PaperExchangeClient(
            repository=repository,
            fill_delay=0.1,
            initial_balance=10000.0,
        )

    def _get_exchange_name(self) -> OrderExchange:
        """Get exchange identifier.

        Returns:
            "paper" as OrderExchange literal.
        """
        return "paper"
