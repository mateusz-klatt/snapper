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

from snapper.application.process_manager.registry import register_process
from snapper.core.types import ExchangeEnum
from snapper.core.types import OrderExchange
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.data.repository import get_repository
from snapper.infrastructure.exchanges.implementations.paper import PaperExchangeClient
from snapper.messaging.executors.base import ExchangeExecutorService


@register_process(
    "executor_paper",
    description="Paper order executor",
    priority=30,
    role=ProcessRoleEnum.CORE,
    tags=("execution", "orders", "paper", "simulation"),
    enabled=False,
    mode=ProcessModeEnum.THREAD,
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

        Reads ``initial_balance`` from the per-wallet
        ``wallet_credentials`` envelope (``credential_type="paper"``)
        resolved by the base-class ``_resolve_credentials`` call
        during ``start()``. Post-0c cleanup removed the hardcoded
        10000.0 default — every paper wallet must have an explicit
        balance in its credential envelope. Tests that instantiate
        ``PaperOrderExecutor()`` with an empty ``wallet_public_id``
        must inject ``self._credentials = {"initial_balance": "..."}``
        before calling ``start()``.

        Returns:
            PaperExchangeClient with simulated balance.
        """
        repository = get_repository(self.settings.db_url)
        if self._credentials is None:
            raise RuntimeError(
                "PaperOrderExecutor: credentials not resolved. Ensure "
                "wallet_public_id is set and wallet_credentials contains "
                "a row for exchange='paper', or inject self._credentials "
                "directly in tests before calling start()."
            )
        initial_balance = float(self._credentials["initial_balance"])
        return PaperExchangeClient(
            repository=repository,
            fill_delay=0.1,
            initial_balance=initial_balance,
        )

    def _get_exchange_name(self) -> OrderExchange:
        """Get exchange identifier.

        Returns:
            "paper" exchange name.
        """
        return ExchangeEnum.PAPER
