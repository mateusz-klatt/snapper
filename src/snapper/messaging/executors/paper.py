"""Paper trading order executor.

This module provides a simulated order execution service for paper trading.
It receives order requests via ZMQ and simulates execution with the
``PaperExchangeClient`` fill delay, providing a safe environment for
strategy testing.

The paper executor maintains a simulated balance and tracks positions without
connecting to real exchange APIs.

Classes
-------
PaperOrderExecutor
    RegisterableProcess for paper trading execution.

Configuration
-------------
Requires a ``wallet_credentials`` row with ``exchange="paper"``,
``credential_type="paper"``, and a Fernet-encrypted envelope
``{"initial_balance": "..."}``. Loaded at executor startup via
``CredentialResolver``. The concrete executor passes a fixed
``fill_delay=0.1`` to ``PaperExchangeClient``.
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
    Useful for strategy backtesting and development. Uses the
    ``PaperExchangeClient`` simulated fill path.

    Topics Subscribed:
        - orders.commands.paper.{instrument}.submit
        - orders.commands.paper.{instrument}.cancel
        - orders.commands.paper.{instrument}.replace
        - system.symbol_aliases
        - system.settings

    Topics Published:
        - orders.events.paper.{instrument}.submitted
        - orders.events.paper.{instrument}.accepted
        - orders.events.paper.{instrument}.executed
        - orders.events.paper.{instrument}.cancelled
        - orders.events.paper.{instrument}.rejected
        - system.heartbeats.executor.paper[.{wallet_short}]

    Attributes:
        Inherits all attributes from ExchangeExecutorService.

    Direct construction with an empty ``wallet_public_id`` is for
    tests that inject ``self._credentials`` before ``start()``.
    """

    def _create_exchange_client(self) -> PaperExchangeClient:
        """Create paper trading client.

        Reads ``initial_balance`` from the per-wallet
        ``wallet_credentials`` envelope (``credential_type="paper"``)
        resolved by the base-class ``_resolve_credentials`` call
        during ``start()``. Every paper wallet must carry an explicit
        ``initial_balance`` in its credential envelope. Tests that
        instantiate ``PaperOrderExecutor()`` with an empty
        ``wallet_public_id`` must inject
        ``self._credentials = {"initial_balance": "..."}`` before
        calling ``start()``.

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
