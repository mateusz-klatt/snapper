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
Requires a ``wallet_credentials`` row with
``exchange="kraken_futures"``, ``credential_type="api_key_secret"``,
and a Fernet-encrypted envelope ``{"api_key": "...", "api_secret":
"..."}``. Loaded at executor startup via ``CredentialResolver``.
``wallet_credentials`` is the single source of truth for Kraken
Futures API credentials.
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
        - orders.commands.kraken_futures.{instrument}.replace
        - system.symbol_aliases
        - system.settings

    Topics Published:
        - orders.events.kraken_futures.{instrument}.submitted
        - orders.events.kraken_futures.{instrument}.accepted
        - orders.events.kraken_futures.{instrument}.executed
        - orders.events.kraken_futures.{instrument}.cancelled
        - orders.events.kraken_futures.{instrument}.unknown
        - orders.events.kraken_futures.{instrument}.rejected
        - system.heartbeats.executor.kraken_futures[.{wallet_short}]

    Attributes:
        Inherits all attributes from ExchangeExecutorService.
    """

    def _create_exchange_client(self) -> KrakenFuturesExchangeClient:
        """Create authenticated Kraken Futures client.

        Reads ``api_key`` / ``api_secret`` from the per-wallet
        ``wallet_credentials`` envelope resolved by the base-class
        ``_resolve_credentials`` call during ``start()``.

        Returns:
            KrakenFuturesExchangeClient with API credentials.
        """
        repository = get_repository(self.settings.db_url)
        if self._credentials is None:
            raise RuntimeError(
                "KrakenFuturesOrderExecutor: credentials not resolved. "
                "Ensure wallet_public_id is set and wallet_credentials "
                "contains a row for exchange='kraken_futures', or inject "
                "self._credentials directly in tests before calling start()."
            )
        return KrakenFuturesExchangeClient(
            api_key=self._credentials["api_key"],
            api_secret=self._credentials["api_secret"],
            repository=repository,
        )

    def _get_exchange_name(self) -> OrderExchange:
        """Get exchange identifier.

        Returns:
            "kraken_futures" exchange name.
        """
        return ExchangeEnum.KRAKEN_FUTURES
