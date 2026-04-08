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
Requires a ``wallet_credentials`` row with ``exchange="kraken"``,
``credential_type="api_key_secret"``, and a Fernet-encrypted envelope
``{"api_key": "...", "api_secret": "..."}``. Loaded at executor
startup via ``CredentialResolver``. Post-0c cleanup item 1 removed
the legacy ``AppSettings.kraken_api_key`` / ``kraken_api_secret``
fallback — wallet_credentials is now the single source of truth.

Example:
-------
Register and run via process manager::

    executor = KrakenOrderExecutor()
    await executor.start()  # Listens for orders until stopped
"""

from snapper.application.process_manager.registry import register_process
from snapper.core.types import ExchangeEnum
from snapper.core.types import OrderExchange
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.data.repository import get_repository
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.messaging.executors.base import ExchangeExecutorService


@register_process(
    "executor_kraken",
    description="Kraken order executor",
    priority=30,
    role=ProcessRoleEnum.CORE,
    tags=("execution", "orders", "kraken"),
    enabled=False,
    mode=ProcessModeEnum.THREAD,
)
class KrakenOrderExecutor(ExchangeExecutorService[KrakenExchangeClient]):
    """Kraken exchange order execution service.

    Executes orders on Kraken via authenticated API. Supports WebSocket
    execution streaming for real-time fill notifications.

    Topics Subscribed:
        - orders.commands.kraken.{instrument}.submit
        - orders.commands.kraken.{instrument}.cancel
        - system.symbol_aliases
        - system.settings

    Topics Published:
        - orders.events.kraken.{instrument}.submitted
        - orders.events.kraken.{instrument}.executed
        - orders.events.kraken.{instrument}.rejected
        - system.heartbeats.executor.kraken

    Attributes:
        Inherits all attributes from ExchangeExecutorService.

    Example:
        ::

            executor = KrakenOrderExecutor()
            await executor.start()
    """

    def _create_exchange_client(self) -> KrakenExchangeClient:
        """Create authenticated Kraken client.

        Reads ``api_key`` / ``api_secret`` from the per-wallet
        ``wallet_credentials`` envelope resolved by the base-class
        ``_resolve_credentials`` call during ``start()``. Post-0c
        cleanup removed the legacy ``AppSettings.kraken_api_key`` /
        ``kraken_api_secret`` fallback — credentials come from the
        ``wallet_credentials`` table via ``CredentialResolver``, full
        stop. Tests that instantiate ``KrakenOrderExecutor()`` with
        an empty ``wallet_public_id`` must inject ``self._credentials``
        directly before calling ``start()``.

        Returns:
            KrakenExchangeClient with API credentials.

        Raises:
            CredentialNotFound: Propagated from ``_resolve_credentials``
                when the wallet has no active Kraken credential row.
            KeyError: If ``self._credentials`` is missing the
                ``api_key`` / ``api_secret`` keys (malformed envelope).
        """
        repository = get_repository(self.settings.db_url)
        if self._credentials is None:
            raise RuntimeError(
                "KrakenOrderExecutor: credentials not resolved. Ensure "
                "wallet_public_id is set and wallet_credentials contains "
                "a row for exchange='kraken', or inject self._credentials "
                "directly in tests before calling start()."
            )
        return KrakenExchangeClient(
            api_key=self._credentials["api_key"],
            api_secret=self._credentials["api_secret"],
            repository=repository,
        )

    def _get_exchange_name(self) -> OrderExchange:
        """Get exchange identifier.

        Returns:
            "kraken" exchange name.
        """
        return ExchangeEnum.KRAKEN
