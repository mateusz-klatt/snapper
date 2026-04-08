"""Walutomat order execution service.

Processes order requests and reports fills for Walutomat FX exchange.
"""

from snapper.application.process_manager.registry import register_process
from snapper.core.types import ExchangeEnum
from snapper.core.types import OrderExchange
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.data.repository import get_repository
from snapper.infrastructure.exchanges.implementations.walutomat import WalutomatExchangeClient
from snapper.messaging.executors.base import ExchangeExecutorService


@register_process(
    "executor_walutomat",
    description="Walutomat order executor",
    priority=30,
    role=ProcessRoleEnum.CORE,
    tags=("execution", "orders", "walutomat"),
    enabled=False,
    mode=ProcessModeEnum.THREAD,
)
class WalutomatOrderExecutor(ExchangeExecutorService[WalutomatExchangeClient]):
    """Order executor for Walutomat exchange.

    Processes order requests and submits them to the Walutomat API.
    """

    def _create_exchange_client(self) -> WalutomatExchangeClient:
        """Create the Walutomat exchange client.

        Reads ``api_key`` / ``private_key_pem`` from the per-wallet
        ``wallet_credentials`` envelope (``credential_type="rsa_pem"``)
        resolved by the base-class ``_resolve_credentials`` call
        during ``start()``.
        """
        repository = get_repository(self.settings.db_url)
        if self._credentials is None:
            raise RuntimeError(
                "WalutomatOrderExecutor: credentials not resolved. "
                "Ensure wallet_public_id is set and wallet_credentials "
                "contains a row for exchange='walutomat', or inject "
                "self._credentials directly in tests before calling start()."
            )
        return WalutomatExchangeClient(
            api_key=self._credentials["api_key"],
            private_key_data=self._credentials["private_key_pem"],
            repository=repository,
        )

    def _get_exchange_name(self) -> OrderExchange:
        return ExchangeEnum.WALUTOMAT
