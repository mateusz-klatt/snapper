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
    enabled=True,
    mode=ProcessModeEnum.THREAD,
)
class WalutomatOrderExecutor(ExchangeExecutorService[WalutomatExchangeClient]):
    """Order executor for Walutomat exchange.

    Processes order requests and submits them to the Walutomat API.
    """

    def _create_exchange_client(self) -> WalutomatExchangeClient:
        """Create the Walutomat exchange client.

        Phase 0c: when ``self._credentials`` is populated by the
        base-class ``_resolve_credentials`` call (non-empty
        ``wallet_public_id``), the API key and RSA PEM come from the
        per-wallet ``wallet_credentials`` payload
        (``credential_type="rsa_pem"``). Otherwise the legacy
        ``AppSettings.walutomat_api_key/_private_key`` properties
        are used.
        """
        repository = get_repository(self.settings.db_url)
        if self._credentials is not None:
            api_key = self._credentials.get("api_key", "")
            private_key_data = self._credentials.get("private_key_pem", "")
        else:
            api_key = self.settings.walutomat_api_key
            private_key_data = self.settings.walutomat_private_key
        return WalutomatExchangeClient(
            api_key=api_key,
            private_key_data=private_key_data,
            repository=repository,
        )

    def _get_exchange_name(self) -> OrderExchange:
        return ExchangeEnum.WALUTOMAT
