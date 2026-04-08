"""Zonda order execution service.

Processes order requests and reports fills for Zonda cryptocurrency exchange.
"""

from snapper.application.process_manager.registry import register_process
from snapper.core.types import ExchangeEnum
from snapper.core.types import OrderExchange
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.data.repository import get_repository
from snapper.infrastructure.exchanges.implementations.zonda import ZondaExchangeClient
from snapper.messaging.executors.base import ExchangeExecutorService


@register_process(
    "executor_zonda",
    description="Zonda order executor",
    priority=30,
    role=ProcessRoleEnum.CORE,
    tags=("execution", "orders", "zonda"),
    enabled=False,
    mode=ProcessModeEnum.THREAD,
)
class ZondaOrderExecutor(ExchangeExecutorService[ZondaExchangeClient]):
    """Order executor for Zonda exchange.

    Processes order requests and submits them to the Zonda API.
    """

    def _create_exchange_client(self) -> ZondaExchangeClient:
        """Create the Zonda exchange client.

        Reads ``api_key`` / ``api_secret`` from the per-wallet
        ``wallet_credentials`` envelope resolved by the base-class
        ``_resolve_credentials`` call during ``start()``.
        """
        repository = get_repository(self.settings.db_url)
        if self._credentials is None:
            raise RuntimeError(
                "ZondaOrderExecutor: credentials not resolved. Ensure "
                "wallet_public_id is set and wallet_credentials contains "
                "a row for exchange='zonda', or inject self._credentials "
                "directly in tests before calling start()."
            )
        return ZondaExchangeClient(
            api_key=self._credentials["api_key"],
            api_secret=self._credentials["api_secret"],
            repository=repository,
        )

    def _get_exchange_name(self) -> OrderExchange:
        return ExchangeEnum.ZONDA
