"""Zonda order execution service.

Processes order requests and reports fills for Zonda cryptocurrency exchange.
"""

from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.registry import register_process
from snapper.core.types import ExchangeEnum
from snapper.core.types import OrderExchange
from snapper.data.repository import get_repository
from snapper.infrastructure.exchanges.implementations.zonda import ZondaExchangeClient
from snapper.messaging.executors.base import ExchangeExecutorService


@register_process(
    "executor_zonda",
    description="Zonda order executor",
    priority=30,
    role=ProcessRoleEnum.CORE,
    tags=("execution", "orders", "zonda"),
    enabled=True,
    mode="thread",
)
class ZondaOrderExecutor(ExchangeExecutorService[ZondaExchangeClient]):
    """Order executor for Zonda exchange.

    Processes order requests and submits them to the Zonda API.
    """

    def _create_exchange_client(self) -> ZondaExchangeClient:
        repository = get_repository(self.settings.db_url)
        return ZondaExchangeClient(
            api_key=self.settings.zonda_api_key,
            api_secret=self.settings.zonda_api_secret,
            repository=repository,
        )

    def _get_exchange_name(self) -> OrderExchange:
        return ExchangeEnum.ZONDA
