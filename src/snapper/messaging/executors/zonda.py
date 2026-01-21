"""Zonda order execution service.

Processes order requests and reports fills for Zonda cryptocurrency exchange.
"""

from typing import Any

from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.data.repository import get_repository
from snapper.infrastructure.exchanges.implementations.zonda import ZondaExchangeClient
from snapper.infrastructure.symbols.functions import TradingExchange
from snapper.messaging.executors.base import ExchangeExecutorService


@register_process(
    "executor_zonda",
    description="Zonda execution service for processing orders",
    priority=30,
    role=ProcessRoleEnum.CORE,
    tags=("execution", "orders", "zonda"),
    enabled=True,
    mode="thread",
    args=[],
)
class ZondaOrderExecutor(ExchangeExecutorService[ZondaExchangeClient]):
    """Order executor for Zonda exchange.

    Processes order requests and submits them to the Zonda API.
    """

    @staticmethod
    def get_default_kwargs(settings: AppSettings) -> dict[str, Any]:
        """Return default keyword arguments for executor initialization.

        Args:
            settings: Application settings instance.

        Returns:
            Empty dictionary as no additional kwargs are needed.
        """
        return {}

    def _create_exchange_client(self) -> ZondaExchangeClient:
        repository = get_repository(self.settings.db_url)
        return ZondaExchangeClient(
            api_key=self.settings.zonda_api_key,
            api_secret=self.settings.zonda_api_secret,
            repository=repository,
        )

    def _get_exchange_name(self) -> TradingExchange:
        return "zonda"
