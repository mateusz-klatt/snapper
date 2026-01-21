"""Walutomat order execution service.

Processes order requests and reports fills for Walutomat FX exchange.
"""

from typing import Any

from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.data.repository import get_repository
from snapper.infrastructure.exchanges.implementations.walutomat import WalutomatExchangeClient
from snapper.infrastructure.symbols.functions import TradingExchange
from snapper.messaging.executors.base import ExchangeExecutorService


@register_process(
    "executor_walutomat",
    description="Walutomat execution service for processing orders",
    priority=30,
    role=ProcessRoleEnum.CORE,
    tags=("execution", "orders", "walutomat"),
    enabled=True,
    mode="thread",
    args=[],
)
class WalutomatOrderExecutor(ExchangeExecutorService[WalutomatExchangeClient]):
    """Order executor for Walutomat exchange.

    Processes order requests and submits them to the Walutomat API.
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

    def _create_exchange_client(self) -> WalutomatExchangeClient:
        repository = get_repository(self.settings.db_url)
        return WalutomatExchangeClient(
            api_key=self.settings.walutomat_api_key,
            private_key_data=self.settings.walutomat_private_key,
            repository=repository,
        )

    def _get_exchange_name(self) -> TradingExchange:
        return "walutomat"
