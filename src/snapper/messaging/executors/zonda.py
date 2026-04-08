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

        Phase 0c: when ``self._credentials`` is populated by the
        base-class ``_resolve_credentials`` call (non-empty
        ``wallet_public_id``), the API key/secret come from the
        per-wallet ``wallet_credentials`` payload. Otherwise the
        legacy ``AppSettings.zonda_api_key/_secret`` properties
        are used.
        """
        repository = get_repository(self.settings.db_url)
        if self._credentials is not None:
            api_key = self._credentials.get("api_key", "")
            api_secret = self._credentials.get("api_secret", "")
        else:
            api_key = self.settings.zonda_api_key
            api_secret = self.settings.zonda_api_secret
        return ZondaExchangeClient(
            api_key=api_key,
            api_secret=api_secret,
            repository=repository,
        )

    def _get_exchange_name(self) -> OrderExchange:
        return ExchangeEnum.ZONDA
