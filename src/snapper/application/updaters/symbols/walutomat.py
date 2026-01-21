"""Walutomat symbol mapping updater service.

Fetches and persists FX pair symbols from Walutomat REST API.
"""

from datetime import UTC
from datetime import datetime
from typing import Any

from loguru import logger
from sqlalchemy import select

from snapper.application.process_manager.enums import ProcessLifecycleEnum
from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.registry import register_process
from snapper.application.updaters.symbols.base import SymbolMappingUpdaterService
from snapper.config.settings import AppSettings
from snapper.data.models import SymbolMapping
from snapper.infrastructure.exchanges.implementations.walutomat import WalutomatExchangeClient


@register_process(
    "walutomat_symbol_mapping_updater",
    method="start",
    description="Walutomat symbol mapping updater (REST API -> symbol_mappings)",
    priority=16,
    lifecycle=ProcessLifecycleEnum.ONE_SHOT,
    role=ProcessRoleEnum.TASK,
    tags=("maintenance", "symbols", "walutomat"),
    enabled=True,
    mode="thread",
    args=[],
)
class WalutomatSymbolMappingUpdaterService(SymbolMappingUpdaterService[WalutomatExchangeClient]):
    """Service for updating Walutomat symbol mappings from REST API."""

    @staticmethod
    def get_default_kwargs(settings: AppSettings) -> dict[str, Any]:
        """Return default kwargs for the Walutomat updater service.

        Args:
            settings: Application settings instance.

        Returns:
            Dictionary with update_threshold_hours and force parameters.
        """
        return {
            "update_threshold_hours": 168,
            "force": False,
        }

    def _create_exchange_client(self) -> WalutomatExchangeClient:
        """Create a Walutomat exchange client instance.

        Returns:
            Configured WalutomatExchangeClient with default polling interval and timeout.
        """
        return WalutomatExchangeClient(polling_interval=10.0, timeout=5.0)

    def _get_setting_key(self) -> str:
        """Get the settings key for tracking last update timestamp.

        Returns:
            Settings key string for Walutomat symbol mapping updates.
        """
        return "walutomat_symbol_mapping_last_update"

    async def _update_database(self, symbols: list[dict[str, Any]]) -> None:
        """Update database with fetched Walutomat symbol mappings.

        Args:
            symbols: List of symbol dictionaries containing symbol, walutomat_rest_symbol,
                native_symbol, base, and quote keys.
        """
        assert self.repository is not None, "Repository not initialized"
        updated_count = 0
        with self.repository.get_session() as session:
            for instrument in symbols:
                walutomat_symbol = instrument["symbol"]
                walutomat_rest_symbol = instrument["walutomat_rest_symbol"]
                native_symbol = instrument["native_symbol"]
                stmt = select(SymbolMapping).where(
                    (SymbolMapping.walutomat_rest_symbol == walutomat_rest_symbol)
                    | (SymbolMapping.native_symbol == native_symbol)
                )
                mapping = session.execute(stmt).scalar_one_or_none()
                now = datetime.now(UTC)
                if not mapping:
                    mapping = SymbolMapping(
                        walutomat_rest_symbol=walutomat_rest_symbol,
                        native_symbol=native_symbol,
                        walutomat_symbol=walutomat_symbol,
                        kraken_websocket_symbol=None,
                        kraken_rest_symbol=None,
                        ccxt_symbol=None,
                        base_currency=instrument["base"],
                        quote_currency=instrument["quote"],
                        created_at=now,
                        updated_at=now,
                    )
                    session.add(mapping)
                    logger.debug(
                        f"Created new mapping: {walutomat_rest_symbol} -> {walutomat_symbol}"
                    )
                    updated_count += 1
                else:
                    updated = False
                    if mapping.walutomat_symbol != walutomat_symbol:
                        mapping.walutomat_symbol = walutomat_symbol
                        updated = True
                    if mapping.walutomat_rest_symbol != walutomat_rest_symbol:
                        mapping.walutomat_rest_symbol = walutomat_rest_symbol
                        updated = True
                    if updated:
                        mapping.updated_at = now
                        logger.debug(
                            f"Updated mapping: {walutomat_rest_symbol} -> {walutomat_symbol}"
                        )
                        updated_count += 1
            session.commit()
        logger.info(f"Updated {updated_count}/{len(symbols)} Walutomat symbol mappings")
