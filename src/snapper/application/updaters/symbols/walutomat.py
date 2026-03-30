"""Walutomat symbol updater service.

Fetches and persists FX pair symbols from Walutomat REST API.
"""

from datetime import UTC
from datetime import datetime
from typing import Any

from loguru import logger

from snapper.application.process_manager.enums import ProcessLifecycleEnum
from snapper.application.process_manager.enums import ProcessRoleEnum
from snapper.application.process_manager.process_parameters import SymbolUpdaterParameters
from snapper.application.process_manager.registry import register_process
from snapper.application.updaters.symbols.base import SymbolUpdaterService
from snapper.config.settings import AppSettings
from snapper.core.types import AliasChannelEnum
from snapper.core.types import AssetTypeEnum
from snapper.core.types import ExchangeEnum
from snapper.infrastructure.exchanges.implementations.walutomat import WalutomatExchangeClient


@register_process(
    "walutomat_symbol_updater",
    method="start",
    description="Walutomat symbol updater",
    priority=16,
    lifecycle=ProcessLifecycleEnum.ONE_SHOT,
    role=ProcessRoleEnum.TASK,
    tags=("maintenance", "symbols", "walutomat"),
    parameters_model=SymbolUpdaterParameters,
    enabled=True,
    mode="thread",
)
class WalutomatSymbolUpdaterService(SymbolUpdaterService[WalutomatExchangeClient]):
    """Service for updating Walutomat symbol mappings from REST API."""

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Return default parameters for the Walutomat updater service.

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
            Settings key string for Walutomat symbol updates.
        """
        return "walutomat_symbols_last_update"

    async def _update_database(self, symbols: list[dict[str, Any]]) -> None:
        """Persist symbol catalog and alias rows to the database.

        Args:
            symbols: List of symbol dictionaries containing symbol, walutomat_rest_symbol,
                native_symbol, base, and quote keys.
        """
        assert self.repository is not None, "Repository not initialized"
        created_count = 0
        updated_count = 0
        with self.repository.get_session() as session:
            processed_symbol_public_ids: set[str] = set()
            now = datetime.now(UTC)
            for instrument in symbols:
                native_symbol = instrument["native_symbol"]
                sid = self._tracker.session_id
                symbol_public_id = self._upsert_symbol(
                    session,
                    native_symbol,
                    instrument["base"],
                    instrument["quote"],
                    AssetTypeEnum.FOREX,
                    now,
                    session_id=sid,
                    sequence_id=self._tracker.next_sequence("symbols"),
                )
                processed_symbol_public_ids.add(symbol_public_id)
                ws_result = self._upsert_alias(
                    session,
                    symbol_public_id,
                    ExchangeEnum.WALUTOMAT,
                    AliasChannelEnum.WS,
                    instrument["symbol"],
                    now,
                    session_id=sid,
                    sequence_id=self._tracker.next_sequence("aliases"),
                )
                if ws_result == "created":
                    created_count += 1
                elif ws_result == "updated":
                    updated_count += 1
                rest_result = self._upsert_alias(
                    session,
                    symbol_public_id,
                    ExchangeEnum.WALUTOMAT,
                    AliasChannelEnum.REST,
                    instrument["walutomat_rest_symbol"],
                    now,
                    session_id=sid,
                    sequence_id=self._tracker.next_sequence("aliases"),
                )
                if rest_result == "created":
                    created_count += 1
                elif rest_result == "updated":
                    updated_count += 1
                self._upsert_capability(
                    session,
                    symbol_public_id,
                    ExchangeEnum.WALUTOMAT,
                    True,
                    True,
                    "walutomat_updater",
                    None,
                    now,
                    session_id=sid,
                    sequence_id=self._tracker.next_sequence("capabilities"),
                )
                self._ensure_instrument_identity(
                    session,
                    symbol_public_id,
                    ExchangeEnum.WALUTOMAT,
                    now,
                    session_id=sid,
                    sequence_id=self._tracker.next_sequence("instruments"),
                )
            deactivated = self._reconcile_capabilities(
                session,
                ExchangeEnum.WALUTOMAT,
                processed_symbol_public_ids,
                "walutomat_updater",
                now,
                session_id=self._tracker.session_id,
                next_sequence_fn=lambda: self._tracker.next_sequence("capabilities"),
            )
            session.commit()
        logger.info(
            f"Walutomat update complete: {created_count} created, "
            f"{updated_count} updated, {deactivated} deactivated "
            f"(total: {len(symbols)})"
        )
