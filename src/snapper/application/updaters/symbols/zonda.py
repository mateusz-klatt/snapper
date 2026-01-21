"""Zonda exchange symbol mapping updater service.

Fetches and persists trading pair symbols from Zonda (BitBay) API.
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
from snapper.infrastructure.exchanges.implementations.zonda import ZondaExchangeClient
from snapper.infrastructure.symbols.mapper import make_native_symbol


@register_process(
    "zonda_symbol_mapping_updater",
    method="start",
    description="Zonda symbol mapping updater (REST API -> symbol_mappings)",
    priority=17,
    lifecycle=ProcessLifecycleEnum.ONE_SHOT,
    role=ProcessRoleEnum.TASK,
    tags=("maintenance", "symbols", "zonda"),
    enabled=True,
    mode="thread",
    args=[],
)
class ZondaSymbolMappingUpdaterService(SymbolMappingUpdaterService[ZondaExchangeClient]):
    """Service for updating Zonda symbol mappings from CCXT."""

    @staticmethod
    def get_default_kwargs(settings: AppSettings) -> dict[str, Any]:
        """Return default kwargs for the Zonda updater service.

        Args:
            settings: Application settings instance.

        Returns:
            Dictionary with default update_threshold_hours and force parameters.
        """
        return {
            "update_threshold_hours": 24,
            "force": False,
        }

    def _create_exchange_client(self) -> ZondaExchangeClient:
        """Create a Zonda exchange client instance.

        Returns:
            Configured Zonda exchange client for API communication.
        """
        return ZondaExchangeClient()

    def _get_setting_key(self) -> str:
        """Get the settings key for last update timestamp.

        Returns:
            Settings key string for storing Zonda symbol mapping update time.
        """
        return "zonda_symbol_mapping_last_update"

    async def load_zonda_markets(self, client: ZondaExchangeClient) -> list[dict[str, Any]]:
        """Load trading markets from Zonda via CCXT load_markets.

        Args:
            client: Zonda exchange client for fetching market data.

        Returns:
            List of dictionaries containing symbol mapping data for each market.
        """
        logger.info("Loading markets from Zonda via CCXT load_markets()")
        try:
            symbols: list[dict[str, Any]] = []
            async for market in client.subscribe_instruments():
                zonda_symbol_raw = market.get("id")
                if not zonda_symbol_raw or not isinstance(zonda_symbol_raw, str):
                    continue
                zonda_symbol: str = zonda_symbol_raw
                ccxt_symbol_raw = market.get("symbol")
                if not ccxt_symbol_raw or not isinstance(ccxt_symbol_raw, str):
                    continue
                ccxt_symbol: str = ccxt_symbol_raw
                base_raw = market.get("base")
                quote_raw = market.get("quote")
                if not isinstance(base_raw, str) or not isinstance(quote_raw, str):
                    logger.warning(f"Skipping {zonda_symbol}: invalid currency types")
                    continue
                base: str = base_raw
                quote: str = quote_raw
                if not base or not quote:
                    logger.warning(f"Skipping {zonda_symbol}: missing base/quote currency")
                    continue
                native_symbol = make_native_symbol(base, quote)
                symbols.append(
                    {
                        "zonda_symbol": zonda_symbol,
                        "native_symbol": native_symbol,
                        "ccxt_symbol": ccxt_symbol,
                        "base": base,
                        "quote": quote,
                    }
                )
            logger.info(f"Loaded {len(symbols)} trading pairs from Zonda")
            return symbols
        except Exception as e:
            logger.error(f"Error loading Zonda markets: {e}")
            raise

    async def _fetch_symbols(self, client: ZondaExchangeClient) -> list[dict[str, Any]]:
        """Fetch symbol data from Zonda exchange.

        Args:
            client: Zonda exchange client for API communication.

        Returns:
            List of symbol mapping dictionaries from Zonda markets.
        """
        return await self.load_zonda_markets(client)

    async def _update_database(self, symbols: list[dict[str, Any]]) -> None:
        """Persist symbol mappings to the database.

        Args:
            symbols: List of symbol mapping dictionaries to save or update.
        """
        assert self.repository is not None, "Repository not initialized"
        updated_count = 0
        created_count = 0
        try:
            with self.repository.get_session() as session:
                for symbol_data in symbols:
                    native_symbol = symbol_data["native_symbol"]
                    zonda_symbol = symbol_data["zonda_symbol"]
                    stmt = select(SymbolMapping).where(SymbolMapping.native_symbol == native_symbol)
                    existing = session.execute(stmt).scalar_one_or_none()
                    now = datetime.now(UTC)
                    if existing:
                        updated = False
                        if existing.zonda_symbol != zonda_symbol:
                            existing.zonda_symbol = zonda_symbol
                            updated = True
                        ccxt_symbol = symbol_data.get("ccxt_symbol")
                        if existing.ccxt_symbol != ccxt_symbol:
                            existing.ccxt_symbol = ccxt_symbol
                            updated = True
                        if updated:
                            existing.updated_at = now
                            updated_count += 1
                    else:
                        new_mapping = SymbolMapping(
                            native_symbol=native_symbol,
                            zonda_symbol=zonda_symbol,
                            base_currency=symbol_data["base"],
                            quote_currency=symbol_data["quote"],
                            kraken_websocket_symbol=None,
                            kraken_rest_symbol=None,
                            ccxt_symbol=symbol_data.get("ccxt_symbol"),
                            walutomat_symbol=None,
                            walutomat_rest_symbol=None,
                            polygon_symbol=None,
                            created_at=now,
                            updated_at=now,
                        )
                        session.add(new_mapping)
                        created_count += 1
                session.commit()
                logger.info(
                    f"Zonda update complete: {created_count} created, "
                    f"{updated_count} updated (total: {len(symbols)})"
                )
        except Exception as e:
            logger.error(f"Error in Zonda symbol mapping update: {e}", exc_info=True)
            raise
