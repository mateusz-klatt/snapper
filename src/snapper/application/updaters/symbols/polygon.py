"""Polygon.io symbol mapping updater service.

Fetches and persists ticker symbols from Polygon.io API (44k+ tickers).
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
from snapper.infrastructure.exchanges.implementations.polygon import PolygonExchangeClient


@register_process(
    "polygon_symbol_mapping_updater",
    method="start",
    description="Polygon symbol mapping updater (44k+ tickers from API)",
    priority=11,
    lifecycle=ProcessLifecycleEnum.ONE_SHOT,
    role=ProcessRoleEnum.TASK,
    tags=("maintenance", "symbols", "polygon"),
    enabled=True,
    mode="thread",
    args=[],
)
class PolygonSymbolMappingUpdaterService(SymbolMappingUpdaterService[PolygonExchangeClient]):
    """Service for updating Polygon symbol mappings from REST API."""

    @staticmethod
    def get_default_kwargs(settings: AppSettings) -> dict[str, Any]:
        """Return default kwargs for the Polygon updater service.

        Args:
            settings: Application settings instance.

        Returns:
            Dictionary with default configuration values.
        """
        return {
            "update_threshold_hours": 168,
            "force": False,
            "insert_new": False,
        }

    def __init__(
        self,
        update_threshold_hours: int = 168,
        force: bool = False,
        insert_new: bool = False,
    ) -> None:
        """Initialize the instance.

        Args:
            update_threshold_hours: Hours between updates before refresh is needed.
            force: Whether to force update regardless of threshold.
            insert_new: Whether to insert new symbol mappings not in database.
        """
        super().__init__(update_threshold_hours=update_threshold_hours, force=force)
        self.insert_new = insert_new
        self._polygon_client: PolygonExchangeClient | None = None

    def _create_exchange_client(self) -> PolygonExchangeClient:
        """Create or return cached Polygon exchange client.

        Returns:
            Configured Polygon exchange client instance.
        """
        if self._polygon_client is not None:
            return self._polygon_client
        api_key = self.settings.polygon_api_key
        if not api_key:
            raise ValueError("Polygon API key not configured in settings")
        self._polygon_client = PolygonExchangeClient(
            api_key=api_key,
            symbols_cache_file="data/polygon/reference/symbols.csv",
            cache_ttl_hours=168,
        )
        return self._polygon_client

    def _get_setting_key(self) -> str:
        """Get the settings key for last update timestamp.

        Returns:
            Settings key name for tracking last update time.
        """
        return "polygon_symbol_mappings_last_update"

    async def _update_database(self, symbols: list[dict[str, Any]]) -> None:
        """Update database with fetched symbol data.

        Args:
            symbols: List of symbol dictionaries from Polygon API.
        """
        assert self.repository is not None
        stats = {"updated": 0, "inserted": 0, "skipped": 0}
        now = datetime.now(UTC)
        try:
            with self.repository.get_session() as session:
                for symbol_data in symbols:
                    ticker = symbol_data.get("ticker")
                    if not ticker:
                        continue
                    native_symbol = self._match_polygon_to_native(ticker, symbol_data)
                    if not native_symbol:
                        stats["skipped"] += 1
                        continue
                    base: str
                    quote: str | None
                    if "-" in native_symbol:
                        base, quote = native_symbol.split("-", 1)
                    else:
                        base = native_symbol
                        currency = symbol_data.get("currency_symbol") or symbol_data.get(
                            "currency_name", ""
                        )
                        quote = currency.upper() if currency else None
                    stmt = select(SymbolMapping).where(SymbolMapping.native_symbol == native_symbol)
                    mapping = session.execute(stmt).scalar_one_or_none()
                    if mapping is None:
                        if not self.insert_new:
                            stats["skipped"] += 1
                            continue
                        mapping = SymbolMapping(
                            native_symbol=native_symbol,
                            base_currency=base,
                            quote_currency=quote,
                            polygon_symbol=ticker,
                            created_at=now,
                            updated_at=now,
                        )
                        session.add(mapping)
                        stats["inserted"] += 1
                    else:
                        mapping.polygon_symbol = ticker
                        mapping.updated_at = now
                        stats["updated"] += 1
                    total_processed = stats["updated"] + stats["inserted"]
                    if total_processed % 1000 == 0 and total_processed > 0:
                        session.commit()
                        logger.info(f"Committed batch: {stats}")
                session.commit()
                logger.info(f"Polygon update complete: {stats}")
        except Exception as e:
            logger.error(f"Error updating database: {e}")
            raise

    def _match_polygon_to_native(self, ticker: str, symbol_data: dict[str, Any]) -> str | None:
        """Match Polygon ticker to native symbol format.

        Args:
            ticker: Polygon ticker symbol (e.g., X:BTCUSD, C:EURUSD).
            symbol_data: Symbol metadata from Polygon API.

        Returns:
            Native symbol format or None if no match found.
        """
        base_currency = symbol_data.get("base_currency_symbol")
        quote_currency = symbol_data.get("currency_symbol")
        if ticker.startswith("X:"):
            if base_currency and quote_currency:
                return f"{base_currency.upper()}-{quote_currency.upper()}"
            pair = ticker[2:]
            if len(pair) >= 6:
                base = pair[:3]
                quote = pair[3:6]
                return f"{base.upper()}-{quote.upper()}"
        elif ticker.startswith("C:"):
            if base_currency and quote_currency:
                return f"{base_currency.upper()}-{quote_currency.upper()}"
            pair = ticker[2:]
            if len(pair) == 6:
                base = pair[:3]
                quote = pair[3:]
                return f"{base.upper()}-{quote.upper()}"
        elif ticker.startswith("I:"):
            return ticker[2:]
        else:
            return ticker
        return None
