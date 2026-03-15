"""Polygon.io symbol updater service.

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
from snapper.application.updaters.symbols.base import SymbolUpdaterService
from snapper.config.settings import AppSettings
from snapper.core.types import AssetType
from snapper.data.models import Symbol
from snapper.infrastructure.exchanges.implementations.polygon import PolygonExchangeClient


@register_process(
    "polygon_symbol_updater",
    method="start",
    description="Polygon symbol updater",
    priority=11,
    lifecycle=ProcessLifecycleEnum.ONE_SHOT,
    role=ProcessRoleEnum.TASK,
    tags=("maintenance", "symbols", "polygon"),
    enabled=True,
    mode="thread",
    args=[],
)
class PolygonSymbolUpdaterService(SymbolUpdaterService[PolygonExchangeClient]):
    """Service for updating Polygon symbol mappings from REST API."""

    BATCH_COMMIT_SIZE: int = 1000

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
        return "polygon_symbols_last_update"

    @staticmethod
    def _split_native_symbol(
        native_symbol: str, symbol_data: dict[str, Any]
    ) -> tuple[str, str | None]:
        """Split a native symbol into base and quote components.

        For pair symbols (containing '-'), splits on the separator.
        For single-ticker symbols, extracts quote from symbol metadata.

        Args:
            native_symbol: Native symbol string.
            symbol_data: Symbol metadata from Polygon API.

        Returns:
            Tuple of (base currency, quote currency or None).
        """
        if "-" in native_symbol:
            base, quote = native_symbol.split("-", 1)
            return base, quote
        currency: str = symbol_data.get("currency_symbol") or symbol_data.get("currency_name", "")
        quote_value: str | None = currency.upper() if currency else None
        return native_symbol, quote_value

    def _determine_polygon_asset_type(self, ticker: str) -> AssetType:
        """Determine asset type from Polygon ticker prefix.

        Args:
            ticker: Polygon ticker (e.g., ``X:BTCUSD``, ``C:EURUSD``, ``I:SPX``).

        Returns:
            Asset type string: crypto, forex, index, or equity.
        """
        if ticker.startswith("X:"):
            return "crypto"
        if ticker.startswith("C:"):
            return "forex"
        if ticker.startswith("I:"):
            return "index"
        return "equity"

    def _upsert_polygon_mapping(
        self,
        session: Any,
        native_symbol: str,
        ticker: str,
        base: str,
        quote: str | None,
        now: datetime,
        stats: dict[str, int],
    ) -> str | None:
        """Insert or update Polygon symbol identity and alias rows.

        When ``insert_new`` is False, only updates aliases for symbols that
        already have a symbol identity row. New symbols are skipped.

        Args:
            session: SQLAlchemy session.
            native_symbol: Native symbol string.
            ticker: Polygon ticker string.
            base: Base currency code.
            quote: Quote currency code or None.
            now: Current timestamp for created_at/updated_at.
            stats: Mutable stats dict to increment counters.

        Returns:
            The symbol public_id, or None if skipped.
        """
        existing_symbol = session.execute(
            select(Symbol).where(
                Symbol.native_symbol == native_symbol,
                Symbol.timestamp <= now,
                Symbol.known_to > now,
            )
        ).scalar_one_or_none()
        if existing_symbol is None and not self.insert_new:
            stats["skipped"] += 1
            return None
        asset_type = self._determine_polygon_asset_type(ticker)
        symbol_public_id = self._upsert_symbol(
            session,
            native_symbol,
            base,
            quote,
            asset_type,
            now,
        )
        alias_result = self._upsert_alias(
            session,
            symbol_public_id,
            "polygon",
            "rest",
            ticker,
            now,
        )
        self._upsert_capability(
            session,
            symbol_public_id,
            "polygon",
            True,
            False,
            "polygon_updater",
            None,
            now,
        )
        if alias_result == "created":
            stats["inserted"] += 1
        elif alias_result == "updated":
            stats["updated"] += 1
        return symbol_public_id

    async def _update_database(self, symbols: list[dict[str, Any]]) -> None:
        """Update database with fetched symbol data.

        Args:
            symbols: List of symbol dictionaries from Polygon API.
        """
        assert self.repository is not None
        stats = {"updated": 0, "inserted": 0, "skipped": 0}
        now = datetime.now(UTC)
        processed_symbol_public_ids: set[str] = set()
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
                    base, quote = self._split_native_symbol(native_symbol, symbol_data)
                    spid = self._upsert_polygon_mapping(
                        session, native_symbol, ticker, base, quote, now, stats
                    )
                    if spid is not None:
                        processed_symbol_public_ids.add(spid)
                    total_processed = stats["updated"] + stats["inserted"]
                    if total_processed % self.BATCH_COMMIT_SIZE == 0 and total_processed > 0:
                        session.commit()
                        logger.info(f"Committed batch: {stats}")
                deactivated = self._reconcile_capabilities(
                    session, "polygon", processed_symbol_public_ids, "polygon_updater", now
                )
                session.commit()
                stats["deactivated"] = deactivated
                logger.info(f"Polygon update complete: {stats}")
        except Exception as e:
            logger.error(f"Error updating database: {e}")
            raise

    @staticmethod
    def _native_from_currencies(
        base_currency: str | None, quote_currency: str | None
    ) -> str | None:
        """Build native symbol from base and quote currency if both present.

        Args:
            base_currency: Base currency symbol.
            quote_currency: Quote currency symbol.

        Returns:
            Native symbol string or None if either currency is missing.
        """
        if base_currency and quote_currency:
            return f"{base_currency.upper()}-{quote_currency.upper()}"
        return None

    @staticmethod
    def _native_from_crypto_pair(pair: str) -> str | None:
        """Parse crypto pair string (X: prefix removed) into native format.

        Args:
            pair: Ticker string after removing the X: prefix.

        Returns:
            Native symbol or None if pair is too short.
        """
        if len(pair) >= 6:
            return f"{pair[:3].upper()}-{pair[3:6].upper()}"
        return None

    @staticmethod
    def _native_from_forex_pair(pair: str) -> str | None:
        """Parse forex pair string (C: prefix removed) into native format.

        Args:
            pair: Ticker string after removing the C: prefix.

        Returns:
            Native symbol or None if pair length is not exactly 6.
        """
        if len(pair) == 6:
            return f"{pair[:3].upper()}-{pair[3:].upper()}"
        return None

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
            return self._native_from_currencies(base_currency, quote_currency) or (
                self._native_from_crypto_pair(ticker[2:])
            )
        if ticker.startswith("C:"):
            return self._native_from_currencies(base_currency, quote_currency) or (
                self._native_from_forex_pair(ticker[2:])
            )
        if ticker.startswith("I:"):
            return ticker[2:]
        return ticker
