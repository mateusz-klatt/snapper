"""Symbol mapping service with database-backed caching.

This module provides the SymbolMapperService singleton that manages bidirectional
symbol mappings between Snapper's native format and exchange-specific formats.
Mappings are loaded from the database and cached in memory for fast lookups.

The native symbol format is ``BASE-QUOTE`` (e.g., ``BTC-USD``), while
exchange-specific formats vary:
    - Kraken WebSocket: ``XBT/USD``
    - Kraken REST: ``XBTUSD``
    - CCXT: ``BTC/USD``
    - Zonda: ``BTC-USD``
    - Walutomat: ``BTCUSD``
    - Polygon: ``C:BTCUSD`` (crypto) or ``X:EURUSD`` (forex)

Example:
    >>> mapper = SymbolMapperService.get_instance()
    >>> mapper.native_to_ws["BTC-USD"]
    "XBT/USD"
    >>> mapper.ws_to_native["XBT/USD"]
    "BTC-USD"
"""

from loguru import logger
from sqlalchemy import select
from sqlalchemy.exc import OperationalError

from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.data.models import SymbolMapping
from snapper.data.repository import DatabaseRepository


def _get_bootstrap_settings() -> BootstrapSettingsLoader:
    """Get bootstrap settings loader for database configuration.

    Returns:
        BootstrapSettingsLoader instance with database URL configuration.
    """
    return BootstrapSettingsLoader()


__all__ = ["SymbolMapperService", "make_native_symbol", "NATIVE_SEPARATOR"]
NATIVE_SEPARATOR = "-"


def make_native_symbol(base: str, quote: str) -> str:
    """Create a native symbol from base and quote currencies.

    Constructs a normalized symbol string in Snapper's native format by
    uppercasing and joining the currencies with the native separator.

    Args:
        base: Base currency code (e.g., ``BTC``, ``ETH``).
        quote: Quote currency code (e.g., ``USD``, ``EUR``).

    Returns:
        Native symbol string in ``BASE-QUOTE`` format.

    Raises:
        ValueError: If either base or quote is empty after stripping whitespace.

    Example:
        >>> make_native_symbol("btc", "usd")
        "BTC-USD"
    """
    base_clean = base.strip().upper()
    quote_clean = quote.strip().upper()
    if not base_clean or not quote_clean:
        raise ValueError("Base and quote currencies must be non-empty")
    return f"{base_clean}{NATIVE_SEPARATOR}{quote_clean}"


class SymbolMapperService:
    """Singleton service for symbol mapping across exchanges.

    Maintains bidirectional mapping dictionaries between Snapper's native
    symbol format and various exchange-specific formats. Mappings are
    loaded from the database on initialization and cached in memory.

    The singleton pattern ensures consistent mappings across the application
    and efficient memory usage.

    Attributes:
        repository: Database repository for loading symbol mappings.
        native_to_ws: Native to Kraken WebSocket symbol mapping.
        native_to_rest: Native to Kraken REST symbol mapping.
        native_to_ccxt: Native to CCXT symbol mapping.
        native_to_zonda: Native to Zonda symbol mapping.
        native_to_walutomat: Native to Walutomat WebSocket symbol mapping.
        native_to_walutomat_rest: Native to Walutomat REST symbol mapping.
        native_to_polygon: Native to Polygon symbol mapping.
        ws_to_native: Kraken WebSocket to native symbol mapping.
        rest_to_native: Kraken REST to native symbol mapping.
        ccxt_to_native: CCXT to native symbol mapping.
        zonda_to_native: Zonda to native symbol mapping.
        walutomat_to_native: Walutomat WebSocket to native symbol mapping.
        walutomat_rest_to_native: Walutomat REST to native symbol mapping.
        polygon_to_native: Polygon to native symbol mapping.

    Example:
        >>> mapper = SymbolMapperService.get_instance()
        >>> kraken_ws = mapper.native_to_ws.get("BTC-USD")
        >>> native = mapper.ws_to_native.get("XBT/USD")
    """

    _instance: "SymbolMapperService | None" = None
    _initialized: bool = False

    def __new__(cls) -> "SymbolMapperService":
        """Create or return the singleton instance.

        Returns:
            The singleton SymbolMapperService instance.
        """
        if cls._instance is None:
            instance = super().__new__(cls)
            cls._instance = instance
        return cls._instance

    def __init__(self) -> None:
        """Initialize the service with database mappings.

        Loads symbol mappings from the database on first initialization.
        Subsequent calls are no-ops due to singleton pattern.

        Raises:
            Exception: If database connection or mapping load fails.
        """
        if self._initialized:
            return
        self._initialized = True
        settings = _get_bootstrap_settings()
        self.repository = DatabaseRepository(settings.db_url)
        self.native_to_ws: dict[str, str] = {}
        self.native_to_rest: dict[str, str] = {}
        self.native_to_ccxt: dict[str, str] = {}
        self.ws_to_native: dict[str, str] = {}
        self.rest_to_native: dict[str, str] = {}
        self.ccxt_to_native: dict[str, str] = {}
        self.native_to_zonda: dict[str, str] = {}
        self.zonda_to_native: dict[str, str] = {}
        self.native_to_walutomat: dict[str, str] = {}
        self.walutomat_to_native: dict[str, str] = {}
        self.native_to_walutomat_rest: dict[str, str] = {}
        self.walutomat_rest_to_native: dict[str, str] = {}
        self.native_to_polygon: dict[str, str] = {}
        self.polygon_to_native: dict[str, str] = {}
        self._cache_loaded = False
        try:
            self.trigger_cache_invalidation(fail_fast=True)
            logger.info(f"SymbolMapperService warm-up: loaded {len(self.native_to_ws)} symbols")
        except Exception as e:
            logger.error(f"SymbolMapperService warm-up failed: {e}")
            raise

    def load_mappings_from_db(self) -> list[SymbolMapping]:
        """Load all symbol mappings from the database.

        Queries the symbol_mappings table for all defined mappings. If the
        table doesn't exist (before migrations), returns an empty list.

        Returns:
            List of SymbolMapping ORM objects from the database.

        Raises:
            OperationalError: If database error occurs (except missing table).
        """
        try:
            with self.repository.get_session() as session:
                stmt = select(SymbolMapping)
                result = session.execute(stmt)
                mappings = result.scalars().all()
                logger.info(f"Loaded {len(mappings)} symbol mappings from database")
                return list(mappings)
        except OperationalError as exc:
            error_message = str(exc).lower()
            if "no such table" in error_message and "symbol_mappings" in error_message:
                logger.warning(
                    "Symbol mappings table missing; skipping cache warm-up until migrations finish."
                )
                return []
            raise

    def load_cache_if_needed(self, fail_fast: bool = False) -> None:
        """Load symbol mappings into cache if not already loaded.

        Populates all bidirectional mapping dictionaries from database
        records. Handles both currency pair symbols and single-ticker
        symbols (stocks).

        Args:
            fail_fast: If True, re-raise exceptions on load failure.
                If False, continue with existing cache on error.
        """
        if self._cache_loaded:
            return
        try:
            mappings = self.load_mappings_from_db()
            new_native_to_ws: dict[str, str] = {}
            new_native_to_rest: dict[str, str] = {}
            new_native_to_ccxt: dict[str, str] = {}
            new_ws_to_native: dict[str, str] = {}
            new_rest_to_native: dict[str, str] = {}
            new_ccxt_to_native: dict[str, str] = {}
            new_native_to_zonda: dict[str, str] = {}
            new_zonda_to_native: dict[str, str] = {}
            new_native_to_walutomat: dict[str, str] = {}
            new_walutomat_to_native: dict[str, str] = {}
            new_native_to_walutomat_rest: dict[str, str] = {}
            new_walutomat_rest_to_native: dict[str, str] = {}
            new_native_to_polygon: dict[str, str] = {}
            new_polygon_to_native: dict[str, str] = {}
            for mapping in mappings:
                polygon_symbol = mapping.polygon_symbol or ""
                is_polygon_pair = polygon_symbol.startswith(("C:", "X:"))
                if mapping.quote_currency and (is_polygon_pair or not polygon_symbol):
                    native_symbol = make_native_symbol(
                        mapping.base_currency, mapping.quote_currency
                    )
                else:
                    native_symbol = mapping.base_currency
                if mapping.kraken_websocket_symbol:
                    new_native_to_ws[native_symbol] = mapping.kraken_websocket_symbol
                    new_ws_to_native[mapping.kraken_websocket_symbol] = native_symbol
                if mapping.kraken_rest_symbol:
                    new_native_to_rest[native_symbol] = mapping.kraken_rest_symbol
                    new_rest_to_native[mapping.kraken_rest_symbol] = native_symbol
                if mapping.ccxt_symbol:
                    new_native_to_ccxt[native_symbol] = mapping.ccxt_symbol
                    new_ccxt_to_native[mapping.ccxt_symbol] = native_symbol
                if mapping.zonda_symbol:
                    new_native_to_zonda[native_symbol] = mapping.zonda_symbol
                    new_zonda_to_native[mapping.zonda_symbol] = native_symbol
                if mapping.walutomat_symbol:
                    new_native_to_walutomat[native_symbol] = mapping.walutomat_symbol
                    new_walutomat_to_native[mapping.walutomat_symbol] = native_symbol
                if mapping.walutomat_rest_symbol:
                    new_native_to_walutomat_rest[native_symbol] = mapping.walutomat_rest_symbol
                    new_walutomat_rest_to_native[mapping.walutomat_rest_symbol] = native_symbol
                if mapping.polygon_symbol:
                    new_native_to_polygon[native_symbol] = mapping.polygon_symbol
                    new_polygon_to_native[mapping.polygon_symbol] = native_symbol
            self.native_to_ws = new_native_to_ws
            self.native_to_rest = new_native_to_rest
            self.native_to_ccxt = new_native_to_ccxt
            self.ws_to_native = new_ws_to_native
            self.rest_to_native = new_rest_to_native
            self.ccxt_to_native = new_ccxt_to_native
            self.native_to_zonda = new_native_to_zonda
            self.zonda_to_native = new_zonda_to_native
            self.native_to_walutomat = new_native_to_walutomat
            self.walutomat_to_native = new_walutomat_to_native
            self.native_to_walutomat_rest = new_native_to_walutomat_rest
            self.walutomat_rest_to_native = new_walutomat_rest_to_native
            self.native_to_polygon = new_native_to_polygon
            self.polygon_to_native = new_polygon_to_native
            logger.info(f"Loaded symbol maps cache with {len(self.native_to_ws)} native symbols")
        except Exception as e:
            logger.error(f"Error loading symbol maps cache: {e}")
            if fail_fast:
                raise
            else:
                logger.warning("Continuing with existing cache due to DB error")
        finally:
            self._cache_loaded = True

    def trigger_cache_invalidation(self, fail_fast: bool = False) -> None:
        """Invalidate and reload the symbol mapping cache.

        Marks the cache as stale and triggers a fresh load from the database.
        Useful after symbol mapping updates in the database.

        Args:
            fail_fast: If True, re-raise exceptions on reload failure.
        """
        self._cache_loaded = False
        self.load_cache_if_needed(fail_fast=fail_fast)

    @classmethod
    def get_instance(cls) -> "SymbolMapperService":
        """Get or create the singleton instance.

        Returns:
            The singleton SymbolMapperService instance.
        """
        if cls._instance is None:
            cls._instance = SymbolMapperService()
        return cls._instance

    @classmethod
    def clear_instance(cls) -> None:
        """Clear the singleton instance.

        Resets the singleton state, allowing a fresh instance to be created
        on next access. Useful for testing.
        """
        cls._instance = None
