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
    >>> mapper.to_exchange("BTC-USD", "kraken", "ws")
    "XBT/USD"
    >>> mapper.to_native("XBT/USD", "kraken", "ws")
    "BTC-USD"
"""

from typing import NamedTuple

from loguru import logger
from sqlalchemy import select
from sqlalchemy.exc import OperationalError

from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolExchangeCapability
from snapper.data.repository import DatabaseRepository


class CapabilityInfo(NamedTuple):
    """Cached capability data for a (native_symbol, exchange) pair.

    Attributes:
        can_market_data: Whether exchange provides market data for this symbol.
        can_trade: Whether exchange supports trading this symbol.
        source: Origin of the capability information.
        reason: Human-readable explanation for the capability values.
    """

    can_market_data: bool
    can_trade: bool
    source: str | None
    reason: str | None


def _get_bootstrap_settings() -> BootstrapSettingsLoader:
    """Get bootstrap settings loader for database configuration.

    Returns:
        BootstrapSettingsLoader instance with database URL configuration.
    """
    return BootstrapSettingsLoader()


__all__ = ["SymbolMapperService", "make_native_symbol", "NATIVE_SEPARATOR", "CapabilityInfo"]
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


_COMPAT_FORWARD: tuple[tuple[str, str, str], ...] = (
    ("kraken", "ws", "native_to_ws"),
    ("kraken", "rest", "native_to_rest"),
    ("kraken", "ccxt", "native_to_ccxt"),
    ("zonda", "ws", "native_to_zonda"),
    ("walutomat", "ws", "native_to_walutomat"),
    ("walutomat", "rest", "native_to_walutomat_rest"),
    ("polygon", "rest", "native_to_polygon"),
)

_COMPAT_REVERSE: tuple[tuple[str, str, str], ...] = (
    ("kraken", "ws", "ws_to_native"),
    ("kraken", "rest", "rest_to_native"),
    ("kraken", "ccxt", "ccxt_to_native"),
    ("zonda", "ws", "zonda_to_native"),
    ("walutomat", "ws", "walutomat_to_native"),
    ("walutomat", "rest", "walutomat_rest_to_native"),
    ("polygon", "rest", "polygon_to_native"),
)


class SymbolMapperService:
    """Singleton service for symbol mapping across exchanges.

    Maintains bidirectional mapping dictionaries between Snapper's native
    symbol format and various exchange-specific formats. Mappings are
    loaded from the database on initialization and cached in memory.

    The singleton pattern ensures consistent mappings across the application
    and efficient memory usage.

    The canonical data store is ``forward`` and ``reverse`` dicts keyed by
    ``(exchange, channel)`` tuples. Legacy named attributes (``native_to_ws``,
    ``ws_to_native``, etc.) are kept as direct references into those dicts
    for backward compatibility with ``functions.py``.

    Attributes:
        repository: Database repository for loading symbol aliases.
        forward: Native-to-exchange maps keyed by ``(exchange, channel)``.
        reverse: Exchange-to-native maps keyed by ``(exchange, channel)``.
        native_to_ws: Alias for ``forward[("kraken", "ws")]``.
        native_to_rest: Alias for ``forward[("kraken", "rest")]``.
        native_to_ccxt: Alias for ``forward[("kraken", "ccxt")]``.
        native_to_zonda: Alias for ``forward[("zonda", "ws")]``.
        native_to_walutomat: Alias for ``forward[("walutomat", "ws")]``.
        native_to_walutomat_rest: Alias for ``forward[("walutomat", "rest")]``.
        native_to_polygon: Alias for ``forward[("polygon", "rest")]``.
        ws_to_native: Alias for ``reverse[("kraken", "ws")]``.
        rest_to_native: Alias for ``reverse[("kraken", "rest")]``.
        ccxt_to_native: Alias for ``reverse[("kraken", "ccxt")]``.
        zonda_to_native: Alias for ``reverse[("zonda", "ws")]``.
        walutomat_to_native: Alias for ``reverse[("walutomat", "ws")]``.
        walutomat_rest_to_native: Alias for ``reverse[("walutomat", "rest")]``.
        polygon_to_native: Alias for ``reverse[("polygon", "rest")]``.

    Example:
        >>> mapper = SymbolMapperService.get_instance()
        >>> mapper.to_exchange("BTC-USD", "kraken", "ws")
        "XBT/USD"
        >>> mapper.to_native("XBT/USD", "kraken", "ws")
        "BTC-USD"
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
        """Initialize the service with database aliases.

        Loads symbol aliases from the database on first initialization.
        Subsequent calls are no-ops due to singleton pattern.

        Raises:
            Exception: If database connection or alias load fails.
        """
        if self._initialized:
            return
        self._initialized = True
        settings = _get_bootstrap_settings()
        self.repository = DatabaseRepository(settings.db_url)
        self.forward: dict[tuple[str, str], dict[str, str]] = {}
        self.reverse: dict[tuple[str, str], dict[str, str]] = {}
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
        self.capabilities: dict[tuple[str, str], CapabilityInfo] = {}
        self._cache_loaded = False
        try:
            self.trigger_cache_invalidation(fail_fast=True)
            logger.info(f"SymbolMapperService warm-up: loaded {len(self.native_to_ws)} symbols")
        except Exception as e:
            logger.error(f"SymbolMapperService warm-up failed: {e}")
            raise

    def load_mappings_from_db(self) -> list[SymbolAlias]:
        """Load all symbol aliases from the database.

        Queries the symbol_aliases table for all defined aliases. If the
        table doesn't exist (before migrations), returns an empty list.

        Returns:
            List of SymbolAlias ORM objects from the database.

        Raises:
            OperationalError: If database error occurs (except missing table).
        """
        try:
            with self.repository.get_session() as session:
                stmt = select(SymbolAlias)
                result = session.execute(stmt)
                aliases = result.scalars().all()
                logger.info(f"Loaded {len(aliases)} symbol aliases from database")
                return list(aliases)
        except OperationalError as exc:
            error_message = str(exc).lower()
            if "no such table" in error_message and "symbol_aliases" in error_message:
                logger.warning(
                    "Symbol aliases table missing; skipping cache warm-up until migrations finish."
                )
                return []
            raise

    def _populate_maps_from_aliases(
        self,
        aliases: list[SymbolAlias],
    ) -> None:
        """Populate all bidirectional mapping dicts from alias rows.

        Builds fresh forward and reverse dicts keyed by ``(exchange, channel)``
        and updates backward-compatible named attributes atomically.

        Args:
            aliases: List of SymbolAlias ORM objects.
        """
        fwd: dict[tuple[str, str], dict[str, str]] = {}
        rev: dict[tuple[str, str], dict[str, str]] = {}
        for alias in aliases:
            key = (alias.exchange, alias.channel)
            fwd.setdefault(key, {})[alias.native_symbol] = alias.exchange_symbol
            rev.setdefault(key, {})[alias.exchange_symbol] = alias.native_symbol
        self.forward = fwd
        self.reverse = rev
        for exchange, channel, attr_name in _COMPAT_FORWARD:
            setattr(self, attr_name, fwd.get((exchange, channel), {}))
        for exchange, channel, attr_name in _COMPAT_REVERSE:
            setattr(self, attr_name, rev.get((exchange, channel), {}))

    def load_capabilities_from_db(self) -> list[SymbolExchangeCapability]:
        """Load all symbol exchange capabilities from the database.

        Queries the symbol_exchange_capabilities table. If the table
        does not exist (before migrations), returns an empty list.

        Returns:
            List of SymbolExchangeCapability ORM objects.

        Raises:
            OperationalError: If database error occurs (except missing table).
        """
        try:
            with self.repository.get_session() as session:
                stmt = select(SymbolExchangeCapability)
                result = session.execute(stmt)
                capabilities = result.scalars().all()
                logger.info(f"Loaded {len(capabilities)} symbol capabilities from database")
                return list(capabilities)
        except OperationalError as exc:
            error_message = str(exc).lower()
            if "no such table" in error_message and "symbol_exchange_capabilities" in error_message:
                logger.warning(
                    "Symbol capabilities table missing; skipping until migrations finish."
                )
                return []
            raise

    def _populate_capabilities_from_rows(
        self,
        rows: list[SymbolExchangeCapability],
    ) -> None:
        """Populate capabilities cache from database rows.

        Args:
            rows: List of SymbolExchangeCapability ORM objects.
        """
        caps: dict[tuple[str, str], CapabilityInfo] = {}
        for row in rows:
            caps[(row.native_symbol, row.exchange)] = CapabilityInfo(
                can_market_data=row.can_market_data,
                can_trade=row.can_trade,
                source=row.source,
                reason=row.reason,
            )
        self.capabilities = caps

    def to_exchange(self, native_symbol: str, exchange: str, channel: str) -> str:
        """Convert a native symbol to an exchange-specific format.

        Args:
            native_symbol: Native symbol (e.g., ``BTC-USD``).
            exchange: Exchange identifier (e.g., ``kraken``).
            channel: Channel identifier (``ws``, ``rest``, or ``ccxt``).

        Returns:
            Exchange-specific symbol string.

        Raises:
            ValueError: If no alias exists for the given combination.
        """
        fwd = self.forward.get((exchange, channel), {})
        result = fwd.get(native_symbol)
        if result is None:
            raise ValueError(f"No alias for {native_symbol} on {exchange}/{channel}")
        return result

    def to_native(self, exchange_symbol: str, exchange: str, channel: str) -> str:
        """Convert an exchange-specific symbol to native format.

        Args:
            exchange_symbol: Exchange symbol (e.g., ``XBT/USD``).
            exchange: Exchange identifier (e.g., ``kraken``).
            channel: Channel identifier (``ws``, ``rest``, or ``ccxt``).

        Returns:
            Native symbol string in ``BASE-QUOTE`` format.

        Raises:
            ValueError: If no alias exists for the given combination.
        """
        rev = self.reverse.get((exchange, channel), {})
        result = rev.get(exchange_symbol)
        if result is None:
            raise ValueError(f"No native symbol for {exchange_symbol} on {exchange}/{channel}")
        return result

    def load_cache_if_needed(self, fail_fast: bool = False) -> None:
        """Load symbol aliases and capabilities into cache if not already loaded.

        Populates all bidirectional mapping dictionaries and capability
        cache from database records.

        Args:
            fail_fast: If True, re-raise exceptions on load failure.
                If False, continue with existing cache on error.
        """
        if self._cache_loaded:
            return
        try:
            aliases = self.load_mappings_from_db()
            self._populate_maps_from_aliases(aliases)
            capabilities = self.load_capabilities_from_db()
            self._populate_capabilities_from_rows(capabilities)
            logger.info(
                f"Loaded symbol maps cache with {len(self.native_to_ws)} native symbols "
                f"and {len(self.capabilities)} capabilities"
            )
        except Exception as e:
            logger.error(f"Error loading symbol maps cache: {e}")
            if fail_fast:
                raise
            else:
                logger.warning("Continuing with existing cache due to DB error")
        finally:
            self._cache_loaded = True

    def trigger_cache_invalidation(self, fail_fast: bool = False) -> None:
        """Invalidate and reload the symbol alias cache.

        Marks the cache as stale and triggers a fresh load from the database.
        Useful after symbol alias updates in the database.

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
