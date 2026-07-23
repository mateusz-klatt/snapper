"""Symbol mapping service with database-backed caching.

This module provides the SymbolMapperService singleton that manages bidirectional
symbol mappings between Snapper's native format and exchange-specific formats.
Mappings are loaded from the database and cached in memory for fast lookups.

The native symbol format is ``BASE-QUOTE`` (e.g., ``BTC-USD``), while
exchange-specific formats vary:
    - Kraken WebSocket: ``XBT/USD``
    - Kraken REST: ``XBTUSD``
    - CCXT: ``BTC/USD``
    - Walutomat: ``BTCUSD``
    - Polygon: ``C:BTCUSD`` (crypto) or ``X:EURUSD`` (forex)

Example:
    >>> mapper = SymbolMapperService.get_instance()
    >>> mapper.to_exchange("BTC-USD", "kraken", "ws")
    "XBT/USD"
    >>> mapper.to_native("XBT/USD", "kraken", "ws")
    "BTC-USD"
"""

from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from typing import NamedTuple

from loguru import logger
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import ProgrammingError

from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.core.types import AliasChannelEnum
from snapper.core.types import ExchangeEnum
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolExchangeCapability
from snapper.data.models import SymbolMarketDataChannelCapability
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


class ChannelCapabilityInfo(NamedTuple):
    """Cached channel capability data for a (native_symbol, exchange, channel) key.

    Attributes:
        can_market_data: Whether this market-data channel is enabled.
        source: Origin of the channel capability information.
        reason: Human-readable explanation for the channel capability value.
    """

    can_market_data: bool
    source: str | None
    reason: str | None


def _is_missing_channel_capability_table_error(
    exc: OperationalError | ProgrammingError,
) -> bool:
    """Return True when channel capability loading raced a missing table."""
    orig = getattr(exc, "orig", None)
    sqlstates = {
        getattr(orig, "sqlstate", None),
        getattr(orig, "pgcode", None),
        getattr(getattr(orig, "diag", None), "sqlstate", None),
    }
    if "42P01" in sqlstates:
        return True
    error_message = str(orig if orig is not None else exc).strip().lower()
    table_names = ("symbol_market_data_channel_capabilities", "symbols")
    return error_message in {f"no such table: {table_name}" for table_name in table_names}


def _get_bootstrap_settings() -> BootstrapSettingsLoader:
    """Get bootstrap settings loader for database configuration.

    Returns:
        BootstrapSettingsLoader instance with database URL configuration.
    """
    return BootstrapSettingsLoader()


__all__ = [
    "SymbolMapperService",
    "make_native_symbol",
    "NATIVE_SEPARATOR",
    "CapabilityInfo",
    "ChannelCapabilityInfo",
]
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


_SHORTCUT_FORWARD: tuple[tuple[str, str, str], ...] = (
    (ExchangeEnum.KRAKEN, AliasChannelEnum.WS, "native_to_kraken_ws"),
    (ExchangeEnum.KRAKEN, AliasChannelEnum.REST, "native_to_kraken_rest"),
    (ExchangeEnum.KRAKEN_FUTURES, AliasChannelEnum.WS, "native_to_kraken_futures_ws"),
    (ExchangeEnum.KRAKEN_EQUITIES, AliasChannelEnum.WS, "native_to_kraken_equities_ws"),
    (ExchangeEnum.WALUTOMAT, AliasChannelEnum.WS, "native_to_walutomat_ws"),
    (ExchangeEnum.WALUTOMAT, AliasChannelEnum.REST, "native_to_walutomat_rest"),
    (ExchangeEnum.POLYGON, AliasChannelEnum.REST, "native_to_polygon_rest"),
)

_SHORTCUT_REVERSE: tuple[tuple[str, str, str], ...] = (
    (ExchangeEnum.KRAKEN, AliasChannelEnum.WS, "kraken_ws_to_native"),
    (ExchangeEnum.KRAKEN, AliasChannelEnum.REST, "kraken_rest_to_native"),
    (ExchangeEnum.KRAKEN_FUTURES, AliasChannelEnum.WS, "kraken_futures_ws_to_native"),
    (ExchangeEnum.KRAKEN_EQUITIES, AliasChannelEnum.WS, "kraken_equities_ws_to_native"),
    (ExchangeEnum.WALUTOMAT, AliasChannelEnum.WS, "walutomat_ws_to_native"),
    (ExchangeEnum.WALUTOMAT, AliasChannelEnum.REST, "walutomat_rest_to_native"),
    (ExchangeEnum.POLYGON, AliasChannelEnum.REST, "polygon_rest_to_native"),
)


_invalidation_callbacks: list[Callable[[], None]] = []


def register_invalidation_callback(callback: Callable[[], None]) -> None:
    """Register a callback to fire on every ``trigger_cache_invalidation``.

    Used by downstream module-level caches (most importantly the
    cross-exchange ``get_available_symbols_set()`` frozenset hit by
    publish-time topic validation) to stay in sync when the mapper
    reloads its alias cache. Registering twice is idempotent — duplicate
    callbacks are skipped so reload paths in tests do not re-add.

    Args:
        callback: Zero-arg function to invoke after every mapper reload.
    """
    if callback not in _invalidation_callbacks:
        _invalidation_callbacks.append(callback)


class SymbolMapperService:
    """Singleton service for symbol mapping across exchanges.

    Maintains bidirectional mapping dictionaries between Snapper's native
    symbol format and various exchange-specific formats. Mappings are
    loaded from the database on initialization and cached in memory.

    The singleton pattern ensures consistent mappings across the application
    and efficient memory usage.

    The canonical data store is ``forward`` and ``reverse`` dicts keyed by
    ``(exchange, channel)`` tuples. Shortcut attributes follow the naming
    convention ``native_to_{exchange}_{channel}`` / ``{exchange}_{channel}_to_native``
    and provide direct references into those dicts so that callers in
    ``functions.py`` can map symbols with minimal arguments.

    Attributes:
        repository: Database repository for loading symbol aliases.
        forward: Native-to-exchange maps keyed by ``(exchange, channel)``.
        reverse: Exchange-to-native maps keyed by ``(exchange, channel)``.
        native_to_kraken_ws: Alias for ``forward[("kraken", "ws")]``.
        native_to_kraken_rest: Alias for ``forward[("kraken", "rest")]``.
        native_to_kraken_futures_ws: Alias for ``forward[("kraken_futures", "ws")]``.
        native_to_kraken_equities_ws: Alias for ``forward[("kraken_equities", "ws")]``.
        native_to_ccxt: Union of all ``forward[(*, "ccxt")]`` across exchanges.
        native_to_walutomat_ws: Alias for ``forward[("walutomat", "ws")]``.
        native_to_walutomat_rest: Alias for ``forward[("walutomat", "rest")]``.
        native_to_polygon_rest: Alias for ``forward[("polygon", "rest")]``.
        kraken_ws_to_native: Alias for ``reverse[("kraken", "ws")]``.
        kraken_rest_to_native: Alias for ``reverse[("kraken", "rest")]``.
        kraken_futures_ws_to_native: Alias for ``reverse[("kraken_futures", "ws")]``.
        kraken_equities_ws_to_native: Alias for ``reverse[("kraken_equities", "ws")]``.
        ccxt_to_native: Union of all ``reverse[(*, "ccxt")]`` across exchanges.
        walutomat_ws_to_native: Alias for ``reverse[("walutomat", "ws")]``.
        walutomat_rest_to_native: Alias for ``reverse[("walutomat", "rest")]``.
        polygon_rest_to_native: Alias for ``reverse[("polygon", "rest")]``.

    Example:
        >>> mapper = SymbolMapperService.get_instance()
        >>> mapper.to_exchange("BTC-USD", "kraken", "ws")
        "XBT/USD"
        >>> mapper.to_native("XBT/USD", "kraken", "ws")
        "BTC-USD"
    """

    _instance: SymbolMapperService | None = None
    _initialized: bool = False

    def __new__(cls) -> SymbolMapperService:
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
        self.native_to_kraken_ws: dict[str, str] = {}
        self.native_to_kraken_rest: dict[str, str] = {}
        self.native_to_kraken_futures_ws: dict[str, str] = {}
        self.native_to_kraken_equities_ws: dict[str, str] = {}
        self.native_to_ccxt: dict[str, str] = {}
        self.kraken_ws_to_native: dict[str, str] = {}
        self.kraken_rest_to_native: dict[str, str] = {}
        self.kraken_futures_ws_to_native: dict[str, str] = {}
        self.kraken_equities_ws_to_native: dict[str, str] = {}
        self.ccxt_to_native: dict[str, str] = {}
        self.native_to_walutomat_ws: dict[str, str] = {}
        self.walutomat_ws_to_native: dict[str, str] = {}
        self.native_to_walutomat_rest: dict[str, str] = {}
        self.walutomat_rest_to_native: dict[str, str] = {}
        self.native_to_polygon_rest: dict[str, str] = {}
        self.polygon_rest_to_native: dict[str, str] = {}
        self.capabilities: dict[tuple[str, str], CapabilityInfo] = {}
        self.channel_capabilities: dict[tuple[str, str, str], ChannelCapabilityInfo] = {}
        self._cache_loaded = False
        try:
            self.trigger_cache_invalidation(fail_fast=True)
            logger.info(
                f"SymbolMapperService warm-up: loaded {len(self.native_to_kraken_ws)} symbols"
            )
        except Exception as e:
            logger.error(f"SymbolMapperService warm-up failed: {e}")
            raise

    def load_mappings_from_db(self) -> list[tuple[str, str, str, str]]:
        """Load all symbol aliases joined with active Symbol native_symbol.

        Queries the symbol_aliases table joined with symbols to resolve
        symbol_public_id to native_symbol. If the table doesn't exist
        (before migrations), returns an empty list.

        Returns:
            List of (native_symbol, exchange, channel, exchange_symbol) tuples.

        Raises:
            OperationalError: If database error occurs (except missing table).
        """
        try:
            with self.repository.get_session() as session:
                now = datetime.now(UTC)
                stmt = (
                    select(
                        Symbol.native_symbol,
                        SymbolAlias.exchange,
                        SymbolAlias.channel,
                        SymbolAlias.exchange_symbol,
                    )
                    .join(
                        Symbol,
                        Symbol.public_id == SymbolAlias.symbol_public_id,
                    )
                    .where(
                        SymbolAlias.timestamp <= now,
                        SymbolAlias.known_to > now,
                        Symbol.timestamp <= now,
                        Symbol.known_to > now,
                    )
                )
                rows = session.execute(stmt).all()
                logger.info(f"Loaded {len(rows)} symbol aliases from database")
                return [(r[0], r[1], r[2], r[3]) for r in rows]
        except OperationalError as exc:
            error_message = str(exc).lower()
            aliases_table_missing = "symbol_aliases" in error_message or "symbols" in error_message
            if "no such table" in error_message and aliases_table_missing:
                logger.warning(
                    "Symbol aliases table missing; skipping cache warm-up until migrations finish."
                )
                return []
            raise

    def _populate_maps_from_aliases(
        self,
        alias_rows: list[tuple[str, str, str, str]],
    ) -> None:
        """Populate all bidirectional mapping dicts from alias tuples.

        Builds fresh forward and reverse dicts keyed by ``(exchange, channel)``
        and updates shortcut named attributes atomically.

        Args:
            alias_rows: List of (native_symbol, exchange, channel, exchange_symbol).
        """
        fwd: dict[tuple[str, str], dict[str, str]] = {}
        rev: dict[tuple[str, str], dict[str, str]] = {}
        for native_symbol, exchange, channel, exchange_symbol in alias_rows:
            key = (exchange, channel)
            fwd.setdefault(key, {})[native_symbol] = exchange_symbol
            rev.setdefault(key, {})[exchange_symbol] = native_symbol
        self.forward = fwd
        self.reverse = rev
        for exchange, channel, attr_name in _SHORTCUT_FORWARD:
            setattr(self, attr_name, fwd.get((exchange, channel), {}))
        for exchange, channel, attr_name in _SHORTCUT_REVERSE:
            setattr(self, attr_name, rev.get((exchange, channel), {}))
        ccxt_fwd: dict[str, str] = {}
        ccxt_rev: dict[str, str] = {}
        for (_ex, ch), mapping in fwd.items():
            if ch == AliasChannelEnum.CCXT:
                ccxt_fwd.update(mapping)
        for (_ex, ch), mapping in rev.items():
            if ch == AliasChannelEnum.CCXT:
                ccxt_rev.update(mapping)
        self.native_to_ccxt = ccxt_fwd
        self.ccxt_to_native = ccxt_rev

    def load_capabilities_from_db(
        self,
    ) -> list[tuple[str, str, bool, bool, str | None, str | None]]:
        """Load all symbol exchange capabilities joined with active Symbol.

        Queries the symbol_exchange_capabilities table joined with symbols
        to resolve symbol_public_id to native_symbol. If the table
        does not exist (before migrations), returns an empty list.

        Returns:
            List of (native_symbol, exchange, can_market_data, can_trade,
            source, reason) tuples.

        Raises:
            OperationalError: If database error occurs (except missing table).
        """
        try:
            with self.repository.get_session() as session:
                now = datetime.now(UTC)
                stmt = (
                    select(
                        Symbol.native_symbol,
                        SymbolExchangeCapability.exchange,
                        SymbolExchangeCapability.can_market_data,
                        SymbolExchangeCapability.can_trade,
                        SymbolExchangeCapability.source,
                        SymbolExchangeCapability.reason,
                    )
                    .join(
                        Symbol,
                        Symbol.public_id == SymbolExchangeCapability.symbol_public_id,
                    )
                    .where(
                        SymbolExchangeCapability.timestamp <= now,
                        SymbolExchangeCapability.known_to > now,
                        Symbol.timestamp <= now,
                        Symbol.known_to > now,
                    )
                )
                rows = session.execute(stmt).all()
                logger.info(f"Loaded {len(rows)} symbol capabilities from database")
                return [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows]
        except OperationalError as exc:
            error_message = str(exc).lower()
            capabilities_table_missing = "symbol_exchange_capabilities" in error_message
            symbols_table_missing = "symbols" in error_message
            related_table_missing = capabilities_table_missing or symbols_table_missing
            if "no such table" in error_message and related_table_missing:
                logger.warning(
                    "Symbol capabilities table missing; skipping until migrations finish."
                )
                return []
            raise

    def _populate_capabilities_from_rows(
        self,
        rows: list[tuple[str, str, bool, bool, str | None, str | None]],
    ) -> None:
        """Populate capabilities cache from joined result tuples.

        Args:
            rows: List of (native_symbol, exchange, can_market_data,
                  can_trade, source, reason) tuples.
        """
        caps: dict[tuple[str, str], CapabilityInfo] = {}
        for native_symbol, exchange, can_market_data, can_trade, source, reason in rows:
            caps[(native_symbol, exchange)] = CapabilityInfo(
                can_market_data=can_market_data,
                can_trade=can_trade,
                source=source,
                reason=reason,
            )
        self.capabilities = caps

    def load_channel_capabilities_from_db(
        self,
    ) -> list[tuple[str, str, str, bool, str | None, str | None]]:
        """Load all active channel capabilities joined with active Symbol.

        Returns an empty list when the table does not exist yet so a newly
        deployed process can start before the migration has run.

        Returns:
            List of (native_symbol, exchange, channel, can_market_data,
            source, reason) tuples.

        Raises:
            OperationalError: If database error occurs except missing table.
            ProgrammingError: If database error occurs except missing table.
        """
        try:
            with self.repository.get_session() as session:
                now = datetime.now(UTC)
                stmt = (
                    select(
                        Symbol.native_symbol,
                        SymbolMarketDataChannelCapability.exchange,
                        SymbolMarketDataChannelCapability.channel,
                        SymbolMarketDataChannelCapability.can_market_data,
                        SymbolMarketDataChannelCapability.source,
                        SymbolMarketDataChannelCapability.reason,
                    )
                    .join(
                        Symbol,
                        Symbol.public_id == SymbolMarketDataChannelCapability.symbol_public_id,
                    )
                    .where(
                        SymbolMarketDataChannelCapability.timestamp <= now,
                        SymbolMarketDataChannelCapability.known_to > now,
                        Symbol.timestamp <= now,
                        Symbol.known_to > now,
                    )
                )
                rows = session.execute(stmt).all()
                logger.info(f"Loaded {len(rows)} symbol channel capabilities from database")
                return [(r[0], r[1], r[2], r[3], r[4], r[5]) for r in rows]
        except (OperationalError, ProgrammingError) as exc:
            if _is_missing_channel_capability_table_error(exc):
                logger.warning(
                    "Symbol channel capabilities table missing; skipping until migrations finish."
                )
                return []
            raise

    def _populate_channel_capabilities_from_rows(
        self,
        rows: list[tuple[str, str, str, bool, str | None, str | None]],
    ) -> None:
        """Populate channel capabilities cache from joined result tuples.

        Args:
            rows: List of (native_symbol, exchange, channel,
                can_market_data, source, reason) tuples.
        """
        caps: dict[tuple[str, str, str], ChannelCapabilityInfo] = {}
        for native_symbol, exchange, channel, can_market_data, source, reason in rows:
            caps[(native_symbol, exchange, channel)] = ChannelCapabilityInfo(
                can_market_data=can_market_data,
                source=source,
                reason=reason,
            )
        self.channel_capabilities = caps

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
            channel_capabilities = self.load_channel_capabilities_from_db()
            self._populate_channel_capabilities_from_rows(channel_capabilities)
            logger.info(
                f"Loaded symbol maps cache with {len(self.native_to_kraken_ws)} native symbols "
                f"and {len(self.capabilities)} capabilities "
                f"and {len(self.channel_capabilities)} channel capabilities"
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
        Useful after symbol alias updates in the database. Also fires every
        registered downstream invalidation callback (see
        :func:`register_invalidation_callback`) so module-level caches that
        depend on the mapper's symbol universe stay in sync — notably the
        ``get_available_symbols_set()`` frozenset that publish-time topic
        validation hits per tick.

        Args:
            fail_fast: If True, re-raise exceptions on reload failure.
        """
        self._cache_loaded = False
        self.load_cache_if_needed(fail_fast=fail_fast)
        for callback in _invalidation_callbacks:
            callback()

    @classmethod
    def get_instance(cls) -> SymbolMapperService:
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
        instance = cls._instance
        cls._instance = None
        cls._initialized = False
        if instance is None:
            return
        repository = getattr(instance, "repository", None)
        dispose = getattr(repository, "dispose", None)
        if callable(dispose):
            try:
                dispose()
            except Exception as exc:
                logger.warning(f"Failed to dispose SymbolMapperService repository: {exc}")
