"""Symbol conversion functions for multi-exchange support.

This module provides stateless functions for converting trading symbols between
Snapper's native format and exchange-specific formats. All functions delegate
to the SymbolMapperService singleton for actual lookups.
Functions are organized by exchange
    Kraken: WebSocket and REST symbol conversions
    CCXT: CCXT library symbol format
    Walutomat: Walutomat WebSocket and REST formats
    Polygon: Polygon.io ticker format
Each conversion direction has a corresponding function pair
    ``native_to_<exchange>`` for outbound conversion
    ``<exchange>_to_native`` for inbound conversion
Example
    >>> from snapper.infrastructure.symbols.functions import (
    native_to_kraken_websocket
    kraken_websocket_to_native
    >>> native_to_kraken_websocket("BTC-USD")
    "XBT/USD"
    >>> kraken_websocket_to_native("XBT/USD")
    "BTC-USD".
"""

from datetime import datetime
from typing import get_args

from sqlalchemy import select

from snapper.core.types import ExchangeEnum
from snapper.core.types import MarketDataExchange
from snapper.core.types import MarketSubscribeExchange
from snapper.core.types import OrderExchange
from snapper.data.models import Symbol
from snapper.data.repository import Repository
from snapper.data.repository import where_active
from snapper.infrastructure.symbols.mapper import SymbolMapperService
from snapper.infrastructure.symbols.mapper import register_invalidation_callback

__all__ = [
    "resolve_symbol_public_id",
    "kraken_websocket_to_ccxt",
    "ccxt_to_kraken_websocket",
    "native_to_ccxt",
    "ccxt_to_native",
    "native_to_kraken_websocket",
    "kraken_websocket_to_native",
    "native_to_kraken_rest",
    "kraken_rest_to_native",
    "native_to_walutomat_ws",
    "walutomat_ws_to_native",
    "native_to_walutomat_rest",
    "walutomat_rest_to_native",
    "native_to_polygon_rest",
    "polygon_rest_to_native",
    "native_to_kraken_futures_ws",
    "kraken_futures_ws_to_native",
    "get_available_kraken_futures_symbols",
    "get_available_walutomat_symbols",
    "get_available_polygon_symbols",
    "get_available_symbols",
    "get_available_kraken_symbols",
    "get_available_ws_symbols",
    "get_available_kraken_rest_symbols",
    "validate_symbol",
    "get_available_exchanges",
    "get_market_subscribe_exchanges",
    "get_market_data_exchanges",
    "get_available_polygon_rest_symbols",
    "get_available_walutomat_rest_symbols",
    "is_tradeable",
    "is_market_data_available",
    "get_tradeable_symbols",
    "get_market_data_symbols",
    "_get_db_mapper",
]


async def resolve_symbol_public_id(
    repo: Repository,
    native_symbol: str,
    as_of: datetime,
) -> str | None:
    """Look up the active Symbol row by native_symbol and return its public_id.

    Args:
        repo: Async repository providing a session context manager.
        native_symbol: Canonical native symbol (e.g., ``BTC-USD``).
        as_of: Point-in-time for temporal query.

    Returns:
        The ``public_id`` of the active Symbol row, or ``None`` when no
        active row matches.
    """
    async with repo.session() as session:
        ts_filter, kt_filter = where_active(Symbol, as_of)
        result = await session.execute(
            select(Symbol.public_id).where(
                Symbol.native_symbol == native_symbol,
                ts_filter,
                kt_filter,
            )
        )
        row = result.scalar_one_or_none()
        return row


def kraken_websocket_to_ccxt(symbol: str) -> str:
    """Convert Kraken WebSocket symbol to CCXT format.

    Args:
        symbol: Kraken WebSocket v2 symbol (e.g., ``XBT/USD``).

    Returns:
        CCXT-formatted symbol (e.g., ``BTC/USD``).

    Raises:
        ValueError: If symbol is not recognized.
    """
    native = kraken_websocket_to_native(symbol)
    return native_to_ccxt(native)


def ccxt_to_kraken_websocket(symbol: str) -> str:
    """Convert CCXT symbol to Kraken WebSocket format.

    Args:
        symbol: CCXT-formatted symbol (e.g., ``BTC/USD``).

    Returns:
        Kraken WebSocket v2 symbol (e.g., ``XBT/USD``).

    Raises:
        ValueError: If symbol is not recognized.
    """
    native = ccxt_to_native(symbol)
    return native_to_kraken_websocket(native)


def _get_db_mapper() -> SymbolMapperService:
    """Get the singleton SymbolMapperService instance.

    Returns:
        The SymbolMapperService singleton with loaded mappings.
    """
    return SymbolMapperService.get_instance()


def native_to_kraken_websocket(native_symbol: str) -> str:
    """Convert native symbol to Kraken WebSocket v2 format.

    Args:
        native_symbol: Native symbol (e.g., ``BTC-USD``).

    Returns:
        Kraken WebSocket v2 symbol (e.g., ``XBT/USD``).

    Raises:
        ValueError: If native symbol is not mapped to Kraken.
    """
    mapper = _get_db_mapper()
    try:
        return mapper.native_to_kraken_ws[native_symbol]
    except KeyError as exc:
        raise ValueError(f"Unknown native symbol: {native_symbol}") from exc


def native_to_kraken_rest(native_symbol: str) -> str:
    """Convert native symbol to Kraken REST API format.

    Args:
        native_symbol: Native symbol (e.g., ``BTC-USD``).

    Returns:
        Kraken REST symbol (e.g., ``XBTUSD``).

    Raises:
        ValueError: If native symbol is not mapped to Kraken REST.
    """
    mapper = _get_db_mapper()
    try:
        return mapper.native_to_kraken_rest[native_symbol]
    except KeyError as exc:
        raise ValueError(f"Unknown native symbol: {native_symbol}") from exc


def native_to_ccxt(native_symbol: str) -> str:
    """Convert native symbol to CCXT format.

    Args:
        native_symbol: Native symbol (e.g., ``BTC-USD``).

    Returns:
        CCXT-formatted symbol (e.g., ``BTC/USD``).

    Raises:
        ValueError: If native symbol is not mapped to CCXT.
    """
    mapper = _get_db_mapper()
    try:
        return mapper.native_to_ccxt[native_symbol]
    except KeyError as exc:
        raise ValueError(f"Unknown native symbol: {native_symbol}") from exc


def kraken_websocket_to_native(symbol: str) -> str:
    """Convert Kraken WebSocket v2 symbol to native format.

    Args:
        symbol: Kraken WebSocket v2 symbol (e.g., ``XBT/USD``).

    Returns:
        Native symbol (e.g., ``BTC-USD``).

    Raises:
        ValueError: If Kraken symbol is not recognized.
    """
    mapper = _get_db_mapper()
    try:
        return mapper.kraken_ws_to_native[symbol]
    except KeyError as exc:
        raise ValueError(f"Unknown Kraken WebSocket v2 symbol: {symbol}") from exc


def kraken_rest_to_native(symbol: str) -> str:
    """Convert Kraken REST API symbol to native format.

    Args:
        symbol: Kraken REST symbol (e.g., ``XBTUSD``).

    Returns:
        Native symbol (e.g., ``BTC-USD``).

    Raises:
        ValueError: If Kraken REST symbol is not recognized.
    """
    mapper = _get_db_mapper()
    try:
        return mapper.kraken_rest_to_native[symbol]
    except KeyError as exc:
        raise ValueError(f"Unknown Kraken REST symbol: {symbol}") from exc


def ccxt_to_native(symbol: str) -> str:
    """Convert CCXT symbol to native format.

    Args:
        symbol: CCXT-formatted symbol (e.g., ``BTC/USD``).

    Returns:
        Native symbol (e.g., ``BTC-USD``).

    Raises:
        ValueError: If CCXT symbol is not recognized.
    """
    mapper = _get_db_mapper()
    try:
        return mapper.ccxt_to_native[symbol]
    except KeyError as exc:
        raise ValueError(f"Unknown CCXT symbol: {symbol}") from exc


def native_to_walutomat_ws(native_symbol: str) -> str:
    """Convert native symbol to Walutomat WebSocket format.

    Args:
        native_symbol: Native symbol (e.g., ``EUR-PLN``).

    Returns:
        Walutomat WebSocket symbol (e.g., ``EUR_PLN``).

    Raises:
        ValueError: If native symbol is not available on Walutomat.
    """
    mapper = _get_db_mapper()
    try:
        return mapper.native_to_walutomat_ws[native_symbol]
    except KeyError as exc:
        raise ValueError(
            f"Unknown native symbol (not available on Walutomat WS): {native_symbol}"
        ) from exc


def walutomat_ws_to_native(symbol: str) -> str:
    """Convert Walutomat WebSocket symbol to native format.

    Args:
        symbol: Walutomat WebSocket symbol (e.g., ``EUR_PLN``).

    Returns:
        Native symbol (e.g., ``EUR-PLN``).

    Raises:
        ValueError: If Walutomat WebSocket symbol is not recognized.
    """
    mapper = _get_db_mapper()
    try:
        return mapper.walutomat_ws_to_native[symbol]
    except KeyError as exc:
        raise ValueError(f"Unknown Walutomat WebSocket symbol: {symbol}") from exc


def native_to_walutomat_rest(native_symbol: str) -> str:
    """Convert native symbol to Walutomat REST API format.

    Args:
        native_symbol: Native symbol (e.g., ``EUR-PLN``).

    Returns:
        Walutomat REST symbol (e.g., ``EURPLN``).

    Raises:
        ValueError: If native symbol is not available on Walutomat REST.
    """
    mapper = _get_db_mapper()
    try:
        return mapper.native_to_walutomat_rest[native_symbol]
    except KeyError as exc:
        raise ValueError(
            f"Unknown native symbol (not available on Walutomat REST): {native_symbol}"
        ) from exc


def walutomat_rest_to_native(symbol: str) -> str:
    """Convert Walutomat REST API symbol to native format.

    Args:
        symbol: Walutomat REST symbol (e.g., ``EURPLN``).

    Returns:
        Native symbol (e.g., ``EUR-PLN``).

    Raises:
        ValueError: If Walutomat REST symbol is not recognized.
    """
    mapper = _get_db_mapper()
    try:
        return mapper.walutomat_rest_to_native[symbol]
    except KeyError as exc:
        raise ValueError(f"Unknown Walutomat REST symbol: {symbol}") from exc


def native_to_polygon_rest(native_symbol: str) -> str:
    """Convert native symbol to Polygon.io REST ticker format.

    Args:
        native_symbol: Native symbol (e.g., ``BTC-USD``).

    Returns:
        Polygon ticker (e.g., ``C:BTCUSD`` for crypto or ``X:EURUSD`` for forex).

    Raises:
        ValueError: If native symbol is not available on Polygon.
    """
    mapper = _get_db_mapper()
    try:
        return mapper.native_to_polygon_rest[native_symbol]
    except KeyError as exc:
        raise ValueError(
            f"Unknown native symbol (not available on Polygon): {native_symbol}"
        ) from exc


def polygon_rest_to_native(symbol: str) -> str:
    """Convert Polygon.io REST ticker to native format.

    Args:
        symbol: Polygon ticker (e.g., ``C:BTCUSD`` or ``X:EURUSD``).

    Returns:
        Native symbol (e.g., ``BTC-USD``).

    Raises:
        ValueError: If Polygon ticker is not recognized.
    """
    mapper = _get_db_mapper()
    try:
        return mapper.polygon_rest_to_native[symbol]
    except KeyError as exc:
        raise ValueError(f"Unknown Polygon REST symbol: {symbol}") from exc


def get_available_polygon_rest_symbols() -> list[str]:
    """Get all available Polygon.io REST API tickers.

    Returns:
        Sorted list of Polygon tickers (e.g., ``["C:BTCUSD", "X:EURUSD"]``).
    """
    mapper = _get_db_mapper()
    return sorted(mapper.polygon_rest_to_native.keys())


def native_to_kraken_futures_ws(native_symbol: str) -> str:
    """Convert native symbol to Kraken Futures WebSocket format.

    Args:
        native_symbol: Native symbol (e.g., ``BTC-USD-PERP``).

    Returns:
        Kraken Futures WS symbol (e.g., ``PF_XBTUSD``).

    Raises:
        ValueError: If native symbol is not mapped to Kraken Futures.
    """
    mapper = _get_db_mapper()
    try:
        return mapper.native_to_kraken_futures_ws[native_symbol]
    except KeyError as exc:
        raise ValueError(f"Unknown native symbol: {native_symbol}") from exc


def kraken_futures_ws_to_native(symbol: str) -> str:
    """Convert Kraken Futures WebSocket symbol to native format.

    Args:
        symbol: Kraken Futures WS symbol (e.g., ``PF_XBTUSD``).

    Returns:
        Native symbol (e.g., ``BTC-USD-PERP``).

    Raises:
        ValueError: If Kraken Futures symbol is not recognized.
    """
    mapper = _get_db_mapper()
    try:
        return mapper.kraken_futures_ws_to_native[symbol]
    except KeyError as exc:
        raise ValueError(f"Unknown Kraken Futures WS symbol: {symbol}") from exc


def get_available_kraken_futures_symbols() -> list[str]:
    """Get all available native symbols that have Kraken Futures mappings.

    Returns:
        Sorted list of native symbols with Kraken Futures support.
    """
    mapper = _get_db_mapper()
    return sorted(mapper.native_to_kraken_futures_ws.keys())


def native_to_kraken_equities_ws(native_symbol: str) -> str:
    """Convert native symbol to Kraken Equities WebSocket format.

    Converts dash-separated native format to dot-separated exchange format.
    Falls back to direct dash→dot replacement for unmapped symbols.

    Args:
        native_symbol: Native symbol (e.g., ``CLM6-NYMEX``).

    Returns:
        Kraken Equities WS symbol (e.g., ``CLM6.NYMEX``).

    Raises:
        ValueError: If native symbol is not mapped to Kraken Equities.
    """
    mapper = _get_db_mapper()
    try:
        return mapper.native_to_kraken_equities_ws[native_symbol]
    except KeyError as exc:
        raise ValueError(f"Unknown native symbol: {native_symbol}") from exc


def kraken_equities_ws_to_native(symbol: str) -> str:
    """Convert Kraken Equities WebSocket symbol to native format.

    Converts dot-separated exchange format to dash-separated native format.
    Falls back to direct dot→dash replacement for unmapped symbols.

    Args:
        symbol: Kraken Equities WS symbol (e.g., ``CLM6.NYMEX``).

    Returns:
        Native symbol (e.g., ``CLM6-NYMEX``).

    Raises:
        ValueError: If Kraken Equities symbol is not recognized.
    """
    mapper = _get_db_mapper()
    try:
        return mapper.kraken_equities_ws_to_native[symbol]
    except KeyError as exc:
        raise ValueError(f"Unknown Kraken Equities WS symbol: {symbol}") from exc


def get_available_kraken_equities_symbols() -> list[str]:
    """Get all available native symbols that have Kraken Equities mappings.

    Returns:
        Sorted list of native symbols with Kraken Equities support.
    """
    mapper = _get_db_mapper()
    return sorted(mapper.native_to_kraken_equities_ws.keys())


def validate_symbol(symbol: str) -> bool:
    """Check if a native symbol is valid (has Kraken WebSocket mapping).

    Args:
        symbol: Native symbol to validate.

    Returns:
        True if symbol exists in Kraken WebSocket mappings, False otherwise.
    """
    mapper = _get_db_mapper()
    return symbol in mapper.native_to_kraken_ws


def get_available_ws_symbols() -> list[str]:
    """Get all available Kraken WebSocket v2 symbols.

    Returns:
        Sorted list of Kraken WebSocket symbols (e.g., ``["XBT/USD", ...]``).
    """
    mapper = _get_db_mapper()
    return sorted(mapper.kraken_ws_to_native.keys())


def get_available_kraken_rest_symbols() -> list[str]:
    """Get all available Kraken REST API symbols.

    Returns:
        Sorted list of Kraken REST symbols (e.g., ``["XBTUSD", ...]``).
    """
    mapper = _get_db_mapper()
    return sorted(mapper.kraken_rest_to_native.keys())


def get_available_kraken_symbols() -> list[str]:
    """Get all available native symbols that have Kraken mappings.

    Returns:
        Sorted list of native symbols with Kraken support.
    """
    mapper = _get_db_mapper()
    return sorted(mapper.native_to_kraken_ws.keys())


def get_available_walutomat_rest_symbols() -> list[str]:
    """Get all available Walutomat REST API symbols.

    Returns:
        Sorted list of Walutomat REST symbols (e.g., ``["EURPLN", ...]``).
    """
    mapper = _get_db_mapper()
    return sorted(mapper.walutomat_rest_to_native.keys())


def get_available_walutomat_symbols() -> list[str]:
    """Get all available native symbols that have Walutomat mappings.

    Returns:
        Sorted list of native symbols with Walutomat support.
    """
    mapper = _get_db_mapper()
    return sorted(mapper.walutomat_ws_to_native.values())


def get_available_polygon_symbols() -> list[str]:
    """Get all available native symbols that have Polygon mappings.

    Returns:
        Sorted list of native symbols with Polygon support.
    """
    mapper = _get_db_mapper()
    return sorted(mapper.polygon_rest_to_native.values())


class _AvailableSymbolsCache:
    """Process-local cache for the cross-exchange native-symbol union.

    A class-attribute holder dodges the lint warning against module-level
    ``global`` rebinding while keeping invalidation O(1). One instance is
    created at import; mutation flows through the class rather than the
    bare ``global`` statement.

    Attributes:
        frozen: Cached :class:`frozenset` for O(1) membership lookups —
            populated on first :func:`get_available_symbols_set` call
            and on every invalidation that follows. ``None`` while cold.
        sorted_list: Cached sorted ``list[str]`` mirror returned by
            :func:`get_available_symbols`. ``None`` while cold.
    """

    frozen: frozenset[str] | None = None
    sorted_list: list[str] | None = None


def _rebuild_available_symbols_cache() -> frozenset[str]:
    """Recompute the union of native symbols across all exchanges.

    Hot-path callers (publish-time topic validation) hit this once on
    cold start and once after each
    :meth:`SymbolMapperService.trigger_cache_invalidation` call, instead
    of paying the 5-exchange union + sort every tick.

    Returns:
        Frozen set of every native symbol active on at least one
        configured exchange. Mirrored on
        :class:`_AvailableSymbolsCache`.
    """
    union: set[str] = set()
    union.update(get_available_kraken_symbols())
    union.update(get_available_kraken_futures_symbols())
    union.update(get_available_kraken_equities_symbols())
    union.update(get_available_walutomat_symbols())
    union.update(get_available_polygon_symbols())
    frozen = frozenset(union)
    _AvailableSymbolsCache.frozen = frozen
    _AvailableSymbolsCache.sorted_list = sorted(union)
    return frozen


def get_available_symbols_set() -> frozenset[str]:
    """Return the cached frozenset of every native symbol.

    Use this from any hot path that needs membership lookups (publish-
    time topic validation runs once per tick at peak rates of several
    thousand per second). Cache is populated lazily on first call and
    invalidated when :meth:`SymbolMapperService.trigger_cache_invalidation`
    fires after a symbol alias upsert.

    Returns:
        Frozen set of all active native symbols, or an empty frozenset
        if the mapper cache is uninitialised.
    """
    if _AvailableSymbolsCache.frozen is None:
        return _rebuild_available_symbols_cache()
    return _AvailableSymbolsCache.frozen


def invalidate_available_symbols_cache() -> None:
    """Drop the cached symbol set so the next caller rebuilds it.

    Called by :meth:`SymbolMapperService.trigger_cache_invalidation` so
    a freshly-loaded mapper sees the latest symbol universe at the next
    validation. Tests that mutate the mapper between assertions also
    use this directly.
    """
    _AvailableSymbolsCache.frozen = None
    _AvailableSymbolsCache.sorted_list = None


register_invalidation_callback(invalidate_available_symbols_cache)


def get_available_symbols() -> list[str]:
    """Get all available native symbols across all exchanges.

    Combines symbols from Kraken, Kraken Futures, Kraken Equities, Walutomat, and Polygon.

    The result is cached after the first call and only rebuilt when
    :func:`invalidate_available_symbols_cache` is invoked (typically
    by :meth:`SymbolMapperService.trigger_cache_invalidation`).

    Returns:
        Sorted list of unique native symbols from all exchanges.
    """
    if _AvailableSymbolsCache.sorted_list is None:
        _rebuild_available_symbols_cache()
    cached = _AvailableSymbolsCache.sorted_list or []
    return list(cached)


def get_available_exchanges() -> list[OrderExchange]:
    """Get list of order-capable trading exchanges.

    Returns:
        List of exchange identifiers for order execution (paper + live venues).
    """
    return list(get_args(OrderExchange))


def get_market_subscribe_exchanges() -> list[MarketSubscribeExchange]:
    """Get exchanges that provide live market data feeds.

    Returns:
        List of live feed exchange identifiers (no paper, no polygon).
    """
    return list(get_args(MarketSubscribeExchange))


def get_market_data_exchanges() -> list[MarketDataExchange]:
    """Get exchanges valid as paper replay data sources.

    Returns:
        List of exchange identifiers that can be replayed in paper mode.
        Excludes 'paper' — paper is the consumer, not a source.
    """
    return list(get_args(MarketDataExchange))


def is_tradeable(native_symbol: str, exchange: str) -> bool:
    """Check if a symbol is tradeable on the given exchange.

    Paper exchange returns True only for symbols that have at least one
    alias in any forward map (paper has no rows in
    symbol_exchange_capabilities but can trade any known symbol).
    For other exchanges, returns False if no capability row exists
    (default-deny policy).

    Args:
        native_symbol: Native symbol (e.g., ``BTC-USD``).
        exchange: Exchange identifier (e.g., ``kraken``, ``paper``).

    Returns:
        True if the symbol is tradeable on the exchange, False otherwise.
    """
    if exchange == ExchangeEnum.PAPER:
        mapper = _get_db_mapper()
        return any(native_symbol in fwd_map for fwd_map in mapper.forward.values())
    mapper = _get_db_mapper()
    cap = mapper.capabilities.get((native_symbol, exchange))
    if cap is None:
        return False
    return cap.can_trade


def is_market_data_available(native_symbol: str, exchange: str) -> bool:
    """Check if market data is available for a symbol on the given exchange.

    Returns False if no capability row exists (default-deny).

    Args:
        native_symbol: Native symbol (e.g., ``BTC-USD``).
        exchange: Exchange identifier (e.g., ``kraken``).

    Returns:
        True if market data is available, False otherwise.
    """
    mapper = _get_db_mapper()
    cap = mapper.capabilities.get((native_symbol, exchange))
    if cap is None:
        return False
    return cap.can_market_data


def get_tradeable_symbols(exchange: str) -> list[str]:
    """Get all tradeable native symbols for an exchange.

    Paper exchange returns all symbols that have any alias
    (union of all forward map keys).

    Args:
        exchange: Exchange identifier (e.g., ``kraken``, ``paper``).

    Returns:
        Sorted list of native symbols tradeable on the exchange.
    """
    mapper = _get_db_mapper()
    if exchange == ExchangeEnum.PAPER:
        all_symbols: set[str] = set()
        for fwd_map in mapper.forward.values():
            all_symbols.update(fwd_map.keys())
        return sorted(all_symbols)
    return sorted(
        sym
        for (sym, exch), cap in mapper.capabilities.items()
        if exch == exchange and cap.can_trade
    )


def get_market_data_symbols(exchange: str) -> list[str]:
    """Get all native symbols with market data on an exchange.

    Args:
        exchange: Exchange identifier (e.g., ``kraken``, ``polygon``).

    Returns:
        Sorted list of native symbols with market data on the exchange.
    """
    mapper = _get_db_mapper()
    return sorted(
        sym
        for (sym, exch), cap in mapper.capabilities.items()
        if exch == exchange and cap.can_market_data
    )
