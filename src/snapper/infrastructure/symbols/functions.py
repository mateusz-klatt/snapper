"""Symbol conversion functions for multi-exchange support.

This module provides stateless functions for converting trading symbols between
Snapper's native format and exchange-specific formats. All functions delegate
to the SymbolMapperService singleton for actual lookups.

Functions are organized by exchange:
    - Kraken: WebSocket and REST symbol conversions
    - CCXT: CCXT library symbol format
    - Zonda: Zonda exchange symbol format
    - Walutomat: Walutomat WebSocket and REST formats
    - Polygon: Polygon.io ticker format

Each conversion direction has a corresponding function pair:
    - ``native_to_<exchange>`` for outbound conversion
    - ``<exchange>_to_native`` for inbound conversion

Example:
    >>> from snapper.infrastructure.symbols.functions import (
    ...     native_to_kraken_websocket,
    ...     kraken_websocket_to_native,
    ... )
    >>> native_to_kraken_websocket("BTC-USD")
    "XBT/USD"
    >>> kraken_websocket_to_native("XBT/USD")
    "BTC-USD"
"""

from datetime import datetime

from sqlalchemy import select

from snapper.core.types import MarketDataExchange
from snapper.core.types import MarketSubscribeExchange
from snapper.core.types import OrderExchange
from snapper.data.models import Symbol
from snapper.data.repository import Repository
from snapper.data.repository import where_active
from snapper.infrastructure.symbols.mapper import SymbolMapperService

__all__ = [
    "OrderExchange",
    "resolve_symbol_public_id",
    "kraken_websocket_to_ccxt",
    "ccxt_to_kraken_websocket",
    "native_to_ccxt",
    "ccxt_to_native",
    "native_to_kraken_websocket",
    "kraken_websocket_to_native",
    "native_to_kraken_rest",
    "kraken_rest_to_native",
    "native_to_zonda_ws",
    "zonda_ws_to_native",
    "native_to_walutomat_ws",
    "walutomat_ws_to_native",
    "native_to_walutomat_rest",
    "walutomat_rest_to_native",
    "native_to_polygon_rest",
    "polygon_rest_to_native",
    "get_available_zonda_symbols",
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
    as_of: datetime | None = None,
) -> str | None:
    """Look up the active Symbol row by native_symbol and return its public_id.

    Args:
        repo: Async repository providing a session context manager.
        native_symbol: Canonical native symbol (e.g., ``BTC-USD``).
        as_of: Point-in-time to query. Defaults to now.

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


def native_to_zonda_ws(native_symbol: str) -> str:
    """Convert native symbol to Zonda WebSocket format.

    Args:
        native_symbol: Native symbol (e.g., ``BTC-PLN``).

    Returns:
        Zonda WebSocket symbol (e.g., ``BTC-PLN``).

    Raises:
        ValueError: If native symbol is not available on Zonda.
    """
    mapper = _get_db_mapper()
    try:
        return mapper.native_to_zonda_ws[native_symbol]
    except KeyError as exc:
        raise ValueError(
            f"Unknown native symbol (not available on Zonda): {native_symbol}"
        ) from exc


def zonda_ws_to_native(symbol: str) -> str:
    """Convert Zonda WebSocket symbol to native format.

    Args:
        symbol: Zonda WebSocket symbol (e.g., ``BTC-PLN``).

    Returns:
        Native symbol (e.g., ``BTC-PLN``).

    Raises:
        ValueError: If Zonda symbol is not recognized.
    """
    mapper = _get_db_mapper()
    try:
        return mapper.zonda_ws_to_native[symbol]
    except KeyError as exc:
        raise ValueError(f"Unknown Zonda WebSocket symbol: {symbol}") from exc


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


def get_available_zonda_symbols() -> list[str]:
    """Get all available Zonda exchange symbols.

    Returns:
        Sorted list of Zonda symbols (e.g., ``["BTC-PLN", ...]``).
    """
    mapper = _get_db_mapper()
    return sorted(mapper.zonda_ws_to_native.keys())


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


def get_available_symbols() -> list[str]:
    """Get all available native symbols across all exchanges.

    Combines symbols from Kraken, Zonda, Walutomat, and Polygon.

    Returns:
        Sorted list of unique native symbols from all exchanges.
    """
    all_symbols: set[str] = set()
    all_symbols.update(get_available_kraken_symbols())
    all_symbols.update(get_available_zonda_symbols())
    all_symbols.update(get_available_walutomat_symbols())
    all_symbols.update(get_available_polygon_symbols())
    return sorted(all_symbols)


def get_available_exchanges() -> list[OrderExchange]:
    """Get list of order-capable trading exchanges.

    Returns:
        List of exchange identifiers for order execution (paper + live venues).
    """
    return ["kraken", "paper", "walutomat", "zonda"]


def get_market_subscribe_exchanges() -> list[MarketSubscribeExchange]:
    """Get exchanges that provide live market data feeds.

    Returns:
        List of live feed exchange identifiers (no paper, no polygon).
    """
    return ["kraken", "walutomat", "zonda"]


def get_market_data_exchanges() -> list[MarketDataExchange]:
    """Get exchanges valid as paper replay data sources.

    Returns:
        List of exchange identifiers that can be replayed in paper mode.
        Excludes 'paper' — paper is the consumer, not a source.
    """
    return ["kraken", "polygon", "walutomat", "zonda"]


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
    if exchange == "paper":
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
    if exchange == "paper":
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
