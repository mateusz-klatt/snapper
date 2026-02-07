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

from snapper.core.types import MarketSubscribeExchange
from snapper.core.types import ReplaySourceExchange
from snapper.core.types import TradingExchange
from snapper.infrastructure.symbols.mapper import SymbolMapperService

__all__ = [
    "TradingExchange",
    "kraken_websocket_to_ccxt",
    "ccxt_to_kraken_websocket",
    "native_to_ccxt",
    "ccxt_to_native",
    "native_to_kraken_websocket",
    "kraken_websocket_to_native",
    "native_to_kraken_rest",
    "kraken_rest_to_native",
    "native_to_zonda",
    "zonda_to_native",
    "native_to_walutomat",
    "walutomat_to_native",
    "native_to_walutomat_rest",
    "walutomat_rest_to_native",
    "native_to_polygon",
    "polygon_to_native",
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
    "get_replay_source_exchanges",
    "get_available_polygon_rest_symbols",
    "get_available_walutomat_rest_symbols",
    "_get_db_mapper",
]


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
        return mapper.native_to_ws[native_symbol]
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
        return mapper.native_to_rest[native_symbol]
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
        return mapper.ws_to_native[symbol]
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
        return mapper.rest_to_native[symbol]
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


def native_to_zonda(native_symbol: str) -> str:
    """Convert native symbol to Zonda exchange format.

    Args:
        native_symbol: Native symbol (e.g., ``BTC-PLN``).

    Returns:
        Zonda symbol (e.g., ``BTC-PLN``).

    Raises:
        ValueError: If native symbol is not available on Zonda.
    """
    mapper = _get_db_mapper()
    try:
        return mapper.native_to_zonda[native_symbol]
    except KeyError as exc:
        raise ValueError(
            f"Unknown native symbol (not available on Zonda): {native_symbol}"
        ) from exc


def zonda_to_native(symbol: str) -> str:
    """Convert Zonda symbol to native format.

    Args:
        symbol: Zonda symbol (e.g., ``BTC-PLN``).

    Returns:
        Native symbol (e.g., ``BTC-PLN``).

    Raises:
        ValueError: If Zonda symbol is not recognized.
    """
    mapper = _get_db_mapper()
    try:
        return mapper.zonda_to_native[symbol]
    except KeyError as exc:
        raise ValueError(f"Unknown Zonda symbol: {symbol}") from exc


def native_to_walutomat(native_symbol: str) -> str:
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
        return mapper.native_to_walutomat[native_symbol]
    except KeyError as exc:
        raise ValueError(
            f"Unknown native symbol (not available on Walutomat): {native_symbol}"
        ) from exc


def walutomat_to_native(symbol: str) -> str:
    """Convert Walutomat WebSocket symbol to native format.

    Args:
        symbol: Walutomat WebSocket symbol (e.g., ``EUR_PLN``).

    Returns:
        Native symbol (e.g., ``EUR-PLN``).

    Raises:
        ValueError: If Walutomat symbol is not recognized.
    """
    mapper = _get_db_mapper()
    try:
        return mapper.walutomat_to_native[symbol]
    except KeyError as exc:
        raise ValueError(f"Unknown Walutomat symbol: {symbol}") from exc


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


def native_to_polygon(native_symbol: str) -> str:
    """Convert native symbol to Polygon.io ticker format.

    Args:
        native_symbol: Native symbol (e.g., ``BTC-USD``).

    Returns:
        Polygon ticker (e.g., ``C:BTCUSD`` for crypto or ``X:EURUSD`` for forex).

    Raises:
        ValueError: If native symbol is not available on Polygon.
    """
    mapper = _get_db_mapper()
    try:
        return mapper.native_to_polygon[native_symbol]
    except KeyError as exc:
        raise ValueError(
            f"Unknown native symbol (not available on Polygon): {native_symbol}"
        ) from exc


def polygon_to_native(symbol: str) -> str:
    """Convert Polygon.io ticker to native format.

    Args:
        symbol: Polygon ticker (e.g., ``C:BTCUSD`` or ``X:EURUSD``).

    Returns:
        Native symbol (e.g., ``BTC-USD``).

    Raises:
        ValueError: If Polygon ticker is not recognized.
    """
    mapper = _get_db_mapper()
    try:
        return mapper.polygon_to_native[symbol]
    except KeyError as exc:
        raise ValueError(f"Unknown Polygon symbol: {symbol}") from exc


def get_available_polygon_rest_symbols() -> list[str]:
    """Get all available Polygon.io REST API tickers.

    Returns:
        Sorted list of Polygon tickers (e.g., ``["C:BTCUSD", "X:EURUSD"]``).
    """
    mapper = _get_db_mapper()
    return sorted(mapper.polygon_to_native.keys())


def validate_symbol(symbol: str) -> bool:
    """Check if a native symbol is valid (has Kraken WebSocket mapping).

    Args:
        symbol: Native symbol to validate.

    Returns:
        True if symbol exists in Kraken WebSocket mappings, False otherwise.
    """
    mapper = _get_db_mapper()
    return symbol in mapper.native_to_ws


def get_available_ws_symbols() -> list[str]:
    """Get all available Kraken WebSocket v2 symbols.

    Returns:
        Sorted list of Kraken WebSocket symbols (e.g., ``["XBT/USD", ...]``).
    """
    mapper = _get_db_mapper()
    return sorted(mapper.ws_to_native.keys())


def get_available_kraken_rest_symbols() -> list[str]:
    """Get all available Kraken REST API symbols.

    Returns:
        Sorted list of Kraken REST symbols (e.g., ``["XBTUSD", ...]``).
    """
    mapper = _get_db_mapper()
    return sorted(mapper.rest_to_native.keys())


def get_available_kraken_symbols() -> list[str]:
    """Get all available native symbols that have Kraken mappings.

    Returns:
        Sorted list of native symbols with Kraken support.
    """
    mapper = _get_db_mapper()
    return sorted(mapper.native_to_ws.keys())


def get_available_zonda_symbols() -> list[str]:
    """Get all available Zonda exchange symbols.

    Returns:
        Sorted list of Zonda symbols (e.g., ``["BTC-PLN", ...]``).
    """
    mapper = _get_db_mapper()
    return sorted(mapper.zonda_to_native.keys())


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
    return sorted(mapper.walutomat_to_native.values())


def get_available_polygon_symbols() -> list[str]:
    """Get all available native symbols that have Polygon mappings.

    Returns:
        Sorted list of native symbols with Polygon support.
    """
    mapper = _get_db_mapper()
    return sorted(mapper.polygon_to_native.values())


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


def get_available_exchanges() -> list[TradingExchange]:
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


def get_replay_source_exchanges() -> list[ReplaySourceExchange]:
    """Get exchanges valid as paper replay data sources.

    Returns:
        List of exchange identifiers that can be replayed in paper mode.
        Excludes 'paper' — paper is the consumer, not a source.
    """
    return ["kraken", "polygon", "walutomat", "zonda"]
