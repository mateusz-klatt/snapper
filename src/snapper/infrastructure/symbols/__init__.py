"""Symbol mapping and conversion utilities.

This package provides services and functions for converting trading symbols
between Snapper's native format and exchange-specific formats. It supports
bidirectional mapping for all integrated exchanges (Kraken, Walutomat,
Polygon).

Modules:
    mapper: Singleton SymbolMapperService for centralized symbol mapping with
        database-backed cache.
    functions: Stateless conversion functions for symbol format transformations.

The native symbol format is ``BASE-QUOTE`` (e.g., ``BTC-USD``), while
exchange-specific formats vary (e.g., ``BTC/USD`` for Kraken WebSocket,
``EUR_USD`` for Walutomat).
"""
