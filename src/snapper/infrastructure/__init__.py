"""Infrastructure layer services.

This package contains infrastructure-level services that provide cross-cutting
concerns for the Snapper trading platform. These services handle external
integrations, data persistence, and system-level utilities.

Subpackages:
    exchanges: Exchange client implementations and adapters for trading
        operations across multiple exchanges (Kraken, Walutomat,
        Paper, Polygon).
    symbols: Symbol mapping services for converting between native and
        exchange-specific symbol formats.
    security: Encryption services for protecting sensitive configuration data.
    market_data: Real-time market snapshot collection services.
    historical: Historical market data loading and caching services.
    logging: Custom logging handlers for unified log management.

Architecture:
    The infrastructure layer follows the ports and adapters pattern, with
    exchange contracts defining interfaces that concrete implementations
    fulfill. Services are typically singletons with lazy initialization.
"""
