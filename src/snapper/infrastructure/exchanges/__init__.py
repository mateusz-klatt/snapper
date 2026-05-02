"""Exchange infrastructure package.

This package provides abstractions and implementations for connecting to
cryptocurrency and FX exchanges. It includes:

- Abstract base class defining the exchange client interface
- Concrete implementations for supported exchanges (Kraken, Walutomat, Polygon)
- Paper trading simulation client for testing
- Pydantic schemas for exchange-specific data validation
- Adapter functions for data format conversions

Subpackages:
    implementations: Concrete exchange client implementations
    adapters: Data format conversion utilities
    schemas: Pydantic validation schemas for exchange data
"""
