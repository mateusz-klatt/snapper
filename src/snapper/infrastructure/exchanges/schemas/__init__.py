"""Exchange schemas package.

This package contains Pydantic schemas for validating and parsing
exchange-specific data formats. Each exchange has its own schema module
with models for tickers, trades, orders, and other exchange data.

Modules:
    base: Common configuration objects for all schemas
    kraken: Kraken WebSocket and REST API schemas
    zonda: Zonda (BitBay) WebSocket schemas
    walutomat: Walutomat REST API schemas
    polygon: Polygon.io API response schemas
"""
