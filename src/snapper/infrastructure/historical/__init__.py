"""Historical market data loading and caching.

This package provides services for fetching and caching historical market data
from data providers. Currently supports Polygon.io for stocks, forex, and
crypto markets.

Subpackages:
    polygon: Polygon.io historical data loader with CSV caching for aggregates
        and grouped daily data.

Features:
    - Rate-limited API access with configurable delays
    - Automatic CSV caching organized by ticker, timespan, and date
    - Support for resuming interrupted downloads
    - Decimal precision for financial data
"""
