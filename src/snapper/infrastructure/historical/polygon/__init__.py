"""Polygon.io historical data services.

This package provides the PolygonHistoricalLoader for fetching and caching
historical market data from Polygon.io API.

Modules:
    loader: PolygonHistoricalLoader class with aggregate and grouped daily
        data fetching capabilities.

Exported classes:
    AggregateCandle: OHLCV candle data for a single time period.
    GroupedDailyRow: Daily aggregated data for a single ticker.
    PolygonHistoricalLoader: Main service for historical data retrieval.
"""
