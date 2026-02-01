"""Pydantic schemas for Polygon.io market data API responses.

This module provides Pydantic models for validating and parsing Polygon.io
API responses. Polygon.io is a market data provider supporting stocks,
forex, and crypto with both REST API and WebSocket feeds.

Aggregate Data:
    - PolygonAgg: Single ticker aggregated bar (OHLCV)
    - PolygonGroupedAgg: Daily grouped aggregates for all tickers
    - PolygonPreviousClose: Previous day's close data

Ticker Information:
    - PolygonTicker: Ticker/instrument metadata and status

All schemas include factory methods (from_sdk_agg, from_sdk_ticker) for
converting Polygon SDK response objects to typed Pydantic models. This
provides validation while handling the various attribute naming conventions
used by different Polygon API versions.
"""

from typing import Any

from pydantic import BaseModel
from pydantic import Field

from snapper.infrastructure.exchanges.schemas.base import EXCHANGE_SCHEMA_CONFIG

_OPEN_DESC = "Opening price"
_HIGH_DESC = "High price"
_LOW_DESC = "Low price"
_CLOSE_DESC = "Closing price"
_VOLUME_DESC = "Trading volume"
_VWAP_DESC = "Volume weighted average price"
_TIMESTAMP_DESC = "Unix timestamp in milliseconds"


class PolygonAgg(BaseModel):
    """OHLCV aggregate bar from Polygon.io API."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    open: float | None = Field(default=None, description=_OPEN_DESC)
    high: float | None = Field(default=None, description=_HIGH_DESC)
    low: float | None = Field(default=None, description=_LOW_DESC)
    close: float | None = Field(default=None, description=_CLOSE_DESC)
    volume: float | None = Field(default=None, description=_VOLUME_DESC)
    vwap: float | None = Field(default=None, description=_VWAP_DESC)
    timestamp: int | None = Field(default=None, description=_TIMESTAMP_DESC)
    transactions: int | None = Field(default=None, description="Number of transactions")
    otc: bool | None = Field(default=None, description="Whether this is OTC data")

    @classmethod
    def from_sdk_agg(cls, agg: Any) -> "PolygonAgg":
        """Create PolygonAgg from Polygon SDK response object.

        Args:
            agg: Polygon SDK aggregate object with OHLCV attributes.

        Returns:
            Validated PolygonAgg instance.
        """
        return cls(
            open=getattr(agg, "open", None),
            high=getattr(agg, "high", None),
            low=getattr(agg, "low", None),
            close=getattr(agg, "close", None),
            volume=getattr(agg, "volume", None),
            vwap=getattr(agg, "vwap", None),
            timestamp=getattr(agg, "timestamp", None),
            transactions=getattr(agg, "transactions", None),
            otc=getattr(agg, "otc", None),
        )


class PolygonGroupedAgg(BaseModel):
    """Daily grouped aggregate for all tickers from Polygon.io."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    ticker: str = Field(description="Ticker symbol (e.g., X:BTCUSD)")
    open: float | None = Field(default=None, description=_OPEN_DESC)
    high: float | None = Field(default=None, description=_HIGH_DESC)
    low: float | None = Field(default=None, description=_LOW_DESC)
    close: float | None = Field(default=None, description=_CLOSE_DESC)
    volume: float | None = Field(default=None, description=_VOLUME_DESC)
    vwap: float | None = Field(default=None, description=_VWAP_DESC)
    timestamp: int | None = Field(default=None, description=_TIMESTAMP_DESC)
    transactions: int | None = Field(default=None, description="Number of transactions")

    @classmethod
    def from_sdk_agg(cls, agg: Any) -> "PolygonGroupedAgg":
        """Create PolygonGroupedAgg from Polygon SDK response object.

        Args:
            agg: Polygon SDK grouped aggregate with ticker and OHLCV data.

        Returns:
            Validated PolygonGroupedAgg instance.
        """
        return cls(
            ticker=getattr(agg, "ticker", "") or getattr(agg, "T", ""),
            open=getattr(agg, "open", None) or getattr(agg, "o", None),
            high=getattr(agg, "high", None) or getattr(agg, "h", None),
            low=getattr(agg, "low", None) or getattr(agg, "l", None),
            close=getattr(agg, "close", None) or getattr(agg, "c", None),
            volume=getattr(agg, "volume", None) or getattr(agg, "v", None),
            vwap=getattr(agg, "vwap", None) or getattr(agg, "vw", None),
            timestamp=getattr(agg, "timestamp", None) or getattr(agg, "t", None),
            transactions=getattr(agg, "transactions", None) or getattr(agg, "n", None),
        )


class PolygonPreviousClose(BaseModel):
    """Previous trading day close data from Polygon.io."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    ticker: str = Field(description="Ticker symbol")
    open: float | None = Field(default=None, description=_OPEN_DESC)
    high: float | None = Field(default=None, description=_HIGH_DESC)
    low: float | None = Field(default=None, description=_LOW_DESC)
    close: float | None = Field(default=None, description=_CLOSE_DESC)
    volume: float | None = Field(default=None, description=_VOLUME_DESC)
    vwap: float | None = Field(default=None, description=_VWAP_DESC)
    timestamp: int | None = Field(default=None, description=_TIMESTAMP_DESC)

    @classmethod
    def from_sdk_agg(cls, ticker: str, agg: Any) -> "PolygonPreviousClose":
        """Create PolygonPreviousClose from Polygon SDK response object.

        Args:
            ticker: Ticker symbol for the data.
            agg: Polygon SDK aggregate object with OHLCV data.

        Returns:
            Validated PolygonPreviousClose instance.
        """
        return cls(
            ticker=ticker,
            open=getattr(agg, "open", None),
            high=getattr(agg, "high", None),
            low=getattr(agg, "low", None),
            close=getattr(agg, "close", None),
            volume=getattr(agg, "volume", None),
            vwap=getattr(agg, "vwap", None),
            timestamp=getattr(agg, "timestamp", None),
        )


class PolygonTicker(BaseModel):
    """Ticker metadata and status information from Polygon.io."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    ticker: str = Field(description="Ticker symbol (e.g., X:BTCUSD)")
    name: str | None = Field(default=None, description="Full name of the asset")
    market: str | None = Field(default=None, description="Market type (crypto, fx, stocks)")
    locale: str | None = Field(default=None, description="Locale (us, global)")
    currency_symbol: str | None = Field(default=None, description="Currency symbol")
    currency_name: str | None = Field(default=None, description="Currency name")
    base_currency_symbol: str | None = Field(default=None, description="Base currency symbol")
    base_currency_name: str | None = Field(default=None, description="Base currency name")
    active: bool | None = Field(default=None, description="Whether ticker is active")
    last_updated_utc: str | None = Field(default=None, description="Last update timestamp")

    @classmethod
    def from_sdk_ticker(cls, ticker: Any) -> "PolygonTicker":
        """Create PolygonTicker from Polygon SDK response object.

        Args:
            ticker: Polygon SDK ticker object with metadata attributes.

        Returns:
            Validated PolygonTicker instance.
        """
        return cls(
            ticker=getattr(ticker, "ticker", ""),
            name=getattr(ticker, "name", None),
            market=getattr(ticker, "market", None),
            locale=getattr(ticker, "locale", None),
            currency_symbol=getattr(ticker, "currency_symbol", None),
            currency_name=getattr(ticker, "currency_name", None),
            base_currency_symbol=getattr(ticker, "base_currency_symbol", None),
            base_currency_name=getattr(ticker, "base_currency_name", None),
            active=getattr(ticker, "active", None),
            last_updated_utc=getattr(ticker, "last_updated_utc", None),
        )
