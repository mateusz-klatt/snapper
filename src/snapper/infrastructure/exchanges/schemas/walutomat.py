"""Pydantic schemas for Walutomat FX exchange REST API data.

This module provides Pydantic models for validating and parsing Walutomat
exchange API responses. Walutomat is a Polish FX exchange specializing
in currency trading pairs like EUR/PLN, USD/PLN, etc.

Market Data:
    - WalutomatBestOffer: Current best bid/ask prices with forex rate
    - WalutomatLastExchange: Recent exchange transaction record
    - WalutomatDayExchange: Daily exchange volume summary
    - WalutomatMarketPair: Complete market pair data with offers and history
    - WalutomatMarketResponse: API response containing all market pairs

Note: Walutomat provides REST API only (no WebSocket) with polling-based
price updates. The schemas handle timestamp parsing and provide factory
methods for converting API responses to typed objects.
"""

from datetime import datetime
from typing import Any

from pydantic import BaseModel
from pydantic import Field
from pydantic import field_validator

from snapper.infrastructure.exchanges.schemas.base import EXCHANGE_SCHEMA_CONFIG


class WalutomatBestOffer(BaseModel):
    """Current best bid/ask prices from Walutomat."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    bid_now: float = Field(description="Current best bid price")
    ask_now: float = Field(description="Current best ask price")
    forex_now: float = Field(description="Mid-market forex rate")
    bid_old: float | None = Field(default=None, description="Previous best bid price")
    ask_old: float | None = Field(default=None, description="Previous best ask price")
    forex_old: float | None = Field(default=None, description="Previous forex rate")
    ask_trend: str | None = Field(default=None, description="Ask price trend")
    bid_trend: str | None = Field(default=None, description="Bid price trend")
    forex_trend: str | None = Field(default=None, description="Forex rate trend")


class WalutomatLastExchange(BaseModel):
    """Recent exchange transaction record from Walutomat."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    ts: datetime = Field(description="Timestamp of the exchange")
    price: float = Field(description="Exchange price")
    volume: float = Field(description="Volume exchanged")

    @field_validator("ts", mode="before")
    @classmethod
    def parse_ts(cls, v: str | datetime) -> datetime:
        """Parse timestamp string to datetime.

        Args:
            v: ISO format timestamp string or datetime.

        Returns:
            Parsed datetime object.
        """
        if isinstance(v, datetime):
            return v
        return datetime.fromisoformat(v.replace("Z", "+00:00"))


class WalutomatDayExchange(BaseModel):
    """Daily exchange volume summary from Walutomat."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    day: datetime = Field(description="Day as datetime")
    volume: float = Field(description="Total volume for the day")

    @field_validator("day", mode="before")
    @classmethod
    def parse_day(cls, v: str | datetime) -> datetime:
        """Parse day string to datetime.

        Args:
            v: ISO format date string or datetime.

        Returns:
            Parsed datetime object.
        """
        if isinstance(v, datetime):
            return v
        return datetime.fromisoformat(v.replace("Z", "+00:00"))


class WalutomatMarketPair(BaseModel):
    """Complete market pair data with offers and exchange history."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    pair: str = Field(description="Currency pair in Walutomat format (EUR_PLN)")
    best_offers: WalutomatBestOffer = Field(
        alias="bestOffers", description="Current best bid/ask offers"
    )
    last_exchanges: list[WalutomatLastExchange] = Field(
        default_factory=list,
        alias="lastExchanges",
        description="Recent exchange transactions",
    )
    day_exchanges: list[WalutomatDayExchange] = Field(
        default_factory=list,
        alias="dayExchanges",
        description="Daily exchange summary",
    )


class WalutomatMarketResponse(BaseModel):
    """API response containing all Walutomat market pairs."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    pairs: list[WalutomatMarketPair] = Field(
        default_factory=list, description="List of all market pairs"
    )

    @classmethod
    def from_api_response(cls, data: list[dict[str, Any]]) -> "WalutomatMarketResponse":
        """Create response from raw API data.

        Args:
            data: List of market pair dictionaries from API.

        Returns:
            Validated WalutomatMarketResponse instance.
        """
        pairs = [WalutomatMarketPair.model_validate(item) for item in data]
        return cls(pairs=pairs)

    def to_dict(self) -> dict[str, WalutomatMarketPair]:
        """Convert to dictionary keyed by pair name.

        Returns:
            Dictionary mapping pair names to WalutomatMarketPair objects.
        """
        return {pair.pair: pair for pair in self.pairs}
