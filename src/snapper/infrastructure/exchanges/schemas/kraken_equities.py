"""Pydantic schemas for Kraken Equities (FCM Futures) data.

This module provides Pydantic models for validating and parsing
Kraken Equities exchange data from ``wss://ws-equities.kraken.com``
and the internal REST API.

Instrument Data (REST):
    - KrakenEquitiesInstrumentSchema: FCM futures contract metadata.

Ticker Channel (WS channel: ``ticker``):
    - KrakenEquitiesTickerSchema: Real-time ticker (bid/ask/last/volume).

Trade Channel (WS channel: ``trade``):
    - KrakenEquitiesTradeSchema: Individual trade execution.

The WS protocol is identical to Kraken Spot WS v2 with an additional
``asset_class`` field (always ``futures_contract``).
"""

from pydantic import Field

from snapper.infrastructure.exchanges.schemas.base import ExchangeResponse


class KrakenEquitiesInstrumentSchema(ExchangeResponse):
    """FCM futures contract specification from internal REST API.

    Source: ``iapi.kraken.com/api/internal/markets/all/futures-contracts``

    Attributes:
        symbol: Contract symbol (e.g., ``CLM6.NYMEX``).
        name: Full display name (e.g., ``CLM6 19May26``).
        short_name: Human-readable product name (e.g., ``Crude Oil``).
        contract_name: Contract code without exchange (e.g., ``CLM6``).
        tradable: Whether the contract is currently tradable.
        status: Trading status (e.g., ``active``, ``undefined``).
        instrument_status: Instrument lifecycle (e.g., ``active``, ``inactive``).
        category: Product category (e.g., ``Energies``, ``Metals``, ``Indices``).
        exchange: Exchange venue (e.g., ``NYMEX``, ``CME``, ``COMEX``, ``CBOT``).
        maturity: Contract expiry as Unix timestamp.
        maturity_type: Expiry cycle (e.g., ``monthly``, ``quarterly``).
        contract_size: Contract multiplier as string.
        tick_size: Minimum price increment as string.
        tick_value: Dollar value per tick as string.
        base: Base currency (e.g., ``USD``).
        quote: Quote currency (e.g., ``USD``).
        intraday_margin: Intraday margin requirement as string.
        initial_margin: Initial margin requirement as string.
        maintenance_margin: Maintenance margin requirement as string.
        delayed: Whether data is delayed.
    """

    symbol: str
    name: str = ""
    short_name: str = Field(default="", alias="short_name")
    contract_name: str = Field(default="", alias="contract_name")
    tradable: bool = False
    status: str = ""
    instrument_status: str = Field(default="", alias="instrument_status")
    category: str = ""
    exchange: str = ""
    maturity: int | None = None
    maturity_type: str = Field(default="", alias="maturity_type")
    contract_size: str = Field(default="0", alias="contract_size")
    tick_size: str = Field(default="0", alias="tick_size")
    tick_value: str = Field(default="0", alias="tick_value")
    base: str = ""
    quote: str = ""
    intraday_margin: str = Field(default="0", alias="intraday_margin")
    initial_margin: str = Field(default="0", alias="initial_margin")
    maintenance_margin: str = Field(default="0", alias="maintenance_margin")
    delayed: bool = True
    rank: int | None = None


class KrakenEquitiesTickerSchema(ExchangeResponse):
    """Kraken Equities ticker from WS ``ticker`` channel.

    Attributes:
        symbol: Contract symbol (e.g., ``CLM6.NYMEX``).
        bid: Best bid price.
        bid_qty: Best bid quantity.
        ask: Best ask price.
        ask_qty: Best ask quantity.
        last: Last traded price.
        volume: 24-hour traded volume.
        vwap: 24-hour volume-weighted average price.
        low: 24-hour low.
        high: 24-hour high.
        open: Session open price.
        close: Session close price (present in snapshots).
        change: Price change (absolute).
        change_pct: Price change (percentage).
        prev_day_close: Previous day close.
        prev_day_volume: Previous day volume.
        open_interest: Total open interest.
        is_extended_hours: Whether in extended trading hours.
    """

    symbol: str
    bid: float | None = None
    bid_qty: float | None = None
    ask: float | None = None
    ask_qty: float | None = None
    last: float | None = None
    volume: float | None = None
    vwap: float | None = None
    low: float | None = None
    high: float | None = None
    open: float | None = None
    close: float | None = None
    change: float | None = None
    change_pct: float | None = None
    prev_day_close: float | None = None
    prev_day_volume: float | None = None
    open_interest: float | None = None
    is_extended_hours: bool | None = None


class KrakenEquitiesTradeSchema(ExchangeResponse):
    """Kraken Equities individual trade from WS ``trade`` channel.

    Attributes:
        symbol: Contract symbol (e.g., ``CLM6.NYMEX``).
        side: Trade direction (``buy``, ``sell``, or ``undefined``).
        price: Execution price.
        qty: Execution quantity (number of contracts).
        timestamp: Trade time (ISO 8601).
        sequence: Sequence number for ordering.
        index: Unique trade index.
    """

    symbol: str
    side: str
    price: float
    qty: float
    timestamp: str
    sequence: int
    index: int


__all__ = [
    "KrakenEquitiesInstrumentSchema",
    "KrakenEquitiesTickerSchema",
    "KrakenEquitiesTradeSchema",
]
