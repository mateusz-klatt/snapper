"""Pydantic schemas for Kraken Futures WebSocket and REST API data.

This module provides Pydantic models for validating and parsing
Kraken Futures exchange data:

Instrument Data (REST):
    - KrakenFuturesMarginLevelSchema: Margin tier specification.
    - KrakenFuturesInstrumentSchema: Futures product metadata.

Ticker Channel (WS feed: ``ticker``):
    - KrakenFuturesTickerSchema: Real-time ticker (bid/ask/last/mark/funding).

Trade Channel (WS feed: ``trade``):
    - KrakenFuturesTradeSchema: Individual trade execution.
    - KrakenFuturesTradeEventSchema: Wrapper for trade feed messages.

All schemas inherit ExchangeResponse for parsing exchange responses
(allows extra fields to tolerate API additions).
"""

from typing import Literal

from pydantic import Field

from snapper.infrastructure.exchanges.schemas.base import ExchangeResponse


class KrakenFuturesMarginLevelSchema(ExchangeResponse):
    """Margin tier for a futures instrument.

    Flexible futures (PF_*, FF_*) use numNonContractUnits instead of
    contracts, so both fields are optional.
    """

    contracts: int | None = None
    num_non_contract_units: float | None = Field(default=None, alias="numNonContractUnits")
    initial_margin: float = Field(alias="initialMargin")
    maintenance_margin: float = Field(alias="maintenanceMargin")


class KrakenFuturesInstrumentSchema(ExchangeResponse):
    """Kraken Futures product specification from REST get_instruments().

    Attributes:
        symbol: Exchange product ID (e.g., ``PI_XBTUSD``).
        type: Product type (e.g., ``futures_inverse``, ``futures_vanilla``).
        underlying: Reference rate symbol (e.g., ``rr_xbtusd``).
        tick_size: Minimum price increment.
        contract_size: Contract multiplier.
        tradeable: Whether the product is currently tradeable.
        base: Base currency (e.g., ``BTC``).
        quote: Quote currency (e.g., ``USD``).
        pair: Currency pair string (e.g., ``BTC:USD``).
        opening_date: Product listing date (ISO 8601).
        last_trading_time: Expiry for fixed-maturity, None for perpetuals.
        margin_levels: Professional margin tier schedule.
        funding_rate_coefficient: Funding period multiplier (perpetuals only).
        max_relative_funding_rate: Max funding rate cap (perpetuals only).
        isin: ISIN identifier.
        post_only: Whether only post-only orders are accepted.
        category: Product category tag.
        tradfi: Whether this is a TradFi futures product.
        tags: Product classification tags.
    """

    symbol: str
    type: str
    underlying: str | None = None
    tick_size: float = Field(alias="tickSize")
    contract_size: float = Field(alias="contractSize")
    tradeable: bool
    base: str | None = None
    quote: str | None = None
    pair: str | None = None
    opening_date: str | None = Field(default=None, alias="openingDate")
    last_trading_time: str | None = Field(default=None, alias="lastTradingTime")
    margin_levels: list[KrakenFuturesMarginLevelSchema] = Field(
        default_factory=list, alias="marginLevels"
    )
    funding_rate_coefficient: int | None = Field(default=None, alias="fundingRateCoefficient")
    max_relative_funding_rate: float | None = Field(default=None, alias="maxRelativeFundingRate")
    impact_mid_size: float | None = Field(default=None, alias="impactMidSize")
    max_position_size: float | None = Field(default=None, alias="maxPositionSize")
    isin: str | None = None
    contract_value_trade_precision: int | None = Field(
        default=None, alias="contractValueTradePrecision"
    )
    post_only: bool = Field(default=False, alias="postOnly")
    fee_schedule_uid: str | None = Field(default=None, alias="feeScheduleUid")
    mtf: bool | None = None
    category: str | None = None
    tradfi: bool = False
    tags: list[str] = Field(default=[])


class KrakenFuturesTickerSchema(ExchangeResponse):
    """Kraken Futures ticker data from REST or WS ``ticker`` feed.

    Attributes:
        symbol: Product ID (e.g., ``PF_XBTUSD``).
        last: Last traded price.
        last_time: Timestamp of last trade (ISO 8601).
        last_size: Size of last trade.
        tag: Product tag (``perpetual``, ``month``, ``quarter``).
        pair: Currency pair (e.g., ``BTC:USD``).
        mark_price: Current mark price.
        bid: Best bid price.
        bid_size: Best bid size.
        ask: Best ask price.
        ask_size: Best ask size.
        vol24h: 24-hour volume.
        volume_quote: 24-hour volume in quote currency.
        open_interest: Total open interest.
        open24h: Price 24 hours ago.
        high24h: 24-hour high.
        low24h: 24-hour low.
        funding_rate: Current funding rate (perpetuals only).
        funding_rate_prediction: Predicted next funding rate.
        index_price: Underlying index price.
        suspended: Whether trading is suspended.
        post_only: Whether only post-only orders are accepted.
        change24h: 24-hour price change percentage.
    """

    symbol: str
    last: float | None = None
    last_time: str | None = Field(default=None, alias="lastTime")
    last_size: float | None = Field(default=None, alias="lastSize")
    tag: str | None = None
    pair: str | None = None
    mark_price: float | None = Field(default=None, alias="markPrice")
    bid: float | None = None
    bid_size: float | None = Field(default=None, alias="bidSize")
    ask: float | None = None
    ask_size: float | None = Field(default=None, alias="askSize")
    vol24h: float | None = None
    volume_quote: float | None = Field(default=None, alias="volumeQuote")
    open_interest: float | None = Field(default=None, alias="openInterest")
    open24h: float | None = None
    high24h: float | None = None
    low24h: float | None = None
    funding_rate: float | None = Field(default=None, alias="fundingRate")
    funding_rate_prediction: float | None = Field(default=None, alias="fundingRatePrediction")
    index_price: float | None = Field(default=None, alias="indexPrice")
    suspended: bool = False
    post_only: bool = Field(default=False, alias="postOnly")
    change24h: float | None = None


class KrakenFuturesTradeSchema(ExchangeResponse):
    """Kraken Futures individual trade from REST or WS ``trade`` feed.

    Attributes:
        time: Trade timestamp (ISO 8601 string from REST, int ms from WS).
        trade_id: Sequential trade ID within the product.
        price: Execution price.
        size: Execution size in contracts (aliased from ``qty`` in WS).
        side: Trade direction (``buy`` or ``sell``).
        seq: Sequence number for ordering (WS only).
        type: Trade type (e.g., ``fill``, REST only).
        uid: Unique trade identifier (UUID).
    """

    time: int | str
    trade_id: int | None = Field(default=None, alias="trade_id")
    price: float
    size: float = Field(alias="qty")
    side: Literal["buy", "sell"]
    seq: int | None = None
    type: str | None = None
    uid: str | None = None


class KrakenFuturesTradeEventSchema(ExchangeResponse):
    """Wrapper for WS ``trade`` feed messages.

    The Kraken Futures WS delivers trade events with a ``feed`` discriminator
    and a nested list of trades per product.

    Attributes:
        feed: Feed name (always ``trade``).
        product_id: Product symbol (e.g., ``PI_XBTUSD``).
        trades: List of individual trade records.
    """

    feed: Literal["trade"]
    product_id: str
    trades: list[KrakenFuturesTradeSchema] = Field(default=[])


class KrakenFuturesTickerEventSchema(ExchangeResponse):
    """Wrapper for WS ``ticker`` feed messages.

    The Kraken Futures WS delivers ticker snapshots with a ``feed`` discriminator
    per product.

    Attributes:
        feed: Feed name (``ticker`` or ``ticker_lite``).
        product_id: Product symbol (e.g., ``PF_XBTUSD``).
    """

    feed: Literal["ticker", "ticker_lite"]
    product_id: str
    bid: float | None = None
    bid_size: float | None = None
    ask: float | None = None
    ask_size: float | None = None
    last: float | None = None
    change: float | None = None
    premium: float | None = None
    funding_rate: float | None = None
    funding_rate_prediction: float | None = None
    mark_price: float | None = Field(default=None, alias="markPrice")
    index_price: float | None = Field(default=None, alias="indexPrice")
    volume: float | None = None
    volume_quote: float | None = Field(default=None, alias="volumeQuote")
    open_interest: float | None = Field(default=None, alias="openInterest")
    suspended: bool | None = None
    tag: str | None = None
    pair: str | None = None
    post_only: bool | None = None
    time: float | None = None
    relative_funding_rate: float | None = None
    relative_funding_rate_prediction: float | None = None
    next_funding_rate_time: float | None = None


class KrakenFuturesFillSchema(ExchangeResponse):
    """Private fill event from authenticated ``fills``/``fills_snapshot`` WS channel.

    The WS payload uses ``instrument`` (not ``symbol``), ``buy: bool`` (not
    ``side``), ``qty`` (not ``size``), and ``time: int`` (ms epoch, not ISO).

    Attributes:
        instrument: Kraken Futures product ID (e.g., ``PI_ETHUSD``).
        time: Fill timestamp as millisecond epoch integer.
        price: Execution price.
        seq: Sequence number for ordering.
        buy: True if the fill is a buy, False for sell.
        qty: Fill quantity in contracts.
        remaining_order_qty: Remaining unfilled quantity on the order.
        order_id: Exchange order ID that generated this fill.
        fill_id: Unique fill identifier.
        fill_type: Fill classification (``maker``, ``taker``, ``liquidation``, etc.).
        fee_paid: Fee amount charged for this fill.
        fee_currency: Currency of fee_paid.
        taker_order_type: Order type of the taker (``lmt``, ``ioc``, ``mkt``, etc.).
        order_type: Order type of this fill's order.
        cli_ord_id: Client-provided order ID (if set on order creation).
    """

    instrument: str
    time: int
    price: float
    seq: int | None = None
    buy: bool
    qty: float
    remaining_order_qty: float | None = None
    order_id: str
    fill_id: str
    fill_type: str | None = None
    fee_paid: float | None = None
    fee_currency: str | None = None
    taker_order_type: str | None = None
    order_type: str | None = None
    cli_ord_id: str | None = None


class KrakenFuturesOpenOrderSchema(ExchangeResponse):
    """Private order from ``open_orders_snapshot`` or ``open_orders`` delta WS channel.

    The WS payload uses ``instrument`` (not ``symbol``), ``direction: int``
    (0=buy, 1=sell), ``type`` (not ``orderType``), and ``time: int`` (ms epoch).

    Attributes:
        instrument: Kraken Futures product ID (e.g., ``PI_XBTUSD``).
        time: Order creation timestamp as millisecond epoch.
        last_update_time: Last update timestamp as millisecond epoch.
        qty: Total order quantity.
        filled: Cumulative filled quantity.
        limit_price: Limit price (0.0 if not applicable).
        stop_price: Stop price (0.0 if not applicable).
        type: Order type string (``limit``, ``stop``, ``take_profit``).
        order_id: Exchange order ID.
        direction: 0 for buy, 1 for sell.
        reduce_only: Whether the order is reduce-only.
        cli_ord_id: Client-provided order ID.
    """

    instrument: str
    time: int
    last_update_time: int | None = None
    qty: float
    filled: float = 0.0
    limit_price: float = 0.0
    stop_price: float = 0.0
    type: str = "limit"
    order_id: str
    direction: int = 0
    reduce_only: bool = False
    cli_ord_id: str | None = None


__all__ = [
    "KrakenFuturesFillSchema",
    "KrakenFuturesInstrumentSchema",
    "KrakenFuturesMarginLevelSchema",
    "KrakenFuturesOpenOrderSchema",
    "KrakenFuturesTickerEventSchema",
    "KrakenFuturesTickerSchema",
    "KrakenFuturesTradeEventSchema",
    "KrakenFuturesTradeSchema",
]
