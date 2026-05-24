"""Pydantic schemas for Kraken WebSocket and REST API data.

This module provides comprehensive Pydantic models for validating and parsing
Kraken exchange data across multiple channels:

Instrument Channel:
    - KrakenInstrumentSubscribeParamsSchema: Subscription request parameters
    - KrakenInstrumentPairSchema: Trading pair specifications
    - KrakenInstrumentEventEnvelope: Instrument update messages

Ticker Channel:
    - KrakenTickerSubscribeParamsSchema: Subscription request parameters
    - KrakenTickerSchema: Real-time ticker data (bid/ask/last/volume)
    - KrakenTickerEventEnvelope: Ticker update messages

OHLC Channel:
    - KrakenOhlcSubscribeParamsSchema: Subscription with interval parameter
    - KrakenCandleSchema: OHLCV candle data
    - KrakenOhlcEventEnvelope: Candle update messages

Trade Channel:
    - KrakenTradeSubscribeParamsSchema: Subscription request parameters
    - KrakenTradeSchema: Individual trade data
    - KrakenTradeEventEnvelope: Trade update messages

Execution Channel (authenticated):
    - KrakenExecutionSubscribeParamsSchema: Subscription with auth token
    - KrakenExecutionSchema: User's order execution reports
    - KrakenExecutionEventEnvelope: Execution update messages

Order Management:
    - KrakenAddOrderParamsSchema: Order creation request
    - KrakenAddOrderResponseSchema: Order creation response
    - KrakenCancelOrderParamsSchema: Order cancellation request
    - KrakenCancelOrderResponseSchema: Order cancellation response

All schemas inherit ExchangeResponse for parsing exchange responses
(allows extra fields) or ExchangeRequest for outgoing requests
(forbids extra fields to catch typos).
"""

from collections.abc import Iterable
from collections.abc import Sequence
from typing import Any
from typing import Literal

from pydantic import Field

from snapper.infrastructure.exchanges.schemas.base import ExchangeRequest
from snapper.infrastructure.exchanges.schemas.base import ExchangeResponse


def _empty_pair_list() -> list[KrakenInstrumentPairSchema]:
    """Create an empty list of instrument pairs.

    Returns:
        Empty list for use as default factory.
    """
    return []


def _empty_asset_list() -> list[KrakenInstrumentAssetSchema]:
    """Create an empty list of instrument assets.

    Returns:
        Empty list for use as default factory.
    """
    return []


class KrakenInstrumentSubscribeParamsSchema(ExchangeRequest):
    """Parameters for subscribing to instrument WebSocket channel."""

    channel: Literal["instrument"] = Field(default="instrument", frozen=True)
    snapshot: bool = True
    include_tokenized_assets: bool = True

    def as_params(self) -> dict[str, object]:
        """Convert schema to WebSocket subscription parameters.

        Returns:
            Dictionary of subscription parameters with aliases applied.
        """
        return self.model_dump(by_alias=True, exclude_none=True)


class KrakenInstrumentSubscriptionAckSchema(ExchangeResponse):
    """Subscription acknowledgement for instrument channel."""

    channel: Literal["instrument"]
    event: Literal["subscribe", "unsubscribe"]
    status: Literal["ok", "error"]
    message: str | None = None
    request_id: int | None = Field(default=None, alias="reqid")


class KrakenInstrumentAssetSchema(ExchangeResponse):
    """Kraken asset/currency metadata."""

    asset: str | None = None
    status: str | None = None
    altname: str | None = None
    decimals: int | None = None


class KrakenInstrumentFeeScheduleSchema(ExchangeResponse):
    """Kraken trading fee schedule entry."""

    type: str
    percent: float | None = None
    symbol: str | None = None


class KrakenInstrumentPairSchema(ExchangeResponse):
    """Kraken trading pair specification and constraints."""

    symbol: str
    status: str | None = None
    wsname: str | None = None
    altname: str | None = None
    base_currency: str | None = Field(default=None, alias="baseCurrency")
    quote_currency: str | None = Field(default=None, alias="quoteCurrency")
    base_asset: str | None = Field(default=None, alias="base")
    quote_asset: str | None = Field(default=None, alias="quote")
    fees: list[KrakenInstrumentFeeScheduleSchema] | None = None
    qty_precision: int | None = None
    qty_increment: float | None = None
    price_precision: int | None = None
    cost_precision: int | None = None
    qty_decimals: int | None = None
    cost_decimals: int | None = None
    tick_size: float | None = None
    price_increment: float | None = None
    qty_min: float | None = None
    qty_max: float | None = None
    cost_min: float | None = None
    marginable: bool | None = None
    has_index: bool | None = None
    margin_initial: float | None = None
    position_limit_long: float | None = None
    position_limit_short: float | None = None

    def as_summary(self) -> dict[str, object]:
        """Convert to summary dictionary excluding None values.

        Returns:
            Dictionary with pair data, aliases applied, None values excluded.
        """
        return self.model_dump(by_alias=True, exclude_none=True)


class KrakenInstrumentSnapshotSchema(ExchangeResponse):
    """Snapshot of all available instruments and assets."""

    pairs: list[KrakenInstrumentPairSchema] = Field(default_factory=_empty_pair_list)
    assets: list[KrakenInstrumentAssetSchema] = Field(default_factory=_empty_asset_list)

    def iter_pairs(self) -> Iterable[KrakenInstrumentPairSchema]:
        """Iterate over trading pairs in snapshot.

        Returns:
            Tuple of trading pair schemas from the snapshot.
        """
        return tuple(self.pairs)


class KrakenInstrumentEventEnvelope(ExchangeResponse):
    """WebSocket message envelope for instrument updates."""

    channel: Literal["instrument"]
    type: str
    data: (
        KrakenInstrumentSnapshotSchema
        | list[KrakenInstrumentPairSchema]
        | KrakenInstrumentPairSchema
    )
    time: float | None = None

    def iter_pairs(self) -> list[KrakenInstrumentPairSchema]:
        """Extract and return all trading pairs from envelope.

        Returns:
            List of trading pair schemas extracted from the envelope data.
        """
        if isinstance(self.data, KrakenInstrumentSnapshotSchema):
            return list(self.data.iter_pairs())
        if isinstance(self.data, KrakenInstrumentPairSchema):
            return [self.data]
        return list(self.data)

    def as_dicts(self) -> list[dict[str, object]]:
        """Convert all pairs to summary dictionaries.

        Returns:
            List of dictionaries, one per trading pair.
        """
        return [pair.as_summary() for pair in self.iter_pairs()]


class KrakenTickerSubscribeParamsSchema(ExchangeRequest):
    """Parameters for subscribing to ticker WebSocket channel."""

    channel: Literal["ticker"] = Field(default="ticker", frozen=True)
    symbol: Sequence[str]
    snapshot: bool = True

    def as_params(self) -> dict[str, object]:
        """Convert schema to WebSocket subscription parameters.

        Returns:
            Dictionary of subscription parameters with aliases applied.
        """
        return self.model_dump(by_alias=True, exclude_none=True)


class KrakenTickerSubscriptionResultSchema(ExchangeResponse):
    """Result details from ticker subscription acknowledgement."""

    channel: Literal["ticker"]
    symbol: str
    snapshot: bool | None = None
    warnings: list[str] | None = None


class KrakenTickerSubscriptionAckSchema(ExchangeResponse):
    """Subscription acknowledgement for ticker channel."""

    method: Literal["subscribe"]
    result: KrakenTickerSubscriptionResultSchema
    success: bool
    time_in: str | None = Field(default=None, alias="time_in")
    time_out: str | None = Field(default=None, alias="time_out")
    error: str | None = None
    request_id: int | None = Field(default=None, alias="reqid")


class KrakenTickerSchema(ExchangeResponse):
    """Real-time ticker data with bid/ask/last prices."""

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
    change: float | None = None
    change_pct: float | None = None


class KrakenTickerEventEnvelope(ExchangeResponse):
    """WebSocket message envelope for ticker updates."""

    channel: Literal["ticker"]
    type: str | None = None
    symbol: str
    data: dict[str, Any] | KrakenTickerSchema
    time: float | None = None


class KrakenOhlcSubscribeParamsSchema(ExchangeRequest):
    """Parameters for subscribing to OHLC WebSocket channel."""

    channel: Literal["ohlc"] = Field(default="ohlc", frozen=True)
    symbol: Sequence[str]
    interval: int
    snapshot: bool = True
    request_id: int | None = Field(default=None, alias="reqid")

    def as_params(self) -> dict[str, object]:
        """Convert schema to WebSocket subscription parameters.

        Returns:
            Dictionary of subscription parameters with aliases applied.
        """
        return self.model_dump(by_alias=True, exclude_none=True)


class KrakenOhlcSubscriptionResultSchema(ExchangeResponse):
    """Result details from OHLC subscription acknowledgement."""

    channel: Literal["ohlc"]
    symbol: str
    interval: int | None = None
    snapshot: bool | None = None
    warnings: list[str] | None = None


class KrakenOhlcSubscriptionAckSchema(ExchangeResponse):
    """Subscription acknowledgement for OHLC channel."""

    method: Literal["subscribe"]
    result: KrakenOhlcSubscriptionResultSchema
    success: bool
    time_in: str | None = Field(default=None, alias="time_in")
    time_out: str | None = Field(default=None, alias="time_out")
    error: str | None = None
    request_id: int | None = Field(default=None, alias="reqid")


class KrakenCandleSchema(ExchangeResponse):
    """OHLCV candle data from Kraken.

    Kraken v2 deprecated the ``timestamp`` field in favour of
    ``interval_begin`` (which carries the candle bucket start, not the
    server emission time). We use ``interval_begin`` exclusively; the
    legacy ``timestamp`` field is not modelled here. ``ExchangeResponse``
    is configured with ``extra="allow"`` so Kraken can keep emitting
    ``timestamp`` for back-compat without breaking deserialization.
    """

    symbol: str | None = None
    open: float
    high: float
    low: float
    close: float
    vwap: float | None = None
    trades: int | None = None
    volume: float | None = None
    interval_begin: str | None = None
    interval: int | None = None

    def as_dict(self) -> dict[str, Any]:
        """Convert candle to dictionary excluding None values.

        Returns:
            Dictionary with OHLCV data, None values excluded.
        """
        return self.model_dump(exclude_none=True)


class KrakenOhlcEventEnvelope(ExchangeResponse):
    """WebSocket message envelope for OHLC candle updates."""

    channel: Literal["ohlc"]
    type: Literal["snapshot", "update"] | None = None
    data: list[KrakenCandleSchema]
    timestamp: str | None = None

    def as_dicts(self) -> list[dict[str, Any]]:
        """Convert all candles to dictionaries.

        Returns:
            List of dictionaries, one per candle.
        """
        return [candle.as_dict() for candle in self.data]

    def primary_symbol(self) -> str | None:
        """Extract first non-null symbol from candle data.

        Returns:
            Symbol string if found, None otherwise.
        """
        for candle in self.data:
            if candle.symbol:
                return candle.symbol
        return None


class KrakenTradeSubscribeParamsSchema(ExchangeRequest):
    """Parameters for subscribing to trade WebSocket channel."""

    channel: Literal["trade"] = Field(default="trade", frozen=True)
    symbol: Sequence[str]
    snapshot: bool = False
    request_id: int | None = Field(default=None, alias="reqid")

    def as_params(self) -> dict[str, object]:
        """Convert schema to WebSocket subscription parameters.

        Returns:
            Dictionary of subscription parameters with aliases applied.
        """
        return self.model_dump(by_alias=True, exclude_none=True)


class KrakenTradeSubscriptionAckSchema(ExchangeResponse):
    """Subscription acknowledgement for trade channel."""

    method: Literal["subscribe"]
    result: dict[str, Any]
    success: bool
    time_in: str | None = Field(default=None, alias="time_in")
    time_out: str | None = Field(default=None, alias="time_out")
    request_id: int | None = Field(default=None, alias="reqid")
    error: str | None = None
    warnings: list[str] | None = None


class KrakenTradeSchema(ExchangeResponse):
    """Individual trade data from Kraken."""

    symbol: str
    side: str
    qty: float
    price: float
    ord_type: str | None = Field(default=None, alias="ord_type")
    trade_id: int | None = None
    timestamp: str

    def as_dict(self) -> dict[str, Any]:
        """Convert trade to dictionary excluding None values.

        Returns:
            Dictionary with trade data, None values excluded.
        """
        return self.model_dump(exclude_none=True)


class KrakenTradeEventEnvelope(ExchangeResponse):
    """WebSocket message envelope for trade updates."""

    channel: Literal["trade"]
    type: Literal["snapshot", "update"] | None = None
    data: list[KrakenTradeSchema]

    def symbol(self) -> str | None:
        """Extract first non-null symbol from trade data.

        Returns:
            Symbol string if found, None otherwise.
        """
        for event in self.data:
            if event.symbol:
                return event.symbol
        return None

    def as_dicts(self) -> list[dict[str, Any]]:
        """Convert all trades to dictionaries.

        Returns:
            List of dictionaries, one per trade.
        """
        return [event.as_dict() for event in self.data]


class KrakenExecutionSubscribeParamsSchema(ExchangeRequest):
    """Parameters for subscribing to executions WebSocket channel."""

    channel: Literal["executions"] = Field(default="executions", frozen=True)
    token: str | None = None
    snap_trades: bool | None = None
    snap_orders: bool | None = None
    order_status: bool | None = None
    rebased: bool | None = None
    ratecounter: bool | None = None
    users: Literal["all"] | None = None
    snapshot_trades: bool | None = None
    snapshot: bool | None = None
    request_id: int | None = Field(default=None, alias="reqid")

    def as_params(self) -> dict[str, object]:
        """Convert schema to WebSocket subscription parameters.

        Returns:
            Dictionary of subscription parameters with aliases applied.
        """
        return self.model_dump(by_alias=True, exclude_none=True)


class KrakenExecutionSubscriptionResultSchema(ExchangeResponse):
    """Result details from executions subscription acknowledgement."""

    channel: Literal["executions"]
    snap_trades: bool | None = None
    snap_orders: bool | None = None
    maxratecount: int | None = None
    snapshot: bool | None = None


class KrakenExecutionSubscriptionAckSchema(ExchangeResponse):
    """Subscription acknowledgement for executions channel."""

    method: Literal["subscribe"]
    result: KrakenExecutionSubscriptionResultSchema
    success: bool
    time_in: str | None = Field(default=None, alias="time_in")
    time_out: str | None = Field(default=None, alias="time_out")
    warnings: list[str] | None = None
    error: str | None = None
    request_id: int | None = Field(default=None, alias="reqid")


class KrakenExecutionFeeSchema(ExchangeResponse):
    """Execution fee details."""

    asset: str | None = None
    qty: float | None = None

    def as_dict(self) -> dict[str, Any]:
        """Convert fee to dictionary excluding None values.

        Returns:
            Dictionary with fee data, None values excluded.
        """
        return self.model_dump(exclude_none=True)


class KrakenExecutionSchema(ExchangeResponse):
    """Order execution report data."""

    order_id: str | None = None
    order_userref: int | None = None
    symbol: str | None = None
    exec_id: str | None = None
    exec_type: str | None = None
    trade_id: int | None = None
    side: str | None = None
    order_type: str | None = None
    order_status: str | None = None
    time_in_force: str | None = None
    position_status: str | None = None
    liquidity_ind: str | None = None
    reason: str | None = None
    user: str | None = None
    timestamp: str | None = None
    amended: bool | None = None
    liquidated: bool | None = None
    margin: bool | None = None
    margin_borrow: bool | None = None
    post_only: bool | None = None
    reduce_only: bool | None = None
    no_mpp: bool | None = None
    contingent: dict[str, Any] | None = None
    triggers: dict[str, Any] | None = None
    fees: list[KrakenExecutionFeeSchema] | None = None
    order_qty: float | None = None
    cash_order_qty: float | None = None
    cum_qty: float | None = None
    cum_cost: float | None = None
    last_qty: float | None = None
    last_price: float | None = None
    avg_price: float | None = None
    cost: float | None = None
    limit_price: float | None = None
    stop_price: float | None = None
    display_qty: float | None = None
    display_qty_remain: float | None = None
    fee_usd_equiv: float | None = None
    fee_ccy_pref: str | None = None

    def as_dict(self) -> dict[str, Any]:
        """Convert execution to dictionary excluding None values.

        Returns:
            Dictionary with execution data, None values excluded.
        """
        return self.model_dump(exclude_none=True)


class KrakenExecutionEventEnvelope(ExchangeResponse):
    """WebSocket message envelope for execution updates."""

    channel: Literal["executions"]
    type: Literal["snapshot", "update"] | None = None
    data: list[KrakenExecutionSchema]
    sequence: int | None = None

    def primary_symbol(self) -> str | None:
        """Extract first non-null symbol from execution data.

        Returns:
            Symbol string if found, None otherwise.
        """
        for report in self.data:
            if report.symbol:
                return report.symbol
        return None

    def as_dicts(self) -> list[dict[str, Any]]:
        """Convert all executions to dictionaries.

        Returns:
            List of dictionaries, one per execution report.
        """
        return [report.as_dict() for report in self.data]


class KrakenAddOrderParamsSchema(ExchangeRequest):
    """Parameters for adding a new order via WebSocket."""

    order_type: str
    side: str
    symbol: str
    limit_price: float | None = None
    order_qty: float | None = None
    cash_order_qty: float | None = None
    time_in_force: str | None = None
    expire_time: str | None = None
    post_only: bool | None = None
    reduce_only: bool | None = None
    margin: bool | None = None
    cl_ord_id: str | None = None
    validate_only: bool | None = Field(default=None, alias="validate")
    stop_price: float | None = None
    trigger: str | None = None
    token: str | None = None


class KrakenAddOrderResultSchema(ExchangeResponse):
    """Result from successful order creation."""

    order_id: str
    order_userref: int | None = None
    cl_ord_id: str | None = None
    warning: list[str] | None = None


class KrakenAddOrderResponseSchema(ExchangeResponse):
    """Response from add_order WebSocket request."""

    method: Literal["add_order"]
    result: KrakenAddOrderResultSchema | None = None
    success: bool
    error: str | None = None
    time_in: str | None = Field(default=None, alias="time_in")
    time_out: str | None = Field(default=None, alias="time_out")
    request_id: int | None = Field(default=None, alias="reqid")


class KrakenCancelOrderParamsSchema(ExchangeRequest):
    """Parameters for cancelling an order via WebSocket."""

    order_id: list[str] | None = None
    cl_ord_id: list[str] | None = None
    token: str | None = None


class KrakenCancelOrderResultSchema(ExchangeResponse):
    """Result from successful order cancellation."""

    order_id: str | None = None
    cl_ord_id: str | None = None
    warning: list[str] | None = None


class KrakenCancelOrderResponseSchema(ExchangeResponse):
    """Response from cancel_order WebSocket request."""

    method: Literal["cancel_order"]
    result: KrakenCancelOrderResultSchema | None = None
    success: bool
    error: str | None = None
    time_in: str | None = Field(default=None, alias="time_in")
    time_out: str | None = Field(default=None, alias="time_out")
    request_id: int | None = Field(default=None, alias="reqid")


__all__ = [
    "KrakenInstrumentSubscribeParamsSchema",
    "KrakenInstrumentSubscriptionAckSchema",
    "KrakenInstrumentAssetSchema",
    "KrakenInstrumentFeeScheduleSchema",
    "KrakenInstrumentPairSchema",
    "KrakenInstrumentSnapshotSchema",
    "KrakenInstrumentEventEnvelope",
    "KrakenTickerSubscribeParamsSchema",
    "KrakenTickerSubscriptionResultSchema",
    "KrakenTickerSubscriptionAckSchema",
    "KrakenTickerSchema",
    "KrakenTickerEventEnvelope",
    "KrakenOhlcSubscribeParamsSchema",
    "KrakenOhlcSubscriptionResultSchema",
    "KrakenOhlcSubscriptionAckSchema",
    "KrakenCandleSchema",
    "KrakenOhlcEventEnvelope",
    "KrakenTradeSubscribeParamsSchema",
    "KrakenTradeSubscriptionAckSchema",
    "KrakenTradeSchema",
    "KrakenTradeEventEnvelope",
    "KrakenExecutionSubscribeParamsSchema",
    "KrakenExecutionSubscriptionResultSchema",
    "KrakenExecutionSubscriptionAckSchema",
    "KrakenExecutionFeeSchema",
    "KrakenExecutionSchema",
    "KrakenExecutionEventEnvelope",
    "KrakenAddOrderParamsSchema",
    "KrakenAddOrderResultSchema",
    "KrakenAddOrderResponseSchema",
    "KrakenCancelOrderParamsSchema",
    "KrakenCancelOrderResultSchema",
    "KrakenCancelOrderResponseSchema",
]
