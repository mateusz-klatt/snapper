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

All schemas use EXCHANGE_SCHEMA_CONFIG for parsing exchange responses
(allows extra fields) or STRICT_SCHEMA_CONFIG for outgoing requests
(forbids extra fields to catch typos).
"""

from collections.abc import Iterable
from collections.abc import Sequence
from typing import Any
from typing import Literal

from pydantic import BaseModel
from pydantic import Field

from snapper.infrastructure.exchanges.schemas.base import EXCHANGE_SCHEMA_CONFIG
from snapper.infrastructure.exchanges.schemas.base import STRICT_SCHEMA_CONFIG


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


class KrakenInstrumentSubscribeParamsSchema(BaseModel):
    """Parameters for subscribing to instrument WebSocket channel."""

    model_config = STRICT_SCHEMA_CONFIG
    channel: Literal["instrument"] = Field(default="instrument", frozen=True)
    snapshot: bool = True
    include_tokenized_assets: bool = True

    def as_params(self) -> dict[str, object]:
        """Convert schema to WebSocket subscription parameters.

        Returns:
            Dictionary of subscription parameters with aliases applied.
        """
        return self.model_dump(by_alias=True, exclude_none=True)


class KrakenInstrumentSubscriptionAckSchema(BaseModel):
    """Subscription acknowledgement for instrument channel."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    channel: Literal["instrument"]
    event: Literal["subscribe", "unsubscribe"]
    status: Literal["ok", "error"]
    message: str | None = None
    request_id: int | None = Field(default=None, alias="reqid")


class KrakenInstrumentAssetSchema(BaseModel):
    """Kraken asset/currency metadata."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    asset: str | None = None
    status: str | None = None
    altname: str | None = None
    decimals: int | None = None


class KrakenInstrumentFeeScheduleSchema(BaseModel):
    """Kraken trading fee schedule entry."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    type: str
    percent: float | None = None
    symbol: str | None = None


class KrakenInstrumentPairSchema(BaseModel):
    """Kraken trading pair specification and constraints."""

    model_config = EXCHANGE_SCHEMA_CONFIG
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


class KrakenInstrumentSnapshotSchema(BaseModel):
    """Snapshot of all available instruments and assets."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    pairs: list[KrakenInstrumentPairSchema] = Field(default_factory=_empty_pair_list)
    assets: list[KrakenInstrumentAssetSchema] = Field(default_factory=_empty_asset_list)

    def iter_pairs(self) -> Iterable[KrakenInstrumentPairSchema]:
        """Iterate over trading pairs in snapshot.

        Returns:
            Tuple of trading pair schemas from the snapshot.
        """
        return tuple(self.pairs)


class KrakenInstrumentEventEnvelope(BaseModel):
    """WebSocket message envelope for instrument updates."""

    model_config = EXCHANGE_SCHEMA_CONFIG
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


class KrakenTickerSubscribeParamsSchema(BaseModel):
    """Parameters for subscribing to ticker WebSocket channel."""

    model_config = STRICT_SCHEMA_CONFIG
    channel: Literal["ticker"] = Field(default="ticker", frozen=True)
    symbol: Sequence[str]
    snapshot: bool = True

    def as_params(self) -> dict[str, object]:
        """Convert schema to WebSocket subscription parameters.

        Returns:
            Dictionary of subscription parameters with aliases applied.
        """
        return self.model_dump(by_alias=True, exclude_none=True)


class KrakenTickerSubscriptionAckSchema(BaseModel):
    """Subscription acknowledgement for ticker channel."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    channel: Literal["ticker"]
    event: Literal["subscribe", "unsubscribe"]
    status: Literal["ok", "error"]
    message: str | None = None
    request_id: int | None = Field(default=None, alias="reqid")


class KrakenTickerSchema(BaseModel):
    """Real-time ticker data with bid/ask/last prices."""

    model_config = EXCHANGE_SCHEMA_CONFIG
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


class KrakenTickerEventEnvelope(BaseModel):
    """WebSocket message envelope for ticker updates."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    channel: Literal["ticker"]
    type: str | None = None
    symbol: str
    data: dict[str, Any] | KrakenTickerSchema
    time: float | None = None


class KrakenOhlcSubscribeParamsSchema(BaseModel):
    """Parameters for subscribing to OHLC WebSocket channel."""

    model_config = STRICT_SCHEMA_CONFIG
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


class KrakenOhlcSubscriptionResultSchema(BaseModel):
    """Result details from OHLC subscription acknowledgement."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    channel: Literal["ohlc"]
    symbol: str
    interval: int | None = None
    snapshot: bool | None = None
    warnings: list[str] | None = None


class KrakenOhlcSubscriptionAckSchema(BaseModel):
    """Subscription acknowledgement for OHLC channel."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    method: Literal["subscribe"]
    result: KrakenOhlcSubscriptionResultSchema
    success: bool
    time_in: str | None = Field(default=None, alias="time_in")
    time_out: str | None = Field(default=None, alias="time_out")
    error: str | None = None
    request_id: int | None = Field(default=None, alias="reqid")


class KrakenCandleSchema(BaseModel):
    """OHLCV candle data from Kraken."""

    model_config = EXCHANGE_SCHEMA_CONFIG
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
    timestamp: str | None = None

    def as_dict(self) -> dict[str, Any]:
        """Convert candle to dictionary excluding None values.

        Returns:
            Dictionary with OHLCV data, None values excluded.
        """
        return self.model_dump(exclude_none=True)


class KrakenOhlcEventEnvelope(BaseModel):
    """WebSocket message envelope for OHLC candle updates."""

    model_config = EXCHANGE_SCHEMA_CONFIG
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


class KrakenTradeSubscribeParamsSchema(BaseModel):
    """Parameters for subscribing to trade WebSocket channel."""

    model_config = STRICT_SCHEMA_CONFIG
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


class KrakenTradeSubscriptionAckSchema(BaseModel):
    """Subscription acknowledgement for trade channel."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    method: Literal["subscribe"]
    result: dict[str, Any]
    success: bool
    time_in: str | None = Field(default=None, alias="time_in")
    time_out: str | None = Field(default=None, alias="time_out")
    request_id: int | None = Field(default=None, alias="reqid")
    error: str | None = None
    warnings: list[str] | None = None


class KrakenTradeSchema(BaseModel):
    """Individual trade data from Kraken."""

    model_config = EXCHANGE_SCHEMA_CONFIG
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


class KrakenTradeEventEnvelope(BaseModel):
    """WebSocket message envelope for trade updates."""

    model_config = EXCHANGE_SCHEMA_CONFIG
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


class KrakenExecutionSubscribeParamsSchema(BaseModel):
    """Parameters for subscribing to executions WebSocket channel."""

    model_config = STRICT_SCHEMA_CONFIG
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


class KrakenExecutionSubscriptionResultSchema(BaseModel):
    """Result details from executions subscription acknowledgement."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    channel: Literal["executions"]
    snap_trades: bool | None = None
    snap_orders: bool | None = None
    maxratecount: int | None = None
    snapshot: bool | None = None


class KrakenExecutionSubscriptionAckSchema(BaseModel):
    """Subscription acknowledgement for executions channel."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    method: Literal["subscribe"]
    result: KrakenExecutionSubscriptionResultSchema
    success: bool
    time_in: str | None = Field(default=None, alias="time_in")
    time_out: str | None = Field(default=None, alias="time_out")
    warnings: list[str] | None = None
    error: str | None = None
    request_id: int | None = Field(default=None, alias="reqid")


class KrakenExecutionFeeSchema(BaseModel):
    """Execution fee details."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    asset: str | None = None
    qty: float | None = None

    def as_dict(self) -> dict[str, Any]:
        """Convert fee to dictionary excluding None values.

        Returns:
            Dictionary with fee data, None values excluded.
        """
        return self.model_dump(exclude_none=True)


class KrakenExecutionSchema(BaseModel):
    """Order execution report data."""

    model_config = EXCHANGE_SCHEMA_CONFIG
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


class KrakenExecutionEventEnvelope(BaseModel):
    """WebSocket message envelope for execution updates."""

    model_config = EXCHANGE_SCHEMA_CONFIG
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


class KrakenAddOrderParamsSchema(BaseModel):
    """Parameters for adding a new order via WebSocket."""

    model_config = STRICT_SCHEMA_CONFIG
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


class KrakenAddOrderResultSchema(BaseModel):
    """Result from successful order creation."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    order_id: str
    order_userref: int | None = None
    cl_ord_id: str | None = None
    warning: list[str] | None = None


class KrakenAddOrderResponseSchema(BaseModel):
    """Response from add_order WebSocket request."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    method: Literal["add_order"]
    result: KrakenAddOrderResultSchema | None = None
    success: bool
    error: str | None = None
    time_in: str | None = Field(default=None, alias="time_in")
    time_out: str | None = Field(default=None, alias="time_out")
    request_id: int | None = Field(default=None, alias="reqid")


class KrakenCancelOrderParamsSchema(BaseModel):
    """Parameters for cancelling an order via WebSocket."""

    model_config = STRICT_SCHEMA_CONFIG
    order_id: list[str] | None = None
    cl_ord_id: list[str] | None = None
    token: str | None = None


class KrakenCancelOrderResultSchema(BaseModel):
    """Result from successful order cancellation."""

    model_config = EXCHANGE_SCHEMA_CONFIG
    order_id: str | None = None
    cl_ord_id: str | None = None
    warning: list[str] | None = None


class KrakenCancelOrderResponseSchema(BaseModel):
    """Response from cancel_order WebSocket request."""

    model_config = EXCHANGE_SCHEMA_CONFIG
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
