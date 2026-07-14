"""Kraken data format adapter functions.

This module provides functions for parsing Kraken WebSocket messages
into internal Snapper data structures. It handles:

- Ticker updates (bid/ask/last price, volume)
- OHLC candle updates
- Trade execution events
- User execution reports
- Instrument/pair specifications

All parsing functions validate data using Pydantic schemas before
converting to internal contract types. Symbol conversion from Kraken
WebSocket format to native format is handled automatically.

Each ``_list`` function catches per-item errors so that a single
unparseable item (e.g. an unmapped symbol) does not discard the
entire batch.
"""

from datetime import datetime
from time import monotonic
from typing import Any
from typing import cast

from loguru import logger

from snapper.core.types import TradeSideEnum
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import ExecType
from snapper.infrastructure.exchanges.contracts import ExecutionFeeBreakdown
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import InstrumentPairDescriptor
from snapper.infrastructure.exchanges.contracts import LiquidityIndicator
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TimeInForceEnum
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.schemas.kraken import KrakenCandleSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenExecutionSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenInstrumentPairSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenTickerSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenTradeSchema
from snapper.infrastructure.symbols.functions import kraken_websocket_to_native

_UTC_SUFFIX = "+00:00"


def parse_kraken_ticker(data: dict[str, Any]) -> TickerUpdate:
    """Parse a Kraken ticker WebSocket message into TickerUpdate.

    Args:
        data: Raw ticker data dictionary from Kraken WebSocket.

    Returns:
        TickerUpdate with normalized symbol and price data.
    """
    schema = KrakenTickerSchema.model_validate(data)
    return TickerUpdate(
        symbol=kraken_websocket_to_native(schema.symbol),
        bid=schema.bid or 0.0,
        bid_qty=schema.bid_qty or 0.0,
        ask=schema.ask or 0.0,
        ask_qty=schema.ask_qty or 0.0,
        last=schema.last or 0.0,
        volume=schema.volume or 0.0,
        vwap=schema.vwap or 0.0,
        low=schema.low or 0.0,
        high=schema.high or 0.0,
        change=schema.change or 0.0,
        change_pct=schema.change_pct or 0.0,
    )


_UNPARSEABLE_LOG_INTERVAL_S = 300.0
_unparseable_log_state: dict[str, float] = {}


def parse_kraken_ticker_list(data: list[dict[str, Any]]) -> list[TickerUpdate]:
    """Parse a list of Kraken ticker messages.

    Skips individual items that fail to parse (e.g. unmapped symbols)
    so that one bad item does not discard the entire batch.

    Logs are rate-limited per-symbol to one warning every
    ``_UNPARSEABLE_LOG_INTERVAL_S`` seconds — the prior per-occurrence
    warning emitted thousands of identical lines per minute when Kraken
    streamed perpetual-suffixed symbols (e.g. ``BTC/USD:BTNL``) that are
    not in the alias table. Operators still see the first occurrence;
    the spam stops dominating the log.

    Args:
        data: List of raw ticker data dictionaries.

    Returns:
        List of successfully parsed TickerUpdate objects.
    """
    results: list[TickerUpdate] = []
    for item in data:
        try:
            results.append(parse_kraken_ticker(item))
        except ValueError as exc:
            symbol = item.get("symbol", "?") if isinstance(item, dict) else "?"
            now = monotonic()
            last = _unparseable_log_state.get(symbol, 0.0)
            if now - last >= _UNPARSEABLE_LOG_INTERVAL_S:
                logger.warning(f"Skipping unparseable ticker (symbol={symbol}): {exc}")
                _unparseable_log_state[symbol] = now
    return results


def parse_kraken_candle(data: dict[str, Any]) -> CandleUpdate:
    """Parse a Kraken OHLC candle WebSocket message into CandleUpdate.

    Args:
        data: Raw candle data dictionary from Kraken WebSocket.

    Returns:
        CandleUpdate with OHLCV data and interval information.

    Raises:
        ValueError: If interval_begin timestamp is missing.
    """
    schema = KrakenCandleSchema.model_validate(data)
    if not schema.interval_begin:
        raise ValueError("Candle data missing required 'interval_begin' timestamp")
    interval_begin = datetime.fromisoformat(schema.interval_begin.replace("Z", _UTC_SUFFIX))
    return CandleUpdate(
        symbol=kraken_websocket_to_native(schema.symbol) if schema.symbol else "",
        open=float(schema.open),
        high=float(schema.high),
        low=float(schema.low),
        close=float(schema.close),
        vwap=float(schema.vwap) if schema.vwap else 0.0,
        trades=schema.trades or 0,
        volume=float(schema.volume) if schema.volume else 0.0,
        interval_begin=interval_begin,
        interval=schema.interval or 0,
    )


def parse_kraken_candle_list(data: list[dict[str, Any]]) -> list[CandleUpdate]:
    """Parse a list of Kraken candle messages.

    Skips individual items that fail to parse so that one bad item
    does not discard the entire batch.

    Args:
        data: List of raw candle data dictionaries.

    Returns:
        List of successfully parsed CandleUpdate objects.
    """
    results: list[CandleUpdate] = []
    for item in data:
        try:
            results.append(parse_kraken_candle(item))
        except ValueError as exc:
            symbol = item.get("symbol", "?") if isinstance(item, dict) else "?"
            logger.warning(f"Skipping unparseable candle (symbol={symbol}): {exc}")
    return results


def parse_kraken_trade(data: dict[str, Any]) -> TradeUpdate:
    """Parse a Kraken trade WebSocket message into TradeUpdate.

    Args:
        data: Raw trade data dictionary from Kraken WebSocket.

    Returns:
        TradeUpdate with trade execution details.
    """
    schema = KrakenTradeSchema.model_validate(data)
    timestamp = datetime.fromisoformat(schema.timestamp.replace("Z", _UTC_SUFFIX))
    return TradeUpdate(
        symbol=kraken_websocket_to_native(schema.symbol),
        side=schema.side,
        quantity=float(schema.qty),
        price=float(schema.price),
        ord_type=schema.ord_type or "unknown",
        trade_id=str(schema.trade_id) if schema.trade_id is not None else None,
        timestamp=timestamp,
    )


def parse_kraken_trade_list(data: list[dict[str, Any]]) -> list[TradeUpdate]:
    """Parse a list of Kraken trade messages.

    Skips individual items that fail to parse so that one bad item
    does not discard the entire batch.

    Args:
        data: List of raw trade data dictionaries.

    Returns:
        List of successfully parsed TradeUpdate objects.
    """
    results: list[TradeUpdate] = []
    for item in data:
        try:
            results.append(parse_kraken_trade(item))
        except ValueError as exc:
            symbol = item.get("symbol", "?") if isinstance(item, dict) else "?"
            logger.warning(f"Skipping unparseable trade (symbol={symbol}): {exc}")
    return results


def _parse_order_side(side: str | None) -> OrderSideEnum:
    """Convert Kraken order side string to OrderSideEnum.

    Args:
        side: Order side string ("buy" or "sell").

    Returns:
        Corresponding OrderSideEnum value.

    Raises:
        ValueError: If side is unknown.
    """
    if side == TradeSideEnum.BUY:
        return OrderSideEnum.BUY
    if side == TradeSideEnum.SELL:
        return OrderSideEnum.SELL
    raise ValueError(f"Unknown order side: {side}")


def _parse_order_type(order_type: str | None) -> ExchangeOrderTypeEnum:
    """Convert Kraken order type string to ExchangeOrderTypeEnum.

    Args:
        order_type: Order type string (e.g., "limit", "market").

    Returns:
        Corresponding ExchangeOrderTypeEnum value.

    Raises:
        ValueError: If order type is unknown.
    """
    mapping = {
        "limit": ExchangeOrderTypeEnum.LIMIT,
        "market": ExchangeOrderTypeEnum.MARKET,
        "iceberg": ExchangeOrderTypeEnum.ICEBERG,
        "stop-loss": ExchangeOrderTypeEnum.STOP_LOSS,
        "stop-loss-limit": ExchangeOrderTypeEnum.STOP_LOSS_LIMIT,
        "take-profit": ExchangeOrderTypeEnum.TAKE_PROFIT,
        "take-profit-limit": ExchangeOrderTypeEnum.TAKE_PROFIT_LIMIT,
        "trailing-stop": ExchangeOrderTypeEnum.TRAILING_STOP,
        "trailing-stop-limit": ExchangeOrderTypeEnum.TRAILING_STOP_LIMIT,
        "settle-position": ExchangeOrderTypeEnum.SETTLE_POSITION,
    }
    if order_type and order_type in mapping:
        return mapping[order_type]
    raise ValueError(f"Unknown order type: {order_type}")


def _parse_order_status(status: str | None) -> ExchangeOrderStatusEnum:
    """Convert Kraken order status string to ExchangeOrderStatusEnum.

    Args:
        status: Order status string (e.g., "open", "closed").

    Returns:
        Corresponding ExchangeOrderStatusEnum value.

    Raises:
        ValueError: If status is unknown.
    """
    mapping = {
        "pending": ExchangeOrderStatusEnum.PENDING,
        "open": ExchangeOrderStatusEnum.OPEN,
        "closed": ExchangeOrderStatusEnum.CLOSED,
        "pending_new": ExchangeOrderStatusEnum.PENDING_NEW,
        "new": ExchangeOrderStatusEnum.NEW,
        "partially_filled": ExchangeOrderStatusEnum.PARTIALLY_FILLED,
        "filled": ExchangeOrderStatusEnum.FILLED,
        "canceled": ExchangeOrderStatusEnum.CANCELED,
        "expired": ExchangeOrderStatusEnum.EXPIRED,
    }
    if status and status in mapping:
        return mapping[status]
    raise ValueError(f"Unknown order status: {status}")


def _parse_time_in_force(tif: str | None) -> TimeInForceEnum | None:
    """Convert Kraken time-in-force string to TimeInForceEnum.

    Args:
        tif: Time-in-force string (e.g., "GTC", "IOC").

    Returns:
        Corresponding TimeInForceEnum value, or None if not provided.
    """
    if not tif:
        return None
    mapping = {
        "GTC": TimeInForceEnum.GTC,
        "GTD": TimeInForceEnum.GTD,
        "IOC": TimeInForceEnum.IOC,
    }
    return mapping.get(tif)


def _optional_float(value: Any) -> float | None:
    """Convert a value to float if truthy, otherwise return None.

    Args:
        value: Numeric value or None.

    Returns:
        Float conversion of the value, or None if falsy.
    """
    return float(value) if value else None


def _parse_execution_fees(
    schema: KrakenExecutionSchema, raw_fees: object
) -> list[ExecutionFeeBreakdown] | None:
    """Parse fee breakdown from Kraken execution schema.

    Args:
        schema: Validated Kraken execution schema.
        raw_fees: Original venue fee rows used to retain decimal strings.

    Returns:
        List of fee breakdowns, or None if no fees present.
    """
    if not schema.fees:
        return None
    raw_rows = raw_fees if isinstance(raw_fees, list) else []
    parsed: list[ExecutionFeeBreakdown] = []
    for index, fee in enumerate(schema.fees):
        raw_row = raw_rows[index] if index < len(raw_rows) else None
        raw_quantity = raw_row.get("qty") if isinstance(raw_row, dict) else None
        parsed.append(
            ExecutionFeeBreakdown(
                asset=fee.asset or "",
                quantity=float(fee.qty) if fee.qty else 0.0,
                quantity_decimal=raw_quantity if isinstance(raw_quantity, str) else None,
            )
        )
    return parsed


def parse_kraken_execution(data: dict[str, Any]) -> ExecutionUpdate:
    """Parse a Kraken execution WebSocket message into ExecutionUpdate.

    This handles user's order execution reports including fills, partial fills,
    and status changes. Converts all Kraken-specific formats to internal types.

    Args:
        data: Raw execution data dictionary from Kraken WebSocket.

    Returns:
        ExecutionUpdate with order execution details.

    Raises:
        ValueError: If required timestamp field is missing.
    """
    validation_data = data.copy()
    for field in ("last_qty", "last_price", "fee_usd_equiv"):
        raw_value = validation_data.get(field)
        if isinstance(raw_value, str):
            validation_data[field] = float(raw_value)
    raw_fees = validation_data.get("fees")
    if isinstance(raw_fees, list):
        validation_fees: list[object] = []
        for raw_fee in raw_fees:
            if isinstance(raw_fee, dict):
                validation_fee = raw_fee.copy()
                raw_quantity = validation_fee.get("qty")
                if isinstance(raw_quantity, str):
                    validation_fee["qty"] = float(raw_quantity)
                validation_fees.append(validation_fee)
            else:
                validation_fees.append(raw_fee)
        validation_data["fees"] = validation_fees
    schema = KrakenExecutionSchema.model_validate(validation_data)
    if not schema.timestamp:
        raise ValueError("Execution data missing required 'timestamp' field")
    timestamp = datetime.fromisoformat(schema.timestamp.replace("Z", _UTC_SUFFIX))
    return ExecutionUpdate(
        order_id=schema.order_id or "",
        exec_type=cast(ExecType, schema.exec_type) if schema.exec_type else None,
        symbol=kraken_websocket_to_native(schema.symbol) if schema.symbol else "",
        side=_parse_order_side(schema.side),
        order_type=_parse_order_type(schema.order_type),
        order_status=_parse_order_status(schema.order_status),
        timestamp=timestamp,
        cum_qty=_optional_float(schema.cum_qty),
        cum_cost=_optional_float(schema.cum_cost),
        order_userref=schema.order_userref,
        exec_id=schema.exec_id,
        trade_id=schema.trade_id,
        last_qty=_optional_float(schema.last_qty),
        last_price=_optional_float(schema.last_price),
        liquidity_ind=(
            cast(LiquidityIndicator, schema.liquidity_ind) if schema.liquidity_ind else None
        ),
        cost=_optional_float(schema.cost),
        average_price=_optional_float(schema.avg_price),
        fee_usd_equiv=_optional_float(schema.fee_usd_equiv),
        fees=_parse_execution_fees(schema, data.get("fees")),
        order_qty=_optional_float(schema.order_qty),
        limit_price=_optional_float(schema.limit_price),
        cash_order_qty=_optional_float(schema.cash_order_qty),
        margin=schema.margin,
        margin_borrow=schema.margin_borrow,
        post_only=schema.post_only,
        reduce_only=schema.reduce_only,
        time_in_force=_parse_time_in_force(schema.time_in_force),
        last_qty_decimal=(data["last_qty"] if isinstance(data.get("last_qty"), str) else None),
        last_price_decimal=(
            data["last_price"] if isinstance(data.get("last_price"), str) else None
        ),
        fee_usd_equiv_decimal=(
            data["fee_usd_equiv"] if isinstance(data.get("fee_usd_equiv"), str) else None
        ),
        reason=schema.reason,
    )


def parse_kraken_execution_list(data: list[dict[str, Any]]) -> list[ExecutionUpdate]:
    """Parse a list of Kraken execution messages.

    Skips individual items that fail to parse so that one bad item
    does not discard the entire batch.

    Args:
        data: List of raw execution data dictionaries.

    Returns:
        List of successfully parsed ExecutionUpdate objects.
    """
    results: list[ExecutionUpdate] = []
    for item in data:
        try:
            results.append(parse_kraken_execution(item))
        except ValueError as exc:
            logger.warning(f"Skipping unparseable execution: {exc}")
    return results


def parse_kraken_instrument(data: dict[str, Any]) -> InstrumentPairDescriptor:
    """Parse Kraken instrument/pair info into InstrumentPairDescriptor.

    Converts Kraken's trading pair specification including precision,
    minimum quantities, and margin settings to internal format.

    Args:
        data: Raw instrument data dictionary from Kraken.

    Returns:
        InstrumentPairDescriptor with pair specifications.
    """
    schema = KrakenInstrumentPairSchema.model_validate(data)
    return InstrumentPairDescriptor(
        symbol=kraken_websocket_to_native(schema.symbol),
        base=schema.base_asset or "",
        quote=schema.quote_asset or "",
        status=schema.status or "unknown",
        qty_precision=schema.qty_precision or 0,
        qty_increment=schema.qty_increment or 0.0,
        qty_min=schema.qty_min or 0.0,
        price_precision=schema.price_precision or 0,
        price_increment=schema.price_increment or 0.0,
        cost_precision=schema.cost_precision or 0,
        cost_min=schema.cost_min or 0.0,
        marginable=schema.marginable or False,
        has_index=schema.has_index or False,
        margin_initial=schema.margin_initial,
        position_limit_long=int(schema.position_limit_long) if schema.position_limit_long else None,
        position_limit_short=(
            int(schema.position_limit_short) if schema.position_limit_short else None
        ),
        tick_size=schema.tick_size,
    )


def parse_kraken_instrument_list(data: list[dict[str, Any]]) -> list[InstrumentPairDescriptor]:
    """Parse a list of Kraken instrument specifications.

    Skips individual items that fail to parse so that one bad item
    does not discard the entire batch.

    Args:
        data: List of raw instrument data dictionaries.

    Returns:
        List of successfully parsed InstrumentPairDescriptor objects.
    """
    results: list[InstrumentPairDescriptor] = []
    for item in data:
        try:
            results.append(parse_kraken_instrument(item))
        except ValueError as exc:
            symbol = item.get("symbol", "?") if isinstance(item, dict) else "?"
            logger.warning(f"Skipping unparseable instrument (symbol={symbol}): {exc}")
    return results


__all__ = [
    "parse_kraken_ticker",
    "parse_kraken_ticker_list",
    "parse_kraken_candle",
    "parse_kraken_candle_list",
    "parse_kraken_trade",
    "parse_kraken_trade_list",
    "parse_kraken_execution",
    "parse_kraken_execution_list",
    "parse_kraken_instrument",
    "parse_kraken_instrument_list",
]
