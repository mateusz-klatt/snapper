"""Kraken Futures data format adapter functions.

This module provides functions for parsing Kraken Futures WebSocket and
REST messages into internal Snapper data structures. It handles:

- Ticker updates (bid/ask/last/mark price, funding rate, open interest)
- Trade execution events
- Instrument/product specifications
- Private fill events (authenticated ``fills`` WS channel)
- Private order status updates (authenticated ``open_orders`` WS channel)

All parsing functions validate data using Pydantic schemas before
converting to internal contract types. Symbol conversion from Kraken
Futures format to native format is handled automatically.

Each ``_list`` function catches per-item errors so that a single
unparseable item does not discard the entire batch.
"""

from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from typing import Any

from loguru import logger

from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import ExecType
from snapper.infrastructure.exchanges.contracts import ExecutionFeeBreakdown
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import InstrumentPairDescriptor
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.schemas.kraken_futures import KrakenFuturesFillSchema
from snapper.infrastructure.exchanges.schemas.kraken_futures import KrakenFuturesInstrumentSchema
from snapper.infrastructure.exchanges.schemas.kraken_futures import KrakenFuturesOpenOrderSchema
from snapper.infrastructure.exchanges.schemas.kraken_futures import KrakenFuturesTickerSchema
from snapper.infrastructure.exchanges.schemas.kraken_futures import KrakenFuturesTradeSchema
from snapper.infrastructure.symbols.functions import kraken_futures_ws_to_native

_UTC_SUFFIX = "+00:00"


def parse_kraken_futures_ticker(data: dict[str, Any]) -> TickerUpdate:
    """Parse a Kraken Futures ticker message into TickerUpdate.

    Args:
        data: Raw ticker data dictionary from Kraken Futures REST or WS.

    Returns:
        TickerUpdate with normalized symbol and price data.
    """
    schema = KrakenFuturesTickerSchema.model_validate(data)
    return TickerUpdate(
        symbol=kraken_futures_ws_to_native(schema.symbol),
        bid=schema.bid if schema.bid is not None else 0.0,
        bid_qty=schema.bid_size if schema.bid_size is not None else 0.0,
        ask=schema.ask if schema.ask is not None else 0.0,
        ask_qty=schema.ask_size if schema.ask_size is not None else 0.0,
        last=schema.last if schema.last is not None else 0.0,
        volume=schema.vol24h if schema.vol24h is not None else 0.0,
        vwap=0.0,
        low=schema.low24h if schema.low24h is not None else 0.0,
        high=schema.high24h if schema.high24h is not None else 0.0,
        change=schema.change24h if schema.change24h is not None else 0.0,
        change_pct=0.0,
    )


def parse_kraken_futures_ticker_list(data: list[dict[str, Any]]) -> list[TickerUpdate]:
    """Parse a list of Kraken Futures ticker messages.

    Skips individual items that fail to parse so that one bad item
    does not discard the entire batch.

    Args:
        data: List of raw ticker data dictionaries.

    Returns:
        List of successfully parsed TickerUpdate objects.
    """
    results: list[TickerUpdate] = []
    for item in data:
        try:
            results.append(parse_kraken_futures_ticker(item))
        except (ValueError, KeyError) as exc:
            symbol = item.get("symbol", "?") if isinstance(item, dict) else "?"
            logger.warning(f"Skipping unparseable futures ticker (symbol={symbol}): {exc}")
    return results


def parse_kraken_futures_trade(data: dict[str, Any]) -> TradeUpdate:
    """Parse a Kraken Futures trade message into TradeUpdate.

    Accepts both REST format (ISO 8601 time string) and WS format
    (integer millisecond timestamp).

    Args:
        data: Raw trade data dictionary from Kraken Futures REST or WS.

    Returns:
        TradeUpdate with normalized symbol and trade data.
    """
    schema = KrakenFuturesTradeSchema.model_validate(data)
    if isinstance(schema.time, int):
        ts = datetime.fromtimestamp(schema.time / 1000, tz=UTC)
    else:
        ts = datetime.fromisoformat(str(schema.time).replace("Z", _UTC_SUFFIX))
    product_id = data.get("product_id") or data.get("symbol") or ""
    if not product_id:
        raise ValueError("Trade missing both product_id and symbol")
    return TradeUpdate(
        symbol=kraken_futures_ws_to_native(product_id),
        side=schema.side,
        quantity=schema.size,
        price=schema.price,
        ord_type="fill",
        timestamp=ts,
        trade_id=schema.uid,
    )


def parse_kraken_futures_trade_list(data: list[dict[str, Any]]) -> list[TradeUpdate]:
    """Parse a list of Kraken Futures trade messages.

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
            results.append(parse_kraken_futures_trade(item))
        except (ValueError, KeyError) as exc:
            uid = item.get("uid", "?") if isinstance(item, dict) else "?"
            logger.warning(f"Skipping unparseable futures trade (uid={uid}): {exc}")
    return results


def parse_kraken_futures_instrument(data: dict[str, Any]) -> InstrumentPairDescriptor:
    """Parse a Kraken Futures instrument into InstrumentPairDescriptor.

    Args:
        data: Raw instrument data dictionary from Kraken Futures REST API.

    Returns:
        InstrumentPairDescriptor with product specifications.
    """
    schema = KrakenFuturesInstrumentSchema.model_validate(data)
    initial_margin = schema.margin_levels[0].initial_margin if schema.margin_levels else None
    return InstrumentPairDescriptor(
        symbol=schema.symbol,
        base=schema.base or "",
        quote=schema.quote or "",
        status="active" if schema.tradeable else "inactive",
        qty_precision=schema.contract_value_trade_precision or 0,
        qty_increment=schema.contract_size,
        qty_min=schema.contract_size,
        price_precision=_tick_size_to_precision(schema.tick_size),
        price_increment=schema.tick_size,
        cost_precision=2,
        cost_min=0.0,
        marginable=bool(schema.margin_levels),
        has_index=schema.underlying is not None,
        margin_initial=initial_margin,
        tick_size=schema.tick_size,
    )


def parse_kraken_futures_instrument_list(
    data: list[dict[str, Any]],
) -> list[InstrumentPairDescriptor]:
    """Parse a list of Kraken Futures instruments.

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
            results.append(parse_kraken_futures_instrument(item))
        except (ValueError, KeyError) as exc:
            symbol = item.get("symbol", "?") if isinstance(item, dict) else "?"
            logger.warning(f"Skipping unparseable futures instrument (symbol={symbol}): {exc}")
    return results


def _tick_size_to_precision(tick_size: float) -> int:
    """Convert tick size to decimal precision.

    Uses Decimal for exact representation to avoid float formatting
    artifacts (e.g., 0.1 → 0.10000000000000001).

    Args:
        tick_size: Minimum price increment (e.g., 0.5, 0.05, 1.0).

    Returns:
        Number of decimal places (e.g., 0.5 -> 1, 0.05 -> 2, 1.0 -> 0).
    """
    d = Decimal(str(tick_size)).normalize()
    exp = d.as_tuple().exponent
    return max(0, -int(exp))


_SIDE_MAP: dict[str, OrderSideEnum] = {"buy": OrderSideEnum.BUY, "sell": OrderSideEnum.SELL}
_STATUS_MAP: dict[str, ExchangeOrderStatusEnum] = {
    "placed": ExchangeOrderStatusEnum.OPEN,
    "partiallyFilled": ExchangeOrderStatusEnum.OPEN,
    "filled": ExchangeOrderStatusEnum.CLOSED,
    "cancelled": ExchangeOrderStatusEnum.CANCELED,
    "canceled": ExchangeOrderStatusEnum.CANCELED,
    "untouched": ExchangeOrderStatusEnum.OPEN,
    "ENTERED_BOOK": ExchangeOrderStatusEnum.OPEN,
    "FULLY_EXECUTED": ExchangeOrderStatusEnum.CLOSED,
}
_ORDER_TYPE_MAP: dict[str, ExchangeOrderTypeEnum] = {
    "lmt": ExchangeOrderTypeEnum.LIMIT,
    "post": ExchangeOrderTypeEnum.LIMIT,
    "ioc": ExchangeOrderTypeEnum.LIMIT,
    "mkt": ExchangeOrderTypeEnum.MARKET,
    "stp": ExchangeOrderTypeEnum.STOP_LOSS,
    "take_profit": ExchangeOrderTypeEnum.TAKE_PROFIT,
    "trailing_stop": ExchangeOrderTypeEnum.TRAILING_STOP,
}


_DIRECTION_MAP: dict[int, OrderSideEnum] = {0: OrderSideEnum.BUY, 1: OrderSideEnum.SELL}
_WS_ORDER_TYPE_MAP: dict[str, ExchangeOrderTypeEnum] = {
    "limit": ExchangeOrderTypeEnum.LIMIT,
    "stop": ExchangeOrderTypeEnum.STOP_LOSS,
    "take_profit": ExchangeOrderTypeEnum.TAKE_PROFIT,
}


def parse_kraken_futures_fill(
    data: dict[str, Any],
    symbol_mapper: Callable[[str], str],
) -> ExecutionUpdate:
    """Parse a Kraken Futures WS fill event to ExecutionUpdate.

    The WS payload uses ``instrument`` (not ``symbol``), ``buy: bool``
    (not ``side``), ``qty`` (not ``size``), and ``time: int`` (ms epoch).

    Args:
        data: Raw fill data dictionary from the ``fills``/``fills_snapshot`` WS channel.
        symbol_mapper: Function to convert Kraken instrument to native format.

    Returns:
        ExecutionUpdate with fill details.
    """
    schema = KrakenFuturesFillSchema.model_validate(data)
    native_symbol = symbol_mapper(schema.instrument)
    ts = datetime.fromtimestamp(schema.time / 1000, tz=UTC)
    side = OrderSideEnum.BUY if schema.buy else OrderSideEnum.SELL
    order_type = _ORDER_TYPE_MAP.get(schema.order_type or "", ExchangeOrderTypeEnum.LIMIT)
    fee_currency = schema.fee_currency or "USD"
    fee_usd: float | None = schema.fee_paid if fee_currency == "USD" else None
    fees: list[ExecutionFeeBreakdown] | None = None
    if schema.fee_paid and fee_currency != "USD":
        fees = [ExecutionFeeBreakdown(asset=fee_currency, quantity=schema.fee_paid)]
    return ExecutionUpdate(
        order_id=schema.order_id,
        exec_type="trade",
        symbol=native_symbol,
        side=side,
        order_type=order_type,
        order_status=ExchangeOrderStatusEnum.OPEN,
        timestamp=ts,
        last_qty=schema.qty,
        last_price=schema.price,
        exec_id=schema.fill_id,
        cl_ord_id=schema.cli_ord_id,
        fee_usd_equiv=fee_usd,
        fees=fees,
    )


def parse_kraken_futures_order_status(
    data: dict[str, Any],
    symbol_mapper: Callable[[str], str],
) -> ExecutionUpdate:
    """Parse a Kraken Futures WS open_orders event to ExecutionUpdate.

    The WS payload uses ``instrument`` (not ``symbol``), ``direction: int``
    (0=buy, 1=sell), ``type`` (not ``orderType``), and ``time: int`` (ms epoch).

    Args:
        data: Raw order data from ``open_orders``/``open_orders_snapshot`` WS channel.
        symbol_mapper: Function to convert Kraken instrument to native format.

    Returns:
        ExecutionUpdate with order status details.
    """
    schema = KrakenFuturesOpenOrderSchema.model_validate(data)
    native_symbol = symbol_mapper(schema.instrument)
    ts_ms = schema.last_update_time if schema.last_update_time is not None else schema.time
    ts = datetime.fromtimestamp(ts_ms / 1000, tz=UTC)
    is_fully_filled = schema.filled >= schema.qty and schema.qty > 0
    status = ExchangeOrderStatusEnum.CLOSED if is_fully_filled else ExchangeOrderStatusEnum.OPEN
    exec_type: ExecType = "status"
    return ExecutionUpdate(
        order_id=schema.order_id,
        exec_type=exec_type,
        symbol=native_symbol,
        side=_DIRECTION_MAP.get(schema.direction, OrderSideEnum.BUY),
        order_type=_WS_ORDER_TYPE_MAP.get(schema.type, ExchangeOrderTypeEnum.LIMIT),
        order_status=status,
        timestamp=ts,
        cum_qty=schema.filled,
        order_qty=schema.qty,
        limit_price=schema.limit_price if schema.limit_price else None,
        cl_ord_id=schema.cli_ord_id,
    )
