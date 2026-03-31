"""Kraken Futures data format adapter functions.

This module provides functions for parsing Kraken Futures WebSocket and
REST messages into internal Snapper data structures. It handles:

- Ticker updates (bid/ask/last/mark price, funding rate, open interest)
- Trade execution events
- Instrument/product specifications

All parsing functions validate data using Pydantic schemas before
converting to internal contract types. Symbol conversion from Kraken
Futures format to native format is handled automatically.

Each ``_list`` function catches per-item errors so that a single
unparseable item does not discard the entire batch.
"""

from datetime import UTC
from datetime import datetime
from typing import Any

from loguru import logger

from snapper.infrastructure.exchanges.contracts import InstrumentPairDescriptor
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.schemas.kraken_futures import KrakenFuturesInstrumentSchema
from snapper.infrastructure.exchanges.schemas.kraken_futures import KrakenFuturesTickerSchema
from snapper.infrastructure.exchanges.schemas.kraken_futures import KrakenFuturesTradeSchema
from snapper.infrastructure.symbols.functions import kraken_futures_ws_to_native


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
        bid=schema.bid or 0.0,
        bid_qty=schema.bid_size or 0.0,
        ask=schema.ask or 0.0,
        ask_qty=schema.ask_size or 0.0,
        last=schema.last or 0.0,
        volume=schema.vol24h or 0.0,
        vwap=0.0,
        low=schema.low24h or 0.0,
        high=schema.high24h or 0.0,
        change=schema.change24h or 0.0,
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
        ts = datetime.fromisoformat(str(schema.time).replace("Z", "+00:00"))
    product_id = data.get("product_id", data.get("symbol", ""))
    return TradeUpdate(
        symbol=kraken_futures_ws_to_native(product_id) if product_id else "",
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

    Args:
        tick_size: Minimum price increment (e.g., 0.5, 0.05, 1.0).

    Returns:
        Number of decimal places (e.g., 0.5 -> 1, 0.05 -> 2, 1.0 -> 0).
    """
    s = f"{tick_size:.10f}".rstrip("0")
    return len(s.split(".")[1]) if "." in s else 0
