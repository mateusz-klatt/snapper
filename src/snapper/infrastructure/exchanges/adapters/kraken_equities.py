"""Kraken Equities (FCM Futures) data format adapter functions.

This module provides functions for parsing Kraken Equities WebSocket and
REST messages into internal Snapper data structures. It handles:

- Ticker updates (bid/ask/last/volume, open interest, change)
- Trade execution events
- Instrument/contract specifications

The WS protocol matches Kraken Spot WS v2 with ``asset_class`` field.
Symbol conversion from exchange format (``CLM6.NYMEX``) to native
format (``CLM6-NYMEX``) is handled by the symbol functions module.

Each ``_list`` function catches per-item errors so that a single
unparseable item does not discard the entire batch.
"""

from datetime import datetime
from decimal import Decimal
from typing import Any

from loguru import logger

from snapper.infrastructure.exchanges.contracts import InstrumentPairDescriptor
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.schemas.kraken_equities import KrakenEquitiesInstrumentSchema
from snapper.infrastructure.exchanges.schemas.kraken_equities import KrakenEquitiesTickerSchema
from snapper.infrastructure.exchanges.schemas.kraken_equities import KrakenEquitiesTradeSchema
from snapper.infrastructure.symbols.functions import kraken_equities_ws_to_native


def parse_kraken_equities_ticker(
    data: dict[str, Any],
    *,
    envelope_delayed: bool = False,
) -> TickerUpdate:
    """Parse a Kraken Equities ticker message into TickerUpdate.

    Args:
        data: Raw ticker data dictionary from WS ``ticker`` channel.
        envelope_delayed: Value of the outer WS envelope's ``delayed`` flag.
            Kraken FCM publishes this once per frame at the envelope level
            rather than per item; the caller extracts it and passes it in so
            that every TickerUpdate carries the correct ``is_delayed`` value.

    Returns:
        TickerUpdate with normalized symbol, price data, and delay/session
        flags ready for downstream ZMQ propagation.
    """
    schema = KrakenEquitiesTickerSchema.model_validate(data)
    return TickerUpdate(
        symbol=kraken_equities_ws_to_native(schema.symbol),
        bid=schema.bid if schema.bid is not None else 0.0,
        bid_qty=schema.bid_qty if schema.bid_qty is not None else 0.0,
        ask=schema.ask if schema.ask is not None else 0.0,
        ask_qty=schema.ask_qty if schema.ask_qty is not None else 0.0,
        last=schema.last if schema.last is not None else 0.0,
        volume=schema.volume if schema.volume is not None else 0.0,
        vwap=schema.vwap if schema.vwap is not None else 0.0,
        low=schema.low if schema.low is not None else 0.0,
        high=schema.high if schema.high is not None else 0.0,
        change=schema.change if schema.change is not None else 0.0,
        change_pct=schema.change_pct if schema.change_pct is not None else 0.0,
        is_delayed=envelope_delayed,
        is_extended_hours=schema.is_extended_hours,
    )


def parse_kraken_equities_ticker_list(
    data: list[dict[str, Any]],
    *,
    envelope_delayed: bool = False,
) -> list[TickerUpdate]:
    """Parse a list of Kraken Equities ticker messages.

    Args:
        data: List of raw ticker data dictionaries.
        envelope_delayed: Outer-envelope ``delayed`` flag applied to every
            successfully-parsed item.

    Returns:
        List of successfully parsed TickerUpdate objects.
    """
    results: list[TickerUpdate] = []
    for item in data:
        try:
            results.append(parse_kraken_equities_ticker(item, envelope_delayed=envelope_delayed))
        except (ValueError, KeyError) as exc:
            symbol = item.get("symbol", "?") if isinstance(item, dict) else "?"
            logger.warning(f"Skipping unparseable equities ticker (symbol={symbol}): {exc}")
    return results


def parse_kraken_equities_trade(data: dict[str, Any]) -> TradeUpdate:
    """Parse a Kraken Equities trade message into TradeUpdate.

    Args:
        data: Raw trade data dictionary from WS ``trade`` channel.

    Returns:
        TradeUpdate with normalized symbol and trade data.
    """
    schema = KrakenEquitiesTradeSchema.model_validate(data)
    ts = datetime.fromisoformat(schema.timestamp.replace("Z", "+00:00"))
    side = schema.side if schema.side in ("buy", "sell") else "buy"
    return TradeUpdate(
        symbol=kraken_equities_ws_to_native(schema.symbol),
        side=side,
        quantity=schema.qty,
        price=schema.price,
        ord_type="fill",
        timestamp=ts,
        trade_id=str(schema.index),
    )


def parse_kraken_equities_trade_list(data: list[dict[str, Any]]) -> list[TradeUpdate]:
    """Parse a list of Kraken Equities trade messages.

    Args:
        data: List of raw trade data dictionaries.

    Returns:
        List of successfully parsed TradeUpdate objects.
    """
    results: list[TradeUpdate] = []
    for item in data:
        try:
            results.append(parse_kraken_equities_trade(item))
        except (ValueError, KeyError) as exc:
            symbol = item.get("symbol", "?") if isinstance(item, dict) else "?"
            logger.warning(f"Skipping unparseable equities trade (symbol={symbol}): {exc}")
    return results


def parse_kraken_equities_instrument(data: dict[str, Any]) -> InstrumentPairDescriptor:
    """Parse a Kraken Equities instrument into InstrumentPairDescriptor.

    Args:
        data: Raw instrument data dictionary from internal REST API.

    Returns:
        InstrumentPairDescriptor with contract specifications.
    """
    schema = KrakenEquitiesInstrumentSchema.model_validate(data)
    tick_size = float(schema.tick_size) if schema.tick_size else 0.01
    contract_size = float(schema.contract_size) if schema.contract_size else 1.0
    initial_margin = float(schema.initial_margin) if schema.initial_margin else None
    return InstrumentPairDescriptor(
        symbol=schema.symbol,
        base=schema.base,
        quote=schema.quote,
        status="active" if schema.tradable and schema.status == "active" else "inactive",
        qty_precision=0,
        qty_increment=contract_size,
        qty_min=contract_size,
        price_precision=_tick_size_to_precision(tick_size),
        price_increment=tick_size,
        cost_precision=2,
        cost_min=0.0,
        marginable=True,
        has_index=False,
        margin_initial=initial_margin,
        tick_size=tick_size,
    )


def parse_kraken_equities_instrument_list(
    data: list[dict[str, Any]],
) -> list[InstrumentPairDescriptor]:
    """Parse a list of Kraken Equities instruments.

    Args:
        data: List of raw instrument data dictionaries.

    Returns:
        List of successfully parsed InstrumentPairDescriptor objects.
    """
    results: list[InstrumentPairDescriptor] = []
    for item in data:
        try:
            results.append(parse_kraken_equities_instrument(item))
        except (ValueError, KeyError) as exc:
            symbol = item.get("symbol", "?") if isinstance(item, dict) else "?"
            logger.warning(f"Skipping unparseable equities instrument (symbol={symbol}): {exc}")
    return results


def _tick_size_to_precision(tick_size: float) -> int:
    """Convert tick size to decimal precision.

    Args:
        tick_size: Minimum price increment (e.g., 0.01, 0.25, 1.0).

    Returns:
        Number of decimal places (e.g., 0.01 -> 2, 0.25 -> 2, 1.0 -> 0).
    """
    d = Decimal(str(tick_size)).normalize()
    exp = d.as_tuple().exponent
    return max(0, -int(exp))
