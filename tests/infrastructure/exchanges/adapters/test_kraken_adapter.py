"""Tests for Kraken exchange response adapter functions."""

from datetime import datetime
from typing import Any
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_candle
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_candle_list
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_execution
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_execution_list
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_instrument
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_instrument_list
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_ticker
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_ticker_list
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_trade
from snapper.infrastructure.exchanges.adapters.kraken import parse_kraken_trade_list
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import InstrumentPairDescriptor
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import OrderStatusEnum
from snapper.infrastructure.exchanges.contracts import OrderTypeEnum
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TimeInForceEnum
from snapper.infrastructure.exchanges.contracts import TradeUpdate


@pytest.fixture(autouse=True)
def mock_symbol_mapper() -> Any:
    """Provide a mock symbol mapper for Kraken adapter tests."""
    with patch(
        "snapper.infrastructure.exchanges.adapters.kraken.kraken_websocket_to_native"
    ) as mock:
        mock.side_effect = lambda s: s.replace("/", "-")
        yield mock


def test_parse_kraken_ticker_valid() -> None:
    """Parse valid Kraken ticker to TickerUpdate.

    Given: Raw ticker data with all required fields,
    When: parse_kraken_ticker is called,
    Then: Returns TickerUpdate with converted symbol.
    """
    raw_data: dict[str, Any] = {
        "symbol": "BTC/USD",
        "bid": 45000.0,
        "bid_qty": 1.5,
        "ask": 45100.0,
        "ask_qty": 2.0,
        "last": 45050.0,
        "volume": 1000.0,
        "vwap": 45025.0,
        "low": 44000.0,
        "high": 46000.0,
        "change": 500.0,
        "change_pct": 1.11,
    }
    result = parse_kraken_ticker(raw_data)
    assert isinstance(result, TickerUpdate)
    assert result.symbol == "BTC-USD"
    assert result.bid == pytest.approx(45000.0)
    assert result.ask == pytest.approx(45100.0)
    assert result.last == pytest.approx(45050.0)


def test_parse_kraken_ticker_list_valid() -> None:
    """Parse list of Kraken tickers.

    Given: List of raw ticker data for multiple symbols,
    When: parse_kraken_ticker_list is called,
    Then: Returns list of TickerUpdate objects.
    """
    raw_data: list[dict[str, Any]] = [
        {
            "symbol": "BTC/USD",
            "bid": 45000.0,
            "bid_qty": 1.5,
            "ask": 45100.0,
            "ask_qty": 2.0,
            "last": 45050.0,
            "volume": 1000.0,
            "vwap": 45025.0,
            "low": 44000.0,
            "high": 46000.0,
            "change": 500.0,
            "change_pct": 1.11,
        },
        {
            "symbol": "ETH/USD",
            "bid": 3000.0,
            "bid_qty": 10.0,
            "ask": 3010.0,
            "ask_qty": 15.0,
            "last": 3005.0,
            "volume": 5000.0,
            "vwap": 3002.0,
            "low": 2900.0,
            "high": 3100.0,
            "change": 50.0,
            "change_pct": 1.69,
        },
    ]
    result = parse_kraken_ticker_list(raw_data)
    assert len(result) == 2
    assert all(isinstance(item, TickerUpdate) for item in result)
    assert result[0].symbol == "BTC-USD"
    assert result[1].symbol == "ETH-USD"


def test_parse_kraken_ticker_list_empty() -> None:
    """Handle empty ticker list.

    Given: Empty list of ticker data,
    When: parse_kraken_ticker_list is called,
    Then: Returns empty list.
    """
    result = parse_kraken_ticker_list([])
    assert result == []


def test_parse_kraken_ticker_list_skips_bad_item(mock_symbol_mapper: Any) -> None:
    """List parser keeps valid tickers when one item fails.

    Given: Batch with one valid and one unmapped ticker symbol,
    When: parse_kraken_ticker_list is called,
    Then: Returns only the successfully parsed ticker.
    """

    def _mapper(symbol: str) -> str:
        if symbol == "UNKNOWN/USD":
            raise ValueError(f"Unknown: {symbol}")
        return symbol.replace("/", "-")

    mock_symbol_mapper.side_effect = _mapper
    raw_data: list[dict[str, Any]] = [
        {"symbol": "BTC/USD", "bid": 45000.0, "ask": 45100.0, "last": 45050.0},
        {"symbol": "UNKNOWN/USD", "bid": 1.0, "ask": 2.0, "last": 1.5},
    ]
    result = parse_kraken_ticker_list(raw_data)
    assert len(result) == 1
    assert result[0].symbol == "BTC-USD"


def test_parse_kraken_ticker_missing_symbol() -> None:
    """Reject ticker without required symbol field.

    Given: Ticker data missing symbol field,
    When: parse_kraken_ticker is called,
    Then: Raises ValidationError.
    """
    raw_data: dict[str, Any] = {
        "bid": 45000.0,
        "ask": 45100.0,
    }
    with pytest.raises(ValidationError):
        parse_kraken_ticker(raw_data)


def test_parse_kraken_candle_valid() -> None:
    """Parse valid Kraken candle to CandleUpdate.

    Given: Raw OHLC data with interval_begin timestamp,
    When: parse_kraken_candle is called,
    Then: Returns CandleUpdate with parsed datetime.
    """
    raw_data: dict[str, Any] = {
        "symbol": "BTC/USD",
        "open": 45000.0,
        "high": 46000.0,
        "low": 44000.0,
        "close": 45500.0,
        "vwap": 45250.0,
        "trades": 100,
        "volume": 500.0,
        "interval_begin": "2024-12-22T10:00:00.000000Z",
        "interval": 5,
    }
    result = parse_kraken_candle(raw_data)
    assert isinstance(result, CandleUpdate)
    assert result.symbol == "BTC-USD"
    assert result.open == pytest.approx(45000.0)
    assert result.high == pytest.approx(46000.0)
    assert result.low == pytest.approx(44000.0)
    assert result.close == pytest.approx(45500.0)
    assert result.interval == 5
    assert isinstance(result.interval_begin, datetime)


def test_parse_kraken_candle_list_valid() -> None:
    """Parse list of Kraken candles.

    Given: List of raw OHLC data,
    When: parse_kraken_candle_list is called,
    Then: Returns list of CandleUpdate objects.
    """
    raw_data: list[dict[str, Any]] = [
        {
            "symbol": "BTC/USD",
            "open": 45000.0,
            "high": 46000.0,
            "low": 44000.0,
            "close": 45500.0,
            "vwap": 45250.0,
            "trades": 100,
            "volume": 500.0,
            "interval_begin": "2024-12-22T10:00:00.000000Z",
            "interval": 5,
        },
    ]
    result = parse_kraken_candle_list(raw_data)
    assert len(result) == 1
    assert result[0].symbol == "BTC-USD"


def test_parse_kraken_candle_list_skips_bad_item(mock_symbol_mapper: Any) -> None:
    """List parser keeps valid candles when one item fails.

    Given: Batch with one valid candle and one with unmapped symbol,
    When: parse_kraken_candle_list is called,
    Then: Returns only the successfully parsed candle.
    """

    def _mapper(symbol: str) -> str:
        if symbol == "UNKNOWN/USD":
            raise ValueError(f"Unknown: {symbol}")
        return symbol.replace("/", "-")

    mock_symbol_mapper.side_effect = _mapper
    raw_data: list[dict[str, Any]] = [
        {
            "symbol": "BTC/USD",
            "open": 45000.0,
            "high": 46000.0,
            "low": 44000.0,
            "close": 45500.0,
            "interval_begin": "2024-12-22T10:00:00.000000Z",
            "interval": 5,
        },
        {
            "symbol": "UNKNOWN/USD",
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "interval_begin": "2024-12-22T10:00:00.000000Z",
            "interval": 5,
        },
    ]
    result = parse_kraken_candle_list(raw_data)
    assert len(result) == 1
    assert result[0].symbol == "BTC-USD"


def test_parse_kraken_trade_valid() -> None:
    """Parse valid Kraken trade to TradeUpdate.

    Given: Raw trade data with all fields,
    When: parse_kraken_trade is called,
    Then: Returns TradeUpdate with quantity mapped from qty.
    """
    raw_data: dict[str, Any] = {
        "symbol": "BTC/USD",
        "side": "buy",
        "qty": 0.5,
        "price": 45000.0,
        "ord_type": "limit",
        "trade_id": 12345,
        "timestamp": "2024-12-22T10:00:00.000000Z",
    }
    result = parse_kraken_trade(raw_data)
    assert isinstance(result, TradeUpdate)
    assert result.symbol == "BTC-USD"
    assert result.side == "buy"
    assert result.quantity == pytest.approx(0.5)
    assert result.price == pytest.approx(45000.0)
    assert result.ord_type == "limit"
    assert result.trade_id == 12345


def test_parse_kraken_trade_list_valid() -> None:
    """Parse list of Kraken trades.

    Given: List of raw trade data,
    When: parse_kraken_trade_list is called,
    Then: Returns list of TradeUpdate objects.
    """
    raw_data: list[dict[str, Any]] = [
        {
            "symbol": "BTC/USD",
            "side": "buy",
            "qty": 0.5,
            "price": 45000.0,
            "ord_type": "limit",
            "trade_id": 12345,
            "timestamp": "2024-12-22T10:00:00.000000Z",
        },
        {
            "symbol": "ETH/USD",
            "side": "sell",
            "qty": 2.0,
            "price": 3000.0,
            "ord_type": "market",
            "trade_id": 12346,
            "timestamp": "2024-12-22T10:01:00.000000Z",
        },
    ]
    result = parse_kraken_trade_list(raw_data)
    assert len(result) == 2
    assert result[0].symbol == "BTC-USD"
    assert result[0].quantity == pytest.approx(0.5)
    assert result[1].symbol == "ETH-USD"
    assert result[1].quantity == pytest.approx(2.0)


def test_parse_kraken_trade_list_skips_bad_item(mock_symbol_mapper: Any) -> None:
    """List parser keeps valid trades when one item fails.

    Given: Batch with one valid and one unmapped trade symbol,
    When: parse_kraken_trade_list is called,
    Then: Returns only the successfully parsed trade.
    """

    def _mapper(symbol: str) -> str:
        if symbol == "UNKNOWN/USD":
            raise ValueError(f"Unknown: {symbol}")
        return symbol.replace("/", "-")

    mock_symbol_mapper.side_effect = _mapper
    raw_data: list[dict[str, Any]] = [
        {
            "symbol": "BTC/USD",
            "side": "buy",
            "qty": 0.5,
            "price": 45000.0,
            "timestamp": "2024-12-22T10:00:00.000000Z",
        },
        {
            "symbol": "UNKNOWN/USD",
            "side": "sell",
            "qty": 1.0,
            "price": 100.0,
            "timestamp": "2024-12-22T10:01:00.000000Z",
        },
    ]
    result = parse_kraken_trade_list(raw_data)
    assert len(result) == 1
    assert result[0].symbol == "BTC-USD"


def test_parse_kraken_execution_valid() -> None:
    """Parse valid Kraken execution to ExecutionUpdate.

    Given: Raw execution data with fees array,
    When: parse_kraken_execution is called,
    Then: Returns ExecutionUpdate with mapped status and fees.
    """
    raw_data: dict[str, Any] = {
        "order_id": "OQCLSE-BW3P3-BUCMWZ",
        "exec_type": "trade",
        "symbol": "BTC/USD",
        "side": "buy",
        "order_type": "limit",
        "order_status": "partially_filled",
        "timestamp": "2024-12-22T10:00:00.000000Z",
        "cum_qty": 0.5,
        "cum_cost": 22500.0,
        "last_qty": 0.5,
        "last_price": 45000.0,
        "fees": [
            {"asset": "USD", "qty": 22.5},
            {"asset": "EUR", "qty": 0.01},
        ],
    }
    result = parse_kraken_execution(raw_data)
    assert isinstance(result, ExecutionUpdate)
    assert result.symbol == "BTC-USD"
    assert result.order_id == "OQCLSE-BW3P3-BUCMWZ"
    assert result.exec_type == "trade"
    assert result.side == OrderSideEnum.BUY
    assert result.order_type == OrderTypeEnum.LIMIT
    assert result.order_status == OrderStatusEnum.OPEN
    assert result.cum_qty == pytest.approx(0.5)
    assert result.fees is not None
    assert len(result.fees) == 2
    assert result.fees[0].asset == "USD"
    assert result.fees[0].quantity == pytest.approx(22.5)


def test_parse_kraken_execution_list_valid() -> None:
    """Parse list of Kraken executions.

    Given: List of raw execution data,
    When: parse_kraken_execution_list is called,
    Then: Returns list with mapped order statuses.
    """
    raw_data: list[dict[str, Any]] = [
        {
            "order_id": "ORDER1",
            "exec_type": "new",
            "symbol": "BTC/USD",
            "side": "buy",
            "order_type": "limit",
            "order_status": "new",
            "timestamp": "2024-12-22T10:00:00.000000Z",
        },
        {
            "order_id": "ORDER2",
            "exec_type": "filled",
            "symbol": "ETH/USD",
            "side": "sell",
            "order_type": "market",
            "order_status": "filled",
            "timestamp": "2024-12-22T10:01:00.000000Z",
        },
    ]
    result = parse_kraken_execution_list(raw_data)
    assert len(result) == 2
    assert result[0].symbol == "BTC-USD"
    assert result[1].symbol == "ETH-USD"
    assert result[0].order_status == OrderStatusEnum.OPEN
    assert result[1].order_status == OrderStatusEnum.CLOSED


def test_parse_kraken_execution_list_skips_bad_item(mock_symbol_mapper: Any) -> None:
    """List parser keeps valid executions when one item fails.

    Given: Batch with one valid execution and one with unmapped symbol,
    When: parse_kraken_execution_list is called,
    Then: Returns only the successfully parsed execution.
    """

    def _mapper(symbol: str) -> str:
        if symbol == "UNKNOWN/USD":
            raise ValueError(f"Unknown: {symbol}")
        return symbol.replace("/", "-")

    mock_symbol_mapper.side_effect = _mapper
    raw_data: list[dict[str, Any]] = [
        {
            "order_id": "ORDER1",
            "exec_type": "new",
            "symbol": "BTC/USD",
            "side": "buy",
            "order_type": "limit",
            "order_status": "new",
            "timestamp": "2024-12-22T10:00:00.000000Z",
        },
        {
            "order_id": "ORDER2",
            "exec_type": "filled",
            "symbol": "UNKNOWN/USD",
            "side": "sell",
            "order_type": "market",
            "order_status": "filled",
            "timestamp": "2024-12-22T10:01:00.000000Z",
        },
    ]
    result = parse_kraken_execution_list(raw_data)
    assert len(result) == 1
    assert result[0].symbol == "BTC-USD"


def test_parse_kraken_instrument_valid() -> None:
    """Parse valid Kraken instrument to descriptor.

    Given: Raw instrument data with precision fields,
    When: parse_kraken_instrument is called,
    Then: Returns InstrumentPairDescriptor with all fields.
    """
    raw_data: dict[str, Any] = {
        "symbol": "BTC/USD",
        "status": "online",
        "base": "BTC",
        "quote": "USD",
        "qty_precision": 8,
        "qty_increment": 0.00000001,
        "qty_min": 0.0001,
        "price_precision": 2,
        "price_increment": 0.01,
        "cost_precision": 5,
        "cost_min": 0.5,
        "marginable": True,
        "has_index": True,
    }
    result = parse_kraken_instrument(raw_data)
    assert isinstance(result, InstrumentPairDescriptor)
    assert result.symbol == "BTC-USD"
    assert result.base == "BTC"
    assert result.quote == "USD"
    assert result.status == "online"
    assert result.qty_precision == 8
    assert result.marginable is True


def test_parse_kraken_instrument_list_valid() -> None:
    """Parse list of Kraken instruments.

    Given: List of raw instrument data,
    When: parse_kraken_instrument_list is called,
    Then: Returns list of descriptors.
    """
    raw_data: list[dict[str, Any]] = [
        {
            "symbol": "BTC/USD",
            "status": "online",
            "base": "BTC",
            "quote": "USD",
            "qty_precision": 8,
            "qty_increment": 0.00000001,
            "qty_min": 0.0001,
            "price_precision": 2,
            "price_increment": 0.01,
            "cost_precision": 5,
            "cost_min": 0.5,
            "marginable": True,
            "has_index": True,
        },
    ]
    result = parse_kraken_instrument_list(raw_data)
    assert len(result) == 1
    assert result[0].symbol == "BTC-USD"


def test_parse_kraken_instrument_list_skips_bad_item(mock_symbol_mapper: Any) -> None:
    """List parser keeps valid instruments when one item fails.

    Given: Batch with one valid instrument and one with unmapped symbol,
    When: parse_kraken_instrument_list is called,
    Then: Returns only the successfully parsed instrument.
    """

    def _mapper(symbol: str) -> str:
        if symbol == "UNKNOWN/USD":
            raise ValueError(f"Unknown: {symbol}")
        return symbol.replace("/", "-")

    mock_symbol_mapper.side_effect = _mapper
    raw_data: list[dict[str, Any]] = [
        {
            "symbol": "BTC/USD",
            "status": "online",
            "base": "BTC",
            "quote": "USD",
            "qty_precision": 8,
            "qty_increment": 0.00000001,
            "qty_min": 0.0001,
            "price_precision": 2,
            "price_increment": 0.01,
            "cost_precision": 5,
            "cost_min": 0.5,
        },
        {
            "symbol": "UNKNOWN/USD",
            "status": "online",
            "base": "UNK",
            "quote": "USD",
            "qty_precision": 2,
            "qty_increment": 0.01,
            "qty_min": 1.0,
            "price_precision": 2,
            "price_increment": 0.01,
            "cost_precision": 2,
            "cost_min": 1.0,
        },
    ]
    result = parse_kraken_instrument_list(raw_data)
    assert len(result) == 1
    assert result[0].symbol == "BTC-USD"


def test_kraken_ticker_schema_allows_extra_fields() -> None:
    """Ticker schema ignores unknown API fields.

    Given: Ticker data with extra unknown field,
    When: parse_kraken_ticker is called,
    Then: Parses successfully ignoring extra field.
    """
    raw_data: dict[str, Any] = {
        "symbol": "BTC/USD",
        "bid": 45000.0,
        "bid_qty": 1.5,
        "ask": 45100.0,
        "ask_qty": 2.0,
        "last": 45050.0,
        "volume": 1000.0,
        "vwap": 45025.0,
        "low": 44000.0,
        "high": 46000.0,
        "change": 500.0,
        "change_pct": 1.11,
        "new_field_from_api_update": "should not fail",
    }
    result = parse_kraken_ticker(raw_data)
    assert result.symbol == "BTC-USD"


def test_kraken_execution_without_optional_fields() -> None:
    """Parse execution with minimal required fields.

    Given: Execution data without optional fields,
    When: parse_kraken_execution is called,
    Then: Optional fields are None.
    """
    raw_data: dict[str, Any] = {
        "order_id": "ORDER123",
        "exec_type": "new",
        "symbol": "BTC/USD",
        "side": "buy",
        "order_type": "limit",
        "order_status": "new",
        "timestamp": "2024-12-22T10:00:00.000000Z",
    }
    result = parse_kraken_execution(raw_data)
    assert result.order_id == "ORDER123"
    assert result.fees is None
    assert result.cum_qty is None
    assert result.last_price is None


def test_parse_kraken_candle_missing_interval_begin() -> None:
    """Reject candle without required timestamp.

    Given: Candle data missing interval_begin,
    When: parse_kraken_candle is called,
    Then: Raises ValueError with descriptive message.
    """
    raw_data: dict[str, Any] = {
        "symbol": "BTC/USD",
        "open": 45000.0,
        "high": 46000.0,
        "low": 44000.0,
        "close": 45500.0,
    }
    with pytest.raises(ValueError, match="Candle data missing required 'interval_begin' timestamp"):
        parse_kraken_candle(raw_data)


def test_parse_kraken_execution_missing_timestamp() -> None:
    """Reject execution without required timestamp.

    Given: Execution data missing timestamp field,
    When: parse_kraken_execution is called,
    Then: Raises ValueError with descriptive message.
    """
    raw_data: dict[str, Any] = {
        "order_id": "ORDER123",
        "exec_type": "new",
        "symbol": "BTC/USD",
        "side": "buy",
        "order_type": "limit",
        "order_status": "new",
    }
    with pytest.raises(ValueError, match="Execution data missing required 'timestamp' field"):
        parse_kraken_execution(raw_data)


def test_parse_kraken_execution_invalid_side() -> None:
    """Reject execution with unknown order side.

    Given: Execution data with invalid side value,
    When: parse_kraken_execution is called,
    Then: Raises ValueError for unknown side.
    """
    raw_data: dict[str, Any] = {
        "order_id": "ORDER123",
        "exec_type": "new",
        "symbol": "BTC/USD",
        "side": "invalid_side",
        "order_type": "limit",
        "order_status": "new",
        "timestamp": "2024-12-22T10:00:00.000000Z",
    }
    with pytest.raises(ValueError, match="Unknown order side: invalid_side"):
        parse_kraken_execution(raw_data)


def test_parse_kraken_execution_invalid_order_type() -> None:
    """Reject execution with unknown order type.

    Given: Execution data with invalid order_type,
    When: parse_kraken_execution is called,
    Then: Raises ValueError for unknown type.
    """
    raw_data: dict[str, Any] = {
        "order_id": "ORDER123",
        "exec_type": "new",
        "symbol": "BTC/USD",
        "side": "buy",
        "order_type": "unknown_type",
        "order_status": "new",
        "timestamp": "2024-12-22T10:00:00.000000Z",
    }
    with pytest.raises(ValueError, match="Unknown order type: unknown_type"):
        parse_kraken_execution(raw_data)


def test_parse_kraken_execution_invalid_order_status() -> None:
    """Reject execution with unknown order status.

    Given: Execution data with invalid order_status,
    When: parse_kraken_execution is called,
    Then: Raises ValueError for unknown status.
    """
    raw_data: dict[str, Any] = {
        "order_id": "ORDER123",
        "exec_type": "new",
        "symbol": "BTC/USD",
        "side": "buy",
        "order_type": "limit",
        "order_status": "bad_status",
        "timestamp": "2024-12-22T10:00:00.000000Z",
    }
    with pytest.raises(ValueError, match="Unknown order status: bad_status"):
        parse_kraken_execution(raw_data)


def test_parse_kraken_execution_with_time_in_force_gtc() -> None:
    """Parse execution with GTC time in force.

    Given: Execution data with time_in_force="GTC",
    When: parse_kraken_execution is called,
    Then: Returns ExecutionUpdate with TimeInForceEnum.GTC.
    """
    raw_data: dict[str, Any] = {
        "order_id": "ORDER123",
        "exec_type": "new",
        "symbol": "BTC/USD",
        "side": "buy",
        "order_type": "limit",
        "order_status": "new",
        "timestamp": "2024-12-22T10:00:00.000000Z",
        "time_in_force": "GTC",
    }
    result = parse_kraken_execution(raw_data)
    assert result.time_in_force == TimeInForceEnum.GTC


def test_parse_kraken_execution_with_time_in_force_ioc() -> None:
    """Parse execution with IOC time in force.

    Given: Execution data with time_in_force="IOC",
    When: parse_kraken_execution is called,
    Then: Returns ExecutionUpdate with TimeInForceEnum.IOC.
    """
    raw_data: dict[str, Any] = {
        "order_id": "ORDER123",
        "exec_type": "new",
        "symbol": "BTC/USD",
        "side": "buy",
        "order_type": "limit",
        "order_status": "new",
        "timestamp": "2024-12-22T10:00:00.000000Z",
        "time_in_force": "IOC",
    }
    result = parse_kraken_execution(raw_data)
    assert result.time_in_force == TimeInForceEnum.IOC


def test_parse_kraken_execution_with_time_in_force_gtd() -> None:
    """Parse execution with GTD time in force.

    Given: Execution data with time_in_force="GTD",
    When: parse_kraken_execution is called,
    Then: Returns ExecutionUpdate with TimeInForceEnum.GTD.
    """
    raw_data: dict[str, Any] = {
        "order_id": "ORDER123",
        "exec_type": "new",
        "symbol": "BTC/USD",
        "side": "buy",
        "order_type": "limit",
        "order_status": "new",
        "timestamp": "2024-12-22T10:00:00.000000Z",
        "time_in_force": "GTD",
    }
    result = parse_kraken_execution(raw_data)
    assert result.time_in_force == TimeInForceEnum.GTD


def test_parse_kraken_execution_with_unknown_time_in_force() -> None:
    """Handle execution with unknown time in force.

    Given: Execution data with unrecognized time_in_force,
    When: parse_kraken_execution is called,
    Then: Returns ExecutionUpdate with time_in_force=None.
    """
    raw_data: dict[str, Any] = {
        "order_id": "ORDER123",
        "exec_type": "new",
        "symbol": "BTC/USD",
        "side": "buy",
        "order_type": "limit",
        "order_status": "new",
        "timestamp": "2024-12-22T10:00:00.000000Z",
        "time_in_force": "UNKNOWN_TIF",
    }
    result = parse_kraken_execution(raw_data)
    assert result.time_in_force is None


def test_parse_kraken_execution_with_no_time_in_force() -> None:
    """Handle execution without time in force field.

    Given: Execution data without time_in_force field,
    When: parse_kraken_execution is called,
    Then: Returns ExecutionUpdate with time_in_force=None.
    """
    raw_data: dict[str, Any] = {
        "order_id": "ORDER123",
        "exec_type": "new",
        "symbol": "BTC/USD",
        "side": "buy",
        "order_type": "limit",
        "order_status": "new",
        "timestamp": "2024-12-22T10:00:00.000000Z",
    }
    result = parse_kraken_execution(raw_data)
    assert result.time_in_force is None
