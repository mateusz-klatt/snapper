"""Tests for Kraken Futures data format adapter functions."""

from collections.abc import Generator
from unittest.mock import patch

import pytest

from snapper.infrastructure.exchanges.adapters.kraken_futures import _tick_size_to_precision
from snapper.infrastructure.exchanges.adapters.kraken_futures import parse_kraken_futures_fill
from snapper.infrastructure.exchanges.adapters.kraken_futures import parse_kraken_futures_instrument
from snapper.infrastructure.exchanges.adapters.kraken_futures import (
    parse_kraken_futures_instrument_list,
)
from snapper.infrastructure.exchanges.adapters.kraken_futures import (
    parse_kraken_futures_order_status,
)
from snapper.infrastructure.exchanges.adapters.kraken_futures import parse_kraken_futures_ticker
from snapper.infrastructure.exchanges.adapters.kraken_futures import (
    parse_kraken_futures_ticker_list,
)
from snapper.infrastructure.exchanges.adapters.kraken_futures import parse_kraken_futures_trade
from snapper.infrastructure.exchanges.adapters.kraken_futures import parse_kraken_futures_trade_list
from snapper.infrastructure.exchanges.contracts import ExecutionFeeBreakdown
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import OrderStatusEnum
from snapper.infrastructure.exchanges.contracts import OrderTypeEnum


@pytest.fixture(autouse=True)
def _patch_symbol_mapper() -> Generator[None]:
    """Patch symbol mapper for all adapter tests."""
    rev = {
        "PF_XBTUSD": "BTC-USD-PERP",
        "PF_ETHUSD": "ETH-USD-PERP",
        "PI_XBTUSD": "BTC-USD-PERP-INV",
    }

    def _lookup(s: str) -> str:
        if s in rev:
            return rev[s]
        raise ValueError(f"Unknown Kraken Futures WS symbol: {s}")

    with patch(
        "snapper.infrastructure.exchanges.adapters.kraken_futures.kraken_futures_ws_to_native",
        side_effect=_lookup,
    ):
        yield


class TestParseKrakenFuturesTicker:
    """Tests for parse_kraken_futures_ticker."""

    def test_parse_full_ticker(self) -> None:
        """Parse a complete Kraken Futures ticker into TickerUpdate.

        Given: Full ticker dict from REST API,
        When: parse_kraken_futures_ticker is called,
        Then: Returns TickerUpdate with mapped fields.
        """
        raw = {
            "symbol": "PF_XBTUSD",
            "last": 66621.0,
            "lastTime": "2026-03-31T12:00:00Z",
            "lastSize": 10.0,
            "tag": "perpetual",
            "pair": "BTC:USD",
            "markPrice": 66500.5,
            "bid": 66500.0,
            "bidSize": 50.0,
            "ask": 66510.0,
            "askSize": 30.0,
            "vol24h": 12345.0,
            "open24h": 66000.0,
            "high24h": 67000.0,
            "low24h": 65500.0,
            "fundingRate": -0.0004,
            "indexPrice": 66505.0,
            "suspended": False,
            "postOnly": False,
            "change24h": 0.94,
        }
        result = parse_kraken_futures_ticker(raw)
        assert result.symbol == "BTC-USD-PERP"
        assert result.bid == pytest.approx(66500.0)
        assert result.ask == pytest.approx(66510.0)
        assert result.last == pytest.approx(66621.0)
        assert result.volume == pytest.approx(12345.0)
        assert result.low == pytest.approx(65500.0)
        assert result.high == pytest.approx(67000.0)

    def test_parse_minimal_ticker(self) -> None:
        """Parse ticker with mostly None fields.

        Given: Ticker dict with only symbol,
        When: parse_kraken_futures_ticker is called,
        Then: Returns TickerUpdate with zeroed optional fields.
        """
        raw = {"symbol": "PF_XBTUSD"}
        result = parse_kraken_futures_ticker(raw)
        assert result.symbol == "BTC-USD-PERP"
        assert result.bid == pytest.approx(0.0)
        assert result.ask == pytest.approx(0.0)


class TestParseKrakenFuturesTickerList:
    """Tests for parse_kraken_futures_ticker_list."""

    def test_parse_list_with_valid_items(self) -> None:
        """Parse list of valid tickers.

        Given: List with two valid ticker dicts,
        When: parse_kraken_futures_ticker_list is called,
        Then: Returns list of two TickerUpdate objects.
        """
        data = [
            {"symbol": "PF_XBTUSD", "last": 66621.0},
            {"symbol": "PF_ETHUSD", "last": 3500.0},
        ]
        results = parse_kraken_futures_ticker_list(data)
        assert len(results) == 2

    def test_skip_unparseable_items(self) -> None:
        """Skip items that fail to parse.

        Given: List with one valid and one invalid ticker,
        When: parse_kraken_futures_ticker_list is called,
        Then: Returns only the successfully parsed item.
        """
        data = [
            {"symbol": "PF_XBTUSD", "last": 66621.0},
            {"symbol": "UNMAPPED_SYMBOL", "last": 100.0},
        ]
        results = parse_kraken_futures_ticker_list(data)
        assert len(results) == 1
        assert results[0].symbol == "BTC-USD-PERP"


class TestParseKrakenFuturesTrade:
    """Tests for parse_kraken_futures_trade."""

    def test_parse_rest_trade(self) -> None:
        """Parse a trade from REST API with ISO 8601 time.

        Given: REST trade dict with string timestamp and qty field,
        When: parse_kraken_futures_trade is called,
        Then: Returns TradeUpdate with parsed timestamp.
        """
        raw = {
            "symbol": "PF_XBTUSD",
            "time": "2026-03-31T11:32:44.255Z",
            "trade_id": 100,
            "price": 66621.0,
            "qty": 10.0,
            "side": "sell",
            "type": "fill",
            "uid": "b3416619-01a6-4593-8aab-faa496aa8d72",
        }
        result = parse_kraken_futures_trade(raw)
        assert result.symbol == "BTC-USD-PERP"
        assert result.price == pytest.approx(66621.0)
        assert result.quantity == pytest.approx(10.0)
        assert result.side == "sell"
        assert result.trade_id == "b3416619-01a6-4593-8aab-faa496aa8d72"

    def test_parse_ws_trade(self) -> None:
        """Parse a trade from WS with integer millisecond timestamp.

        Given: WS trade dict with int ms time and product_id context,
        When: parse_kraken_futures_trade is called,
        Then: Returns TradeUpdate with correctly converted timestamp.
        """
        raw = {
            "product_id": "PI_XBTUSD",
            "time": 1640995200123,
            "price": 50000.0,
            "qty": 0.5,
            "side": "buy",
            "uid": "abc-123",
        }
        result = parse_kraken_futures_trade(raw)
        assert result.symbol == "BTC-USD-PERP-INV"
        assert result.quantity == pytest.approx(0.5)
        assert result.timestamp.year == 2022

    def test_raises_when_both_product_id_and_symbol_missing(self) -> None:
        """Raise ValueError when trade has neither product_id nor symbol.

        Given: Trade dict without product_id or symbol keys,
        When: parse_kraken_futures_trade is called,
        Then: Raises ValueError indicating missing identifier.
        """
        raw = {
            "time": 1640995200000,
            "price": 100.0,
            "qty": 5.0,
            "side": "buy",
        }
        with pytest.raises(ValueError, match="missing both product_id and symbol"):
            parse_kraken_futures_trade(raw)


class TestParseKrakenFuturesTradeList:
    """Tests for parse_kraken_futures_trade_list."""

    def test_parse_list_with_valid_items(self) -> None:
        """Parse list of valid trades.

        Given: List with one valid trade,
        When: parse_kraken_futures_trade_list is called,
        Then: Returns list with one TradeUpdate.
        """
        data = [
            {
                "symbol": "PF_XBTUSD",
                "time": 1640995200000,
                "price": 50000.0,
                "qty": 1.0,
                "side": "buy",
            }
        ]
        results = parse_kraken_futures_trade_list(data)
        assert len(results) == 1

    def test_skip_unparseable_items(self) -> None:
        """Skip trades that fail to parse.

        Given: List with one valid and one invalid trade,
        When: parse_kraken_futures_trade_list is called,
        Then: Returns only the valid trade.
        """
        data = [
            {
                "symbol": "PF_XBTUSD",
                "time": 1640995200000,
                "price": 50000.0,
                "qty": 1.0,
                "side": "buy",
            },
            {"invalid": "data"},
        ]
        results = parse_kraken_futures_trade_list(data)
        assert len(results) == 1


class TestParseKrakenFuturesInstrument:
    """Tests for parse_kraken_futures_instrument."""

    def test_parse_perpetual(self) -> None:
        """Parse a perpetual futures instrument.

        Given: Full instrument dict for PI_XBTUSD,
        When: parse_kraken_futures_instrument is called,
        Then: Returns InstrumentPairDescriptor with correct fields.
        """
        raw = {
            "symbol": "PI_XBTUSD",
            "type": "futures_inverse",
            "underlying": "rr_xbtusd",
            "tickSize": 0.5,
            "contractSize": 1,
            "tradeable": True,
            "base": "BTC",
            "quote": "USD",
            "pair": "BTC:USD",
            "marginLevels": [{"contracts": 0, "initialMargin": 0.02, "maintenanceMargin": 0.01}],
        }
        result = parse_kraken_futures_instrument(raw)
        assert result.symbol == "PI_XBTUSD"
        assert result.base == "BTC"
        assert result.quote == "USD"
        assert result.status == "active"
        assert result.marginable is True
        assert result.has_index is True
        assert result.margin_initial == pytest.approx(0.02)
        assert result.tick_size == pytest.approx(0.5)
        assert result.price_precision == 1

    def test_parse_inactive_instrument(self) -> None:
        """Parse an inactive instrument.

        Given: Instrument with tradeable=False and no margins,
        When: parse_kraken_futures_instrument is called,
        Then: Returns descriptor with status=inactive and marginable=False.
        """
        raw = {
            "symbol": "FF_EXPIRED",
            "type": "futures_vanilla",
            "tickSize": 1.0,
            "contractSize": 0.001,
            "tradeable": False,
        }
        result = parse_kraken_futures_instrument(raw)
        assert result.status == "inactive"
        assert result.marginable is False
        assert result.margin_initial is None
        assert result.has_index is False


class TestParseKrakenFuturesInstrumentList:
    """Tests for parse_kraken_futures_instrument_list."""

    def test_parse_list(self) -> None:
        """Parse list of instruments.

        Given: List with two valid instruments,
        When: parse_kraken_futures_instrument_list is called,
        Then: Returns list of InstrumentPairDescriptor objects.
        """
        data = [
            {
                "symbol": "PI_XBTUSD",
                "type": "futures_inverse",
                "tickSize": 0.5,
                "contractSize": 1,
                "tradeable": True,
            },
            {
                "symbol": "PI_ETHUSD",
                "type": "futures_inverse",
                "tickSize": 0.05,
                "contractSize": 1,
                "tradeable": True,
            },
        ]
        results = parse_kraken_futures_instrument_list(data)
        assert len(results) == 2

    def test_skip_invalid(self) -> None:
        """Skip instruments that fail to parse.

        Given: List with one valid and one missing required fields,
        When: parse_kraken_futures_instrument_list is called,
        Then: Returns only the valid instrument.
        """
        data = [
            {
                "symbol": "PI_XBTUSD",
                "type": "futures_inverse",
                "tickSize": 0.5,
                "contractSize": 1,
                "tradeable": True,
            },
            {"invalid": "data"},
        ]
        results = parse_kraken_futures_instrument_list(data)
        assert len(results) == 1


class TestTickSizeToPrecision:
    """Tests for _tick_size_to_precision helper."""

    def test_whole_number(self) -> None:
        """Tick size of 1.0 yields precision 0.

        Given: tick_size=1.0,
        When: _tick_size_to_precision is called,
        Then: Returns 0.
        """
        assert _tick_size_to_precision(1.0) == 0

    def test_half_step(self) -> None:
        """Tick size of 0.5 yields precision 1.

        Given: tick_size=0.5,
        When: _tick_size_to_precision is called,
        Then: Returns 1.
        """
        assert _tick_size_to_precision(0.5) == 1

    def test_two_decimals(self) -> None:
        """Tick size of 0.05 yields precision 2.

        Given: tick_size=0.05,
        When: _tick_size_to_precision is called,
        Then: Returns 2.
        """
        assert _tick_size_to_precision(0.05) == 2

    def test_large_tick(self) -> None:
        """Tick size of 10.0 yields precision 0.

        Given: tick_size=10.0,
        When: _tick_size_to_precision is called,
        Then: Returns 0.
        """
        assert _tick_size_to_precision(10.0) == 0

    def test_decimal_above_one(self) -> None:
        """Tick size of 2.5 yields precision 1.

        Given: tick_size=2.5 (some equity index futures),
        When: _tick_size_to_precision is called,
        Then: Returns 1 (not 0).
        """
        assert _tick_size_to_precision(2.5) == 1


class TestParseKrakenFuturesFill:
    """Tests for parse_kraken_futures_fill."""

    def test_parse_fill_to_execution_update(self) -> None:
        """Parse a fill event into an ExecutionUpdate.

        Given: Fill dict from the fills WS channel with buy=True,
        When: parse_kraken_futures_fill is called with a symbol mapper,
        Then: Returns ExecutionUpdate with correct fields mapped.
        """
        data = {
            "fill_id": "fill-001",
            "order_id": "order-001",
            "instrument": "PF_XBTUSD",
            "buy": True,
            "qty": 5.0,
            "price": 66500.0,
            "time": 1743676200000,
            "fill_type": "taker",
            "order_type": "lmt",
            "fee_paid": 1.25,
            "cli_ord_id": "my-order-1",
        }
        mapper = {"PF_XBTUSD": "BTC-USD-PERP"}
        result = parse_kraken_futures_fill(data, lambda s: mapper[s])
        assert result.order_id == "order-001"
        assert result.exec_type == "trade"
        assert result.symbol == "BTC-USD-PERP"
        assert result.side == OrderSideEnum.BUY
        assert result.order_type == OrderTypeEnum.LIMIT
        assert result.last_qty == pytest.approx(5.0)
        assert result.last_price == pytest.approx(66500.0)
        assert result.exec_id == "fill-001"
        assert result.cl_ord_id == "my-order-1"
        assert result.fee_usd_equiv == pytest.approx(1.25)
        assert result.timestamp.year == 2025

    def test_parse_sell_fill(self) -> None:
        """Parse a sell-side fill.

        Given: Fill dict with buy=False,
        When: parse_kraken_futures_fill is called,
        Then: Returns ExecutionUpdate with SELL side.
        """
        data = {
            "fill_id": "fill-002",
            "order_id": "order-002",
            "instrument": "PF_ETHUSD",
            "buy": False,
            "qty": 10.0,
            "price": 3500.0,
            "time": 1743678000000,
        }
        mapper = {"PF_ETHUSD": "ETH-USD-PERP"}
        result = parse_kraken_futures_fill(data, lambda s: mapper[s])
        assert result.side == OrderSideEnum.SELL
        assert result.symbol == "ETH-USD-PERP"

    def test_usd_fee_maps_to_fee_usd_equiv(self) -> None:
        """Map USD fee to fee_usd_equiv.

        Given: Fill with fee_currency=USD,
        When: parse_kraken_futures_fill is called,
        Then: fee_usd_equiv is populated and fees is None.
        """
        data = {
            "fill_id": "fill-usd",
            "order_id": "order-usd",
            "instrument": "PF_XBTUSD",
            "buy": True,
            "qty": 1.0,
            "price": 66000.0,
            "time": 1743676200000,
            "fee_paid": 2.50,
            "fee_currency": "USD",
        }
        mapper = {"PF_XBTUSD": "BTC-USD-PERP"}
        result = parse_kraken_futures_fill(data, lambda s: mapper[s])
        assert result.fee_usd_equiv == pytest.approx(2.50)
        assert result.fees is None

    def test_non_usd_fee_maps_to_fees_breakdown(self) -> None:
        """Map non-USD fee to fees breakdown list.

        Given: Fill with fee_currency=ETH (not USD),
        When: parse_kraken_futures_fill is called,
        Then: fee_usd_equiv is None and fees contains breakdown.
        """
        data = {
            "fill_id": "fill-eth",
            "order_id": "order-eth",
            "instrument": "PF_ETHUSD",
            "buy": True,
            "qty": 5.0,
            "price": 3500.0,
            "time": 1743676200000,
            "fee_paid": 0.001,
            "fee_currency": "ETH",
        }
        mapper = {"PF_ETHUSD": "ETH-USD-PERP"}
        result = parse_kraken_futures_fill(data, lambda s: mapper[s])
        assert result.fee_usd_equiv is None
        assert result.fees is not None
        assert len(result.fees) == 1
        assert result.fees[0] == ExecutionFeeBreakdown(asset="ETH", quantity=0.001)

    def test_no_fee_currency_defaults_to_usd(self) -> None:
        """Default to USD when fee_currency is absent.

        Given: Fill with fee_paid but no fee_currency,
        When: parse_kraken_futures_fill is called,
        Then: fee_usd_equiv is populated (assumes USD).
        """
        data = {
            "fill_id": "fill-noc",
            "order_id": "order-noc",
            "instrument": "PF_XBTUSD",
            "buy": True,
            "qty": 1.0,
            "price": 66000.0,
            "time": 1743676200000,
            "fee_paid": 1.0,
        }
        mapper = {"PF_XBTUSD": "BTC-USD-PERP"}
        result = parse_kraken_futures_fill(data, lambda s: mapper[s])
        assert result.fee_usd_equiv == pytest.approx(1.0)
        assert result.fees is None


class TestParseKrakenFuturesOrderStatus:
    """Tests for parse_kraken_futures_order_status."""

    def test_parse_partially_filled_order(self) -> None:
        """Parse a partially filled order status update.

        Given: Order dict with filled < qty (partially filled),
        When: parse_kraken_futures_order_status is called,
        Then: Returns ExecutionUpdate with OPEN status and cum_qty.
        """
        data = {
            "order_id": "order-001",
            "instrument": "PF_XBTUSD",
            "direction": 0,
            "type": "limit",
            "qty": 10.0,
            "filled": 3.0,
            "limit_price": 66000.0,
            "time": 1743676200000,
            "cli_ord_id": "my-order-1",
        }
        mapper = {"PF_XBTUSD": "BTC-USD-PERP"}
        result = parse_kraken_futures_order_status(data, lambda s: mapper[s])
        assert result.order_id == "order-001"
        assert result.exec_type == "status"
        assert result.symbol == "BTC-USD-PERP"
        assert result.side == OrderSideEnum.BUY
        assert result.order_type == OrderTypeEnum.LIMIT
        assert result.order_status == OrderStatusEnum.OPEN
        assert result.cum_qty == pytest.approx(3.0)
        assert result.order_qty == pytest.approx(10.0)
        assert result.limit_price == pytest.approx(66000.0)
        assert result.cl_ord_id == "my-order-1"

    def test_parse_fully_filled_order(self) -> None:
        """Parse a fully filled order status update.

        Given: Order dict with filled >= qty,
        When: parse_kraken_futures_order_status is called,
        Then: Returns ExecutionUpdate with CLOSED status.
        """
        data = {
            "order_id": "order-002",
            "instrument": "PF_ETHUSD",
            "direction": 1,
            "type": "limit",
            "qty": 5.0,
            "filled": 5.0,
            "time": 1743678000000,
        }
        mapper = {"PF_ETHUSD": "ETH-USD-PERP"}
        result = parse_kraken_futures_order_status(data, lambda s: mapper[s])
        assert result.order_status == OrderStatusEnum.CLOSED
        assert result.exec_type == "status"
        assert result.side == OrderSideEnum.SELL

    def test_parse_stop_order_type(self) -> None:
        """Parse an order with stop type.

        Given: Order dict with type=stop,
        When: parse_kraken_futures_order_status is called,
        Then: Returns ExecutionUpdate with STOP_LOSS order type.
        """
        data = {
            "order_id": "order-003",
            "instrument": "PF_XBTUSD",
            "direction": 0,
            "type": "stop",
            "qty": 1.0,
            "time": 1743676200000,
        }
        mapper = {"PF_XBTUSD": "BTC-USD-PERP"}
        result = parse_kraken_futures_order_status(data, lambda s: mapper[s])
        assert result.order_type == OrderTypeEnum.STOP_LOSS
        assert result.order_status == OrderStatusEnum.OPEN

    def test_parse_take_profit_order_type(self) -> None:
        """Parse an order with take_profit type.

        Given: Order dict with type=take_profit,
        When: parse_kraken_futures_order_status is called,
        Then: Returns ExecutionUpdate with TAKE_PROFIT order type.
        """
        data = {
            "order_id": "order-004",
            "instrument": "PF_XBTUSD",
            "direction": 1,
            "type": "take_profit",
            "qty": 2.0,
            "time": 1743676200000,
        }
        mapper = {"PF_XBTUSD": "BTC-USD-PERP"}
        result = parse_kraken_futures_order_status(data, lambda s: mapper[s])
        assert result.order_type == OrderTypeEnum.TAKE_PROFIT
        assert result.side == OrderSideEnum.SELL

    def test_zero_limit_price_becomes_none(self) -> None:
        """Parse order with zero limit_price maps to None.

        Given: Order dict with limit_price=0.0,
        When: parse_kraken_futures_order_status is called,
        Then: Returns ExecutionUpdate with limit_price=None.
        """
        data = {
            "order_id": "order-005",
            "instrument": "PF_XBTUSD",
            "direction": 0,
            "type": "limit",
            "qty": 1.0,
            "limit_price": 0.0,
            "time": 1743676200000,
        }
        mapper = {"PF_XBTUSD": "BTC-USD-PERP"}
        result = parse_kraken_futures_order_status(data, lambda s: mapper[s])
        assert result.limit_price is None

    def test_last_update_time_preferred_over_time(self) -> None:
        """Prefer last_update_time over creation time for timestamp.

        Given: Order dict with both time and last_update_time,
        When: parse_kraken_futures_order_status is called,
        Then: Timestamp uses last_update_time (the more recent value).
        """
        data = {
            "order_id": "order-ts",
            "instrument": "PF_XBTUSD",
            "direction": 0,
            "type": "limit",
            "qty": 1.0,
            "time": 1743676200000,
            "last_update_time": 1743680000000,
        }
        mapper = {"PF_XBTUSD": "BTC-USD-PERP"}
        result = parse_kraken_futures_order_status(data, lambda s: mapper[s])
        assert result.timestamp.year == 2025
        expected_ts = 1743680000000 / 1000
        assert result.timestamp.timestamp() == pytest.approx(expected_ts)

    def test_fallback_to_time_when_no_last_update_time(self) -> None:
        """Fall back to creation time when last_update_time is absent.

        Given: Order dict with only time (no last_update_time),
        When: parse_kraken_futures_order_status is called,
        Then: Timestamp uses time field.
        """
        data = {
            "order_id": "order-ft",
            "instrument": "PF_XBTUSD",
            "direction": 0,
            "type": "limit",
            "qty": 1.0,
            "time": 1743676200000,
        }
        mapper = {"PF_XBTUSD": "BTC-USD-PERP"}
        result = parse_kraken_futures_order_status(data, lambda s: mapper[s])
        expected_ts = 1743676200000 / 1000
        assert result.timestamp.timestamp() == pytest.approx(expected_ts)
