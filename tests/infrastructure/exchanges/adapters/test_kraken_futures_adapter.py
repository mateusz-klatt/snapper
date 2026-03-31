"""Tests for Kraken Futures data format adapter functions."""

from collections.abc import Generator
from unittest.mock import patch

import pytest

from snapper.infrastructure.exchanges.adapters.kraken_futures import _tick_size_to_precision
from snapper.infrastructure.exchanges.adapters.kraken_futures import parse_kraken_futures_instrument
from snapper.infrastructure.exchanges.adapters.kraken_futures import (
    parse_kraken_futures_instrument_list,
)
from snapper.infrastructure.exchanges.adapters.kraken_futures import parse_kraken_futures_ticker
from snapper.infrastructure.exchanges.adapters.kraken_futures import (
    parse_kraken_futures_ticker_list,
)
from snapper.infrastructure.exchanges.adapters.kraken_futures import parse_kraken_futures_trade
from snapper.infrastructure.exchanges.adapters.kraken_futures import parse_kraken_futures_trade_list


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
