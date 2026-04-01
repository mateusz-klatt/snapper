"""Tests for Kraken Equities data format adapter functions."""

from collections.abc import Generator
from unittest.mock import patch

import pytest

from snapper.infrastructure.exchanges.adapters.kraken_equities import _tick_size_to_precision
from snapper.infrastructure.exchanges.adapters.kraken_equities import (
    parse_kraken_equities_instrument,
)
from snapper.infrastructure.exchanges.adapters.kraken_equities import (
    parse_kraken_equities_instrument_list,
)
from snapper.infrastructure.exchanges.adapters.kraken_equities import parse_kraken_equities_ticker
from snapper.infrastructure.exchanges.adapters.kraken_equities import (
    parse_kraken_equities_ticker_list,
)
from snapper.infrastructure.exchanges.adapters.kraken_equities import parse_kraken_equities_trade
from snapper.infrastructure.exchanges.adapters.kraken_equities import (
    parse_kraken_equities_trade_list,
)


@pytest.fixture(autouse=True)
def _patch_symbol_mapper() -> Generator[None]:
    """Patch symbol mapper for all adapter tests."""
    rev = {
        "CLM6.NYMEX": "CLM6-NYMEX",
        "GCQ6.COMEX": "GCQ6-COMEX",
        "ESM6.CME": "ESM6-CME",
    }

    def _lookup(s: str) -> str:
        if s in rev:
            return rev[s]
        raise ValueError(f"Unknown Kraken Equities WS symbol: {s}")

    with patch(
        "snapper.infrastructure.exchanges.adapters.kraken_equities.kraken_equities_ws_to_native",
        side_effect=_lookup,
    ):
        yield


class TestParseKrakenEquitiesTicker:
    """Tests for parse_kraken_equities_ticker."""

    def test_parse_full_ticker(self) -> None:
        """Parse a complete Kraken Equities ticker into TickerUpdate.

        Given: Full ticker dict from WS ticker channel,
        When: parse_kraken_equities_ticker is called,
        Then: Returns TickerUpdate with mapped fields.
        """
        raw = {
            "symbol": "CLM6.NYMEX",
            "bid": 90.10,
            "bid_qty": 3,
            "ask": 90.13,
            "ask_qty": 4,
            "last": 90.11,
            "volume": 148427.0,
            "vwap": 91.2,
            "low": 88.7,
            "high": 94.81,
            "change": -3.05,
            "change_pct": -3.27,
        }
        result = parse_kraken_equities_ticker(raw)
        assert result.symbol == "CLM6-NYMEX"
        assert result.bid == pytest.approx(90.10)
        assert result.bid_qty == pytest.approx(3.0)
        assert result.ask == pytest.approx(90.13)
        assert result.ask_qty == pytest.approx(4.0)
        assert result.last == pytest.approx(90.11)
        assert result.volume == pytest.approx(148427.0)
        assert result.vwap == pytest.approx(91.2)
        assert result.low == pytest.approx(88.7)
        assert result.high == pytest.approx(94.81)
        assert result.change == pytest.approx(-3.05)
        assert result.change_pct == pytest.approx(-3.27)

    def test_parse_minimal_ticker(self) -> None:
        """Parse ticker with mostly None fields.

        Given: Ticker dict with only symbol,
        When: parse_kraken_equities_ticker is called,
        Then: Returns TickerUpdate with zeroed optional fields.
        """
        raw = {"symbol": "CLM6.NYMEX"}
        result = parse_kraken_equities_ticker(raw)
        assert result.symbol == "CLM6-NYMEX"
        assert result.bid == pytest.approx(0.0)
        assert result.ask == pytest.approx(0.0)
        assert result.last == pytest.approx(0.0)
        assert result.volume == pytest.approx(0.0)
        assert result.vwap == pytest.approx(0.0)
        assert result.low == pytest.approx(0.0)
        assert result.high == pytest.approx(0.0)
        assert result.change == pytest.approx(0.0)
        assert result.change_pct == pytest.approx(0.0)


class TestParseKrakenEquitiesTickerList:
    """Tests for parse_kraken_equities_ticker_list."""

    def test_parse_list_with_valid_items(self) -> None:
        """Parse list of valid tickers.

        Given: List with two valid ticker dicts,
        When: parse_kraken_equities_ticker_list is called,
        Then: Returns list of two TickerUpdate objects.
        """
        data = [
            {"symbol": "CLM6.NYMEX", "last": 90.11},
            {"symbol": "GCQ6.COMEX", "last": 2350.50},
        ]
        results = parse_kraken_equities_ticker_list(data)
        assert len(results) == 2

    def test_skip_unparseable_items(self) -> None:
        """Skip items that fail to parse.

        Given: List with one valid and one unmapped ticker,
        When: parse_kraken_equities_ticker_list is called,
        Then: Returns only the successfully parsed item.
        """
        data = [
            {"symbol": "CLM6.NYMEX", "last": 90.11},
            {"symbol": "UNMAPPED_SYMBOL", "last": 100.0},
        ]
        results = parse_kraken_equities_ticker_list(data)
        assert len(results) == 1
        assert results[0].symbol == "CLM6-NYMEX"


class TestParseKrakenEquitiesTrade:
    """Tests for parse_kraken_equities_trade."""

    def test_parse_buy_trade(self) -> None:
        """Parse a buy trade from WS.

        Given: Trade dict with side=buy and ISO timestamp,
        When: parse_kraken_equities_trade is called,
        Then: Returns TradeUpdate with correct fields.
        """
        raw = {
            "symbol": "CLM6.NYMEX",
            "side": "buy",
            "price": 90.12,
            "qty": 1,
            "timestamp": "2026-04-01T17:40:36.368Z",
            "sequence": 95579,
            "index": 7623847138430121307,
        }
        result = parse_kraken_equities_trade(raw)
        assert result.symbol == "CLM6-NYMEX"
        assert result.price == pytest.approx(90.12)
        assert result.quantity == pytest.approx(1.0)
        assert result.side == "buy"
        assert result.trade_id == str(7623847138430121307)
        assert result.timestamp.year == 2026

    def test_parse_sell_trade(self) -> None:
        """Parse a sell trade from WS.

        Given: Trade dict with side=sell,
        When: parse_kraken_equities_trade is called,
        Then: Returns TradeUpdate with side=sell.
        """
        raw = {
            "symbol": "GCQ6.COMEX",
            "side": "sell",
            "price": 2350.50,
            "qty": 5,
            "timestamp": "2026-04-01T18:00:00.000Z",
            "sequence": 10001,
            "index": 123456789,
        }
        result = parse_kraken_equities_trade(raw)
        assert result.symbol == "GCQ6-COMEX"
        assert result.side == "sell"

    def test_parse_undefined_side_defaults_to_buy(self) -> None:
        """Parse a trade with undefined side, defaulting to buy.

        Given: Trade dict with side=undefined,
        When: parse_kraken_equities_trade is called,
        Then: Returns TradeUpdate with side=buy (fallback).
        """
        raw = {
            "symbol": "CLM6.NYMEX",
            "side": "undefined",
            "price": 90.00,
            "qty": 2,
            "timestamp": "2026-04-01T17:41:00.000Z",
            "sequence": 95580,
            "index": 7623847138430121308,
        }
        result = parse_kraken_equities_trade(raw)
        assert result.side == "buy"


class TestParseKrakenEquitiesTradeList:
    """Tests for parse_kraken_equities_trade_list."""

    def test_parse_list_with_valid_items(self) -> None:
        """Parse list of valid trades.

        Given: List with one valid trade,
        When: parse_kraken_equities_trade_list is called,
        Then: Returns list with one TradeUpdate.
        """
        data = [
            {
                "symbol": "CLM6.NYMEX",
                "side": "buy",
                "price": 90.12,
                "qty": 1,
                "timestamp": "2026-04-01T17:40:36.368Z",
                "sequence": 95579,
                "index": 7623847138430121307,
            }
        ]
        results = parse_kraken_equities_trade_list(data)
        assert len(results) == 1

    def test_skip_unparseable_items(self) -> None:
        """Skip trades that fail to parse.

        Given: List with one valid and one invalid trade,
        When: parse_kraken_equities_trade_list is called,
        Then: Returns only the valid trade.
        """
        data = [
            {
                "symbol": "CLM6.NYMEX",
                "side": "buy",
                "price": 90.12,
                "qty": 1,
                "timestamp": "2026-04-01T17:40:36.368Z",
                "sequence": 95579,
                "index": 7623847138430121307,
            },
            {"invalid": "data"},
        ]
        results = parse_kraken_equities_trade_list(data)
        assert len(results) == 1


class TestParseKrakenEquitiesInstrument:
    """Tests for parse_kraken_equities_instrument."""

    def test_parse_active_instrument(self) -> None:
        """Parse an active tradable instrument.

        Given: Full instrument dict for CLM6.NYMEX,
        When: parse_kraken_equities_instrument is called,
        Then: Returns InstrumentPairDescriptor with correct fields.
        """
        raw = {
            "symbol": "CLM6.NYMEX",
            "name": "CLM6 19May26",
            "short_name": "Crude Oil",
            "contract_name": "CLM6",
            "tradable": True,
            "status": "active",
            "instrument_status": "active",
            "category": "Energies",
            "exchange": "NYMEX",
            "maturity": 1766172600,
            "contract_size": "1000",
            "tick_size": "0.01",
            "tick_value": "10",
            "base": "USD",
            "quote": "USD",
            "initial_margin": "4646.02",
        }
        result = parse_kraken_equities_instrument(raw)
        assert result.symbol == "CLM6.NYMEX"
        assert result.base == "USD"
        assert result.quote == "USD"
        assert result.status == "active"
        assert result.marginable is True
        assert result.has_index is False
        assert result.margin_initial == pytest.approx(4646.02)
        assert result.tick_size == pytest.approx(0.01)
        assert result.price_precision == 2
        assert result.qty_increment == pytest.approx(1000.0)
        assert result.qty_min == pytest.approx(1000.0)

    def test_parse_inactive_instrument(self) -> None:
        """Parse an inactive instrument.

        Given: Instrument with tradable=False,
        When: parse_kraken_equities_instrument is called,
        Then: Returns descriptor with status=inactive.
        """
        raw = {
            "symbol": "CLK5.NYMEX",
            "tradable": False,
            "status": "inactive",
            "contract_size": "1000",
            "tick_size": "0.01",
        }
        result = parse_kraken_equities_instrument(raw)
        assert result.status == "inactive"

    def test_parse_instrument_active_but_not_tradable(self) -> None:
        """Parse instrument with active status but tradable=False.

        Given: Instrument with status=active but tradable=False,
        When: parse_kraken_equities_instrument is called,
        Then: Returns descriptor with status=inactive.
        """
        raw = {
            "symbol": "CLK5.NYMEX",
            "tradable": False,
            "status": "active",
            "contract_size": "1000",
            "tick_size": "0.01",
        }
        result = parse_kraken_equities_instrument(raw)
        assert result.status == "inactive"


class TestParseKrakenEquitiesInstrumentList:
    """Tests for parse_kraken_equities_instrument_list."""

    def test_parse_list(self) -> None:
        """Parse list of instruments.

        Given: List with two valid instruments,
        When: parse_kraken_equities_instrument_list is called,
        Then: Returns list of InstrumentPairDescriptor objects.
        """
        data = [
            {
                "symbol": "CLM6.NYMEX",
                "tradable": True,
                "status": "active",
                "tick_size": "0.01",
                "contract_size": "1000",
            },
            {
                "symbol": "GCQ6.COMEX",
                "tradable": True,
                "status": "active",
                "tick_size": "0.10",
                "contract_size": "100",
            },
        ]
        results = parse_kraken_equities_instrument_list(data)
        assert len(results) == 2

    def test_skip_invalid(self) -> None:
        """Skip instruments that fail to parse.

        Given: List with one valid and one missing required fields,
        When: parse_kraken_equities_instrument_list is called,
        Then: Returns only the valid instrument.
        """
        data = [
            {
                "symbol": "CLM6.NYMEX",
                "tradable": True,
                "status": "active",
                "tick_size": "0.01",
                "contract_size": "1000",
            },
            {"invalid": "data"},
        ]
        results = parse_kraken_equities_instrument_list(data)
        assert len(results) == 1


class TestTickSizeToPrecision:
    """Tests for _tick_size_to_precision helper."""

    def test_two_decimals(self) -> None:
        """Tick size of 0.01 yields precision 2.

        Given: tick_size=0.01,
        When: _tick_size_to_precision is called,
        Then: Returns 2.
        """
        assert _tick_size_to_precision(0.01) == 2

    def test_quarter_step(self) -> None:
        """Tick size of 0.25 yields precision 2.

        Given: tick_size=0.25,
        When: _tick_size_to_precision is called,
        Then: Returns 2.
        """
        assert _tick_size_to_precision(0.25) == 2

    def test_whole_number(self) -> None:
        """Tick size of 1.0 yields precision 0.

        Given: tick_size=1.0,
        When: _tick_size_to_precision is called,
        Then: Returns 0.
        """
        assert _tick_size_to_precision(1.0) == 0

    def test_three_decimals(self) -> None:
        """Tick size of 0.001 yields precision 3.

        Given: tick_size=0.001,
        When: _tick_size_to_precision is called,
        Then: Returns 3.
        """
        assert _tick_size_to_precision(0.001) == 3
