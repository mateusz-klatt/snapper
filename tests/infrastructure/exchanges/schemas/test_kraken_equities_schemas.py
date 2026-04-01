"""Tests for Kraken Equities (FCM Futures) exchange schemas."""

import pytest
from pydantic import ValidationError

from snapper.infrastructure.exchanges.schemas.kraken_equities import KrakenEquitiesInstrumentSchema
from snapper.infrastructure.exchanges.schemas.kraken_equities import KrakenEquitiesTickerSchema
from snapper.infrastructure.exchanges.schemas.kraken_equities import KrakenEquitiesTradeSchema


class TestKrakenEquitiesInstrumentSchema:
    """Tests for FCM futures contract instrument schema."""

    def test_parse_full_instrument(self) -> None:
        """Parse a complete FCM futures contract from REST API.

        Given: Raw instrument dict for CLM6.NYMEX with all fields,
        When: Parsed via KrakenEquitiesInstrumentSchema,
        Then: All fields correctly extracted including margin data.
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
            "maturity_type": "monthly",
            "contract_size": "1000",
            "tick_size": "0.01",
            "tick_value": "10",
            "base": "USD",
            "quote": "USD",
            "intraday_margin": "1000",
            "initial_margin": "4646.02",
            "maintenance_margin": "4223.65",
            "delayed": True,
        }
        schema = KrakenEquitiesInstrumentSchema.model_validate(raw)
        assert schema.symbol == "CLM6.NYMEX"
        assert schema.name == "CLM6 19May26"
        assert schema.short_name == "Crude Oil"
        assert schema.contract_name == "CLM6"
        assert schema.tradable is True
        assert schema.status == "active"
        assert schema.instrument_status == "active"
        assert schema.category == "Energies"
        assert schema.exchange == "NYMEX"
        assert schema.maturity == 1766172600
        assert schema.maturity_type == "monthly"
        assert schema.contract_size == "1000"
        assert schema.tick_size == "0.01"
        assert schema.tick_value == "10"
        assert schema.base == "USD"
        assert schema.quote == "USD"
        assert schema.intraday_margin == "1000"
        assert schema.initial_margin == "4646.02"
        assert schema.maintenance_margin == "4223.65"
        assert schema.delayed is True

    def test_parse_minimal_instrument(self) -> None:
        """Parse instrument with only required field (symbol).

        Given: Instrument dict with only symbol,
        When: Parsed via KrakenEquitiesInstrumentSchema,
        Then: Optional fields default to empty strings, zeros, or False.
        """
        raw = {"symbol": "GCQ6.COMEX"}
        schema = KrakenEquitiesInstrumentSchema.model_validate(raw)
        assert schema.symbol == "GCQ6.COMEX"
        assert schema.name == ""
        assert schema.short_name == ""
        assert schema.contract_name == ""
        assert schema.tradable is False
        assert schema.status == ""
        assert schema.category == ""
        assert schema.exchange == ""
        assert schema.maturity is None
        assert schema.tick_size == "0"
        assert schema.delayed is True

    def test_extra_fields_allowed(self) -> None:
        """Allow extra fields from API without breaking parsing.

        Given: Instrument dict with an unknown field,
        When: Parsed via KrakenEquitiesInstrumentSchema,
        Then: Parsing succeeds (ExchangeResponse allows extra fields).
        """
        raw = {
            "symbol": "ESM6.CME",
            "unknownNewField": "some_value",
            "another_extra": 42,
        }
        schema = KrakenEquitiesInstrumentSchema.model_validate(raw)
        assert schema.symbol == "ESM6.CME"

    def test_required_symbol_missing_raises(self) -> None:
        """Reject instrument without required symbol field.

        Given: Instrument dict without symbol key,
        When: Parsed via KrakenEquitiesInstrumentSchema,
        Then: Raises ValidationError.
        """
        raw = {"name": "CLM6 19May26", "tradable": True}
        with pytest.raises(ValidationError):
            KrakenEquitiesInstrumentSchema.model_validate(raw)


class TestKrakenEquitiesTickerSchema:
    """Tests for Kraken Equities ticker schema."""

    def test_parse_full_ticker(self) -> None:
        """Parse a complete ticker with all fields.

        Given: Full ticker dict from WS ticker channel,
        When: Parsed via KrakenEquitiesTickerSchema,
        Then: All fields correctly mapped.
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
            "open": 93.4,
            "change": -3.05,
            "change_pct": -3.27,
            "prev_day_close": 93.16,
            "prev_day_volume": 232944.0,
            "open_interest": 228090,
            "is_extended_hours": False,
        }
        schema = KrakenEquitiesTickerSchema.model_validate(raw)
        assert schema.symbol == "CLM6.NYMEX"
        assert schema.bid == pytest.approx(90.10)
        assert schema.bid_qty == pytest.approx(3.0)
        assert schema.ask == pytest.approx(90.13)
        assert schema.ask_qty == pytest.approx(4.0)
        assert schema.last == pytest.approx(90.11)
        assert schema.volume == pytest.approx(148427.0)
        assert schema.vwap == pytest.approx(91.2)
        assert schema.low == pytest.approx(88.7)
        assert schema.high == pytest.approx(94.81)
        assert schema.open == pytest.approx(93.4)
        assert schema.change == pytest.approx(-3.05)
        assert schema.change_pct == pytest.approx(-3.27)
        assert schema.prev_day_close == pytest.approx(93.16)
        assert schema.prev_day_volume == pytest.approx(232944.0)
        assert schema.open_interest == pytest.approx(228090.0)
        assert schema.is_extended_hours is False

    def test_parse_minimal_ticker(self) -> None:
        """Parse ticker with only symbol (all optional fields None).

        Given: Ticker dict with only required symbol field,
        When: Parsed via KrakenEquitiesTickerSchema,
        Then: Optional fields default to None.
        """
        raw = {"symbol": "GCQ6.COMEX"}
        schema = KrakenEquitiesTickerSchema.model_validate(raw)
        assert schema.symbol == "GCQ6.COMEX"
        assert schema.bid is None
        assert schema.ask is None
        assert schema.last is None
        assert schema.volume is None
        assert schema.vwap is None
        assert schema.open_interest is None
        assert schema.is_extended_hours is None

    def test_close_field_present(self) -> None:
        """Parse ticker with close field (present in snapshots).

        Given: Ticker dict with close field,
        When: Parsed via KrakenEquitiesTickerSchema,
        Then: Close field is correctly parsed.
        """
        raw = {"symbol": "CLM6.NYMEX", "close": 93.16}
        schema = KrakenEquitiesTickerSchema.model_validate(raw)
        assert schema.close == pytest.approx(93.16)


class TestKrakenEquitiesTradeSchema:
    """Tests for Kraken Equities trade schema."""

    def test_parse_buy_trade(self) -> None:
        """Parse a buy trade execution.

        Given: Raw trade dict with side=buy,
        When: Parsed via KrakenEquitiesTradeSchema,
        Then: All fields correctly extracted.
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
        schema = KrakenEquitiesTradeSchema.model_validate(raw)
        assert schema.symbol == "CLM6.NYMEX"
        assert schema.side == "buy"
        assert schema.price == pytest.approx(90.12)
        assert schema.qty == pytest.approx(1.0)
        assert schema.timestamp == "2026-04-01T17:40:36.368Z"
        assert schema.sequence == 95579
        assert schema.index == 7623847138430121307

    def test_parse_sell_trade(self) -> None:
        """Parse a sell trade execution.

        Given: Raw trade dict with side=sell,
        When: Parsed via KrakenEquitiesTradeSchema,
        Then: Side is correctly parsed as sell.
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
        schema = KrakenEquitiesTradeSchema.model_validate(raw)
        assert schema.side == "sell"
        assert schema.price == pytest.approx(2350.50)

    def test_parse_undefined_side_trade(self) -> None:
        """Parse a trade with undefined side.

        Given: Raw trade dict with side=undefined,
        When: Parsed via KrakenEquitiesTradeSchema,
        Then: Side is accepted as-is (no validation on side enum).
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
        schema = KrakenEquitiesTradeSchema.model_validate(raw)
        assert schema.side == "undefined"

    def test_missing_required_field_raises(self) -> None:
        """Reject trade missing required fields.

        Given: Trade dict without price field,
        When: Parsed via KrakenEquitiesTradeSchema,
        Then: Raises ValidationError.
        """
        raw = {
            "symbol": "CLM6.NYMEX",
            "side": "buy",
            "qty": 1,
            "timestamp": "2026-04-01T17:40:36.368Z",
            "sequence": 95579,
            "index": 7623847138430121307,
        }
        with pytest.raises(ValidationError):
            KrakenEquitiesTradeSchema.model_validate(raw)
