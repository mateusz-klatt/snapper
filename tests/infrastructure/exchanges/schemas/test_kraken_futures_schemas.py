"""Tests for Kraken Futures exchange schemas."""

import pytest
from pydantic import ValidationError

from snapper.infrastructure.exchanges.schemas.kraken_futures import KrakenFuturesFillSchema
from snapper.infrastructure.exchanges.schemas.kraken_futures import KrakenFuturesInstrumentSchema
from snapper.infrastructure.exchanges.schemas.kraken_futures import KrakenFuturesMarginLevelSchema
from snapper.infrastructure.exchanges.schemas.kraken_futures import KrakenFuturesOpenOrderSchema
from snapper.infrastructure.exchanges.schemas.kraken_futures import KrakenFuturesTickerEventSchema
from snapper.infrastructure.exchanges.schemas.kraken_futures import KrakenFuturesTickerSchema
from snapper.infrastructure.exchanges.schemas.kraken_futures import KrakenFuturesTradeEventSchema
from snapper.infrastructure.exchanges.schemas.kraken_futures import KrakenFuturesTradeSchema


class TestKrakenFuturesMarginLevelSchema:
    """Tests for margin level schema."""

    def test_parse_margin_level(self) -> None:
        """Parse a margin level entry from API response.

        Given: Raw margin level dict with camelCase keys,
        When: Parsed via KrakenFuturesMarginLevelSchema,
        Then: Fields are correctly mapped to snake_case attributes.
        """
        raw = {"contracts": 500000, "initialMargin": 0.04, "maintenanceMargin": 0.02}
        schema = KrakenFuturesMarginLevelSchema.model_validate(raw)
        assert schema.contracts == 500000
        assert schema.initial_margin == pytest.approx(0.04)
        assert schema.maintenance_margin == pytest.approx(0.02)


class TestKrakenFuturesInstrumentSchema:
    """Tests for instrument metadata schema."""

    def test_parse_perpetual_inverse(self) -> None:
        """Parse a perpetual inverse futures instrument.

        Given: Raw instrument dict from get_instruments() for PI_XBTUSD,
        When: Parsed via KrakenFuturesInstrumentSchema,
        Then: All fields correctly extracted including nested margin levels.
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
            "openingDate": "2018-08-31T00:00:00Z",
            "marginLevels": [{"contracts": 0, "initialMargin": 0.02, "maintenanceMargin": 0.01}],
            "fundingRateCoefficient": 24,
            "maxRelativeFundingRate": 0.0025,
            "isin": "GB00J62YGL67",
            "impactMidSize": 1000.0,
            "maxPositionSize": 75000000.0,
            "contractValueTradePrecision": 0,
            "postOnly": False,
            "feeScheduleUid": "a6cbc326-9477-4a6c-911a-d4cb3ed7481e",
            "mtf": True,
            "category": "",
            "tags": [],
            "tradfi": False,
        }
        schema = KrakenFuturesInstrumentSchema.model_validate(raw)
        assert schema.symbol == "PI_XBTUSD"
        assert schema.type == "futures_inverse"
        assert schema.underlying == "rr_xbtusd"
        assert schema.tick_size == pytest.approx(0.5)
        assert schema.contract_size == 1
        assert schema.tradeable is True
        assert schema.base == "BTC"
        assert schema.quote == "USD"
        assert schema.pair == "BTC:USD"
        assert schema.opening_date == "2018-08-31T00:00:00Z"
        assert len(schema.margin_levels) == 1
        assert schema.margin_levels[0].initial_margin == pytest.approx(0.02)
        assert schema.funding_rate_coefficient == 24
        assert schema.max_relative_funding_rate == pytest.approx(0.0025)
        assert schema.isin == "GB00J62YGL67"
        assert schema.tradfi is False

    def test_parse_minimal_instrument(self) -> None:
        """Parse instrument with only required fields.

        Given: Instrument dict with minimal fields,
        When: Parsed via KrakenFuturesInstrumentSchema,
        Then: Optional fields default to None or empty.
        """
        raw = {
            "symbol": "FF_XBTUSD_250627",
            "type": "futures_vanilla",
            "tickSize": 1.0,
            "contractSize": 0.001,
            "tradeable": False,
        }
        schema = KrakenFuturesInstrumentSchema.model_validate(raw)
        assert schema.symbol == "FF_XBTUSD_250627"
        assert schema.base is None
        assert schema.quote is None
        assert schema.underlying is None
        assert schema.margin_levels == []
        assert schema.funding_rate_coefficient is None
        assert schema.tradfi is False
        assert schema.tags == []

    def test_extra_fields_allowed(self) -> None:
        """Allow extra fields from API without breaking parsing.

        Given: Instrument dict with an unknown field,
        When: Parsed via KrakenFuturesInstrumentSchema,
        Then: Parsing succeeds (ExchangeResponse allows extra fields).
        """
        raw = {
            "symbol": "PI_ETHUSD",
            "type": "futures_inverse",
            "tickSize": 0.05,
            "contractSize": 1,
            "tradeable": True,
            "unknownNewField": "some_value",
        }
        schema = KrakenFuturesInstrumentSchema.model_validate(raw)
        assert schema.symbol == "PI_ETHUSD"


class TestKrakenFuturesTickerSchema:
    """Tests for REST ticker schema."""

    def test_parse_full_ticker(self) -> None:
        """Parse a complete ticker response.

        Given: Full ticker dict from get_tickers() for PF_XBTUSD,
        When: Parsed via KrakenFuturesTickerSchema,
        Then: All fields correctly mapped.
        """
        raw = {
            "symbol": "PF_XBTUSD",
            "last": 66621.0,
            "lastTime": "2026-03-31T12:18:26.265568Z",
            "lastSize": 10.0,
            "tag": "perpetual",
            "pair": "BTC:USD",
            "markPrice": 66500.5,
            "bid": 66500.0,
            "bidSize": 50.0,
            "ask": 66510.0,
            "askSize": 30.0,
            "vol24h": 12345.0,
            "volumeQuote": 820000000.0,
            "openInterest": 500000.0,
            "open24h": 66000.0,
            "high24h": 67000.0,
            "low24h": 65500.0,
            "fundingRate": -0.0004,
            "fundingRatePrediction": -0.0005,
            "indexPrice": 66505.0,
            "suspended": False,
            "postOnly": False,
            "change24h": 0.94,
        }
        schema = KrakenFuturesTickerSchema.model_validate(raw)
        assert schema.symbol == "PF_XBTUSD"
        assert schema.last == pytest.approx(66621.0)
        assert schema.mark_price == pytest.approx(66500.5)
        assert schema.bid == pytest.approx(66500.0)
        assert schema.ask == pytest.approx(66510.0)
        assert schema.funding_rate == pytest.approx(-0.0004)
        assert schema.index_price == pytest.approx(66505.0)
        assert schema.suspended is False

    def test_parse_minimal_ticker(self) -> None:
        """Parse ticker with only symbol (all optional fields None).

        Given: Ticker dict with only required symbol field,
        When: Parsed via KrakenFuturesTickerSchema,
        Then: Optional fields default to None.
        """
        raw = {"symbol": "PI_ETHUSD"}
        schema = KrakenFuturesTickerSchema.model_validate(raw)
        assert schema.symbol == "PI_ETHUSD"
        assert schema.last is None
        assert schema.bid is None
        assert schema.funding_rate is None


class TestKrakenFuturesTradeSchema:
    """Tests for trade schema."""

    def test_parse_rest_trade(self) -> None:
        """Parse an individual trade from REST trade history.

        Given: Raw trade dict from get_trade_history() with ISO time and qty,
        When: Parsed via KrakenFuturesTradeSchema,
        Then: All fields correctly extracted, qty mapped to size.
        """
        raw = {
            "time": "2026-03-31T11:32:44.255Z",
            "trade_id": 100,
            "price": 66621.0,
            "qty": 10.0,
            "side": "sell",
            "type": "fill",
            "uid": "b3416619-01a6-4593-8aab-faa496aa8d72",
        }
        schema = KrakenFuturesTradeSchema.model_validate(raw)
        assert schema.time == "2026-03-31T11:32:44.255Z"
        assert schema.trade_id == 100
        assert schema.price == pytest.approx(66621.0)
        assert schema.size == pytest.approx(10.0)
        assert schema.side == "sell"
        assert schema.type == "fill"
        assert schema.uid == "b3416619-01a6-4593-8aab-faa496aa8d72"

    def test_parse_ws_trade(self) -> None:
        """Parse a trade from WebSocket trade feed.

        Given: WS trade with integer millisecond time and qty field,
        When: Parsed via KrakenFuturesTradeSchema,
        Then: Fields correctly extracted, time accepted as int, qty mapped to size.
        """
        raw = {
            "uid": "b3416619-01a6-4593-8aab-faa496aa8d72",
            "side": "buy",
            "time": 1640995200123,
            "qty": 0.5,
            "price": 50000.0,
            "seq": 42,
        }
        schema = KrakenFuturesTradeSchema.model_validate(raw)
        assert schema.time == 1640995200123
        assert schema.size == pytest.approx(0.5)
        assert schema.seq == 42
        assert schema.side == "buy"

    def test_parse_minimal_trade(self) -> None:
        """Parse trade with only required fields.

        Given: Trade dict with time, price, qty, side only,
        When: Parsed via KrakenFuturesTradeSchema,
        Then: Optional fields default to None.
        """
        raw = {"time": 1640995200000, "price": 50000.0, "qty": 5.0, "side": "buy"}
        schema = KrakenFuturesTradeSchema.model_validate(raw)
        assert schema.trade_id is None
        assert schema.uid is None
        assert schema.seq is None

    def test_invalid_side_rejected(self) -> None:
        """Reject trade with invalid side value.

        Given: Trade dict with side not in buy/sell,
        When: Parsed via KrakenFuturesTradeSchema,
        Then: Raises ValidationError.
        """
        raw = {"time": 1640995200000, "price": 50000.0, "qty": 5.0, "side": "hold"}
        with pytest.raises(ValidationError):
            KrakenFuturesTradeSchema.model_validate(raw)


class TestKrakenFuturesTradeEventSchema:
    """Tests for WS trade event wrapper schema."""

    def test_parse_trade_event(self) -> None:
        """Parse WS trade feed message with nested trades.

        Given: WS trade event dict with product_id and trades list,
        When: Parsed via KrakenFuturesTradeEventSchema,
        Then: Feed, product_id, and trades correctly extracted.
        """
        raw = {
            "feed": "trade",
            "product_id": "PI_XBTUSD",
            "trades": [{"time": 1640995200000, "price": 66621.0, "qty": 10.0, "side": "sell"}],
        }
        schema = KrakenFuturesTradeEventSchema.model_validate(raw)
        assert schema.feed == "trade"
        assert schema.product_id == "PI_XBTUSD"
        assert len(schema.trades) == 1
        assert schema.trades[0].price == pytest.approx(66621.0)

    def test_empty_trades_list(self) -> None:
        """Parse trade event with no trades.

        Given: WS trade event with empty trades list,
        When: Parsed via KrakenFuturesTradeEventSchema,
        Then: Trades list is empty.
        """
        raw = {"feed": "trade", "product_id": "PI_XBTUSD", "trades": []}
        schema = KrakenFuturesTradeEventSchema.model_validate(raw)
        assert schema.trades == []


class TestKrakenFuturesTickerEventSchema:
    """Tests for WS ticker event wrapper schema."""

    def test_parse_ticker_event(self) -> None:
        """Parse WS ticker feed message.

        Given: WS ticker event dict with product_id and price fields,
        When: Parsed via KrakenFuturesTickerEventSchema,
        Then: Feed, product_id, and price fields correctly extracted.
        """
        raw = {
            "feed": "ticker",
            "product_id": "PF_XBTUSD",
            "bid": 66500.0,
            "ask": 66510.0,
            "last": 66505.0,
            "mark_price": 66502.0,
            "volume": 12345.0,
            "time": 1711882706.265,
        }
        schema = KrakenFuturesTickerEventSchema.model_validate(raw)
        assert schema.feed == "ticker"
        assert schema.product_id == "PF_XBTUSD"
        assert schema.bid == pytest.approx(66500.0)
        assert schema.mark_price == pytest.approx(66502.0)

    def test_parse_ticker_event_with_camel_case_aliases(self) -> None:
        """Parse WS ticker event using camelCase field names from API.

        Given: WS ticker event with camelCase keys (markPrice, openInterest),
        When: Parsed via KrakenFuturesTickerEventSchema,
        Then: Aliased fields correctly mapped to snake_case attributes.
        """
        raw = {
            "feed": "ticker",
            "product_id": "PF_XBTUSD",
            "markPrice": 66500.5,
            "openInterest": 50000.0,
            "indexPrice": 66505.0,
            "volumeQuote": 820000000.0,
        }
        schema = KrakenFuturesTickerEventSchema.model_validate(raw)
        assert schema.mark_price == pytest.approx(66500.5)
        assert schema.open_interest == pytest.approx(50000.0)
        assert schema.index_price == pytest.approx(66505.0)
        assert schema.volume_quote == pytest.approx(820000000.0)

    def test_parse_ticker_lite_event(self) -> None:
        """Parse WS ticker_lite feed message.

        Given: WS ticker_lite event,
        When: Parsed via KrakenFuturesTickerEventSchema,
        Then: Feed correctly parsed as ticker_lite.
        """
        raw = {"feed": "ticker_lite", "product_id": "PI_ETHUSD", "last": 3500.0}
        schema = KrakenFuturesTickerEventSchema.model_validate(raw)
        assert schema.feed == "ticker_lite"
        assert schema.last == pytest.approx(3500.0)

    def test_minimal_ticker_event(self) -> None:
        """Parse ticker event with only required fields.

        Given: Ticker event with only feed and product_id,
        When: Parsed via KrakenFuturesTickerEventSchema,
        Then: All optional fields default to None.
        """
        raw = {"feed": "ticker", "product_id": "PF_SOLUSD"}
        schema = KrakenFuturesTickerEventSchema.model_validate(raw)
        assert schema.bid is None
        assert schema.ask is None
        assert schema.volume is None


class TestKrakenFuturesFillSchema:
    """Tests for private fill event schema."""

    def test_parse_full_fill(self) -> None:
        """Parse a complete fill event from the fills WS channel.

        Given: Full fill dict with all fields from WS payload,
        When: Parsed via KrakenFuturesFillSchema,
        Then: All fields correctly extracted.
        """
        raw = {
            "fill_id": "fill-001",
            "order_id": "order-001",
            "instrument": "PF_XBTUSD",
            "buy": True,
            "qty": 5.0,
            "price": 66500.0,
            "time": 1743676200000,
            "seq": 42,
            "remaining_order_qty": 5.0,
            "fill_type": "taker",
            "taker_order_type": "lmt",
            "order_type": "lmt",
            "fee_paid": 1.25,
            "fee_currency": "USD",
            "cli_ord_id": "my-order-1",
        }
        schema = KrakenFuturesFillSchema.model_validate(raw)
        assert schema.fill_id == "fill-001"
        assert schema.order_id == "order-001"
        assert schema.instrument == "PF_XBTUSD"
        assert schema.buy is True
        assert schema.qty == pytest.approx(5.0)
        assert schema.price == pytest.approx(66500.0)
        assert schema.time == 1743676200000
        assert schema.seq == 42
        assert schema.remaining_order_qty == pytest.approx(5.0)
        assert schema.fill_type == "taker"
        assert schema.taker_order_type == "lmt"
        assert schema.order_type == "lmt"
        assert schema.fee_paid == pytest.approx(1.25)
        assert schema.fee_currency == "USD"
        assert schema.cli_ord_id == "my-order-1"

    def test_parse_minimal_fill(self) -> None:
        """Parse fill with only required fields.

        Given: Fill dict with only required fields,
        When: Parsed via KrakenFuturesFillSchema,
        Then: Optional fields default to None.
        """
        raw = {
            "fill_id": "fill-002",
            "order_id": "order-002",
            "instrument": "PF_ETHUSD",
            "buy": False,
            "qty": 10.0,
            "price": 3500.0,
            "time": 1743678000000,
        }
        schema = KrakenFuturesFillSchema.model_validate(raw)
        assert schema.buy is False
        assert schema.seq is None
        assert schema.remaining_order_qty is None
        assert schema.fill_type is None
        assert schema.fee_paid is None
        assert schema.fee_currency is None
        assert schema.taker_order_type is None
        assert schema.order_type is None
        assert schema.cli_ord_id is None

    def test_missing_required_instrument_rejected(self) -> None:
        """Reject fill with missing required instrument field.

        Given: Fill dict without instrument key,
        When: Parsed via KrakenFuturesFillSchema,
        Then: Raises ValidationError.
        """
        raw = {
            "fill_id": "fill-003",
            "order_id": "order-003",
            "buy": True,
            "qty": 5.0,
            "price": 66500.0,
            "time": 1743676200000,
        }
        with pytest.raises(ValidationError):
            KrakenFuturesFillSchema.model_validate(raw)


class TestKrakenFuturesOpenOrderSchema:
    """Tests for private open order event schema."""

    def test_parse_full_order(self) -> None:
        """Parse a complete order update from open_orders WS channel.

        Given: Full order dict with WS payload fields,
        When: Parsed via KrakenFuturesOpenOrderSchema,
        Then: All fields correctly mapped.
        """
        raw = {
            "order_id": "order-001",
            "instrument": "PF_XBTUSD",
            "direction": 0,
            "type": "limit",
            "qty": 10.0,
            "filled": 3.0,
            "limit_price": 66000.0,
            "stop_price": 0.0,
            "time": 1743676200000,
            "last_update_time": 1743676500000,
            "reduce_only": False,
            "cli_ord_id": "my-order-1",
        }
        schema = KrakenFuturesOpenOrderSchema.model_validate(raw)
        assert schema.order_id == "order-001"
        assert schema.instrument == "PF_XBTUSD"
        assert schema.direction == 0
        assert schema.type == "limit"
        assert schema.qty == pytest.approx(10.0)
        assert schema.filled == pytest.approx(3.0)
        assert schema.limit_price == pytest.approx(66000.0)
        assert schema.stop_price == pytest.approx(0.0)
        assert schema.time == 1743676200000
        assert schema.last_update_time == 1743676500000
        assert schema.reduce_only is False
        assert schema.cli_ord_id == "my-order-1"

    def test_parse_minimal_order(self) -> None:
        """Parse order with only required fields.

        Given: Order dict with minimal fields,
        When: Parsed via KrakenFuturesOpenOrderSchema,
        Then: Optional fields default to their defaults.
        """
        raw = {
            "order_id": "order-002",
            "instrument": "PF_ETHUSD",
            "qty": 5.0,
            "time": 1743678000000,
        }
        schema = KrakenFuturesOpenOrderSchema.model_validate(raw)
        assert schema.filled == pytest.approx(0.0)
        assert schema.limit_price == pytest.approx(0.0)
        assert schema.stop_price == pytest.approx(0.0)
        assert schema.type == "limit"
        assert schema.direction == 0
        assert schema.reduce_only is False
        assert schema.last_update_time is None
        assert schema.cli_ord_id is None

    def test_sell_direction(self) -> None:
        """Parse order with sell direction.

        Given: Order dict with direction=1 (sell),
        When: Parsed via KrakenFuturesOpenOrderSchema,
        Then: Direction field is 1.
        """
        raw = {
            "order_id": "order-003",
            "instrument": "PF_XBTUSD",
            "direction": 1,
            "type": "stop",
            "qty": 10.0,
            "time": 1743676200000,
        }
        schema = KrakenFuturesOpenOrderSchema.model_validate(raw)
        assert schema.direction == 1
        assert schema.type == "stop"
