"""Tests for Kraken exchange Pydantic schemas."""

import pytest

from snapper.infrastructure.exchanges.schemas.kraken import KrakenAddOrderParamsSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenAddOrderResponseSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenAddOrderResultSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenCancelOrderParamsSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenCancelOrderResponseSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenCancelOrderResultSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenCandleSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenExecutionEventEnvelope
from snapper.infrastructure.exchanges.schemas.kraken import KrakenExecutionFeeSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenExecutionSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenExecutionSubscribeParamsSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenInstrumentAssetSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenInstrumentEventEnvelope
from snapper.infrastructure.exchanges.schemas.kraken import KrakenInstrumentFeeScheduleSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenInstrumentPairSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenInstrumentSnapshotSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenInstrumentSubscribeParamsSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenInstrumentSubscriptionAckSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenOhlcEventEnvelope
from snapper.infrastructure.exchanges.schemas.kraken import KrakenTickerEventEnvelope
from snapper.infrastructure.exchanges.schemas.kraken import KrakenTickerSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenTickerSubscribeParamsSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenTradeEventEnvelope
from snapper.infrastructure.exchanges.schemas.kraken import KrakenTradeSchema
from snapper.infrastructure.exchanges.schemas.kraken import KrakenTradeSubscribeParamsSchema
from snapper.infrastructure.exchanges.schemas.kraken import _empty_asset_list
from snapper.infrastructure.exchanges.schemas.kraken import _empty_pair_list


class TestDefaultFactoryFunctions:
    """Tests for Kraken schema default factory functions."""

    def test_empty_pair_list_returns_empty_list(self) -> None:
        """Empty pair list factory returns empty list.

        Given: No input,
        When: _empty_pair_list is called,
        Then: Returns an empty list.
        """
        result = _empty_pair_list()
        assert result == []
        assert isinstance(result, list)

    def test_empty_asset_list_returns_empty_list(self) -> None:
        """Empty asset list factory returns empty list.

        Given: No input,
        When: _empty_asset_list is called,
        Then: Returns an empty list.
        """
        result = _empty_asset_list()
        assert result == []
        assert isinstance(result, list)


class TestKrakenInstrumentSchemas:
    """Tests for Kraken instrument subscription schemas."""

    def test_instrument_subscribe_params_as_params(self) -> None:
        """Serialize instrument subscription parameters.

        Given: KrakenInstrumentSubscribeParamsSchema with snapshot=True,
        When: as_params is called,
        Then: Returns dict with channel and configuration.
        """
        params = KrakenInstrumentSubscribeParamsSchema(
            snapshot=True, include_tokenized_assets=False
        )
        result = params.as_params()
        assert result["channel"] == "instrument"
        assert result["snapshot"] is True
        assert result["include_tokenized_assets"] is False

    def test_instrument_subscription_ack_with_message(self) -> None:
        """Parse subscription acknowledgment with error message.

        Given: Subscription ack with error status and message,
        When: Schema is instantiated,
        Then: Message and request_id are accessible.
        """
        ack = KrakenInstrumentSubscriptionAckSchema(
            channel="instrument",
            event="subscribe",
            status="error",
            message="subscription failed",
            reqid=123,
        )
        assert ack.message == "subscription failed"
        assert ack.request_id == 123

    def test_instrument_asset_schema_optional_fields(self) -> None:
        """Parse asset schema with optional fields.

        Given: Asset data with decimals field,
        When: Schema is instantiated,
        Then: Optional fields are correctly parsed.
        """
        asset = KrakenInstrumentAssetSchema(
            asset="BTC",
            status="online",
            altname="XBT",
            decimals=8,
        )
        assert asset.asset == "BTC"
        assert asset.decimals == 8

    def test_instrument_fee_schedule_schema(self) -> None:
        """Parse fee schedule schema.

        Given: Fee schedule with type, percent, and symbol,
        When: Schema is instantiated,
        Then: All fields are correctly accessible.
        """
        fee = KrakenInstrumentFeeScheduleSchema(type="maker", percent=0.1, symbol="BTC/USD")
        assert fee.type == "maker"
        assert fee.percent == pytest.approx(0.1)

    def test_instrument_pair_schema_as_summary(self) -> None:
        """Export pair schema as summary dictionary.

        Given: Instrument pair with symbol and precision fields,
        When: as_summary is called,
        Then: Returns dictionary with key fields.
        """
        pair = KrakenInstrumentPairSchema(
            symbol="BTC/USD",
            status="online",
            baseCurrency="BTC",
            quoteCurrency="USD",
            qty_min=0.001,
            price_precision=2,
        )
        summary = pair.as_summary()
        assert summary["symbol"] == "BTC/USD"
        assert summary["status"] == "online"
        assert summary.get("baseCurrency") == "BTC" or summary.get("base_currency") == "BTC"

    def test_instrument_snapshot_iter_pairs(self) -> None:
        """Iterate over pairs in snapshot.

        Given: Snapshot containing two trading pairs,
        When: iter_pairs is called,
        Then: Returns iterator with both pairs.
        """
        pair1 = KrakenInstrumentPairSchema(symbol="BTC/USD")
        pair2 = KrakenInstrumentPairSchema(symbol="ETH/USD")
        snapshot = KrakenInstrumentSnapshotSchema(pairs=[pair1, pair2])
        pairs = list(snapshot.iter_pairs())
        assert len(pairs) == 2
        assert pairs[0].symbol == "BTC/USD"
        assert pairs[1].symbol == "ETH/USD"

    def test_instrument_event_envelope_iter_pairs_with_snapshot(self) -> None:
        """Extract pairs from envelope containing snapshot.

        Given: Event envelope with snapshot type data,
        When: iter_pairs is called,
        Then: Returns pairs from nested snapshot.
        """
        pair1 = KrakenInstrumentPairSchema(symbol="BTC/USD")
        snapshot = KrakenInstrumentSnapshotSchema(pairs=[pair1])
        envelope = KrakenInstrumentEventEnvelope(
            channel="instrument",
            type="snapshot",
            data=snapshot,
        )
        pairs = envelope.iter_pairs()
        assert len(pairs) == 1
        assert pairs[0].symbol == "BTC/USD"

    def test_instrument_event_envelope_iter_pairs_with_single_pair(self) -> None:
        """Extract pairs from envelope with single pair data.

        Given: Event envelope with single pair as data,
        When: iter_pairs is called,
        Then: Returns list with single pair.
        """
        pair = KrakenInstrumentPairSchema(symbol="ETH/USD")
        envelope = KrakenInstrumentEventEnvelope(
            channel="instrument",
            type="update",
            data=pair,
        )
        pairs = envelope.iter_pairs()
        assert len(pairs) == 1
        assert pairs[0].symbol == "ETH/USD"

    def test_instrument_event_envelope_iter_pairs_with_list(self) -> None:
        """Extract pairs from envelope with list data.

        Given: Event envelope with list of pairs as data,
        When: iter_pairs is called,
        Then: Returns all pairs from list.
        """
        pair1 = KrakenInstrumentPairSchema(symbol="BTC/USD")
        pair2 = KrakenInstrumentPairSchema(symbol="ETH/USD")
        envelope = KrakenInstrumentEventEnvelope(
            channel="instrument",
            type="update",
            data=[pair1, pair2],
        )
        pairs = envelope.iter_pairs()
        assert len(pairs) == 2

    def test_instrument_event_envelope_as_dicts(self) -> None:
        """Export envelope pairs as dictionaries.

        Given: Event envelope with pair data,
        When: as_dicts is called,
        Then: Returns list of dictionaries.
        """
        pair = KrakenInstrumentPairSchema(symbol="BTC/USD", status="online")
        envelope = KrakenInstrumentEventEnvelope(
            channel="instrument",
            type="update",
            data=pair,
        )
        dicts = envelope.as_dicts()
        assert len(dicts) == 1
        assert dicts[0]["symbol"] == "BTC/USD"


class TestKrakenTickerSchemas:
    """Tests for Kraken ticker subscription schemas."""

    def test_ticker_subscribe_params_as_params(self) -> None:
        """Serialize ticker subscription parameters.

        Given: Ticker subscribe params with symbols and snapshot flag,
        When: as_params is called,
        Then: Returns dict with channel and symbols.
        """
        params = KrakenTickerSubscribeParamsSchema(symbol=["BTC/USD", "ETH/USD"], snapshot=False)
        result = params.as_params()
        assert result["channel"] == "ticker"
        assert result["symbol"] == ["BTC/USD", "ETH/USD"]
        assert result["snapshot"] is False

    def test_ticker_schema_model_dump(self) -> None:
        """Export ticker schema as dictionary.

        Given: Ticker schema with bid/ask prices,
        When: model_dump is called with exclude_none,
        Then: Returns dictionary without None values.
        """
        ticker = KrakenTickerSchema(
            symbol="BTC/USD",
            bid=50000.0,
            bid_qty=1.5,
            ask=50100.0,
            ask_qty=2.0,
        )
        result = ticker.model_dump(exclude_none=True)
        assert result["symbol"] == "BTC/USD"
        assert result["bid"] == pytest.approx(50000.0)

    def test_ticker_event_envelope_with_dict_data(self) -> None:
        """Handle ticker envelope with raw dict data.

        Given: Ticker envelope with dictionary as data,
        When: Envelope is accessed,
        Then: Data is accessible as dictionary.
        """
        envelope = KrakenTickerEventEnvelope(
            channel="ticker",
            type="update",
            symbol="BTC/USD",
            data={"bid": 50000.0, "ask": 50100.0},
        )
        assert envelope.symbol == "BTC/USD"
        assert isinstance(envelope.data, dict)
        assert envelope.data.get("bid") == pytest.approx(50000.0)

    def test_ticker_event_envelope_with_ticker_schema(self) -> None:
        """Handle ticker envelope with schema data.

        Given: Ticker envelope with KrakenTickerSchema as data,
        When: Envelope is accessed,
        Then: Data is accessible as schema instance.
        """
        ticker = KrakenTickerSchema(symbol="BTC/USD", bid=50000.0)
        envelope = KrakenTickerEventEnvelope(
            channel="ticker",
            type="update",
            symbol="BTC/USD",
            data=ticker,
        )
        assert envelope.symbol == "BTC/USD"
        assert isinstance(envelope.data, KrakenTickerSchema)


class TestKrakenOhlcSchemas:
    """Tests for Kraken OHLC subscription schemas."""

    def test_ohlc_event_envelope_primary_symbol(self) -> None:
        """Extract primary symbol from OHLC envelope.

        Given: OHLC envelope with candle data,
        When: primary_symbol is called,
        Then: Returns first candle's symbol.
        """
        candle = KrakenCandleSchema(
            symbol="BTC/USD",
            open=50000.0,
            high=51000.0,
            low=49000.0,
            close=50500.0,
            vwap=50250.0,
            volume=100.0,
            trades=500,
            interval=5,
        )
        envelope = KrakenOhlcEventEnvelope(
            channel="ohlc",
            type="update",
            data=[candle],
        )
        assert envelope.primary_symbol() == "BTC/USD"

    def test_ohlc_event_envelope_primary_symbol_none(self) -> None:
        """Handle empty OHLC envelope.

        Given: OHLC envelope with empty data list,
        When: primary_symbol is called,
        Then: Returns None.
        """
        envelope = KrakenOhlcEventEnvelope(
            channel="ohlc",
            type="update",
            data=[],
        )
        assert envelope.primary_symbol() is None

    def test_ohlc_event_envelope_primary_symbol_first_none(self) -> None:
        """Skip candles with None symbol.

        Given: OHLC envelope where first candle has no symbol,
        When: primary_symbol is called,
        Then: Returns first non-None symbol.
        """
        candle_no_symbol = KrakenCandleSchema(
            symbol=None,
            open=50000.0,
            high=51000.0,
            low=49000.0,
            close=50500.0,
        )
        candle_with_symbol = KrakenCandleSchema(
            symbol="BTC/USD",
            open=50100.0,
            high=51100.0,
            low=49100.0,
            close=50600.0,
        )
        envelope = KrakenOhlcEventEnvelope(
            channel="ohlc",
            type="update",
            data=[candle_no_symbol, candle_with_symbol],
        )
        assert envelope.primary_symbol() == "BTC/USD"


class TestKrakenTradeSchemas:
    """Tests for Kraken trade subscription schemas."""

    def test_trade_subscribe_params_as_params(self) -> None:
        """Serialize trade subscription parameters.

        Given: Trade subscribe params with symbols and reqid,
        When: as_params is called,
        Then: Returns dict with channel and request ID.
        """
        params = KrakenTradeSubscribeParamsSchema(symbol=["BTC/USD"], snapshot=True, reqid=42)
        result = params.as_params()
        assert result["channel"] == "trade"
        assert result["symbol"] == ["BTC/USD"]
        assert result["reqid"] == 42

    def test_trade_schema_as_dict(self) -> None:
        """Export trade schema as dictionary.

        Given: Trade schema with all fields populated,
        When: as_dict is called,
        Then: Returns dictionary with trade data.
        """
        trade = KrakenTradeSchema(
            symbol="BTC/USD",
            side="buy",
            qty=0.5,
            price=50000.0,
            ord_type="limit",
            trade_id=12345,
            timestamp="2024-01-01T00:00:00Z",
        )
        result = trade.as_dict()
        assert result["symbol"] == "BTC/USD"
        assert result["qty"] == pytest.approx(0.5)
        assert result["trade_id"] == 12345

    def test_trade_event_envelope_symbol(self) -> None:
        """Extract symbol from trade envelope.

        Given: Trade envelope with trade data,
        When: symbol is called,
        Then: Returns first trade's symbol.
        """
        trade = KrakenTradeSchema(
            symbol="ETH/USD",
            side="sell",
            qty=1.0,
            price=3000.0,
            timestamp="2024-01-01T00:00:00Z",
        )
        envelope = KrakenTradeEventEnvelope(
            channel="trade",
            type="update",
            data=[trade],
        )
        assert envelope.symbol() == "ETH/USD"

    def test_trade_event_envelope_symbol_none(self) -> None:
        """Handle empty trade envelope.

        Given: Trade envelope with empty data list,
        When: symbol is called,
        Then: Returns None.
        """
        envelope = KrakenTradeEventEnvelope(
            channel="trade",
            type="update",
            data=[],
        )
        assert envelope.symbol() is None

    def test_trade_event_envelope_symbol_first_none(self) -> None:
        """Skip trades with empty symbol.

        Given: Trade envelope where first trade has empty symbol,
        When: symbol is called,
        Then: Returns first non-empty symbol.
        """
        trade_no_symbol = KrakenTradeSchema(
            symbol="",
            side="buy",
            qty=0.5,
            price=50000.0,
            timestamp="2024-01-01T00:00:00Z",
        )
        trade_with_symbol = KrakenTradeSchema(
            symbol="BTC/USD",
            side="sell",
            qty=0.3,
            price=50100.0,
            timestamp="2024-01-01T00:00:01Z",
        )
        envelope = KrakenTradeEventEnvelope(
            channel="trade",
            type="update",
            data=[trade_no_symbol, trade_with_symbol],
        )
        assert envelope.symbol() == "BTC/USD"

    def test_trade_event_envelope_as_dicts(self) -> None:
        """Export trade envelope as dictionaries.

        Given: Trade envelope with trade data,
        When: as_dicts is called,
        Then: Returns list of trade dictionaries.
        """
        trade = KrakenTradeSchema(
            symbol="BTC/USD",
            side="buy",
            qty=0.5,
            price=50000.0,
            timestamp="2024-01-01T00:00:00Z",
        )
        envelope = KrakenTradeEventEnvelope(
            channel="trade",
            type="update",
            data=[trade],
        )
        dicts = envelope.as_dicts()
        assert len(dicts) == 1
        assert dicts[0]["symbol"] == "BTC/USD"


class TestKrakenExecutionSchemas:
    """Tests for Kraken execution subscription schemas."""

    def test_execution_subscribe_params_as_params(self) -> None:
        """Serialize execution subscription parameters.

        Given: Execution subscribe params with token and options,
        When: as_params is called,
        Then: Returns dict with channel and auth token.
        """
        params = KrakenExecutionSubscribeParamsSchema(
            token="test-token",
            snap_trades=True,
            snap_orders=True,
            order_status=True,
            reqid=99,
        )
        result = params.as_params()
        assert result["channel"] == "executions"
        assert result["token"] == "test-token"
        assert result["reqid"] == 99

    def test_execution_fee_schema_as_dict(self) -> None:
        """Export fee schema as dictionary.

        Given: Fee schema with asset and quantity,
        When: as_dict is called,
        Then: Returns dictionary with fee data.
        """
        fee = KrakenExecutionFeeSchema(asset="USD", qty=10.5)
        result = fee.as_dict()
        assert result["asset"] == "USD"
        assert result["qty"] == pytest.approx(10.5)

    def test_execution_schema_as_dict(self) -> None:
        """Export execution schema as dictionary.

        Given: Execution schema with order details,
        When: as_dict is called,
        Then: Returns dictionary with execution data.
        """
        execution = KrakenExecutionSchema(
            order_id="ORDER123",
            symbol="BTC/USD",
            side="buy",
            order_type="limit",
            order_status="open",
            order_qty=1.0,
            limit_price=50000.0,
            timestamp="2024-01-01T00:00:00Z",
        )
        result = execution.as_dict()
        assert result["order_id"] == "ORDER123"
        assert result["symbol"] == "BTC/USD"

    def test_execution_event_envelope_primary_symbol(self) -> None:
        """Extract primary symbol from execution envelope.

        Given: Execution envelope with execution data,
        When: primary_symbol is called,
        Then: Returns first execution's symbol.
        """
        execution = KrakenExecutionSchema(
            order_id="ORDER123",
            symbol="BTC/USD",
        )
        envelope = KrakenExecutionEventEnvelope(
            channel="executions",
            type="update",
            data=[execution],
        )
        assert envelope.primary_symbol() == "BTC/USD"

    def test_execution_event_envelope_primary_symbol_none(self) -> None:
        """Handle empty execution envelope.

        Given: Execution envelope with empty data list,
        When: primary_symbol is called,
        Then: Returns None.
        """
        envelope = KrakenExecutionEventEnvelope(
            channel="executions",
            type="update",
            data=[],
        )
        assert envelope.primary_symbol() is None

    def test_execution_event_envelope_primary_symbol_first_none(self) -> None:
        """Skip executions with None symbol.

        Given: Execution envelope where first exec has no symbol,
        When: primary_symbol is called,
        Then: Returns first non-None symbol.
        """
        exec_no_symbol = KrakenExecutionSchema(
            order_id="ORDER123",
            symbol=None,
        )
        exec_with_symbol = KrakenExecutionSchema(
            order_id="ORDER456",
            symbol="ETH/USD",
        )
        envelope = KrakenExecutionEventEnvelope(
            channel="executions",
            type="update",
            data=[exec_no_symbol, exec_with_symbol],
        )
        assert envelope.primary_symbol() == "ETH/USD"

    def test_execution_event_envelope_as_dicts(self) -> None:
        """Export execution envelope as dictionaries.

        Given: Execution envelope with execution data,
        When: as_dicts is called,
        Then: Returns list of execution dictionaries.
        """
        execution = KrakenExecutionSchema(
            order_id="ORDER456",
            exec_type="trade",
            symbol="ETH/USD",
        )
        envelope = KrakenExecutionEventEnvelope(
            channel="executions",
            type="snapshot",
            data=[execution],
            sequence=1,
        )
        dicts = envelope.as_dicts()
        assert len(dicts) == 1
        assert dicts[0]["order_id"] == "ORDER456"


class TestKrakenOrderSchemas:
    """Tests for Kraken order creation and cancellation schemas."""

    def test_add_order_params_schema(self) -> None:
        """Validate add order parameters schema.

        Given: Add order params with all order fields,
        When: Schema is instantiated,
        Then: All fields are correctly set.
        """
        params = KrakenAddOrderParamsSchema(
            order_type="limit",
            side="buy",
            symbol="BTC/USD",
            limit_price=50000.0,
            order_qty=0.1,
            time_in_force="GTC",
            post_only=True,
            reduce_only=False,
            validate=True,
            token="auth-token",
        )
        assert params.order_type == "limit"
        assert params.validate_only is True

    def test_add_order_result_schema(self) -> None:
        """Parse add order result with warnings.

        Given: Order result with order_id and warning list,
        When: Schema is instantiated,
        Then: Warnings are accessible as list.
        """
        result = KrakenAddOrderResultSchema(
            order_id="OABC123",
            order_userref=12345,
            cl_ord_id="client-order-id",
            warning=["minimum not met"],
        )
        assert result.order_id == "OABC123"
        assert result.warning == ["minimum not met"]

    def test_add_order_response_schema(self) -> None:
        """Parse successful add order response.

        Given: Success response with result containing order_id,
        When: Schema is instantiated,
        Then: Success flag and result are accessible.
        """
        result = KrakenAddOrderResultSchema(order_id="OABC123")
        response = KrakenAddOrderResponseSchema(
            method="add_order",
            result=result,
            success=True,
            time_in="2024-01-01T00:00:00Z",
            time_out="2024-01-01T00:00:01Z",
            reqid=42,
        )
        assert response.success is True
        assert response.result is not None
        assert response.result.order_id == "OABC123"

    def test_add_order_response_schema_error(self) -> None:
        """Parse failed add order response.

        Given: Error response with success=False and error message,
        When: Schema is instantiated,
        Then: Error message is accessible.
        """
        response = KrakenAddOrderResponseSchema(
            method="add_order",
            result=None,
            success=False,
            error="insufficient funds",
        )
        assert response.success is False
        assert response.error == "insufficient funds"

    def test_cancel_order_params_schema(self) -> None:
        """Validate cancel order parameters schema.

        Given: Cancel params with order_id and cl_ord_id lists,
        When: Schema is instantiated,
        Then: Both ID lists are accessible.
        """
        params = KrakenCancelOrderParamsSchema(
            order_id=["ORDER1", "ORDER2"],
            cl_ord_id=["CLIENT1"],
            token="auth-token",
        )
        assert params.order_id == ["ORDER1", "ORDER2"]
        assert params.cl_ord_id == ["CLIENT1"]

    def test_cancel_order_result_schema(self) -> None:
        """Parse cancel order result with warnings.

        Given: Cancel result with order_id and warning list,
        When: Schema is instantiated,
        Then: Warning list is accessible.
        """
        result = KrakenCancelOrderResultSchema(
            order_id="ORDER123",
            cl_ord_id="CLIENT123",
            warning=["partial cancel"],
        )
        assert result.order_id == "ORDER123"
        assert result.warning == ["partial cancel"]

    def test_cancel_order_response_schema(self) -> None:
        """Parse successful cancel order response.

        Given: Success response with result containing order_id,
        When: Schema is instantiated,
        Then: Success flag and result are accessible.
        """
        result = KrakenCancelOrderResultSchema(order_id="ORDER123")
        response = KrakenCancelOrderResponseSchema(
            method="cancel_order",
            result=result,
            success=True,
            reqid=100,
        )
        assert response.success is True
        assert response.result is not None
        assert response.result.order_id == "ORDER123"
