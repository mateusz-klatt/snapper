"""Tests for SQLAlchemy ORM models in the data layer."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from unittest.mock import MagicMock

import pytest

from snapper.data.models import Candle
from snapper.data.models import Execution
from snapper.data.models import ExecutionPlan
from snapper.data.models import ExecutionPlanCheckpoint
from snapper.data.models import ExecutionPlanDecision
from snapper.data.models import Instrument
from snapper.data.models import InstrumentOrderCapability
from snapper.data.models import Order
from snapper.data.models import Position
from snapper.data.models import Signal
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolExchangeCapability
from snapper.data.models import Trade
from snapper.data.models import TZDateTime
from snapper.data.models import UUIDColumn
from snapper.data.models import VenueFeeSchedule


class TestInstrumentModel:
    """Tests for Instrument SQLAlchemy ORM model."""

    def test_instrument_creation(self) -> None:
        """Test Instrument model with all fields.

        Given: Valid instrument parameters,
        When: Instrument is created,
        Then: All fields match provided values.
        """
        instrument = Instrument(
            symbol_public_id="00000000-0000-7000-8000-000000000001",
            exchange="kraken",
            session_id="test-session",
            sequence_id=1,
        )
        assert instrument.symbol_public_id == "00000000-0000-7000-8000-000000000001"
        assert instrument.exchange == "kraken"

    def test_instrument_string_representation(self) -> None:
        """Test Instrument has string representation.

        Given: An Instrument instance,
        When: Converted to string,
        Then: Returns string representation.
        """
        instrument = Instrument(
            symbol_public_id="00000000-0000-7000-8000-000000000002",
            exchange="kraken",
            session_id="test-session",
            sequence_id=1,
        )
        str_repr = str(instrument)
        assert isinstance(str_repr, str)

    def test_instrument_default_exchange(self) -> None:
        """Test Instrument accepts omitted exchange for backward compat.

        Given: Instrument created without exchange,
        When: Checking exchange field before flush,
        Then: Attribute is None (INSERT default supplies empty string).
        """
        instrument = Instrument(
            symbol_public_id="00000000-0000-7000-8000-000000000003",
            session_id="test-session",
            sequence_id=1,
        )
        assert instrument.exchange is None


class TestCandleModel:
    """Tests for Candle SQLAlchemy ORM model."""

    def test_candle_creation(self) -> None:
        """Test Candle model with OHLCV data.

        Given: Valid candle parameters,
        When: Candle is created,
        Then: All OHLCV fields are set correctly.
        """
        now = datetime.now(UTC)
        candle = Candle(
            instrument_public_id="test-instrument-uuid",
            timestamp=now,
            timeframe="1h",
            open=49000.0,
            high=51000.0,
            low=48500.0,
            close=50500.0,
            volume=150.5,
            session_id="test-session",
            sequence_id=1,
        )
        assert candle.instrument_public_id == "test-instrument-uuid"
        assert candle.timestamp == now
        assert candle.timeframe == "1h"
        assert candle.open == pytest.approx(49000.0)
        assert candle.high == pytest.approx(51000.0)
        assert candle.low == pytest.approx(48500.0)
        assert candle.close == pytest.approx(50500.0)
        assert candle.volume == pytest.approx(150.5)

    def test_candle_ohlc_validation(self) -> None:
        """Test Candle OHLC relationship constraints.

        Given: A valid candle with OHLC data,
        When: Checking high/low relationships,
        Then: High >= open,close,low and low <= all.
        """
        candle = Candle(
            instrument_public_id="test-instrument-uuid",
            timestamp=datetime.now(UTC),
            timeframe="1h",
            open=50000.0,
            high=52000.0,
            low=48000.0,
            close=51000.0,
            volume=100.0,
            session_id="test-session",
            sequence_id=1,
        )
        assert candle.high >= candle.open
        assert candle.high >= candle.close
        assert candle.high >= candle.low
        assert candle.low <= candle.open
        assert candle.low <= candle.close
        assert candle.low <= candle.high


class TestTradeModel:
    """Tests for Trade SQLAlchemy ORM model."""

    def test_trade_creation(self) -> None:
        """Test Trade model with all fields.

        Given: Valid trade parameters,
        When: Trade is created,
        Then: All fields match provided values.
        """
        trade = Trade(
            instrument_public_id="test-instrument-uuid",
            timestamp=datetime.now(UTC),
            price=50000.0,
            size=1.5,
            side="buy",
            trade_id="trade-123",
            session_id="test-session",
            sequence_id=1,
        )
        assert trade.instrument_public_id == "test-instrument-uuid"
        assert trade.price == pytest.approx(50000.0)
        assert trade.size == pytest.approx(1.5)
        assert trade.side == "buy"
        assert trade.trade_id == "trade-123"
        assert isinstance(trade.timestamp, datetime)

    def test_trade_value_calculation(self) -> None:
        """Test Trade value is size times price.

        Given: A trade with price and size,
        When: Calculating trade value,
        Then: Value equals size * price.
        """
        trade = Trade(
            instrument_public_id="test-instrument-uuid",
            timestamp=datetime.now(UTC),
            price=45000.0,
            size=2.0,
            side="buy",
            trade_id="trade-123",
            session_id="test-session",
            sequence_id=1,
        )
        expected_value = 2.0 * 45000.0
        assert trade.size * trade.price == expected_value

    def test_trade_sides(self) -> None:
        """Test Trade supports buy and sell sides.

        Given: Two trades with different sides,
        When: Checking side values,
        Then: Both buy and sell are supported.
        """
        buy_trade = Trade(
            instrument_public_id="test-instrument-uuid",
            timestamp=datetime.now(UTC),
            price=50000.0,
            size=1.0,
            side="buy",
            trade_id="buy-trade",
            session_id="test-session",
            sequence_id=1,
        )
        sell_trade = Trade(
            instrument_public_id="test-instrument-uuid",
            timestamp=datetime.now(UTC),
            price=50000.0,
            size=1.0,
            side="sell",
            trade_id="sell-trade",
            session_id="test-session",
            sequence_id=1,
        )
        assert buy_trade.side == "buy"
        assert sell_trade.side == "sell"


class TestOrderModel:
    """Tests for Order SQLAlchemy ORM model."""

    def test_order_creation(self) -> None:
        """Test Order model with all fields.

        Given: Valid order parameters,
        When: Order is created,
        Then: All fields match provided values.
        """
        now = datetime.now(UTC)
        order = Order(
            instrument_public_id="test-instrument-uuid",
            client_order_id="client-123",
            exchange_order_id="exchange-456",
            created_at=now,
            timestamp=now,
            side="buy",
            order_type="limit",
            price=50000.0,
            size=1.0,
            filled_size=0.0,
            average_price=None,
            status="pending",
            session_id="test-session",
            sequence_id=1,
        )
        assert order.instrument_public_id == "test-instrument-uuid"
        assert order.client_order_id == "client-123"
        assert order.exchange_order_id == "exchange-456"
        assert order.side == "buy"
        assert order.order_type == "limit"
        assert order.price == pytest.approx(50000.0)
        assert order.size == pytest.approx(1.0)
        assert order.filled_size == pytest.approx(0.0)
        assert order.average_price is None
        assert order.status == "pending"

    def test_market_order(self) -> None:
        """Test Order with market type has no price.

        Given: Market order parameters,
        When: Order is created,
        Then: Price is None for market orders.
        """
        now = datetime.now(UTC)
        order = Order(
            instrument_public_id="test-instrument-uuid",
            created_at=now,
            timestamp=now,
            side="buy",
            order_type="market",
            price=None,
            size=0.5,
            status="pending",
            session_id="test-session",
            sequence_id=1,
        )
        assert order.order_type == "market"
        assert order.price is None

    def test_order_status_updates(self) -> None:
        """Test Order status can be updated.

        Given: An order with pending status,
        When: Status is changed to filled,
        Then: New status is reflected.
        """
        now = datetime.now(UTC)
        order = Order(
            instrument_public_id="test-instrument-uuid",
            created_at=now,
            timestamp=now,
            side="buy",
            order_type="limit",
            price=50000.0,
            size=1.0,
            status="pending",
            session_id="test-session",
            sequence_id=1,
        )
        assert order.status == "pending"
        order.status = "filled"
        assert order.status == "filled"


class TestExecutionModel:
    """Tests for Execution SQLAlchemy ORM model."""

    def test_execution_creation(self) -> None:
        """Test Execution model with all fields.

        Given: Valid execution parameters,
        When: Execution is created,
        Then: All fields match provided values.
        """
        execution = Execution(
            order_public_id="order-pub-id-1",
            timestamp=datetime.now(UTC),
            side="buy",
            status="filled",
            price=50000.0,
            size=1.0,
            fee=5.0,
            fee_asset="USD",
            session_id="test-session",
            sequence_id=1,
        )
        assert execution.order_public_id == "order-pub-id-1"
        assert execution.price == pytest.approx(50000.0)
        assert execution.size == pytest.approx(1.0)
        assert execution.fee == pytest.approx(5.0)
        assert execution.fee_asset == "USD"
        assert execution.executed_at is None

    def test_execution_fee_calculation(self) -> None:
        """Test Execution fee percentage is valid.

        Given: An execution with fee,
        When: Calculating fee percentage,
        Then: Percentage is between 0 and 100.
        """
        execution = Execution(
            order_public_id="order-pub-id-1",
            timestamp=datetime.now(UTC),
            side="buy",
            status="filled",
            price=50000.0,
            size=2.0,
            fee=10.0,
            fee_asset="USD",
            session_id="test-session",
            sequence_id=1,
        )
        trade_value = execution.price * execution.size
        fee_percentage = (execution.fee / trade_value) * 100
        assert fee_percentage >= 0.0
        assert fee_percentage <= 100.0


class TestPositionModel:
    """Tests for Position SQLAlchemy ORM model."""

    def test_position_creation(self) -> None:
        """Test Position model with all fields.

        Given: Valid position parameters,
        When: Position is created,
        Then: All fields match provided values.
        """
        position = Position(
            instrument_public_id="test-instrument-uuid",
            quantity=2.5,
            average_price=48000.0,
            unrealized_pnl=5000.0,
            realized_pnl=1000.0,
            timestamp=datetime.now(UTC),
            session_id="test-session",
            sequence_id=1,
        )
        assert position.instrument_public_id == "test-instrument-uuid"
        assert position.quantity == pytest.approx(2.5)
        assert position.average_price == pytest.approx(48000.0)
        assert position.unrealized_pnl == pytest.approx(5000.0)
        assert position.realized_pnl == pytest.approx(1000.0)

    def test_position_calculations(self) -> None:
        """Test Position market value calculation.

        Given: A position with quantity and price,
        When: Calculating market value,
        Then: Value equals quantity * average_price.
        """
        position = Position(
            instrument_public_id="test-instrument-uuid",
            quantity=1.0,
            average_price=45000.0,
            unrealized_pnl=0.0,
            realized_pnl=0.0,
            timestamp=datetime.now(UTC),
            session_id="test-session",
            sequence_id=1,
        )
        market_value = position.quantity * position.average_price
        assert market_value == pytest.approx(45000.0)

    def test_position_pnl(self) -> None:
        """Test Position total PnL calculation.

        Given: A position with unrealized and realized PnL,
        When: Calculating total PnL,
        Then: Total equals sum of both.
        """
        position = Position(
            instrument_public_id="test-instrument-uuid",
            quantity=1.0,
            average_price=50000.0,
            unrealized_pnl=2000.0,
            realized_pnl=500.0,
            timestamp=datetime.now(UTC),
            session_id="test-session",
            sequence_id=1,
        )
        total_pnl = position.unrealized_pnl + position.realized_pnl
        assert total_pnl == pytest.approx(2500.0)


class TestSymbolModel:
    """Tests for Symbol SQLAlchemy ORM model."""

    def test_symbol_creation(self) -> None:
        """Test Symbol temporal table creation.

        Given: Symbol parameters with base, quote, asset_type,
        When: Symbol is created,
        Then: All fields match provided values.
        """
        now = datetime.now(UTC)
        sym = Symbol(
            native_symbol="BTC-USD",
            base="BTC",
            quote="USD",
            asset_type="crypto",
            created_at=now,
            timestamp=now,
            session_id="test-session",
            sequence_id=1,
        )
        assert sym.native_symbol == "BTC-USD"
        assert sym.base == "BTC"
        assert sym.quote == "USD"
        assert sym.asset_type == "crypto"
        assert sym.created_at == now
        assert sym.timestamp == now

    def test_symbol_equity_nullable_quote(self) -> None:
        """Test Symbol allows nullable quote for equity.

        Given: Equity parameters without quote,
        When: Symbol is created,
        Then: Quote is None.
        """
        now = datetime.now(UTC)
        sym = Symbol(
            native_symbol="AAPL",
            base="AAPL",
            quote=None,
            asset_type="equity",
            created_at=now,
            timestamp=now,
            session_id="test-session",
            sequence_id=1,
        )
        assert sym.quote is None
        assert sym.asset_type == "equity"


class TestSymbolAliasModel:
    """Tests for SymbolAlias SQLAlchemy ORM model."""

    def test_symbol_alias_creation(self) -> None:
        """Test SymbolAlias for a Kraken WebSocket alias.

        Given: Alias parameters for kraken ws channel,
        When: SymbolAlias is created,
        Then: All fields match.
        """
        now = datetime.now(UTC)
        alias = SymbolAlias(
            symbol_public_id="test-uuid-1234",
            exchange="kraken",
            channel="ws",
            exchange_symbol="BTC/USD",
            created_at=now,
            timestamp=now,
            session_id="test-session",
            sequence_id=1,
        )
        assert alias.symbol_public_id == "test-uuid-1234"
        assert alias.exchange == "kraken"
        assert alias.channel == "ws"
        assert alias.exchange_symbol == "BTC/USD"

    def test_symbol_alias_polygon_rest(self) -> None:
        """Test SymbolAlias for a Polygon REST alias.

        Given: Alias parameters for polygon rest channel,
        When: SymbolAlias is created,
        Then: Exchange symbol uses Polygon prefix format.
        """
        now = datetime.now(UTC)
        alias = SymbolAlias(
            symbol_public_id="test-uuid-5678",
            exchange="polygon",
            channel="rest",
            exchange_symbol="X:BTCUSD",
            created_at=now,
            timestamp=now,
            session_id="test-session",
            sequence_id=1,
        )
        assert alias.exchange == "polygon"
        assert alias.channel == "rest"
        assert alias.exchange_symbol == "X:BTCUSD"


class TestSymbolExchangeCapabilityModel:
    """Tests for SymbolExchangeCapability SQLAlchemy ORM model."""

    def test_capability_tablename(self) -> None:
        """Test SymbolExchangeCapability tablename is correct.

        Given: SymbolExchangeCapability model class,
        When: Checking __tablename__,
        Then: Returns 'symbol_exchange_capabilities'.
        """
        assert SymbolExchangeCapability.__tablename__ == "symbol_exchange_capabilities"

    def test_capability_creation_with_all_fields(self) -> None:
        """Test SymbolExchangeCapability with all fields populated.

        Given: All capability parameters including source and reason,
        When: SymbolExchangeCapability is created,
        Then: All fields match provided values.
        """
        now = datetime.now(UTC)
        cap = SymbolExchangeCapability(
            symbol_public_id="test-uuid-cap-1",
            exchange="kraken",
            can_market_data=True,
            can_trade=True,
            source="kraken_updater",
            reason="Listed on exchange ticker list",
            created_at=now,
            timestamp=now,
            session_id="test-session",
            sequence_id=1,
        )
        assert cap.symbol_public_id == "test-uuid-cap-1"
        assert cap.exchange == "kraken"
        assert cap.can_market_data is True
        assert cap.can_trade is True
        assert cap.source == "kraken_updater"
        assert cap.reason == "Listed on exchange ticker list"
        assert cap.created_at == now
        assert cap.timestamp == now

    def test_capability_default_booleans(self) -> None:
        """Test SymbolExchangeCapability with False boolean flags.

        Given: Capability parameters with both booleans set to False,
        When: SymbolExchangeCapability is created,
        Then: Both can_market_data and can_trade are False.
        """
        now = datetime.now(UTC)
        cap = SymbolExchangeCapability(
            symbol_public_id="test-uuid-cap-2",
            exchange="zonda",
            can_market_data=False,
            can_trade=False,
            source="seed",
            reason=None,
            created_at=now,
            timestamp=now,
            session_id="test-session",
            sequence_id=1,
        )
        assert cap.can_market_data is False
        assert cap.can_trade is False

    def test_capability_nullable_source_and_reason(self) -> None:
        """Test SymbolExchangeCapability with None source and reason.

        Given: Capability parameters with source=None and reason=None,
        When: SymbolExchangeCapability is created,
        Then: Source and reason are None.
        """
        now = datetime.now(UTC)
        cap = SymbolExchangeCapability(
            symbol_public_id="test-uuid-cap-3",
            exchange="kraken",
            can_market_data=True,
            can_trade=False,
            source=None,
            reason=None,
            created_at=now,
            timestamp=now,
            session_id="test-session",
            sequence_id=1,
        )
        assert cap.source is None
        assert cap.reason is None

    def test_capability_with_source_and_reason(self) -> None:
        """Test SymbolExchangeCapability with actual source and reason values.

        Given: Capability parameters with populated source and reason,
        When: SymbolExchangeCapability is created,
        Then: Source and reason match provided values.
        """
        now = datetime.now(UTC)
        cap = SymbolExchangeCapability(
            symbol_public_id="test-uuid-cap-4",
            exchange="kraken",
            can_market_data=True,
            can_trade=True,
            source="kraken_updater",
            reason="WS-only, no REST ticker",
            created_at=now,
            timestamp=now,
            session_id="test-session",
            sequence_id=1,
        )
        assert cap.source == "kraken_updater"
        assert cap.reason == "WS-only, no REST ticker"


class TestSignalEventModel:
    """Tests for Signal SQLAlchemy ORM model."""

    def test_signal_event_creation(self) -> None:
        """Test Signal model with all fields.

        Given: Valid signal parameters,
        When: Signal is created,
        Then: All fields match provided values.
        """
        event = Signal(
            instrument_public_id="test-instrument-uuid",
            timestamp=datetime.now(UTC),
            side="buy",
            strength=0.8,
            reason="RSI oversold",
            strategy_name="RSIReversion",
            price=49000.0,
            session_id="test-session",
            sequence_id=1,
        )
        assert event.instrument_public_id == "test-instrument-uuid"
        assert event.side == "buy"
        assert event.strength == pytest.approx(0.8)
        assert event.reason == "RSI oversold"
        assert event.strategy_name == "RSIReversion"
        assert event.price == pytest.approx(49000.0)

    def test_signal_event_strength_validation(self) -> None:
        """Test Signal strength is normalized 0-1.

        Given: A signal with strength value,
        When: Checking strength bounds,
        Then: Strength is between 0 and 1.
        """
        event = Signal(
            instrument_public_id="test-instrument-uuid",
            timestamp=datetime.now(UTC),
            side="sell",
            strength=0.9,
            reason="MACD bearish cross",
            strategy_name="MACDCrossover",
            session_id="test-session",
            sequence_id=1,
        )
        assert 0.0 <= event.strength <= 1.0

    def test_signal_event_without_price(self) -> None:
        """Test Signal with null price.

        Given: Signal parameters without price,
        When: Signal is created,
        Then: Price field is None.
        """
        event = Signal(
            instrument_public_id="test-instrument-uuid",
            timestamp=datetime.now(UTC),
            side="buy",
            strength=0.7,
            reason="Custom signal",
            price=None,
            session_id="test-session",
            sequence_id=1,
        )
        assert event.price is None


class TestTZDateTime:
    """Tests for TZDateTime custom SQLAlchemy type."""

    def test_process_bind_param_none(self) -> None:
        """Test TZDateTime passes through None.

        Given: None value for bind param,
        When: process_bind_param is called,
        Then: Returns None.
        """
        tz_dt = TZDateTime()
        mock_dialect = MagicMock()
        result = tz_dt.process_bind_param(None, mock_dialect)
        assert result is None

    def test_process_bind_param_naive_datetime_raises(self) -> None:
        """Test TZDateTime rejects naive datetimes.

        Given: Naive datetime without tzinfo,
        When: process_bind_param is called,
        Then: Raises ValueError.
        """
        tz_dt = TZDateTime()
        mock_dialect = MagicMock()
        naive = datetime(2024, 1, 1, 12, 0, 0)
        with pytest.raises(ValueError, match="Cannot save naive datetime"):
            tz_dt.process_bind_param(naive, mock_dialect)

    def test_process_bind_param_aware_datetime_converted_to_utc(self) -> None:
        """Test TZDateTime converts non-UTC to UTC.

        Given: Aware datetime in CET timezone,
        When: process_bind_param is called,
        Then: Converts to UTC with adjusted hour.
        """
        tz_dt = TZDateTime()
        mock_dialect = MagicMock()
        cet = timezone(offset=timedelta(hours=1))
        aware = datetime(2024, 1, 1, 13, 0, 0, tzinfo=cet)
        result = tz_dt.process_bind_param(aware, mock_dialect)
        assert result is not None
        assert result.tzinfo == UTC
        assert result.hour == 12

    def test_process_result_value_none(self) -> None:
        """Test TZDateTime returns None for None result.

        Given: None value from database,
        When: process_result_value is called,
        Then: Returns None.
        """
        tz_dt = TZDateTime()
        mock_dialect = MagicMock()
        result = tz_dt.process_result_value(None, mock_dialect)
        assert result is None

    def test_process_result_value_naive_adds_utc(self) -> None:
        """Test TZDateTime adds UTC to naive result.

        Given: Naive datetime from database,
        When: process_result_value is called,
        Then: Returns datetime with UTC tzinfo.
        """
        tz_dt = TZDateTime()
        mock_dialect = MagicMock()
        naive = datetime(2024, 1, 1, 12, 0, 0)
        result = tz_dt.process_result_value(naive, mock_dialect)
        assert result is not None
        assert result.tzinfo == UTC

    def test_process_result_value_aware_preserves_timezone(self) -> None:
        """Test TZDateTime preserves aware datetime tzinfo.

        Given: UTC-aware datetime from database,
        When: process_result_value is called,
        Then: Returns datetime with preserved UTC.
        """
        tz_dt = TZDateTime()
        mock_dialect = MagicMock()
        aware = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        result = tz_dt.process_result_value(aware, mock_dialect)
        assert result is not None
        assert result.tzinfo == UTC


class TestUUIDColumn:
    """Tests for UUIDColumn custom SQLAlchemy type."""

    def test_load_dialect_impl_postgresql(self) -> None:
        """Test UUIDColumn uses native UUID on PostgreSQL.

        Given: A PostgreSQL dialect,
        When: load_dialect_impl is called,
        Then: Returns native UUID type descriptor.
        """
        col = UUIDColumn()
        mock_dialect = MagicMock()
        mock_dialect.name = "postgresql"
        mock_dialect.type_descriptor = MagicMock(side_effect=lambda t: t)
        col.load_dialect_impl(mock_dialect)
        mock_dialect.type_descriptor.assert_called_once()

    def test_process_bind_param_none(self) -> None:
        """Test UUIDColumn returns None for None input.

        Given: None value,
        When: process_bind_param is called,
        Then: Returns None.
        """
        col = UUIDColumn()
        mock_dialect = MagicMock()
        result = col.process_bind_param(None, mock_dialect)
        assert result is None


class TestInstrumentOrderCapabilityModel:
    """Tests for InstrumentOrderCapability ORM model."""

    def test_creation_with_required_fields(self) -> None:
        """Verify model creation with all required fields.

        Given: Required field values for capability matrix,
        When: InstrumentOrderCapability instantiated,
        Then: All fields set correctly with expected defaults.
        """
        now = datetime.now(UTC)
        cap = InstrumentOrderCapability(
            instrument_public_id="inst-1",
            exchange="kraken",
            supported_order_types=["market", "limit"],
            session_id="s1",
            sequence_id=1,
            timestamp=now,
        )
        assert cap.instrument_public_id == "inst-1"
        assert cap.exchange == "kraken"
        assert cap.supported_order_types == ["market", "limit"]
        assert cap.min_notional is None
        assert hasattr(cap, "supports_post_only")
        assert hasattr(cap, "top_of_book_quality")

    def test_creation_with_full_capabilities(self) -> None:
        """Verify model creation with all capability flags enabled.

        Given: Full capability values for a liquid exchange,
        When: InstrumentOrderCapability instantiated,
        Then: All flags reflect the provided values.
        """
        now = datetime.now(UTC)
        cap = InstrumentOrderCapability(
            instrument_public_id="inst-2",
            exchange="kraken_futures",
            supported_order_types=["market", "limit", "stop", "trailing_stop"],
            supports_post_only=True,
            supports_reduce_only=True,
            supports_amend_in_place=True,
            supports_native_stop_loss=True,
            supports_native_take_profit=True,
            supports_market_making=True,
            supports_short_selling=True,
            supports_leverage=True,
            max_leverage_long=10.0,
            max_leverage_short=10.0,
            min_notional=5.0,
            max_order_size=1000.0,
            top_of_book_quality="realtime",
            session_id="s1",
            sequence_id=1,
            timestamp=now,
        )
        assert cap.supports_post_only is True
        assert cap.supports_leverage is True
        assert cap.max_leverage_long == 10.0
        assert cap.top_of_book_quality == "realtime"


class TestVenueFeeScheduleModel:
    """Tests for VenueFeeSchedule ORM model."""

    def test_creation_with_required_fields(self) -> None:
        """Verify model creation with required fee schedule fields.

        Given: Default tier fee values,
        When: VenueFeeSchedule instantiated,
        Then: All fields set correctly.
        """
        now = datetime.now(UTC)
        fee = VenueFeeSchedule(
            exchange="kraken",
            fee_tier="default",
            maker_bps=16.0,
            taker_bps=26.0,
            currency="USD",
            session_id="s1",
            sequence_id=1,
            timestamp=now,
        )
        assert fee.exchange == "kraken"
        assert fee.fee_tier == "default"
        assert fee.maker_bps == 16.0
        assert fee.taker_bps == 26.0
        assert fee.instrument_public_id is None
        assert fee.min_volume_30d is None

    def test_creation_with_rebate(self) -> None:
        """Verify negative maker_bps (rebate) is accepted.

        Given: Market maker tier with negative maker fee,
        When: VenueFeeSchedule instantiated,
        Then: maker_bps is negative (rebate).
        """
        now = datetime.now(UTC)
        fee = VenueFeeSchedule(
            exchange="kraken_futures",
            fee_tier="market_maker",
            maker_bps=-2.0,
            taker_bps=5.0,
            min_volume_30d=1_000_000.0,
            currency="USD",
            session_id="s1",
            sequence_id=1,
            timestamp=now,
        )
        assert fee.maker_bps == -2.0
        assert fee.min_volume_30d == 1_000_000.0


class TestExecutionPlanModel:
    """Tests for ExecutionPlan ORM model."""

    def test_creation_manual_once(self) -> None:
        """Verify manual_once plan creation with required fields.

        Given: Manual order plan parameters,
        When: ExecutionPlan instantiated,
        Then: All fields set correctly with defaults.
        """
        now = datetime.now(UTC)
        plan = ExecutionPlan(
            plan_type="manual_once",
            created_by_user_id="user-1",
            created_via="ui",
            instrument_public_id="inst-1",
            exchange="kraken",
            mode="live",
            shard_key="kraken:BTC-USD:live",
            wallet_public_id="wallet-1",
            total_quantity=0.5,
            side="buy",
            params={"order_type": "limit", "price": 50000.0},
            status="pending",
            created_at=now,
            session_id="s1",
            sequence_id=1,
            timestamp=now,
        )
        assert plan.plan_type == "manual_once"
        assert plan.created_by_user_id == "user-1"
        assert plan.created_via == "ui"
        assert plan.total_quantity == 0.5
        assert plan.side == "buy"
        assert plan.status == "pending"
        assert plan.operator_public_id is None
        assert plan.parent_plan_public_id is None
        assert plan.last_error is None
        assert plan.idempotency_key is None

    def test_creation_strategy_plan(self) -> None:
        """Verify strategy-originated plan with operator set.

        Given: Strategy-originated bracket plan,
        When: ExecutionPlan instantiated,
        Then: created_by_strategy and operator fields set.
        """
        now = datetime.now(UTC)
        plan = ExecutionPlan(
            plan_type="bracket",
            created_by_strategy="mean_revert_v3",
            created_via="strategy",
            instrument_public_id="inst-2",
            exchange="kraken_futures",
            mode="paper",
            shard_key="kraken_futures:ETH-USD:paper",
            wallet_public_id="wallet-2",
            operator_public_id="op-1",
            total_quantity=10.0,
            side="sell",
            params={"stop_loss": 2000.0, "take_profit": 1500.0},
            status="armed",
            created_at=now,
            parent_plan_public_id="parent-1",
            idempotency_key="idem-123",
            session_id="s1",
            sequence_id=1,
            timestamp=now,
        )
        assert plan.created_by_strategy == "mean_revert_v3"
        assert plan.operator_public_id == "op-1"
        assert plan.parent_plan_public_id == "parent-1"
        assert plan.idempotency_key == "idem-123"


class TestExecutionPlanCheckpointModel:
    """Tests for ExecutionPlanCheckpoint ORM model."""

    def test_creation(self) -> None:
        """Verify checkpoint creation with evaluator state.

        Given: Evaluator state snapshot,
        When: ExecutionPlanCheckpoint instantiated,
        Then: All fields set correctly.
        """
        now = datetime.now(UTC)
        cp = ExecutionPlanCheckpoint(
            plan_public_id="plan-1",
            state={"peak_price": 51000.0, "current_stop": 49000.0},
            last_venue_event_id=42,
            last_tick_timestamp=now,
            checkpoint_at=now,
            session_id="s1",
            sequence_id=1,
            timestamp=now,
        )
        assert cp.plan_public_id == "plan-1"
        assert cp.state["peak_price"] == 51000.0
        assert cp.last_venue_event_id == 42
        assert cp.checkpoint_at == now


class TestExecutionPlanDecisionModel:
    """Tests for ExecutionPlanDecision ORM model."""

    def test_creation_action(self) -> None:
        """Verify action-level decision log entry.

        Given: Command emission decision,
        When: ExecutionPlanDecision instantiated,
        Then: All fields set correctly with action importance.
        """
        now = datetime.now(UTC)
        dec = ExecutionPlanDecision(
            plan_public_id="plan-1",
            decision_type="emitted_command",
            decided_at=now,
            trigger_type="tick",
            evidence={"bid": 50000.0, "ask": 50001.0, "mid": 50000.5},
            emitted_command_public_id="cmd-1",
            reason="Price crossed threshold, emitting limit buy",
            decision_importance="action",
            session_id="s1",
            sequence_id=1,
            timestamp=now,
        )
        assert dec.decision_type == "emitted_command"
        assert dec.trigger_type == "tick"
        assert dec.emitted_command_public_id == "cmd-1"
        assert dec.decision_importance == "action"
        assert dec.new_status is None

    def test_creation_routine(self) -> None:
        """Verify routine-level sampled skip decision.

        Given: Sampled tick-skip summary,
        When: ExecutionPlanDecision instantiated,
        Then: importance is routine with aggregated evidence.
        """
        now = datetime.now(UTC)
        dec = ExecutionPlanDecision(
            plan_public_id="plan-2",
            decision_type="skipped_tick",
            decided_at=now,
            trigger_type="tick",
            evidence={"skip_count": 60, "min_price": 49900.0, "max_price": 50100.0},
            reason="60 ticks skipped, condition not met (bid < threshold)",
            decision_importance="routine",
            session_id="s1",
            sequence_id=1,
            timestamp=now,
        )
        assert dec.decision_importance == "routine"
        assert dec.evidence["skip_count"] == 60


class TestOrderPlanPublicId:
    """Tests for Order.plan_public_id back-reference."""

    def test_order_with_plan(self) -> None:
        """Order can reference an execution plan."""
        now = datetime.now(UTC)
        order = Order(
            instrument_public_id="inst-1",
            wallet_public_id="wallet-1",
            side="buy",
            order_type="limit",
            price=50000.0,
            size=0.5,
            status="open",
            created_at=now,
            plan_public_id="plan-1",
            session_id="s1",
            sequence_id=1,
            timestamp=now,
        )
        assert order.plan_public_id == "plan-1"

    def test_order_without_plan(self) -> None:
        """Legacy orders have null plan_public_id."""
        now = datetime.now(UTC)
        order = Order(
            instrument_public_id="inst-1",
            wallet_public_id="wallet-1",
            side="buy",
            order_type="limit",
            price=50000.0,
            size=0.5,
            status="open",
            created_at=now,
            session_id="s1",
            sequence_id=1,
            timestamp=now,
        )
        assert order.plan_public_id is None
