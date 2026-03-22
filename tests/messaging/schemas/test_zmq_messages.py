"""Tests for ZMQ message data schemas."""

import json
from datetime import UTC
from datetime import datetime

import pytest

from snapper.messaging.schemas.data import CandleData
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.schemas.data import OrderCancelData
from snapper.messaging.schemas.data import OrderEventData
from snapper.messaging.schemas.data import OrderReplaceData
from snapper.messaging.schemas.data import OrderRequestData
from snapper.messaging.schemas.data import SignalData
from snapper.messaging.schemas.data import TickData
from snapper.messaging.schemas.messages import MessageParseError
from snapper.messaging.schemas.messages import parse_message


class TestMessages:
    """Tests for ZMQ message data schemas."""

    def test_market_data_message_serialization(self) -> None:
        """Test TickData JSON serialization.

        Given: A TickData with market data,
        When: Serialized to JSON,
        Then: JSON contains type, instrument, last, volume, and timestamp.
        """
        msg = TickData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            instrument="BTCUSD",
            volume=0.1,
            last=50000.0,
            exchange="kraken",
        )
        json_str = msg.to_json()
        data = json.loads(json_str)
        assert data["type"] == "tick"
        assert data["instrument"] == "BTCUSD"
        assert data["last"] == pytest.approx(50000.0)
        assert data["volume"] == pytest.approx(0.1)
        assert "timestamp" in data

    def test_market_data_bar_message(self) -> None:
        """Test CandleData round-trip serialization.

        Given: A CandleData with OHLCV data,
        When: Serialized to JSON and parsed back,
        Then: All fields are preserved.
        """
        msg = CandleData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            instrument="ETHUSD",
            exchange="kraken",
            volume=5.0,
            timeframe="1m",
            open=2990.0,
            high=3010.0,
            low=2985.0,
            close=3000.0,
            vwap=2998.5,
            trades=42,
            open_at=datetime.now(UTC),
        )
        json_str = msg.to_json()
        parsed = CandleData.from_json(json_str)
        assert isinstance(parsed, CandleData)
        assert parsed.type == "candle"
        assert parsed.instrument == "ETHUSD"
        assert parsed.timeframe == "1m"
        assert parsed.open == pytest.approx(2990.0)
        assert parsed.high == pytest.approx(3010.0)
        assert parsed.low == pytest.approx(2985.0)
        assert parsed.close == pytest.approx(3000.0)
        assert parsed.trades == 42

    def test_signal_message(self) -> None:
        """Test SignalData serialization.

        Given: A SignalData with strategy signal,
        When: Serialized and parsed,
        Then: Strategy name, side, and strength are preserved.
        """
        msg = SignalData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            fired_at=datetime.now(UTC),
            strategy_name="rsi_reversion#1",
            instrument="BTCUSD",
            exchange="kraken",
            side="buy",
            strength=0.85,
            reason="RSI below threshold",
        )
        json_str = msg.to_json()
        parsed = SignalData.from_json(json_str)
        assert isinstance(parsed, SignalData)
        assert parsed.strategy_name == "rsi_reversion#1"
        assert parsed.side == "buy"
        assert parsed.strength == pytest.approx(0.85)

    def test_order_request_message(self) -> None:
        """Test OrderRequestData serialization.

        Given: An OrderRequestData with order details,
        When: Serialized and parsed,
        Then: Strategy ID, mode, side, and quantity are preserved.
        """
        msg = OrderRequestData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            strategy_id="strategy_1",
            instrument="BTCUSD",
            mode="paper",
            side="buy",
            order_type="market",
            quantity=0.01,
            client_order_id="test_order_123",
            exchange="kraken",
        )
        json_str = msg.to_json()
        parsed = OrderRequestData.from_json(json_str)
        assert isinstance(parsed, OrderRequestData)
        assert parsed.strategy_id == "strategy_1"
        assert parsed.mode == "paper"
        assert parsed.side == "buy"
        assert parsed.quantity == pytest.approx(0.01)

    def test_fill_message(self) -> None:
        """Test ExecutionData serialization.

        Given: An ExecutionData with execution fill,
        When: Serialized and parsed,
        Then: Order ID, size, price, and status are preserved.
        """
        msg = ExecutionData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            trade_id="TRADE-XYZ",
            exchange_order_id="KRAKEN-ABC123",
            client_order_id="test_order_123",
            instrument="BTCUSD",
            exchange="kraken",
            side="buy",
            size=0.01,
            price=50000.0,
            fee=0.001,
            fee_asset="USD",
            status="filled",
            executed_at=datetime.now(UTC),
        )
        json_str = msg.to_json()
        parsed = ExecutionData.from_json(json_str)
        assert isinstance(parsed, ExecutionData)
        assert parsed.client_order_id == "test_order_123"
        assert parsed.size == pytest.approx(0.01)
        assert parsed.price == pytest.approx(50000.0)
        assert parsed.status == "filled"

    def test_heartbeat_message(self) -> None:
        """Test HeartbeatData serialization.

        Given: A HeartbeatData with health status,
        When: Serialized and parsed,
        Then: Component, sequence, and status are preserved.
        """
        msg = HeartbeatData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            component="feed.kraken.BTCUSD",
            sequence=12345,
            status="healthy",
            lag_ms=0,
            meta={"last_price": 50000.0},
        )
        json_str = msg.to_json()
        parsed = HeartbeatData.from_json(json_str)
        assert isinstance(parsed, HeartbeatData)
        assert parsed.component == "feed.kraken.BTCUSD"
        assert parsed.sequence == 12345
        assert parsed.status == "healthy"

    def test_parse_message_function(self) -> None:
        """Test parse_message type dispatch.

        Given: A serialized TickData,
        When: Parsed with parse_message,
        Then: Correct TickData type is returned.
        """
        tick_msg = TickData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            instrument="BTCUSD",
            exchange="kraken",
            volume=0.1,
            last=50000.0,
        )
        json_str = tick_msg.to_json()
        parsed = parse_message(json_str)
        assert isinstance(parsed, TickData)
        assert parsed.type == "tick"

    def test_parse_message_invalid_json(self) -> None:
        """Test parse_message handles invalid JSON.

        Given: An invalid JSON string,
        When: Parsed with parse_message,
        Then: MessageParseError is raised.
        """
        with pytest.raises(MessageParseError, match="Invalid JSON"):
            parse_message("invalid json")

    def test_parse_message_unknown_type(self) -> None:
        """Test parse_message handles unknown type.

        Given: A JSON message with unknown type,
        When: Parsed with parse_message,
        Then: MessageParseError is raised.
        """
        unknown_msg = {"type": "unknown", "data": "test"}
        json_str = json.dumps(unknown_msg)
        with pytest.raises(MessageParseError, match="Unknown message type: unknown"):
            parse_message(json_str)

    def test_parse_message_missing_type(self) -> None:
        """Test parse_message handles missing type field.

        Given: A JSON message without type field,
        When: Parsed with parse_message,
        Then: MessageParseError is raised.
        """
        msg_without_type = {"data": "test"}
        json_str = json.dumps(msg_without_type)
        with pytest.raises(MessageParseError, match="Message missing 'type' field"):
            parse_message(json_str)

    def test_message_timestamps(self) -> None:
        """Test explicit timestamp is preserved.

        Given: A TickData created with an explicit timestamp,
        When: Timestamp is accessed,
        Then: It equals the exact value provided.
        """
        explicit_ts = datetime(2024, 1, 1, tzinfo=UTC)
        msg = TickData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=explicit_ts,
            instrument="BTCUSD",
            exchange="kraken",
            volume=0.1,
            last=50000.0,
        )
        assert msg.timestamp == explicit_ts

    def test_signal_strength_validation(self) -> None:
        """Test SignalData strength validation.

        Given: A SignalData with strength value,
        When: Strength is outside valid range [0, 1],
        Then: ValueError is raised.
        """
        msg = SignalData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            fired_at=datetime.now(UTC),
            instrument="BTCUSD",
            exchange="kraken",
            side="buy",
            strength=0.5,
            reason="test",
        )
        assert msg.strength == pytest.approx(0.5)
        with pytest.raises(ValueError):
            SignalData(
                session_id="",
                sequence_id=0,
                public_id="test-public-id",
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                fired_at=datetime.now(UTC),
                instrument="BTCUSD",
                exchange="kraken",
                side="buy",
                strength=1.5,
                reason="test",
            )

    def test_order_quantity_validation(self) -> None:
        """Test OrderRequestData quantity validation.

        Given: An OrderRequestData with quantity,
        When: Quantity is negative,
        Then: ValueError is raised.
        """
        msg = OrderRequestData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            strategy_id="test",
            instrument="BTCUSD",
            mode="paper",
            side="buy",
            order_type="market",
            quantity=0.01,
            client_order_id="test",
            exchange="kraken",
        )
        assert msg.quantity == pytest.approx(0.01)
        with pytest.raises(ValueError):
            OrderRequestData(
                session_id="",
                sequence_id=0,
                public_id="test-public-id",
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                strategy_id="test",
                instrument="BTCUSD",
                mode="paper",
                side="buy",
                order_type="market",
                quantity=-0.01,
                client_order_id="test",
                exchange="kraken",
            )

    def test_order_cancel_message(self) -> None:
        """Test OrderCancelData serialization.

        Given: An OrderCancelData with cancel details,
        When: Serialized and parsed,
        Then: Exchange, instrument, and exchange_order_id are preserved.
        """
        msg = OrderCancelData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            exchange_order_id="KRAKEN-ABC123",
            client_order_id="client_order_456",
        )
        json_str = msg.to_json()
        parsed = OrderCancelData.from_json(json_str)
        assert isinstance(parsed, OrderCancelData)
        assert parsed.type == "order_cancel"
        assert parsed.exchange == "kraken"
        assert parsed.instrument == "BTC-USD"
        assert parsed.exchange_order_id == "KRAKEN-ABC123"
        assert parsed.client_order_id == "client_order_456"

    def test_order_cancel_parse_message(self) -> None:
        """Test OrderCancelData parsing via parse_message.

        Given: A JSON string with order_cancel type,
        When: Parsed via parse_message,
        Then: Returns OrderCancelData instance.
        """
        json_data = {
            "type": "order_cancel",
            "session_id": "",
            "sequence_id": 0,
            "public_id": "test-pid",
            "timestamp": "2024-01-01T00:00:00Z",
            "exchange": "paper",
            "instrument": "ETH-USD",
            "exchange_order_id": "PAPER-XYZ789",
            "client_order_id": "client_789",
        }
        msg = parse_message(json.dumps(json_data))
        assert isinstance(msg, OrderCancelData)
        assert msg.exchange_order_id == "PAPER-XYZ789"

    def test_order_replace_message(self) -> None:
        """Test OrderReplaceData serialization.

        Given: An OrderReplaceData with replace details,
        When: Serialized and parsed,
        Then: Exchange, instrument, exchange_order_id, and new values are preserved.
        """
        msg = OrderReplaceData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            exchange_order_id="KRAKEN-ABC123",
            client_order_id="client_order_456",
            new_quantity=0.5,
            new_price=48000.0,
        )
        json_str = msg.to_json()
        parsed = OrderReplaceData.from_json(json_str)
        assert isinstance(parsed, OrderReplaceData)
        assert parsed.type == "order_replace"
        assert parsed.exchange == "kraken"
        assert parsed.instrument == "BTC-USD"
        assert parsed.exchange_order_id == "KRAKEN-ABC123"
        assert parsed.new_quantity == pytest.approx(0.5)
        assert parsed.new_price == pytest.approx(48000.0)

    def test_order_replace_partial_update(self) -> None:
        """Test OrderReplaceData with only quantity update.

        Given: An OrderReplaceData with only new_quantity,
        When: Serialized and parsed,
        Then: new_quantity is set and new_price is None.
        """
        msg = OrderReplaceData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            exchange_order_id="KRAKEN-ABC123",
            client_order_id="client_456",
            new_quantity=1.0,
        )
        assert msg.new_quantity == pytest.approx(1.0)
        assert msg.new_price is None

    def test_order_replace_parse_message(self) -> None:
        """Test OrderReplaceData parsing via parse_message.

        Given: A JSON string with order_replace type,
        When: Parsed via parse_message,
        Then: Returns OrderReplaceData instance.
        """
        json_data = {
            "type": "order_replace",
            "session_id": "",
            "sequence_id": 0,
            "public_id": "test-pid",
            "timestamp": "2024-01-01T00:00:00Z",
            "exchange": "zonda",
            "instrument": "BTC-PLN",
            "exchange_order_id": "ZONDA-111",
            "client_order_id": "client_111",
            "new_price": 200000.0,
        }
        msg = parse_message(json.dumps(json_data))
        assert isinstance(msg, OrderReplaceData)
        assert msg.new_price == pytest.approx(200000.0)
        assert msg.new_quantity is None

    def test_order_event_message(self) -> None:
        """Test OrderEventData serialization.

        Given: An OrderEventData with event details,
        When: Serialized and parsed,
        Then: All fields are preserved correctly.
        """
        msg = OrderEventData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange_order_id="KRAKEN-ABC123",
            client_order_id="client_order_456",
            exchange="kraken",
            instrument="BTC-USD",
            event="cancelled",
        )
        json_str = msg.to_json()
        parsed = OrderEventData.from_json(json_str)
        assert isinstance(parsed, OrderEventData)
        assert parsed.type == "order_event"
        assert parsed.exchange_order_id == "KRAKEN-ABC123"
        assert parsed.client_order_id == "client_order_456"
        assert parsed.exchange == "kraken"
        assert parsed.instrument == "BTC-USD"
        assert parsed.event == "cancelled"
        assert parsed.reason is None

    def test_order_event_with_reason(self) -> None:
        """Test OrderEventData with rejection reason.

        Given: An OrderEventData with a reason,
        When: Serialized and parsed,
        Then: Reason is preserved.
        """
        msg = OrderEventData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange_order_id="KRAKEN-ABC123",
            client_order_id="client_456",
            exchange="kraken",
            instrument="BTC-USD",
            event="rejected",
            reason="Insufficient balance",
        )
        assert msg.event == "rejected"
        assert msg.reason == "Insufficient balance"

    def test_order_event_parse_message(self) -> None:
        """Test OrderEventData parsing via parse_message.

        Given: A JSON string with order_event type,
        When: Parsed via parse_message,
        Then: Returns OrderEventData instance.
        """
        json_data = {
            "type": "order_event",
            "session_id": "",
            "sequence_id": 0,
            "public_id": "test-pid",
            "timestamp": "2024-01-01T00:00:00Z",
            "exchange_order_id": "PAPER-XYZ789",
            "client_order_id": "client_789",
            "exchange": "paper",
            "instrument": "ETH-USD",
            "event": "replaced",
        }
        msg = parse_message(json.dumps(json_data))
        assert isinstance(msg, OrderEventData)
        assert msg.exchange_order_id == "PAPER-XYZ789"
        assert msg.event == "replaced"
