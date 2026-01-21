"""Tests for ZMQ message envelope schemas."""

import json
from datetime import UTC
from datetime import datetime

import pytest

from snapper.messaging.schemas.messages import BarEnvelope
from snapper.messaging.schemas.messages import FillEnvelope
from snapper.messaging.schemas.messages import HeartbeatEnvelope
from snapper.messaging.schemas.messages import MessageParseError
from snapper.messaging.schemas.messages import OrderRequestEnvelope
from snapper.messaging.schemas.messages import SignalEnvelope
from snapper.messaging.schemas.messages import TickEnvelope
from snapper.messaging.schemas.messages import parse_message


class TestMessages:
    """Tests for ZMQ message envelope schemas."""

    def test_market_data_message_serialization(self) -> None:
        """Test TickEnvelope JSON serialization.

        Given: A TickEnvelope with market data,
        When: Serialized to JSON,
        Then: JSON contains type, instrument, last, volume, and timestamp.
        """
        msg = TickEnvelope(instrument="BTCUSD", volume=0.1, last=50000.0, exchange="kraken")
        json_str = msg.to_json()
        data = json.loads(json_str)
        assert data["type"] == "tick"
        assert data["instrument"] == "BTCUSD"
        assert data["last"] == 50000.0
        assert data["volume"] == 0.1
        assert "timestamp" in data

    def test_market_data_bar_message(self) -> None:
        """Test BarEnvelope round-trip serialization.

        Given: A BarEnvelope with OHLCV data,
        When: Serialized to JSON and parsed back,
        Then: All fields are preserved.
        """
        msg = BarEnvelope(
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
        )
        json_str = msg.to_json()
        parsed = BarEnvelope.from_json(json_str)
        assert isinstance(parsed, BarEnvelope)
        assert parsed.type == "bar"
        assert parsed.instrument == "ETHUSD"
        assert parsed.timeframe == "1m"
        assert parsed.open == 2990.0
        assert parsed.high == 3010.0
        assert parsed.low == 2985.0
        assert parsed.close == 3000.0
        assert parsed.trades == 42

    def test_signal_message(self) -> None:
        """Test SignalEnvelope serialization.

        Given: A SignalEnvelope with strategy signal,
        When: Serialized and parsed,
        Then: Strategy name, side, and strength are preserved.
        """
        msg = SignalEnvelope(
            strategy_name="rsi_reversion#1",
            instrument="BTCUSD",
            exchange="kraken",
            side="buy",
            strength=0.85,
            reason="RSI below threshold",
        )
        json_str = msg.to_json()
        parsed = SignalEnvelope.from_json(json_str)
        assert isinstance(parsed, SignalEnvelope)
        assert parsed.strategy_name == "rsi_reversion#1"
        assert parsed.side == "buy"
        assert parsed.strength == 0.85

    def test_order_request_message(self) -> None:
        """Test OrderRequestEnvelope serialization.

        Given: An OrderRequestEnvelope with order details,
        When: Serialized and parsed,
        Then: Strategy ID, mode, side, and quantity are preserved.
        """
        msg = OrderRequestEnvelope(
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
        parsed = OrderRequestEnvelope.from_json(json_str)
        assert isinstance(parsed, OrderRequestEnvelope)
        assert parsed.strategy_id == "strategy_1"
        assert parsed.mode == "paper"
        assert parsed.side == "buy"
        assert parsed.quantity == 0.01

    def test_fill_message(self) -> None:
        """Test FillEnvelope serialization.

        Given: A FillEnvelope with execution fill,
        When: Serialized and parsed,
        Then: Order ID, size, price, and status are preserved.
        """
        msg = FillEnvelope(
            id="KRAKEN-ABC123",
            order_id="test_order_123",
            instrument="BTCUSD",
            exchange="kraken",
            side="buy",
            size=0.01,
            price=50000.0,
            fee=0.001,
            fee_asset="USD",
            status="filled",
        )
        json_str = msg.to_json()
        parsed = FillEnvelope.from_json(json_str)
        assert isinstance(parsed, FillEnvelope)
        assert parsed.order_id == "test_order_123"
        assert parsed.size == 0.01
        assert parsed.price == 50000.0
        assert parsed.status == "filled"

    def test_heartbeat_message(self) -> None:
        """Test HeartbeatEnvelope serialization.

        Given: A HeartbeatEnvelope with health status,
        When: Serialized and parsed,
        Then: Component, sequence, and status are preserved.
        """
        msg = HeartbeatEnvelope(
            component="feed.kraken.BTCUSD",
            sequence=12345,
            status="healthy",
            lag_ms=0,
            meta={"last_price": 50000.0},
        )
        json_str = msg.to_json()
        parsed = HeartbeatEnvelope.from_json(json_str)
        assert isinstance(parsed, HeartbeatEnvelope)
        assert parsed.component == "feed.kraken.BTCUSD"
        assert parsed.sequence == 12345
        assert parsed.status == "healthy"

    def test_parse_message_function(self) -> None:
        """Test parse_message type dispatch.

        Given: A serialized TickEnvelope,
        When: Parsed with parse_message,
        Then: Correct TickEnvelope type is returned.
        """
        tick_msg = TickEnvelope(
            instrument="BTCUSD",
            exchange="kraken",
            volume=0.1,
            last=50000.0,
        )
        json_str = tick_msg.to_json()
        parsed = parse_message(json_str)
        assert isinstance(parsed, TickEnvelope)
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
        """Test envelope timestamp defaults to now.

        Given: A TickEnvelope created without explicit timestamp,
        When: Timestamp is accessed,
        Then: It is within 1 second of current time.
        """
        msg = TickEnvelope(
            instrument="BTCUSD",
            exchange="kraken",
            volume=0.1,
            last=50000.0,
        )
        now = datetime.now(UTC)
        assert abs((msg.timestamp - now).total_seconds()) < 1.0

    def test_signal_strength_validation(self) -> None:
        """Test SignalEnvelope strength validation.

        Given: A SignalEnvelope with strength value,
        When: Strength is outside valid range [0, 1],
        Then: ValueError is raised.
        """
        msg = SignalEnvelope(
            instrument="BTCUSD", exchange="kraken", side="buy", strength=0.5, reason="test"
        )
        assert msg.strength == 0.5
        with pytest.raises(ValueError):
            SignalEnvelope(
                instrument="BTCUSD",
                exchange="kraken",
                side="buy",
                strength=1.5,
                reason="test",
            )

    def test_order_quantity_validation(self) -> None:
        """Test OrderRequestEnvelope quantity validation.

        Given: An OrderRequestEnvelope with quantity,
        When: Quantity is negative,
        Then: ValueError is raised.
        """
        msg = OrderRequestEnvelope(
            strategy_id="test",
            instrument="BTCUSD",
            mode="paper",
            side="buy",
            order_type="market",
            quantity=0.01,
            client_order_id="test",
            exchange="kraken",
        )
        assert msg.quantity == 0.01
        with pytest.raises(ValueError):
            OrderRequestEnvelope(
                strategy_id="test",
                instrument="BTCUSD",
                mode="paper",
                side="buy",
                order_type="market",
                quantity=-0.01,
                client_order_id="test",
                exchange="kraken",
            )
