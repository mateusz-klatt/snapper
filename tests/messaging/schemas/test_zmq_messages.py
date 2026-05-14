"""Tests for ZMQ message data schemas."""

import json
from datetime import UTC
from datetime import datetime

import pytest
from pydantic import ValidationError

from snapper.messaging.schemas.data import AlertEventData
from snapper.messaging.schemas.data import CandleData
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import ExecutionPlanDecisionEventData
from snapper.messaging.schemas.data import FundingAccrualData
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.schemas.data import OrderCancelData
from snapper.messaging.schemas.data import OrderEventData
from snapper.messaging.schemas.data import OrderReplaceData
from snapper.messaging.schemas.data import OrderRequestData
from snapper.messaging.schemas.data import ProcessConfiguredEventData
from snapper.messaging.schemas.data import ProcessRunEventData
from snapper.messaging.schemas.data import ProcessSummaryEventData
from snapper.messaging.schemas.data import ProcessSummaryItem
from snapper.messaging.schemas.data import SignalData
from snapper.messaging.schemas.data import StrategyListEventData
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

    def test_tick_data_delay_defaults_and_override(self) -> None:
        """Default ``is_delayed``/``is_extended_hours`` + explicit override survive round-trip.

        Given: a TickData constructed without delay flags, and another with
            both ``is_delayed=True`` + ``is_extended_hours=True`` set,
        When: each is serialized to JSON and parsed back,
        Then: defaults remain (``False``/``None``) and overrides are preserved.
        """
        default_msg = TickData(
            session_id="",
            sequence_id=0,
            public_id="tick-default",
            timestamp=datetime(2026, 4, 21, tzinfo=UTC),
            instrument="MNQM6-CME",
            volume=0.0,
            exchange="kraken_equities",
        )
        assert default_msg.is_delayed is False
        assert default_msg.is_extended_hours is None
        round_default = TickData.from_json(default_msg.to_json())
        assert isinstance(round_default, TickData)
        assert round_default.is_delayed is False
        assert round_default.is_extended_hours is None

        override_msg = TickData(
            session_id="",
            sequence_id=1,
            public_id="tick-override",
            timestamp=datetime(2026, 4, 21, tzinfo=UTC),
            instrument="MNQM6-CME",
            volume=1.0,
            exchange="kraken_equities",
            is_delayed=True,
            is_extended_hours=True,
        )
        round_override = TickData.from_json(override_msg.to_json())
        assert isinstance(round_override, TickData)
        assert round_override.is_delayed is True
        assert round_override.is_extended_hours is True

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

    def test_signal_ai_review_attribution_defaults_to_none(self) -> None:
        """Test SignalData AI-review attribution defaults.

        Given: A SignalData without AI-review attribution kwargs,
        When: Constructed with only required + standard fields,
        Then: Both ``ai_review_public_id`` and
            ``ai_review_dispatch_version`` default to ``None`` so
            non-AI strategy emits remain backward-compatible at
            the schema level.
        """
        msg = SignalData(
            session_id="",
            sequence_id=0,
            public_id="test-default-attr",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            fired_at=datetime.now(UTC),
            strategy_name="rsi_reversion#1",
            instrument="BTCUSD",
            exchange="kraken",
            side="buy",
            strength=0.5,
            reason="defaults",
        )
        assert msg.ai_review_public_id is None
        assert msg.ai_review_dispatch_version is None

    def test_signal_ai_review_attribution_round_trip(self) -> None:
        """Test SignalData AI-review attribution survives round-trip.

        Given: A SignalData with both AI-review attribution fields set,
        When: Serialized to JSON and parsed back,
        Then: Both ``ai_review_public_id`` (str) and
            ``ai_review_dispatch_version`` (int) are preserved
            byte-equal across the wire boundary so the trader
            coordinator can read them on the receiving side.
        """
        msg = SignalData(
            session_id="",
            sequence_id=0,
            public_id="test-attr-rt",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            fired_at=datetime.now(UTC),
            strategy_name="rsi_reversion#1",
            instrument="BTCUSD",
            exchange="kraken",
            side="buy",
            strength=0.5,
            reason="ai-attributed",
            ai_review_public_id="01890000-7000-7000-8000-000000000000",
            ai_review_dispatch_version=2,
        )
        json_str = msg.to_json()
        parsed = SignalData.from_json(json_str)
        assert isinstance(parsed, SignalData)
        assert parsed.ai_review_public_id == "01890000-7000-7000-8000-000000000000"
        assert parsed.ai_review_dispatch_version == 2

    def test_signal_ai_review_attribution_unset_round_trip(self) -> None:
        """Test SignalData round-trip preserves None AI-review attribution.

        Given: A SignalData without AI-review attribution kwargs,
        When: Serialized and parsed,
        Then: Both attribution fields remain ``None`` post round-trip
            so the receiver cannot misinterpret a non-AI signal as
            an AI-attributed one due to schema serialization.
        """
        msg = SignalData(
            session_id="",
            sequence_id=0,
            public_id="test-attr-unset-rt",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            fired_at=datetime.now(UTC),
            strategy_name="rsi_reversion#1",
            instrument="BTCUSD",
            exchange="kraken",
            side="buy",
            strength=0.5,
            reason="non-ai",
        )
        parsed = SignalData.from_json(msg.to_json())
        assert parsed.ai_review_public_id is None
        assert parsed.ai_review_dispatch_version is None

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
            last_size=0.01,
            last_price=50000.0,
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
            "exchange": "walutomat",
            "instrument": "EUR-PLN",
            "exchange_order_id": "WALUTOMAT-111",
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


class TestMultiTenantFields:
    """Tests for the multi-tenant identity surface on schemas.

    The seven command/event/signal schemas (``SignalData``,
    ``ExecutionData``, ``OrderData``, ``OrderRequestData``,
    ``OrderCancelData``, ``OrderReplaceData``, ``OrderEventData``)
    each carry optional ``wallet_public_id`` / ``operator_public_id``
    / ``user_public_id`` fields. These tests lock in the default
    values and verify a non-default round trip survives JSON
    serialization on the ``SignalData`` and ``ExecutionData`` shapes
    — the same field semantics apply to the other five.
    """

    def test_signal_default_multi_tenant_fields(self) -> None:
        """SignalData defaults: empty wallet, None operator, None user.

        Given: A SignalData built without multi-tenant kwargs,
        When: The instance is constructed,
        Then: ``wallet_public_id`` is the empty-string sentinel and
            both operator/user IDs are ``None``.
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
            reason="default-test",
        )
        assert msg.wallet_public_id == ""
        assert msg.operator_public_id is None
        assert msg.user_public_id is None

    def test_signal_non_default_round_trip(self) -> None:
        """SignalData multi-tenant fields survive a JSON round trip.

        Given: A SignalData with explicit wallet/operator/user IDs,
        When: The message is serialized and re-parsed,
        Then: All three multi-tenant fields are preserved.
        """
        msg = SignalData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            fired_at=datetime.now(UTC),
            instrument="BTCUSD",
            exchange="kraken",
            side="sell",
            strength=0.9,
            reason="round-trip",
            wallet_public_id="wallet-7",
            operator_public_id="op-77",
            user_public_id="user-777",
        )
        parsed = SignalData.from_json(msg.to_json())
        assert parsed.wallet_public_id == "wallet-7"
        assert parsed.operator_public_id == "op-77"
        assert parsed.user_public_id == "user-777"

    def test_execution_default_multi_tenant_fields(self) -> None:
        """ExecutionData defaults: empty wallet, None operator, None user.

        Given: An ExecutionData built without multi-tenant kwargs,
        When: The instance is constructed,
        Then: All three fields take their safe defaults so
            existing fixtures continue to work.
        """
        msg = ExecutionData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            client_order_id="cid",
            instrument="BTCUSD",
            exchange="kraken",
            side="buy",
            size=1.0,
            price=50000.0,
            last_size=1.0,
            last_price=50000.0,
            fee=0.0,
            fee_asset="USD",
            status="filled",
            executed_at=datetime.now(UTC),
        )
        assert msg.wallet_public_id == ""
        assert msg.operator_public_id is None
        assert msg.user_public_id is None


class TestFundingAccrualData:
    """Tests for FundingAccrualData Pydantic schema."""

    def test_construction_with_all_fields(self) -> None:
        """Verify FundingAccrualData can be constructed with all fields."""
        now = datetime.now(UTC)
        msg = FundingAccrualData(
            public_id="test-id",
            timestamp=now,
            session_id="sess1",
            sequence_id=1,
            instrument="BTC-USD",
            exchange="kraken",
            mode="live",
            accrual_type="rollover",
            accrued_at=now,
            amount=25.0,
            amount_asset="USD",
            rate=0.00025,
            notional=100_000.0,
            position_quantity=1.0,
        )
        assert msg.type == "funding_accrual"
        assert msg.instrument == "BTC-USD"
        assert msg.exchange == "kraken"
        assert msg.accrual_type == "rollover"
        assert msg.amount == pytest.approx(25.0)

    def test_funding_accrual_type(self) -> None:
        """Verify funding accrual_type is accepted."""
        now = datetime.now(UTC)
        msg = FundingAccrualData(
            public_id="test-id",
            timestamp=now,
            session_id="sess1",
            sequence_id=1,
            instrument="PF_XBTUSD",
            exchange="kraken_futures",
            mode="live",
            accrual_type="funding",
            accrued_at=now,
            amount=-5.0,
            amount_asset="USD",
            rate=0.00001,
            notional=50_000.0,
            position_quantity=-0.5,
        )
        assert msg.accrual_type == "funding"
        assert msg.amount == pytest.approx(-5.0)


class TestAlertEventDataSchema:
    """iOS Push Foundation: ``AlertEventData`` dataclass + parse dispatch."""

    def _minimal(self) -> AlertEventData:
        """Return a fully-valid minimal ``AlertEventData`` fixture."""
        return AlertEventData(
            session_id="s1",
            sequence_id=7,
            public_id="envelope-pid",
            timestamp=datetime(2026, 4, 23, 12, tzinfo=UTC),
            user_public_id="019dbb34-f439-77bd-afa8-ee5321d60307",
            alert_type="order_fill_full",
            title="Filled",
            body="BTC-USD 0.1 filled",
        )

    def test_roundtrip_through_parse_message(self) -> None:
        """Serialize + deserialize through ``parse_message`` preserves fields."""
        original = self._minimal()

        parsed = parse_message(original.to_json())

        assert isinstance(parsed, AlertEventData)
        assert parsed.alert_type == "order_fill_full"
        assert parsed.title == "Filled"
        assert parsed.priority == "medium"
        assert parsed.is_safety_critical is False

    def test_critical_overrides_bypass_prefs_semantics(self) -> None:
        """``is_safety_critical=True`` + ``high`` priority both round-trip."""
        critical = AlertEventData(
            session_id="s1",
            sequence_id=7,
            public_id="envelope-pid",
            timestamp=datetime(2026, 4, 23, 12, tzinfo=UTC),
            user_public_id="019dbb34-f439-77bd-afa8-ee5321d60307",
            alert_type="critical_system_error",
            priority="high",
            is_safety_critical=True,
            title="Bus down",
            body="ZMQ bridge unreachable",
        )

        parsed = parse_message(critical.to_json())

        assert isinstance(parsed, AlertEventData)
        assert parsed.priority == "high"
        assert parsed.is_safety_critical is True

    def test_title_min_length_enforced(self) -> None:
        """Empty title is rejected by Pydantic (min_length=1)."""
        with pytest.raises(ValidationError) as exc:
            AlertEventData(
                session_id="s1",
                sequence_id=7,
                public_id="envelope-pid",
                timestamp=datetime(2026, 4, 23, 12, tzinfo=UTC),
                user_public_id="019dbb34-f439-77bd-afa8-ee5321d60307",
                alert_type="order_fill_full",
                title="",
                body="body",
            )

        assert "title" in str(exc.value).lower()

    def test_unknown_alert_type_rejected(self) -> None:
        """Literal enforcement blocks unknown ``alert_type``."""
        with pytest.raises(ValidationError):
            AlertEventData(
                session_id="s1",
                sequence_id=7,
                public_id="envelope-pid",
                timestamp=datetime(2026, 4, 23, 12, tzinfo=UTC),
                user_public_id="019dbb34-f439-77bd-afa8-ee5321d60307",
                alert_type="not_a_real_alert",
                title="Filled",
                body="body",
            )


class TestExecutionPlanDecisionEventDataSchema:
    """``ExecutionPlanDecisionEventData`` schema + dispatch."""

    def _minimal(self) -> ExecutionPlanDecisionEventData:
        """Return a fully-valid minimal decision event fixture."""
        return ExecutionPlanDecisionEventData(
            session_id="s1",
            sequence_id=7,
            public_id="019dbb34-f439-77bd-afa8-ee5321d60308",
            timestamp=datetime(2026, 4, 23, 12, tzinfo=UTC),
            decision_public_id="019dbb34-f439-77bd-afa8-ee5321d60309",
            plan_public_id="019dbb34-f439-77bd-afa8-ee5321d60307",
            decision_type="evaluator",
            trigger_type="tick",
            reason="sl_hit",
            triggered_at=datetime(2026, 4, 23, 12, tzinfo=UTC),
        )

    def test_roundtrip_through_parse_message(self) -> None:
        """Serialize + deserialize through ``parse_message`` preserves fields."""
        original = self._minimal()

        parsed = parse_message(original.to_json())

        assert isinstance(parsed, ExecutionPlanDecisionEventData)
        assert parsed.reason == "sl_hit"
        assert parsed.plan_public_id == "019dbb34-f439-77bd-afa8-ee5321d60307"
        assert parsed.decision_type == "evaluator"
        assert parsed.trigger_type == "tick"

    def test_free_form_reason_accepted(self) -> None:
        """``reason`` is free-form ``str`` (Plan v1.12 R10.B-3 closure)."""
        event = ExecutionPlanDecisionEventData(
            session_id="s1",
            sequence_id=7,
            public_id="019dbb34-f439-77bd-afa8-ee5321d60308",
            timestamp=datetime(2026, 4, 23, 12, tzinfo=UTC),
            decision_public_id="019dbb34-f439-77bd-afa8-ee5321d6030a",
            plan_public_id="019dbb34-f439-77bd-afa8-ee5321d60307",
            decision_type="lifecycle",
            trigger_type="execution",
            reason="Cycle 42 closed before command dispatch",
            triggered_at=datetime(2026, 4, 23, 12, tzinfo=UTC),
        )

        parsed = parse_message(event.to_json())

        assert isinstance(parsed, ExecutionPlanDecisionEventData)
        assert parsed.reason == "Cycle 42 closed before command dispatch"


class TestProcessAndStrategyEventSchemas:
    """Round-trip tests for the 2026-05-14 process/strategy event schemas.

    Q3 declarations register the schemas + topics; emit sites land in a
    follow-up. Verifying ``parse_message`` recognises the type
    discriminators NOW ensures the bridge will dispatch frames the
    moment producers start publishing.
    """

    def test_process_summary_event_roundtrip(self) -> None:
        """``ProcessSummaryEventData`` survives JSON roundtrip via parse_message."""
        event = ProcessSummaryEventData(
            session_id="s1",
            sequence_id=1,
            public_id="019dbb34-f439-77bd-afa8-ee5321d60311",
            timestamp=datetime(2026, 5, 14, 12, tzinfo=UTC),
            processes=[
                ProcessSummaryItem(
                    name="trader_coordinator",
                    running=True,
                    enabled=True,
                    role="core",
                    lifecycle="long_running",
                    active_public_id="019dbb34-f439-77bd-afa8-ee5321d60411",
                ),
                ProcessSummaryItem(
                    name="paper_backfill",
                    running=False,
                    enabled=False,
                    role="strategy",
                    lifecycle="one_shot",
                ),
            ],
            snapshot_at=datetime(2026, 5, 14, 12, tzinfo=UTC),
        )

        parsed = parse_message(event.to_json())

        assert isinstance(parsed, ProcessSummaryEventData)
        assert len(parsed.processes) == 2
        assert parsed.processes[0].name == "trader_coordinator"
        assert parsed.processes[0].running is True
        assert parsed.processes[0].role == "core"
        assert parsed.processes[1].running is False
        assert parsed.processes[1].active_public_id is None

    def test_process_configured_event_roundtrip(self) -> None:
        """``ProcessConfiguredEventData`` carries the process-name list."""
        event = ProcessConfiguredEventData(
            session_id="s1",
            sequence_id=2,
            public_id="019dbb34-f439-77bd-afa8-ee5321d60312",
            timestamp=datetime(2026, 5, 14, 12, tzinfo=UTC),
            process_names=["trader_coordinator", "feed_publisher"],
            snapshot_at=datetime(2026, 5, 14, 12, tzinfo=UTC),
        )

        parsed = parse_message(event.to_json())

        assert isinstance(parsed, ProcessConfiguredEventData)
        assert parsed.process_names == ["trader_coordinator", "feed_publisher"]

    def test_process_run_event_roundtrip(self) -> None:
        """``ProcessRunEventData`` carries a single run transition."""
        event = ProcessRunEventData(
            session_id="s1",
            sequence_id=3,
            public_id="019dbb34-f439-77bd-afa8-ee5321d60313",
            timestamp=datetime(2026, 5, 14, 12, tzinfo=UTC),
            process_name="trader_coordinator",
            run_id="run-42",
            status="started",
            started_at=datetime(2026, 5, 14, 11, tzinfo=UTC),
        )

        parsed = parse_message(event.to_json())

        assert isinstance(parsed, ProcessRunEventData)
        assert parsed.run_id == "run-42"
        assert parsed.status == "started"
        assert parsed.completed_at is None
        assert parsed.exit_code is None

    def test_strategy_list_event_roundtrip(self) -> None:
        """``StrategyListEventData`` carries the strategy class path roster."""
        event = StrategyListEventData(
            session_id="s1",
            sequence_id=4,
            public_id="019dbb34-f439-77bd-afa8-ee5321d60314",
            timestamp=datetime(2026, 5, 14, 12, tzinfo=UTC),
            strategy_classes=[
                "snapper.strategies.rsi_reversion.RSIReversion",
                "snapper.strategies.macd_crossover.MACDCrossover",
            ],
            snapshot_at=datetime(2026, 5, 14, 12, tzinfo=UTC),
        )

        parsed = parse_message(event.to_json())

        assert isinstance(parsed, StrategyListEventData)
        assert "snapper.strategies.rsi_reversion.RSIReversion" in parsed.strategy_classes
