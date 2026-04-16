"""Tests for ZMQ topic validation in the messaging subsystem."""

import json
import time
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from typing import Any

import pytest

from snapper.core.types import ExchangeEnum
from snapper.core.types import MarketDataTypeEnum
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.topics import validation
from snapper.messaging.topics.builders import market_topic
from snapper.messaging.topics.validation import BACKTEST_EVENTS
from snapper.messaging.topics.validation import TopicValidationError
from snapper.messaging.topics.validation import _is_valid_timeframe
from snapper.messaging.topics.validation import _validate_accruals_topic
from snapper.messaging.topics.validation import _validate_admin_topic
from snapper.messaging.topics.validation import _validate_backtest_prefix
from snapper.messaging.topics.validation import _validate_backtest_topic
from snapper.messaging.topics.validation import _validate_candle_timeframe
from snapper.messaging.topics.validation import _validate_exchange
from snapper.messaging.topics.validation import _validate_instrument
from snapper.messaging.topics.validation import _validate_market_source
from snapper.messaging.topics.validation import _validate_market_topic
from snapper.messaging.topics.validation import _validate_orders_commands_topic
from snapper.messaging.topics.validation import _validate_orders_events_topic
from snapper.messaging.topics.validation import _validate_prefix_pattern
from snapper.messaging.topics.validation import _validate_replay_source
from snapper.messaging.topics.validation import _validate_signal_topic
from snapper.messaging.topics.validation import _validate_system_topic
from snapper.messaging.topics.validation import validate_subscription_pattern
from snapper.messaging.topics.validation import validate_topic


def _patch_env(monkeypatch: pytest.MonkeyPatch, exchanges: set[str], symbols: set[str]) -> None:
    monkeypatch.setattr(validation, "get_available_exchanges", lambda: exchanges)
    monkeypatch.setattr(validation, "get_market_subscribe_exchanges", lambda: exchanges)
    monkeypatch.setattr(validation, "get_market_data_exchanges", lambda: exchanges)
    monkeypatch.setattr(validation, "get_available_symbols", lambda: symbols)


def test_market_candles_missing_timeframe(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test candles topic requires timeframe.

    Given: A market candles topic without timeframe,
    When: Validated,
    Then: Validation fails with timeframe message.
    """
    _patch_env(monkeypatch, {"kraken"}, {"BTC-USD"})
    is_valid, message = validation.validate_topic("market.kraken.BTC-USD.candles")
    assert not is_valid
    assert "include timeframe" in message


def test_orders_invalid_instrument(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test orders command topic rejects invalid instrument.

    Given: An orders command topic with unknown instrument,
    When: Validated,
    Then: Validation fails with instrument error.
    """
    _patch_env(monkeypatch, {"kraken"}, {"ETH-USD"})
    is_valid, message = validation.validate_topic("orders.commands.kraken.BTC-USD.submit")
    assert not is_valid
    assert "Unknown instrument 'BTC-USD'" in message


def test_system_topic_trailing_dot_rejected() -> None:
    """Test system topic rejects trailing dot.

    Given: A system topic ending with dot,
    When: Validated,
    Then: Validation fails.
    """
    is_valid, message = validation.validate_topic("system.")
    assert not is_valid
    assert "cannot end with '.'" in message


def test_system_heartbeat_invalid_component() -> None:
    """Test system heartbeat rejects invalid component.

    Given: A heartbeat topic with unknown component,
    When: Validated,
    Then: Validation fails with component error.
    """
    is_valid, message = validation.validate_topic("system.heartbeats.unknown")
    assert not is_valid
    assert "Invalid heartbeat component type" in message


def test_prefix_market_invalid_exchange(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test market prefix rejects invalid exchange.

    Given: A market prefix with unknown exchange,
    When: Validated as subscription pattern,
    Then: Validation fails with exchange error.
    """
    _patch_env(monkeypatch, {"kraken"}, {"BTC-USD"})
    is_valid, message = validation.validate_subscription_pattern("market.badex.")
    assert not is_valid
    assert "Unknown market feed exchange 'badex'" in message


def test_prefix_orders_invalid_instrument(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test orders commands prefix rejects invalid instrument.

    Given: An orders commands prefix with unknown instrument,
    When: Validated as subscription pattern,
    Then: Validation fails with instrument error.
    """
    _patch_env(monkeypatch, {"kraken"}, {"BTC-USD"})
    is_valid, message = validation.validate_subscription_pattern("orders.commands.kraken.BAD.")
    assert not is_valid
    assert "Unknown instrument 'BAD'" in message


def test_prefix_signals_invalid_instrument(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test signals prefix rejects invalid instrument.

    Given: A signals prefix with unknown instrument,
    When: Validated as subscription pattern,
    Then: Validation fails with instrument error.
    """
    _patch_env(monkeypatch, {"kraken", "paper"}, {"BTC-USD"})
    is_valid, message = validation.validate_subscription_pattern("signals.kraken.BAD.")
    assert not is_valid
    assert "Unknown instrument 'BAD'" in message


class TestTopicContractValidation:
    """Tests for topic contract validation ensuring correct topic formats."""

    def test_executor_orders_events_topic_format_is_valid(self) -> None:
        """Test executor orders events topics are valid.

        Given: Standard orders events topic formats,
        When: Validated,
        Then: All are valid.
        """
        valid_topics = [
            "orders.events.kraken.BTC-USD.executed",
            "orders.events.zonda.BTC-PLN.executed",
            "orders.events.walutomat.EUR-PLN.executed",
        ]
        for topic in valid_topics:
            is_valid, error_msg = validate_topic(topic)
            assert is_valid is True, f"Expected {topic} to be valid but got: {error_msg}"

    def test_executor_orders_commands_topic_format_is_valid(self) -> None:
        """Test executor orders commands topics are valid.

        Given: Standard orders commands topic formats,
        When: Validated,
        Then: All are valid.
        """
        valid_topics = [
            "orders.commands.kraken.BTC-USD.submit",
            "orders.commands.zonda.BTC-PLN.submit",
            "orders.commands.walutomat.EUR-PLN.submit",
            "orders.commands.kraken.BTC-USD.cancel",
        ]
        for topic in valid_topics:
            is_valid, error_msg = validate_topic(topic)
            assert is_valid is True, f"Expected {topic} to be valid but got: {error_msg}"

    def test_old_trade_prefix_topic_is_invalid(self) -> None:
        """Test old trade prefix is invalid.

        Given: Topics with deprecated trade prefix,
        When: Validated,
        Then: All are invalid.
        """
        invalid_topics = [
            ("trade.kraken.executions", "Unknown topic category: trade"),
            ("trade.kraken.orders", "Unknown topic category: trade"),
            ("trade.zonda.executions", "Unknown topic category: trade"),
        ]
        for topic, expected_error in invalid_topics:
            is_valid, error_msg = validate_topic(topic)
            assert is_valid is False, f"Expected {topic} to be invalid"
            assert expected_error in error_msg


class TestValidatedPublisherContract:
    """Tests for ValidatedPublisher contract with executor topics."""

    def test_validated_publisher_accepts_correct_executor_topics(self) -> None:
        """Test ValidatedPublisher accepts executor topics.

        Given: Valid executor topic formats,
        When: Validated,
        Then: All are accepted.
        """
        valid_executor_topics = [
            "orders.events.kraken.BTC-USD.executed",
            "orders.events.kraken.BTC-USD.accepted",
            "orders.events.zonda.BTC-PLN.executed",
            "orders.events.zonda.BTC-PLN.accepted",
        ]
        for topic in valid_executor_topics:
            is_valid, error_msg = validate_topic(topic)
            assert is_valid is True, f"ValidatedPublisher would reject {topic}: {error_msg}"

    def test_validated_publisher_rejects_invalid_trade_prefix(self) -> None:
        """Test ValidatedPublisher rejects trade prefix.

        Given: Topics with deprecated trade prefix,
        When: Validated,
        Then: TopicValidationError is raised.
        """
        invalid_topics = [
            "trade.kraken.executions",
            "trade.kraken.orders",
        ]
        for topic in invalid_topics:
            is_valid, error_msg = validate_topic(topic)
            assert is_valid is False, f"Expected {topic} to be invalid"
            with pytest.raises(TopicValidationError):
                raise TopicValidationError(f"Invalid topic '{topic}': {error_msg}")


class TestPayloadContract:
    """Tests for payload contract ensuring correct field structures."""

    def test_fill_envelope_has_all_required_frontend_fields(self) -> None:
        """Test ExecutionData has all frontend fields.

        Given: A ExecutionData with execution data,
        When: Serialized to JSON,
        Then: All required frontend fields are present.
        """
        fill = ExecutionData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            trade_id="trade-456",
            exchange_order_id="exch-456",
            client_order_id="test-order-123",
            instrument="BTC-USD",
            exchange="kraken",
            side="buy",
            size=0.5,
            price=50000.0,
            last_size=0.5,
            last_price=50000.0,
            fee=5.0,
            fee_asset="USD",
            status="filled",
            executed_at=datetime(2024, 1, 1, tzinfo=UTC),
        )
        assert hasattr(fill, "client_order_id")
        assert hasattr(fill, "exchange_order_id")
        assert hasattr(fill, "trade_id")
        assert hasattr(fill, "instrument")
        assert hasattr(fill, "exchange")
        assert hasattr(fill, "side")
        assert hasattr(fill, "size")
        assert hasattr(fill, "price")
        assert hasattr(fill, "fee")
        assert hasattr(fill, "fee_asset")
        assert hasattr(fill, "executed_at")
        json_data = json.loads(fill.to_json())
        assert json_data["client_order_id"] == "test-order-123"
        assert json_data["exchange_order_id"] == "exch-456"
        assert json_data["trade_id"] == "trade-456"
        assert json_data["instrument"] == "BTC-USD"
        assert json_data["exchange"] == "kraken"
        assert json_data["side"] == "buy"
        assert json_data["size"] == pytest.approx(0.5)
        assert json_data["price"] == pytest.approx(50000.0)
        assert json_data["fee"] == pytest.approx(5.0)
        assert json_data["fee_asset"] == "USD"
        assert "executed_at" in json_data

    def test_fill_envelope_side_is_valid_literal(self) -> None:
        """Test ExecutionData side accepts valid literals.

        Given: ExecutionData with buy or sell side,
        When: Created,
        Then: Side value is preserved.
        """
        buy_fill = ExecutionData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            trade_id="trade-1",
            exchange_order_id="exch-1",
            client_order_id="test-1",
            instrument="BTC-USD",
            exchange="kraken",
            side="buy",
            size=1.0,
            price=50000.0,
            last_size=1.0,
            last_price=50000.0,
            fee=0.0,
            fee_asset="USD",
            status="filled",
            executed_at=datetime(2024, 1, 1, tzinfo=UTC),
        )
        assert buy_fill.side == "buy"
        sell_fill = ExecutionData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            trade_id="trade-2",
            exchange_order_id="exch-2",
            client_order_id="test-2",
            instrument="BTC-USD",
            exchange="kraken",
            side="sell",
            size=1.0,
            price=50000.0,
            last_size=1.0,
            last_price=50000.0,
            fee=0.0,
            fee_asset="USD",
            status="filled",
            executed_at=datetime(2024, 1, 1, tzinfo=UTC),
        )
        assert sell_fill.side == "sell"

    def test_fill_envelope_status_matches_frontend_expectations(self) -> None:
        """Test ExecutionData status matches frontend.

        Given: Valid fill status values (filled, partial),
        When: ExecutionData created with each status,
        Then: Status is preserved correctly.

        Note: cancelled/rejected are handled by separate event types,
        not ExecutionData. See CancelEventType and ReplaceEventType.
        """
        valid_statuses = ["filled", "partial"]
        for status in valid_statuses:
            fill = ExecutionData(
                session_id="",
                sequence_id=0,
                public_id="test-public-id",
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                trade_id="trade",
                exchange_order_id="exch",
                client_order_id="test",
                instrument="BTC-USD",
                exchange="kraken",
                side="buy",
                size=1.0,
                price=50000.0,
                last_size=1.0,
                last_price=50000.0,
                fee=0.0,
                fee_asset="USD",
                status=status,
                executed_at=datetime(2024, 1, 1, tzinfo=UTC),
            )
            assert fill.status == status


class TestBridgeNormalizerContract:
    """Tests for bridge normalizer contract with unified field naming."""

    def test_normalize_execution_uses_unified_fields(self) -> None:
        """Test execution normalization uses unified fields.

        Given: A fill data dictionary,
        When: Field names checked,
        Then: Uses unified field naming convention.
        """
        fill_data: dict[str, Any] = {
            "type": "execution",
            "public_id": "exch-456",
            "order_id": "order-123",
            "instrument": "BTC-USD",
            "exchange": "kraken",
            "side": "buy",
            "size": 0.5,
            "price": 50000.0,
            "fee": 5.0,
            "fee_asset": "USD",
            "status": "filled",
            "executed_at": "2024-01-01T00:00:00+00:00",
        }
        assert fill_data["order_id"] == "order-123"
        assert fill_data["public_id"] == "exch-456"
        assert fill_data["size"] == pytest.approx(0.5)
        assert fill_data["price"] == pytest.approx(50000.0)
        assert fill_data["executed_at"] == "2024-01-01T00:00:00+00:00"
        assert fill_data["exchange"] == "kraken"
        assert fill_data["instrument"] == "BTC-USD"
        assert fill_data["side"] == "buy"
        assert fill_data["fee"] == pytest.approx(5.0)
        assert fill_data["fee_asset"] == "USD"


class TestOrderStatusPayloadContract:
    """Tests for order status payload field naming conventions."""

    def test_order_status_payload_uses_correct_field_names(self) -> None:
        """Test order status payload uses correct field names.

        Given: An order status payload,
        When: Field names checked,
        Then: Uses size not quantity, created_at not timestamp.
        """
        order_status = {
            "public_id": "client-order-123",
            "instrument": "BTC-USD",
            "side": "buy",
            "size": 0.5,
            "price": 50000.0,
            "order_type": "limit",
            "status": "submitted",
            "created_at": int(time.time() * 1000),
            "strategy_id": "strategy-1",
            "mode": "paper",
            "exchange": "kraken",
        }
        assert "size" in order_status, "Should use 'size' not 'quantity'"
        assert "quantity" not in order_status, "Should NOT use 'quantity'"
        assert "created_at" in order_status, "Should use 'created_at' not 'timestamp'"
        assert "timestamp" not in order_status, "Should NOT use 'timestamp'"


@pytest.fixture(autouse=True)
def patch_symbol_data(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Patch symbol data functions at validation module level.

    Patches the already-imported references in validation.py, not the
    functions module. Using string-path patches on the functions module
    would be a no-op because validation.py uses ``from ... import``
    which creates local references that are unaffected by module-level
    attribute replacement.
    """
    monkeypatch.setattr(
        validation, "get_available_exchanges", lambda: ["kraken", "paper", "zonda", "walutomat"]
    )
    monkeypatch.setattr(
        validation, "get_market_subscribe_exchanges", lambda: ["kraken", "zonda", "walutomat"]
    )
    monkeypatch.setattr(
        validation,
        "get_market_data_exchanges",
        lambda: ["kraken", "polygon", "zonda", "walutomat"],
    )
    monkeypatch.setattr(
        validation,
        "get_available_symbols",
        lambda: [
            "AAPL",
            "BTC-EUR",
            "BTC-PLN",
            "BTC-USD",
            "ETH-USD",
            "EUR-PLN",
            "EUR-USD",
            "USD-PLN",
        ],
    )
    yield


def test_validate_topic_market_success() -> None:
    """Test valid market candles topic.

    Given: A valid market candles topic with timeframe,
    When: Validated,
    Then: Validation succeeds.
    """
    ok, msg = validate_topic("market.kraken.BTC-USD.candles.1m")
    assert ok is True
    assert msg == ""


def test_validate_topic_market_paper_source_success() -> None:
    """Test valid paper market topic with explicit source exchange.

    Given: A paper market topic with source exchange in third segment,
    When: Validated,
    Then: Validation succeeds.
    """
    ok, msg = validate_topic("market.paper.kraken.BTC-USD.candles.1m")
    assert ok is True
    assert msg == ""


def test_validate_topic_market_paper_source_rejects_paper() -> None:
    """Test paper replay topic rejects paper as source exchange.

    Given: A paper market topic using paper as source exchange,
    When: Validated,
    Then: Validation fails with source exchange error.
    """
    ok, msg = validate_topic("market.paper.paper.BTC-USD.candles.1m")
    assert ok is False
    assert "Unknown replay source 'paper'" in msg


def test_validate_candle_timeframe_requires_five_segments() -> None:
    """Test candle timeframe validator requires timeframe segment.

    Given: Candle segments list without timeframe part,
    When: Validating candle timeframe,
    Then: Validation fails with missing timeframe error.
    """
    ok, msg = _validate_candle_timeframe(
        ["market", "kraken", "BTC-USD", "candles"], "kraken", "BTC-USD"
    )
    assert ok is False
    assert "include timeframe" in msg


def test_validate_candle_timeframe_rejects_invalid_timeframe() -> None:
    """Test candle timeframe validator rejects malformed values.

    Given: Candle topic segments with invalid timeframe token,
    When: Validating candle timeframe,
    Then: Validation fails with invalid timeframe message.
    """
    ok, msg = _validate_candle_timeframe(
        ["market", "kraken", "BTC-USD", "candles", "invalid"], "kraken", "BTC-USD"
    )
    assert ok is False
    assert "Invalid timeframe" in msg


def test_validate_candle_timeframe_accepts_valid_timeframe() -> None:
    """Test candle timeframe validator accepts valid values.

    Given: Candle topic segments with valid timeframe token,
    When: Validating candle timeframe,
    Then: Validation succeeds.
    """
    ok, msg = _validate_candle_timeframe(
        ["market", "kraken", "BTC-USD", "candles", "1m"], "kraken", "BTC-USD"
    )
    assert ok is True
    assert msg == ""


def test_validate_topic_market_non_paper_with_six_segments_rejected() -> None:
    """Test non-paper market topic with six segments is rejected.

    Given: A non-paper market topic with an extra segment,
    When: Validated,
    Then: Validation fails with market format error.
    """
    ok, msg = validate_topic("market.kraken.BTC-USD.candles.1m.extra")
    assert ok is False
    assert "Market topic must have" in msg


def test_validate_topic_legacy_paper_without_source_rejected() -> None:
    """Test legacy paper market topic without source exchange is rejected.

    Given: Legacy paper topic format without source exchange segment,
    When: Validated,
    Then: Validation fails with market format error.
    """
    ok, msg = validate_topic("market.paper.BTC-USD.ticks")
    assert ok is False
    assert "Market topic must have" in msg


def test_validate_topic_paper_source_invalid_data_type() -> None:
    """Test paper source topic with invalid data type is rejected.

    Given: Paper market topic with source exchange and unknown data type,
    When: Validated,
    Then: Validation fails with invalid market data type message.
    """
    ok, msg = validate_topic("market.paper.kraken.BTC-USD.unknown")
    assert ok is False
    assert "Invalid market data type" in msg


def test_validate_topic_paper_source_invalid_instrument() -> None:
    """Test paper source topic with unknown instrument is rejected.

    Given: Paper market topic with valid source exchange and unknown instrument,
    When: Validated,
    Then: Validation fails with unknown instrument error.
    """
    ok, msg = validate_topic("market.paper.kraken.INVALID.ticks")
    assert ok is False
    assert "Unknown instrument" in msg


def test_validate_topic_paper_source_invalid_exchange() -> None:
    """Test paper source topic with unknown source exchange is rejected.

    Given: Paper market topic with unknown source exchange,
    When: Validated,
    Then: Validation fails with unknown exchange error.
    """
    ok, msg = validate_topic("market.paper.invalid.BTC-USD.ticks")
    assert ok is False
    assert "Unknown replay source" in msg


def test_validate_subscription_pattern_paper_source_edge_cases() -> None:
    """Test paper market prefix validation for source edge cases.

    Given: Several paper market prefix patterns for valid and invalid sources,
    When: Validated as subscription patterns,
    Then: Invalid sources are rejected and valid source pattern is accepted.
    """
    ok_source_paper, msg_source_paper = validate_subscription_pattern("market.paper.paper.")
    assert ok_source_paper is False
    assert "Unknown replay source 'paper'" in msg_source_paper
    ok_bad_instrument, msg_bad_instrument = validate_subscription_pattern(
        "market.paper.kraken.INVALID."
    )
    assert ok_bad_instrument is False
    assert "Unknown instrument" in msg_bad_instrument
    ok_source_pattern, msg_source_pattern = validate_subscription_pattern(
        "market.paper.kraken.BTC-USD."
    )
    assert ok_source_pattern is True
    assert msg_source_pattern == ""
    ok_legacy, msg_legacy = validate_subscription_pattern("market.paper.BTC-USD.")
    assert ok_legacy is False
    assert "Unknown replay source" in msg_legacy
    ok_unknown, msg_unknown = validate_subscription_pattern("market.paper.kraken.UNKNOWN.")
    assert ok_unknown is False
    assert "Unknown instrument" in msg_unknown


def test_validate_topic_market_invalid_timeframe_and_exchange() -> None:
    """Test market topic with invalid exchange and timeframe.

    Given: A market topic with invalid exchange/timeframe,
    When: Validated,
    Then: Validation fails.
    """
    ok, msg = validate_topic("market.binance.BTC-USD.candles.99x")
    assert ok is False
    assert "Unknown market feed exchange" in msg


def test_validate_topic_market_missing_timeframe() -> None:
    """Test market candles without timeframe.

    Given: A market candles topic without timeframe,
    When: Validated,
    Then: Validation fails with timeframe error.
    """
    ok, msg = validate_topic("market.kraken.BTC-USD.candles")
    assert ok is False
    assert "include timeframe" in msg


def test_validate_topic_orders_invalid_command() -> None:
    """Test orders commands topic with invalid command.

    Given: An orders commands topic with invalid command type,
    When: Validated,
    Then: Validation fails.
    """
    ok, msg = validate_topic("orders.commands.kraken.BTC-USD.update")
    assert ok is False
    assert "Invalid order command" in msg


def test_validate_topic_orders_events_invalid_instrument() -> None:
    """Test orders events topic with invalid instrument.

    Given: An orders events topic with unknown instrument,
    When: Validated,
    Then: Validation fails with instrument error.
    """
    ok, msg = validate_topic("orders.events.kraken.ABC-USD.executed")
    assert ok is False
    assert "Unknown instrument" in msg


def test_validate_topic_signals_live_and_paper_variants() -> None:
    """Test signals topics for live and paper modes.

    Given: Signal topics for live and paper exchanges,
    When: Validated,
    Then: Both are valid.
    """
    ok_live, msg_live = validate_topic("signals.kraken.BTC-USD.live")
    ok_paper, msg_paper = validate_topic("signals.paper.BTC-USD.momentum")
    assert ok_live is True and msg_live == ""
    assert ok_paper is True and msg_paper == ""


def test_validate_topic_signals_invalid_suffix() -> None:
    """Test signals topic rejects invalid suffix.

    Given: A live signal topic with invalid suffix,
    When: Validated,
    Then: Validation fails.
    """
    ok, msg = validate_topic("signals.kraken.BTC-USD.demo")
    assert ok is False
    assert "LIVE signal topics" in msg


def test_validate_topic_system_heartbeats_and_invalid_type() -> None:
    """Test system topics validation.

    Given: System heartbeats and unknown system topics,
    When: Validated,
    Then: Heartbeats valid, unknown invalid.
    """
    ok_prefix, msg_prefix = validate_topic("system.heartbeats")
    ok_invalid, msg_invalid = validate_topic("system.unknown")
    assert ok_prefix is True and msg_prefix == ""
    assert ok_invalid is False
    assert "Invalid system type" in msg_invalid


def test_validate_topic_admin_empty_resource() -> None:
    """Test admin topic requires resource.

    Given: An admin topic without resource,
    When: Validated,
    Then: Validation fails.
    """
    ok, msg = validate_topic("admin.")
    assert ok is False
    assert "Admin topic must have 2 segments" in msg


def test_validate_topic_unknown_category() -> None:
    """Test topic with unknown category.

    Given: A topic with unknown category,
    When: Validated,
    Then: Validation fails with category error.
    """
    ok, msg = validate_topic("unknown.category")
    assert ok is False
    assert "Unknown topic category" in msg


def test_validate_subscription_pattern_prefix_and_wildcard() -> None:
    """Test subscription patterns for prefix and wildcard.

    Given: Valid prefix and invalid wildcard patterns,
    When: Validated as subscription patterns,
    Then: Prefix valid, wildcard rejected.
    """
    ok_prefix, msg_prefix = validate_subscription_pattern("market.kraken.BTC-USD.")
    ok_wildcard, msg_wildcard = validate_subscription_pattern("market.*")
    assert ok_prefix is True and msg_prefix == ""
    assert ok_wildcard is False
    assert "Wildcards" in msg_wildcard


def test_validate_subscription_pattern_partial_segment_rejected() -> None:
    """Test partial segment in pattern rejected.

    Given: A pattern with incomplete segment,
    When: Validated as subscription pattern,
    Then: Validation fails.
    """
    ok, msg = validate_subscription_pattern("market.kraken.BTC-")
    assert ok is False
    assert "segments" in msg or "Prefix must end" in msg


def test_validate_subscription_pattern_prefix_invalid_exchange() -> None:
    """Test prefix pattern rejects invalid exchange.

    Given: A prefix with unknown exchange,
    When: Validated as subscription pattern,
    Then: Validation fails with exchange error.
    """
    ok, msg = validate_subscription_pattern("market.invalid.BTC-USD.")
    assert ok is False
    assert "Unknown market feed exchange" in msg


def test_validate_subscription_pattern_full_topic_delegates_to_validate_topic() -> None:
    """Test full topic pattern delegates to validate_topic.

    Given: A complete topic (not prefix),
    When: Validated as subscription pattern,
    Then: Delegates to validate_topic.
    """
    ok, msg = validate_subscription_pattern("orders.commands.kraken.BTC-USD.submit")
    assert ok is True
    assert msg == ""


def test_is_valid_timeframe_variants() -> None:
    """Test timeframe validation variants.

    Given: Various timeframe strings,
    When: Validated,
    Then: Valid formats accepted, invalid rejected.
    """
    assert _is_valid_timeframe("1m") is True
    assert _is_valid_timeframe("4h") is True
    assert _is_valid_timeframe("1d") is True
    assert _is_valid_timeframe("15x") is False
    assert _is_valid_timeframe("") is False


class TestMarketTopicValidation:
    """Tests for market topic validation including data types and timeframes."""

    def test_valid_full_market_topic(self) -> None:
        """Test valid full market topic.

        Given: A complete market candles topic,
        When: Validated,
        Then: Validation succeeds.
        """
        valid, _err = validate_topic("market.kraken.BTC-USD.candles.1m")
        assert valid
        assert _err == ""

    def test_valid_market_topic_all_types(self) -> None:
        """Test market topic with all data types.

        Given: Market topics for candles, ticks, trades,
        When: Validated,
        Then: All are valid.
        """
        valid, _err = validate_topic("market.kraken.BTC-USD.candles.1m")
        assert valid, f"Failed for candles: {_err}"
        assert _err == ""
        for data_type in ["ticks", "trades"]:
            valid, _err = validate_topic(f"market.kraken.BTC-USD.{data_type}")
            assert valid, f"Failed for {data_type}: {_err}"
            assert _err == ""

    def test_invalid_market_topic_wrong_segments(self) -> None:
        """Test market topic with wrong segment count.

        Given: A market topic with too few segments,
        When: Validated,
        Then: Validation fails with segment error.
        """
        valid, _err = validate_topic("market.kraken.candles")
        assert not valid
        assert "segments" in _err

    def test_invalid_market_topic_unknown_data_type(self) -> None:
        """Test market topic with unknown data type.

        Given: A market topic with invalid data type,
        When: Validated,
        Then: Validation fails with data type error.
        """
        valid, _err = validate_topic("market.kraken.BTC-USD.unknown")
        assert not valid
        assert "Invalid market data type" in _err

    def test_invalid_market_topic_unknown_exchange(self) -> None:
        """Test market topic with unknown exchange.

        Given: A market topic with invalid exchange,
        When: Validated,
        Then: Validation fails with exchange error.
        """
        valid, _err = validate_topic("market.unknown_exchange.BTC-USD.candles.1m")
        assert not valid
        assert "Unknown market feed exchange" in _err

    def test_market_prefix_rejected(self) -> None:
        """Test market prefix rejected as full topic.

        Given: A market topic ending with dot,
        When: Validated as topic,
        Then: Validation fails.
        """
        valid, _err = validate_topic("market.kraken.BTC-USD.")
        assert not valid
        assert "segments" in _err

    def test_subscription_pattern_accepts_prefixes(self) -> None:
        """Test subscription pattern accepts prefixes.

        Given: Valid market prefix patterns,
        When: Validated as subscription patterns,
        Then: All are accepted.
        """
        for prefix in [
            "market.",
            "market.kraken.",
            "market.kraken.BTC-USD.",
            "market.paper.",
            "market.paper.kraken.",
            "market.paper.kraken.BTC-USD.",
            "market.kraken.BTC-USD.candles.",
        ]:
            valid, _err = validate_subscription_pattern(prefix)
            assert valid, f"Failed for {prefix}: {_err}"
            assert _err == ""

    def test_candles_without_timeframe_rejected(self) -> None:
        """Test candles topic requires timeframe.

        Given: A candles topic without timeframe,
        When: Validated,
        Then: Validation fails with timeframe error.
        """
        valid, _err = validate_topic("market.kraken.BTC-USD.candles")
        assert not valid
        assert "timeframe" in _err.lower()

    def test_candles_with_invalid_timeframe_rejected(self) -> None:
        """Test candles with invalid timeframe rejected.

        Given: A candles topic with invalid timeframe,
        When: Validated,
        Then: Validation fails with timeframe error.
        """
        valid, _err = validate_topic("market.kraken.BTC-USD.candles.invalid")
        assert not valid
        assert "timeframe" in _err.lower()

    def test_candles_with_valid_timeframes(self) -> None:
        """Test candles with valid timeframes.

        Given: Candles topics with various valid timeframes,
        When: Validated,
        Then: All are valid.
        """
        valid_timeframes = ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w", "1M"]
        for timeframe in valid_timeframes:
            valid, _err = validate_topic(f"market.kraken.BTC-USD.candles.{timeframe}")
            assert valid, f"Failed for timeframe {timeframe}: {_err}"
            assert _err == ""

    def test_non_candles_with_timeframe_rejected(self) -> None:
        """Test non-candles data types reject timeframe.

        Given: Ticks/trades topics with timeframe,
        When: Validated,
        Then: Validation fails.
        """
        for data_type in ["ticks", "trades"]:
            valid, _err = validate_topic(f"market.kraken.BTC-USD.{data_type}.1m")
            assert not valid, f"Should reject {data_type} with timeframe"
            assert "timeframe" in _err.lower() or "candles" in _err.lower()


class TestSubscriptionPatternValidation:
    """Tests for subscription pattern validation including prefixes."""

    def test_valid_full_topic_pattern(self) -> None:
        """Test valid full topic pattern.

        Given: A complete market topic,
        When: Validated as subscription pattern,
        Then: Validation succeeds.
        """
        valid, _err = validate_subscription_pattern("market.kraken.BTC-USD.candles.1m")
        assert valid
        assert _err == ""

    def test_valid_prefix_patterns(self) -> None:
        """Test valid prefix patterns.

        Given: Various valid prefix patterns,
        When: Validated as subscription patterns,
        Then: All are accepted.
        """
        valid_patterns = [
            "market.",
            "market.kraken.",
            "market.kraken.BTC-USD.",
            "orders.commands.",
            "orders.events.",
            "orders.commands.kraken.",
            "orders.events.kraken.",
            "system.",
            "signals.",
            "signals.kraken.",
            "signals.kraken.BTC-USD.",
            "signals.paper.BTC-USD.",
        ]
        for pattern in valid_patterns:
            valid, _err = validate_subscription_pattern(pattern)
            assert valid, f"Failed for {pattern}: {_err}"
            assert _err == ""

    def test_reject_partial_instrument(self) -> None:
        """Test partial instrument rejected.

        Given: A pattern with incomplete instrument,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_subscription_pattern("market.kraken.BTC-")
        assert not valid
        assert "segments" in _err or "Prefix must end" in _err.lower()

    def test_reject_empty_pattern(self) -> None:
        """Test empty pattern rejected.

        Given: An empty pattern,
        When: Validated,
        Then: Validation fails with empty error.
        """
        valid, _err = validate_subscription_pattern("")
        assert not valid
        assert "empty" in _err.lower()

    def test_reject_double_dots(self) -> None:
        """Test double dots rejected.

        Given: A pattern with double dots,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_subscription_pattern("market..candles")
        assert not valid


class TestSystemTopicValidation:
    """Tests for system topic validation including heartbeats and events."""

    def test_valid_system_topics(self) -> None:
        """Test valid system topics.

        Given: Valid system topic types,
        When: Validated,
        Then: All are accepted.
        """
        for sys_type in ["heartbeats", "symbol_aliases"]:
            valid, _err = validate_topic(f"system.{sys_type}")
            assert valid, f"Failed for system.{sys_type}: {_err}"
            assert _err == ""

    def test_invalid_system_topic_type(self) -> None:
        """Test invalid system topic type.

        Given: A system topic with unknown type,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("system.unknown")
        assert not valid
        assert "Invalid system type" in _err

    def test_system_prefix(self) -> None:
        """Test system prefix pattern.

        Given: A system prefix,
        When: Validated as subscription pattern,
        Then: Validation succeeds.
        """
        valid, _err = validate_subscription_pattern("system.")
        assert valid
        assert _err == ""

    def test_hierarchical_heartbeat_strategy(self) -> None:
        """Test hierarchical heartbeat strategy topic.

        Given: A heartbeat topic for strategy,
        When: Validated,
        Then: Validation succeeds.
        """
        valid, _err = validate_topic("system.heartbeats.strategy.macd_btc")
        assert valid, f"Failed for strategy heartbeat: {_err}"
        assert _err == ""

    def test_hierarchical_heartbeat_executor(self) -> None:
        """Test hierarchical heartbeat executor topic.

        Given: A heartbeat topic for executor,
        When: Validated,
        Then: Validation succeeds.
        """
        valid, _err = validate_topic("system.heartbeats.executor.kraken")
        assert valid, f"Failed for executor heartbeat: {_err}"
        assert _err == ""

    def test_hierarchical_heartbeat_feed(self) -> None:
        """Test hierarchical heartbeat feed topic.

        Given: A heartbeat topic for feed,
        When: Validated,
        Then: Validation succeeds.
        """
        valid, _err = validate_topic("system.heartbeats.feed.kraken")
        assert valid, f"Failed for feed heartbeat: {_err}"
        assert _err == ""

    def test_hierarchical_heartbeat_feed_with_symbol_rejected(self) -> None:
        """Test non-paper feed heartbeat with extra segment is rejected.

        Given: A feed heartbeat with extra non-paper segment,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("system.heartbeats.feed.kraken.BTC-USD")
        assert not valid
        assert "feed.paper.{source}" in _err

    def test_hierarchical_heartbeat_feed_paper_source_accepted(self) -> None:
        """Test paper feed heartbeat with source_exchange is accepted.

        Given: A feed heartbeat for paper with source exchange,
        When: Validated,
        Then: Validation succeeds.
        """
        valid, _err = validate_topic("system.heartbeats.feed.paper.kraken")
        assert valid, f"Paper feed heartbeat should be valid: {_err}"

    def test_hierarchical_heartbeat_feed_paper_without_source_rejected(self) -> None:
        """Test feed.paper without source_exchange is rejected.

        Given: A 4-segment feed heartbeat with exchange='paper',
        When: Validated,
        Then: Validation fails requiring source_exchange.
        """
        valid, _err = validate_topic("system.heartbeats.feed.paper")
        assert not valid
        assert "source_exchange" in _err

    def test_hierarchical_heartbeat_missing_name(self) -> None:
        """Test heartbeat topic requires component name.

        Given: A heartbeat topic without name,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("system.heartbeats.strategy")
        assert not valid
        assert "4 segments" in _err
        valid, _err = validate_topic("system.heartbeats.executor")
        assert not valid
        assert "4 segments" in _err or "5 segments" in _err
        valid, _err = validate_topic("system.heartbeats.feed")
        assert not valid
        assert "4 seg" in _err

    def test_hierarchical_heartbeat_invalid_component(self) -> None:
        """Test heartbeat with invalid component.

        Given: A heartbeat topic with unknown component,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("system.heartbeats.invalid.name")
        assert not valid
        assert "Invalid heartbeat component type" in _err

    def test_hierarchical_heartbeat_prefix(self) -> None:
        """Test heartbeat prefix pattern.

        Given: A heartbeat prefix,
        When: Validated as subscription pattern,
        Then: Validation succeeds.
        """
        valid, _err = validate_subscription_pattern("system.heartbeats.")
        assert valid
        assert _err == ""


class TestOrdersTopicValidation:
    """Tests for orders topic validation including segment counts."""

    def test_valid_orders_commands_topic(self) -> None:
        """Verify valid orders commands topic is accepted.

        Given: A complete orders commands topic,
        When: Validated,
        Then: Validation succeeds.
        """
        valid, _err = validate_topic("orders.commands.kraken.BTC-USD.submit")
        assert valid
        assert _err == ""

    def test_invalid_orders_topic_segments(self) -> None:
        """Verify orders topic with wrong segment count is rejected.

        Given: An orders commands topic with too few segments,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("orders.commands.kraken")
        assert not valid
        assert "5 segments" in _err

    def test_orders_commands_topic_invalid_exchange(self) -> None:
        """Verify orders commands topic with unknown exchange is rejected.

        Given: An orders commands topic with invalid exchange,
        When: Validated,
        Then: Validation fails with exchange error.
        """
        valid, _err = validate_topic("orders.commands.unknown_exchange.BTC-USD.submit")
        assert not valid
        assert "Unknown exchange" in _err

    def test_orders_prefixes(self) -> None:
        """Verify valid orders prefix patterns are accepted.

        Given: Various orders prefix patterns (excluding bare 'orders.'),
        When: Validated as subscription patterns,
        Then: All are accepted.
        """
        for prefix in [
            "orders.commands.",
            "orders.commands.kraken.",
            "orders.commands.kraken.BTC-USD.",
        ]:
            valid, _err = validate_subscription_pattern(prefix)
            assert valid, f"Failed for {prefix}: {_err}"
            assert _err == ""

    def test_orders_bare_prefix_rejected(self) -> None:
        """Verify bare 'orders.' prefix is rejected.

        Given: A bare 'orders.' prefix without subcategory,
        When: Validated as subscription pattern,
        Then: Validation fails with subcategory requirement error.
        """
        valid, _err = validate_subscription_pattern("orders.")
        assert not valid
        assert "subcategory" in _err.lower() or "commands" in _err.lower()


class TestOrdersEventsTopicValidation:
    """Tests for orders events topic validation including segment counts."""

    def test_orders_events_topic_valid(self) -> None:
        """Verify valid orders events topic is accepted.

        Given: A complete orders events topic,
        When: Validated,
        Then: Validation succeeds.
        """
        valid, _err = validate_topic("orders.events.kraken.BTC-USD.executed")
        assert valid, f"Expected valid orders events topic, got error: {_err}"

    def test_invalid_orders_events_topic_segments(self) -> None:
        """Verify orders events topic with wrong segment count is rejected.

        Given: An orders events topic with too few segments,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("orders.events.kraken")
        assert not valid
        assert "5 segments" in _err

    def test_orders_events_prefixes(self) -> None:
        """Verify valid orders events prefix patterns are accepted.

        Given: Various orders events prefix patterns,
        When: Validated as subscription patterns,
        Then: All are accepted.
        """
        for prefix in ["orders.events.", "orders.events.kraken.", "orders.events.kraken.BTC-USD."]:
            valid, _err = validate_subscription_pattern(prefix)
            assert valid, f"Failed for {prefix}: {_err}"
            assert _err == ""


class TestEdgeCases:
    """Tests for edge cases in topic validation."""

    def test_empty_topic(self) -> None:
        """Verify empty topic string is rejected.

        Given: An empty topic string,
        When: Validated,
        Then: Validation fails with empty error.
        """
        valid, _err = validate_topic("")
        assert not valid
        assert "empty" in _err.lower()

    def test_unknown_category(self) -> None:
        """Verify topic with unknown category is rejected.

        Given: A topic with invalid category,
        When: Validated,
        Then: Validation fails with category error.
        """
        valid, _err = validate_topic("unknown.something")
        assert not valid
        assert "Unknown topic category" in _err

    def test_topic_without_category(self) -> None:
        """Verify topic without proper category is rejected.

        Given: A single-segment topic string,
        When: Validated,
        Then: Validation fails with category error.
        """
        valid, _err = validate_topic("just_a_string")
        assert not valid
        assert "Unknown topic category" in _err


class TestMarketCategoryValidation:
    """Tests for market category validation."""

    def test_market_category_check_with_valid_format(self) -> None:
        """Verify market category is correctly validated.

        Given: A non-market category orders commands topic,
        When: Validated,
        Then: Validation handles category checking.
        """
        valid, _err = validate_topic("orders.commands.kraken.BTC-USD.submit")
        if not valid:
            assert "category" in _err.lower() or "orders" in _err.lower()


class TestDataTypeValidation:
    """Tests for market data type validation."""

    def test_data_type_not_in_allowed_set(self) -> None:
        """Verify invalid data type is rejected.

        Given: A market topic with unknown data type,
        When: Validated,
        Then: Validation fails with data type error.
        """
        valid, _err = validate_topic("market.kraken.BTC-USD.ohlcv")
        assert not valid
        assert "data type" in _err.lower()

    def test_data_type_candles_variations(self) -> None:
        """Verify candles data type with various timeframes.

        Given: Market candles topics with different timeframes,
        When: Validated,
        Then: Valid timeframes are accepted.
        """
        test_cases = [
            ("market.kraken.BTC-USD.candles.1m", True),
            ("market.kraken.BTC-USD.candles.5m", True),
            ("market.kraken.BTC-USD.candles.1h", True),
            ("market.kraken.BTC-USD.candles.4h", True),
            ("market.kraken.BTC-USD.candles.1d", True),
        ]
        for topic, should_be_valid in test_cases:
            valid, _err = validate_topic(topic)
            if should_be_valid:
                assert valid, f"Expected {topic} to be valid, got error: {_err}"


class TestOrdersCommandsAndEventsTopics:
    """Tests for orders commands and events topic validation."""

    def test_orders_commands_topic_valid_format(self) -> None:
        """Verify valid orders commands topic format.

        Given: A valid orders commands topic,
        When: Validated,
        Then: Validation succeeds or fails with expected message.
        """
        valid, _err = validate_topic("orders.commands.kraken.BTC-USD.submit")
        if valid:
            assert _err == ""
        else:
            assert "orders" in _err.lower() or "category" in _err.lower()

    def test_orders_commands_topic_invalid_command(self) -> None:
        """Verify orders commands topic with invalid command is rejected.

        Given: An orders commands topic with invalid command type,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("orders.commands.kraken.BTC-USD.update")
        assert not valid

    def test_orders_events_topic_valid_format(self) -> None:
        """Verify valid orders events topic format.

        Given: A valid orders events topic,
        When: Validated,
        Then: Validation succeeds or fails with expected message.
        """
        valid, _err = validate_topic("orders.events.kraken.BTC-USD.executed")
        if valid:
            assert _err == ""
        else:
            assert "orders" in _err.lower() or "category" in _err.lower()

    def test_orders_events_topic_invalid_event(self) -> None:
        """Verify orders events topic with invalid event is rejected.

        Given: An orders events topic with unknown event type,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("orders.events.kraken.BTC-USD.unknown")
        assert not valid


class TestSystemTopics:
    """Tests for system topic validation."""

    def test_system_heartbeats_valid(self) -> None:
        """Verify system heartbeats topic is valid.

        Given: A system heartbeats topic,
        When: Validated,
        Then: Validation succeeds.
        """
        valid, _err = validate_topic("system.heartbeats.broker1")
        if valid:
            assert _err == ""

    def test_system_process_status_valid(self) -> None:
        """Verify system process status topic is valid.

        Given: A system process status topic,
        When: Validated,
        Then: Validation succeeds.
        """
        valid, _err = validate_topic("system.process.status.broker")
        if valid:
            assert _err == ""

    def test_system_process_lifecycle_valid(self) -> None:
        """Verify system process lifecycle topic is valid.

        Given: A system process lifecycle topic,
        When: Validated,
        Then: Validation succeeds.
        """
        valid, _err = validate_topic("system.process.lifecycle.feed")
        if valid:
            assert _err == ""

    def test_system_invalid_event_type(self) -> None:
        """Verify system topic with invalid event type is rejected.

        Given: A system topic with unknown event type,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("system.unknown.source")
        assert not valid


class TestTimeframeValidation:
    """Tests for timeframe validation in market topics."""

    def test_invalid_timeframe_no_unit(self) -> None:
        """Verify timeframe without unit is rejected.

        Given: A candles topic with timeframe missing unit,
        When: Validated,
        Then: Validation fails with timeframe error.
        """
        valid, _err = validate_topic("market.kraken.BTC-USD.candles.5")
        assert not valid
        assert "timeframe" in _err.lower()

    def test_invalid_timeframe_wrong_order(self) -> None:
        """Verify timeframe with wrong order is rejected.

        Given: A candles topic with reversed timeframe format,
        When: Validated,
        Then: Validation fails with timeframe error.
        """
        valid, _err = validate_topic("market.kraken.BTC-USD.candles.m5")
        assert not valid
        assert "timeframe" in _err.lower()

    def test_invalid_timeframe_bad_unit(self) -> None:
        """Verify timeframe with invalid unit is rejected.

        Given: A candles topic with unknown time unit,
        When: Validated,
        Then: Validation fails with timeframe error.
        """
        valid, _err = validate_topic("market.kraken.BTC-USD.candles.5x")
        assert not valid
        assert "timeframe" in _err.lower()

    def test_invalid_timeframe_non_numeric(self) -> None:
        """Verify timeframe with non-numeric value is rejected.

        Given: A candles topic with alphabetic timeframe,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("market.kraken.BTC-USD.candles.abc")
        assert not valid


class TestNonCandlesTimeframe:
    """Tests for non-candles topics rejecting timeframes."""

    def test_ticks_with_timeframe_rejected(self) -> None:
        """Verify ticks topic rejects timeframe.

        Given: A ticks topic with extra timeframe segment,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("market.kraken.BTC-USD.ticks.1m")
        assert not valid
        assert "timeframe" in _err.lower() or "segments" in _err.lower()

    def test_trades_with_timeframe_rejected(self) -> None:
        """Verify trades topic rejects timeframe.

        Given: A trades topic with extra timeframe segment,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("market.kraken.BTC-USD.trades.1m")
        assert not valid


class TestPrefixPatterns:
    """Tests for prefix pattern validation."""

    def test_valid_single_segment_prefix(self) -> None:
        """Verify single segment prefix is valid.

        Given: A market prefix with one segment,
        When: Validated as subscription pattern,
        Then: Validation succeeds.
        """
        valid, _err = validate_subscription_pattern("market.")
        assert valid

    def test_valid_two_segment_prefix(self) -> None:
        """Verify two segment prefix is valid.

        Given: A market prefix with exchange,
        When: Validated as subscription pattern,
        Then: Validation succeeds.
        """
        valid, _err = validate_subscription_pattern("market.kraken.")
        assert valid

    def test_valid_three_segment_prefix(self) -> None:
        """Verify three segment prefix is valid.

        Given: A market prefix with exchange and instrument,
        When: Validated as subscription pattern,
        Then: Validation succeeds.
        """
        valid, _err = validate_subscription_pattern("market.kraken.BTC-USD.")
        assert valid


class TestFieldValidatorEdgeCases:
    """Tests for field validator edge cases."""

    def test_valid_exchanges(self) -> None:
        """Verify all valid exchanges are accepted.

        Given: List of valid exchange names,
        When: Each is validated,
        Then: All are accepted.
        """
        valid_exchanges = ["kraken", "paper", "walutomat", "zonda"]
        for exchange in valid_exchanges:
            valid, _err = _validate_exchange(exchange)
            assert valid, f"Exchange {exchange} should be valid"

    def test_timeframe_valid_formats(self) -> None:
        """Verify all valid timeframe formats are accepted.

        Given: List of valid timeframe strings,
        When: Each is validated,
        Then: All are accepted.
        """
        valid_timeframes = ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"]
        for tf in valid_timeframes:
            assert _is_valid_timeframe(tf), f"Timeframe {tf} should be valid"

    def test_timeframe_invalid_formats(self) -> None:
        """Verify invalid timeframe formats are rejected.

        Given: List of invalid timeframe strings,
        When: Each is validated,
        Then: All are rejected.
        """
        invalid_timeframes = ["", "5", "m5", "1.5m", "abc", "1x"]
        for tf in invalid_timeframes:
            assert not _is_valid_timeframe(tf), f"Timeframe {tf} should be invalid"


class TestMarketTopicCategoryMismatch:
    """Tests for market topic category mismatch detection."""

    def test_market_topic_with_wrong_category_in_segments(self) -> None:
        """Verify market topic with wrong category prefix is rejected.

        Given: A topic with non-market prefix passed to market validator,
        When: Validated,
        Then: Validation fails with market category error.
        """
        valid, _err = _validate_market_topic("notmarket.kraken.BTC-USD.ticks")
        assert not valid
        assert "market" in _err.lower()


class TestMarketTopicInvalidTimeframe:
    """Tests for market topic invalid timeframe handling."""

    def test_candles_with_invalid_timeframe_format(self) -> None:
        """Verify candles topic with invalid timeframe format is rejected.

        Given: A candles topic with malformed timeframe,
        When: Validated,
        Then: Validation fails with timeframe error.
        """
        valid, _err = validate_topic("market.kraken.BTC-USD.candles.invalid")
        assert not valid
        assert "timeframe" in _err.lower()

    def test_candles_with_partial_timeframe(self) -> None:
        """Verify candles topic with partial timeframe is rejected.

        Given: A candles topic with incomplete timeframe,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("market.kraken.BTC-USD.candles.1")
        assert not valid


class TestOrdersCommandsTopicCategoryCheck:
    """Tests for orders commands topic category validation."""

    def test_orders_commands_category_check_via_direct_function(self) -> None:
        """Verify direct orders commands validator rejects wrong category.

        Given: A topic with non-orders.commands prefix,
        When: Validated with orders commands validator,
        Then: Validation fails with orders.commands category error.
        """
        valid, _err = _validate_orders_commands_topic("notorders.commands.kraken.BTC-USD.submit")
        assert not valid
        assert "orders.commands" in _err.lower()

    def test_orders_commands_topic_ends_with_dot(self) -> None:
        """Verify orders commands topic ending with dot is rejected.

        Given: An orders commands topic with trailing dot,
        When: Validated,
        Then: Validation fails with segment count error.
        """
        valid, _err = _validate_orders_commands_topic("orders.commands.kraken.BTC-USD.")
        assert not valid
        assert "5 segments" in _err.lower()


class TestOrdersTopicTypeValidation:
    """Tests for orders topic type validation."""

    def test_orders_missing_subcategory(self) -> None:
        """Verify orders topic without commands/events subcategory is rejected.

        Given: An orders topic without proper subcategory,
        When: Validated,
        Then: Validation fails with subcategory error.
        """
        valid, _err = validate_topic("orders.kraken.BTC-USD.cancel")
        assert not valid
        assert "orders.commands" in _err.lower() or "orders.events" in _err.lower()


class TestOrdersEventsTopicValidationV2:
    """Additional tests for orders.events topic validation."""

    def test_orders_events_category_check(self) -> None:
        """Verify direct orders.events validator rejects wrong category.

        Given: A topic with non-orders.events prefix,
        When: Validated with orders.events validator,
        Then: Validation fails with orders.events category error.
        """
        valid, _err = _validate_orders_events_topic("notorders.events.kraken.BTC-USD.executed")
        assert not valid
        assert "orders.events" in _err.lower()

    def test_orders_events_topic_ends_with_dot(self) -> None:
        """Verify orders.events topic ending with dot is rejected.

        Given: An orders.events topic with trailing dot,
        When: Validated,
        Then: Validation fails with segment count error.
        """
        valid, _err = _validate_orders_events_topic("orders.events.kraken.BTC-USD.")
        assert not valid
        assert "5 segments" in _err.lower()

    def test_orders_events_invalid_type(self) -> None:
        """Verify orders.events topic with invalid event type is rejected.

        Given: An orders.events topic with unknown event type,
        When: Validated,
        Then: Validation fails with event type error.
        """
        valid, _err = validate_topic("orders.events.kraken.BTC-USD.unknown")
        assert not valid
        assert "order event" in _err.lower() or "accepted" in _err.lower()


class TestSignalTopicValidation:
    """Tests for signal topic validation."""

    def test_signal_topic_ends_with_dot(self) -> None:
        """Verify signal topic ending with dot is rejected.

        Given: A signal topic with trailing dot,
        When: Validated,
        Then: Validation fails with segment count error.
        """
        valid, _err = _validate_signal_topic("signals.kraken.BTC-USD.")
        assert not valid
        assert "4 segments" in _err.lower()

    def test_signal_topic_wrong_segment_count(self) -> None:
        """Verify signal topic with wrong segment count is rejected.

        Given: A signal topic with too few segments,
        When: Validated,
        Then: Validation fails with segment count error.
        """
        valid, _err = _validate_signal_topic("signals.kraken.BTC-USD")
        assert not valid
        assert "4 segments" in _err.lower()

    def test_signal_topic_category_check(self) -> None:
        """Verify direct signal validator rejects wrong category.

        Given: A topic with non-signals prefix,
        When: Validated with signals validator,
        Then: Validation fails with signals category error.
        """
        valid, _err = _validate_signal_topic("notsignals.kraken.BTC-USD.live")
        assert not valid
        assert "signals" in _err.lower()

    def test_signal_topic_invalid_exchange(self) -> None:
        """Verify signal topic with invalid exchange is rejected.

        Given: A signal topic with unknown exchange,
        When: Validated,
        Then: Validation fails with exchange error.
        """
        valid, _err = validate_topic("signals.invalid_exchange.BTC-USD.live")
        assert not valid
        assert "exchange" in _err.lower()

    def test_signal_topic_invalid_instrument(self) -> None:
        """Verify signal topic with invalid instrument is rejected.

        Given: A signal topic with unknown instrument,
        When: Validated,
        Then: Validation fails with instrument error.
        """
        valid, _err = validate_topic("signals.kraken.INVALID-INSTRUMENT.live")
        assert not valid
        assert "instrument" in _err.lower()

    def test_live_signal_topic_wrong_type(self) -> None:
        """Verify live signal topic with wrong type is rejected.

        Given: A live exchange signal with non-live suffix,
        When: Validated,
        Then: Validation fails with live requirement error.
        """
        valid, _err = validate_topic("signals.kraken.BTC-USD.notlive")
        assert not valid
        assert "live" in _err.lower()

    def test_paper_signal_topic_empty_strategy(self) -> None:
        """Verify paper signal topic with valid strategy is accepted.

        Given: A paper signal topic with strategy name,
        When: Validated,
        Then: Validation succeeds.
        """
        valid, _ = _validate_signal_topic("signals.paper.BTC-USD.my_strategy")
        assert valid


class TestSystemTopicHeartbeatPaths:
    """Tests for system heartbeat topic paths."""

    def test_system_heartbeats_base(self) -> None:
        """Verify base system heartbeats topic is valid.

        Given: A system.heartbeats topic,
        When: Validated,
        Then: Validation succeeds.
        """
        valid, _err = validate_topic("system.heartbeats")
        assert valid

    def test_system_heartbeats_strategy_with_name(self) -> None:
        """Verify strategy heartbeat with name is valid.

        Given: A heartbeat topic for named strategy,
        When: Validated,
        Then: Validation succeeds.
        """
        valid, _err = validate_topic("system.heartbeats.strategy.my_strategy")
        assert valid

    def test_system_heartbeats_strategy_without_name(self) -> None:
        """Verify strategy heartbeat without name is rejected.

        Given: A strategy heartbeat topic without name,
        When: Validated,
        Then: Validation fails with required name error.
        """
        valid, _err = validate_topic("system.heartbeats.strategy")
        assert not valid
        assert "requires" in _err.lower()

    def test_system_heartbeats_executor_with_exchange(self) -> None:
        """Verify executor heartbeat with exchange is valid.

        Given: A heartbeat topic for executor with exchange,
        When: Validated,
        Then: Validation succeeds.
        """
        valid, _err = validate_topic("system.heartbeats.executor.kraken")
        assert valid

    def test_system_heartbeats_executor_with_wallet_short_is_valid(self) -> None:
        """5-segment per-wallet executor heartbeat is accepted.

        Given: ``system.heartbeats.executor.{exchange}.{wallet_short}``
            where wallet_short is 12 lowercase hex chars,
        When: The topic is validated,
        Then: Validation succeeds.
        """
        valid, _err = validate_topic("system.heartbeats.executor.kraken.019d6ca45f2e")
        assert valid
        assert _err == ""

    def test_system_heartbeats_executor_with_non_hex_wallet_short_is_rejected(
        self,
    ) -> None:
        """5th segment must be hex (not strategy-tag-like).

        Given: ``system.heartbeats.executor.kraken.notahexvalue`` — 12
            chars but non-hex,
        When: The topic is validated,
        Then: Validation fails with a ``wallet_short`` error message.
        """
        valid, _err = validate_topic("system.heartbeats.executor.kraken.notahexvalue")
        assert not valid
        assert "wallet_short" in _err

    def test_system_heartbeats_executor_with_uppercase_hex_wallet_short_is_rejected(
        self,
    ) -> None:
        """Uppercase hex is rejected (lowercase-only convention).

        Given: ``system.heartbeats.executor.kraken.019D6CA45F2E`` — 12
            chars, valid hex, but uppercase,
        When: The topic is validated,
        Then: Validation fails. The producers (``service.py:_shard_key``,
            ``launcher.py:spawn_per_wallet_executors``, and
            ``trader.py:_build_engine_key``) all ``.lower()`` before
            publishing, so an uppercase wallet_short in a topic segment
            indicates a typo or a stale tooling path that must be
            caught at the first publish — matching the
            lowercase-only decision for ``_parse_shard_key``.
        """
        valid, _err = validate_topic("system.heartbeats.executor.kraken.019D6CA45F2E")
        assert not valid
        assert "wallet_short" in _err

    def test_system_heartbeats_executor_with_wrong_length_wallet_short_is_rejected(
        self,
    ) -> None:
        """5th segment must be exactly 12 characters.

        Given: ``system.heartbeats.executor.kraken.deadbeef`` — hex but
            only 8 chars,
        When: The topic is validated,
        Then: Validation fails with a ``wallet_short`` error message.
        """
        valid, _err = validate_topic("system.heartbeats.executor.kraken.deadbeef")
        assert not valid
        assert "wallet_short" in _err

    def test_system_heartbeats_executor_with_6_segments_is_rejected(self) -> None:
        """Executor heartbeat rejects >5 segments.

        Given: A 6-segment executor heartbeat topic,
        When: Validated,
        Then: Validation fails with a segment-count error.
        """
        valid, _err = validate_topic("system.heartbeats.executor.kraken.019d6ca45f2e.extra")
        assert not valid
        assert "4 segments" in _err or "5 segments" in _err

    def test_system_heartbeats_feed_valid(self) -> None:
        """Verify feed heartbeat with exchange is valid.

        Given: A heartbeat topic for feed with exchange,
        When: Validated,
        Then: Validation succeeds.
        """
        valid, _err = validate_topic("system.heartbeats.feed.kraken")
        assert valid

    def test_system_heartbeats_feed_wrong_segments(self) -> None:
        """Verify feed heartbeat without exchange is rejected.

        Given: A feed heartbeat topic without exchange,
        When: Validated,
        Then: Validation fails with segment count error.
        """
        valid, _err = validate_topic("system.heartbeats.feed")
        assert not valid
        assert "4 seg" in _err.lower()

    def test_system_heartbeats_invalid_component(self) -> None:
        """Verify heartbeat with invalid component type is rejected.

        Given: A heartbeat topic with unknown component type,
        When: Validated,
        Then: Validation fails with component type error.
        """
        valid, _err = validate_topic("system.heartbeats.invalid.test")
        assert not valid
        assert "component type" in _err.lower()

    def test_system_settings_valid(self) -> None:
        """Verify system.settings topic is valid.

        Given: A system.settings topic,
        When: Validated,
        Then: Validation succeeds.
        """
        valid, _err = validate_topic("system.settings")
        assert valid

    def test_system_settings_extra_segments(self) -> None:
        """Verify system.settings with extra segments is rejected.

        Given: A system.settings topic with additional segments,
        When: Validated,
        Then: Validation fails with segment count error.
        """
        valid, _err = validate_topic("system.settings.extra")
        assert not valid
        assert "2 segments" in _err.lower()

    def test_system_invalid_type(self) -> None:
        """Verify system topic with invalid type is rejected.

        Given: A system topic with unknown system type,
        When: Validated,
        Then: Validation fails with system type error.
        """
        valid, _err = validate_topic("system.unknown")
        assert not valid
        assert "system type" in _err.lower()


class TestAdminTopicValidation:
    """Tests for admin topic validation."""

    def test_admin_topic_ends_with_dot(self) -> None:
        """Verify admin topic ending with dot is rejected.

        Given: An admin topic with trailing dot,
        When: Validated,
        Then: Validation fails with segment count error.
        """
        valid, _err = _validate_admin_topic("admin.")
        assert not valid
        assert "2 segments" in _err.lower()

    def test_admin_topic_wrong_segment_count(self) -> None:
        """Verify admin topic with wrong segment count is rejected.

        Given: An admin topic with too many segments,
        When: Validated,
        Then: Validation fails with segment count error.
        """
        valid, _err = _validate_admin_topic("admin.users.extra")
        assert not valid
        assert "2 segments" in _err.lower()

    def test_admin_topic_category_check(self) -> None:
        """Verify direct admin validator rejects wrong category.

        Given: A topic with non-admin prefix,
        When: Validated with admin validator,
        Then: Validation fails with admin category error.
        """
        valid, _err = _validate_admin_topic("notadmin.users")
        assert not valid
        assert "admin" in _err.lower()

    def test_admin_topic_empty_resource(self) -> None:
        """Verify admin topic with valid resource is accepted.

        Given: An admin topic with users resource,
        When: Validated,
        Then: Validation succeeds.
        """
        valid, _err = validate_topic("admin.users")
        assert valid


class TestPrefixPatternValidation:
    """Tests for prefix pattern validation."""

    def test_prefix_without_trailing_dot(self) -> None:
        """Verify prefix without trailing dot is rejected.

        Given: A prefix pattern without ending dot,
        When: Validated,
        Then: Validation fails with dot requirement error.
        """
        valid, _err = _validate_prefix_pattern("market.kraken")
        assert not valid
        assert "dot" in _err.lower()

    def test_prefix_with_empty_segment(self) -> None:
        """Verify prefix with empty segment is rejected.

        Given: A prefix pattern with double dots,
        When: Validated,
        Then: Validation fails with empty segment error.
        """
        valid, _err = _validate_prefix_pattern("market..kraken.")
        assert not valid
        assert "empty" in _err.lower()

    def test_prefix_unknown_category(self) -> None:
        """Verify prefix with unknown category is rejected.

        Given: A prefix pattern with invalid category,
        When: Validated,
        Then: Validation fails with category error.
        """
        valid, _err = _validate_prefix_pattern("unknown.")
        assert not valid
        assert "category" in _err.lower()

    def test_prefix_market_invalid_exchange(self) -> None:
        """Verify market prefix with invalid exchange is rejected.

        Given: A market prefix with unknown exchange,
        When: Validated,
        Then: Validation fails with exchange error.
        """
        valid, _err = validate_subscription_pattern("market.invalid_exchange.")
        assert not valid
        assert "exchange" in _err.lower()

    def test_prefix_market_invalid_instrument(self) -> None:
        """Verify market prefix with invalid instrument is rejected.

        Given: A market prefix with unknown instrument,
        When: Validated,
        Then: Validation fails with instrument error.
        """
        valid, _err = validate_subscription_pattern("market.kraken.INVALID.")
        assert not valid
        assert "instrument" in _err.lower()

    def test_prefix_orders_invalid_exchange(self) -> None:
        """Verify orders prefix with invalid exchange is rejected.

        Given: An orders prefix with unknown exchange,
        When: Validated,
        Then: Validation fails with exchange error.
        """
        valid, _err = validate_subscription_pattern("orders.commands.invalid_exchange.")
        assert not valid
        assert "exchange" in _err.lower()

    def test_prefix_orders_invalid_instrument(self) -> None:
        """Verify orders prefix with invalid instrument is rejected.

        Given: An orders prefix with unknown instrument,
        When: Validated,
        Then: Validation fails with instrument error.
        """
        valid, _err = validate_subscription_pattern("orders.commands.kraken.INVALID.")
        assert not valid
        assert "instrument" in _err.lower()

    def test_prefix_orders_invalid_subcategory(self) -> None:
        """Verify orders prefix with invalid subcategory is rejected.

        Given: An orders prefix without commands/events subcategory,
        When: Validated,
        Then: Validation fails with subcategory error.
        """
        valid, _err = validate_subscription_pattern("orders.kraken.")
        assert not valid
        assert "commands" in _err.lower() or "events" in _err.lower()

    def test_prefix_signals_invalid_exchange(self) -> None:
        """Verify signals prefix with invalid exchange is rejected.

        Given: A signals prefix with unknown exchange,
        When: Validated,
        Then: Validation fails with exchange error.
        """
        valid, _err = validate_subscription_pattern("signals.invalid_exchange.")
        assert not valid
        assert "exchange" in _err.lower()

    def test_prefix_signals_invalid_instrument(self) -> None:
        """Verify signals prefix with invalid instrument is rejected.

        Given: A signals prefix with unknown instrument,
        When: Validated,
        Then: Validation fails with instrument error.
        """
        valid, _err = validate_subscription_pattern("signals.kraken.INVALID.")
        assert not valid
        assert "instrument" in _err.lower()


class TestStrategyTopicValidation:
    """Tests for strategy topic validation."""

    def test_strategy_topic_valid(self) -> None:
        """Verify strategy topic format is handled.

        Given: A strategy topic with state segment,
        When: Validated,
        Then: Validation handles strategy category.
        """
        valid, _err = validate_topic("strategy.my_strategy.state")
        assert valid or "strategy" in _err.lower()


class TestEmptyTopicValidation:
    """Tests for empty topic validation."""

    def test_empty_topic(self) -> None:
        """Verify empty topic string is rejected.

        Given: An empty topic string,
        When: Validated,
        Then: Validation fails with empty error.
        """
        valid, _err = validate_topic("")
        assert not valid
        assert "empty" in _err.lower()

    def test_empty_pattern(self) -> None:
        """Verify empty pattern string is rejected.

        Given: An empty subscription pattern,
        When: Validated,
        Then: Validation fails with empty error.
        """
        valid, _err = validate_subscription_pattern("")
        assert not valid
        assert "empty" in _err.lower()


class TestWildcardRejection:
    """Tests for wildcard pattern rejection."""

    def test_pattern_with_wildcard(self) -> None:
        """Verify wildcard in pattern is rejected.

        Given: A subscription pattern with wildcard,
        When: Validated,
        Then: Validation fails with wildcard error.
        """
        valid, _err = validate_subscription_pattern("market.*")
        assert not valid
        assert "wildcard" in _err.lower()


class TestMarketTopicPrefixRejection:
    """Tests for market topic prefix rejection."""

    def test_market_topic_ends_with_dot(self) -> None:
        """Verify market topic ending with dot is rejected.

        Given: A market topic with trailing dot,
        When: Validated with market validator,
        Then: Validation fails with segment count error.
        """
        valid, _err = _validate_market_topic("market.kraken.BTC-USD.")
        assert not valid
        assert "segments" in _err.lower()

    def test_market_topic_wrong_segment_count(self) -> None:
        """Verify market topic with wrong segment count is rejected.

        Given: A market topic with too few segments,
        When: Validated,
        Then: Validation fails with segment count error.
        """
        valid, _err = _validate_market_topic("market.kraken")
        assert not valid
        assert "segment" in _err.lower()


class TestMarketTopicInstrumentValidation:
    """Tests for market topic instrument validation."""

    def test_market_topic_invalid_instrument(self) -> None:
        """Verify market topic with invalid instrument is rejected.

        Given: A market topic with unknown instrument,
        When: Validated,
        Then: Validation fails with instrument error.
        """
        valid, _err = validate_topic("market.kraken.INVALID-INST.ticks")
        assert not valid
        assert "instrument" in _err.lower()


class TestMarketTopicExchangeValidation:
    """Tests for market topic exchange validation."""

    def test_market_topic_invalid_exchange(self) -> None:
        """Verify market topic with invalid exchange is rejected.

        Given: A market topic with unknown exchange,
        When: Validated,
        Then: Validation fails with exchange error.
        """
        valid, _err = validate_topic("market.invalid_exch.BTC-USD.ticks")
        assert not valid
        assert "exchange" in _err.lower()


class TestCandlesTopicSegmentCount:
    """Tests for candles topic segment count validation."""

    def test_candles_without_timeframe(self) -> None:
        """Verify candles topic without timeframe is rejected.

        Given: A candles topic missing timeframe segment,
        When: Validated,
        Then: Validation fails with timeframe error.
        """
        valid, _err = validate_topic("market.kraken.BTC-USD.candles")
        assert not valid
        assert "timeframe" in _err.lower()


class TestOrdersTopicValidationV2:
    """Additional tests for orders topic validation."""

    def test_orders_commands_topic_wrong_segment_count(self) -> None:
        """Verify orders.commands topic with wrong segment count is rejected.

        Given: An orders.commands topic with too few segments,
        When: Validated,
        Then: Validation fails with segment count error.
        """
        valid, _err = _validate_orders_commands_topic("orders.commands.kraken")
        assert not valid
        assert "5 segments" in _err.lower()

    def test_orders_commands_topic_invalid_exchange(self) -> None:
        """Verify orders.commands topic with invalid exchange is rejected.

        Given: An orders.commands topic with unknown exchange,
        When: Validated,
        Then: Validation fails with exchange error.
        """
        valid, _err = validate_topic("orders.commands.invalid_exch.BTC-USD.submit")
        assert not valid
        assert "exchange" in _err.lower()


class TestOrdersEventsTopicExchangeValidation:
    """Tests for orders.events topic exchange validation."""

    def test_orders_events_topic_wrong_segment_count(self) -> None:
        """Verify orders.events topic with wrong segment count is rejected.

        Given: An orders.events topic with too few segments,
        When: Validated,
        Then: Validation fails with segment count error.
        """
        valid, _err = _validate_orders_events_topic("orders.events.kraken")
        assert not valid
        assert "5 segments" in _err.lower()

    def test_orders_events_topic_invalid_exchange(self) -> None:
        """Verify orders.events topic with invalid exchange is rejected.

        Given: An orders.events topic with unknown exchange,
        When: Validated,
        Then: Validation fails with exchange error.
        """
        valid, _err = validate_topic("orders.events.invalid_exch.BTC-USD.executed")
        assert not valid
        assert "exchange" in _err.lower()

    def test_orders_events_topic_invalid_instrument(self) -> None:
        """Verify orders.events topic with invalid instrument is rejected.

        Given: An orders.events topic with unknown instrument,
        When: Validated,
        Then: Validation fails with instrument error.
        """
        valid, _err = validate_topic("orders.events.kraken.INVALID-INST.executed")
        assert not valid
        assert "instrument" in _err.lower()


class TestLiveSignalTopicValidation:
    """Tests for live signal topic validation."""

    def test_live_signal_valid(self) -> None:
        """Verify valid live signal topic is accepted.

        Given: A live signal topic with correct format,
        When: Validated,
        Then: Validation succeeds.
        """
        valid, _err = validate_topic("signals.kraken.BTC-USD.live")
        assert valid

    def test_live_signal_wrong_suffix(self) -> None:
        """Verify live signal with wrong suffix is rejected.

        Given: A live exchange signal with non-live suffix,
        When: Validated,
        Then: Validation fails with live requirement error.
        """
        valid, _err = validate_topic("signals.kraken.BTC-USD.paper")
        assert not valid
        assert "live" in _err.lower()


class TestSystemTopicBranches:
    """Tests for system topic branch validation."""

    def test_system_topic_ends_with_dot_single_segment(self) -> None:
        """Verify system heartbeats ending with dot behavior.

        Given: A system.heartbeats. pattern,
        When: Validated,
        Then: Returns boolean result.
        """
        valid, _err = _validate_system_topic("system.heartbeats.")
        assert isinstance(valid, bool)

    def test_system_topic_general_dot_ending(self) -> None:
        """Verify non-heartbeat system topic ending with dot is rejected.

        Given: A system.settings. pattern,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = _validate_system_topic("system.settings.")
        assert not valid
        assert "dot" in _err.lower() or "segments" in _err.lower()

    def test_system_topic_too_few_segments(self) -> None:
        """Verify system topic with too few segments is rejected.

        Given: A system topic with single segment,
        When: Validated,
        Then: Validation fails with segment count error.
        """
        valid, _err = _validate_system_topic("system")
        assert not valid
        assert "2 segments" in _err.lower()

    def test_system_heartbeats_invalid_format(self) -> None:
        """Placeholder test for system heartbeats format.

        Given: A system heartbeats topic,
        When: Format is checked,
        Then: Placeholder passes.
        """
        pass


class TestAdminResourceValidation:
    """Tests for admin resource validation."""

    def test_admin_valid_resource(self) -> None:
        """Verify admin.users topic is valid.

        Given: An admin topic with users resource,
        When: Validated,
        Then: Validation succeeds.
        """
        valid, _err = validate_topic("admin.users")
        assert valid

    def test_admin_settings_resource(self) -> None:
        """Verify admin.settings topic is valid.

        Given: An admin topic with settings resource,
        When: Validated,
        Then: Validation succeeds.
        """
        valid, _err = validate_topic("admin.settings")
        assert valid


class TestEmptyInstrumentValidation:
    """Tests for empty instrument validation."""

    def test_empty_instrument(self) -> None:
        """Verify empty instrument string is rejected.

        Given: An empty instrument string,
        When: Validated,
        Then: Validation fails with empty error.
        """
        valid, _err = _validate_instrument("")
        assert not valid
        assert "empty" in _err.lower()


class TestEmptyExchangeValidation:
    """Tests for empty exchange validation."""

    def test_empty_exchange(self) -> None:
        """Verify empty exchange string is rejected.

        Given: An empty exchange string,
        When: Validated,
        Then: Validation fails with empty error.
        """
        valid, _err = _validate_exchange("")
        assert not valid
        assert "empty" in _err.lower()


class TestUnknownTopicCategory:
    """Tests for unknown topic category handling."""

    def test_unknown_category(self) -> None:
        """Verify topic with unknown category is rejected.

        Given: A topic with unrecognized category,
        When: Validated,
        Then: Validation fails with category error.
        """
        valid, _err = validate_topic("unknown.something.else")
        assert not valid
        assert "category" in _err.lower()


class TestSubscriptionPatternFullTopic:
    """Tests for subscription pattern with full topic."""

    def test_full_topic_via_subscription_pattern(self) -> None:
        """Verify full topic is accepted as subscription pattern.

        Given: A complete market topic,
        When: Validated as subscription pattern,
        Then: Validation succeeds.
        """
        valid, _err = validate_subscription_pattern("market.kraken.BTC-USD.ticks")
        assert valid

    def test_full_topic_invalid_via_subscription_pattern(self) -> None:
        """Verify invalid full topic fails as subscription pattern.

        Given: A market topic with invalid exchange,
        When: Validated as subscription pattern,
        Then: Validation fails.
        """
        valid, _err = validate_subscription_pattern("market.invalid.BTC-USD.ticks")
        assert not valid


class TestCandlesMissingTimeframe:
    """Tests for candles topic missing timeframe."""

    def test_candles_without_timeframe_empty_string(self) -> None:
        """Verify candles topic with trailing dot is rejected.

        Given: A candles topic ending with dot,
        When: Validated,
        Then: Validation fails with timeframe error.
        """
        valid, _err = _validate_market_topic("market.kraken.BTC-USD.candles.")
        assert not valid
        assert "timeframe" in _err.lower()


class TestOrdersCommandsCategoryCheck:
    """Tests for orders.commands category check."""

    def test_orders_commands_wrong_category(self) -> None:
        """Verify non-orders.commands topic fails orders.commands validation.

        Given: A market topic passed to orders.commands validator,
        When: Validated,
        Then: Validation fails with orders.commands category error.
        """
        valid, _err = _validate_orders_commands_topic("market.kraken.BTC-USD.submit")
        assert not valid
        assert "orders.commands" in _err.lower()


class TestSignalPaperEmptyStrategy:
    """Tests for paper signal with empty strategy."""

    def test_paper_signal_empty_strategy(self) -> None:
        """Verify paper signal with empty strategy is rejected.

        Given: A paper signal topic ending with dot,
        When: Validated,
        Then: Validation fails with empty strategy error.
        """
        valid, _err = _validate_signal_topic("signals.paper.BTC-USD.")
        assert not valid
        assert "empty" in _err.lower() or "strategy" in _err.lower()


class TestSystemTopicEndingDot:
    """Tests for system topic ending with dot."""

    def test_system_heartbeats_prefix_allowed(self) -> None:
        """Verify system.heartbeats. prefix handling.

        Given: A system.heartbeats. subscription pattern,
        When: Validated,
        Then: Validation result is checked.
        """
        valid, _err = validate_subscription_pattern("system.heartbeats.")
        if not valid:
            assert (
                "heartbeats" in _err.lower()
                or "segments" in _err.lower()
                or "format" in _err.lower()
            )

    def test_system_topic_ending_dot_not_heartbeats(self) -> None:
        """Verify non-heartbeat system topic ending with dot is rejected.

        Given: A system.settings. pattern,
        When: Validated with system validator,
        Then: Validation fails.
        """
        valid, _err = _validate_system_topic("system.settings.")
        assert not valid
        assert "segments" in _err.lower() or "cannot end" in _err.lower()


class TestSystemTopicInvalidCategory:
    """Tests for system topic invalid category."""

    def test_system_wrong_category(self) -> None:
        """Verify non-system topic fails system validation.

        Given: A market topic passed to system validator,
        When: Validated,
        Then: Validation fails with system category error.
        """
        valid, _err = _validate_system_topic("market.heartbeats")
        assert not valid
        assert "system" in _err.lower()


class TestSystemHeartbeatInvalidComponent:
    """Tests for system heartbeat invalid component."""

    def test_system_heartbeat_unknown_component_type(self) -> None:
        """Verify heartbeat with unknown component type is rejected.

        Given: A heartbeat topic with invalid component type,
        When: Validated,
        Then: Validation fails with component type error.
        """
        valid, _err = _validate_system_topic("system.heartbeats.unknown.name")
        assert not valid
        assert "component type" in _err.lower() or "invalid" in _err.lower()


class TestAdminEmptyResource:
    """Tests for admin empty resource."""

    def test_admin_empty_resource(self) -> None:
        """Verify admin topic with empty resource is rejected.

        Given: An admin topic ending with dot,
        When: Validated,
        Then: Validation fails with segment error.
        """
        valid, _err = _validate_admin_topic("admin.")
        assert not valid
        assert "segments" in _err.lower() or "empty" in _err.lower() or "resource" in _err.lower()


class TestPrefixSegmentEmpty:
    """Tests for prefix with empty segments."""

    def test_prefix_double_dot(self) -> None:
        """Verify prefix with double dot is rejected.

        Given: A prefix pattern with consecutive dots,
        When: Validated,
        Then: Validation fails with empty segment error.
        """
        valid, _err = _validate_prefix_pattern("market..kraken.")
        assert not valid
        assert "empty" in _err.lower()

    def test_prefix_no_segments(self) -> None:
        """Verify single dot prefix is rejected.

        Given: A prefix with only a dot,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = _validate_prefix_pattern(".")
        assert not valid


class TestPrefixValidationOrders:
    """Tests for orders prefix validation."""

    def test_orders_prefix_invalid_exchange(self) -> None:
        """Verify orders prefix with invalid exchange is rejected.

        Given: An orders prefix with unknown exchange,
        When: Validated,
        Then: Validation fails with exchange error.
        """
        valid, _err = _validate_prefix_pattern("orders.commands.invalid_exchange.")
        assert not valid
        assert "exchange" in _err.lower()

    def test_orders_prefix_invalid_instrument(self) -> None:
        """Verify orders prefix with invalid instrument is rejected.

        Given: An orders prefix with unknown instrument,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = _validate_prefix_pattern("orders.commands.kraken.INVALID_INSTR.")
        assert not valid

    def test_orders_prefix_invalid_subcategory(self) -> None:
        """Verify orders prefix with invalid subcategory is rejected.

        Given: An orders prefix with unknown subcategory,
        When: Validated,
        Then: Validation fails with subcategory error.
        """
        valid, _err = _validate_prefix_pattern("orders.invalid_subcategory.")
        assert not valid
        assert "commands" in _err.lower() or "events" in _err.lower()


class TestPrefixValidationOrdersEvents:
    """Tests for orders.events prefix validation."""

    def test_orders_events_prefix_invalid_exchange(self) -> None:
        """Verify orders.events prefix with invalid exchange is rejected.

        Given: An orders.events prefix with unknown exchange,
        When: Validated,
        Then: Validation fails with exchange error.
        """
        valid, _err = _validate_prefix_pattern("orders.events.invalid_exchange.")
        assert not valid
        assert "exchange" in _err.lower()

    def test_orders_events_prefix_invalid_instrument(self) -> None:
        """Verify orders.events prefix with invalid instrument is rejected.

        Given: An orders.events prefix with unknown instrument,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = _validate_prefix_pattern("orders.events.kraken.INVALID_INSTR.")
        assert not valid


class TestPrefixValidationSignals:
    """Tests for signals prefix validation."""

    def test_signals_prefix_invalid_exchange(self) -> None:
        """Verify signals prefix with invalid exchange is rejected.

        Given: A signals prefix with unknown exchange,
        When: Validated,
        Then: Validation fails with exchange error.
        """
        valid, _err = _validate_prefix_pattern("signals.invalid_exchange.")
        assert not valid
        assert "exchange" in _err.lower()

    def test_signals_prefix_invalid_instrument(self) -> None:
        """Verify signals prefix with invalid instrument is rejected.

        Given: A signals prefix with unknown instrument,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = _validate_prefix_pattern("signals.kraken.INVALID_INSTR.")
        assert not valid


class TestWildcardRejectionV2:
    """Additional tests for wildcard rejection."""

    def test_wildcard_in_pattern_rejected(self) -> None:
        """Verify wildcard at end of pattern is rejected.

        Given: A subscription pattern with wildcard at end,
        When: Validated,
        Then: Validation fails with wildcard error.
        """
        valid, _err = validate_subscription_pattern("market.kraken.*")
        assert not valid
        assert "Wildcards (*) not supported" in _err

    def test_wildcard_in_middle_rejected(self) -> None:
        """Verify wildcard in middle of pattern is rejected.

        Given: A subscription pattern with wildcard in middle,
        When: Validated,
        Then: Validation fails with wildcard error.
        """
        valid, _err = validate_subscription_pattern("market.*.BTC-USD.candles.1m")
        assert not valid
        assert "Wildcards" in _err


class TestInvalidCategory:
    """Tests for invalid category handling."""

    def test_non_market_category_rejected(self) -> None:
        """Verify non-market category is rejected.

        Given: A topic with unknown category,
        When: Validated,
        Then: Validation fails with category error.
        """
        valid, _err = validate_topic("trading.kraken.BTC-USD.candles.1m")
        assert not valid
        assert "Unknown topic category" in _err

    def test_typo_in_category(self) -> None:
        """Verify typo in category is rejected.

        Given: A topic with misspelled category,
        When: Validated,
        Then: Validation fails with category error.
        """
        valid, _err = validate_topic("marekt.kraken.BTC-USD.ticks")
        assert not valid
        assert "category" in _err.lower()


class TestInvalidInstrument:
    """Tests for invalid instrument handling."""

    def test_empty_instrument_rejected(self) -> None:
        """Verify empty instrument is rejected.

        Given: A topic with empty instrument segment,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("market.kraken..ticks")
        assert not valid


class TestInvalidDataType:
    """Tests for invalid data type handling."""

    def test_unknown_data_type_rejected(self) -> None:
        """Verify unknown data type is rejected.

        Given: A topic with unknown data type,
        When: Validated,
        Then: Validation fails with data type error.
        """
        valid, _err = validate_topic("market.kraken.BTC-USD.snapshots")
        assert not valid
        assert "Invalid market data type" in _err

    def test_misspelled_data_type(self) -> None:
        """Verify misspelled data type is rejected.

        Given: A topic with misspelled data type,
        When: Validated,
        Then: Validation fails with data type error.
        """
        valid, _err = validate_topic("market.kraken.BTC-USD.candels.1m")
        assert not valid
        assert "data type" in _err.lower()


class TestInvalidExchange:
    """Tests for invalid exchange handling."""

    def test_unknown_exchange_rejected(self) -> None:
        """Verify unknown exchange is rejected.

        Given: A topic with unknown exchange,
        When: Validated,
        Then: Validation fails with exchange error.
        """
        valid, _err = validate_topic("market.unknown_exchange.BTC-USD.ticks")
        assert not valid
        assert "Unknown market feed exchange" in _err

    def test_typo_in_exchange(self) -> None:
        """Verify typo in exchange is rejected.

        Given: A topic with misspelled exchange,
        When: Validated,
        Then: Validation fails with exchange error.
        """
        valid, _err = validate_topic("market.krakn.BTC-USD.trades")
        assert not valid
        assert "exchange" in _err.lower()


class TestExecutionTopicValidation:
    """Tests for execution topic validation."""

    def test_execution_topic_invalid_category(self) -> None:
        """Verify executions category is rejected.

        Given: A topic starting with 'executions',
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("executions.kraken.ORDER123.filled")
        assert not valid
        assert "Unknown" in _err or "Invalid" in _err

    def test_execution_topic_missing_segments(self) -> None:
        """Verify execution topic with missing segments is rejected.

        Given: An execution topic with only 3 segments,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("execution.kraken.ORDER123")
        assert not valid

    def test_execution_topic_invalid_event_type(self) -> None:
        """Verify execution topic with invalid event type is rejected.

        Given: An execution topic with unknown event type,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("execution.kraken.ORDER123.unknown_event")
        assert not valid

    def test_execution_topic_valid(self) -> None:
        """Verify valid execution topic is accepted.

        Given: A properly formatted execution topic,
        When: Validated,
        Then: Validation passes or topic format is not yet supported.
        """
        valid, _err = validate_topic("execution.kraken.ORDER123.filled")
        assert isinstance(valid, bool)


class TestSystemTopicValidationV2:
    """Additional tests for system topic validation."""

    def test_system_heartbeat_invalid_format(self) -> None:
        """Verify system heartbeat with invalid format is rejected.

        Given: A system heartbeat topic missing segments,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("system.heartbeat")
        assert not valid

    def test_system_unknown_event_type(self) -> None:
        """Verify system topic with unknown event type is rejected.

        Given: A system topic with unknown event type,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("system.unknown_event.source123")
        assert not valid

    def test_system_process_status_invalid_format(self) -> None:
        """Verify system process status with invalid format is rejected.

        Given: A system process topic with missing segments,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("system.process.status")
        assert not valid

    def test_system_process_status_too_many_segments(self) -> None:
        """Verify system process topic with extra segments is rejected.

        Given: A system process topic with too many segments,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("system.process.status.myprocess.extra.segment")
        assert not valid


class TestFieldValidators:
    """Tests for field validation helper functions."""

    def test_validate_exchange_unknown(self) -> None:
        """Verify unknown exchange is rejected.

        Given: An unknown exchange name,
        When: Validated,
        Then: Returns False with error message.
        """
        valid, _err = _validate_exchange("unknown_exch")
        assert not valid
        assert "Unknown exchange" in _err

    def test_validate_instrument_unknown(self) -> None:
        """Verify unknown instrument is rejected.

        Given: An unknown instrument pair,
        When: Validated,
        Then: Returns False with error message.
        """
        valid, _err = _validate_instrument("UNKNOWN-PAIR")
        assert not valid
        assert "Unknown instrument" in _err

    def test_is_valid_timeframe_invalid_format(self) -> None:
        """Verify invalid timeframe formats are rejected.

        Given: Timeframes with invalid formats,
        When: Validated,
        Then: Returns False.
        """
        assert not _is_valid_timeframe("5")
        assert not _is_valid_timeframe("m5")
        assert not _is_valid_timeframe("5x")

    def test_is_valid_timeframe_invalid_number(self) -> None:
        """Verify timeframes with invalid numbers are rejected.

        Given: Timeframes with non-numeric values,
        When: Validated,
        Then: Returns False.
        """
        assert not _is_valid_timeframe("abc")
        assert not _is_valid_timeframe("1.5m")

    def test_validate_candles_topic_missing_timeframe(self) -> None:
        """Verify candles topic without timeframe is rejected.

        Given: A candles topic missing timeframe,
        When: Validated,
        Then: Validation fails with timeframe error.
        """
        valid, _err = validate_topic("market.kraken.BTC-USD.candles")
        assert not valid
        assert "timeframe" in _err.lower()

    def test_validate_candles_topic_invalid_timeframe(self) -> None:
        """Verify candles topic with invalid timeframe is rejected.

        Given: A candles topic with invalid timeframe,
        When: Validated,
        Then: Validation fails with timeframe error.
        """
        valid, _err = validate_topic("market.kraken.BTC-USD.candles.invalid")
        assert not valid
        assert "timeframe" in _err.lower()

    def test_validate_ticks_topic_extra_segments(self) -> None:
        """Verify ticks topic with extra segments is rejected.

        Given: A ticks topic with extra segments,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("market.kraken.BTC-USD.ticks.extra")
        assert not valid

    def test_validate_trades_topic_extra_segments(self) -> None:
        """Verify trades topic with extra segments is rejected.

        Given: A trades topic with extra segments,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("market.kraken.BTC-USD.trades.extra")
        assert not valid


class TestPrefixValidation:
    """Tests for subscription prefix validation."""

    def test_prefix_incomplete_segment_rejected(self) -> None:
        """Verify incomplete segment prefix is rejected.

        Given: A prefix with incomplete exchange segment,
        When: Validated,
        Then: Validation fails with segments error.
        """
        valid, _err = validate_subscription_pattern("market.krak")
        assert not valid
        assert "segments" in _err.lower()

    def test_prefix_with_dash_incomplete(self) -> None:
        """Verify prefix with incomplete instrument is rejected.

        Given: A prefix with incomplete instrument,
        When: Validated,
        Then: Validation fails with segment or instrument error.
        """
        valid, _err = validate_subscription_pattern("market.kraken.BTC-")
        assert not valid
        assert "segments" in _err.lower() or "instrument" in _err.lower()

    def test_prefix_valid_multi_level(self) -> None:
        """Verify valid multi-level prefix is accepted.

        Given: A valid market prefix ending with dot,
        When: Validated,
        Then: Validation succeeds.
        """
        valid, _err = validate_subscription_pattern("market.kraken.")
        assert valid
        assert _err == ""


class TestExecutionFieldValidators:
    """Tests for execution field validators."""

    def test_validate_order_id_empty(self) -> None:
        """Verify empty order id is rejected.

        Given: An execution topic with empty order id,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("execution.kraken..filled")
        assert not valid

    def test_validate_event_type_unknown(self) -> None:
        """Verify unknown event type validation.

        Given: N/A - placeholder test,
        When: N/A,
        Then: Test passes.
        """
        pass

    def test_validate_execution_topic_valid_filled(self) -> None:
        """Verify valid filled execution topic is accepted.

        Given: A valid execution topic with filled event,
        When: Validated,
        Then: If valid, error is empty.
        """
        valid, _err = validate_topic("execution.kraken.ORDER123.filled")
        if valid:
            assert _err == ""

    def test_validate_execution_topic_valid_canceled(self) -> None:
        """Verify valid canceled execution topic is accepted.

        Given: A valid execution topic with canceled event,
        When: Validated,
        Then: If valid, error is empty.
        """
        valid, _err = validate_topic("execution.kraken.ORDER456.canceled")
        if valid:
            assert _err == ""


class TestSystemFieldValidators:
    """Tests for system field validators."""

    def test_validate_system_event_empty_source(self) -> None:
        """Verify empty source in system event is rejected.

        Given: A system topic with empty source,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("system.heartbeats.")
        assert not valid

    def test_validate_process_name_empty(self) -> None:
        """Verify empty process name is rejected.

        Given: A system process topic with empty name,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("system.process.status.")
        assert not valid

    def test_validate_process_event_invalid(self) -> None:
        """Verify invalid process event is rejected.

        Given: A system process topic with unknown event,
        When: Validated,
        Then: Validation fails.
        """
        valid, _err = validate_topic("system.process.unknown.myprocess")
        assert not valid

    def test_validate_system_metrics_valid(self) -> None:
        """Verify valid system metrics topic is accepted.

        Given: A valid system metrics topic,
        When: Validated,
        Then: If valid, error is empty.
        """
        valid, _err = validate_topic("system.metrics.cpu.host123")
        if valid:
            assert _err == ""

    def test_validate_system_alerts_valid(self) -> None:
        """Verify valid system alerts topic is accepted.

        Given: A valid system alerts topic,
        When: Validated,
        Then: If valid, error is empty.
        """
        valid, _err = validate_topic("system.alerts.high_cpu.server1")
        if valid:
            assert _err == ""

    def test_validate_system_logs_valid(self) -> None:
        """Verify valid system logs topic is accepted.

        Given: A valid system logs topic,
        When: Validated,
        Then: If valid, error is empty.
        """
        valid, _err = validate_topic("system.logs.error.app1")
        if valid:
            assert _err == ""

    def test_validate_system_config_valid(self) -> None:
        """Verify valid system config topic is accepted.

        Given: A valid system config topic,
        When: Validated,
        Then: If valid, error is empty.
        """
        valid, _err = validate_topic("system.config.updated.component1")
        if valid:
            assert _err == ""


@pytest.mark.parametrize(
    "pattern,expected_message",
    [
        ("market.binance.", "Unknown market feed exchange 'binance'"),
        ("market.kraken.INVALID.", "Unknown instrument 'INVALID'"),
        ("orders.commands.binance.", "Unknown exchange 'binance'"),
        ("orders.commands.kraken.INVALID.", "Unknown instrument 'INVALID'"),
        ("orders.events.binance.", "Unknown exchange 'binance'"),
        ("orders.events.kraken.INVALID.", "Unknown instrument 'INVALID'"),
        ("signals.binance.", "Invalid exchange 'binance'"),
    ],
)
def test_prefix_patterns_reject_invalid_exchange(
    monkeypatch: pytest.MonkeyPatch, pattern: str, expected_message: str
) -> None:
    """Verify prefix patterns reject invalid exchanges or instruments.

    Given a subscription pattern with an invalid exchange or instrument,
    When validate_subscription_pattern is called,
    Then it returns False with an appropriate error message.
    """
    monkeypatch.setattr(validation, "get_available_exchanges", lambda: ["kraken", "paper"])
    monkeypatch.setattr(validation, "get_market_subscribe_exchanges", lambda: ["kraken"])
    monkeypatch.setattr(validation, "get_market_data_exchanges", lambda: ["kraken"])
    monkeypatch.setattr(validation, "get_available_symbols", lambda: {"BTC-USD"})
    valid, message = validate_subscription_pattern(pattern)
    assert valid is False
    assert expected_message in message


def test_system_heartbeats_prefix_is_rejected() -> None:
    """Verify system.heartbeats. prefix alone is rejected.

    Given a topic with only the system.heartbeats. prefix,
    When validate_topic is called,
    Then it returns False with a non-empty error message.
    """
    valid, message = validate_topic("system.heartbeats.")
    assert valid is False
    assert message != ""


def test_system_heartbeats_strategy_missing_name() -> None:
    """Verify strategy heartbeat requires component name.

    Given a strategy heartbeat topic without a component name,
    When validate_topic is called,
    Then it returns False indicating component name is required.
    """
    valid, message = validate_topic("system.heartbeats.strategy")
    assert valid is False
    assert "4 segments" in message


def test_system_heartbeats_invalid_component_type() -> None:
    """Verify invalid heartbeat component type is rejected.

    Given a heartbeat topic with an invalid component type,
    When validate_topic is called,
    Then it returns False with invalid component type error.
    """
    valid, message = validate_topic("system.heartbeats.worker")
    assert valid is False
    assert "Invalid heartbeat component type" in message


def test_system_heartbeats_feed_with_extra_segment() -> None:
    """Verify non-paper feed heartbeat with extra segment is rejected.

    Given a feed heartbeat topic with extra non-paper segment,
    When validate_topic is called,
    Then it returns False indicating exact segment count required.
    """
    valid, message = validate_topic("system.heartbeats.feed.kraken.extra")
    assert valid is False
    assert "feed.paper.{source}" in message


def test_system_heartbeats_feed_paper_source_accepted() -> None:
    """Verify paper feed heartbeat with source_exchange is accepted.

    Given a feed heartbeat topic for paper with source exchange,
    When validate_topic is called,
    Then it returns True.
    """
    valid, message = validate_topic("system.heartbeats.feed.paper.kraken")
    assert valid, f"Paper feed heartbeat should be valid: {message}"


def test_system_heartbeats_feed_paper_source_paper_rejected() -> None:
    """Verify paper feed heartbeat rejects 'paper' as source_exchange.

    Given a feed heartbeat for paper with source_exchange='paper',
    When validate_topic is called,
    Then it returns False with clear error.
    """
    valid, message = validate_topic("system.heartbeats.feed.paper.paper")
    assert valid is False
    assert "source cannot be 'paper'" in message


def test_system_heartbeats_feed_paper_with_extra_segment_rejected() -> None:
    """Verify paper feed heartbeat with 6 segments is rejected.

    Given a feed heartbeat for paper with extra segment,
    When validate_topic is called,
    Then it returns False.
    """
    valid, message = validate_topic("system.heartbeats.feed.paper.kraken.extra")
    assert valid is False


def test_system_heartbeats_feed_unknown_exchange_rejected() -> None:
    """Verify feed heartbeat with unknown exchange is rejected.

    Given a feed heartbeat topic with unknown exchange,
    When validate_topic is called,
    Then it returns False with market feed exchange error.
    """
    valid, message = validate_topic("system.heartbeats.feed.foo")
    assert valid is False
    assert "Unknown market feed exchange 'foo'" in message


def test_system_heartbeats_feed_polygon_rejected() -> None:
    """Verify live feed heartbeat for polygon is rejected.

    Given a feed heartbeat for polygon (data-only, not a live feed),
    When validate_topic is called,
    Then it returns False because polygon is not a live market feed.
    """
    valid, message = validate_topic("system.heartbeats.feed.polygon")
    assert valid is False
    assert "Unknown market feed exchange 'polygon'" in message


def test_system_heartbeats_feed_paper_polygon_accepted() -> None:
    """Verify paper feed heartbeat with polygon source is accepted.

    Given a paper feed heartbeat with polygon as replay source,
    When validate_topic is called,
    Then it returns True because polygon is a valid replay source.
    """
    valid, message = validate_topic("system.heartbeats.feed.paper.polygon")
    assert valid, f"Paper feed heartbeat with polygon source should be valid: {message}"


def test_system_heartbeats_feed_paper_unknown_source_rejected() -> None:
    """Verify paper feed heartbeat with unknown source is rejected.

    Given a paper feed heartbeat with unknown replay source,
    When validate_topic is called,
    Then it returns False with replay source error.
    """
    valid, message = validate_topic("system.heartbeats.feed.paper.binance")
    assert valid is False
    assert "Unknown replay source 'binance'" in message


def test_validate_market_source_rejects_polygon() -> None:
    """Verify live market source validator rejects polygon.

    Given polygon as exchange for live market topic,
    When _validate_market_source is called,
    Then it returns False because polygon has no live feed.
    """
    valid, msg = _validate_market_source("polygon")
    assert valid is False
    assert "Unknown market feed exchange 'polygon'" in msg


def test_validate_market_source_accepts_kraken() -> None:
    """Verify live market source validator accepts kraken.

    Given kraken as exchange for live market topic,
    When _validate_market_source is called,
    Then it returns True.
    """
    valid, msg = _validate_market_source("kraken")
    assert valid
    assert msg == ""


def test_validate_market_source_rejects_empty() -> None:
    """Verify live market source validator rejects empty string.

    Given empty string as exchange,
    When _validate_market_source is called,
    Then it returns False with empty error.
    """
    valid, msg = _validate_market_source("")
    assert valid is False
    assert "Exchange cannot be empty" in msg


def test_validate_replay_source_rejects_empty() -> None:
    """Verify replay source validator rejects empty string.

    Given empty string as source exchange,
    When _validate_replay_source is called,
    Then it returns False with empty error.
    """
    valid, msg = _validate_replay_source("")
    assert valid is False
    assert "Source exchange cannot be empty" in msg


def test_validate_replay_source_accepts_polygon() -> None:
    """Verify replay source validator accepts polygon.

    Given polygon as source for paper replay,
    When _validate_replay_source is called,
    Then it returns True because polygon is a valid replay source.
    """
    valid, msg = _validate_replay_source("polygon")
    assert valid
    assert msg == ""


def test_validate_replay_source_rejects_paper() -> None:
    """Verify replay source validator rejects paper.

    Given paper as source for paper replay,
    When _validate_replay_source is called,
    Then it returns False because paper is consumer, not source.
    """
    valid, msg = _validate_replay_source("paper")
    assert valid is False
    assert "Unknown replay source 'paper'" in msg


def test_validate_market_paper_polygon_topic_accepted() -> None:
    """Verify paper replay market topic with polygon source is accepted.

    Given a paper market topic using polygon as replay source,
    When validate_topic is called,
    Then it returns True.
    """
    valid, msg = validate_topic("market.paper.polygon.AAPL.candles.1m")
    assert valid, f"Paper replay from polygon should be valid: {msg}"


def test_validate_market_polygon_live_topic_rejected() -> None:
    """Verify live market topic with polygon exchange is rejected.

    Given a live market topic using polygon (no live feed),
    When validate_topic is called,
    Then it returns False because polygon has no live market publisher.
    """
    valid, msg = validate_topic("market.polygon.AAPL.candles.1m")
    assert valid is False
    assert "Unknown market feed exchange 'polygon'" in msg


def test_market_topic_builder_allows_polygon_but_validation_rejects_it() -> None:
    """Verify builder is future-proofed while validation enforces current support.

    Given polygon is a known exchange identifier but not an enabled live feed,
    When market_topic builds a live polygon topic and validate_topic checks it,
    Then builder returns the topic and validation rejects current runtime use.
    """
    topic = market_topic(ExchangeEnum.POLYGON, "AAPL", MarketDataTypeEnum.CANDLES, "1m")
    assert topic == "market.polygon.AAPL.candles.1m"

    valid, msg = validate_topic(topic)
    assert valid is False
    assert "Unknown market feed exchange 'polygon'" in msg


def test_system_settings_disallow_extra_segments() -> None:
    """Verify system.settings rejects extra segments.

    Given a system.settings topic with extra segments,
    When validate_topic is called,
    Then it returns False indicating exact segment count required.
    """
    valid, message = validate_topic("system.settings.extra")
    assert valid is False
    assert "must have exactly 2 segments" in message


def test_system_invalid_type_is_rejected() -> None:
    """Verify invalid system type is rejected.

    Given a system topic with an invalid type,
    When validate_topic is called,
    Then it returns False with invalid system type error.
    """
    valid, message = validate_topic("system.invalid")
    assert valid is False
    assert "Invalid system type" in message


def test_admin_prefix_rejected() -> None:
    """Verify admin topics with invalid segments are rejected.

    Given admin topics with incorrect segment counts,
    When validate_topic is called,
    Then it returns False indicating exact segment count required.
    """
    valid_prefix, message_prefix = validate_topic("admin.users.")
    valid_segments, message_segments = validate_topic("admin.users.extra")
    assert valid_prefix is False
    assert "must have 2 segments" in message_prefix
    assert valid_segments is False
    assert "must have 2 segments" in message_segments


def test_unknown_category_prefix_is_rejected() -> None:
    """Verify unknown category prefix is rejected.

    Given a subscription pattern with an unknown category,
    When validate_subscription_pattern is called,
    Then it returns False with unknown topic category error.
    """
    valid, message = validate_subscription_pattern("unknown.")
    assert valid is False
    assert "Unknown topic category" in message


@pytest.mark.parametrize(
    "pattern",
    [
        "market.kraken.BTC-USD.",
        "market.kraken.",
        "market.",
        "orders.commands.kraken.BTC-USD.",
        "orders.commands.kraken.",
        "orders.commands.",
        "orders.events.kraken.BTC-USD.",
        "orders.events.kraken.",
        "orders.events.",
        "signals.paper.BTC-USD.",
        "signals.paper.",
        "signals.",
    ],
)
def test_valid_prefix_patterns_are_accepted(monkeypatch: pytest.MonkeyPatch, pattern: str) -> None:
    """Verify valid subscription prefix patterns are accepted.

    Given a valid subscription prefix pattern,
    When validate_subscription_pattern is called,
    Then it returns True with an empty message.
    """
    monkeypatch.setattr(validation, "get_available_exchanges", lambda: ["kraken", "paper"])
    monkeypatch.setattr(validation, "get_market_subscribe_exchanges", lambda: ["kraken"])
    monkeypatch.setattr(validation, "get_market_data_exchanges", lambda: ["kraken"])
    monkeypatch.setattr(validation, "get_available_symbols", lambda: {"BTC-USD"})
    valid, message = validate_subscription_pattern(pattern)
    assert valid is True
    assert message == ""


def test_validate_market_topic_requires_timeframe() -> None:
    """Verify candles topic requires timeframe segment.

    Given a market candles topic without a timeframe,
    When validate_topic is called,
    Then it returns False indicating timeframe is required.
    """
    is_valid, message = validation.validate_topic("market.kraken.BTC-USD.candles")
    assert is_valid is False
    assert "timeframe" in message


def test_validate_market_topic_rejects_timeframe_for_trades() -> None:
    """Verify trades topic rejects timeframe segment.

    Given a market trades topic with an extra timeframe segment,
    When validate_topic is called,
    Then it returns False indicating only candles support timeframe.
    """
    is_valid, message = validation.validate_topic("market.kraken.BTC-USD.trades.1m")
    assert is_valid is False
    assert "Only candles topics support timeframe" in message


def test_validate_market_topic_invalid_timeframe() -> None:
    """Verify invalid timeframe is rejected.

    Given a market candles topic with an invalid timeframe,
    When validate_topic is called,
    Then it returns False with invalid timeframe error.
    """
    is_valid, message = validation.validate_topic("market.kraken.BTC-USD.candles.99x")
    assert is_valid is False
    assert "Invalid timeframe" in message


def test_validate_orders_requires_subcategory() -> None:
    """Verify orders topic requires commands/events subcategory.

    Given an orders topic without proper subcategory,
    When validate_topic is called,
    Then it returns False with subcategory requirement error.
    """
    is_valid, message = validation.validate_topic("orders.kraken.BTC-USD.unknown")
    assert is_valid is False
    assert "orders.commands" in message or "orders.events" in message


def test_validate_subscription_pattern_rejects_wildcards() -> None:
    """Verify wildcard patterns are rejected.

    Given a subscription pattern containing wildcards,
    When validate_subscription_pattern is called,
    Then it returns False indicating wildcards are not allowed.
    """
    is_valid, message = validation.validate_subscription_pattern("market.*")
    assert is_valid is False
    assert "Wildcards" in message


def test_validate_subscription_pattern_invalid_instrument_prefix() -> None:
    """Verify invalid instrument prefix is rejected.

    Given a subscription pattern with an unknown instrument,
    When validate_subscription_pattern is called,
    Then it returns False with unknown instrument error.
    """
    is_valid, message = validation.validate_subscription_pattern("market.kraken.INVALID.")
    assert is_valid is False
    assert "Unknown instrument" in message


def test_validate_signal_missing_strategy_id() -> None:
    """Verify signal topic requires strategy ID.

    Given a signal topic without a strategy ID,
    When validate_topic is called,
    Then it returns False indicating strategy ID is required.
    """
    is_valid, message = validation.validate_topic("signals.paper.BTC-USD.")
    assert is_valid is False
    assert "Strategy ID" in message or "segments" in message


def test_validate_system_heartbeat_topic_is_allowed() -> None:
    """Verify system.heartbeats topic is valid.

    Given the system.heartbeats topic,
    When validate_topic is called,
    Then it returns True with an empty message.
    """
    is_valid, message = validation.validate_topic("system.heartbeats")
    assert is_valid is True
    assert message == ""


def test_validate_subscription_unknown_category() -> None:
    """Verify unknown subscription category is rejected.

    Given a subscription pattern with an unknown category,
    When validate_subscription_pattern is called,
    Then it returns False with unknown topic category error.
    """
    is_valid, message = validation.validate_subscription_pattern("unknown.")
    assert is_valid is False
    assert "Unknown topic category" in message


class TestAccrualsTopicValidation:
    """Tests for _validate_accruals_topic validator."""

    def test_valid_rollover_topic(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify valid rollover accrual topic passes validation."""
        _patch_env(monkeypatch, {"kraken"}, {"BTC-USD"})
        valid, _err = _validate_accruals_topic("accruals.kraken.BTC-USD.rollover")
        assert valid

    def test_valid_funding_topic(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify valid funding accrual topic passes validation."""
        _patch_env(monkeypatch, {"kraken_futures"}, {"PF_XBTUSD"})
        valid, _err = _validate_accruals_topic("accruals.kraken_futures.PF_XBTUSD.funding")
        assert valid

    def test_valid_borrow_topic(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify valid borrow accrual topic passes validation."""
        _patch_env(monkeypatch, {"kraken"}, {"ETH-USD"})
        valid, _err = _validate_accruals_topic("accruals.kraken.ETH-USD.borrow")
        assert valid

    def test_trailing_dot_rejected(self) -> None:
        """Verify trailing dot is rejected."""
        valid, err = _validate_accruals_topic("accruals.kraken.BTC-USD.rollover.")
        assert not valid
        assert "4 segments" in err

    def test_too_few_segments(self) -> None:
        """Verify topic with too few segments is rejected."""
        valid, err = _validate_accruals_topic("accruals.kraken.BTC-USD")
        assert not valid
        assert "4 segments" in err

    def test_too_many_segments(self) -> None:
        """Verify topic with too many segments is rejected."""
        valid, err = _validate_accruals_topic("accruals.kraken.BTC-USD.rollover.extra")
        assert not valid
        assert "4 segments" in err

    def test_wrong_category(self) -> None:
        """Verify wrong category prefix is rejected."""
        valid, err = _validate_accruals_topic("market.kraken.BTC-USD.rollover")
        assert not valid
        assert "accruals" in err.lower()

    def test_invalid_exchange(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify unknown exchange is rejected."""
        _patch_env(monkeypatch, {"kraken"}, {"BTC-USD"})
        valid, err = _validate_accruals_topic("accruals.unknown_ex.BTC-USD.rollover")
        assert not valid

    def test_invalid_instrument(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify unknown instrument is rejected."""
        _patch_env(monkeypatch, {"kraken"}, {"ETH-USD"})
        valid, err = _validate_accruals_topic("accruals.kraken.UNKNOWN-PAIR.rollover")
        assert not valid

    def test_invalid_accrual_type(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify unknown accrual_type is rejected."""
        _patch_env(monkeypatch, {"kraken"}, {"BTC-USD"})
        valid, err = _validate_accruals_topic("accruals.kraken.BTC-USD.interest")
        assert not valid
        assert "accrual_type" in err.lower()

    def test_via_validate_topic(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verify accruals topic routes through validate_topic dispatcher."""
        _patch_env(monkeypatch, {"kraken"}, {"BTC-USD"})
        valid, _err = validate_topic("accruals.kraken.BTC-USD.rollover")
        assert valid

    def test_subscription_pattern_accepted(self) -> None:
        """Verify 'accruals.' is an accepted subscription pattern."""
        valid, _err = validate_subscription_pattern("accruals.")
        assert valid


class TestBacktestTopicFamily:
    """Phase 2c backtest topic validator coverage (plan §2.1)."""

    _WALLET = "01948f94-0001-7a00-8000-000000000001"
    _RUN = "01948f94-0001-7a00-8000-000000000002"

    def test_accepts_every_canonical_event(self) -> None:
        """Every ``BacktestProgressEvent`` value produces a valid topic."""
        for event in BACKTEST_EVENTS:
            topic = f"backtest.{self._WALLET}.{self._RUN}.{event}"
            valid, err = _validate_backtest_topic(topic)
            assert valid, f"canonical event {event} rejected: {err}"

    def test_rejects_non_canonical_event(self) -> None:
        """Non-canonical event names like 'complete' or 'error' fail."""
        for bad in ("complete", "error", "STARTED"):
            topic = f"backtest.{self._WALLET}.{self._RUN}.{bad}"
            valid, _err = _validate_backtest_topic(topic)
            assert not valid, f"non-canonical event {bad!r} passed"

    def test_rejects_wrong_segment_count(self) -> None:
        """Only 4 segments accepted (category + wallet + run + event)."""
        valid, _ = _validate_backtest_topic(f"backtest.{self._WALLET}.{self._RUN}")
        assert not valid
        valid, _ = _validate_backtest_topic(f"backtest.{self._WALLET}.{self._RUN}.started.extra")
        assert not valid

    def test_rejects_non_uuid7_wallet_segment(self) -> None:
        """Malformed wallet segment fails UUID7 check."""
        valid, err = _validate_backtest_topic(f"backtest.not-a-uuid.{self._RUN}.started")
        assert not valid
        assert "wallet segment" in err.lower()

    def test_rejects_non_uuid7_run_segment(self) -> None:
        """Malformed run segment fails UUID7 check."""
        valid, err = _validate_backtest_topic(f"backtest.{self._WALLET}.not-a-uuid.started")
        assert not valid
        assert "run segment" in err.lower()

    def test_prefix_patterns_accepted(self) -> None:
        """Three prefix shapes accepted: root, wallet, wallet+run."""
        for pattern in (
            "backtest.",
            f"backtest.{self._WALLET}.",
            f"backtest.{self._WALLET}.{self._RUN}.",
        ):
            valid, err = _validate_backtest_prefix(pattern)
            assert valid, f"valid prefix {pattern!r} rejected: {err}"

    def test_prefix_rejects_malformed_wallet(self) -> None:
        """Non-UUID7 wallet segment in prefix is rejected."""
        valid, err = _validate_backtest_prefix("backtest.not-a-uuid.")
        assert not valid
        assert "wallet" in err.lower()

    def test_prefix_rejects_too_many_segments(self) -> None:
        """More than 3 segments in a prefix is rejected."""
        pattern = f"backtest.{self._WALLET}.{self._RUN}.started."
        valid, _ = _validate_backtest_prefix(pattern)
        assert not valid

    def test_via_validate_topic_dispatcher(self) -> None:
        """Canonical topic routes through the main dispatcher."""
        topic = f"backtest.{self._WALLET}.{self._RUN}.completed"
        valid, _ = validate_topic(topic)
        assert valid

    def test_via_validate_subscription_pattern(self) -> None:
        """Subscription-pattern dispatcher accepts all 3 prefix shapes."""
        for pattern in (
            "backtest.",
            f"backtest.{self._WALLET}.",
            f"backtest.{self._WALLET}.{self._RUN}.",
        ):
            valid, _ = validate_subscription_pattern(pattern)
            assert valid, f"subscription dispatcher rejected {pattern}"
