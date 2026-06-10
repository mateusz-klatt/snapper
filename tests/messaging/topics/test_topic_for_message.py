"""Tests for topic_for_message() and heartbeat_topic_from_component()."""

from datetime import UTC
from datetime import datetime

import pytest

from snapper.api.schemas.base import StrictDataSchema
from snapper.messaging.schemas.data import AlertEventData
from snapper.messaging.schemas.data import CandleData
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.schemas.data import OrderCancelData
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.data import OrderEventData
from snapper.messaging.schemas.data import OrderReplaceData
from snapper.messaging.schemas.data import OrderRequestData
from snapper.messaging.schemas.data import ReplayEndData
from snapper.messaging.schemas.data import ReplayStartData
from snapper.messaging.schemas.data import SettingChangedData
from snapper.messaging.schemas.data import SignalData
from snapper.messaging.schemas.data import SymbolAliasUpdateData
from snapper.messaging.schemas.data import TickData
from snapper.messaging.schemas.data import TradeData
from snapper.messaging.topics.builders import heartbeat_topic_from_component
from snapper.messaging.topics.builders import topic_for_message


class TestTopicForMessage:
    """Tests for topic_for_message() dispatch function."""

    def test_tick_data(self) -> None:
        """TickData maps to market.{exchange}.{instrument}.ticks topic.

        Given: A TickData instance for kraken BTC-USD,
        When: Deriving topic,
        Then: Returns market.kraken.BTC-USD.ticks.
        """
        data = TickData(
            session_id="",
            sequence_id=0,
            exchange="kraken",
            instrument="BTC-USD",
            volume=1.0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )
        assert topic_for_message(data) == "market.kraken.BTC-USD.ticks"

    def test_candle_data(self) -> None:
        """CandleData maps to market.{exchange}.{instrument}.candles.{timeframe}.

        Given: A CandleData instance with 1m timeframe,
        When: Deriving topic,
        Then: Returns market.kraken.BTC-USD.candles.1m.
        """
        data = CandleData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            timeframe="1m",
            open_at=datetime.now(UTC),
            open=100.0,
            high=110.0,
            low=90.0,
            close=105.0,
            volume=500.0,
        )
        assert topic_for_message(data) == "market.kraken.BTC-USD.candles.1m"

    def test_trade_data(self) -> None:
        """TradeData maps to market.{exchange}.{instrument}.trades topic.

        Given: A TradeData instance,
        When: Deriving topic,
        Then: Returns market.walutomat.EUR-PLN.trades.
        """
        data = TradeData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="walutomat",
            instrument="EUR-PLN",
            price=200000.0,
            volume=0.5,
        )
        assert topic_for_message(data) == "market.walutomat.EUR-PLN.trades"

    def test_order_request_data(self) -> None:
        """OrderRequestData maps to orders.commands.{exchange}.{instrument}.submit.

        Given: An OrderRequestData instance,
        When: Deriving topic,
        Then: Returns orders.commands.kraken.BTC-USD.submit.
        """
        data = OrderRequestData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            strategy_id="strat-1",
            exchange="kraken",
            instrument="BTC-USD",
            mode="live",
            side="buy",
            order_type="limit",
            quantity=1.0,
            price=50000.0,
            client_order_id="ord-001",
        )
        assert topic_for_message(data) == "orders.commands.kraken.BTC-USD.submit"

    def test_order_cancel_data(self) -> None:
        """OrderCancelData maps to orders.commands.{exchange}.{instrument}.cancel.

        Given: An OrderCancelData instance,
        When: Deriving topic,
        Then: Returns orders.commands.kraken.ETH-USD.cancel.
        """
        data = OrderCancelData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="ETH-USD",
            exchange_order_id="exch-123",
            client_order_id="ord-001",
        )
        assert topic_for_message(data) == "orders.commands.kraken.ETH-USD.cancel"

    def test_order_replace_data(self) -> None:
        """OrderReplaceData maps to orders.commands.{exchange}.{instrument}.replace.

        Given: An OrderReplaceData instance,
        When: Deriving topic,
        Then: Returns orders.commands.kraken.BTC-USD.replace.
        """
        data = OrderReplaceData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="kraken",
            instrument="BTC-USD",
            exchange_order_id="exch-456",
            client_order_id="ord-002",
            new_price=51000.0,
        )
        assert topic_for_message(data) == "orders.commands.kraken.BTC-USD.replace"

    def test_order_data(self) -> None:
        """OrderData maps to orders.events.{exchange}.{instrument}.{status}.

        Given: An OrderData with status 'submitted',
        When: Deriving topic,
        Then: Returns orders.events.kraken.BTC-USD.submitted.
        """
        data = OrderData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            client_order_id="ord-001",
            instrument="BTC-USD",
            exchange="kraken",
            side="buy",
            status="submitted",
            order_type="limit",
            size=1.0,
            filled_size=0.0,
            price=50000.0,
            created_at=datetime.now(UTC),
        )
        assert topic_for_message(data) == "orders.events.kraken.BTC-USD.submitted"

    def test_order_data_unknown_status(self) -> None:
        """OrderData with the ambiguous-submit status derives the unknown topic.

        Given: An OrderData with status 'unknown' (submit outcome
            ambiguous, venue verification pending),
        When: Deriving topic,
        Then: Returns orders.events.kraken.BTC-USD.unknown.
        """
        data = OrderData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            client_order_id="ord-unk",
            instrument="BTC-USD",
            exchange="kraken",
            side="buy",
            status="unknown",
            order_type="market",
            size=1.0,
            filled_size=0.0,
            price=None,
            created_at=datetime.now(UTC),
        )
        assert topic_for_message(data) == "orders.events.kraken.BTC-USD.unknown"

    def test_order_event_data(self) -> None:
        """OrderEventData maps to orders.events.{exchange}.{instrument}.{event}.

        Given: An OrderEventData with event 'cancelled',
        When: Deriving topic,
        Then: Returns orders.events.kraken.BTC-USD.cancelled.
        """
        data = OrderEventData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange_order_id="exch-789",
            client_order_id="ord-003",
            exchange="kraken",
            instrument="BTC-USD",
            event="cancelled",
        )
        assert topic_for_message(data) == "orders.events.kraken.BTC-USD.cancelled"

    def test_execution_data(self) -> None:
        """ExecutionData maps to orders.events.{exchange}.{instrument}.executed.

        Given: An ExecutionData instance,
        When: Deriving topic,
        Then: Returns orders.events.kraken.BTC-USD.executed.
        """
        data = ExecutionData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            client_order_id="ord-001",
            instrument="BTC-USD",
            exchange="kraken",
            side="buy",
            size=1.0,
            price=50000.0,
            last_size=1.0,
            last_price=50000.0,
            fee=5.0,
            fee_asset="USD",
            status="filled",
            executed_at=datetime.now(UTC),
        )
        assert topic_for_message(data) == "orders.events.kraken.BTC-USD.executed"

    def test_signal_data_live(self) -> None:
        """Live SignalData maps to signals.{exchange}.{instrument}.live.

        Given: A SignalData with exchange kraken (live),
        When: Deriving topic,
        Then: Returns signals.kraken.BTC-USD.live.
        """
        data = SignalData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            fired_at=datetime.now(UTC),
            instrument="BTC-USD",
            exchange="kraken",
            side="buy",
            strength=0.8,
            reason="breakout",
        )
        assert topic_for_message(data) == "signals.kraken.BTC-USD.live"

    def test_signal_data_paper_with_strategy_name(self) -> None:
        """Paper SignalData with strategy_name maps to signals.paper.{instrument}.{strategy}.

        Given: A SignalData with paper exchange and strategy_name,
        When: Deriving topic,
        Then: Returns signals.paper.BTC-USD.momentum.
        """
        data = SignalData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            fired_at=datetime.now(UTC),
            instrument="BTC-USD",
            exchange="paper",
            side="sell",
            strength=0.6,
            reason="reversal",
            strategy_name="momentum",
        )
        assert topic_for_message(data) == "signals.paper.BTC-USD.momentum"

    def test_signal_data_paper_without_strategy_name_raises(self) -> None:
        """Paper SignalData without strategy_name raises ValueError.

        Given: A SignalData with paper exchange and no strategy_name,
        When: Constructing the object,
        Then: ValueError is raised by the model validator.
        """
        with pytest.raises(ValueError, match="strategy_name"):
            SignalData(
                session_id="",
                sequence_id=0,
                public_id="test-public-id",
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                fired_at=datetime.now(UTC),
                instrument="BTC-USD",
                exchange="paper",
                side="buy",
                strength=0.5,
                reason="test",
            )

    def test_topic_for_message_paper_signal_without_strategy_name_raises(self) -> None:
        """topic_for_message raises ValueError for paper SignalData with no strategy_name.

        Given: A paper SignalData constructed bypassing the model validator with no strategy_name,
        When: topic_for_message() is called,
        Then: ValueError is raised with a message referencing strategy_name.
        """
        data = SignalData.model_construct(
            type="signal",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            exchange="paper",
            instrument="BTC-USD",
            strategy_name=None,
            side="buy",
            strength=0.5,
            reason="test",
            fired_at=datetime(2024, 1, 1, tzinfo=UTC),
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
        )
        with pytest.raises(ValueError, match="strategy_name"):
            topic_for_message(data)

    def test_heartbeat_data(self) -> None:
        """HeartbeatData maps to system.heartbeats.{component}.

        Given: A HeartbeatData with component 'executor.kraken',
        When: Deriving topic,
        Then: Returns system.heartbeats.executor.kraken.
        """
        data = HeartbeatData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            component="executor.kraken",
            sequence=1,
            status="healthy",
            lag_ms=10,
        )
        assert topic_for_message(data) == "system.heartbeats.executor.kraken"

    def test_setting_changed_data(self) -> None:
        """SettingChangedData maps to system.settings.

        Given: A SettingChangedData instance,
        When: Deriving topic,
        Then: Returns system.settings.
        """
        data = SettingChangedData(
            session_id="",
            sequence_id=0,
            key="max_position",
            value="100",
            category="trading",
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )
        assert topic_for_message(data) == "system.settings"

    def test_symbol_alias_update_data(self) -> None:
        """SymbolAliasUpdateData maps to system.symbol_aliases.

        Given: A SymbolAliasUpdateData instance,
        When: Deriving topic,
        Then: Returns system.symbol_aliases.
        """
        data = SymbolAliasUpdateData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )
        assert topic_for_message(data) == "system.symbol_aliases"

    def test_replay_start_data(self) -> None:
        """ReplayStartData maps to system.replay.start.

        Given: A ReplayStartData instance,
        When: Deriving topic,
        Then: Returns system.replay.start.
        """
        data = ReplayStartData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )
        assert topic_for_message(data) == "system.replay.start"

    def test_replay_end_data(self) -> None:
        """ReplayEndData maps to system.replay.end.

        Given: A ReplayEndData instance,
        When: Deriving topic,
        Then: Returns system.replay.end.
        """
        data = ReplayEndData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )
        assert topic_for_message(data) == "system.replay.end"

    def test_unknown_type_raises(self) -> None:
        """Unknown StrictDataSchema subclass raises ValueError.

        Given: A custom StrictDataSchema subclass not handled by topic_for_message,
        When: Deriving topic,
        Then: ValueError is raised.
        """
        data = StrictDataSchema(
            session_id="",
            sequence_id=0,
            type="unknown_thing",
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )
        with pytest.raises(ValueError, match="No topic derivation"):
            topic_for_message(data)

    def test_alert_event_data_routes_to_alerts_topic(self) -> None:
        """AlertEventData maps to ``alerts.{user_public_id}.{alert_type}``.

        Given: An AlertEventData with user_public_id + alert_type,
        When: Deriving topic,
        Then: Returns ``alerts.<uuid7>.<alert_type>``.
        """
        data = AlertEventData(
            session_id="s1",
            sequence_id=1,
            public_id="test-envelope-pid",
            timestamp=datetime(2026, 4, 23, 12, tzinfo=UTC),
            user_public_id="019dbb34-f439-77bd-afa8-ee5321d60307",
            alert_type="order_fill_full",
            title="Filled",
            body="BTC-USD 0.1 filled",
        )

        topic = topic_for_message(data)

        assert topic == "alerts.019dbb34-f439-77bd-afa8-ee5321d60307.order_fill_full"


class TestHeartbeatTopicFromComponent:
    """Tests for heartbeat_topic_from_component() helper."""

    def test_simple_component(self) -> None:
        """Dotted component name produces correct heartbeat topic.

        Given: Component name 'executor.kraken',
        When: Deriving heartbeat topic,
        Then: Returns system.heartbeats.executor.kraken.
        """
        assert (
            heartbeat_topic_from_component("executor.kraken") == "system.heartbeats.executor.kraken"
        )
