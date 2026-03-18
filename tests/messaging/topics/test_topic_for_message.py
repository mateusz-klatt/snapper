"""Tests for topic_for_message() and heartbeat_topic_from_component()."""

from datetime import UTC
from datetime import datetime

import pytest

from snapper.api.schemas.base import StrictDataSchema
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
        data = TickData(exchange="kraken", instrument="BTC-USD", volume=1.0)
        assert topic_for_message(data) == "market.kraken.BTC-USD.ticks"

    def test_candle_data(self) -> None:
        """CandleData maps to market.{exchange}.{instrument}.candles.{timeframe}.

        Given: A CandleData instance with 1m timeframe,
        When: Deriving topic,
        Then: Returns market.kraken.BTC-USD.candles.1m.
        """
        data = CandleData(
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
        Then: Returns market.zonda.BTC-PLN.trades.
        """
        data = TradeData(exchange="zonda", instrument="BTC-PLN", price=200000.0, volume=0.5)
        assert topic_for_message(data) == "market.zonda.BTC-PLN.trades"

    def test_order_request_data(self) -> None:
        """OrderRequestData maps to orders.commands.{exchange}.{instrument}.submit.

        Given: An OrderRequestData instance,
        When: Deriving topic,
        Then: Returns orders.commands.kraken.BTC-USD.submit.
        """
        data = OrderRequestData(
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
            client_order_id="ord-001",
            instrument="BTC-USD",
            exchange="kraken",
            side="buy",
            status="submitted",
            order_type="limit",
            size=1.0,
            filled_size=0.0,
            price=50000.0,
        )
        assert topic_for_message(data) == "orders.events.kraken.BTC-USD.submitted"

    def test_order_event_data(self) -> None:
        """OrderEventData maps to orders.events.{exchange}.{instrument}.{event}.

        Given: An OrderEventData with event 'cancelled',
        When: Deriving topic,
        Then: Returns orders.events.kraken.BTC-USD.cancelled.
        """
        data = OrderEventData(
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
            client_order_id="ord-001",
            instrument="BTC-USD",
            exchange="kraken",
            side="buy",
            size=1.0,
            price=50000.0,
            fee=5.0,
            fee_asset="USD",
            status="filled",
        )
        assert topic_for_message(data) == "orders.events.kraken.BTC-USD.executed"

    def test_signal_data_live(self) -> None:
        """Live SignalData maps to signals.{exchange}.{instrument}.live.

        Given: A SignalData with exchange kraken (live),
        When: Deriving topic,
        Then: Returns signals.kraken.BTC-USD.live.
        """
        data = SignalData(
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
            session_id="",
            sequence_id=0,
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
        data = SettingChangedData(key="max_position", value="100", category="trading")
        assert topic_for_message(data) == "system.settings"

    def test_symbol_alias_update_data(self) -> None:
        """SymbolAliasUpdateData maps to system.symbol_aliases.

        Given: A SymbolAliasUpdateData instance,
        When: Deriving topic,
        Then: Returns system.symbol_aliases.
        """
        data = SymbolAliasUpdateData()
        assert topic_for_message(data) == "system.symbol_aliases"

    def test_replay_start_data(self) -> None:
        """ReplayStartData maps to system.replay.start.

        Given: A ReplayStartData instance,
        When: Deriving topic,
        Then: Returns system.replay.start.
        """
        data = ReplayStartData()
        assert topic_for_message(data) == "system.replay.start"

    def test_replay_end_data(self) -> None:
        """ReplayEndData maps to system.replay.end.

        Given: A ReplayEndData instance,
        When: Deriving topic,
        Then: Returns system.replay.end.
        """
        data = ReplayEndData()
        assert topic_for_message(data) == "system.replay.end"

    def test_unknown_type_raises(self) -> None:
        """Unknown StrictDataSchema subclass raises ValueError.

        Given: A custom StrictDataSchema subclass not handled by topic_for_message,
        When: Deriving topic,
        Then: ValueError is raised.
        """
        data = StrictDataSchema(type="unknown_thing")
        with pytest.raises(ValueError, match="No topic derivation"):
            topic_for_message(data)


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
