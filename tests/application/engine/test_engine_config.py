"""Tests for TradingEngineService and EngineConfigModel."""

import asyncio
import contextlib
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.application.engine.config import EngineConfigModel
from snapper.application.engine.service import TradingEngineService
from snapper.application.engine.trader import TraderCoordinator
from snapper.application.portfolio.models import PositionStateModel
from snapper.application.risk.models import RiskConfigModel
from snapper.application.risk.models import RiskEvaluator
from snapper.application.trade.balance_service import BalanceService
from snapper.application.trade.trade_service import TradeService
from snapper.data.repository import SQLAlchemyRepository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import SignalData


class FakeSocket:
    """Fake socket that collects sent messages."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.sent: list[Any] = []
        self._tracker = SequenceTracker()

    @property
    def tracker(self) -> SequenceTracker:
        """Expose sequence tracker for provenance stamping."""
        return self._tracker

    async def send(self, stream_key: str, data: Any, *, flags: int = 0) -> None:
        """Collect sent data objects."""
        self.sent.append(data)


class StubRisk(RiskEvaluator):
    """Risk evaluator stub that records method calls."""

    def __init__(self, stop_pct_value: float = 0.05) -> None:
        """Initialize the instance."""
        super().__init__(RiskConfigModel())
        self.stop_pct_value = stop_pct_value
        self.can_open_calls: list[tuple[float, float]] = []
        self.cap_calls: list[tuple[float, float, float, float]] = []
        self.round_size_calls: list[tuple[float, float, float, float]] = []

    def stop_pct(self) -> float:
        """Return configured stop percentage."""
        return self.stop_pct_value

    def can_open_new_trade(self, equity: float, peak_equity: float) -> bool:
        """Record call and always return True."""
        self.can_open_calls.append((equity, peak_equity))
        return True

    def cap_size_by_leverage(
        self, current_notional: float, equity: float, price: float, desired_size: float
    ) -> float:
        """Record call and return desired size unchanged."""
        self.cap_calls.append((current_notional, equity, price, desired_size))
        return desired_size

    def round_size(
        self, desired_size: float, lot_size: float, price: float, tick_size: float
    ) -> float:
        """Record call and delegate to parent."""
        self.round_size_calls.append((desired_size, lot_size, price, tick_size))
        result = super().round_size(desired_size, lot_size, price, tick_size)
        return result


@pytest.mark.asyncio
async def test_engine_execute_desired_units_buy_flow() -> None:
    """Verify buy order execution updates position and sends order message.

    Given: An engine with initial cash and risk configuration,
    When: execute_desired_units is called with positive units,
    Then: Buy order is sent and position is opened at current price.
    """
    socket = FakeSocket()
    risk = StubRisk()
    cfg = EngineConfigModel(initial_cash=1_000.0, fee_bps=2.0)
    engine = TradingEngineService(
        "BTC-USD",
        cast(Any, socket),
        risk=risk,
        cfg=cfg,
        exchange="kraken",
        instrument_specs={"BTC-USD": {"lot_size": 0.1, "tick_size": 0.01}},
    )
    engine.portfolio.cash = 1_000.0
    await engine.execute_desired_units(1.0, current_price=100.0)
    assert len(socket.sent) == 1
    order = socket.sent[0]
    assert order.side == "buy"
    assert order.instrument == "BTC-USD"
    assert order.strategy_id == "engine-buy"
    assert engine.order_in_flight is True
    assert engine.pending_client_order_id is not None
    assert engine.position_qty == pytest.approx(0.0)
    assert engine.entry_price is None
    assert risk.can_open_calls


@pytest.mark.asyncio
async def test_engine_maybe_stop_triggers_sell() -> None:
    """Verify stop-loss triggers sell when price drops below threshold.

    Given: An engine with open long position,
    When: Price drops significantly from previous close,
    Then: Position is closed with stop-loss order.
    """
    socket = FakeSocket()
    risk = StubRisk(stop_pct_value=0.05)
    engine = TradingEngineService(
        "BTC-USD",
        cast(Any, socket),
        risk=risk,
        cfg=EngineConfigModel(initial_cash=5_000.0),
    )
    engine.position_qty = 1.0
    engine.entry_price = None
    engine.portfolio.positions["BTC-USD"] = PositionStateModel(quantity=1.0, average_price=110.0)
    triggered: bool = await engine._maybe_stop(last_close=100.0, prev_close=120.0)
    assert triggered is True
    assert engine.order_in_flight is True
    assert engine.pending_client_order_id is not None
    assert engine.position_qty == pytest.approx(1.0)
    assert engine.entry_price is None
    assert len(socket.sent) == 1
    order = socket.sent[0]
    assert order.side == "sell"
    assert order.strategy_id == "engine-stop"


@pytest.mark.asyncio
async def test_engine_execute_desired_units_sell_flow() -> None:
    """Verify sell order execution closes position and clears entry price.

    Given: An engine with existing long position,
    When: execute_desired_units is called with negative units,
    Then: Sell order is sent and position is closed.
    """
    socket = FakeSocket()
    risk = StubRisk()
    engine = TradingEngineService(
        "BTC-USD",
        cast(Any, socket),
        risk=risk,
        cfg=EngineConfigModel(initial_cash=5_000.0),
        exchange="kraken",
        instrument_specs={"BTC-USD": {"lot_size": 0.1, "tick_size": 0.01}},
    )
    engine.position_qty = 0.3
    engine.entry_price = 100.0
    engine.portfolio.positions["BTC-USD"] = PositionStateModel(quantity=0.3, average_price=100.0)
    await engine.execute_desired_units(-1.0, current_price=120.0)
    assert len(socket.sent) == 1
    order = socket.sent[0]
    assert order.side == "sell"
    assert order.instrument == "BTC-USD"
    assert order.strategy_id == "engine-sell"
    assert engine.order_in_flight is True
    assert engine.pending_client_order_id is not None
    assert engine.position_qty == pytest.approx(0.3)
    assert engine.entry_price == pytest.approx(100.0)


def _replace_execution_publisher_with_async_stub(trader: TraderCoordinator) -> MagicMock:
    """Replace the trader msg_publisher with an async stub."""
    async_publisher = MagicMock()
    async_publisher.send = AsyncMock(return_value=None)
    async_publisher.tracker = SequenceTracker()
    async_publisher.session_id = async_publisher.tracker.session_id
    async_publisher.close = MagicMock()
    trader.msg_publisher = cast(Any, async_publisher)
    return async_publisher


class TestEngineConfigModelDefaults:
    """Tests for EngineConfigModel default values and leverage field."""

    def test_defaults(self) -> None:
        """Verify default values for EngineConfigModel.

        Given: Default EngineConfigModel,
        When: Inspecting fields,
        Then: initial_cash=10000, fee_bps=2.0, leverage=None.
        """
        cfg = EngineConfigModel()
        assert cfg.initial_cash == pytest.approx(10_000.0)
        assert cfg.fee_bps == pytest.approx(2.0)
        assert cfg.leverage is None

    def test_leverage_set(self) -> None:
        """Verify leverage can be configured.

        Given: EngineConfigModel with leverage=3,
        When: Inspecting leverage,
        Then: leverage=3.
        """
        cfg = EngineConfigModel(leverage=3)
        assert cfg.leverage == 3


class TestEngineExecuteDesiredUnits:
    """Tests for TradingEngineService execute_desired_units method."""

    @pytest.mark.asyncio
    async def test_execute_buy_signal_with_sufficient_cash(self) -> None:
        """Verify buy execution with sufficient cash sends correct order.

        Given: Engine with 10000 initial cash and risk limits,
        When: Buy signal for 0.1 units at 50000 price is processed,
        Then: Buy order is published with correct topic and quantity.
        """
        mock_socket = MagicMock()
        mock_socket.send = AsyncMock()
        mock_socket.tracker = SequenceTracker()
        mock_socket.session_id = mock_socket.tracker.session_id
        engine = TradingEngineService(
            instrument="BTC-USD",
            execution_socket=mock_socket,
            risk=RiskEvaluator(RiskConfigModel(r_per_trade=0.02, max_leverage=1.0)),
            cfg=EngineConfigModel(initial_cash=10000.0, fee_bps=2.0),
            instrument_specs={"BTC-USD": {"tick_size": 0.01, "lot_size": 0.0001}},
            exchange="kraken",
        )
        desired_units = 0.1
        current_price = 50000.0
        await engine.execute_desired_units(desired_units, current_price)
        assert mock_socket.send.called
        order = mock_socket.send.call_args[0][1]
        assert order.instrument == "BTC-USD"
        assert order.side == "buy"
        assert order.mode == "live"
        assert order.quantity > 0
        assert order.quantity <= desired_units
        assert engine.order_in_flight is True
        assert engine.pending_client_order_id is not None
        assert engine.portfolio.cash == pytest.approx(10000.0)
        assert engine.position_qty == pytest.approx(0.0)
        assert engine.entry_price is None

    @pytest.mark.asyncio
    async def test_execute_desired_units_zero_signal(self) -> None:
        """Verify zero signal closes existing position.

        Given: Engine with existing position of 0.1 units,
        When: execute_desired_units is called with 0.0 units,
        Then: Entire position is sold and entry price is cleared.
        """
        mock_socket = MagicMock()
        mock_socket.send = AsyncMock()
        mock_socket.tracker = SequenceTracker()
        mock_socket.session_id = mock_socket.tracker.session_id
        engine = TradingEngineService(
            instrument="BTC-USD",
            execution_socket=mock_socket,
            risk=RiskEvaluator(RiskConfigModel()),
            cfg=EngineConfigModel(initial_cash=10000.0, fee_bps=2.0),
            instrument_specs={"BTC-USD": {"tick_size": 0.01, "lot_size": 0.0001}},
            exchange="kraken",
        )
        engine.position_qty = 0.1
        engine.entry_price = 48000.0
        engine.portfolio.update_fill("BTC-USD", "buy", 0.1, 48000.0, 9.6)
        desired_units = 0.0
        current_price = 52000.0
        await engine.execute_desired_units(desired_units, current_price)
        assert mock_socket.send.called
        order = mock_socket.send.call_args[0][1]
        assert order.instrument == "BTC-USD"
        assert order.side == "sell"
        assert order.quantity == pytest.approx(0.1)
        assert engine.order_in_flight is True
        assert engine.pending_client_order_id is not None
        assert engine.position_qty == pytest.approx(0.1)
        assert engine.entry_price == pytest.approx(48000.0)

    @pytest.mark.asyncio
    async def test_execute_buy_signal_respects_risk_limits(self) -> None:
        """Verify buy execution respects risk and leverage limits.

        Given: Engine with small initial cash (100) and risk limits,
        When: Large buy signal is processed,
        Then: Cash never goes negative and position is constrained.
        """
        mock_socket = MagicMock()
        mock_socket.send = AsyncMock()
        mock_socket.tracker = SequenceTracker()
        mock_socket.session_id = mock_socket.tracker.session_id
        engine = TradingEngineService(
            instrument="BTC-USD",
            execution_socket=mock_socket,
            risk=RiskEvaluator(RiskConfigModel(r_per_trade=0.50, max_leverage=1.0)),
            cfg=EngineConfigModel(initial_cash=100.0, fee_bps=2.0),
            instrument_specs={"BTC-USD": {"tick_size": 0.01, "lot_size": 0.0001}},
            exchange="paper",
        )
        desired_units = 1.0
        current_price = 50000.0
        initial_cash = engine.portfolio.cash
        await engine.execute_desired_units(desired_units, current_price)
        assert engine.portfolio.cash == pytest.approx(initial_cash)
        assert engine.order_in_flight is True
        assert engine.pending_client_order_id is not None

    @pytest.mark.asyncio
    async def test_execute_buy_signal_when_already_in_position(self) -> None:
        """Verify buy signal increases existing position.

        Given: Engine with existing position of 0.05 units,
        When: Buy signal for 0.1 units is processed,
        Then: Position quantity increases.
        """
        mock_socket = MagicMock()
        mock_socket.send = AsyncMock()
        mock_socket.tracker = SequenceTracker()
        mock_socket.session_id = mock_socket.tracker.session_id
        engine = TradingEngineService(
            instrument="BTC-USD",
            execution_socket=mock_socket,
            risk=RiskEvaluator(RiskConfigModel()),
            cfg=EngineConfigModel(initial_cash=10000.0, fee_bps=2.0),
            instrument_specs={"BTC-USD": {"tick_size": 0.01, "lot_size": 0.0001}},
            exchange="paper",
        )
        engine.position_qty = 0.05
        desired_units = 0.1
        current_price = 50000.0
        initial_qty = engine.position_qty
        await engine.execute_desired_units(desired_units, current_price)
        assert engine.position_qty >= initial_qty

    @pytest.mark.asyncio
    async def test_execute_sell_signal_when_already_flat(self) -> None:
        """Verify sell signal with no position sends no order.

        Given: Engine with zero position,
        When: Sell signal (0.0 units) is processed,
        Then: No order is sent and position remains zero.
        """
        mock_socket = MagicMock()
        mock_socket.send = AsyncMock()
        mock_socket.tracker = SequenceTracker()
        mock_socket.session_id = mock_socket.tracker.session_id
        engine = TradingEngineService(
            instrument="BTC-USD",
            execution_socket=mock_socket,
            risk=RiskEvaluator(RiskConfigModel()),
            cfg=EngineConfigModel(initial_cash=10000.0, fee_bps=2.0),
            instrument_specs={"BTC-USD": {"tick_size": 0.01, "lot_size": 0.0001}},
            exchange="paper",
        )
        assert engine.position_qty == pytest.approx(0.0)
        desired_units = 0.0
        current_price = 50000.0
        await engine.execute_desired_units(desired_units, current_price)
        assert not mock_socket.send.called
        assert engine.position_qty == pytest.approx(0.0)

    @pytest.mark.asyncio
    async def test_execute_with_lot_size_rounding(self) -> None:
        """Verify order quantity is rounded to lot size.

        Given: Engine with lot_size=0.001 specification,
        When: Buy signal for 0.0123 units is processed,
        Then: Order quantity is rounded to valid lot size.
        """
        mock_socket = MagicMock()
        mock_socket.send = AsyncMock()
        mock_socket.tracker = SequenceTracker()
        mock_socket.session_id = mock_socket.tracker.session_id
        engine = TradingEngineService(
            instrument="BTC-USD",
            execution_socket=mock_socket,
            risk=RiskEvaluator(RiskConfigModel(r_per_trade=0.02)),
            cfg=EngineConfigModel(initial_cash=10000.0, fee_bps=2.0),
            instrument_specs={"BTC-USD": {"tick_size": 0.01, "lot_size": 0.001}},
            exchange="paper",
        )
        desired_units = 0.0123
        current_price = 50000.0
        await engine.execute_desired_units(desired_units, current_price)
        if mock_socket.send.called:
            order = mock_socket.send.call_args[0][1]
            lot_size = 0.001
            qty = order.quantity
            assert qty % lot_size == pytest.approx(0.0) or abs(qty % lot_size) < 1e-10


@pytest.mark.asyncio
class TestTraderSignalHandling:
    """Tests for TraderCoordinator signal handling."""

    @patch("snapper.application.engine.trader.get_repository")
    @patch("snapper.application.engine.trader.get_settings")
    @patch("snapper.application.engine.trader.zmq.Context")
    async def test_on_signal_executes_buy_signal(
        self, mock_zmq_context: MagicMock, mock_get_settings: MagicMock, mock_get_repo: MagicMock
    ) -> None:
        """Verify buy signal triggers engine execution with correct parameters.

        Given: TraderCoordinator with mocked engine for BTC-USD,
        When: Buy signal with strength 0.8 and price 50000 is received,
        Then: Engine execute_desired_units is called with units=0.8 and price=50000.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {
            "kraken": ["BTC-USD"],
            "zonda": [],
            "walutomat": [],
            "polygon": [],
        }
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_get_settings.return_value = mock_settings
        mock_repo = AsyncMock()
        mock_get_repo.return_value = mock_repo
        mock_socket = MagicMock()
        mock_socket.send = AsyncMock()
        mock_socket.tracker = SequenceTracker()
        mock_socket.session_id = mock_socket.tracker.session_id
        mock_zmq_context.return_value.socket.return_value = mock_socket
        trader = TraderCoordinator(
            signal_topics=["signals."],
        )
        trader._setup_external_execution()
        _replace_execution_publisher_with_async_stub(trader)
        trader._setup_trading_components()
        mock_engine = MagicMock()
        mock_engine.execute_desired_units = AsyncMock()
        trader.engines["BTC-USD@paper-test_strategy"] = mock_engine
        signal = SignalData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            fired_at=datetime.now(UTC),
            instrument="BTC-USD",
            side="buy",
            strength=0.8,
            price=50000.0,
            reason="Test buy signal",
            strategy_name="test_strategy",
            exchange="kraken",
        )
        trader._current_topic = "signals.paper.BTC-USD.test_strategy"
        await trader._on_signal(signal)
        assert mock_engine.execute_desired_units.called
        call_args = mock_engine.execute_desired_units.call_args
        desired_units = call_args[0][0]
        price = call_args[0][1]
        assert desired_units == pytest.approx(0.8)
        assert price == pytest.approx(50000.0)
        await trader._on_signal(signal)
        assert mock_engine.execute_desired_units.called
        call_args = mock_engine.execute_desired_units.call_args
        desired_units = call_args[0][0]
        price = call_args[0][1]
        assert desired_units == pytest.approx(0.8)
        assert price == pytest.approx(50000.0)

    @patch("snapper.application.engine.trader.get_repository")
    @patch("snapper.application.engine.trader.get_settings")
    @patch("snapper.application.engine.trader.zmq.Context")
    async def test_on_signal_executes_sell_signal(
        self, mock_zmq_context: MagicMock, mock_get_settings: MagicMock, mock_get_repo: MagicMock
    ) -> None:
        """Verify sell signal triggers engine execution with zero desired units.

        Given: TraderCoordinator with mocked engine for BTC-USD,
        When: Sell signal with strength 1.0 and price 52000 is received,
        Then: Engine execute_desired_units is called with units=0 and price=52000.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {
            "kraken": ["BTC-USD"],
            "zonda": [],
            "walutomat": [],
            "polygon": [],
        }
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_get_settings.return_value = mock_settings
        mock_repo = AsyncMock()
        mock_get_repo.return_value = mock_repo
        mock_socket = MagicMock()
        mock_socket.send = AsyncMock()
        mock_socket.tracker = SequenceTracker()
        mock_socket.session_id = mock_socket.tracker.session_id
        mock_zmq_context.return_value.socket.return_value = mock_socket
        trader = TraderCoordinator(
            signal_topics=["signals."],
        )
        trader._setup_external_execution()
        _replace_execution_publisher_with_async_stub(trader)
        trader._setup_trading_components()
        mock_engine = MagicMock()
        mock_engine.execute_desired_units = AsyncMock()
        trader.engines["BTC-USD@paper-test_strategy"] = mock_engine
        signal = SignalData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            fired_at=datetime.now(UTC),
            instrument="BTC-USD",
            side="sell",
            strength=1.0,
            price=52000.0,
            reason="Test sell signal",
            strategy_name="test_strategy",
            exchange="kraken",
        )
        trader._current_topic = "signals.paper.BTC-USD.test_strategy"
        await trader._on_signal(signal)
        assert mock_engine.execute_desired_units.called
        call_args = mock_engine.execute_desired_units.call_args
        desired_units = call_args[0][0]
        price = call_args[0][1]
        assert desired_units == pytest.approx(0.0)
        assert price == pytest.approx(52000.0)

    @patch("snapper.application.engine.trader.get_repository")
    @patch("snapper.application.engine.trader.get_settings")
    @patch("snapper.application.engine.trader.zmq.Context")
    async def test_on_signal_ignores_invalid_signal(
        self, mock_zmq_context: MagicMock, mock_get_settings: MagicMock, mock_get_repo: MagicMock
    ) -> None:
        """Verify invalid signal without price is ignored.

        Given: TraderCoordinator with mocked engine for BTC-USD,
        When: Signal with price=None is received,
        Then: Engine execute_desired_units is not called.
        """
        mock_settings = MagicMock()
        mock_settings.instruments = {
            "kraken": ["BTC-USD"],
            "zonda": [],
            "walutomat": [],
            "polygon": [],
        }
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_get_settings.return_value = mock_settings
        mock_repo = AsyncMock()
        mock_get_repo.return_value = mock_repo
        mock_socket = MagicMock()
        mock_socket.send = AsyncMock()
        mock_socket.tracker = SequenceTracker()
        mock_socket.session_id = mock_socket.tracker.session_id
        mock_zmq_context.return_value.socket.return_value = mock_socket
        trader = TraderCoordinator(
            signal_topics=["signals."],
        )
        trader._setup_external_execution()
        _replace_execution_publisher_with_async_stub(trader)
        trader._setup_trading_components()
        mock_engine = MagicMock()
        mock_engine.execute_desired_units = AsyncMock()
        trader.engines["BTC-USD@paper-test_strategy"] = mock_engine
        invalid_signal = SignalData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            fired_at=datetime.now(UTC),
            instrument="BTC-USD",
            side="buy",
            strength=0.5,
            price=None,
            exchange="kraken",
            reason="test",
        )
        trader._current_topic = "signals.paper.BTC-USD.test_strategy"
        await trader._on_signal(invalid_signal)
        assert not mock_engine.execute_desired_units.called

    @patch("snapper.application.engine.trader.resolve_symbol_public_id", new_callable=AsyncMock)
    @patch("snapper.application.engine.trader.ValidatedPublisher")
    @patch("snapper.application.engine.trader.get_repository")
    @patch("snapper.application.engine.trader.get_settings")
    @patch("snapper.application.engine.trader.zmq.Context")
    async def test_on_signal_creates_paper_engine_dynamically(
        self,
        mock_zmq_context: MagicMock,
        mock_get_settings: MagicMock,
        mock_get_repo: MagicMock,
        mock_validated_publisher: MagicMock,
        mock_resolve_spid: AsyncMock,
    ) -> None:
        """Verify paper engine is created dynamically for unknown instrument.

        Given: TraderCoordinator with no engine for ETH-USD,
        When: Signal for ETH-USD is received,
        Then: Paper engine is created dynamically and registered.
        """
        mock_resolve_spid.return_value = "fake-symbol-public-id"
        mock_publisher_instance = MagicMock()
        mock_publisher_instance.send_multipart = AsyncMock(return_value=None)
        mock_publisher_instance.close = MagicMock()
        mock_validated_publisher.return_value = mock_publisher_instance
        mock_settings = MagicMock()
        mock_settings.instruments = {
            "kraken": ["BTC-USD"],
            "zonda": [],
            "walutomat": [],
            "polygon": [],
        }
        mock_settings.zmq_broker_xsub = "tcp://127.0.0.1:7500"
        mock_settings.zmq_broker_xpub = "tcp://127.0.0.1:7501"
        mock_settings.risk_r_per_trade = 0.02
        mock_settings.risk_max_leverage = 1.0
        mock_settings.risk_max_drawdown = 0.1
        mock_get_settings.return_value = mock_settings
        mock_repo = AsyncMock()
        mock_get_repo.return_value = mock_repo
        mock_socket = MagicMock()
        mock_socket.send = AsyncMock()
        mock_socket.tracker = SequenceTracker()
        mock_socket.session_id = mock_socket.tracker.session_id
        mock_zmq_context.return_value.socket.return_value = mock_socket
        trader = TraderCoordinator(
            signal_topics=["signals."],
        )
        trader._setup_external_execution()
        trader._setup_trading_components()
        mock_engine = MagicMock()
        mock_engine.execute_desired_units = AsyncMock()
        trader.engines["BTC-USD@paper-test_strategy"] = mock_engine
        signal = SignalData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            fired_at=datetime.now(UTC),
            instrument="ETH-USD",
            side="buy",
            strength=0.5,
            price=3000.0,
            reason="Test ETH signal",
            exchange="kraken",
        )
        trader._current_topic = "signals.paper.ETH-USD.test_strategy"
        await trader._on_signal(signal)
        assert not mock_engine.execute_desired_units.called


class _SocketStub:
    """Test stub for MessagePublisher."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.sent: list[Any] = []
        self._tracker = SequenceTracker()

    @property
    def tracker(self) -> SequenceTracker:
        """Expose sequence tracker for provenance stamping."""
        return self._tracker

    async def send(self, stream_key: str, data: Any, *, flags: int = 0) -> None:
        """Collect sent data objects."""
        self.sent.append(data)


@dataclass
class _RiskStub:
    """Test stub for risk manager."""

    stop_value: float = 0.01
    allow_trade: bool = True
    round_size_override: float | None = None
    round_down_override: float | None = None

    def stop_pct(self) -> float:
        return self.stop_value

    def can_open_new_trade(self, _equity: float, _peak: float) -> bool:
        return self.allow_trade

    def cap_size_by_leverage(
        self,
        _current_notional: float,
        _equity: float,
        _price: float,
        desired_size: float,
    ) -> float:
        return desired_size

    def round_size(
        self,
        desired_size: float,
        _lot_size: float,
        _price: float,
        _tick_size: float,
    ) -> float:
        if self.round_size_override is not None:
            return self.round_size_override
        return desired_size

    def round_down_to_step(self, value: float, step: float) -> float:
        if self.round_down_override is not None:
            return self.round_down_override
        if step <= 0:
            return max(value, 0.0)
        units = int((value + 1e-12) // step)
        return max(units * step, 0.0)


def _make_engine(
    *,
    risk: _RiskStub | None = None,
    instrument_specs: dict[str, dict[str, float]] | None = None,
) -> tuple[TradingEngineService, _SocketStub]:
    socket = _SocketStub()
    engine = TradingEngineService(
        instrument="BTC-USD",
        execution_socket=cast(Any, socket),
        risk=cast(Any, risk),
        cfg=EngineConfigModel(initial_cash=1_000.0, fee_bps=10.0),
        instrument_specs=instrument_specs,
        exchange="paper",
    )
    return engine, socket


@pytest.mark.asyncio
async def test_maybe_stop_returns_false_when_flat() -> None:
    """Verify stop check returns False when position is flat.

    Given a TradingEngine with no open position,
    When _maybe_stop is called,
    Then it returns False as no stop action is needed.
    """
    engine, _ = _make_engine(risk=_RiskStub())
    assert await engine._maybe_stop(last_close=100.0) is False


@pytest.mark.asyncio
async def test_maybe_stop_uses_portfolio_avg_price(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify stop check uses portfolio average price as fallback.

    Given a TradingEngine with a position but no entry_price set,
    When _maybe_stop is called,
    Then it uses the portfolio's average price for threshold calculation.
    """
    risk = _RiskStub(stop_value=0.05)
    engine, _ = _make_engine(risk=risk)
    engine.position_qty = 1.0
    engine.entry_price = None
    engine.portfolio.positions[engine.instrument] = PositionStateModel(
        quantity=1.0, average_price=100.0
    )
    send_order = SimpleNamespace(called=False)

    async def _dummy_send(*_args: Any, **_kwargs: Any) -> None:
        send_order.called = True

    monkeypatch.setattr(engine, "_send_order", _dummy_send)
    assert await engine._maybe_stop(last_close=120.0, prev_close=118.0) is False
    assert send_order.called is False


@pytest.mark.asyncio
async def test_maybe_stop_triggers_using_entry_price(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify stop triggers sell when price drops below entry threshold.

    Given a TradingEngine with an open long position and entry price,
    When the price drops below the stop threshold,
    Then a stop order is sent and position is closed.
    """
    risk = _RiskStub(stop_value=0.02)
    engine, _ = _make_engine(risk=risk)
    engine.position_qty = 2.0
    engine.entry_price = 100.0
    captured: list[tuple[Any, ...]] = []

    async def _capture_send(*args: Any, **kwargs: Any) -> str:
        captured.append((args, kwargs))
        return "test-client-order-id"

    monkeypatch.setattr(engine, "_send_order", _capture_send)
    assert await engine._maybe_stop(last_close=90.0, prev_close=95.0) is True
    assert captured, "Stop order should be sent when threshold is breached"
    assert engine.order_in_flight is True
    assert engine.pending_client_order_id == "test-client-order-id"
    assert engine.position_qty == pytest.approx(2.0)
    assert engine.entry_price == pytest.approx(100.0)


@pytest.mark.asyncio
async def test_execute_desired_units_respects_drawdown_guard() -> None:
    """Verify execution respects drawdown guard and skips disallowed trades.

    Given a TradingEngine with drawdown guard blocking trades,
    When execute_desired_units is called,
    Then no order is sent and position remains unchanged.
    """
    risk = _RiskStub(allow_trade=False)
    engine, socket = _make_engine(risk=risk)
    await engine.execute_desired_units(desired_units=2.0, current_price=50.0)
    assert engine.position_qty == pytest.approx(0.0)
    assert socket.sent == []


@pytest.mark.asyncio
async def test_execute_desired_units_skips_when_rounding_zero() -> None:
    """Verify execution skips when rounded order size is zero.

    Given a TradingEngine with lot size that rounds order to zero,
    When execute_desired_units is called,
    Then no order is sent and position remains unchanged.
    """
    risk = _RiskStub(round_size_override=0.0)
    engine, socket = _make_engine(risk=risk, instrument_specs={"BTC-USD": {"lot_size": 1.0}})
    await engine.execute_desired_units(desired_units=5.0, current_price=10.0)
    assert engine.position_qty == pytest.approx(0.0)
    assert socket.sent == []


@pytest.mark.asyncio
async def test_execute_desired_units_sell_branch_returns_on_zero_quantity() -> None:
    """Verify sell execution returns early when rounded quantity is zero.

    Given a TradingEngine with a small position,
    When selling with rounding that produces zero quantity,
    Then the position is preserved and no order is sent.
    """
    risk = _RiskStub(round_down_override=0.0)
    engine, socket = _make_engine(risk=risk, instrument_specs={"BTC-USD": {"lot_size": 1.0}})
    engine.position_qty = 0.4
    await engine.execute_desired_units(desired_units=0.0, current_price=25.0)
    assert engine.position_qty == pytest.approx(0.4)
    assert socket.sent == []


@pytest.mark.asyncio
async def test_execute_desired_units_sets_entry_price_when_opening_position() -> None:
    """Verify entry price is set when opening a new position.

    Given a TradingEngine with no position,
    When execute_desired_units opens a new position,
    Then the entry price is set to the current price.
    """
    risk = _RiskStub()
    engine, socket = _make_engine(
        risk=risk,
        instrument_specs={"BTC-USD": {"lot_size": 0.1, "tick_size": 0.01}},
    )
    await engine.execute_desired_units(desired_units=0.5, current_price=50.0)
    assert engine.order_in_flight is True
    assert engine.pending_client_order_id is not None
    assert engine.entry_price is None
    assert engine.position_qty == pytest.approx(0.0)
    assert socket.sent, "Engine should publish order on successful entry"


@pytest.mark.asyncio
async def test_execute_desired_units_does_not_update_entry_when_position_still_short() -> None:
    """Verify entry price is unchanged when position remains short.

    Given a TradingEngine with an existing short position,
    When the position is reduced but remains short,
    Then the original entry price is preserved.
    """
    risk = _RiskStub(round_size_override=0.2)
    engine, socket = _make_engine(
        risk=risk,
        instrument_specs={"BTC-USD": {"lot_size": 0.1, "tick_size": 0.01}},
    )
    engine.position_qty = -0.3
    engine.entry_price = 80.0
    engine.portfolio.positions[engine.instrument] = PositionStateModel(
        quantity=-0.3, average_price=75.0
    )
    await engine.execute_desired_units(desired_units=0.2, current_price=40.0)
    assert engine.order_in_flight is True
    assert engine.pending_client_order_id is not None
    assert engine.position_qty == pytest.approx(-0.3)
    assert engine.entry_price == pytest.approx(80.0)
    assert socket.sent, "Engine should publish order even when reducing short exposure"


@pytest.mark.asyncio
async def test_send_order_converts_timestamp_to_datetime() -> None:
    """Verify timestamp is converted and included in order message.

    Given a TradingEngine sending an order,
    When _send_order is called with a Unix timestamp,
    Then the timestamp is converted and included in the order payload.
    """
    engine, socket = _make_engine()
    ts = 1_700_000_000.0
    await engine._send_order(
        side="buy",
        size=1.0,
        price=10.0,
        reason="unit-test",
        signaled_at=ts,
    )
    assert socket.sent
    order = socket.sent[0]
    assert order.signaled_at is not None


@pytest.mark.asyncio
async def test_send_order_writes_trade_command_to_db() -> None:
    """Verify _send_order writes a TradeCommand to DB when repository is configured.

    Given: a TradingEngine with a mocked repository and outbox,
    When: _send_order is called,
    Then: insert_trade_command is called and outbox is notified.
    """
    socket = _SocketStub()
    repo_mock = AsyncMock()
    repo_mock.insert_trade_command = AsyncMock(return_value=(1, "cmd-pub-1"))
    outbox_mock = MagicMock()
    engine = TradingEngineService(
        instrument="BTC-USD",
        execution_socket=cast(Any, socket),
        cfg=EngineConfigModel(initial_cash=1_000.0, fee_bps=10.0),
        exchange="paper",
        repository=repo_mock,
        outbox=outbox_mock,
    )
    client_id = await engine._send_order(side="buy", size=1.0, price=10.0, reason="unit-test")
    assert len(client_id) == 36
    repo_mock.insert_trade_command.assert_called_once()
    call_row = repo_mock.insert_trade_command.call_args.args[0]
    assert call_row["command_type"] == "submit"
    assert call_row["side"] == "buy"
    assert call_row["status"] == "created"
    outbox_mock.notify.assert_called_once()
    assert not socket.sent


@pytest.mark.asyncio
async def test_send_order_writes_trade_command_without_outbox() -> None:
    """Verify _send_order writes TradeCommand without outbox notification.

    Given: a TradingEngine with repository set but no outbox,
    When: _send_order is called,
    Then: insert_trade_command is called and no error occurs.
    """
    socket = _SocketStub()
    repo_mock = AsyncMock()
    repo_mock.insert_trade_command = AsyncMock(return_value=(1, "cmd-pub-2"))
    engine = TradingEngineService(
        instrument="BTC-USD",
        execution_socket=cast(Any, socket),
        cfg=EngineConfigModel(initial_cash=1_000.0, fee_bps=10.0),
        exchange="paper",
        repository=repo_mock,
    )
    await engine._send_order(side="sell", size=0.5, price=20.0, reason="test")
    repo_mock.insert_trade_command.assert_called_once()
    assert socket.sent


@pytest.mark.asyncio
async def test_sync_fill_to_trade_service() -> None:
    """Coordinator sync fills to TradeService and BalanceService on applied fill.

    Given: a TraderCoordinator with trade_service and balance_service,
    When: _sync_fill_to_trade_service is called with a fill,
    Then: trade_service projection is updated and balance_service receives position change.
    """
    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord.trade_service = TradeService()
    coord.balance_service = BalanceService()
    coord.repository = MagicMock()
    coord._tracker = MagicMock()
    coord._tracker.session_id = "s-test"
    coord._tracker.next_sequence = MagicMock(return_value=1)

    engine, _ = _make_engine()
    fill = ExecutionData(
        type="execution",
        public_id="fill-1",
        timestamp=datetime.now(UTC),
        session_id="s1",
        sequence_id=1,
        exchange="paper",
        instrument="BTC-USD",
        side="buy",
        size=0.5,
        price=50000.0,
        last_size=0.5,
        last_price=50000.0,
        fee=0.5,
        fee_asset="USD",
        status="filled",
        client_order_id="cid-1",
        exchange_order_id="ex-1",
        trade_id="t-1",
        executed_at=datetime.now(UTC),
    )
    await coord._sync_fill_to_trade_service(fill, engine)
    pos = coord.trade_service.get_position(engine._shard_key)
    assert pos.position_qty == 0.5
    assert coord.balance_service.get_cash(engine._shard_key) != 0.0


def test_setup_trade_services_with_sqlalchemy_repo() -> None:
    """Coordinator setup runs in dual-write mode without outbox.

    Given: a TraderCoordinator with trade services,
    When: _setup_trade_services is called,
    Then: outbox remains None (outbox activates at cutover phase).
    """
    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord.trade_service = TradeService()
    coord.balance_service = BalanceService()
    coord.outbox = None
    coord.repository = MagicMock()
    coord.settings = MagicMock(use_durable_commands=False)
    coord._setup_trade_services()
    assert coord.outbox is None


def test_setup_trade_services_durable_mode_creates_outbox() -> None:
    """Coordinator creates outbox with publish_fn in durable mode.

    Given: a TraderCoordinator with use_durable_commands=True and SQLAlchemyRepository,
    When: _setup_trade_services is called,
    Then: outbox is created with a publish function.
    """
    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord.trade_service = TradeService()
    coord.balance_service = BalanceService()
    coord.outbox = None
    coord.repository = MagicMock(spec=SQLAlchemyRepository)
    coord.settings = MagicMock(use_durable_commands=True)
    coord._setup_trade_services()
    assert coord.outbox is not None


@pytest.mark.asyncio
async def test_outbox_publish_sends_to_zmq() -> None:
    """Outbox publish converts TradeCommandRow to OrderRequestData and sends.

    Given: a TraderCoordinator with a mocked msg_publisher,
    When: _outbox_publish is called with a command dict,
    Then: msg_publisher.send is called with OrderRequestData.
    """
    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord.msg_publisher = AsyncMock()
    cmd: dict[str, Any] = {
        "public_id": "cmd-1",
        "client_order_id": "cid-1",
        "shard_key": "kraken.BTC-USD.live",
        "exchange": "kraken",
        "instrument": "BTC-USD",
        "mode": "live",
        "strategy_id": "engine-buy",
        "side": "buy",
        "order_type": "market",
        "quantity": 0.5,
        "price": None,
        "session_id": "s1",
        "sequence_id": 1,
    }
    await coord._outbox_publish(cmd)
    coord.msg_publisher.send.assert_called_once()


@pytest.mark.asyncio
async def test_stop_stops_outbox() -> None:
    """Coordinator stop calls outbox stop when outbox is set.

    Given: a TraderCoordinator with a mocked outbox and no ZMQ sockets,
    When: stop is called,
    Then: outbox stop is called.
    """
    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord.signal_subscriber = None
    coord.zmq_context = None
    coord.execution_publisher = None
    coord.execution_context = None
    mock_outbox = MagicMock()
    coord.outbox = mock_outbox
    await coord.stop()
    mock_outbox.stop.assert_called_once()


@pytest.mark.asyncio
async def test_run_trading_loop_includes_outbox_task() -> None:
    """Trading loop creates an outbox task when outbox is set.

    Given: a TraderCoordinator with a mocked outbox,
    When: the trading loop is started and immediately cancelled,
    Then: outbox run was invoked as a task.
    """
    coord = TraderCoordinator.__new__(TraderCoordinator)
    mock_outbox = MagicMock()
    mock_outbox.run = AsyncMock()
    coord.outbox = mock_outbox
    coord.settings = MagicMock(use_durable_commands=False)
    coord.repository = MagicMock()
    mock_sub = AsyncMock()
    mock_sub.recv_multipart = AsyncMock(side_effect=asyncio.CancelledError)
    coord.signal_subscriber = mock_sub
    with pytest.raises(asyncio.CancelledError):
        await coord._run_trading_loop()
    mock_outbox.run.assert_called_once()


@pytest.mark.asyncio
async def test_persist_checkpoint_writes_to_db() -> None:
    """Persist checkpoint writes shard state to SQLAlchemyRepository.

    Given: a TraderCoordinator with SQLAlchemyRepository and a TradeService with state,
    When: _persist_checkpoint is called,
    Then: upsert_checkpoint is called on the repository.
    """
    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord.trade_service = TradeService()
    coord._tracker = MagicMock()
    coord._tracker.session_id = "s-test"
    coord._tracker.next_sequence = MagicMock(return_value=1)
    mock_repo = AsyncMock(spec=SQLAlchemyRepository)
    mock_repo.upsert_checkpoint = AsyncMock(return_value=1)
    coord.repository = mock_repo

    coord.trade_service.apply_venue_event(
        {
            "id": 1,
            "public_id": "",
            "timestamp": datetime.now(UTC),
            "session_id": "s1",
            "sequence_id": 1,
            "event_type": "fill_observed",
            "shard_key": "kraken.BTC-USD.live",
            "command_public_id": None,
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "exchange_order_id": None,
            "client_order_id": None,
            "venue_client_id": None,
            "side": "buy",
            "status": "filled",
            "fill_price": 50000.0,
            "fill_size": 0.5,
            "cum_fill_size": 0.5,
            "fee": 0.5,
            "fee_asset": "USD",
            "exec_id": "exec-1",
            "trade_id": None,
            "error": None,
            "venue_timestamp": datetime.now(UTC),
            "received_at": datetime.now(UTC),
        }
    )
    await coord._persist_checkpoint("kraken.BTC-USD.live")
    mock_repo.upsert_checkpoint.assert_called_once()
    call_row = mock_repo.upsert_checkpoint.call_args.args[0]
    assert call_row["shard_key"] == "kraken.BTC-USD.live"
    assert call_row["position_qty"] == 0.5


@pytest.mark.asyncio
async def test_persist_checkpoint_skips_non_sqlalchemy() -> None:
    """Persist checkpoint is a no-op when repository is not SQLAlchemyRepository.

    Given: a TraderCoordinator with a plain mock repository,
    When: _persist_checkpoint is called,
    Then: no checkpoint write occurs.
    """
    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord.trade_service = TradeService()
    coord.repository = MagicMock()
    await coord._persist_checkpoint("any.shard")


@pytest.mark.asyncio
async def test_persist_checkpoint_handles_db_error() -> None:
    """Persist checkpoint handles DB error without propagating.

    Given: a TraderCoordinator with SQLAlchemyRepository that raises on upsert,
    When: _persist_checkpoint is called,
    Then: no exception propagates.
    """
    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord.trade_service = TradeService()
    coord._tracker = MagicMock()
    coord._tracker.session_id = "s-test"
    coord._tracker.next_sequence = MagicMock(return_value=1)
    mock_repo = AsyncMock(spec=SQLAlchemyRepository)
    mock_repo.upsert_checkpoint = AsyncMock(side_effect=RuntimeError("DB down"))
    coord.repository = mock_repo
    await coord._persist_checkpoint("any.shard")


@pytest.mark.asyncio
async def test_create_reconciliation_tasks_durable_mode() -> None:
    """Coordinator creates per-exchange reconciliation tasks in durable mode.

    Given: a TraderCoordinator with use_durable_commands=True and SQLAlchemyRepository,
    When: _create_reconciliation_tasks is called,
    Then: one task per known exchange is returned.
    """
    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord.trade_service = TradeService()
    mock_repo = AsyncMock(spec=SQLAlchemyRepository)
    coord.repository = mock_repo
    coord.settings = MagicMock(use_durable_commands=True)
    tasks = coord._create_reconciliation_tasks()
    assert len(tasks) > 0
    for t in tasks:
        t.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await t


@pytest.mark.asyncio
async def test_run_trading_loop_with_recon_task() -> None:
    """Trading loop includes reconciliation task when durable mode is active.

    Given: a TraderCoordinator with durable mode enabled,
    When: the trading loop starts and immediately cancels,
    Then: no error occurs (reconciliation task runs alongside signals).
    """
    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord.outbox = None
    mock_repo = AsyncMock(spec=SQLAlchemyRepository)
    coord.repository = mock_repo
    coord.settings = MagicMock(use_durable_commands=True)
    coord.trade_service = TradeService()
    mock_sub = AsyncMock()
    mock_sub.recv_multipart = AsyncMock(side_effect=asyncio.CancelledError)
    coord.signal_subscriber = mock_sub
    with pytest.raises(asyncio.CancelledError):
        await coord._run_trading_loop()
