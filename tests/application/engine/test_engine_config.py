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
from snapper.messaging.schemas.data import OrderCancelData
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


class TestEngineApplyFillShortSelling:
    """Tests for TradingEngineService.apply_fill with short positions."""

    def _make_engine(self) -> TradingEngineService:
        """Create a minimal engine for apply_fill testing."""
        socket = FakeSocket()
        risk = StubRisk()
        return TradingEngineService(
            "BTC-USD",
            cast(Any, socket),
            risk=risk,
            cfg=EngineConfigModel(initial_cash=10_000.0),
            exchange="kraken",
            instrument_specs={},
        )

    def _make_fill(
        self, side: str, last_size: float, last_price: float, trade_id: str = "t1"
    ) -> ExecutionData:
        """Create an ExecutionData fill event."""
        return ExecutionData(
            public_id="pub-1",
            timestamp=datetime.now(UTC),
            session_id="sess-1",
            sequence_id=1,
            trade_id=trade_id,
            exchange_order_id="exch-1",
            client_order_id="cli-1",
            instrument="BTC-USD",
            exchange="kraken",
            side=side,
            size=last_size,
            price=last_price,
            last_size=last_size,
            last_price=last_price,
            fee=0.0,
            fee_asset="USD",
            status="filled",
            executed_at=datetime.now(UTC),
        )

    def test_sell_from_flat_opens_short(self) -> None:
        """Verify SELL from flat opens a short position.

        Given: Engine with position_qty=0,
        When: SELL fill applied,
        Then: position_qty goes negative, entry_price set.
        """
        engine = self._make_engine()
        fill = self._make_fill("sell", 0.5, 100.0)
        engine.apply_fill(fill)
        assert engine.position_qty == pytest.approx(-0.5)
        assert engine.entry_price == pytest.approx(100.0)

    def test_buy_covers_short_to_flat(self) -> None:
        """Verify BUY covering short brings position to flat.

        Given: Engine with position_qty=-0.5,
        When: BUY fill of 0.5,
        Then: position_qty=0, entry_price=None.
        """
        engine = self._make_engine()
        engine.position_qty = -0.5
        engine.entry_price = 100.0
        fill = self._make_fill("buy", 0.5, 90.0, trade_id="t2")
        engine.apply_fill(fill)
        assert engine.position_qty == pytest.approx(0.0)
        assert engine.entry_price is None

    def test_sell_adds_to_short_vwaps_entry(self) -> None:
        """Verify additional SELL increases short magnitude with VWAP entry_price.

        Given: Engine with position_qty=-0.5 at entry_price=100,
        When: SELL fill of 0.3 at 110,
        Then: position_qty=-0.8, entry_price = (0.5*100 + 0.3*110)/0.8 = 103.75.
        """
        engine = self._make_engine()
        engine.position_qty = -0.5
        engine.entry_price = 100.0
        fill = self._make_fill("sell", 0.3, 110.0, trade_id="t3")
        engine.apply_fill(fill)
        assert engine.position_qty == pytest.approx(-0.8)
        assert engine.entry_price == pytest.approx(103.75)

    def test_buy_flips_short_to_long(self) -> None:
        """Verify oversized BUY flips from short to long.

        Given: Engine with position_qty=-0.5,
        When: BUY fill of 0.8,
        Then: position_qty=+0.3, entry_price reset to fill price.
        """
        engine = self._make_engine()
        engine.position_qty = -0.5
        engine.entry_price = 100.0
        fill = self._make_fill("buy", 0.8, 90.0, trade_id="t4")
        engine.apply_fill(fill)
        assert engine.position_qty == pytest.approx(0.3)
        assert engine.entry_price == pytest.approx(90.0)

    def test_sell_flips_long_to_short(self) -> None:
        """Verify oversized SELL flips from long to short.

        Given: Engine with position_qty=+0.5,
        When: SELL fill of 0.8,
        Then: position_qty=-0.3, entry_price reset to fill price.
        """
        engine = self._make_engine()
        engine.position_qty = 0.5
        engine.entry_price = 100.0
        fill = self._make_fill("sell", 0.8, 110.0, trade_id="t5")
        engine.apply_fill(fill)
        assert engine.position_qty == pytest.approx(-0.3)
        assert engine.entry_price == pytest.approx(110.0)


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


class TestExecuteDesiredUnitsShortSelling:
    """Tests for execute_desired_units short selling transitions."""

    def _make_engine(self) -> tuple[TradingEngineService, MagicMock]:
        """Create engine with mock socket for short selling tests."""
        mock_socket = MagicMock()
        mock_socket.send = AsyncMock()
        mock_socket.tracker = SequenceTracker()
        mock_socket.session_id = mock_socket.tracker.session_id
        engine = TradingEngineService(
            instrument="BTC-USD",
            execution_socket=mock_socket,
            risk=RiskEvaluator(RiskConfigModel(r_per_trade=0.02, max_leverage=1.0)),
            cfg=EngineConfigModel(initial_cash=10_000.0, fee_bps=2.0),
            instrument_specs={"BTC-USD": {"tick_size": 0.01, "lot_size": 0.0001}},
            exchange="kraken",
        )
        return engine, mock_socket

    @pytest.mark.asyncio
    async def test_open_short_from_flat(self) -> None:
        """Verify negative desired_units opens a short position.

        Given: Flat engine (position_qty=0),
        When: desired_units=-0.1,
        Then: SELL order sent.
        """
        engine, mock_socket = self._make_engine()
        await engine.execute_desired_units(-0.1, current_price=50_000.0)
        assert mock_socket.send.called
        order = mock_socket.send.call_args[0][1]
        assert order.side == "sell"
        assert order.quantity > 0
        assert engine.order_in_flight is True

    @pytest.mark.asyncio
    async def test_cover_short_to_flat(self) -> None:
        """Verify desired_units=0 closes a short position.

        Given: Engine with position_qty=-0.1,
        When: desired_units=0,
        Then: BUY order sent for 0.1.
        """
        engine, mock_socket = self._make_engine()
        engine.position_qty = -0.1
        engine.entry_price = 50_000.0
        engine.portfolio.update_fill("BTC-USD", "sell", 0.1, 50_000.0, 0.0)
        await engine.execute_desired_units(0.0, current_price=50_000.0)
        assert mock_socket.send.called
        order = mock_socket.send.call_args[0][1]
        assert order.side == "buy"
        assert order.quantity == pytest.approx(0.1)

    @pytest.mark.asyncio
    async def test_flip_long_to_short(self) -> None:
        """Verify desired_units < 0 when long sends SELL for full flip.

        Given: Engine with position_qty=+0.05,
        When: desired_units=-0.05,
        Then: SELL order for closing(0.05) + opening(<=0.05).
        """
        engine, mock_socket = self._make_engine()
        engine.position_qty = 0.05
        engine.entry_price = 50_000.0
        engine.portfolio.update_fill("BTC-USD", "buy", 0.05, 50_000.0, 0.0)
        await engine.execute_desired_units(-0.05, current_price=50_000.0)
        assert mock_socket.send.called
        order = mock_socket.send.call_args[0][1]
        assert order.side == "sell"
        assert order.quantity >= 0.05

    @pytest.mark.asyncio
    async def test_flip_short_to_long(self) -> None:
        """Verify desired_units > 0 when short sends BUY for full flip.

        Given: Engine with position_qty=-0.05,
        When: desired_units=+0.05,
        Then: BUY order for covering(0.05) + opening(<=0.05).
        """
        engine, mock_socket = self._make_engine()
        engine.position_qty = -0.05
        engine.entry_price = 50_000.0
        engine.portfolio.update_fill("BTC-USD", "sell", 0.05, 50_000.0, 0.0)
        await engine.execute_desired_units(0.05, current_price=50_000.0)
        assert mock_socket.send.called
        order = mock_socket.send.call_args[0][1]
        assert order.side == "buy"
        assert order.quantity >= 0.05

    @pytest.mark.asyncio
    async def test_short_drawdown_gate_blocks(self) -> None:
        """Verify drawdown gate blocks new short position.

        Given: Flat engine with drawdown exceeding limit,
        When: desired_units=-0.1,
        Then: No order sent.
        """
        engine, mock_socket = self._make_engine()
        engine.peak_equity = 20_000.0
        await engine.execute_desired_units(-0.1, current_price=50_000.0)
        assert not mock_socket.send.called

    @pytest.mark.asyncio
    async def test_close_short_no_drawdown_check(self) -> None:
        """Verify closing a short skips drawdown gate.

        Given: Short position with drawdown exceeding limit,
        When: desired_units=0 (close short),
        Then: BUY order sent (closing is always allowed).
        """
        engine, mock_socket = self._make_engine()
        engine.position_qty = -0.1
        engine.entry_price = 50_000.0
        engine.portfolio.update_fill("BTC-USD", "sell", 0.1, 50_000.0, 0.0)
        engine.peak_equity = 20_000.0
        await engine.execute_desired_units(0.0, current_price=50_000.0)
        assert mock_socket.send.called
        order = mock_socket.send.call_args[0][1]
        assert order.side == "buy"

    @pytest.mark.asyncio
    async def test_flip_long_to_short_drawdown_closes_only(self) -> None:
        """Verify flip under drawdown still closes the long but skips opening short.

        Given: Engine with position_qty=+0.05 and drawdown exceeding limit,
        When: desired_units=-0.05,
        Then: SELL order sent for closing portion (0.05) only, no short opened.
        """
        engine, mock_socket = self._make_engine()
        engine.position_qty = 0.05
        engine.entry_price = 50_000.0
        engine.portfolio.update_fill("BTC-USD", "buy", 0.05, 50_000.0, 0.0)
        engine.peak_equity = 20_000.0
        await engine.execute_desired_units(-0.05, current_price=50_000.0)
        assert mock_socket.send.called
        order = mock_socket.send.call_args[0][1]
        assert order.side == "sell"
        assert order.quantity == pytest.approx(0.05)

    @pytest.mark.asyncio
    async def test_already_at_target_no_order(self) -> None:
        """Verify no order when already at desired position.

        Given: Engine with position_qty=-0.1,
        When: desired_units=-0.1,
        Then: No order sent (delta is zero).
        """
        engine, mock_socket = self._make_engine()
        engine.position_qty = -0.1
        await engine.execute_desired_units(-0.1, current_price=50_000.0)
        assert not mock_socket.send.called


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
        assert desired_units == pytest.approx(-1.0)
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

    @patch("snapper.application.engine.trader.get_repository")
    @patch("snapper.application.engine.trader.get_settings")
    @patch("snapper.application.engine.trader.zmq.Context")
    async def test_on_signal_routes_wallet_tagged_signal_to_wallet_engine(
        self, mock_zmq_context: MagicMock, mock_get_settings: MagicMock, mock_get_repo: MagicMock
    ) -> None:
        """A signal with a populated wallet routes to its own engine.

        Given: A live signal whose wallet_public_id is populated AND a
            pre-existing flat-key engine entry for the same instrument
            (representing the legacy single-wallet template path),
        When: ``_on_signal`` is invoked,
        Then: The flat-key engine is NOT touched, and the wallet
            filter routes the signal to a separate wallet-keyed engine
            (``BTC-USD@kraken-live-w{wallet_short}``). The
            fail-closed guard is gone — wallet identity is now
            first-class through the engine key.
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
        mock_get_repo.return_value = AsyncMock()
        mock_socket = MagicMock()
        mock_socket.send = AsyncMock()
        mock_socket.tracker = SequenceTracker()
        mock_socket.session_id = mock_socket.tracker.session_id
        mock_zmq_context.return_value.socket.return_value = mock_socket
        trader = TraderCoordinator(signal_topics=["signals."])
        trader._setup_external_execution()
        _replace_execution_publisher_with_async_stub(trader)
        trader._setup_trading_components()
        legacy_engine = MagicMock()
        legacy_engine.execute_desired_units = AsyncMock()
        trader.engines["BTC-USD@kraken-live"] = legacy_engine
        wallet_engine = MagicMock()
        wallet_engine.execute_desired_units = AsyncMock()
        wallet_engine.pending_client_order_id = None
        wallet_engine._shard_key = "kraken.BTC-USD.live.w01975a8b3c7d"
        trader.engines["BTC-USD@kraken-live-w01975a8b3c7d"] = wallet_engine
        wallet_signal = SignalData(
            session_id="",
            sequence_id=0,
            public_id="test-public-id",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            fired_at=datetime.now(UTC),
            instrument="BTC-USD",
            side="buy",
            strength=0.5,
            price=50000.0,
            exchange="kraken",
            reason="phase-0c-wallet-routing",
            wallet_public_id="01975a8b-3c7d-7000-8000-aaaaaaaaaaaa",
        )
        trader._current_topic = "signals.kraken.BTC-USD.live"
        await trader._on_signal(wallet_signal)
        assert not legacy_engine.execute_desired_units.called
        assert wallet_engine.execute_desired_units.called

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
async def test_maybe_stop_short_triggers_on_price_rise(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify stop triggers BUY when short position and price rises.

    Given: Short position at entry_price=100 with 2% stop,
    When: Price rises to 103 (>2% above entry),
    Then: BUY order sent with size=abs(position_qty).
    """
    risk = _RiskStub(stop_value=0.02)
    engine, _ = _make_engine(risk=risk)
    engine.position_qty = -2.0
    engine.entry_price = 100.0
    captured: list[tuple[Any, ...]] = []

    async def _capture_send(*args: Any, **kwargs: Any) -> str:
        captured.append((args, kwargs))
        return "test-stop-short"

    monkeypatch.setattr(engine, "_send_order", _capture_send)
    assert await engine._maybe_stop(last_close=103.0) is True
    assert engine.order_in_flight is True
    call_kwargs = captured[0][1]
    assert call_kwargs["side"] == "buy"
    assert call_kwargs["size"] == pytest.approx(2.0)


@pytest.mark.asyncio
async def test_maybe_stop_short_no_trigger_on_price_fall(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify no stop when short position and price falls (favorable).

    Given: Short position at entry_price=100 with 2% stop,
    When: Price falls to 97,
    Then: No stop triggered.
    """
    risk = _RiskStub(stop_value=0.02)
    engine, _ = _make_engine(risk=risk)
    engine.position_qty = -2.0
    engine.entry_price = 100.0

    async def _noop_send(*_args: Any, **_kwargs: Any) -> str:
        raise AssertionError("Should not send order")

    monkeypatch.setattr(engine, "_send_order", _noop_send)
    assert await engine._maybe_stop(last_close=97.0) is False


@pytest.mark.asyncio
async def test_maybe_stop_short_uses_portfolio_avg_price(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify short stop uses portfolio avg_price when entry_price is None.

    Given: Short position with entry_price=None, portfolio avg_price=100,
    When: Price rises above stop threshold,
    Then: Stop triggers using portfolio avg_price as reference.
    """
    risk = _RiskStub(stop_value=0.05)
    engine, _ = _make_engine(risk=risk)
    engine.position_qty = -1.0
    engine.entry_price = None
    engine.portfolio.positions[engine.instrument] = PositionStateModel(
        quantity=-1.0, average_price=100.0
    )
    captured: list[tuple[Any, ...]] = []

    async def _capture_send(*args: Any, **kwargs: Any) -> str:
        captured.append((args, kwargs))
        return "test-stop-avg"

    monkeypatch.setattr(engine, "_send_order", _capture_send)
    assert await engine._maybe_stop(last_close=106.0) is True
    assert captured[0][1]["side"] == "buy"


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
        "command_type": "create",
        "shard_key": "kraken.BTC-USD.live",
        "exchange": "kraken",
        "instrument": "BTC-USD",
        "mode": "live",
        "strategy_id": "engine-buy",
        "side": "buy",
        "order_type": "market",
        "quantity": 0.5,
        "price": None,
        "leverage": None,
        "reduce_only": False,
        "session_id": "s1",
        "sequence_id": 1,
    }
    await coord._outbox_publish(cmd)
    coord.msg_publisher.send.assert_called_once()


@pytest.mark.asyncio
async def test_outbox_publish_cancel_sends_order_cancel_data() -> None:
    """Outbox publish for command_type='cancel' sends OrderCancelData.

    Given: a TraderCoordinator with a mocked msg_publisher and a
        TradeCommandRow whose ``command_type`` is ``cancel``,
    When: ``_outbox_publish`` is invoked,
    Then: the publisher receives an ``OrderCancelData`` frame routed to
        the ``.cancel`` topic, so the executor can forward the cancel to
        the venue adapter.
    """
    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord.msg_publisher = AsyncMock()
    cmd: dict[str, Any] = {
        "public_id": "cmd-cancel",
        "client_order_id": "cid-cancel",
        "command_type": "cancel",
        "shard_key": "kraken.BTC-USD.live",
        "exchange": "kraken",
        "instrument": "BTC-USD",
        "mode": "live",
        "strategy_id": "manual",
        "side": "buy",
        "order_type": "market",
        "quantity": 0.5,
        "price": None,
        "leverage": None,
        "reduce_only": False,
        "session_id": "s1",
        "sequence_id": 3,
        "exchange_order_id": "ex-1",
        "wallet_public_id": "wallet-1",
        "operator_public_id": None,
        "user_public_id": None,
    }
    await coord._outbox_publish(cmd)
    coord.msg_publisher.send.assert_called_once()
    sent_topic = coord.msg_publisher.send.call_args[0][0]
    sent_msg = coord.msg_publisher.send.call_args[0][1]
    assert sent_topic.endswith(".cancel")
    assert isinstance(sent_msg, OrderCancelData)
    assert sent_msg.client_order_id == "cid-cancel"
    assert sent_msg.exchange_order_id == "ex-1"


@pytest.mark.asyncio
async def test_outbox_publish_cancel_rehydrates_missing_exchange_order_id() -> None:
    """Cancel dispatch refreshes exchange_order_id from the repo when empty.

    Given: a TraderCoordinator whose outbox is publishing a cancel
        TradeCommand whose ``exchange_order_id`` is ``None`` (the REST
        cancel route snapshotted the row before the venue ACK assigned
        the id),
    When: ``_outbox_publish`` is invoked,
    Then: the dispatcher re-queries the repository at publish time and
        the resulting ``OrderCancelData`` carries the hydrated venue id
        so the live adapter can cancel by exchange id.
    """
    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord.msg_publisher = AsyncMock()
    mock_repo = MagicMock()
    mock_repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value="ex-late")
    coord.repository = mock_repo
    cmd: dict[str, Any] = {
        "public_id": "cmd-cancel",
        "client_order_id": "cid-cancel",
        "command_type": "cancel",
        "shard_key": "kraken.BTC-USD.live",
        "exchange": "kraken",
        "instrument": "BTC-USD",
        "mode": "live",
        "strategy_id": "manual",
        "side": "buy",
        "order_type": "market",
        "quantity": 0.5,
        "price": None,
        "leverage": None,
        "reduce_only": False,
        "session_id": "s1",
        "sequence_id": 3,
        "exchange_order_id": None,
        "wallet_public_id": "wallet-1",
        "operator_public_id": None,
        "user_public_id": None,
    }
    await coord._outbox_publish(cmd)
    mock_repo.get_exchange_order_id_for_client_order_id.assert_awaited_once()
    sent = coord.msg_publisher.send.call_args[0][1]
    assert sent.exchange_order_id == "ex-late"


@pytest.mark.asyncio
async def test_outbox_publish_cancel_repo_returns_none_keeps_empty() -> None:
    """Cancel dispatch still publishes when the repo has no venue id yet.

    Given: a cancel TradeCommand whose ``exchange_order_id`` is empty
        and whose matching ``orders`` row has not yet been ACKed by
        the venue,
    When: ``_outbox_publish`` is invoked,
    Then: the resulting ``OrderCancelData`` is still published with an
        empty ``exchange_order_id`` so paper executors can fall back
        to cancelling by ``client_order_id``.
    """
    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord.msg_publisher = AsyncMock()
    mock_repo = MagicMock()
    mock_repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value=None)
    coord.repository = mock_repo
    cmd: dict[str, Any] = {
        "public_id": "cmd-cancel",
        "client_order_id": "cid-cancel",
        "command_type": "cancel",
        "shard_key": "paper.BTC-USD.live",
        "exchange": "paper",
        "instrument": "BTC-USD",
        "mode": "live",
        "strategy_id": "manual",
        "side": "buy",
        "order_type": "market",
        "quantity": 0.5,
        "price": None,
        "leverage": None,
        "reduce_only": False,
        "session_id": "s1",
        "sequence_id": 3,
        "exchange_order_id": "",
        "wallet_public_id": "wallet-1",
        "operator_public_id": None,
        "user_public_id": None,
    }
    await coord._outbox_publish(cmd)
    sent = coord.msg_publisher.send.call_args[0][1]
    assert sent.exchange_order_id == ""
    assert sent.client_order_id == "cid-cancel"


@pytest.mark.asyncio
async def test_outbox_publish_propagates_leverage_and_reduce_only() -> None:
    """Outbox publish forwards leverage and reduce_only into OrderRequestData.

    Given: a TraderCoordinator with a mocked msg_publisher and a TradeCommandRow
        containing leverage=3 and reduce_only=True (mimicking the durable
        command path used when use_durable_commands=True),
    When: _outbox_publish is called,
    Then: The published OrderRequestData carries the same leverage/reduce_only,
        so the executor (and downstream OrderData WS event + Order DB row)
        observes the request-time margin metadata instead of defaults.
    """
    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord.msg_publisher = AsyncMock()
    cmd: dict[str, Any] = {
        "public_id": "cmd-lev",
        "client_order_id": "cid-lev",
        "command_type": "create",
        "shard_key": "kraken.BTC-USD.live",
        "exchange": "kraken",
        "instrument": "BTC-USD",
        "mode": "live",
        "strategy_id": "engine-sell",
        "side": "sell",
        "order_type": "limit",
        "quantity": 0.5,
        "price": 50000.0,
        "leverage": 3,
        "reduce_only": True,
        "session_id": "s1",
        "sequence_id": 2,
    }
    await coord._outbox_publish(cmd)
    coord.msg_publisher.send.assert_called_once()
    sent_order = coord.msg_publisher.send.call_args[0][1]
    assert sent_order.leverage == 3
    assert sent_order.reduce_only is True


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
    Then: upsert_checkpoint is called on the repository and the
        forwarded row carries the position_opened_at venue timestamp
        captured by the fill (funding fee model dependency).
    """
    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord.trade_service = TradeService()
    coord._tracker = MagicMock()
    coord._tracker.session_id = "s-test"
    coord._tracker.next_sequence = MagicMock(return_value=1)
    unrelated_engine = MagicMock()
    unrelated_engine._shard_key = "kraken.ETH-USD.live"
    coord.engines = {"ETH-USD@kraken-live": unrelated_engine}
    coord._wallet_short_to_id = {}
    mock_repo = AsyncMock(spec=SQLAlchemyRepository)
    mock_repo.upsert_checkpoint = AsyncMock(return_value=1)
    mock_repo.get_latest_venue_event_id = AsyncMock(return_value=1)
    coord.repository = mock_repo

    venue_ts = datetime(2026, 4, 6, 12, 30, 0, tzinfo=UTC)
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
            "venue_timestamp": venue_ts,
            "received_at": datetime.now(UTC),
        }
    )
    await coord._persist_checkpoint("kraken.BTC-USD.live")
    mock_repo.upsert_checkpoint.assert_called_once()
    call_row = mock_repo.upsert_checkpoint.call_args.args[0]
    assert call_row["shard_key"] == "kraken.BTC-USD.live"
    assert call_row["position_qty"] == 0.5
    assert call_row["position_opened_at"] == venue_ts


@pytest.mark.asyncio
async def test_persist_checkpoint_writes_none_when_position_flat() -> None:
    """When the projection has no open cycle the upsert sends position_opened_at=None.

    Given: a TraderCoordinator persisting a checkpoint for a flat shard,
    When: _persist_checkpoint is called before any fill applied to the
        shard,
    Then: the upsert payload's position_opened_at is None (default
        empty-shard state).
    """
    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord.trade_service = TradeService()
    coord._tracker = MagicMock()
    coord._tracker.session_id = "s-test"
    coord._tracker.next_sequence = MagicMock(return_value=1)
    coord.engines = {}
    coord._wallet_short_to_id = {}
    mock_repo = AsyncMock(spec=SQLAlchemyRepository)
    mock_repo.upsert_checkpoint = AsyncMock(return_value=1)
    mock_repo.get_latest_venue_event_id = AsyncMock(return_value=None)
    coord.repository = mock_repo
    coord.trade_service._get_or_create_shard("kraken.BTC-USD.live")
    await coord._persist_checkpoint("kraken.BTC-USD.live")
    mock_repo.upsert_checkpoint.assert_called_once()
    call_row = mock_repo.upsert_checkpoint.call_args.args[0]
    assert call_row["position_opened_at"] is None


@pytest.mark.asyncio
async def test_persist_checkpoint_carries_operator_from_matching_engine() -> None:
    """``operator_public_id`` persisted from engine.

    Given: A TraderCoordinator with an engine whose ``_shard_key``
        matches the shard being checkpointed and whose
        ``operator_public_id`` is populated,
    When: ``_persist_checkpoint`` is called,
    Then: The upsert row carries the engine's ``operator_public_id``
        so that subsequent ``_recover_from_checkpoints`` can restore
        the engine with the correct operator attribution.
    """
    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord.trade_service = TradeService()
    coord._tracker = MagicMock()
    coord._tracker.session_id = "s-test"
    coord._tracker.next_sequence = MagicMock(return_value=1)
    coord._wallet_short_to_id = {}
    mock_engine = MagicMock()
    mock_engine._shard_key = "kraken.BTC-USD.live"
    mock_engine.operator_public_id = "01975a8b-3c7d-7000-8000-000000000aa1"
    coord.engines = {"BTC-USD@kraken-live": mock_engine}
    mock_repo = AsyncMock(spec=SQLAlchemyRepository)
    mock_repo.upsert_checkpoint = AsyncMock(return_value=1)
    mock_repo.get_latest_venue_event_id = AsyncMock(return_value=None)
    coord.repository = mock_repo
    coord.trade_service._get_or_create_shard("kraken.BTC-USD.live")
    await coord._persist_checkpoint("kraken.BTC-USD.live")
    mock_repo.upsert_checkpoint.assert_called_once()
    call_row = mock_repo.upsert_checkpoint.call_args.args[0]
    assert call_row["operator_public_id"] == "01975a8b-3c7d-7000-8000-000000000aa1"


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
    coord.engines = {}
    coord._wallet_short_to_id = {}
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


def _make_cycle_engine(
    instrument: str = "BTC-USD",
    exchange: str = "kraken",
    wallet_public_id: str = "wallet-1",
    operator_public_id: str = "op-1",
    shard_key: str = "kraken.BTC-USD.live.w0000000000aa",
) -> MagicMock:
    """Minimal engine stub for _sync_position_cycle_on_fill unit tests."""
    engine = MagicMock()
    engine.instrument = instrument
    engine.exchange = exchange
    engine.mode = "live"
    engine.wallet_public_id = wallet_public_id
    engine.operator_public_id = operator_public_id
    engine._shard_key = shard_key
    return engine


def _make_cycle_fill(
    client_order_id: str = "cid-1",
    executed_at: datetime | None = None,
    session_id: str = "s-test",
    sequence_id: int = 1,
) -> ExecutionData:
    """Minimal ExecutionData for _sync_position_cycle_on_fill unit tests."""
    return ExecutionData(
        type="execution",
        public_id="fill-1",
        timestamp=datetime.now(UTC),
        session_id=session_id,
        sequence_id=sequence_id,
        exchange="kraken",
        instrument="BTC-USD",
        side="buy",
        size=1.0,
        price=50000.0,
        last_size=1.0,
        last_price=50000.0,
        fee=0.5,
        fee_asset="USD",
        status="filled",
        client_order_id=client_order_id,
        exchange_order_id="ex-1",
        trade_id="t-1",
        executed_at=executed_at or datetime.now(UTC),
    )


def _make_cycle_coord(
    repository: AsyncMock | None = None,
) -> TraderCoordinator:
    """Minimal TraderCoordinator for _sync_position_cycle_on_fill unit tests."""
    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord.trade_service = TradeService()
    coord.repository = repository or AsyncMock()
    return coord


@pytest.mark.asyncio
async def test_sync_cycle_degraded_identity_skips_write() -> None:
    """Degraded identity fail-closed: empty wallet_public_id triggers full skip.

    Given: an engine whose wallet_public_id is the empty string,
    When: _sync_position_cycle_on_fill runs after a flat->long fill,
    Then: no repository method is called and the cycle is left unwritten.
    """
    repo = AsyncMock()
    coord = _make_cycle_coord(repo)
    engine = _make_cycle_engine(wallet_public_id="")
    await coord._sync_position_cycle_on_fill(engine, 0.0, 1.5, _make_cycle_fill())
    repo.get_open_position_cycle.assert_not_called()
    repo.insert_position_cycle.assert_not_called()
    repo.close_position_cycle.assert_not_called()
    repo.flip_position_cycle.assert_not_called()
    repo.update_position_cycle_max_qty.assert_not_called()


@pytest.mark.asyncio
async def test_sync_cycle_transition_none_is_noop() -> None:
    """Same direction equal-magnitude fills are a no-op (None transition).

    Given: an engine with existing position 1.5,
    When: a fill leaves position_qty unchanged at 1.5,
    Then: _detect_cycle_transition returns None and no repo call is made.
    """
    repo = AsyncMock()
    coord = _make_cycle_coord(repo)
    engine = _make_cycle_engine()
    await coord._sync_position_cycle_on_fill(engine, 1.5, 1.5, _make_cycle_fill())
    repo.get_open_position_cycle.assert_not_called()
    repo.insert_position_cycle.assert_not_called()


@pytest.mark.asyncio
async def test_sync_cycle_open_inserts_and_hydrates_cache() -> None:
    """Open transition inserts a new cycle and hydrates the shard cache.

    Given: a flat shard with no existing open cycle in the DB,
    When: a flat->long fill triggers an open transition,
    Then: insert_position_cycle is called and the shard cache mirrors the new row.
    """
    repo = AsyncMock()
    repo.get_open_position_cycle = AsyncMock(return_value=None)
    repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-btc")
    repo.insert_position_cycle = AsyncMock(return_value=(1, "cycle-new"))
    coord = _make_cycle_coord(repo)
    engine = _make_cycle_engine()
    fill = _make_cycle_fill()
    await coord._sync_position_cycle_on_fill(engine, 0.0, 1.5, fill)
    repo.insert_position_cycle.assert_called_once()
    inserted = repo.insert_position_cycle.call_args.args[0]
    assert inserted["direction"] == "long"
    assert inserted["max_qty"] == pytest.approx(1.5)
    assert inserted["status"] == "open"
    assert inserted["instrument_public_id"] == "inst-btc"
    assert inserted["wallet_public_id"] == "wallet-1"
    assert inserted["operator_public_id"] == "op-1"
    shard = coord.trade_service._get_or_create_shard(engine._shard_key)
    assert shard.active_cycle_public_id == "cycle-new"
    assert shard.active_cycle_max_qty == pytest.approx(1.5)


@pytest.mark.asyncio
async def test_sync_cycle_open_short_direction_and_operator_none() -> None:
    """Open transition for a short flips direction and coerces empty operator to None.

    Given: an engine with empty operator_public_id and no existing cycle,
    When: a flat->short fill triggers an open transition,
    Then: the inserted row carries direction='short' and operator_public_id=None.
    """
    repo = AsyncMock()
    repo.get_open_position_cycle = AsyncMock(return_value=None)
    repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-btc")
    repo.insert_position_cycle = AsyncMock(return_value=(1, "cycle-new"))
    coord = _make_cycle_coord(repo)
    engine = _make_cycle_engine(operator_public_id="")
    await coord._sync_position_cycle_on_fill(engine, 0.0, -2.0, _make_cycle_fill())
    inserted = repo.insert_position_cycle.call_args.args[0]
    assert inserted["direction"] == "short"
    assert inserted["max_qty"] == pytest.approx(2.0)
    assert inserted["operator_public_id"] is None


@pytest.mark.asyncio
async def test_sync_cycle_open_existing_cycle_hydrates_cache_only() -> None:
    """Idempotency on open: existing DB row hydrates the cache instead of double-inserting.

    Given: an open cycle already exists in the DB for the shard,
    When: a flat->long fill triggers an open transition,
    Then: insert_position_cycle is skipped and the shard cache is hydrated from the existing row.
    """
    repo = AsyncMock()
    existing_row = {"public_id": "cycle-existing", "max_qty": 3.0}
    repo.get_open_position_cycle = AsyncMock(return_value=existing_row)
    coord = _make_cycle_coord(repo)
    engine = _make_cycle_engine()
    await coord._sync_position_cycle_on_fill(engine, 0.0, 1.5, _make_cycle_fill())
    repo.insert_position_cycle.assert_not_called()
    shard = coord.trade_service._get_or_create_shard(engine._shard_key)
    assert shard.active_cycle_public_id == "cycle-existing"
    assert shard.active_cycle_max_qty == pytest.approx(3.0)


@pytest.mark.asyncio
async def test_sync_cycle_open_unresolved_instrument_skips() -> None:
    """Open transition skips when the instrument symbol cannot be resolved.

    Given: get_instrument_public_id_by_symbol returns None for the engine's symbol,
    When: a flat->long fill triggers an open transition,
    Then: insert_position_cycle is skipped and the shard cache stays empty.
    """
    repo = AsyncMock()
    repo.get_open_position_cycle = AsyncMock(return_value=None)
    repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=None)
    coord = _make_cycle_coord(repo)
    engine = _make_cycle_engine()
    await coord._sync_position_cycle_on_fill(engine, 0.0, 1.5, _make_cycle_fill())
    repo.insert_position_cycle.assert_not_called()
    shard = coord.trade_service._get_or_create_shard(engine._shard_key)
    assert shard.active_cycle_public_id is None


@pytest.mark.asyncio
async def test_sync_cycle_close_happy_path_clears_cache() -> None:
    """Close transition with cache hit closes the cycle and clears the shard cache.

    Given: a shard with an active cycle cached in ShardState,
    When: a long->flat fill triggers a close transition,
    Then: close_position_cycle is called and both cache fields reset.
    """
    repo = AsyncMock()
    repo.close_position_cycle = AsyncMock(return_value=2)
    coord = _make_cycle_coord(repo)
    engine = _make_cycle_engine()
    shard = coord.trade_service._get_or_create_shard(engine._shard_key)
    shard.active_cycle_public_id = "cycle-abc"
    shard.active_cycle_max_qty = 2.5
    await coord._sync_position_cycle_on_fill(engine, 1.5, 0.0, _make_cycle_fill())
    repo.close_position_cycle.assert_called_once()
    kwargs = repo.close_position_cycle.call_args.kwargs
    assert kwargs["cycle_public_id"] == "cycle-abc"
    assert shard.active_cycle_public_id is None
    assert shard.active_cycle_max_qty == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_sync_cycle_close_cache_miss_db_fallback_succeeds() -> None:
    """Close transition recovers via DB fallback when the shard cache is empty.

    Given: an empty shard cache and an open cycle row in the DB,
    When: a long->flat fill triggers a close transition,
    Then: get_open_position_cycle returns the row and close_position_cycle uses its public_id.
    """
    repo = AsyncMock()
    repo.get_open_position_cycle = AsyncMock(return_value={"public_id": "cycle-db", "max_qty": 2.0})
    repo.close_position_cycle = AsyncMock(return_value=2)
    coord = _make_cycle_coord(repo)
    engine = _make_cycle_engine()
    await coord._sync_position_cycle_on_fill(engine, 1.5, 0.0, _make_cycle_fill())
    repo.close_position_cycle.assert_called_once()
    assert repo.close_position_cycle.call_args.kwargs["cycle_public_id"] == "cycle-db"


@pytest.mark.asyncio
async def test_sync_cycle_close_cache_miss_db_miss_skips() -> None:
    """Close transition fail-soft when neither cache nor DB knows of any open cycle.

    Given: empty shard cache and no open cycle in the DB for this shard,
    When: a long->flat fill triggers a close transition,
    Then: close_position_cycle is not called and the helper logs a warning.
    """
    repo = AsyncMock()
    repo.get_open_position_cycle = AsyncMock(return_value=None)
    coord = _make_cycle_coord(repo)
    engine = _make_cycle_engine()
    await coord._sync_position_cycle_on_fill(engine, 1.5, 0.0, _make_cycle_fill())
    repo.close_position_cycle.assert_not_called()


@pytest.mark.asyncio
async def test_sync_cycle_flip_happy_path_hydrates_new_cache() -> None:
    """Flip transition with cache hit calls flip_position_cycle and re-hydrates the cache.

    Given: a shard with an active long cycle cached and a resolvable instrument,
    When: a long->short fill triggers a flip transition,
    Then: flip_position_cycle runs and shard cache carries the new short cycle's public_id.
    """
    repo = AsyncMock()
    repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-btc")
    repo.flip_position_cycle = AsyncMock(return_value=(2, "cycle-new"))
    coord = _make_cycle_coord(repo)
    engine = _make_cycle_engine()
    shard = coord.trade_service._get_or_create_shard(engine._shard_key)
    shard.active_cycle_public_id = "cycle-long"
    shard.active_cycle_max_qty = 1.5
    await coord._sync_position_cycle_on_fill(engine, 1.5, -2.0, _make_cycle_fill())
    repo.flip_position_cycle.assert_called_once()
    kwargs = repo.flip_position_cycle.call_args.kwargs
    assert kwargs["close_cycle_public_id"] == "cycle-long"
    assert kwargs["new_open_row"]["direction"] == "short"
    assert kwargs["new_open_row"]["max_qty"] == pytest.approx(2.0)
    assert shard.active_cycle_public_id == "cycle-new"
    assert shard.active_cycle_max_qty == pytest.approx(2.0)


@pytest.mark.asyncio
async def test_sync_cycle_flip_cache_miss_db_fallback_succeeds() -> None:
    """Flip transition with cache miss recovers cycle_id from DB before calling flip.

    Given: empty shard cache and an open cycle row in the DB,
    When: a long->short fill triggers a flip transition,
    Then: the DB fallback cycle_id is passed to flip_position_cycle.
    """
    repo = AsyncMock()
    repo.get_open_position_cycle = AsyncMock(return_value={"public_id": "cycle-db", "max_qty": 1.0})
    repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-btc")
    repo.flip_position_cycle = AsyncMock(return_value=(2, "cycle-new"))
    coord = _make_cycle_coord(repo)
    engine = _make_cycle_engine()
    await coord._sync_position_cycle_on_fill(engine, 1.5, -2.0, _make_cycle_fill())
    repo.flip_position_cycle.assert_called_once()
    assert repo.flip_position_cycle.call_args.kwargs["close_cycle_public_id"] == "cycle-db"


@pytest.mark.asyncio
async def test_sync_cycle_flip_cache_miss_db_miss_skips() -> None:
    """Flip transition fail-soft when neither cache nor DB has any cycle to close.

    Given: empty shard cache and no open cycle in the DB,
    When: a long->short fill triggers a flip transition,
    Then: flip_position_cycle is not called and the helper logs a warning.
    """
    repo = AsyncMock()
    repo.get_open_position_cycle = AsyncMock(return_value=None)
    coord = _make_cycle_coord(repo)
    engine = _make_cycle_engine()
    await coord._sync_position_cycle_on_fill(engine, 1.5, -2.0, _make_cycle_fill())
    repo.flip_position_cycle.assert_not_called()


@pytest.mark.asyncio
async def test_sync_cycle_flip_unresolved_instrument_degrades_to_close_only() -> None:
    """Flip with unresolved instrument degrades to close-only and clears the cache.

    The position has objectively flipped in TradeService, so leaving the
    old cycle live in the cache/DB would cause subsequent short-leg
    scale_up/close calls to corrupt the wrong row. Instead we degrade
    to close-only: shut the long cycle cleanly, leave the short leg
    uncovered (no cycle row), and clear the cache.

    Given: a flip transition with cached cycle_id but inst_pid lookup returns None,
    When: _sync_position_cycle_on_fill runs,
    Then: flip_position_cycle is skipped, close_position_cycle closes the old row,
        and the shard cache is cleared so subsequent fail-soft paths handle the
        uncovered short leg.
    """
    repo = AsyncMock()
    repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=None)
    repo.close_position_cycle = AsyncMock(return_value=2)
    coord = _make_cycle_coord(repo)
    engine = _make_cycle_engine()
    shard = coord.trade_service._get_or_create_shard(engine._shard_key)
    shard.active_cycle_public_id = "cycle-long"
    shard.active_cycle_max_qty = 1.5
    await coord._sync_position_cycle_on_fill(engine, 1.5, -2.0, _make_cycle_fill())
    repo.flip_position_cycle.assert_not_called()
    repo.close_position_cycle.assert_called_once()
    kwargs = repo.close_position_cycle.call_args.kwargs
    assert kwargs["cycle_public_id"] == "cycle-long"
    assert shard.active_cycle_public_id is None
    assert shard.active_cycle_max_qty == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_sync_cycle_flip_db_fallback_unresolved_instrument_closes_fallback_cycle() -> None:
    """Flip degrade-to-close-only also fires when cycle_id comes from DB fallback.

    Covers the same degraded-close-only path but originating from a cache
    miss (e.g. after degraded startup reconciliation). The DB-fallback
    cycle_id is used as the close target. Without this fix, a
    subsequent short-leg fill would DB-fallback to the same long cycle
    and corrupt it.

    Given: empty shard cache + DB has open long cycle + unresolved instrument,
    When: a long->short fill triggers a flip transition,
    Then: close_position_cycle uses the DB-fallback cycle_id and the shard cache stays empty.
    """
    repo = AsyncMock()
    repo.get_open_position_cycle = AsyncMock(
        return_value={"public_id": "cycle-db-long", "max_qty": 2.0}
    )
    repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=None)
    repo.close_position_cycle = AsyncMock(return_value=2)
    coord = _make_cycle_coord(repo)
    engine = _make_cycle_engine()
    await coord._sync_position_cycle_on_fill(engine, 1.5, -2.0, _make_cycle_fill())
    repo.flip_position_cycle.assert_not_called()
    repo.close_position_cycle.assert_called_once()
    assert repo.close_position_cycle.call_args.kwargs["cycle_public_id"] == "cycle-db-long"
    shard = coord.trade_service._get_or_create_shard(engine._shard_key)
    assert shard.active_cycle_public_id is None
    assert shard.active_cycle_max_qty == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_sync_cycle_scale_up_updates_max() -> None:
    """Scale_up transition bumps max_qty in DB and cache when the new size is a new peak.

    Given: a shard with cached cycle and active_cycle_max_qty=1.5,
    When: a long fill scales position from 1.5 to 3.0,
    Then: update_position_cycle_max_qty is called with new_max_qty=3.0 and the cache reflects it.
    """
    repo = AsyncMock()
    repo.update_position_cycle_max_qty = AsyncMock(return_value=3)
    coord = _make_cycle_coord(repo)
    engine = _make_cycle_engine()
    shard = coord.trade_service._get_or_create_shard(engine._shard_key)
    shard.active_cycle_public_id = "cycle-long"
    shard.active_cycle_max_qty = 1.5
    await coord._sync_position_cycle_on_fill(engine, 1.5, 3.0, _make_cycle_fill())
    repo.update_position_cycle_max_qty.assert_called_once()
    kwargs = repo.update_position_cycle_max_qty.call_args.kwargs
    assert kwargs["cycle_public_id"] == "cycle-long"
    assert kwargs["new_max_qty"] == pytest.approx(3.0)
    assert shard.active_cycle_max_qty == pytest.approx(3.0)


@pytest.mark.asyncio
async def test_sync_cycle_scale_up_below_peak_is_noop() -> None:
    """Scale_up transition is a no-op when the new abs is below the cached peak.

    Given: a shard with cached active_cycle_max_qty=5.0,
    When: a long fill scales position from 1.5 to 3.0 (still below cached peak),
    Then: update_position_cycle_max_qty is not called and cache peak stays at 5.0.
    """
    repo = AsyncMock()
    coord = _make_cycle_coord(repo)
    engine = _make_cycle_engine()
    shard = coord.trade_service._get_or_create_shard(engine._shard_key)
    shard.active_cycle_public_id = "cycle-long"
    shard.active_cycle_max_qty = 5.0
    await coord._sync_position_cycle_on_fill(engine, 1.5, 3.0, _make_cycle_fill())
    repo.update_position_cycle_max_qty.assert_not_called()
    assert shard.active_cycle_max_qty == pytest.approx(5.0)


@pytest.mark.asyncio
async def test_sync_cycle_scale_up_cache_miss_db_fallback_hydrates_and_may_update() -> None:
    """Scale_up with cache miss recovers cycle from DB then bumps if recovered peak is stale.

    Given: empty shard cache and DB has open cycle with max_qty=1.0,
    When: a long fill scales position from 1.5 to 3.0,
    Then: cache is hydrated from DB row and update_position_cycle_max_qty bumps peak to 3.0.
    """
    repo = AsyncMock()
    repo.get_open_position_cycle = AsyncMock(return_value={"public_id": "cycle-db", "max_qty": 1.0})
    repo.update_position_cycle_max_qty = AsyncMock(return_value=3)
    coord = _make_cycle_coord(repo)
    engine = _make_cycle_engine()
    await coord._sync_position_cycle_on_fill(engine, 1.5, 3.0, _make_cycle_fill())
    repo.update_position_cycle_max_qty.assert_called_once()
    assert repo.update_position_cycle_max_qty.call_args.kwargs["cycle_public_id"] == "cycle-db"
    shard = coord.trade_service._get_or_create_shard(engine._shard_key)
    assert shard.active_cycle_public_id == "cycle-db"
    assert shard.active_cycle_max_qty == pytest.approx(3.0)


@pytest.mark.asyncio
async def test_sync_cycle_scale_up_cache_miss_db_miss_skips() -> None:
    """Scale_up fail-soft when neither cache nor DB has a cycle to bump.

    Given: empty shard cache and no open cycle in DB,
    When: a long fill scales position from 1.5 to 3.0,
    Then: update_position_cycle_max_qty is not called and the helper logs a warning.
    """
    repo = AsyncMock()
    repo.get_open_position_cycle = AsyncMock(return_value=None)
    coord = _make_cycle_coord(repo)
    engine = _make_cycle_engine()
    await coord._sync_position_cycle_on_fill(engine, 1.5, 3.0, _make_cycle_fill())
    repo.update_position_cycle_max_qty.assert_not_called()


def _make_reconcile_coord(
    repository: AsyncMock | None = None,
) -> TraderCoordinator:
    """Minimal TraderCoordinator for _reconcile_position_cycles unit tests.

    Initializes self.engines, self.trade_service, self.repository, and
    self._tracker so the reconciliation helper can run in isolation
    without going through ``_recover_engine_state``.
    """
    coord = TraderCoordinator.__new__(TraderCoordinator)
    coord.trade_service = TradeService()
    coord.repository = repository or AsyncMock()
    coord.engines = {}
    coord._tracker = SequenceTracker()
    return coord


@pytest.mark.asyncio
async def test_reconcile_no_engines_is_noop() -> None:
    """Reconciliation is a no-op when there are no engines to walk.

    Given: a TraderCoordinator with an empty engines dict,
    When: _reconcile_position_cycles runs,
    Then: no repository methods are called.
    """
    repo = AsyncMock()
    coord = _make_reconcile_coord(repo)
    await coord._reconcile_position_cycles()
    repo.get_open_position_cycle.assert_not_called()
    repo.insert_position_cycle.assert_not_called()


@pytest.mark.asyncio
async def test_reconcile_flat_engine_without_stale_row_is_noop() -> None:
    """Reconciliation skips a flat shard with no stale cycle row in the DB.

    Given: an engine with position_qty=0 and no open cycle row for the shard,
    When: _reconcile_position_cycles runs,
    Then: get_open_position_cycle is queried but no close or insert is issued.
    """
    repo = AsyncMock()
    repo.get_open_position_cycle = AsyncMock(return_value=None)
    coord = _make_reconcile_coord(repo)
    engine = _make_cycle_engine()
    engine.position_qty = 0.0
    coord.engines["BTC-USD@kraken-live"] = engine
    await coord._reconcile_position_cycles()
    repo.get_open_position_cycle.assert_called_once()
    repo.close_position_cycle.assert_not_called()
    repo.insert_position_cycle.assert_not_called()


@pytest.mark.asyncio
async def test_reconcile_flat_engine_with_stale_row_closes_cycle() -> None:
    """Reconciliation closes a stale open cycle when the recovered position is flat.

    Coordinator was down when the position closed; the open cycle row
    never got its SCD2 close. Reconcile must close it so the next
    live fill does not reuse a stale public_id via DB fallback.

    Given: engine recovered flat + a stale open long cycle still in DB,
    When: _reconcile_position_cycles runs,
    Then: close_position_cycle closes the stale cycle and the shard cache stays empty.
    """
    repo = AsyncMock()
    repo.get_open_position_cycle = AsyncMock(
        return_value={"public_id": "cycle-stale", "direction": "long", "max_qty": 2.0}
    )
    repo.close_position_cycle = AsyncMock(return_value=3)
    coord = _make_reconcile_coord(repo)
    engine = _make_cycle_engine()
    engine.position_qty = 0.0
    coord.engines["BTC-USD@kraken-live"] = engine
    await coord._reconcile_position_cycles()
    repo.close_position_cycle.assert_called_once()
    assert repo.close_position_cycle.call_args.kwargs["cycle_public_id"] == "cycle-stale"
    shard = coord.trade_service._get_or_create_shard(engine._shard_key)
    assert shard.active_cycle_public_id is None
    assert shard.active_cycle_max_qty == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_reconcile_degraded_identity_engine_is_skipped() -> None:
    """Reconciliation skips engines with degraded wallet attribution fail-closed.

    Given: an engine whose wallet_public_id is empty (degraded recovery state),
    When: _reconcile_position_cycles runs,
    Then: no repository methods are called for that engine.
    """
    repo = AsyncMock()
    coord = _make_reconcile_coord(repo)
    engine = _make_cycle_engine(wallet_public_id="")
    engine.position_qty = 1.5
    coord.engines["BTC-USD@kraken-live"] = engine
    await coord._reconcile_position_cycles()
    repo.get_open_position_cycle.assert_not_called()
    repo.insert_position_cycle.assert_not_called()


@pytest.mark.asyncio
async def test_reconcile_existing_cycle_matching_direction_hydrates_cache() -> None:
    """Reconciliation hydrates cache from a matching-direction DB cycle without writing.

    Given: a non-flat long engine and a matching-direction long cycle in DB whose peak >= current,
    When: _reconcile_position_cycles runs,
    Then: shard cache is hydrated and no insert/update/flip is issued.
    """
    repo = AsyncMock()
    repo.get_open_position_cycle = AsyncMock(
        return_value={"public_id": "cycle-existing", "direction": "long", "max_qty": 2.5}
    )
    coord = _make_reconcile_coord(repo)
    engine = _make_cycle_engine()
    engine.position_qty = 1.5
    coord.engines["BTC-USD@kraken-live"] = engine
    await coord._reconcile_position_cycles()
    repo.insert_position_cycle.assert_not_called()
    repo.update_position_cycle_max_qty.assert_not_called()
    repo.flip_position_cycle.assert_not_called()
    shard = coord.trade_service._get_or_create_shard(engine._shard_key)
    assert shard.active_cycle_public_id == "cycle-existing"
    assert shard.active_cycle_max_qty == pytest.approx(2.5)


@pytest.mark.asyncio
async def test_reconcile_existing_cycle_understated_max_qty_bumps_peak() -> None:
    """Reconciliation bumps DB max_qty when downtime scaled the position beyond last peak.

    The position scaled up during downtime beyond the last checkpointed
    peak. Reconcile must bring the DB peak up to the recovered size
    so the next live fill's scale_up guard is monotonic.

    Given: non-flat long with abs=3.0 and DB cycle max_qty=1.5 (understated),
    When: _reconcile_position_cycles runs,
    Then: update_position_cycle_max_qty is called with new_max_qty=3.0 and cache reflects it.
    """
    repo = AsyncMock()
    repo.get_open_position_cycle = AsyncMock(
        return_value={"public_id": "cycle-old", "direction": "long", "max_qty": 1.5}
    )
    repo.update_position_cycle_max_qty = AsyncMock(return_value=7)
    coord = _make_reconcile_coord(repo)
    engine = _make_cycle_engine()
    engine.position_qty = 3.0
    coord.engines["BTC-USD@kraken-live"] = engine
    await coord._reconcile_position_cycles()
    repo.update_position_cycle_max_qty.assert_called_once()
    kwargs = repo.update_position_cycle_max_qty.call_args.kwargs
    assert kwargs["cycle_public_id"] == "cycle-old"
    assert kwargs["new_max_qty"] == pytest.approx(3.0)
    shard = coord.trade_service._get_or_create_shard(engine._shard_key)
    assert shard.active_cycle_public_id == "cycle-old"
    assert shard.active_cycle_max_qty == pytest.approx(3.0)


@pytest.mark.asyncio
async def test_reconcile_direction_mismatch_flips_cycle() -> None:
    """Reconciliation atomically flips a stale opposite-direction cycle on restart.

    The position flipped during downtime. Reconcile must close the
    stale long cycle and open a new short cycle in one transaction
    via flip_position_cycle.

    Given: a recovered short engine + a stale long open cycle in DB + resolvable instrument,
    When: _reconcile_position_cycles runs,
    Then: flip_position_cycle is called and the cache mirrors the new short cycle.
    """
    repo = AsyncMock()
    repo.get_open_position_cycle = AsyncMock(
        return_value={"public_id": "cycle-long", "direction": "long", "max_qty": 2.0}
    )
    repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-btc")
    repo.flip_position_cycle = AsyncMock(return_value=(8, "cycle-new-short"))
    coord = _make_reconcile_coord(repo)
    engine = _make_cycle_engine()
    engine.position_qty = -1.5
    coord.engines["BTC-USD@kraken-live"] = engine
    await coord._reconcile_position_cycles()
    repo.flip_position_cycle.assert_called_once()
    kwargs = repo.flip_position_cycle.call_args.kwargs
    assert kwargs["close_cycle_public_id"] == "cycle-long"
    new_row = kwargs["new_open_row"]
    assert new_row["direction"] == "short"
    assert new_row["max_qty"] == pytest.approx(1.5)
    shard = coord.trade_service._get_or_create_shard(engine._shard_key)
    assert shard.active_cycle_public_id == "cycle-new-short"
    assert shard.active_cycle_max_qty == pytest.approx(1.5)


@pytest.mark.asyncio
async def test_reconcile_direction_mismatch_unresolved_instrument_degrades_to_close_only() -> None:
    """Reconciliation degrades to close-only when flip needs instrument that cannot resolve.

    Mirrors the fill-path degrade-to-close-only behavior: we cannot
    open a new cycle without an instrument_public_id, but the old
    one is objectively dead, so close it so subsequent fails-soft
    paths handle the uncovered leg.

    Given: a recovered short engine + stale long DB cycle + instrument lookup returns None,
    When: _reconcile_position_cycles runs,
    Then: flip_position_cycle is skipped, close_position_cycle closes the stale cycle, cache cleared.
    """
    repo = AsyncMock()
    repo.get_open_position_cycle = AsyncMock(
        return_value={"public_id": "cycle-long", "direction": "long", "max_qty": 2.0}
    )
    repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=None)
    repo.close_position_cycle = AsyncMock(return_value=8)
    coord = _make_reconcile_coord(repo)
    engine = _make_cycle_engine()
    engine.position_qty = -1.5
    coord.engines["BTC-USD@kraken-live"] = engine
    await coord._reconcile_position_cycles()
    repo.flip_position_cycle.assert_not_called()
    repo.close_position_cycle.assert_called_once()
    assert repo.close_position_cycle.call_args.kwargs["cycle_public_id"] == "cycle-long"
    shard = coord.trade_service._get_or_create_shard(engine._shard_key)
    assert shard.active_cycle_public_id is None
    assert shard.active_cycle_max_qty == pytest.approx(0.0)


@pytest.mark.asyncio
async def test_reconcile_non_flat_missing_cycle_bootstraps() -> None:
    """Reconciliation bootstraps a synthetic cycle for a non-flat shard with no DB row.

    Given: a non-flat long engine and no open cycle in DB,
    When: _reconcile_position_cycles runs,
    Then: insert_position_cycle creates a new row matching the recovered direction/qty
        and the shard cache is hydrated from the new public_id.
    """
    repo = AsyncMock()
    repo.get_open_position_cycle = AsyncMock(return_value=None)
    repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-btc")
    repo.insert_position_cycle = AsyncMock(return_value=(5, "cycle-bootstrap"))
    coord = _make_reconcile_coord(repo)
    engine = _make_cycle_engine()
    engine.position_qty = 1.5
    coord.engines["BTC-USD@kraken-live"] = engine
    await coord._reconcile_position_cycles()
    repo.insert_position_cycle.assert_called_once()
    inserted = repo.insert_position_cycle.call_args.args[0]
    assert inserted["direction"] == "long"
    assert inserted["max_qty"] == pytest.approx(1.5)
    assert inserted["status"] == "open"
    assert inserted["instrument_public_id"] == "inst-btc"
    assert inserted["wallet_public_id"] == "wallet-1"
    assert inserted["operator_public_id"] == "op-1"
    shard = coord.trade_service._get_or_create_shard(engine._shard_key)
    assert shard.active_cycle_public_id == "cycle-bootstrap"
    assert shard.active_cycle_max_qty == pytest.approx(1.5)


@pytest.mark.asyncio
async def test_reconcile_short_position_bootstraps_short_direction() -> None:
    """Reconciliation bootstrap derives direction='short' from a negative recovered position.

    Given: a recovered engine with position_qty=-2.0 and no DB row,
    When: _reconcile_position_cycles bootstraps the cycle,
    Then: the inserted row carries direction='short' and max_qty=2.0.
    """
    repo = AsyncMock()
    repo.get_open_position_cycle = AsyncMock(return_value=None)
    repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-btc")
    repo.insert_position_cycle = AsyncMock(return_value=(5, "cycle-short"))
    coord = _make_reconcile_coord(repo)
    engine = _make_cycle_engine()
    engine.position_qty = -2.0
    coord.engines["BTC-USD@kraken-live"] = engine
    await coord._reconcile_position_cycles()
    inserted = repo.insert_position_cycle.call_args.args[0]
    assert inserted["direction"] == "short"
    assert inserted["max_qty"] == pytest.approx(2.0)


@pytest.mark.asyncio
async def test_reconcile_bootstrap_uses_checkpoint_opened_at_if_available() -> None:
    """Reconciliation bootstrap uses checkpoint-restored position_opened_at when available.

    Given: a non-flat engine and TradeService shard with position_opened_at from a checkpoint,
    When: _reconcile_position_cycles bootstraps a synthetic cycle,
    Then: the inserted row's opened_at carries the checkpoint timestamp.
    """
    repo = AsyncMock()
    repo.get_open_position_cycle = AsyncMock(return_value=None)
    repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-btc")
    repo.insert_position_cycle = AsyncMock(return_value=(5, "cycle-new"))
    coord = _make_reconcile_coord(repo)
    engine = _make_cycle_engine()
    engine.position_qty = 1.5
    coord.engines["BTC-USD@kraken-live"] = engine
    checkpoint_ts = datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC)
    shard = coord.trade_service._get_or_create_shard(engine._shard_key)
    shard.position.position_opened_at = checkpoint_ts
    await coord._reconcile_position_cycles()
    inserted = repo.insert_position_cycle.call_args.args[0]
    assert inserted["opened_at"] == checkpoint_ts


@pytest.mark.asyncio
async def test_reconcile_bootstrap_fallback_opened_at_when_no_checkpoint() -> None:
    """Reconciliation bootstrap falls back to now() when no checkpoint timestamp is available.

    Given: a full-replay shard with TradeService position_opened_at=None,
    When: _reconcile_position_cycles bootstraps a synthetic cycle,
    Then: the inserted row's opened_at lies between the test's before/after timestamps.
    """
    repo = AsyncMock()
    repo.get_open_position_cycle = AsyncMock(return_value=None)
    repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-btc")
    repo.insert_position_cycle = AsyncMock(return_value=(5, "cycle-new"))
    coord = _make_reconcile_coord(repo)
    engine = _make_cycle_engine()
    engine.position_qty = 1.5
    coord.engines["BTC-USD@kraken-live"] = engine
    before = datetime.now(UTC)
    await coord._reconcile_position_cycles()
    after = datetime.now(UTC)
    inserted = repo.insert_position_cycle.call_args.args[0]
    assert before <= inserted["opened_at"] <= after


@pytest.mark.asyncio
async def test_reconcile_unresolved_instrument_skips_bootstrap() -> None:
    """Reconciliation bootstrap is skipped when the instrument cannot be resolved.

    Given: a non-flat engine, no DB cycle, and instrument lookup returning None,
    When: _reconcile_position_cycles runs,
    Then: insert_position_cycle is not called and the shard cache stays empty.
    """
    repo = AsyncMock()
    repo.get_open_position_cycle = AsyncMock(return_value=None)
    repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=None)
    coord = _make_reconcile_coord(repo)
    engine = _make_cycle_engine()
    engine.position_qty = 1.5
    coord.engines["BTC-USD@kraken-live"] = engine
    await coord._reconcile_position_cycles()
    repo.insert_position_cycle.assert_not_called()
    shard = coord.trade_service._get_or_create_shard(engine._shard_key)
    assert shard.active_cycle_public_id is None


@pytest.mark.asyncio
async def test_reconcile_mixed_engines_processes_each_independently() -> None:
    """Reconciliation processes each engine in a mixed batch by its own case independently.

    Verifies that flat / degraded / existing-cycle / bootstrap-needed
    engines walk through their respective branches without interfering
    with each other. Degraded identity is skipped before any DB query;
    the other three non-skipped engines all query DB, and only the
    bootstrap one hits insert_position_cycle.

    Given: four engines (flat, degraded-identity, existing-long-cycle, needs-bootstrap),
    When: _reconcile_position_cycles iterates the engines dict in insertion order,
    Then: insert_position_cycle is called exactly once for the bootstrap engine,
        the existing engine's cache is hydrated to the DB row, and degraded/flat are skipped.
    """
    repo = AsyncMock()
    repo.get_open_position_cycle = AsyncMock(
        side_effect=[
            None,
            {"public_id": "cycle-existing", "direction": "long", "max_qty": 2.0},
            None,
        ]
    )
    repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-btc")
    repo.insert_position_cycle = AsyncMock(return_value=(5, "cycle-new"))
    coord = _make_reconcile_coord(repo)
    engine_flat = _make_cycle_engine(shard_key="kraken.FLAT-USD.live.waa")
    engine_flat.position_qty = 0.0
    coord.engines["flat"] = engine_flat
    engine_degraded = _make_cycle_engine(
        wallet_public_id="", shard_key="kraken.DEGRADED-USD.live.wbb"
    )
    engine_degraded.position_qty = 1.0
    coord.engines["degraded"] = engine_degraded
    engine_existing = _make_cycle_engine(shard_key="kraken.BTC-USD.live.wcc")
    engine_existing.position_qty = 1.5
    coord.engines["existing"] = engine_existing
    engine_bootstrap = _make_cycle_engine(shard_key="kraken.ETH-USD.live.wdd")
    engine_bootstrap.position_qty = 3.0
    coord.engines["bootstrap"] = engine_bootstrap
    await coord._reconcile_position_cycles()
    assert repo.insert_position_cycle.call_count == 1
    inserted = repo.insert_position_cycle.call_args.args[0]
    assert inserted["shard_key"] == "kraken.ETH-USD.live.wdd"
    shard_existing = coord.trade_service._get_or_create_shard("kraken.BTC-USD.live.wcc")
    assert shard_existing.active_cycle_public_id == "cycle-existing"
    shard_bootstrap = coord.trade_service._get_or_create_shard("kraken.ETH-USD.live.wdd")
    assert shard_bootstrap.active_cycle_public_id == "cycle-new"
