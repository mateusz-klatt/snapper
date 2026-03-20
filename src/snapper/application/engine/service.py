"""Trading engine service module.

This module provides the core trading execution logic through TradingEngineService.
Each engine instance manages a single instrument, handling position entry/exit,
stop-loss logic, fee calculation, and order publication to ZMQ.
"""

import datetime as dt
import uuid

import zmq
from loguru import logger

from snapper.application.engine.config import EngineConfigModel
from snapper.application.portfolio.models import PortfolioTracker
from snapper.application.risk.models import RiskConfigModel
from snapper.application.risk.models import RiskEvaluator
from snapper.core.types import OrderExchange
from snapper.interface.websocket.schemas import ExecutionMode
from snapper.interface.websocket.schemas import TradeSide
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.schemas.data import OrderRequestData


class TradingEngineService:
    """Trading engine for executing trades on a single instrument.

    Manages the complete trading lifecycle for one symbol including:
    - Position tracking (entry price, quantity)
    - Risk-based position sizing
    - Stop-loss execution
    - Order publishing to ZMQ execution topic
    - Mark-to-market equity calculation

    Each TraderCoordinator creates one TradingEngineService per active symbol.
    The engine consumes strategy signals and translates them into concrete
    order requests.

    Attributes:
        instrument: Symbol being traded (e.g., "BTC-USD").
        execution_socket: ZMQ publisher for sending order requests.
        exchange: Target exchange for order execution.
        cfg: Engine configuration (initial cash, fees).
        portfolio: Tracks cash and positions.
        risk: Risk evaluator for position sizing.
        position_qty: Current position size.
        peak_equity: Highest equity reached (for drawdown calculation).
        entry_price: Price at which current position was opened.
        instrument_specs: Lot size and tick size specifications.
    """

    instrument: str
    execution_socket: MessagePublisher
    exchange: OrderExchange
    cfg: EngineConfigModel
    portfolio: PortfolioTracker
    risk: RiskEvaluator
    position_qty: float
    peak_equity: float
    entry_price: float | None
    instrument_specs: dict[str, dict[str, float]]

    def __init__(
        self,
        instrument: str,
        execution_socket: MessagePublisher,
        risk: RiskEvaluator | None = None,
        cfg: EngineConfigModel | None = None,
        *,
        instrument_specs: dict[str, dict[str, float]] | None = None,
        exchange: OrderExchange = "paper",
    ) -> None:
        """Initialize trading engine for a specific instrument.

        Args:
            instrument: Symbol to trade (e.g., "BTC-USD").
            execution_socket: Message publisher for order requests.
            risk: Risk evaluator instance. Defaults to standard RiskEvaluator.
            cfg: Engine configuration. Defaults to EngineConfigModel defaults.
            instrument_specs: Dict mapping symbol to lot_size/tick_size specs.
            exchange: Target exchange. Defaults to "paper" for simulation.
        """
        self.instrument = instrument
        self.execution_socket = execution_socket
        self.exchange = exchange
        self.cfg = cfg or EngineConfigModel()
        self.portfolio = PortfolioTracker(cash=self.cfg.initial_cash)
        self.risk = risk or RiskEvaluator(RiskConfigModel())
        self.position_qty = 0.0
        self.peak_equity = self.cfg.initial_cash
        self.entry_price: float | None = None
        self.instrument_specs = instrument_specs or {}

    @property
    def mode(self) -> ExecutionMode:
        """Get execution mode based on exchange type.

        Returns:
            "paper" for paper trading, "live" for real exchanges.
        """
        return "paper" if self.exchange == "paper" else "live"

    def _mark_to_market(self, last_close: float) -> float:
        """Calculate current equity and update peak equity.

        Computes total portfolio value as cash plus position value
        at current market price. Updates peak equity for drawdown tracking.

        Args:
            last_close: Current market price of the instrument.

        Returns:
            Current total equity value.
        """
        equity = self.portfolio.cash + self.position_qty * last_close
        self.peak_equity = max(self.peak_equity, equity)
        logger.debug({"equity": equity, "peak": self.peak_equity})
        return equity

    async def _maybe_stop(self, last_close: float, prev_close: float | None = None) -> bool:
        """Check and execute stop-loss if triggered.

        Monitors position against stop-loss threshold based on:
        - Entry price reference (percentage from entry)
        - Fast drop detection (percentage from previous close)

        If triggered, immediately sells entire position.

        Args:
            last_close: Current market price.
            prev_close: Previous bar's close price for fast-drop detection.

        Returns:
            True if stop-loss was triggered and position closed, False otherwise.
        """
        if self.position_qty <= 0:
            return False
        stop_ref = self.entry_price
        if stop_ref is None:
            pos = self.portfolio.positions.get(self.instrument)
            stop_ref = pos.average_price if pos and pos.quantity > 0 else None
        stop_pct = self.risk.stop_pct()
        trigger_ref = stop_ref is not None and last_close <= stop_ref * (1 - stop_pct)
        trigger_fast = prev_close is not None and last_close <= prev_close * (1 - stop_pct)
        if trigger_ref or trigger_fast:
            await self._send_order(
                side="sell",
                size=self.position_qty,
                price=last_close,
                reason="engine-stop",
            )
            fee = last_close * self.position_qty * (self.cfg.fee_bps / 10000.0)
            self.portfolio.update_fill(self.instrument, "sell", self.position_qty, last_close, fee)
            self.position_qty = 0.0
            self.entry_price = None
            return True
        return False

    async def _send_order(
        self,
        side: TradeSide,
        size: float,
        price: float,
        reason: str,
        signaled_at: float | None = None,
    ) -> None:
        """Publish order request to ZMQ execution topic.

        Creates and sends an order request to the execution system
        via ZMQ pub/sub. Orders are published non-blocking.

        Args:
            side: Order side ("buy" or "sell").
            size: Order quantity.
            price: Reference price (for market orders, used for logging).
            reason: Order reason tag (e.g., "engine-buy", "engine-stop").
            signaled_at: Unix timestamp when signal was generated.
        """
        signaled_at_dt = None
        if signaled_at is not None:
            signaled_at_dt = dt.datetime.fromtimestamp(signaled_at, tz=dt.UTC)
        order = OrderRequestData(
            session_id="",
            sequence_id=0,
            strategy_id=reason,
            instrument=self.instrument,
            mode=self.mode,
            side=side,
            order_type="market",
            quantity=size,
            price=None,
            client_order_id=f"{reason}-{uuid.uuid4().hex[:8]}",
            exchange=self.exchange,
            signaled_at=signaled_at_dt,
        )
        await self.execution_socket.publish(order, flags=zmq.NOBLOCK)
        logger.debug(f"Published order command: {order.client_order_id}")

    async def execute_desired_units(
        self, desired_units: float, current_price: float, signaled_at: float | None = None
    ) -> None:
        """Execute position change based on desired position size.

        Main entry point for strategy signal execution. Compares desired position
        with current position and executes appropriate buy/sell orders.

        For buy signals (desired_units > 0):
        - Checks risk constraints (drawdown, leverage)
        - Calculates position size respecting cash available and fees
        - Rounds to lot size specifications
        - Sends buy order and updates portfolio

        For sell signals (desired_units <= 0):
        - Sells current position (rounded to lot size)
        - Sends sell order and updates portfolio

        Args:
            desired_units: Target position size (positive = long, <= 0 = flat).
            current_price: Current market price for sizing calculations.
            signaled_at: Unix timestamp when signal was generated.
        """
        equity = self._mark_to_market(current_price)
        if desired_units > 0 and self.position_qty <= 0:
            if not self.risk.can_open_new_trade(equity, self.peak_equity):
                return
            fee_rate = self.cfg.fee_bps / 10000.0
            reserve = max(0.01, self.portfolio.cash * 1e-3)
            cash_avail = max(self.portfolio.cash - reserve, 0.0)
            exposure = self.portfolio.notional_exposure(self.instrument, current_price)
            effective_cash = cash_avail / (1.0 + fee_rate)
            desired_units = self.risk.cap_size_by_leverage(
                exposure, effective_cash, current_price, desired_units
            )
            max_size_by_cash = effective_cash / current_price if current_price > 0 else 0.0
            desired_units = max(min(desired_units, max_size_by_cash), 0.0)
            specs = self.instrument_specs.get(self.instrument, {})
            lot = float(specs.get("lot_size", 0.0))
            tick = float(specs.get("tick_size", 0.0))
            desired_units = self.risk.round_size(desired_units, lot, current_price, tick)
            if desired_units <= 0:
                return
            was_flat = self.position_qty <= 0
            await self._send_order(
                side="buy",
                size=desired_units,
                price=current_price,
                reason="engine-buy",
                signaled_at=signaled_at,
            )
            fee = current_price * desired_units * fee_rate
            self.portfolio.update_fill(self.instrument, "buy", desired_units, current_price, fee)
            self.position_qty += desired_units
            if was_flat and self.position_qty > 0:
                self.entry_price = current_price
        elif desired_units <= 0 and self.position_qty > 0:
            specs = self.instrument_specs.get(self.instrument, {})
            lot = float(specs.get("lot_size", 0.0))
            qty_to_sell = self.risk.round_down_to_step(self.position_qty, lot)
            if qty_to_sell <= 0:
                return
            await self._send_order(
                side="sell",
                size=qty_to_sell,
                price=current_price,
                reason="engine-sell",
                signaled_at=signaled_at,
            )
            fee_rate = self.cfg.fee_bps / 10000.0
            fee = current_price * qty_to_sell * fee_rate
            self.portfolio.update_fill(self.instrument, "sell", qty_to_sell, current_price, fee)
            self.position_qty = max(self.position_qty - qty_to_sell, 0.0)
            self.entry_price = None
