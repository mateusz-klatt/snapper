"""Trading engine service module.

This module provides the core trading execution logic through TradingEngineService.
Each engine instance manages a single instrument, handling position entry/exit,
stop-loss logic, fee calculation, and order publication to ZMQ.
"""

import datetime as dt
import time
from collections import OrderedDict
from uuid import uuid7

import zmq
from loguru import logger

from snapper.application.engine.config import EngineConfigModel
from snapper.application.portfolio.models import PortfolioTracker
from snapper.application.risk.models import RiskConfigModel
from snapper.application.risk.models import RiskEvaluator
from snapper.application.trade.outbox import OutboxDispatcher
from snapper.core.types import ExchangeEnum
from snapper.core.types import ExecutionModeEnum
from snapper.core.types import OrderCommandEnum
from snapper.core.types import OrderExchange
from snapper.core.types import OrderTypeEnum
from snapper.core.types import TradeSideEnum
from snapper.data.repository import SQLAlchemyRepository
from snapper.interface.websocket.schemas import ExecutionMode
from snapper.interface.websocket.schemas import TradeSide
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import OrderRequestData
from snapper.messaging.topics.builders import order_command_topic


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

    IN_FLIGHT_TIMEOUT: float = 60.0

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
    order_in_flight: bool
    pending_client_order_id: str | None
    seen_exec_ids: OrderedDict[str, None]

    def __init__(
        self,
        instrument: str,
        execution_socket: MessagePublisher,
        risk: RiskEvaluator | None = None,
        cfg: EngineConfigModel | None = None,
        *,
        instrument_specs: dict[str, dict[str, float]] | None = None,
        exchange: OrderExchange = ExchangeEnum.PAPER,
        repository: SQLAlchemyRepository | None = None,
        outbox: OutboxDispatcher | None = None,
        strategy_tag: str | None = None,
        wallet_public_id: str = "",
        operator_public_id: str = "",
    ) -> None:
        """Initialize trading engine for a specific instrument.

        Args:
            instrument: Symbol to trade (e.g., "BTC-USD").
            execution_socket: Message publisher for order requests.
            risk: Risk evaluator instance. Defaults to standard RiskEvaluator.
            cfg: Engine configuration. Defaults to EngineConfigModel defaults.
            instrument_specs: Dict mapping symbol to lot_size/tick_size specs.
            exchange: Target exchange. Defaults to "paper" for simulation.
            repository: Optional DB repository for writing TradeCommand.
            outbox: Optional outbox dispatcher to notify after DB write.
            strategy_tag: Strategy discriminator for paper mode sharding.
                Paper engines with different tags get isolated shard_keys.
                Ignored for live mode (one consolidated position per instrument).
            wallet_public_id: Wallet that owns positions and credentials for
                this engine instance. Transitional default ``""``;
                NOT NULL migration tightens the columns.
            operator_public_id: Trading-identity operator that initiated the
                strategy this engine serves. Stored on the engine for audit
                propagation onto every TradeCommand and OrderRequestData
                this engine emits.
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
        self.order_in_flight = False
        self.pending_client_order_id: str | None = None
        self._in_flight_since: float | None = None
        self.seen_exec_ids: OrderedDict[str, None] = OrderedDict()
        self.read_only = False
        self._repository = repository
        self._outbox = outbox
        self._strategy_tag = strategy_tag
        self.wallet_public_id = wallet_public_id
        self.operator_public_id = operator_public_id
        base = f"{exchange}.{instrument}.{self.mode}"
        if wallet_public_id:
            wallet_short = wallet_public_id.replace("-", "")[:12].lower()
            base = f"{base}.w{wallet_short}"
        self._shard_key = (
            f"{base}.{strategy_tag}"
            if self.mode == ExecutionModeEnum.PAPER and strategy_tag
            else base
        )

    def _check_in_flight_timeout(self) -> None:
        """Clear in-flight guard if timeout has elapsed.

        Called before processing new signals. If the configured timeout
        has passed since the order was sent, logs a warning and clears
        the guard so new signals can be processed.
        """
        if not self.order_in_flight or self._in_flight_since is None:
            return
        elapsed = time.monotonic() - self._in_flight_since
        if elapsed > self.IN_FLIGHT_TIMEOUT:
            logger.warning(
                f"Order {self.pending_client_order_id} in-flight timeout "
                f"after {elapsed:.1f}s for {self.instrument}, clearing guard"
            )
            self.order_in_flight = False
            self.pending_client_order_id = None
            self._in_flight_since = None

    def apply_fill(self, fill: ExecutionData) -> bool:
        """Apply a confirmed execution fill to engine state.

        Uses delta fields (last_size/last_price) for booking. Duplicate
        fills are detected by exec_id/trade_id and silently dropped.
        Clears order_in_flight only when the fill matches the current
        pending order and status is 'filled' (complete).

        Args:
            fill: Execution event with delta fill data.

        Returns:
            True if fill was applied, False if duplicate.
        """
        guard_key = (
            fill.trade_id or f"fallback-{fill.client_order_id}-{fill.last_size}-{fill.last_price}"
        )
        if guard_key in self.seen_exec_ids:
            logger.info(f"Duplicate execution {guard_key} ignored for {self.instrument}")
            return False
        self.seen_exec_ids[guard_key] = None
        if len(self.seen_exec_ids) > 10_000:
            self.seen_exec_ids.popitem(last=False)
        self.portfolio.update_fill(
            self.instrument, fill.side, fill.last_size, fill.last_price, fill.fee
        )
        old_qty = self.position_qty
        if fill.side == TradeSideEnum.BUY:
            self.position_qty += fill.last_size
        else:
            self.position_qty -= fill.last_size
        if abs(self.position_qty) < 1e-12:
            self.position_qty = 0.0
            self.entry_price = None
        elif old_qty <= 0 < self.position_qty or old_qty >= 0 > self.position_qty:
            self.entry_price = fill.last_price
        elif self.entry_price is not None and abs(self.position_qty) > abs(old_qty):
            old_abs = abs(old_qty)
            new_abs = abs(self.position_qty)
            self.entry_price = (
                old_abs * self.entry_price + fill.last_size * fill.last_price
            ) / new_abs
        if fill.client_order_id == self.pending_client_order_id and fill.status == "filled":
            self.order_in_flight = False
            self.pending_client_order_id = None
            self._in_flight_since = None
        return True

    def clear_pending_intent(self, client_order_id: str) -> bool:
        """Clear in-flight state for a rejected or cancelled order.

        Only clears if the given client_order_id matches the current
        pending order, preventing stale events from clearing newer orders.

        Args:
            client_order_id: Order ID from the reject/cancel event.

        Returns:
            True if intent was cleared, False if ID did not match.
        """
        if client_order_id != self.pending_client_order_id:
            return False
        self.order_in_flight = False
        self.pending_client_order_id = None
        self._in_flight_since = None
        return True

    @property
    def mode(self) -> ExecutionMode:
        """Get execution mode based on exchange type.

        Returns:
            "paper" for paper trading, "live" for real exchanges.
        """
        return (
            ExecutionModeEnum.PAPER
            if self.exchange == ExchangeEnum.PAPER
            else ExecutionModeEnum.LIVE
        )

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

        Monitors position against stop-loss threshold:
        - Long: triggers when price drops below entry by stop_pct
        - Short: triggers when price rises above entry by stop_pct
        - Fast move detection from previous close in both directions

        Args:
            last_close: Current market price.
            prev_close: Previous bar's close price for fast-move detection.

        Returns:
            True if stop-loss was triggered and position closed, False otherwise.
        """
        if abs(self.position_qty) < 1e-12:
            return False
        if self.read_only or self.order_in_flight:
            return False
        stop_ref = self.entry_price
        if stop_ref is None:
            pos = self.portfolio.positions.get(self.instrument)
            stop_ref = pos.average_price if pos and pos.quantity != 0 else None
        stop_pct = self.risk.stop_pct()
        if self.position_qty > 0:
            trigger_ref = stop_ref is not None and last_close <= stop_ref * (1 - stop_pct)
            trigger_fast = prev_close is not None and last_close <= prev_close * (1 - stop_pct)
            stop_side = TradeSideEnum.SELL
            stop_size = self.position_qty
        else:
            trigger_ref = stop_ref is not None and last_close >= stop_ref * (1 + stop_pct)
            trigger_fast = prev_close is not None and last_close >= prev_close * (1 + stop_pct)
            stop_side = TradeSideEnum.BUY
            stop_size = abs(self.position_qty)
        if trigger_ref or trigger_fast:
            client_order_id = await self._send_order(
                side=stop_side,
                size=stop_size,
                price=last_close,
                reason="engine-stop",
            )
            self.order_in_flight = True
            self.pending_client_order_id = client_order_id
            self._in_flight_since = time.monotonic()
            return True
        return False

    async def _send_order(
        self,
        side: TradeSide,
        size: float,
        price: float,
        reason: str,
        signaled_at: float | None = None,
        leverage: int | None = None,
        reduce_only: bool = False,
    ) -> str:
        """Publish order request to ZMQ execution topic.

        If a repository is configured, writes a durable TradeCommand to DB
        before publishing to ZMQ (dual-write for migration safety).
        Notifies the outbox dispatcher after DB commit.

        Args:
            side: Order side ("buy" or "sell").
            size: Order quantity.
            price: Reference price (for market orders, used for logging).
            reason: Order reason tag (e.g., "engine-buy", "engine-stop").
            signaled_at: Unix timestamp when signal was generated.
            leverage: Margin leverage (None for spot).
            reduce_only: True when closing a position.

        Returns:
            Client order ID assigned to the published order.
        """
        signaled_at_dt = None
        if signaled_at is not None:
            signaled_at_dt = dt.datetime.fromtimestamp(signaled_at, tz=dt.UTC)
        topic = order_command_topic(self.exchange, self.instrument, OrderCommandEnum.SUBMIT)
        order_public_id = str(uuid7())
        now = dt.datetime.now(dt.UTC)
        session_id = self.execution_socket.tracker.session_id
        sequence_id = self.execution_socket.tracker.next_sequence(topic)

        if self._repository is not None:
            await self._repository.insert_trade_command(
                {
                    "command_type": OrderCommandEnum.SUBMIT,
                    "shard_key": self._shard_key,
                    "exchange": self.exchange,
                    "instrument": self.instrument,
                    "mode": self.mode,
                    "strategy_id": reason,
                    "client_order_id": order_public_id,
                    "venue_client_id": order_public_id,
                    "side": side,
                    "order_type": "market",
                    "quantity": size,
                    "price": None,
                    "leverage": leverage,
                    "reduce_only": reduce_only,
                    "status": "created",
                    "created_at": now,
                    "correlation_id": order_public_id,
                    "session_id": session_id,
                    "sequence_id": sequence_id,
                    "timestamp": now,
                    "wallet_public_id": self.wallet_public_id or "",
                    "operator_public_id": self.operator_public_id or None,
                }
            )

        if self._outbox is not None:
            self._outbox.notify()
            logger.debug(f"Durable command written, outbox notified: {order_public_id}")
        else:
            order = OrderRequestData(
                public_id=order_public_id,
                timestamp=now,
                session_id=session_id,
                sequence_id=sequence_id,
                strategy_id=reason,
                instrument=self.instrument,
                mode=self.mode,
                side=side,
                order_type=OrderTypeEnum.MARKET,
                quantity=size,
                price=None,
                client_order_id=order_public_id,
                exchange=self.exchange,
                strategy_tag=self._strategy_tag,
                signaled_at=signaled_at_dt,
                leverage=leverage,
                reduce_only=reduce_only,
                wallet_public_id=self.wallet_public_id,
                operator_public_id=self.operator_public_id or None,
            )
            await self.execution_socket.send(topic, order, flags=zmq.NOBLOCK)
            if self._repository is not None:
                await self._repository.update_trade_command_status(
                    public_id=order_public_id,
                    new_status="direct_dispatched",
                    bus_time=now,
                    session_id=session_id,
                    sequence_id=sequence_id,
                    dispatched_at=now,
                    attempt_count=1,
                )
            logger.debug(f"Published order command (direct): {order_public_id}")
        return order_public_id

    def _cap_opening_size(
        self, opening_qty: float, current_price: float, closing_proceeds: float = 0.0
    ) -> float:
        """Apply leverage, cash, and lot-size constraints to the opening portion.

        For flip transitions, closing_proceeds estimates the cash freed by
        closing the existing position so the opening cap reflects post-close state.

        Args:
            opening_qty: Unsigned quantity for the new direction.
            current_price: Current market price.
            closing_proceeds: Estimated cash from closing the existing position.

        Returns:
            Capped and rounded opening quantity (unsigned).
        """
        fee_rate = self.cfg.fee_bps / 10000.0
        reserve = max(0.01, self.portfolio.cash * 1e-3)
        cash_avail = max(self.portfolio.cash + closing_proceeds - reserve, 0.0)
        exposure = self.portfolio.notional_exposure(self.instrument, current_price)
        effective_cash = cash_avail / (1.0 + fee_rate)
        post_close_exposure = max(exposure - closing_proceeds, 0.0)
        opening_qty = self.risk.cap_size_by_leverage(
            post_close_exposure, effective_cash, current_price, opening_qty
        )
        max_size_by_cash = effective_cash / current_price if current_price > 0 else 0.0
        opening_qty = max(min(opening_qty, max_size_by_cash), 0.0)
        specs = self.instrument_specs.get(self.instrument, {})
        lot = float(specs.get("lot_size", 0.0))
        tick = float(specs.get("tick_size", 0.0))
        return self.risk.round_size(opening_qty, lot, current_price, tick)

    def _classify_delta(self, desired_units: float, abs_delta: float) -> tuple[float, float]:
        """Split position delta into closing and opening portions.

        For flip transitions the closing portion is unconstrained, while
        the opening portion will be subject to cash and leverage caps.

        Args:
            desired_units: Target position size.
            abs_delta: Absolute size of the position change.

        Returns:
            Tuple of (closing_qty, opening_qty), both unsigned.
        """
        crosses_zero = (self.position_qty > 0 and desired_units < 0) or (
            self.position_qty < 0 and desired_units > 0
        )
        is_opening = (self.position_qty <= 0 and desired_units > 0) or (
            self.position_qty >= 0 and desired_units < 0
        )
        if crosses_zero:
            closing = abs(self.position_qty)
            return closing, abs_delta - closing
        if is_opening:
            return 0.0, abs_delta
        is_reducing = abs(desired_units) < abs(self.position_qty)
        if is_reducing:
            return abs_delta, 0.0
        return 0.0, abs_delta

    async def execute_desired_units(
        self, desired_units: float, current_price: float, signaled_at: float | None = None
    ) -> None:
        """Execute position change based on desired position size.

        Main entry point for strategy signal execution. Compares desired position
        with current position and sends appropriate buy/sell orders. Supports
        long, short, and flip transitions.

        For flips (long→short or short→long), the delta is split into a closing
        portion (uncapped) and an opening portion (subject to cash/leverage caps).

        Portfolio and position state are NOT updated here; they change only
        when confirmed fills arrive via apply_fill().

        Args:
            desired_units: Target position size (positive=long, negative=short, 0=flat).
            current_price: Current market price for sizing calculations.
            signaled_at: Unix timestamp when signal was generated.
        """
        if self.read_only:
            logger.warning(f"Engine {self.instrument} in degraded read-only mode, dropping signal")
            return
        self._check_in_flight_timeout()
        if self.order_in_flight:
            logger.warning(
                f"Order {self.pending_client_order_id} still in flight for "
                f"{self.instrument}, dropping signal"
            )
            return
        delta = desired_units - self.position_qty
        if abs(delta) < 1e-12:
            return
        equity = self._mark_to_market(current_price)
        side = TradeSideEnum.BUY if delta > 0 else TradeSideEnum.SELL
        closing_qty, opening_qty = self._classify_delta(desired_units, abs(delta))
        if opening_qty > 0:
            if not self.risk.can_open_new_trade(equity, self.peak_equity):
                opening_qty = 0.0
            else:
                proceeds = closing_qty * current_price if closing_qty > 0 else 0.0
                opening_qty = self._cap_opening_size(opening_qty, current_price, proceeds)
        total_order = closing_qty + opening_qty
        specs = self.instrument_specs.get(self.instrument, {})
        lot = float(specs.get("lot_size", 0.0))
        total_order = self.risk.round_down_to_step(total_order, lot)
        if total_order <= 0:
            return
        reason = "engine-buy" if side == TradeSideEnum.BUY else "engine-sell"
        is_pure_close = closing_qty > 0 and opening_qty <= 0
        client_order_id = await self._send_order(
            side=side,
            size=total_order,
            price=current_price,
            reason=reason,
            signaled_at=signaled_at,
            leverage=self.cfg.leverage,
            reduce_only=is_pure_close,
        )
        self.order_in_flight = True
        self.pending_client_order_id = client_order_id
        self._in_flight_since = time.monotonic()
