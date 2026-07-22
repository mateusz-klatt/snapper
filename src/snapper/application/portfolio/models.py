"""Portfolio tracking models module.

This module provides portfolio and position tracking for the trading system.
It maintains cash balances, position states, and calculates realized PnL.
"""

from dataclasses import dataclass
from typing import Final

from snapper.core.types import TradeSideEnum

EPSILON_MICRO: Final[float] = 1e-6
"""Tolerance for clamping very small positive cash values."""

EPSILON_NANO: Final[float] = 1e-9
"""Tolerance for detecting near-zero negatives and division guards."""

EPSILON_PICO: Final[float] = 1e-12
"""Threshold below which a position is considered fully closed."""


@dataclass
class PositionStateModel:
    """State of a single position in an instrument.

    Tracks quantity, average entry price, and realized profit/loss.

    Attributes:
        quantity: Current position size (positive = long, negative = short).
        average_price: Volume-weighted average entry price.
        realized_pnl: Total realized profit/loss from closed trades.
    """

    quantity: float = 0.0
    average_price: float = 0.0
    realized_pnl: float = 0.0


class PortfolioTracker:
    """Tracks portfolio cash, positions, and calculates equity.

    Maintains a simple portfolio model with:
    - Cash balance (updated on fills)
    - Position states per instrument
    - Turnover tracking

    Attributes:
        cash: Available cash balance.
        positions: Dict mapping instrument to PositionStateModel.
        turnover: Total trading volume (notional).
    """

    def __init__(self, cash: float = 10_000.0) -> None:
        """Initialize portfolio tracker.

        Args:
            cash: Initial cash balance. Defaults to 10,000.
        """
        self.cash = cash
        self.positions: dict[str, PositionStateModel] = {}
        self.turnover: float = 0.0

    def position_qty(self, instrument: str) -> float:
        """Get current position quantity for an instrument.

        Args:
            instrument: Symbol to query.

        Returns:
            Position quantity, or 0.0 if no position.
        """
        return self.positions.get(instrument, PositionStateModel()).quantity

    def notional_exposure(self, instrument: str, price: float) -> float:
        """Calculate notional exposure for an instrument.

        Args:
            instrument: Symbol to calculate exposure for.
            price: Current market price.

        Returns:
            Absolute notional value of position.
        """
        qty = self.position_qty(instrument)
        return abs(qty) * price

    def equity(self, prices: dict[str, float] | None = None) -> float:
        """Calculate total portfolio equity.

        Args:
            prices: Optional dict mapping instrument to current price.
                If provided, includes unrealized PnL in calculation.

        Returns:
            Total equity (cash + position values).
        """
        eq = self.cash
        if prices:
            for inst, pos in self.positions.items():
                px = prices.get(inst)
                if px is not None:
                    eq += pos.quantity * px
        return eq

    def _clamp_cash(self) -> None:
        """Clamp cash to avoid floating point issues near zero.

        Prevents negative cash from rounding errors.
        """
        if -EPSILON_NANO < self.cash < EPSILON_MICRO:
            self.cash = EPSILON_MICRO
        elif self.cash < 0 and self.cash > -EPSILON_NANO:
            self.cash = 0.0

    def accrue_funding(self, instrument: str, amount: float) -> None:
        """Apply a funding/rollover charge to in-memory portfolio state.

        Mirrors the mutation performed by ``TradeService.add_funding_accrual``
        so the engine's local cash view stays consistent between checkpoint
        writes. This is NOT the durable source of truth — the
        ``AccrualLedger`` table and ``TradeService`` shards are.

        Args:
            instrument: Native symbol of the charged instrument.
            amount: Signed charge. Positive reduces cash (holder pays),
                negative increases cash (holder receives).
        """
        pos = self.positions.setdefault(instrument, PositionStateModel())
        self.cash -= amount
        pos.realized_pnl -= amount
        self._clamp_cash()

    def update_fill(
        self,
        instrument: str,
        side: str,
        size: float,
        price: float,
        fee: float,
        *,
        position_delta: float | None = None,
        cash_fee: float | None = None,
    ) -> None:
        """Update portfolio state from a fill.

        Supports both long and short positions. Uses signed-delta model
        ported from TradeService._update_position:

        - Same-direction fills (adding to position): VWAP avg_price with abs(qty)
        - Opposite-direction fills (reducing/flipping): realize PnL, reset on flip
        - Cash: BUY deducts notional+cash_fee, SELL adds notional-cash_fee

        Args:
            instrument: Symbol that was traded.
            side: "buy" or "sell".
            size: Fill quantity.
            price: Fill price.
            fee: Trading fee paid.
            position_delta: Optional caller-resolved signed base-inventory
                delta. The gross ``size`` remains authoritative for cash and
                turnover when a base-denominated fee changes booked quantity.
            cash_fee: Optional fee charged against the cash leg. Defaults to
                ``fee``; callers pass ``0.0`` for a base-asset fee that already
                reduced ``position_delta`` so the fee is never counted in both
                position quantity and cash.
        """
        pos = self.positions.setdefault(instrument, PositionStateModel())
        self.turnover += size * price
        signed_delta = (
            position_delta
            if position_delta is not None
            else size if side == TradeSideEnum.BUY else -size
        )
        position_size = abs(signed_delta)
        is_increasing = (pos.quantity >= 0 and signed_delta > 0) or (
            pos.quantity <= 0 and signed_delta < 0
        )
        if is_increasing:
            self._increase_position(pos, position_size, price)
        else:
            self._decrease_position(pos, position_size, price)
        pos.quantity += signed_delta
        if abs(pos.quantity) < EPSILON_PICO:
            pos.quantity = 0.0
            pos.average_price = 0.0
        charged_fee = fee if cash_fee is None else cash_fee
        if side == TradeSideEnum.BUY:
            self.cash -= size * price + charged_fee
        else:
            self.cash += size * price - charged_fee
        self._clamp_cash()

    @staticmethod
    def _increase_position(pos: PositionStateModel, fill_size: float, fill_price: float) -> None:
        """Recalculate VWAP entry price for a position-increasing fill.

        Args:
            pos: Current position state.
            fill_size: Unsigned fill quantity.
            fill_price: Fill execution price.
        """
        old_qty = abs(pos.quantity)
        new_qty = old_qty + fill_size
        if pos.average_price > 0 and old_qty > 0 and new_qty > 0:
            pos.average_price = (old_qty * pos.average_price + fill_size * fill_price) / new_qty
        else:
            pos.average_price = fill_price

    @staticmethod
    def _decrease_position(pos: PositionStateModel, fill_size: float, fill_price: float) -> None:
        """Realize PnL and handle overshoot for a position-decreasing fill.

        Args:
            pos: Current position state.
            fill_size: Unsigned fill quantity.
            fill_price: Fill execution price.
        """
        close_qty = min(fill_size, abs(pos.quantity))
        overshoot = fill_size - close_qty
        if pos.average_price > 0 and close_qty > 0:
            pnl_per_unit = fill_price - pos.average_price
            if pos.quantity < 0:
                pnl_per_unit = pos.average_price - fill_price
            pos.realized_pnl += close_qty * pnl_per_unit
        if overshoot > EPSILON_PICO:
            pos.average_price = fill_price
