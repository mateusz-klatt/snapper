"""Portfolio tracking models module.

This module provides portfolio and position tracking for the trading system.
It maintains cash balances, position states, and calculates realized PnL.
"""

from dataclasses import dataclass


@dataclass
class PositionStateModel:
    """State of a single position in an instrument.

    Tracks quantity, average entry price, and realized profit/loss.

    Attributes:
        quantity: Current position size (positive = long).
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
        if -1e-9 < self.cash < 1e-6:
            self.cash = 1e-6
        elif self.cash < 0 and self.cash > -1e-9:
            self.cash = 0.0

    def update_fill(
        self, instrument: str, side: str, size: float, price: float, fee: float
    ) -> None:
        """Update portfolio state from a fill.

        For buys:
        - Adds to position quantity
        - Updates average price
        - Deducts cost + fee from cash

        For sells:
        - Reduces position quantity
        - Adds proceeds - fee to cash
        - Calculates realized PnL

        Args:
            instrument: Symbol that was traded.
            side: "buy" or "sell".
            size: Fill quantity.
            price: Fill price.
            fee: Trading fee paid.
        """
        pos = self.positions.setdefault(instrument, PositionStateModel())
        notional = size * price
        self.turnover += notional
        cost = size * price + fee
        if side == "buy":
            new_qty = pos.quantity + size
            pos.average_price = (pos.average_price * pos.quantity + size * price) / max(
                new_qty, 1e-9
            )
            pos.quantity = new_qty
            self.cash -= cost
            self._clamp_cash()
        else:
            pos.quantity -= size
            self.cash += size * price - fee
            pos.realized_pnl += (price - pos.average_price) * size
            if pos.quantity <= 1e-12:
                pos.average_price = 0.0
            self._clamp_cash()
