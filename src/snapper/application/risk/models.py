"""Risk management models module.

This module provides risk configuration and evaluation for trading operations.
It implements position sizing based on risk-per-trade, leverage limits,
and drawdown constraints.
"""

from dataclasses import dataclass


@dataclass
class RiskConfigModel:
    """Risk management configuration parameters.

    Defines the risk constraints applied to all trading operations.

    Attributes:
        r_per_trade: Fraction of equity to risk per trade.
            Defaults to 0.5% (0.005).
        max_leverage: Maximum portfolio leverage allowed.
            1.0 means no leverage. Defaults to 1.0.
        max_drawdown: Maximum drawdown allowed before blocking new trades.
            0.2 means 20% max drawdown. Defaults to 0.2.
        stop_r_multiple: Stop-loss distance as multiple of base stop.
            Defaults to 1.0 (1% stop).

    Example:
        >>> cfg = RiskConfigModel(r_per_trade=0.01, max_leverage=2.0)
        >>> cfg.max_drawdown
        0.2
    """

    r_per_trade: float = 0.005
    max_leverage: float = 1.0
    max_drawdown: float = 0.2
    stop_r_multiple: float = 1.0


class RiskEvaluator:
    """Evaluates risk constraints and calculates position sizes.

    Uses RiskConfigModel to enforce risk management rules:
    - Position sizing based on risk-per-trade and stop distance
    - Leverage limits
    - Drawdown constraints
    - Lot size rounding

    Attributes:
        cfg: Risk configuration model.
    """

    def __init__(self, cfg: RiskConfigModel) -> None:
        """Initialize risk evaluator with configuration.

        Args:
            cfg: Risk configuration parameters.
        """
        self.cfg = cfg

    def stop_pct(self) -> float:
        """Calculate stop-loss percentage.

        Returns:
            Stop-loss as decimal (e.g., 0.01 for 1% stop).
        """
        return 0.01 * self.cfg.stop_r_multiple

    def size_position(self, equity: float, price: float) -> float:
        """Calculate position size based on risk parameters.

        Uses the smaller of:
        - Risk-based size: equity * r_per_trade / stop_distance
        - Leverage-based size: equity * max_leverage / price

        Args:
            equity: Current portfolio equity.
            price: Current instrument price.

        Returns:
            Maximum position size in units.
        """
        risk_amount = equity * self.cfg.r_per_trade
        stop_distance = max(price * self.stop_pct(), 1e-9)
        size_by_risk = max(risk_amount / stop_distance, 0.0)
        size_by_leverage = (equity * self.cfg.max_leverage) / max(price, 1e-9)
        return max(min(size_by_risk, size_by_leverage), 0.0)

    def can_open_new_trade(self, equity: float, peak_equity: float) -> bool:
        """Check if new trades are allowed based on drawdown.

        Blocks new trades when current drawdown exceeds max_drawdown.

        Args:
            equity: Current portfolio equity.
            peak_equity: Highest equity reached.

        Returns:
            True if new trades are allowed, False if drawdown exceeded.
        """
        if peak_equity <= 0:
            return True
        dd = (peak_equity - equity) / peak_equity
        return dd <= self.cfg.max_drawdown

    def cap_size_by_leverage(
        self, current_notional: float, equity: float, price: float, desired_size: float
    ) -> float:
        """Cap position size to respect leverage limits.

        Calculates remaining notional capacity and limits desired size
        to stay within max_leverage constraint.

        Args:
            current_notional: Current total notional exposure.
            equity: Current portfolio equity.
            price: Current instrument price.
            desired_size: Requested position size.

        Returns:
            Position size capped by leverage constraint.
        """
        max_notional = equity * self.cfg.max_leverage
        allowed_notional = max(max_notional - current_notional, 0.0)
        max_size = allowed_notional / max(price, 1e-9)
        return max(min(desired_size, max_size), 0.0)

    @staticmethod
    def round_down_to_step(value: float, step: float) -> float:
        """Round value down to nearest step.

        Args:
            value: Value to round.
            step: Step size (e.g., lot size).

        Returns:
            Value rounded down to nearest step, minimum 0.
        """
        if step <= 0:
            return max(value, 0.0)
        units = int((value + 1e-12) // step)
        return max(units * step, 0.0)

    def round_size(
        self, desired_size: float, lot_size: float, price: float, tick_size: float
    ) -> float:
        """Round position size to valid lot size.

        Args:
            desired_size: Requested position size.
            lot_size: Minimum lot size increment.
            price: Current price (unused, for future notional rounding).
            tick_size: Minimum price increment (unused).

        Returns:
            Position size rounded down to lot_size.
        """
        qty = self.round_down_to_step(desired_size, lot_size)
        if qty <= 0 or tick_size <= 0:
            return max(qty, 0.0)
        return max(qty, 0.0)
