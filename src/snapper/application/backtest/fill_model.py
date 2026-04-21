"""Backtest fill simulation model.

Simulates order fills at candle close prices with configurable slippage
and commission. supports only MARKET fills (close-price execution).
"""

from dataclasses import dataclass
from datetime import datetime

from snapper.application.portfolio.models import PortfolioTracker


@dataclass
class BacktestFill:
    """Simulated trade fill from a backtest.

    Attributes:
        exchange: Exchange where the fill occurred.
        instrument: Trading pair symbol.
        side: Trade direction ('buy' or 'sell').
        size: Fill quantity.
        price: Execution price (after slippage).
        fee: Trading fee.
        fee_currency: Currency of the fee.
        fill_at: Timestamp of the fill.
        pnl: Per-fill realized PnL (None for entry trades).
        signal_reason: Strategy signal reason.
        signal_strength: Strategy signal strength.
    """

    exchange: str
    instrument: str
    side: str
    size: float
    price: float
    fee: float
    fee_currency: str
    fill_at: datetime
    pnl: float | None
    signal_reason: str | None
    signal_strength: float | None


def simulate_market_fill(
    exchange: str,
    instrument: str,
    side: str,
    close_price: float,
    fill_at: datetime,
    portfolio: PortfolioTracker,
    slippage_bps: float = 0.0,
    commission_bps: float = 0.0,
    signal_strength: float | None = None,
    signal_reason: str | None = None,
) -> BacktestFill | None:
    """Simulate a market fill at the candle close price.

    For buys: size = signal_strength * (portfolio.cash / fill_price).
    For sells: size = abs(current position quantity) (flatten to zero).

    Args:
        exchange: Exchange name.
        instrument: Trading pair symbol.
        side: 'buy' or 'sell'.
        close_price: Candle close price.
        fill_at: Candle timestamp.
        portfolio: Current portfolio state (mutated in place).
        slippage_bps: Slippage in basis points.
        commission_bps: Commission in basis points.
        signal_strength: Signal strength (0-1 for buys).
        signal_reason: Human-readable signal reason.

    Returns:
        BacktestFill or None if fill cannot be executed (zero size/price).
    """
    if close_price <= 0:
        return None

    direction = 1.0 if side == "buy" else -1.0
    fill_price = close_price * (1.0 + direction * slippage_bps / 10_000)

    if side == "buy":
        strength = signal_strength if signal_strength is not None else 1.0
        if strength <= 0 or portfolio.cash <= 0:
            return None
        cost_per_unit = fill_price * (1.0 + commission_bps / 10_000)
        size = strength * portfolio.cash / cost_per_unit
    else:
        size = abs(portfolio.position_qty(instrument))
        if size <= 0:
            return None

    fee = size * fill_price * commission_bps / 10_000

    pnl_before = portfolio.positions.get(instrument)
    realized_before = pnl_before.realized_pnl if pnl_before else 0.0

    portfolio.update_fill(instrument, side, size, fill_price, fee)

    pnl_after = portfolio.positions.get(instrument)
    realized_after = pnl_after.realized_pnl if pnl_after else 0.0
    per_fill_pnl = realized_after - realized_before

    return BacktestFill(
        exchange=exchange,
        instrument=instrument,
        side=side,
        size=size,
        price=fill_price,
        fee=fee,
        fee_currency="USD",
        fill_at=fill_at,
        pnl=per_fill_pnl if side == "sell" else None,
        signal_reason=signal_reason,
        signal_strength=signal_strength,
    )
