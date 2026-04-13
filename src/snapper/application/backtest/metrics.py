"""Backtest metrics computation — pure math, no external dependencies.

Computes aggregate metrics from equity curves and trade lists.
All ratio metrics return 0.0 when inputs are empty (zero-trade policy).
"""

import math
import statistics
from dataclasses import dataclass
from dataclasses import field

from snapper.data.repository_types import BacktestEquityPointInsertRow
from snapper.data.repository_types import BacktestTradeInsertRow


@dataclass
class BacktestMetrics:
    """Aggregate backtest performance metrics.

    Equity-curve-derived metrics use periodic returns.
    Trade-derived metrics use per-fill PnL from FIFO matching.
    """

    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    total_pnl: float = 0.0
    max_drawdown: float = 0.0
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    expectancy: float = 0.0
    avg_trade_pnl: float = 0.0
    max_equity: float = 0.0
    final_equity: float = 0.0
    cagr: float = 0.0
    calmar_ratio: float = 0.0
    extra_metrics: dict[str, float] = field(default_factory=dict)


def compute_metrics(
    equity_points: list[BacktestEquityPointInsertRow],
    trades: list[BacktestTradeInsertRow],
    initial_balance: float = 10_000.0,
    trading_days_per_year: float = 365.0,
) -> BacktestMetrics:
    """Compute aggregate metrics from equity curve and trade list.

    Args:
        equity_points: Equity curve data points.
        trades: Simulated trade fills with PnL.
        initial_balance: Starting cash balance.
        trading_days_per_year: Annualization factor.

    Returns:
        BacktestMetrics with all computed values.
    """
    metrics = BacktestMetrics()

    exit_trades = [t for t in trades if t.get("pnl") is not None]
    metrics.total_trades = len(exit_trades)

    if exit_trades:
        pnls = [float(t["pnl"]) for t in exit_trades if t["pnl"] is not None]
        metrics.winning_trades = sum(1 for p in pnls if p > 0)
        metrics.losing_trades = sum(1 for p in pnls if p < 0)
        metrics.total_pnl = sum(pnls)
        metrics.win_rate = (
            metrics.winning_trades / metrics.total_trades if metrics.total_trades > 0 else 0.0
        )
        metrics.avg_trade_pnl = metrics.total_pnl / metrics.total_trades
        metrics.expectancy = metrics.avg_trade_pnl

        gross_profit = sum(p for p in pnls if p > 0)
        gross_loss = abs(sum(p for p in pnls if p < 0))
        metrics.profit_factor = (
            gross_profit / gross_loss
            if gross_loss > 0
            else float("inf") if gross_profit > 0 else 0.0
        )

    if equity_points:
        equities = [ep["equity"] for ep in equity_points]
        metrics.final_equity = equities[-1]
        metrics.max_equity = max(equities)

        metrics.max_drawdown = _compute_max_drawdown(equities)

        if len(equities) >= 2:
            returns = _compute_returns(equities)
            metrics.sharpe_ratio = _compute_sharpe(returns, trading_days_per_year)
            metrics.sortino_ratio = _compute_sortino(returns, trading_days_per_year)

        if equity_points[0]["equity"] > 0 and len(equity_points) >= 2:
            first_time = equity_points[0]["point_time"]
            last_time = equity_points[-1]["point_time"]
            duration_days = (last_time - first_time).total_seconds() / 86400
            if duration_days > 0:
                metrics.cagr = _compute_cagr(
                    initial_balance, metrics.final_equity, duration_days, trading_days_per_year
                )
                metrics.calmar_ratio = (
                    metrics.cagr / metrics.max_drawdown if metrics.max_drawdown > 0 else 0.0
                )

    return metrics


def _compute_max_drawdown(equities: list[float]) -> float:
    """Compute maximum drawdown as a fraction (0.0 to 1.0).

    Args:
        equities: List of equity values in chronological order.

    Returns:
        Maximum peak-to-trough decline as a fraction.
    """
    if not equities:
        return 0.0
    peak = equities[0]
    max_dd = 0.0
    for eq in equities:
        if eq > peak:
            peak = eq
        dd = (peak - eq) / peak if peak > 0 else 0.0
        if dd > max_dd:
            max_dd = dd
    return max_dd


def _compute_returns(equities: list[float]) -> list[float]:
    """Compute periodic returns from equity series.

    Args:
        equities: List of equity values.

    Returns:
        List of fractional returns (length = len(equities) - 1).
    """
    returns = []
    for i in range(1, len(equities)):
        if equities[i - 1] > 0:
            returns.append((equities[i] - equities[i - 1]) / equities[i - 1])
        else:
            returns.append(0.0)
    return returns


def _compute_sharpe(returns: list[float], periods_per_year: float) -> float:
    """Compute annualized Sharpe ratio (risk-free rate = 0).

    Args:
        returns: List of periodic returns.
        periods_per_year: Annualization factor.

    Returns:
        Annualized Sharpe ratio, or 0.0 if insufficient data.
    """
    if len(returns) < 2:
        return 0.0
    mean_ret = statistics.mean(returns)
    std_ret = statistics.stdev(returns)
    if std_ret < 1e-15:
        return 0.0
    return (mean_ret / std_ret) * math.sqrt(periods_per_year)


def _compute_sortino(returns: list[float], periods_per_year: float) -> float:
    """Compute annualized Sortino ratio (downside deviation only).

    Args:
        returns: List of periodic returns.
        periods_per_year: Annualization factor.

    Returns:
        Annualized Sortino ratio, or 0.0 if no downside risk.
    """
    if len(returns) < 2:
        return 0.0
    mean_ret = statistics.mean(returns)
    downside = [r for r in returns if r < 0]
    if not downside:
        return 0.0
    downside_dev = math.sqrt(sum(r**2 for r in downside) / len(returns))
    if downside_dev < 1e-15:
        return 0.0
    return (mean_ret / downside_dev) * math.sqrt(periods_per_year)


def _compute_cagr(
    initial: float, final: float, duration_days: float, days_per_year: float
) -> float:
    """Compute compound annual growth rate.

    Args:
        initial: Starting equity.
        final: Ending equity.
        duration_days: Total trading days.
        days_per_year: Days in a year for annualization.

    Returns:
        CAGR as a fraction (e.g., 0.15 = 15% annual return).
    """
    if initial <= 0 or duration_days <= 0:
        return 0.0
    if final <= 0:
        return -1.0
    if final == initial:
        return 0.0
    years = duration_days / days_per_year
    if years < 1e-6:
        return 0.0
    try:
        result: float = (final / initial) ** (1.0 / years) - 1.0
        return result
    except OverflowError:
        return 0.0
