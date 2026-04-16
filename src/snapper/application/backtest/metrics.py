"""Backtest metrics computation — pure math, no external dependencies.

Computes aggregate metrics from equity curves and trade lists. Most
ratio metrics return ``0.0`` when inputs are empty (zero-trade policy).
The three Phase 2c advanced metrics that can be genuinely degenerate
(``max_drawdown_duration_seconds``, ``exposure_ratio``,
``turnover_ratio``) return ``None`` plus a ``MetricWarning`` on the
dataclass's ``warnings`` side-channel; the runner drains it and writes
``metric_warning`` events so metric computation stays independent of
DB availability.

``PROMOTED_METRIC_NAMES`` is the single source of truth for which
legacy ``extra_metrics`` JSON keys moved to typed columns in migration
0005 — read-side fallback logic (route layer + comparison diff) uses
this set to de-duplicate mixed-vintage rows.
"""

import math
import statistics
from dataclasses import dataclass
from dataclasses import field
from typing import Final

from snapper.data.repository_types import BacktestEquityPointInsertRow
from snapper.data.repository_types import BacktestTradeInsertRow

PROMOTED_METRIC_NAMES: Final[frozenset[str]] = frozenset(
    {"sortino_ratio", "cagr", "calmar_ratio", "expectancy", "avg_trade_pnl"}
)


@dataclass
class MetricWarning:
    """Non-fatal degenerate-input flag surfaced by ``compute_metrics``.

    Emitted on the ``BacktestMetrics.warnings`` side-channel so the
    runner can translate them into ``metric_warning`` events after the
    engine exits. Keeps metric computation pure (no DB dependency).

    Attributes:
        metric: Metric name (e.g. ``max_drawdown_duration_seconds``).
        reason: Short explanation (e.g. ``zero_run_duration``).
    """

    metric: str
    reason: str


@dataclass
class BacktestMetrics:
    """Aggregate backtest performance metrics.

    Equity-curve-derived metrics use periodic returns. Trade-derived
    metrics use per-fill PnL from FIFO matching. The three Phase 2c
    additions carry ``None`` defaults because they can be legitimately
    degenerate (see edge-case policy in module docstring).
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
    max_drawdown_duration_seconds: float | None = None
    exposure_ratio: float | None = None
    turnover_ratio: float | None = None
    extra_metrics: dict[str, float] = field(default_factory=dict)
    warnings: list[MetricWarning] = field(default_factory=list)


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

    metrics.max_drawdown_duration_seconds = _compute_max_drawdown_duration_seconds(
        equity_points, metrics.warnings
    )
    metrics.exposure_ratio = _compute_exposure_ratio(equity_points, metrics.warnings)
    metrics.turnover_ratio = _compute_turnover_ratio(equity_points, trades, metrics.warnings)

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


def _compute_max_drawdown_duration_seconds(
    equity_points: list[BacktestEquityPointInsertRow],
    warnings: list[MetricWarning],
) -> float | None:
    """Longest peak-to-recovery window in seconds.

    Walks the equity curve tracking the running peak and the time at
    which that peak was first observed. Every time the equity meets or
    exceeds the running peak, the drawdown window that just closed is
    measured from ``peak_time`` to the current sample time; the max
    across all closed windows wins. If the run ends while still below
    the most recent peak, the dangling window from the last peak to
    ``equity_points[-1].point_time`` is also a candidate.

    Returns ``None`` when: fewer than two equity points exist, every
    equity value equals the starting value (no drawdown ever occurred),
    or the curve is strictly monotonically non-decreasing.
    """
    if len(equity_points) < 2:
        warnings.append(
            MetricWarning(
                metric="max_drawdown_duration_seconds",
                reason="fewer_than_two_equity_points",
            )
        )
        return None
    equities = [ep["equity"] for ep in equity_points]
    times = [ep["point_time"] for ep in equity_points]
    if len(set(equities)) == 1:
        warnings.append(
            MetricWarning(
                metric="max_drawdown_duration_seconds",
                reason="flat_equity_curve",
            )
        )
        return None
    peak_value = equities[0]
    peak_time = times[0]
    in_drawdown = False
    current_drawdown_start = peak_time
    max_duration = 0.0
    for eq, t in zip(equities, times, strict=True):
        if eq < peak_value:
            if not in_drawdown:
                in_drawdown = True
                current_drawdown_start = peak_time
        elif eq >= peak_value:
            if in_drawdown:
                duration = (t - current_drawdown_start).total_seconds()
                if duration > max_duration:
                    max_duration = duration
                in_drawdown = False
            peak_value = eq
            peak_time = t
    if in_drawdown:
        dangling = (times[-1] - current_drawdown_start).total_seconds()
        if dangling > max_duration:
            max_duration = dangling
    if max_duration <= 0:
        warnings.append(
            MetricWarning(
                metric="max_drawdown_duration_seconds",
                reason="no_drawdown_observed",
            )
        )
        return None
    return max_duration


def _compute_exposure_ratio(
    equity_points: list[BacktestEquityPointInsertRow],
    warnings: list[MetricWarning],
) -> float | None:
    """Fraction of run duration during which the portfolio held a position.

    Attributes each inter-sample interval ``[t[i-1], t[i]]`` to the
    numerator iff ``equity_points[i-1].position_value != 0`` — the
    leading edge of the interval. The final sample has no trailing
    interval so its attribution is implicit.

    Returns ``None`` when the total run duration is zero (single-sample
    or zero-duration curve). Zero-trade runs with non-zero duration
    return ``0.0`` — this is a well-defined "never exposed" answer, not
    a degenerate case, per R10 sonnet fix.
    """
    if len(equity_points) < 2:
        warnings.append(
            MetricWarning(metric="exposure_ratio", reason="fewer_than_two_equity_points")
        )
        return None
    total_duration = (
        equity_points[-1]["point_time"] - equity_points[0]["point_time"]
    ).total_seconds()
    if total_duration <= 0:
        warnings.append(MetricWarning(metric="exposure_ratio", reason="zero_run_duration"))
        return None
    exposed = 0.0
    for i in range(1, len(equity_points)):
        prev = equity_points[i - 1]
        curr = equity_points[i]
        if abs(prev["position_value"]) > 1e-12:
            exposed += (curr["point_time"] - prev["point_time"]).total_seconds()
    return exposed / total_duration


def _compute_turnover_ratio(
    equity_points: list[BacktestEquityPointInsertRow],
    trades: list[BacktestTradeInsertRow],
    warnings: list[MetricWarning],
) -> float | None:
    """Total notional traded divided by mean equity.

    Mean equity is the arithmetic mean across every equity sample.
    Returns ``None`` when the equity curve is empty or when mean equity
    is non-positive. Zero-trade runs with a valid positive mean equity
    return ``0.0`` (turnover is definitionally zero, not degenerate).
    """
    if not equity_points:
        warnings.append(MetricWarning(metric="turnover_ratio", reason="empty_equity_curve"))
        return None
    mean_equity = statistics.mean(ep["equity"] for ep in equity_points)
    if mean_equity <= 0:
        warnings.append(MetricWarning(metric="turnover_ratio", reason="non_positive_mean_equity"))
        return None
    notional = sum(abs(t["quantity"]) * t["price"] for t in trades)
    return notional / mean_equity
