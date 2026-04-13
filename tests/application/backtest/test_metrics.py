"""Tests for backtest metrics computation."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from snapper.application.backtest.metrics import compute_metrics

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _equity_point(
    hours_offset: int,
    equity: float,
    cash: float = 0.0,
) -> dict:
    """Build an equity point insert row."""
    return {
        "run_public_id": "run-1",
        "point_time": NOW + timedelta(hours=hours_offset),
        "equity": equity,
        "cash": cash,
        "position_value": equity - cash,
        "drawdown": 0.0,
        "session_id": "s1",
        "sequence_id": hours_offset,
        "timestamp": NOW,
    }


def _trade(pnl: float | None = None, side: str = "sell") -> dict:
    """Build a trade insert row."""
    return {
        "run_public_id": "run-1",
        "executed_at": NOW,
        "instrument": "BTC-USD",
        "side": side,
        "quantity": 1.0,
        "price": 100.0,
        "fee": 0.0,
        "pnl": pnl,
        "position_after": 0.0,
        "signal_public_id": None,
        "session_id": "s1",
        "sequence_id": 1,
        "timestamp": NOW,
    }


class TestComputeMetrics:
    """Tests for compute_metrics function."""

    def test_zero_trades_returns_zeros(self) -> None:
        """Empty trades → all ratio metrics = 0.0."""
        metrics = compute_metrics([], [], initial_balance=10000)
        assert metrics.total_trades == 0
        assert metrics.win_rate == 0.0
        assert metrics.sharpe_ratio == 0.0
        assert metrics.max_drawdown == 0.0

    def test_known_trades_win_rate(self) -> None:
        """Known trades → correct win rate and profit factor."""
        trades = [
            _trade(pnl=100.0),
            _trade(pnl=200.0),
            _trade(pnl=-50.0),
        ]
        metrics = compute_metrics([], trades)
        assert metrics.total_trades == 3
        assert metrics.winning_trades == 2
        assert metrics.losing_trades == 1
        assert metrics.win_rate == pytest.approx(2 / 3)
        assert metrics.total_pnl == pytest.approx(250.0)
        assert metrics.avg_trade_pnl == pytest.approx(250.0 / 3)
        assert metrics.profit_factor == pytest.approx(300.0 / 50.0)

    def test_entry_trades_excluded(self) -> None:
        """Entry trades (pnl=None) are excluded from counts."""
        trades = [
            _trade(pnl=None, side="buy"),
            _trade(pnl=100.0),
        ]
        metrics = compute_metrics([], trades)
        assert metrics.total_trades == 1
        assert metrics.winning_trades == 1

    def test_known_equity_curve_drawdown(self) -> None:
        """Known equity curve → expected max drawdown."""
        points = [
            _equity_point(0, 10000),
            _equity_point(1, 12000),
            _equity_point(2, 9000),
            _equity_point(3, 11000),
        ]
        metrics = compute_metrics(points, [])
        assert metrics.max_drawdown == pytest.approx(3000 / 12000)
        assert metrics.max_equity == pytest.approx(12000)
        assert metrics.final_equity == pytest.approx(11000)

    def test_no_drawdown(self) -> None:
        """Monotonically increasing equity → 0 drawdown."""
        points = [
            _equity_point(0, 10000),
            _equity_point(1, 11000),
            _equity_point(2, 12000),
        ]
        metrics = compute_metrics(points, [])
        assert metrics.max_drawdown == pytest.approx(0.0)

    def test_sharpe_with_known_returns(self) -> None:
        """Known returns → Sharpe is non-zero."""
        points = [
            _equity_point(0, 10000),
            _equity_point(24, 10100),
            _equity_point(48, 10200),
            _equity_point(72, 10300),
        ]
        metrics = compute_metrics(points, [], initial_balance=10000)
        assert metrics.sharpe_ratio > 0

    def test_cagr_positive_return(self) -> None:
        """Positive equity growth → positive CAGR."""
        points = [
            _equity_point(0, 10000),
            _equity_point(24 * 365, 20000),
        ]
        metrics = compute_metrics(points, [], initial_balance=10000)
        assert metrics.cagr == pytest.approx(1.0, rel=0.01)

    def test_single_equity_point(self) -> None:
        """Single equity point → no Sharpe/CAGR computation."""
        points = [_equity_point(0, 10000)]
        metrics = compute_metrics(points, [])
        assert metrics.sharpe_ratio == 0.0
        assert metrics.cagr == 0.0
        assert metrics.final_equity == 10000

    def test_all_losing_trades(self) -> None:
        """All losing trades → win_rate=0, profit_factor=0."""
        trades = [_trade(pnl=-100.0), _trade(pnl=-50.0)]
        metrics = compute_metrics([], trades)
        assert metrics.win_rate == 0.0
        assert metrics.profit_factor == 0.0

    def test_all_winning_trades(self) -> None:
        """All winning trades → win_rate=1, profit_factor=inf."""
        trades = [_trade(pnl=100.0), _trade(pnl=200.0)]
        metrics = compute_metrics([], trades)
        assert metrics.win_rate == 1.0
        assert metrics.profit_factor == float("inf")

    def test_breakeven_trade(self) -> None:
        """Breakeven trade (pnl=0.0) counted, not winning or losing."""
        trades = [_trade(pnl=0.0)]
        metrics = compute_metrics([], trades)
        assert metrics.total_trades == 1
        assert metrics.winning_trades == 0
        assert metrics.losing_trades == 0
