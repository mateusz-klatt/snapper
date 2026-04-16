"""Tests for Phase 2c advanced metrics (3 new + warnings side-channel).

Covers ``max_drawdown_duration_seconds``, ``exposure_ratio``, and
``turnover_ratio`` — including edge-case policy (``None`` + warning
event on degenerate input) and the "zero-trade is not degenerate"
rule for the two zero-tradeable metrics (``exposure_ratio`` and
``turnover_ratio``).
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any

from snapper.application.backtest.metrics import PROMOTED_METRIC_NAMES
from snapper.application.backtest.metrics import MetricWarning
from snapper.application.backtest.metrics import _compute_exposure_ratio
from snapper.application.backtest.metrics import _compute_max_drawdown_duration_seconds
from snapper.application.backtest.metrics import _compute_turnover_ratio
from snapper.application.backtest.metrics import compute_metrics

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _ep(hours: float, equity: float, position_value: float = 0.0) -> dict[str, Any]:
    """Build an equity-point insert-row dict."""
    return {
        "run_public_id": "run-1",
        "point_time": NOW + timedelta(hours=hours),
        "equity": equity,
        "cash": 0.0,
        "position_value": position_value,
        "drawdown": 0.0,
        "session_id": "s1",
        "sequence_id": int(hours),
        "timestamp": NOW,
    }


def _trade(quantity: float, price: float, pnl: float | None = None) -> dict[str, Any]:
    """Build a trade insert-row dict."""
    return {
        "run_public_id": "run-1",
        "executed_at": NOW,
        "instrument": "BTC-USD",
        "side": "sell" if pnl is not None else "buy",
        "quantity": quantity,
        "price": price,
        "fee": 0.0,
        "pnl": pnl,
        "position_after": 0.0,
        "signal_public_id": None,
        "session_id": "s1",
        "sequence_id": 1,
        "timestamp": NOW,
    }


class TestPromotedMetricNames:
    """PROMOTED_METRIC_NAMES is the single source of truth for §4.3."""

    def test_contains_all_five_promoted(self) -> None:
        """Set matches the migration 0005 promoted-column list."""
        assert (
            frozenset({"sortino_ratio", "cagr", "calmar_ratio", "expectancy", "avg_trade_pnl"})
            == PROMOTED_METRIC_NAMES
        )


class TestMaxDrawdownDurationSeconds:
    """Edge cases for _compute_max_drawdown_duration_seconds."""

    def test_fewer_than_two_points_returns_none_with_warning(self) -> None:
        """Single equity point cannot define a drawdown window."""
        warnings: list[MetricWarning] = []
        result = _compute_max_drawdown_duration_seconds([_ep(0, 100.0)], warnings)
        assert result is None
        assert len(warnings) == 1
        assert warnings[0].metric == "max_drawdown_duration_seconds"
        assert warnings[0].reason == "fewer_than_two_equity_points"

    def test_flat_equity_curve_returns_none_with_warning(self) -> None:
        """All equity values identical — no drawdown ever occurs."""
        warnings: list[MetricWarning] = []
        points = [_ep(i, 100.0) for i in range(5)]
        result = _compute_max_drawdown_duration_seconds(points, warnings)
        assert result is None
        assert warnings[0].reason == "flat_equity_curve"

    def test_monotonically_increasing_returns_none_with_warning(self) -> None:
        """Curve never drops below its peak — no_drawdown_observed."""
        warnings: list[MetricWarning] = []
        points = [_ep(0, 100.0), _ep(1, 110.0), _ep(2, 120.0)]
        result = _compute_max_drawdown_duration_seconds(points, warnings)
        assert result is None
        assert warnings[0].reason == "no_drawdown_observed"

    def test_closed_drawdown_measures_peak_to_recovery(self) -> None:
        """Peak at h0, trough at h2, recovered at h4 — 4h window wins."""
        warnings: list[MetricWarning] = []
        points = [
            _ep(0, 100.0),
            _ep(1, 95.0),
            _ep(2, 90.0),
            _ep(3, 92.0),
            _ep(4, 100.0),
            _ep(5, 105.0),
        ]
        result = _compute_max_drawdown_duration_seconds(points, warnings)
        assert result == 4 * 3600.0
        assert warnings == []

    def test_unrecovered_drawdown_uses_dangling_suffix(self) -> None:
        """Run ends below the last peak — final suffix is a candidate."""
        warnings: list[MetricWarning] = []
        points = [
            _ep(0, 100.0),
            _ep(1, 110.0),
            _ep(2, 90.0),
            _ep(3, 85.0),
        ]
        result = _compute_max_drawdown_duration_seconds(points, warnings)
        assert result == 2 * 3600.0
        assert warnings == []

    def test_longest_of_multiple_drawdowns_wins(self) -> None:
        """Two closed drawdown windows — the longer one is returned."""
        warnings: list[MetricWarning] = []
        points = [
            _ep(0, 100.0),
            _ep(1, 95.0),
            _ep(2, 100.0),
            _ep(3, 90.0),
            _ep(4, 85.0),
            _ep(5, 88.0),
            _ep(6, 100.0),
        ]
        result = _compute_max_drawdown_duration_seconds(points, warnings)
        assert result == 4 * 3600.0


class TestExposureRatio:
    """Edge cases for _compute_exposure_ratio."""

    def test_fewer_than_two_points_returns_none_with_warning(self) -> None:
        """Single point cannot define a duration."""
        warnings: list[MetricWarning] = []
        result = _compute_exposure_ratio([_ep(0, 100.0)], warnings)
        assert result is None
        assert warnings[0].reason == "fewer_than_two_equity_points"

    def test_zero_run_duration_returns_none_with_warning(self) -> None:
        """Two samples at the same instant produce zero duration."""
        warnings: list[MetricWarning] = []
        points = [_ep(0, 100.0), _ep(0, 110.0)]
        result = _compute_exposure_ratio(points, warnings)
        assert result is None
        assert warnings[0].reason == "zero_run_duration"

    def test_zero_trade_non_zero_duration_returns_zero_no_warning(self) -> None:
        """No position ever held — exposure is a clean 0.0 (not degenerate)."""
        warnings: list[MetricWarning] = []
        points = [_ep(0, 100.0), _ep(1, 100.0), _ep(2, 100.0)]
        result = _compute_exposure_ratio(points, warnings)
        assert result == 0.0
        assert warnings == []

    def test_always_exposed_returns_one(self) -> None:
        """Every leading edge has a position — ratio is 1.0."""
        warnings: list[MetricWarning] = []
        points = [_ep(0, 100.0, 10.0), _ep(1, 100.0, 10.0), _ep(2, 100.0, 10.0)]
        result = _compute_exposure_ratio(points, warnings)
        assert result == 1.0

    def test_half_exposed_returns_half(self) -> None:
        """First interval exposed, second not — 50 % ratio."""
        warnings: list[MetricWarning] = []
        points = [
            _ep(0, 100.0, 10.0),
            _ep(1, 100.0, 0.0),
            _ep(2, 100.0, 0.0),
        ]
        result = _compute_exposure_ratio(points, warnings)
        assert result == 0.5


class TestTurnoverRatio:
    """Edge cases for _compute_turnover_ratio."""

    def test_empty_equity_returns_none_with_warning(self) -> None:
        """Cannot compute mean equity of an empty curve."""
        warnings: list[MetricWarning] = []
        result = _compute_turnover_ratio([], [_trade(1.0, 100.0)], warnings)
        assert result is None
        assert warnings[0].reason == "empty_equity_curve"

    def test_non_positive_mean_equity_returns_none_with_warning(self) -> None:
        """Mean equity <= 0 is degenerate — division is undefined."""
        warnings: list[MetricWarning] = []
        points = [_ep(0, 0.0), _ep(1, 0.0)]
        result = _compute_turnover_ratio(points, [_trade(1.0, 100.0)], warnings)
        assert result is None
        assert warnings[0].reason == "non_positive_mean_equity"

    def test_zero_trade_non_zero_equity_returns_zero_no_warning(self) -> None:
        """No trades — turnover is definitionally 0.0, not degenerate."""
        warnings: list[MetricWarning] = []
        points = [_ep(0, 100.0), _ep(1, 100.0)]
        result = _compute_turnover_ratio(points, [], warnings)
        assert result == 0.0
        assert warnings == []

    def test_turnover_divides_notional_by_mean_equity(self) -> None:
        """Notional = |qty|*price summed; divided by mean equity."""
        warnings: list[MetricWarning] = []
        points = [_ep(0, 100.0), _ep(1, 200.0)]
        trades = [_trade(2.0, 50.0), _trade(-1.0, 100.0)]
        result = _compute_turnover_ratio(points, trades, warnings)
        assert result == 200.0 / 150.0
        assert warnings == []


class TestComputeMetricsIntegratesWarnings:
    """compute_metrics drains the 3 new edge cases into ``warnings``."""

    def test_empty_curve_yields_all_three_warnings(self) -> None:
        """No equity data trips every new-metric degeneracy check."""
        metrics = compute_metrics([], [])
        assert metrics.max_drawdown_duration_seconds is None
        assert metrics.exposure_ratio is None
        assert metrics.turnover_ratio is None
        reasons = {w.metric for w in metrics.warnings}
        assert reasons == {
            "max_drawdown_duration_seconds",
            "exposure_ratio",
            "turnover_ratio",
        }

    def test_healthy_run_yields_no_warnings(self) -> None:
        """A realistic curve + trades produces clean values."""
        points = [
            _ep(0, 10_000.0, 0.0),
            _ep(24, 10_500.0, 5_000.0),
            _ep(48, 9_800.0, 4_000.0),
            _ep(72, 10_400.0, 0.0),
            _ep(96, 11_000.0, 0.0),
        ]
        trades = [_trade(0.5, 10_000.0, pnl=500.0)]
        metrics = compute_metrics(points, trades, initial_balance=10_000.0)
        assert metrics.max_drawdown_duration_seconds is not None
        assert metrics.max_drawdown_duration_seconds > 0
        assert metrics.exposure_ratio is not None
        assert 0.0 < metrics.exposure_ratio <= 1.0
        assert metrics.turnover_ratio is not None
        assert metrics.turnover_ratio > 0
        assert metrics.warnings == []
