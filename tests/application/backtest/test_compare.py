"""Tests for Phase 2c backtest comparison diff computation."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from snapper.application.backtest.compare import _promoted_lookup
from snapper.application.backtest.compare import compute_equity_overlay
from snapper.application.backtest.compare import compute_metrics_diff
from snapper.application.backtest.compare import compute_signals_diff
from snapper.application.backtest.compare import compute_trades_diff
from snapper.data.repository_types import BacktestEquityPointRow
from snapper.data.repository_types import BacktestResultRow
from snapper.data.repository_types import BacktestSignalRow
from snapper.data.repository_types import BacktestTradeRow

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _result(
    *,
    total_trades: int = 0,
    winning_trades: int = 0,
    losing_trades: int = 0,
    total_pnl: float = 0.0,
    max_drawdown: float = 0.0,
    sharpe_ratio: float | None = None,
    win_rate: float | None = None,
    profit_factor: float | None = None,
    final_equity: float = 10_000.0,
    max_equity: float = 10_000.0,
    sortino_ratio: float | None = None,
    cagr: float | None = None,
    calmar_ratio: float | None = None,
    expectancy: float | None = None,
    avg_trade_pnl: float | None = None,
    max_drawdown_duration_seconds: float | None = None,
    exposure_ratio: float | None = None,
    turnover_ratio: float | None = None,
    extra_metrics: dict[str, object] | None = None,
) -> BacktestResultRow:
    """Build a minimal BacktestResultRow with sensible defaults."""
    return BacktestResultRow(
        public_id="res-1",
        timestamp=NOW,
        session_id="s1",
        sequence_id=1,
        run_public_id="run-a",
        total_trades=total_trades,
        winning_trades=winning_trades,
        losing_trades=losing_trades,
        total_pnl=total_pnl,
        max_drawdown=max_drawdown,
        sharpe_ratio=sharpe_ratio,
        win_rate=win_rate,
        profit_factor=profit_factor,
        final_equity=final_equity,
        max_equity=max_equity,
        sortino_ratio=sortino_ratio,
        cagr=cagr,
        calmar_ratio=calmar_ratio,
        expectancy=expectancy,
        avg_trade_pnl=avg_trade_pnl,
        max_drawdown_duration_seconds=max_drawdown_duration_seconds,
        exposure_ratio=exposure_ratio,
        turnover_ratio=turnover_ratio,
        extra_metrics=extra_metrics or {},
    )


def _trade(
    public_id: str,
    instrument: str = "BTC-USD",
    executed_at: datetime | None = None,
    side: str = "buy",
    quantity: float = 1.0,
    price: float = 100.0,
    pnl: float | None = None,
) -> BacktestTradeRow:
    """Build a BacktestTradeRow fixture."""
    return BacktestTradeRow(
        public_id=public_id,
        timestamp=NOW,
        session_id="s1",
        sequence_id=1,
        run_public_id="run",
        executed_at=executed_at or NOW,
        instrument=instrument,
        side=side,
        quantity=quantity,
        price=price,
        fee=0.0,
        pnl=pnl,
        position_after=0.0,
        signal_public_id=None,
    )


def _signal(
    public_id: str,
    instrument: str = "BTC-USD",
    signal_time: datetime | None = None,
    signal_type: str = "buy",
) -> BacktestSignalRow:
    """Build a BacktestSignalRow fixture."""
    return BacktestSignalRow(
        public_id=public_id,
        timestamp=NOW,
        session_id="s1",
        sequence_id=1,
        run_public_id="run",
        signal_time=signal_time or NOW,
        signal_type=signal_type,
        instrument=instrument,
        price=100.0,
        indicators={},
    )


def _equity(point_time: datetime, equity: float) -> BacktestEquityPointRow:
    """Build a BacktestEquityPointRow fixture."""
    return BacktestEquityPointRow(
        public_id=f"ep-{equity}",
        timestamp=NOW,
        session_id="s1",
        sequence_id=1,
        run_public_id="run",
        point_time=point_time,
        equity=equity,
        cash=equity,
        position_value=0.0,
        drawdown=0.0,
    )


class TestPromotedLookupGuard:
    """Direct guard for the defensive _promoted_lookup(None) branch."""

    def test_none_result_returns_none(self) -> None:
        """A None result short-circuits to None even at the helper entry."""
        assert _promoted_lookup(None, "sortino_ratio") is None


class TestMetricsDiff:
    """Coverage for compute_metrics_diff."""

    def test_both_none_results_emits_every_metric_with_nulls(self) -> None:
        """Missing both legs → every metric row has run_a/run_b None."""
        rows = compute_metrics_diff(None, None)
        assert len(rows) >= 18
        assert all(row["run_a"] is None and row["run_b"] is None for row in rows)
        assert all(row["delta"] is None and row["pct"] is None for row in rows)

    def test_typed_columns_populate_explicit_metrics(self) -> None:
        """Typed columns produce side-by-side numbers with delta + pct."""
        a = _result(total_trades=10, total_pnl=100.0, sharpe_ratio=1.5)
        b = _result(total_trades=15, total_pnl=250.0, sharpe_ratio=2.0)
        rows = compute_metrics_diff(a, b)
        by_name = {row["name"]: row for row in rows}
        assert by_name["total_trades"]["run_a"] == pytest.approx(10.0)
        assert by_name["total_trades"]["run_b"] == pytest.approx(15.0)
        assert by_name["total_trades"]["delta"] == pytest.approx(5.0)
        assert by_name["total_trades"]["pct"] == pytest.approx(0.5)
        assert by_name["total_pnl"]["delta"] == pytest.approx(150.0)
        assert by_name["sharpe_ratio"]["run_a"] == pytest.approx(1.5)

    def test_promoted_metric_json_fallback(self) -> None:
        """Pre-0005 row with JSON-only value surfaces via fallback."""
        pre_0005 = _result(extra_metrics={"sortino_ratio": 1.8, "cagr": 0.12})
        post_0005 = _result(sortino_ratio=2.0, cagr=0.15)
        rows = compute_metrics_diff(pre_0005, post_0005)
        by_name = {row["name"]: row for row in rows}
        assert by_name["sortino_ratio"]["run_a"] == pytest.approx(1.8)
        assert by_name["sortino_ratio"]["run_b"] == pytest.approx(2.0)
        assert by_name["cagr"]["run_a"] == pytest.approx(0.12)

    def test_typed_zero_beats_json_fallback(self) -> None:
        """Typed 0.0 takes precedence over non-zero JSON (is-not-None coalescing)."""
        row = _result(expectancy=0.0, extra_metrics={"expectancy": 999.9})
        rows = compute_metrics_diff(row, None)
        by_name = {r["name"]: r for r in rows}
        assert by_name["expectancy"]["run_a"] == pytest.approx(0.0)

    def test_extra_metrics_union_excludes_promoted_names(self) -> None:
        """Union of extra_metrics keys subtracts PROMOTED_METRIC_NAMES."""
        a = _result(extra_metrics={"custom_a": 1.0, "sortino_ratio": 9.9})
        b = _result(extra_metrics={"custom_b": 2.0})
        rows = compute_metrics_diff(a, b)
        names = [row["name"] for row in rows]
        assert names.count("sortino_ratio") == 1
        assert "custom_a" in names
        assert "custom_b" in names

    def test_delta_pct_zero_denominator(self) -> None:
        """A zero ``a`` denominator → delta computed, pct None (avoids div-by-zero)."""
        a = _result(total_pnl=0.0)
        b = _result(total_pnl=10.0)
        rows = compute_metrics_diff(a, b)
        by_name = {row["name"]: row for row in rows}
        assert by_name["total_pnl"]["delta"] == pytest.approx(10.0)
        assert by_name["total_pnl"]["pct"] is None

    def test_only_one_side_present_nulls_delta_and_pct(self) -> None:
        """When one leg is None, delta/pct stay None across the board."""
        a = _result(total_trades=5)
        rows = compute_metrics_diff(a, None)
        by_name = {row["name"]: row for row in rows}
        assert by_name["total_trades"]["run_a"] == pytest.approx(5.0)
        assert by_name["total_trades"]["run_b"] is None
        assert by_name["total_trades"]["delta"] is None
        assert by_name["total_trades"]["pct"] is None

    def test_non_numeric_json_value_becomes_none(self) -> None:
        """Non-numeric extra_metrics value is dropped to None."""
        a = _result(extra_metrics={"custom_x": "not a number"})
        rows = compute_metrics_diff(a, None)
        by_name = {row["name"]: row for row in rows}
        assert by_name["custom_x"]["run_a"] is None


class TestEquityOverlay:
    """Coverage for compute_equity_overlay."""

    def test_disjoint_timestamps_full_outer_join(self) -> None:
        """Samples at different times produce one row per time with None on missing leg."""
        eq_a = [_equity(NOW, 100.0)]
        eq_b = [_equity(NOW + timedelta(hours=1), 150.0)]
        rows = compute_equity_overlay(eq_a, eq_b)
        assert len(rows) == 2
        assert rows[0]["equity_a"] == pytest.approx(100.0)
        assert rows[0]["equity_b"] is None
        assert rows[1]["equity_a"] is None
        assert rows[1]["equity_b"] == pytest.approx(150.0)

    def test_overlapping_timestamps_both_set(self) -> None:
        """Same point_time on both legs produces a single row with both equities."""
        eq_a = [_equity(NOW, 100.0)]
        eq_b = [_equity(NOW, 110.0)]
        rows = compute_equity_overlay(eq_a, eq_b)
        assert len(rows) == 1
        assert rows[0]["equity_a"] == pytest.approx(100.0)
        assert rows[0]["equity_b"] == pytest.approx(110.0)

    def test_sorted_by_point_time(self) -> None:
        """Output rows are sorted chronologically regardless of input order."""
        later = NOW + timedelta(hours=2)
        eq_a = [_equity(later, 200.0), _equity(NOW, 100.0)]
        rows = compute_equity_overlay(eq_a, [])
        assert rows[0]["point_time"] == NOW
        assert rows[1]["point_time"] == later

    def test_empty_inputs_produce_empty_output(self) -> None:
        """Two empty legs produce no rows."""
        assert compute_equity_overlay([], []) == []


class TestTradesDiff:
    """Coverage for compute_trades_diff."""

    def test_matched_pair_common_leg_with_pnl_delta(self) -> None:
        """Same key + both have pnl → common entry with pnl_delta."""
        t_a = [_trade("a1", pnl=10.0)]
        t_b = [_trade("b1", pnl=15.0)]
        rows = compute_trades_diff(t_a, t_b)
        assert len(rows) == 1
        assert rows[0]["leg"] == "common"
        assert rows[0]["pnl_a"] == pytest.approx(10.0)
        assert rows[0]["pnl_b"] == pytest.approx(15.0)
        assert rows[0]["pnl_delta"] == pytest.approx(5.0)

    def test_matched_pair_null_pnl_preserves_null_delta(self) -> None:
        """Missing pnl on either leg → pnl_delta None."""
        t_a = [_trade("a1", pnl=None)]
        t_b = [_trade("b1", pnl=15.0)]
        rows = compute_trades_diff(t_a, t_b)
        assert rows[0]["leg"] == "common"
        assert rows[0]["pnl_delta"] is None

    def test_surplus_on_side_a_becomes_singleton(self) -> None:
        """Three trades on A but one on B → 1 common + 2 a-singletons."""
        t_a = [
            _trade("a1", pnl=1.0),
            _trade("a2", pnl=2.0),
            _trade("a3", pnl=3.0),
        ]
        t_b = [_trade("b1", pnl=10.0)]
        rows = compute_trades_diff(t_a, t_b)
        legs = sorted(row["leg"] for row in rows)
        assert legs == ["a", "a", "common"]

    def test_surplus_on_side_b_singleton_carries_pnl_b(self) -> None:
        """A surplus-b singleton records pnl only on pnl_b."""
        t_a = [_trade("a1", pnl=1.0)]
        t_b = [_trade("b1", pnl=1.0), _trade("b2", pnl=7.5)]
        rows = compute_trades_diff(t_a, t_b)
        singletons = [row for row in rows if row["leg"] == "b"]
        assert len(singletons) == 1
        assert singletons[0]["pnl_b"] == pytest.approx(7.5)
        assert singletons[0]["pnl_a"] is None

    def test_quantization_collapses_ieee_drift(self) -> None:
        """Prices differing by 1e-11 land in the same bucket."""
        t_a = [_trade("a1", price=100.0)]
        t_b = [_trade("b1", price=100.0 + 1e-11)]
        rows = compute_trades_diff(t_a, t_b)
        assert len(rows) == 1
        assert rows[0]["leg"] == "common"

    def test_quantization_keeps_real_tick_distinct(self) -> None:
        """Prices differing by 1e-6 stay in different buckets."""
        t_a = [_trade("a1", price=100.0)]
        t_b = [_trade("b1", price=100.0 + 1e-6)]
        rows = compute_trades_diff(t_a, t_b)
        legs = sorted(row["leg"] for row in rows)
        assert legs == ["a", "b"]

    def test_deterministic_within_bucket_by_public_id(self) -> None:
        """Sorted by public_id before matching → deterministic pairing."""
        t_a = [_trade("a_z", pnl=1.0), _trade("a_a", pnl=2.0)]
        t_b = [_trade("b_a", pnl=10.0)]
        rows = compute_trades_diff(t_a, t_b)
        common = next(r for r in rows if r["leg"] == "common")
        assert common["pnl_a"] == pytest.approx(2.0)

    def test_singleton_with_null_pnl(self) -> None:
        """Surplus singleton with pnl=None → pnl_{a,b} both None."""
        t_a = [_trade("a1", pnl=None)]
        rows = compute_trades_diff(t_a, [])
        assert rows[0]["pnl_a"] is None
        assert rows[0]["pnl_b"] is None
        assert rows[0]["pnl_delta"] is None


class TestSignalsDiff:
    """Coverage for compute_signals_diff."""

    def test_matched_pair_is_common(self) -> None:
        """Same (instrument, time, type) → common entry."""
        s_a = [_signal("a1")]
        s_b = [_signal("b1")]
        rows = compute_signals_diff(s_a, s_b)
        assert len(rows) == 1
        assert rows[0]["leg"] == "common"

    def test_multiset_counts(self) -> None:
        """Three signals vs one signal in same bucket → 1 common + 2 a-singletons."""
        s_a = [_signal(f"a{i}") for i in range(3)]
        s_b = [_signal("b1")]
        rows = compute_signals_diff(s_a, s_b)
        legs = sorted(row["leg"] for row in rows)
        assert legs == ["a", "a", "common"]

    def test_disjoint_keys_all_singletons(self) -> None:
        """Different signal_types → no common entries."""
        s_a = [_signal("a1", signal_type="buy")]
        s_b = [_signal("b1", signal_type="sell")]
        rows = compute_signals_diff(s_a, s_b)
        legs = sorted(row["leg"] for row in rows)
        assert legs == ["a", "b"]
