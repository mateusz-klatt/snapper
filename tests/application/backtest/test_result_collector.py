"""Tests for ResultCollector artifact buffering."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

from snapper.application.backtest.fill_model import BacktestFill
from snapper.application.backtest.result_collector import ResultCollector
from snapper.application.portfolio.models import PortfolioTracker

NOW = datetime(2026, 4, 13, tzinfo=UTC)


class TestResultCollector:
    """Tests for ResultCollector buffering behavior."""

    def test_record_signal(self) -> None:
        """Signal is buffered with correct fields, including engine-supplied public_id."""
        collector = ResultCollector()
        collector.record_signal(
            run_public_id="run-1",
            public_id="00000000-0000-7000-8000-000000000001",
            signal_time=NOW,
            signal_type="buy",
            instrument="BTC-USD",
            price=50000.0,
            indicators={"sma": 49000},
            session_id="s1",
            sequence_id=1,
            bus_time=NOW,
        )
        assert len(collector.signals) == 1
        assert collector.signals[0]["signal_type"] == "buy"
        assert collector.signals[0]["instrument"] == "BTC-USD"
        assert collector.signals[0]["public_id"] == "00000000-0000-7000-8000-000000000001"

    def test_record_trade(self) -> None:
        """Trade fill is buffered with correct fields."""
        collector = ResultCollector()
        fill = BacktestFill(
            exchange="kraken",
            instrument="BTC-USD",
            side="buy",
            size=0.2,
            price=50000.0,
            fee=10.0,
            fee_currency="USD",
            fill_at=NOW,
            pnl=None,
            signal_reason="macd_cross",
            signal_strength=1.0,
        )
        portfolio = PortfolioTracker(cash=10000.0)
        portfolio.update_fill("BTC-USD", "buy", 0.2, 50000.0, 10.0)
        collector.record_trade(
            run_public_id="run-1",
            fill=fill,
            portfolio=portfolio,
            signal_public_id="00000000-0000-7000-8000-000000000002",
            session_id="s1",
            sequence_id=1,
            bus_time=NOW,
        )
        assert len(collector.trades) == 1
        assert collector.trades[0]["side"] == "buy"
        assert collector.trades[0]["quantity"] == 0.2
        assert collector.trades[0]["position_after"] == 0.2
        assert collector.trades[0]["signal_public_id"] == "00000000-0000-7000-8000-000000000002"

    def test_record_trade_signal_public_id_none_for_synthetic_fills(self) -> None:
        """signal_public_id may be None for synthetic fills with no originating signal."""
        collector = ResultCollector()
        fill = BacktestFill(
            exchange="kraken",
            instrument="BTC-USD",
            side="buy",
            size=0.1,
            price=50000.0,
            fee=5.0,
            fee_currency="USD",
            fill_at=NOW,
            pnl=None,
            signal_reason=None,
            signal_strength=None,
        )
        portfolio = PortfolioTracker(cash=10000.0)
        portfolio.update_fill("BTC-USD", "buy", 0.1, 50000.0, 5.0)
        collector.record_trade(
            run_public_id="run-1",
            fill=fill,
            portfolio=portfolio,
            signal_public_id=None,
            session_id="s1",
            sequence_id=1,
            bus_time=NOW,
        )
        assert collector.trades[0]["signal_public_id"] is None

    def test_maybe_record_equity_deduplicates(self) -> None:
        """Equity points are deduplicated by timestamp."""
        collector = ResultCollector()
        portfolio = PortfolioTracker(cash=10000.0)
        closes: dict[str, float] = {}

        collector.maybe_record_equity("run-1", NOW, portfolio, closes, "s1", 1, NOW)
        collector.maybe_record_equity("run-1", NOW, portfolio, closes, "s1", 2, NOW)
        assert len(collector.equity_points) == 1

        t2 = NOW + timedelta(hours=1)
        collector.maybe_record_equity("run-1", t2, portfolio, closes, "s1", 3, NOW)
        assert len(collector.equity_points) == 2

    def test_equity_captures_mark_to_market(self) -> None:
        """Equity point reflects portfolio equity with latest prices."""
        collector = ResultCollector()
        portfolio = PortfolioTracker(cash=10000.0)
        portfolio.update_fill("BTC-USD", "buy", 0.1, 50000.0, 0.0)
        closes = {"BTC-USD": 55000.0}

        collector.maybe_record_equity("run-1", NOW, portfolio, closes, "s1", 1, NOW)
        assert len(collector.equity_points) == 1
        point = collector.equity_points[0]
        remaining_cash = 10000.0 - 0.1 * 50000.0
        expected_equity = remaining_cash + 0.1 * 55000.0
        assert point["equity"] == expected_equity
        assert point["cash"] == remaining_cash

    def test_empty_collector(self) -> None:
        """Fresh collector has empty buffers."""
        collector = ResultCollector()
        assert collector.signals == []
        assert collector.trades == []
        assert collector.equity_points == []


class TestCrossAssetBlockedFills:
    """Cross-asset missing-target-close counter."""

    def test_counter_initial_zero(self) -> None:
        """Fresh collector has cross_asset_blocked_fills == 0.

        Given: a newly constructed ResultCollector,
        When: the cross_asset_blocked_fills attribute is read,
        Then: the value is 0 — single-feed runs surface
            ``extra_metrics == {}`` byte-identically with pre-v1.2.
        """
        collector = ResultCollector()
        assert collector.cross_asset_blocked_fills == 0

    def test_increment_blocked_fill_raises_counter(self) -> None:
        """increment_blocked_fill bumps the counter by exactly one per call.

        Given: a fresh collector,
        When: increment_blocked_fill is called three times with the
            reason='missing_target_close' tag,
        Then: cross_asset_blocked_fills == 3. The debug log is
            exercised by the reason kwarg path.
        """
        collector = ResultCollector()
        collector.increment_blocked_fill(reason="missing_target_close")
        collector.increment_blocked_fill(reason="missing_target_close")
        collector.increment_blocked_fill(reason="missing_target_close")
        assert collector.cross_asset_blocked_fills == 3
