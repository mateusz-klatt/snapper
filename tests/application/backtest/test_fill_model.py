"""Tests for backtest fill simulation model."""

from datetime import UTC
from datetime import datetime

import pytest

from snapper.application.backtest.fill_model import simulate_market_fill
from snapper.application.portfolio.models import PortfolioTracker

NOW = datetime(2026, 4, 13, tzinfo=UTC)


class TestSimulateMarketFill:
    """Tests for simulate_market_fill function."""

    def test_buy_at_close_price(self) -> None:
        """Buy uses close price, strength=1 invests full cash."""
        portfolio = PortfolioTracker(cash=10_000.0)
        fill = simulate_market_fill(
            exchange="kraken",
            instrument="BTC-USD",
            side="buy",
            close_price=50_000.0,
            fill_at=NOW,
            portfolio=portfolio,
            signal_strength=1.0,
        )
        assert fill is not None
        assert fill.side == "buy"
        assert fill.price == pytest.approx(50_000.0)
        assert fill.size == pytest.approx(0.2)
        assert fill.fee == pytest.approx(0.0)
        assert fill.exchange == "kraken"
        assert fill.instrument == "BTC-USD"
        assert fill.fee_currency == "USD"

    def test_buy_with_slippage(self) -> None:
        """Buy applies positive slippage (worse fill)."""
        portfolio = PortfolioTracker(cash=10_000.0)
        fill = simulate_market_fill(
            exchange="kraken",
            instrument="BTC-USD",
            side="buy",
            close_price=50_000.0,
            fill_at=NOW,
            portfolio=portfolio,
            slippage_bps=10.0,
            signal_strength=1.0,
        )
        assert fill is not None
        expected_price = 50_000.0 * (1.0 + 10.0 / 10_000)
        assert fill.price == pytest.approx(expected_price)

    def test_sell_with_slippage(self) -> None:
        """Sell applies negative slippage (worse fill)."""
        portfolio = PortfolioTracker(cash=0.0)
        portfolio.update_fill("BTC-USD", "buy", 0.2, 50_000.0, 0.0)
        fill = simulate_market_fill(
            exchange="kraken",
            instrument="BTC-USD",
            side="sell",
            close_price=55_000.0,
            fill_at=NOW,
            portfolio=portfolio,
            slippage_bps=10.0,
        )
        assert fill is not None
        expected_price = 55_000.0 * (1.0 - 10.0 / 10_000)
        assert fill.price == pytest.approx(expected_price)

    def test_buy_with_commission(self) -> None:
        """Commission deducted from fill."""
        portfolio = PortfolioTracker(cash=10_000.0)
        fill = simulate_market_fill(
            exchange="kraken",
            instrument="BTC-USD",
            side="buy",
            close_price=50_000.0,
            fill_at=NOW,
            portfolio=portfolio,
            commission_bps=10.0,
            signal_strength=1.0,
        )
        assert fill is not None
        expected_fee = fill.size * fill.price * 10.0 / 10_000
        assert fill.fee == pytest.approx(expected_fee)
        assert portfolio.cash >= 0, "Cash must not go negative after commissioned buy"

    def test_sell_flattens_position(self) -> None:
        """Sell uses full position quantity (flatten to zero)."""
        portfolio = PortfolioTracker(cash=0.0)
        portfolio.update_fill("BTC-USD", "buy", 0.5, 40_000.0, 0.0)
        fill = simulate_market_fill(
            exchange="kraken",
            instrument="BTC-USD",
            side="sell",
            close_price=45_000.0,
            fill_at=NOW,
            portfolio=portfolio,
        )
        assert fill is not None
        assert fill.size == pytest.approx(0.5)
        assert fill.side == "sell"

    def test_sell_captures_pnl(self) -> None:
        """Sell fill captures per-fill realized PnL."""
        portfolio = PortfolioTracker(cash=0.0)
        portfolio.update_fill("BTC-USD", "buy", 1.0, 100.0, 0.0)
        fill = simulate_market_fill(
            exchange="kraken",
            instrument="BTC-USD",
            side="sell",
            close_price=120.0,
            fill_at=NOW,
            portfolio=portfolio,
        )
        assert fill is not None
        assert fill.pnl == pytest.approx(20.0)

    def test_buy_pnl_is_none(self) -> None:
        """Entry buy has no realized PnL (None)."""
        portfolio = PortfolioTracker(cash=10_000.0)
        fill = simulate_market_fill(
            exchange="kraken",
            instrument="BTC-USD",
            side="buy",
            close_price=100.0,
            fill_at=NOW,
            portfolio=portfolio,
        )
        assert fill is not None
        assert fill.pnl is None

    def test_breakeven_sell_has_zero_pnl(self) -> None:
        """Sell at entry price produces pnl=0.0, not None."""
        portfolio = PortfolioTracker(cash=0.0)
        portfolio.update_fill("BTC-USD", "buy", 1.0, 100.0, 0.0)
        fill = simulate_market_fill(
            exchange="kraken",
            instrument="BTC-USD",
            side="sell",
            close_price=100.0,
            fill_at=NOW,
            portfolio=portfolio,
        )
        assert fill is not None
        assert fill.pnl is not None
        assert fill.pnl == pytest.approx(0.0)

    def test_zero_close_price_returns_none(self) -> None:
        """Zero close price returns None (cannot fill)."""
        portfolio = PortfolioTracker(cash=10_000.0)
        fill = simulate_market_fill(
            exchange="kraken",
            instrument="BTC-USD",
            side="buy",
            close_price=0.0,
            fill_at=NOW,
            portfolio=portfolio,
        )
        assert fill is None

    def test_negative_close_price_returns_none(self) -> None:
        """Negative close price returns None."""
        portfolio = PortfolioTracker(cash=10_000.0)
        fill = simulate_market_fill(
            exchange="kraken",
            instrument="BTC-USD",
            side="buy",
            close_price=-100.0,
            fill_at=NOW,
            portfolio=portfolio,
        )
        assert fill is None

    def test_zero_strength_returns_none(self) -> None:
        """Zero signal strength returns None (no allocation)."""
        portfolio = PortfolioTracker(cash=10_000.0)
        fill = simulate_market_fill(
            exchange="kraken",
            instrument="BTC-USD",
            side="buy",
            close_price=100.0,
            fill_at=NOW,
            portfolio=portfolio,
            signal_strength=0.0,
        )
        assert fill is None

    def test_sell_no_position_returns_none(self) -> None:
        """Sell with no position returns None."""
        portfolio = PortfolioTracker(cash=10_000.0)
        fill = simulate_market_fill(
            exchange="kraken",
            instrument="BTC-USD",
            side="sell",
            close_price=100.0,
            fill_at=NOW,
            portfolio=portfolio,
        )
        assert fill is None

    def test_half_strength_buy(self) -> None:
        """Signal strength 0.5 invests half the cash."""
        portfolio = PortfolioTracker(cash=10_000.0)
        fill = simulate_market_fill(
            exchange="kraken",
            instrument="BTC-USD",
            side="buy",
            close_price=100.0,
            fill_at=NOW,
            portfolio=portfolio,
            signal_strength=0.5,
        )
        assert fill is not None
        assert fill.size == pytest.approx(50.0)

    def test_default_strength_is_one(self) -> None:
        """No signal_strength defaults to 1.0 (full cash)."""
        portfolio = PortfolioTracker(cash=10_000.0)
        fill = simulate_market_fill(
            exchange="kraken",
            instrument="BTC-USD",
            side="buy",
            close_price=100.0,
            fill_at=NOW,
            portfolio=portfolio,
        )
        assert fill is not None
        assert fill.size == pytest.approx(100.0)

    def test_signal_metadata_preserved(self) -> None:
        """Signal reason and strength are preserved in fill."""
        portfolio = PortfolioTracker(cash=10_000.0)
        fill = simulate_market_fill(
            exchange="kraken",
            instrument="BTC-USD",
            side="buy",
            close_price=100.0,
            fill_at=NOW,
            portfolio=portfolio,
            signal_strength=0.8,
            signal_reason="macd_cross",
        )
        assert fill is not None
        assert fill.signal_reason == "macd_cross"
        assert fill.signal_strength == pytest.approx(0.8)

    def test_zero_cash_buy_returns_none(self) -> None:
        """Buy with zero cash returns None."""
        portfolio = PortfolioTracker(cash=0.0)
        fill = simulate_market_fill(
            exchange="kraken",
            instrument="BTC-USD",
            side="buy",
            close_price=100.0,
            fill_at=NOW,
            portfolio=portfolio,
        )
        assert fill is None
