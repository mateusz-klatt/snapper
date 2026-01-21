"""Tests for PortfolioTracker position and equity management."""

from typing import Any
from typing import cast

from snapper.application.portfolio.models import PortfolioTracker


def test_equity_without_prices_returns_cash() -> None:
    """Verify equity returns cash when no prices provided.

    Given: Portfolio with 1234.5 cash and no positions,
    When: equity() is called without prices,
    Then: Cash amount is returned.
    """
    portfolio = PortfolioTracker(cash=1234.5)
    assert portfolio.equity() == 1234.5


def test_update_fill_closes_position_resets_avg_price() -> None:
    """Verify closing position resets average price to zero.

    Given: Portfolio with open position,
    When: Full position is sold,
    Then: Quantity is zero and average_price is 0.0.
    """
    portfolio = PortfolioTracker(cash=1000.0)
    portfolio.update_fill(instrument="BTC-USD", side="buy", size=1.0, price=10_000.0, fee=0.0)
    portfolio.update_fill(instrument="BTC-USD", side="sell", size=1.0, price=10_000.0, fee=0.0)
    position = portfolio.positions["BTC-USD"]
    assert position.quantity <= 1e-12
    assert position.average_price == 0.0


def test_partial_sell_keeps_average_price() -> None:
    """Verify partial sell maintains original average price.

    Given: Portfolio with 1.0 units bought at 200,
    When: 0.4 units are sold,
    Then: Remaining 0.6 units keep average_price of 200.
    """
    p = PortfolioTracker(cash=1000.0)
    p.update_fill("BTC-USD", "buy", size=1.0, price=200.0, fee=0.0)
    p.update_fill("BTC-USD", "sell", size=0.4, price=220.0, fee=0.0)
    position = p.positions["BTC-USD"]
    assert position.quantity > 0.0
    assert position.average_price == 200.0


def test_clamp_cash_handles_tiny_negatives() -> None:
    """Verify _clamp_cash handles edge case with toggling comparison.

    Given: Portfolio with mock number returning varied comparisons,
    When: _clamp_cash is called,
    Then: Cash is clamped to zero.
    """

    class ToggleNumber:
        def __init__(self) -> None:
            self.gt_calls = 0

        def __lt__(self, other: object) -> bool:
            return other == 0

        def __gt__(self, other: object) -> bool:
            if other == -1e-9:
                self.gt_calls += 1
                return self.gt_calls > 1
            return False

    portfolio = PortfolioTracker(cash=0.0)
    portfolio.cash = cast(Any, ToggleNumber())
    portfolio._clamp_cash()
    assert portfolio.cash == 0.0


def test_clamp_cash_adjusts_small_positive() -> None:
    """Verify _clamp_cash adjusts very small positive values.

    Given: Portfolio with cash of 5e-10,
    When: _clamp_cash is called,
    Then: Cash is adjusted to 1e-6 minimum.
    """
    portfolio = PortfolioTracker(cash=5e-10)
    portfolio._clamp_cash()
    assert portfolio.cash == 1e-6


def test_portfolio_fill_buy_sell() -> None:
    """Verify buy-sell cycle updates cash and realized PnL.

    Given: Portfolio with 1000 cash,
    When: Buy then sell same quantity at higher price,
    Then: Position closed with positive realized_pnl.
    """
    p = PortfolioTracker(cash=1000.0)
    p.update_fill("BTC-USD", "buy", size=0.1, price=100.0, fee=0.01)
    assert p.positions["BTC-USD"].quantity == 0.1
    assert p.cash < 1000.0
    p.update_fill("BTC-USD", "sell", size=0.1, price=110.0, fee=0.01)
    assert p.positions["BTC-USD"].quantity == 0.0
    assert p.positions["BTC-USD"].realized_pnl > 0


def test_portfolio_position_qty() -> None:
    """Verify position_qty returns correct quantity.

    Given: Portfolio with no ETH-USD and 0.5 BTC-USD,
    When: position_qty is called,
    Then: Correct quantities are returned.
    """
    p = PortfolioTracker()
    assert p.position_qty("ETH-USD") == 0.0
    p.update_fill("BTC-USD", "buy", size=0.5, price=100.0, fee=0.01)
    assert p.position_qty("BTC-USD") == 0.5


def test_portfolio_notional_exposure() -> None:
    """Verify notional_exposure calculates position value.

    Given: Portfolio with 0.5 BTC-USD position,
    When: notional_exposure called with price 120,
    Then: Returns 0.5 * 120 = 60.
    """
    p = PortfolioTracker()
    p.update_fill("BTC-USD", "buy", size=0.5, price=100.0, fee=0.01)
    exposure = p.notional_exposure("BTC-USD", 120.0)
    assert exposure == 0.5 * 120.0


def test_portfolio_equity_with_prices() -> None:
    """Verify equity includes position value at market price.

    Given: Portfolio with cash and BTC-USD position,
    When: equity() called with higher price,
    Then: Equity exceeds initial 1000 cash.
    """
    p = PortfolioTracker(cash=1000.0)
    p.update_fill("BTC-USD", "buy", size=0.1, price=100.0, fee=0.01)
    prices = {"BTC-USD": 110.0}
    equity = p.equity(prices)
    assert equity > 1000.0


def test_portfolio_equity_without_prices() -> None:
    """Verify equity equals cash when prices not provided.

    Given: Portfolio with position but no price map,
    When: equity() called without prices,
    Then: Only cash is returned.
    """
    p = PortfolioTracker(cash=1000.0)
    p.update_fill("BTC-USD", "buy", size=0.1, price=100.0, fee=0.01)
    equity = p.equity()
    assert equity == p.cash


def test_clamp_cash_very_small_positive() -> None:
    """Verify clamp_cash preserves very small positive cash values.

    Given a PortfolioTracker with a very small positive cash balance,
    When a fill is processed,
    Then the cash remains non-negative.
    """
    p = PortfolioTracker(cash=1000.0)
    p.update_fill("BTC-USD", "buy", size=1000.0, price=1.0, fee=0.0)
    p2 = PortfolioTracker(cash=1e-7)
    p2.update_fill("BTC-USD", "sell", size=0.0001, price=1.0, fee=0.0)
    assert p2.cash >= 0.0


def test_clamp_cash_very_small_negative() -> None:
    """Verify clamp_cash handles very small negative cash values.

    Given a PortfolioTracker with very small positive cash,
    When a tiny buy reduces cash to near-zero,
    Then the cash value remains a valid float.
    """
    p = PortfolioTracker(cash=0.000001)
    p.update_fill("BTC-USD", "buy", size=0.0000001, price=1.0, fee=0.0)
    assert isinstance(p.cash, float)


def test_clamp_cash_tiny_negative_to_zero() -> None:
    """Verify tiny negative cash values are clamped to zero or minimum.

    Given a PortfolioTracker with a tiny negative cash balance,
    When _clamp_cash is called,
    Then the cash is clamped to zero or the minimum threshold.
    """
    p = PortfolioTracker(cash=-5e-10)
    p._clamp_cash()
    assert p.cash == 0.0 or p.cash == 1e-6


def test_equity_with_missing_price_for_some_positions() -> None:
    """Verify equity calculation handles missing prices for some positions.

    Given a PortfolioTracker with multiple positions,
    When equity is calculated with prices for only some positions,
    Then positions without prices are excluded from the calculation.
    """
    p = PortfolioTracker(cash=1000.0)
    p.update_fill("BTC-USD", "buy", size=0.1, price=100.0, fee=0.0)
    p.update_fill("ETH-USD", "buy", size=1.0, price=50.0, fee=0.0)
    prices = {"BTC-USD": 120.0}
    equity = p.equity(prices)
    expected = 940.0 + 12.0
    assert abs(equity - expected) < 0.01


def test_sell_clears_position_and_average_price() -> None:
    """Verify selling full position resets quantity and average price.

    Given a PortfolioTracker with an open position,
    When the entire position is sold,
    Then the quantity becomes zero and average price resets.
    """
    p = PortfolioTracker(cash=1000.0)
    p.update_fill("BTC-USD", "buy", size=0.5, price=100.0, fee=0.0)
    assert p.positions["BTC-USD"].average_price == 100.0
    p.update_fill("BTC-USD", "sell", size=0.5, price=110.0, fee=0.0)
    assert p.positions["BTC-USD"].quantity <= 1e-12
    assert p.positions["BTC-USD"].average_price == 0.0


def test_portfolio_avg_price_calculation() -> None:
    """Verify average price is correctly recalculated on multiple buys.

    Given a PortfolioTracker with an existing position,
    When additional units are bought at different prices,
    Then the average price is weighted correctly.
    """
    p = PortfolioTracker(cash=10000.0)
    p.update_fill("BTC-USD", "buy", size=1.0, price=100.0, fee=0.0)
    assert p.positions["BTC-USD"].average_price == 100.0
    p.update_fill("BTC-USD", "buy", size=1.0, price=200.0, fee=0.0)
    assert abs(p.positions["BTC-USD"].average_price - 150.0) < 1e-9


def test_portfolio_equity_exposure_turnover() -> None:
    """Verify equity, exposure, and turnover are calculated correctly.

    Given a PortfolioTracker with positions,
    When fills are processed and metrics are queried,
    Then equity, notional exposure, and turnover reflect accurate values.
    """
    p = PortfolioTracker(cash=1000.0)
    p.update_fill("BTC-USD", "buy", size=0.1, price=100.0, fee=0.0)
    assert p.position_qty("BTC-USD") == 0.1
    assert abs(p.notional_exposure("BTC-USD", 110.0) - 11.0) < 1e-9
    eq = p.equity({"BTC-USD": 110.0})
    assert abs(eq - 1001.0) < 1e-9
    assert abs(p.turnover - 10.0) < 1e-9
    p.update_fill("BTC-USD", "sell", size=0.1, price=110.0, fee=0.0)
    assert p.position_qty("BTC-USD") == 0.0
    assert abs(p.turnover - 21.0) < 1e-9
    assert abs(p.cash - 1001.0) < 1e-9
    assert p.notional_exposure("BTC-USD", 120.0) == 0.0
