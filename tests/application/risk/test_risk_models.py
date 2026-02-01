"""Unit tests for risk management models."""

import pytest

from snapper.application.risk.models import RiskConfigModel
from snapper.application.risk.models import RiskEvaluator


def test_size_position() -> None:
    """Verify position sizing returns positive size for valid inputs.

    Given a RiskEvaluator with 1% risk per trade,
    When size_position is called with equity=10000 and price=100,
    Then returned size is greater than zero.
    """
    r = RiskEvaluator(RiskConfigModel(r_per_trade=0.01))
    size = r.size_position(equity=10000, price=100)
    assert size > 0


def test_stop_pct_and_drawdown_gate() -> None:
    """Verify stop percentage and drawdown gate calculations.

    Given a RiskEvaluator with 1% risk, 10% max drawdown, 2x stop multiple,
    When stop_pct is called, Then it returns 2% (0.02).
    When can_open_new_trade with 12% drawdown (880/1000), Then returns False.
    When can_open_new_trade with 5% drawdown (950/1000), Then returns True.
    """
    r = RiskEvaluator(RiskConfigModel(r_per_trade=0.01, max_drawdown=0.1, stop_r_multiple=2.0))
    assert abs(r.stop_pct() - 0.02) < 1e-9
    assert r.can_open_new_trade(equity=880, peak_equity=1000) is False
    assert r.can_open_new_trade(equity=950, peak_equity=1000) is True


def test_can_open_new_trade_zero_peak() -> None:
    """Verify drawdown gate handles zero/negative peak equity.

    Given a RiskEvaluator with 10% max drawdown,
    When can_open_new_trade is called with zero or negative peak_equity,
    Then returns True (allows trading to prevent division errors).
    """
    r = RiskEvaluator(RiskConfigModel(max_drawdown=0.1))
    assert r.can_open_new_trade(equity=100, peak_equity=0) is True
    assert r.can_open_new_trade(equity=100, peak_equity=-10) is True


def test_cap_size_by_leverage() -> None:
    """Verify leverage capping limits position size.

    Given a RiskEvaluator with max_leverage=1.0,
    When cap_size_by_leverage with 600 current notional, 1000 equity, price=100, desired=10,
    Then returns at most 4.0 (remaining capacity: 1000-600=400, 400/100=4).
    """
    r = RiskEvaluator(RiskConfigModel(max_leverage=1.0))
    capped = r.cap_size_by_leverage(current_notional=600, equity=1000, price=100, desired_size=10)
    assert capped <= 4.0 + 1e-9


def test_round_down_to_step() -> None:
    """Verify round_down_to_step handles various step sizes.

    Given different values and step sizes,
    When round_down_to_step is called,
    Then value is rounded down to nearest step (edge cases: step=0 or negative).
    """
    assert RiskEvaluator.round_down_to_step(10.5, 1.0) == pytest.approx(10.0)
    assert RiskEvaluator.round_down_to_step(10.9, 1.0) == pytest.approx(10.0)
    assert RiskEvaluator.round_down_to_step(15.7, 5.0) == pytest.approx(15.0)
    assert RiskEvaluator.round_down_to_step(10.0, 0.0) == pytest.approx(10.0)
    assert RiskEvaluator.round_down_to_step(-5.0, 0.0) == pytest.approx(0.0)
    assert RiskEvaluator.round_down_to_step(10.0, -1.0) == pytest.approx(10.0)


def test_round_size() -> None:
    """Verify round_size applies lot and tick size constraints.

    Given a RiskEvaluator and various size/lot configurations,
    When round_size is called,
    Then size is rounded down to lot_size (returns 0 if below lot_size).
    """
    r = RiskEvaluator(RiskConfigModel())
    rounded = r.round_size(desired_size=10.7, lot_size=1.0, price=100.0, tick_size=0.01)
    assert rounded == pytest.approx(10.0)
    rounded = r.round_size(desired_size=0.5, lot_size=1.0, price=100.0, tick_size=0.01)
    assert rounded == pytest.approx(0.0)
    rounded = r.round_size(desired_size=10.5, lot_size=1.0, price=100.0, tick_size=0.0)
    assert rounded == pytest.approx(10.0)


def test_risk_model_core_behaviour() -> None:
    """Verify complete RiskEvaluator workflow with all constraints.

    Given a RiskConfigModel with 1% risk, 0.5 leverage, 10% max_dd, 2x stop,
    When various risk operations are performed,
    Then stop_pct=2%, position size ~5, drawdown gate works, leverage caps to 0.
    """
    cfg = RiskConfigModel(r_per_trade=0.01, max_leverage=0.5, max_drawdown=0.1, stop_r_multiple=2.0)
    r = RiskEvaluator(cfg)
    assert abs(r.stop_pct() - 0.02) < 1e-9
    size = r.size_position(equity=1000, price=100)
    assert 4.99 < size < 5.01
    assert r.can_open_new_trade(equity=950, peak_equity=1000) is True
    assert r.can_open_new_trade(equity=800, peak_equity=1000) is False
    capped = r.cap_size_by_leverage(current_notional=500, equity=1000, price=100, desired_size=5)
    assert capped == pytest.approx(0.0)
