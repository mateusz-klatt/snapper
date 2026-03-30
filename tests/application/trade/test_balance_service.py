"""Tests for BalanceService — balance projection service."""

from snapper.application.trade.balance_service import BalanceService


def test_default_projection() -> None:
    """New shard starts with zero equity, cash, and exposure.

    Given: a fresh BalanceService with no projections,
    When: equity, cash, and exposure are queried for a non-existent shard,
    Then: all values are 0.0.
    """
    svc = BalanceService()
    assert svc.get_equity("nonexistent") == 0.0
    assert svc.get_cash("nonexistent") == 0.0
    assert svc.get_exposure("nonexistent") == 0.0


def test_on_position_changed() -> None:
    """Position change updates the balance projection for the shard.

    Given: a fresh BalanceService with no projections,
    When: on_position_changed is called with cash=5000 and peak_equity=10000,
    Then: get_cash returns 5000 and get_peak_equity returns 10000.
    """
    svc = BalanceService()
    svc.on_position_changed(
        shard_key="kraken.BTC-USD.live",
        position_qty=0.5,
        entry_price=50000.0,
        cash=5000.0,
        peak_equity=10000.0,
        realized_pnl=0.0,
    )
    assert svc.get_cash("kraken.BTC-USD.live") == 5000.0
    assert svc.get_peak_equity("kraken.BTC-USD.live") == 10000.0


def test_on_mark_price_updates_equity() -> None:
    """Mark price update recalculates equity as cash + qty * mark.

    Given: a BalanceService with a 1.0 BTC position and cash=-50000 (bought at 50000),
    When: on_mark_price is called with 55000,
    Then: equity is -50000 + 1.0*55000 = 5000 and exposure is 55000.
    """
    svc = BalanceService()
    svc.on_position_changed(
        shard_key="kraken.BTC-USD.live",
        position_qty=1.0,
        entry_price=50000.0,
        cash=-50000.0,
        peak_equity=50000.0,
        realized_pnl=0.0,
    )
    svc.on_mark_price("kraken.BTC-USD.live", 55000.0)
    assert svc.get_equity("kraken.BTC-USD.live") == 5000.0
    assert svc.get_exposure("kraken.BTC-USD.live") == 55000.0


def test_drawdown_calculation() -> None:
    """Drawdown from peak equity is computed correctly after price drop.

    Given: a BalanceService with a position marked at 55000 establishing a peak,
    When: on_mark_price is called with a lower price of 45000,
    Then: drawdown is positive and peak equity remains at the prior high.
    """
    svc = BalanceService()
    svc.on_position_changed(
        shard_key="kraken.BTC-USD.live",
        position_qty=1.0,
        entry_price=50000.0,
        cash=0.0,
        peak_equity=10000.0,
        realized_pnl=0.0,
    )
    svc.on_mark_price("kraken.BTC-USD.live", 55000.0)
    peak_with_mark = svc.get_peak_equity("kraken.BTC-USD.live")
    svc.on_mark_price("kraken.BTC-USD.live", 45000.0)
    dd = svc.get_drawdown("kraken.BTC-USD.live")
    assert dd > 0.0
    assert svc.get_peak_equity("kraken.BTC-USD.live") == peak_with_mark


def test_restore_from_checkpoint() -> None:
    """Balance projection is restored from a persisted checkpoint.

    Given: a fresh BalanceService with no state,
    When: restore_from_checkpoint is called with cash, position, peak_equity, and realized_pnl,
    Then: all projection fields reflect the restored values.
    """
    svc = BalanceService()
    svc.restore_from_checkpoint(
        shard_key="kraken.BTC-USD.live",
        cash=5000.0,
        position_qty=0.5,
        entry_price=50000.0,
        peak_equity=12000.0,
        realized_pnl=500.0,
    )
    assert svc.get_cash("kraken.BTC-USD.live") == 5000.0
    assert svc.get_peak_equity("kraken.BTC-USD.live") == 12000.0
    proj = svc.get_projection("kraken.BTC-USD.live")
    assert proj.position_qty == 0.5
    assert proj.realized_pnl == 500.0


def test_unrealized_pnl_without_mark() -> None:
    """Unrealized PnL is zero when no mark price has been set.

    Given: a BalanceService with a 1.0 BTC position at 50000 but no mark price,
    When: get_projection is called for that shard,
    Then: unrealized_pnl is 0.0 and equity is 0.0.
    """
    svc = BalanceService()
    svc.on_position_changed(
        shard_key="kraken.BTC-USD.live",
        position_qty=1.0,
        entry_price=50000.0,
        cash=0.0,
        peak_equity=10000.0,
        realized_pnl=0.0,
    )
    proj = svc.get_projection("kraken.BTC-USD.live")
    assert proj.unrealized_pnl == 0.0
    assert proj.equity == 0.0


def test_unrealized_pnl_with_mark() -> None:
    """Unrealized PnL is computed when mark price, entry price, and position are all set.

    Given: a BalanceService with 1.0 BTC at entry=50000 and mark=55000,
    When: unrealized_pnl property is accessed,
    Then: unrealized_pnl is 5000 (1.0 * (55000 - 50000)).
    """
    svc = BalanceService()
    svc.on_position_changed(
        shard_key="kraken.BTC-USD.live",
        position_qty=1.0,
        entry_price=50000.0,
        cash=-50000.0,
        peak_equity=10000.0,
        realized_pnl=0.0,
    )
    svc.on_mark_price("kraken.BTC-USD.live", 55000.0)
    proj = svc.get_projection("kraken.BTC-USD.live")
    assert proj.unrealized_pnl == 5000.0


def test_drawdown_zero_peak() -> None:
    """Drawdown is zero when peak equity is zero or negative.

    Given: a BalanceService with a projection whose peak_equity is 0,
    When: drawdown is computed,
    Then: drawdown is 0.0 (division guard).
    """
    svc = BalanceService()
    proj = svc.get_projection("empty.shard")
    assert proj.drawdown == 0.0


def test_on_position_changed_updates_peak_with_mark() -> None:
    """Position change recalculates peak equity when mark price is set.

    Given: a BalanceService with mark_price=55000 and peak=1000,
    When: on_position_changed sets qty=1.0 with cash=-50000 (bought at 50000),
    Then: equity = -50000 + 1*55000 = 5000 exceeds peak so peak is updated.
    """
    svc = BalanceService()
    svc.on_mark_price("kraken.BTC-USD.live", 55000.0)
    svc.on_position_changed(
        shard_key="kraken.BTC-USD.live",
        position_qty=1.0,
        entry_price=50000.0,
        cash=-50000.0,
        peak_equity=1000.0,
        realized_pnl=0.0,
    )
    assert svc.get_peak_equity("kraken.BTC-USD.live") == 5000.0


def test_on_mark_price_does_not_lower_peak() -> None:
    """Mark price drop does not lower peak equity.

    Given: a BalanceService with peak=100000 and cash=-50000 with 1 BTC position,
    When: on_mark_price is called with 45000 producing equity=-5000,
    Then: peak equity remains at 100000.
    """
    svc = BalanceService()
    svc.on_position_changed(
        shard_key="kraken.BTC-USD.live",
        position_qty=1.0,
        entry_price=50000.0,
        cash=-50000.0,
        peak_equity=100000.0,
        realized_pnl=0.0,
    )
    svc.on_mark_price("kraken.BTC-USD.live", 45000.0)
    assert svc.get_peak_equity("kraken.BTC-USD.live") == 100000.0


def test_on_mark_price_raises_peak() -> None:
    """Mark price increase that pushes equity above peak updates peak.

    Given: a BalanceService with peak_equity=100 and 1 BTC with cash=-50000,
    When: on_mark_price is called with 51000 producing equity=1000,
    Then: peak equity is updated to 1000.
    """
    svc = BalanceService()
    svc.on_position_changed(
        shard_key="kraken.BTC-USD.live",
        position_qty=1.0,
        entry_price=50000.0,
        cash=-50000.0,
        peak_equity=100.0,
        realized_pnl=0.0,
    )
    svc.on_mark_price("kraken.BTC-USD.live", 51000.0)
    assert svc.get_peak_equity("kraken.BTC-USD.live") == 1000.0


def test_on_position_changed_with_mark_no_peak_update() -> None:
    """Position change with mark price set but equity below peak.

    Given: a BalanceService with mark_price=51000 and peak_equity=999999,
    When: on_position_changed sets position_qty=1.0 with cash=0 and entry=50000,
    Then: equity=1000 does not exceed peak so peak stays at 999999.
    """
    svc = BalanceService()
    svc.on_mark_price("kraken.BTC-USD.live", 51000.0)
    svc.on_position_changed(
        shard_key="kraken.BTC-USD.live",
        position_qty=1.0,
        entry_price=50000.0,
        cash=0.0,
        peak_equity=999999.0,
        realized_pnl=0.0,
    )
    assert svc.get_peak_equity("kraken.BTC-USD.live") == 999999.0
