"""Tests for the shared average-cost position accounting kernel.

Exercises :func:`classify_transition` and :func:`apply_fill` across every
lifecycle branch — open, scale, partial reduce, exact close, flip with
overshoot, both long and short — plus the defensive entry-None / zero-quantity
edges, so the kernel that backs both the live TradeService projection and the
P&L timeline reconstruction is fully pinned.
"""

import pytest

from snapper.application.portfolio.average_cost import PoolFillOutcome
from snapper.application.portfolio.average_cost import apply_fill
from snapper.application.portfolio.average_cost import classify_transition


class TestClassifyTransition:
    """Cover the pure cycle-transition classifier."""

    def test_flat_to_flat_is_none(self) -> None:
        """A position that stays flat needs no cycle boundary."""
        assert classify_transition(0.0, 0.0) is None

    def test_flat_to_long_opens(self) -> None:
        """Flat becoming long opens a cycle."""
        assert classify_transition(0.0, 1.5) == "open"

    def test_flat_to_short_opens(self) -> None:
        """Flat becoming short opens a cycle."""
        assert classify_transition(0.0, -2.0) == "open"

    def test_long_to_flat_closes(self) -> None:
        """A long returning to flat closes the cycle."""
        assert classify_transition(1.5, 0.0) == "close"

    def test_short_to_flat_closes(self) -> None:
        """A short returning to flat closes the cycle."""
        assert classify_transition(-2.0, 0.0) == "close"

    def test_long_to_short_flips(self) -> None:
        """A long crossing to short flips the cycle."""
        assert classify_transition(1.5, -2.0) == "flip"

    def test_short_to_long_flips(self) -> None:
        """A short crossing to long flips the cycle."""
        assert classify_transition(-2.0, 1.5) == "flip"

    def test_long_scale_up(self) -> None:
        """Growing a long absolute quantity scales up."""
        assert classify_transition(1.0, 2.0) == "scale_up"

    def test_short_scale_up(self) -> None:
        """Growing a short absolute quantity scales up."""
        assert classify_transition(-1.0, -2.0) == "scale_up"

    def test_long_scale_down_is_none(self) -> None:
        """Shrinking a long without closing needs no boundary."""
        assert classify_transition(2.0, 1.0) is None

    def test_short_scale_down_is_none(self) -> None:
        """Shrinking a short without closing needs no boundary."""
        assert classify_transition(-2.0, -1.0) is None

    def test_hold_is_none(self) -> None:
        """An unchanged quantity needs no boundary."""
        assert classify_transition(1.5, 1.5) is None

    def test_subepsilon_old_treated_as_flat(self) -> None:
        """A sub-epsilon prior quantity is flat, so the fill opens."""
        assert classify_transition(1e-13, 1.5) == "open"

    def test_subepsilon_new_treated_as_flat(self) -> None:
        """A sub-epsilon resulting quantity is flat, so the fill closes."""
        assert classify_transition(1.5, 1e-13) == "close"


class TestApplyFillIncreasing:
    """Cover position-increasing fills (open and same-direction adds)."""

    def test_open_from_flat_long(self) -> None:
        """A buy from flat opens a long at the fill price."""
        outcome = apply_fill(0.0, None, 2.0, 2.0, 100.0)
        assert outcome == PoolFillOutcome(
            position_qty=2.0,
            entry_price=100.0,
            realized_delta=0.0,
            closed_qty=0.0,
            added_qty=2.0,
            transition="open",
            opened_new_side=True,
        )

    def test_open_from_flat_short(self) -> None:
        """A sell from flat opens a short at the fill price."""
        outcome = apply_fill(0.0, None, -2.0, 2.0, 100.0)
        assert outcome.position_qty == -2.0
        assert outcome.entry_price == 100.0
        assert outcome.transition == "open"
        assert outcome.opened_new_side is True
        assert outcome.added_qty == 2.0

    def test_vwap_add_long(self) -> None:
        """Adding to a long recomputes the weighted-average entry."""
        outcome = apply_fill(2.0, 100.0, 2.0, 2.0, 110.0)
        assert outcome.position_qty == 4.0
        assert outcome.entry_price == pytest.approx(105.0)
        assert outcome.realized_delta == 0.0
        assert outcome.closed_qty == 0.0
        assert outcome.added_qty == 2.0
        assert outcome.transition == "scale_up"
        assert outcome.opened_new_side is False

    def test_vwap_add_short(self) -> None:
        """Adding to a short recomputes the weighted-average entry over abs qty."""
        outcome = apply_fill(-2.0, 100.0, -2.0, 2.0, 90.0)
        assert outcome.position_qty == -4.0
        assert outcome.entry_price == pytest.approx(95.0)
        assert outcome.transition == "scale_up"
        assert outcome.opened_new_side is False

    def test_increase_with_entry_but_zero_old_qty_resets(self) -> None:
        """An increase with a stale entry but zero prior quantity resets to fill.

        Guards the ``old_qty > 0`` arm of the VWAP condition: a non-None entry on
        a flat quantity must still take the reset path, not divide by the fill.
        """
        outcome = apply_fill(0.0, 100.0, 2.0, 2.0, 110.0)
        assert outcome.entry_price == 110.0
        assert outcome.opened_new_side is True
        assert outcome.position_qty == 2.0


class TestApplyFillDecreasing:
    """Cover position-decreasing fills (reduce, close, flip)."""

    def test_partial_reduce_long_realizes_profit(self) -> None:
        """Reducing a long realizes profit on the closed portion, entry held."""
        outcome = apply_fill(4.0, 105.0, -1.0, 1.0, 120.0)
        assert outcome.position_qty == 3.0
        assert outcome.entry_price == 105.0
        assert outcome.realized_delta == pytest.approx(15.0)
        assert outcome.closed_qty == 1.0
        assert outcome.added_qty == 0.0
        assert outcome.transition is None
        assert outcome.opened_new_side is False

    def test_exact_close_long_snaps_flat(self) -> None:
        """Closing a long exactly snaps to flat and clears the entry."""
        outcome = apply_fill(3.0, 105.0, -3.0, 3.0, 120.0)
        assert outcome.position_qty == 0.0
        assert outcome.entry_price is None
        assert outcome.realized_delta == pytest.approx(45.0)
        assert outcome.transition == "close"

    def test_flip_long_to_short_realizes_and_opens(self) -> None:
        """A long-to-short flip closes the long, realizes, and opens the short."""
        outcome = apply_fill(2.0, 100.0, -5.0, 5.0, 110.0)
        assert outcome.position_qty == -3.0
        assert outcome.entry_price == 110.0
        assert outcome.realized_delta == pytest.approx(20.0)
        assert outcome.closed_qty == 2.0
        assert outcome.added_qty == 3.0
        assert outcome.transition == "flip"
        assert outcome.opened_new_side is True

    def test_partial_reduce_short_realizes_profit(self) -> None:
        """Reducing a short realizes profit with the short-side sign."""
        outcome = apply_fill(-4.0, 100.0, 1.0, 1.0, 90.0)
        assert outcome.position_qty == -3.0
        assert outcome.entry_price == 100.0
        assert outcome.realized_delta == pytest.approx(10.0)
        assert outcome.transition is None
        assert outcome.opened_new_side is False

    def test_flip_short_to_long_realizes_and_opens(self) -> None:
        """A short-to-long flip closes the short, realizes, and opens the long."""
        outcome = apply_fill(-2.0, 100.0, 5.0, 5.0, 90.0)
        assert outcome.position_qty == 3.0
        assert outcome.entry_price == 90.0
        assert outcome.realized_delta == pytest.approx(20.0)
        assert outcome.closed_qty == 2.0
        assert outcome.added_qty == 3.0
        assert outcome.transition == "flip"
        assert outcome.opened_new_side is True

    def test_decrease_with_no_entry_skips_realization(self) -> None:
        """A decrease against a None entry realizes nothing and holds the entry.

        Guards the ``entry_price is not None`` arm of the realization condition.
        """
        outcome = apply_fill(2.0, None, -1.0, 1.0, 50.0)
        assert outcome.position_qty == 1.0
        assert outcome.entry_price is None
        assert outcome.realized_delta == 0.0
        assert outcome.transition is None

    def test_zero_size_decrease_is_noop(self) -> None:
        """A zero-quantity fill realizes nothing and leaves the pool unchanged.

        Guards the ``close_qty > 0`` arm: a zero fill routes through the
        decreasing branch with an empty close.
        """
        outcome = apply_fill(2.0, 100.0, 0.0, 0.0, 50.0)
        assert outcome.position_qty == 2.0
        assert outcome.entry_price == 100.0
        assert outcome.realized_delta == 0.0
        assert outcome.closed_qty == 0.0
        assert outcome.transition is None
        assert outcome.opened_new_side is False
