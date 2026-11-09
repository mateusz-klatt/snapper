"""Carried valuation planning stays per gap, bounded and free of invented values."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

from snapper.application.portfolio.pnl_carry import FX_CARRY_READ_STEPS
from snapper.application.portfolio.pnl_carry import MARK_CARRY_BOUND
from snapper.application.portfolio.pnl_carry import CarriedMarkSource
from snapper.application.portfolio.pnl_carry import carried_mark_sources
from snapper.application.portfolio.pnl_carry import carry_serves
from snapper.application.portfolio.pnl_carry import fx_gap_anchors
from snapper.application.portfolio.pnl_carry import leading_mark_gaps
from snapper.application.portfolio.pnl_carry import read_windows

_BASE = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)
_INSTRUMENT = "instrument"


def _at(minutes: int) -> datetime:
    """Return a minute relative to the fixed base."""
    return _BASE + timedelta(minutes=minutes)


def test_read_windows_widen_without_overlap_and_stop_at_the_carry_bound() -> None:
    """Each window is new evidence and none touches the anchor's own bar.

    Given: The configured FX look-backs behind one anchor.
    When: The read windows are planned.
    Then: They are adjacent, end before the anchor's bar and reach exactly the bound.
    """
    windows = read_windows(_at(0), FX_CARRY_READ_STEPS)
    assert windows[0] == (_at(-61), _at(-2))
    for newer, older in zip(windows, windows[1:], strict=False):
        assert older[1] == newer[0] - timedelta(minutes=1)
    assert windows[-1][0] == _at(0) - timedelta(minutes=1) - FX_CARRY_READ_STEPS[-1]
    assert len(windows) == len(FX_CARRY_READ_STEPS)


def test_mark_sources_pick_the_latest_close_inside_the_bound() -> None:
    """A demanded minute uses its newest earlier close and never a later one.

    Given: Closes before and after one minute and a minute with nothing in bound.
    When: Sources are chosen.
    Then: The earlier newest close wins and the out-of-bound minute is absent.
    """
    bound_minutes = int(MARK_CARRY_BOUND.total_seconds() // 60)
    closes = {_INSTRUMENT: {_at(-10): 1.0, _at(-2): 2.0, _at(5): 3.0}}
    demand = {_INSTRUMENT: [_at(0), _at(bound_minutes - 2), _at(bound_minutes + 6)]}
    sources = carried_mark_sources(demand, closes)
    assert sources == {
        (_INSTRUMENT, _at(0)): CarriedMarkSource(2.0, _at(-2)),
        (_INSTRUMENT, _at(bound_minutes - 2)): CarriedMarkSource(3.0, _at(5)),
    }


def test_instrument_without_closes_has_no_source() -> None:
    """Demand alone never produces a value.

    Given: A demanded minute and no closes for that instrument.
    When: Sources are chosen.
    Then: Nothing is returned.
    """
    assert carried_mark_sources({_INSTRUMENT: [_at(0)]}, {}) == {}


def test_only_an_unserved_first_minute_needs_a_read() -> None:
    """A gap is searched only when loaded closes cannot serve its first minute.

    Given: One instrument already served and one whose only close is too old.
    When: Leading gaps are computed.
    Then: Only the second needs a read, anchored at its first demanded minute.
    """
    bound_minutes = int(MARK_CARRY_BOUND.total_seconds() // 60)
    demand = {"served": [_at(0), _at(1)], "stale": [_at(3), _at(4)]}
    closes = {"served": {_at(-1): 1.0}, "stale": {_at(3 - bound_minutes - 1): 1.0}}
    assert leading_mark_gaps(demand, closes) == {"stale": _at(3)}


def test_gap_anchors_cost_one_read_per_contiguous_unserved_run() -> None:
    """Dense gaps share one search while covered and blocked minutes need none.

    Given: Two unserved runs, a published minute, a blocked minute and a covered one.
    When: Anchors are planned with a one-minute bound.
    Then: Exactly the start of each run is returned.
    """
    published = [_at(10)]
    required = {_at(0), _at(1), _at(2), _at(5), _at(10), _at(11), _at(20), _at(21)}
    anchors = fx_gap_anchors(required, published, {_at(5)}, timedelta(minutes=1))
    assert anchors == [_at(0), _at(20)]


def test_covered_minute_splits_runs_so_later_gaps_are_still_searched() -> None:
    """A later gap after evidence gets its own search.

    Given: Unserved minutes separated by a minute served from loaded evidence.
    When: Anchors are planned.
    Then: Both sides anchor independently.
    """
    published = [_at(2)]
    required = {_at(0), _at(3), _at(10)}
    assert fx_gap_anchors(required, published, set(), timedelta(minutes=2)) == [_at(0), _at(10)]


def test_carry_serves_reports_reachability_inside_the_bound() -> None:
    """A later gap is skipped only when a real close can reach it.

    Given: One published minute.
    When: Minutes inside and beyond the bound are checked.
    Then: Only the reachable minute is served.
    """
    assert carry_serves([_at(0)], _at(5), timedelta(minutes=5))
    assert not carry_serves([_at(0)], _at(6), timedelta(minutes=5))
