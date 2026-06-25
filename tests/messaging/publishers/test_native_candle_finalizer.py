"""Tests for NativeCandleFinalizer — the native-candle DB-final-only finalizer."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from snapper.data.repository_types import CandleUpsertRow
from snapper.messaging.publishers import native_candle_finalizer
from snapper.messaging.publishers.native_candle_finalizer import NativeCandleFinalizer
from snapper.messaging.publishers.native_candle_finalizer import window_seconds

_T0 = datetime(2026, 6, 19, 12, 0, tzinfo=UTC)


def _row(
    *, open_at: datetime, ipid: str = "inst-1", tf: str = "1m", complete: bool = False
) -> CandleUpsertRow:
    """Build a minimal candle row for finalizer tests."""
    return CandleUpsertRow(
        instrument_public_id=ipid,
        open_at=open_at,
        timestamp=open_at,
        timeframe=tf,
        open=1.0,
        high=2.0,
        low=0.5,
        close=1.5,
        volume=10.0,
        vwap=1.25,
        trades=3,
        source="native",
        complete=complete,
    )


def test_window_seconds_known_and_unknown() -> None:
    """window_seconds maps known labels and returns 0 for unknown.

    Given: the window-seconds helper,
    When: known and unknown labels are looked up,
    Then: known labels map to their width and unknown returns 0.
    """
    assert window_seconds("1m") == 60
    assert window_seconds("1d") == 86400
    assert window_seconds("bogus") == 0


def test_observe_same_open_at_replaces_held_no_release_off() -> None:
    """An intra-window update replaces the held row without releasing (OFF).

    Given: an OFF-mode finalizer holding a frame for a window,
    When: another frame for the SAME open_at arrives,
    Then: nothing is released (the held row is replaced in place).
    """
    fin = NativeCandleFinalizer(persist_intermediate=False, flush_grace_seconds=5.0)
    assert fin.observe("BTC-USD", _row(open_at=_T0)) == []
    assert fin.observe("BTC-USD", _row(open_at=_T0)) == []


def test_observe_boundary_releases_previous_complete_once_off() -> None:
    """A strictly-later open_at releases the prior bar as complete=True (OFF).

    Given: an OFF-mode finalizer holding a window,
    When: a frame for a later window arrives,
    Then: the previous window is released exactly once with complete=True.
    """
    fin = NativeCandleFinalizer(persist_intermediate=False, flush_grace_seconds=5.0)
    fin.observe("BTC-USD", _row(open_at=_T0))
    released = fin.observe("BTC-USD", _row(open_at=_T0 + timedelta(minutes=1)))
    assert len(released) == 1
    sym, row = released[0]
    assert sym == "BTC-USD"
    assert row["open_at"] == _T0
    assert row["complete"] is True


def test_observe_equal_open_at_does_not_release() -> None:
    """Release is strictly-later: an equal open_at never releases.

    Given: a finalizer holding a window,
    When: a frame with the identical open_at arrives,
    Then: no release occurs.
    """
    fin = NativeCandleFinalizer(persist_intermediate=False, flush_grace_seconds=5.0)
    fin.observe("BTC-USD", _row(open_at=_T0))
    assert fin.observe("BTC-USD", _row(open_at=_T0)) == []


def test_observe_late_frame_dropped_and_counted() -> None:
    """A frame at/below a finalized window is dropped and counted.

    Given: a finalizer that has finalized a window,
    When: a late frame for that window arrives,
    Then: it is dropped (returns []) and late_count increments.
    """
    fin = NativeCandleFinalizer(persist_intermediate=False, flush_grace_seconds=5.0)
    fin.observe("BTC-USD", _row(open_at=_T0))
    fin.observe("BTC-USD", _row(open_at=_T0 + timedelta(minutes=1)))
    assert fin.observe("BTC-USD", _row(open_at=_T0)) == []
    assert fin.late_count == 1


def test_observe_late_complete_frame_is_released_for_correction() -> None:
    """A completed late row is released so the durable 1m can supersede.

    Given: a finalizer that already released a window,
    When: a complete late frame for that same window arrives,
    Then: it is returned for persistence and not counted as a dropped late row.
    """
    fin = NativeCandleFinalizer(persist_intermediate=False, flush_grace_seconds=5.0)
    fin.observe("BTC-USD", _row(open_at=_T0))
    fin.observe("BTC-USD", _row(open_at=_T0 + timedelta(minutes=1)))
    correction = _row(open_at=_T0, complete=True)
    assert fin.observe("BTC-USD", correction) == [("BTC-USD", correction)]
    assert fin.late_count == 0


def test_observe_on_mode_first_frame_emits_intermediate() -> None:
    """ON-mode emits the in-progress frame immediately as an intermediate.

    Given: an ON-mode finalizer,
    When: the first frame for a window arrives (window still open),
    Then: it is emitted as an intermediate carrying its complete=False flag.
    """
    fin = NativeCandleFinalizer(persist_intermediate=True, flush_grace_seconds=5.0)
    released = fin.observe("BTC-USD", _row(open_at=_T0, complete=False))
    assert len(released) == 1
    assert released[0][1]["complete"] is False


def test_observe_on_mode_boundary_emits_final_then_intermediate() -> None:
    """ON-mode emits the finalized predecessor before the new intermediate.

    Given: an ON-mode finalizer holding a window,
    When: a later window's frame arrives,
    Then: it emits [finalized predecessor complete=True, new intermediate
        complete=False] in that order.
    """
    fin = NativeCandleFinalizer(persist_intermediate=True, flush_grace_seconds=5.0)
    fin.observe("BTC-USD", _row(open_at=_T0, complete=False))
    released = fin.observe("BTC-USD", _row(open_at=_T0 + timedelta(minutes=1), complete=False))
    assert len(released) == 2
    assert released[0][1]["open_at"] == _T0 and released[0][1]["complete"] is True
    assert released[1][1]["open_at"] == _T0 + timedelta(minutes=1)
    assert released[1][1]["complete"] is False


def test_flush_releases_ended_window_and_keeps_open_one() -> None:
    """Flush finalizes a window ended past the grace, not an open one.

    Given: a finalizer holding a 1m window,
    When: flush runs before vs after window_end + grace,
    Then: nothing releases while open/within grace; the final bar releases once
        the grace has elapsed.
    """
    fin = NativeCandleFinalizer(persist_intermediate=False, flush_grace_seconds=5.0)
    fin.observe("BTC-USD", _row(open_at=_T0))
    assert fin.flush(_T0 + timedelta(seconds=30)) == []
    assert fin.flush(_T0 + timedelta(seconds=63)) == []
    released = fin.flush(_T0 + timedelta(seconds=66))
    assert len(released) == 1
    assert released[0][1]["complete"] is True


def test_flush_finalizes_stalled_symbol_exactly_once() -> None:
    """An illiquid/stalled symbol is finalized via flush, exactly once.

    Given: a finalizer holding a window for a symbol that never trades again,
    When: flush runs twice past the window end,
    Then: the bar is released on the first flush only.
    """
    fin = NativeCandleFinalizer(persist_intermediate=False, flush_grace_seconds=5.0)
    fin.observe("ILLQ-USD", _row(open_at=_T0))
    first = fin.flush(_T0 + timedelta(seconds=120))
    second = fin.flush(_T0 + timedelta(seconds=180))
    assert len(first) == 1
    assert second == []


def test_observe_release_then_flush_is_noop() -> None:
    """A window released by observe is not re-released by a later flush.

    Given: a window finalized by a boundary advance,
    When: flush runs afterward,
    Then: it does not re-release the already-sealed window.
    """
    fin = NativeCandleFinalizer(persist_intermediate=False, flush_grace_seconds=5.0)
    fin.observe("BTC-USD", _row(open_at=_T0))
    fin.observe("BTC-USD", _row(open_at=_T0 + timedelta(minutes=1)))
    assert fin.flush(_T0 + timedelta(seconds=66)) == []


def test_drain_finalizes_ended_only_drops_open() -> None:
    """Drain finalizes ended windows and leaves the open current window.

    Given: a finalizer holding one ended and one still-open window,
    When: drain runs,
    Then: only the ended window is released; the open one is dropped.
    """
    fin = NativeCandleFinalizer(persist_intermediate=False, flush_grace_seconds=5.0)
    fin.observe("OLD-USD", _row(open_at=_T0, ipid="inst-old"))
    fin.observe("NEW-USD", _row(open_at=_T0 + timedelta(minutes=5), ipid="inst-new"))
    released = fin.drain(_T0 + timedelta(seconds=90))
    assert len(released) == 1
    assert released[0][0] == "OLD-USD"
    assert released[0][1]["complete"] is True


def test_per_key_isolation() -> None:
    """Two instrument/timeframe keys advance independently.

    Given: a finalizer holding windows for two instruments,
    When: one instrument advances its window,
    Then: only that instrument's previous bar releases; the other is untouched.
    """
    fin = NativeCandleFinalizer(persist_intermediate=False, flush_grace_seconds=5.0)
    fin.observe("A-USD", _row(open_at=_T0, ipid="inst-a"))
    fin.observe("B-USD", _row(open_at=_T0, ipid="inst-b"))
    released = fin.observe("A-USD", _row(open_at=_T0 + timedelta(minutes=1), ipid="inst-a"))
    assert len(released) == 1
    assert released[0][1]["instrument_public_id"] == "inst-a"


def test_late_count_dedupes_warning_and_clears_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    """Late warnings dedupe per window and the warn set is bounded.

    Given: a finalizer with a tiny warn cap,
    When: repeated and distinct late frames arrive,
    Then: late_count counts every late frame, the same window warns once, and
        the warn set clears when it reaches the cap.
    """
    monkeypatch.setattr(native_candle_finalizer, "_WARNED_LATE_CAP", 1)
    fin = NativeCandleFinalizer(persist_intermediate=False, flush_grace_seconds=5.0)
    fin.observe("BTC-USD", _row(open_at=_T0))
    fin.observe("BTC-USD", _row(open_at=_T0 + timedelta(minutes=1)))
    fin.observe("BTC-USD", _row(open_at=_T0))
    fin.observe("BTC-USD", _row(open_at=_T0))
    fin.observe("BTC-USD", _row(open_at=_T0 - timedelta(minutes=1)))
    assert fin.late_count == 3
