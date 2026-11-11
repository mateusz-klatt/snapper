"""Unit tests for the higher-timeframe candle synthesis aggregator.

Cover the Phase-1 contract: 1m finalization (exactly-once folding of the final
value despite many in-progress frames), UTC boundary alignment, emit-on-close
via a per-symbol watermark, OHLCV/VWAP roll-up, late-frame dropping, and the
restart seed path (including the cross-timeframe non-suppression watchpoint).
"""

from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.messaging.publishers.candle_aggregator import CandleAggregator
from snapper.messaging.publishers.candle_aggregator import LateCandleDrop
from snapper.messaging.publishers.candle_aggregator import SeededIncompleteWindow

_BASE = datetime(2026, 6, 14, tzinfo=UTC)


def _at(hour: int, minute: int, *, day: int = 14) -> datetime:
    """Return a UTC datetime on the test day at the given hour/minute."""
    return datetime(2026, 6, day, hour, minute, tzinfo=UTC)


def _at_s(hour: int, minute: int, second: int, *, day: int = 14) -> datetime:
    """Return a UTC datetime on the test day at the given hour/minute/second."""
    return datetime(2026, 6, day, hour, minute, second, tzinfo=UTC)


def _candle(
    symbol: str,
    begin: datetime,
    *,
    open_: float = 100.0,
    high: float = 100.0,
    low: float = 100.0,
    close: float = 100.0,
    volume: float = 1.0,
    vwap: float | None = None,
    trades: int = 1,
) -> CandleUpdate:
    """Build a 1m :class:`CandleUpdate` for the given minute boundary."""
    return CandleUpdate(
        symbol=symbol,
        open=open_,
        high=high,
        low=low,
        close=close,
        vwap=close if vwap is None else vwap,
        trades=trades,
        volume=volume,
        interval_begin=begin,
        interval=60,
    )


class TestFloor:
    """Canonical UTC boundary flooring per timeframe."""

    def test_floors_fixed_intervals_to_utc_boundary(self) -> None:
        """Given 14:37 UTC, when flooring, then each TF aligns to UTC."""
        ts = _at(14, 37)
        assert CandleAggregator._floor(ts, 300) == _at(14, 35)
        assert CandleAggregator._floor(ts, 900) == _at(14, 30)
        assert CandleAggregator._floor(ts, 1800) == _at(14, 30)
        assert CandleAggregator._floor(ts, 3600) == _at(14, 0)
        assert CandleAggregator._floor(ts, 14400) == _at(12, 0)

    def test_floors_day_to_midnight_utc(self) -> None:
        """Given any intraday UTC time, when flooring 1d, then 00:00 UTC."""
        assert CandleAggregator._floor(_at(14, 37), 86400) == _BASE


class TestFinalization:
    """In-progress frames fold the FINAL value exactly once."""

    def test_in_progress_frames_fold_once_no_double_count(self) -> None:
        """Two frames for one minute fold once with the final value, not both."""
        agg = CandleAggregator(["5m"])
        agg.fold(_candle("A", _at(10, 0), close=100.0, volume=1.0))
        agg.fold(_candle("A", _at(10, 0), close=105.0, high=110.0, volume=3.0))
        agg.fold(_candle("A", _at(10, 1), close=106.0, volume=2.0))
        agg.fold(_candle("A", _at(10, 5)))
        emitted = agg.fold(_candle("A", _at(10, 6)))
        assert len(emitted) == 1
        label, bar = emitted[0]
        assert label == "5m"
        assert bar.volume == 5.0

    def test_first_frame_is_held_not_emitted(self) -> None:
        """Given a single first frame, when folded, then nothing emits."""
        agg = CandleAggregator(["1h"])
        assert agg.fold(_candle("A", _at(10, 0))) == []

    def test_out_of_order_unfolded_minute_is_folded_not_dropped(self) -> None:
        """An out-of-order minute above the folded watermark still folds in."""
        agg = CandleAggregator(["5m"])
        agg.fold(
            _candle("A", _at(10, 0), open_=100.0, high=100.0, low=100.0, close=100.0, volume=10.0)
        )
        agg.fold(
            _candle("A", _at(10, 2), open_=102.0, high=102.0, low=102.0, close=102.0, volume=1.0)
        )
        agg.fold(
            _candle("A", _at(10, 1), open_=101.0, high=150.0, low=90.0, close=101.0, volume=20.0)
        )
        agg.fold(_candle("A", _at(10, 5)))
        emitted = agg.fold(_candle("A", _at(10, 6)))
        assert len(emitted) == 1
        _label, bar = emitted[0]
        assert bar.high == 150.0
        assert bar.low == 90.0
        assert bar.volume == 31.0
        assert agg.late_rolls_after_close == 0

    def test_late_warned_set_is_bounded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The warn-dedupe set clears at its cap so memory stays bounded."""
        monkeypatch.setattr("snapper.messaging.publishers.candle_aggregator._WARNED_LATE_CAP", 2)
        agg = CandleAggregator(["5m"])
        agg.fold(_candle("A", _at(10, 20)))
        agg.fold(_candle("A", _at(10, 21)))
        for minute in (0, 1, 2):
            agg.fold(_candle("A", _at(10, minute), close=1.0))
        assert agg.late_rolls_after_close == 3
        assert len(agg._warned_late) < 3

    def test_late_drop_signal_is_drainable(self) -> None:
        """Late drops expose a pure signal without changing fold output.

        Given: a finalized minute watermark,
        When: an older corrective 1m arrives,
        Then: the aggregator drops it from in-memory folding and exposes one
            drainable late-drop signal for the publisher repair path.
        """
        agg = CandleAggregator(["5m"])
        agg.fold(_candle("A", _at(10, 0)))
        agg.fold(_candle("A", _at(10, 1)))
        assert agg.fold(_candle("A", _at(10, 0), close=999.0)) == []
        assert agg.pop_late_drops() == [LateCandleDrop("A", _at(10, 0))]
        assert agg.pop_late_drops() == []

    def test_timeframes_and_closed_window_frontier_are_exposed(self) -> None:
        """The publisher can map late minutes and wait for sealed windows.

        Given: an aggregator configured for multiple higher timeframes,
        When: a 5m window is emitted,
        Then: the configured timeframes are visible and the emitted window is
            reported as closed while future windows are not.
        """
        agg = CandleAggregator(["5m", "1h"])
        assert agg.timeframes == ("5m", "1h")
        assert not agg.has_closed_window("A", "5m", _at(10, 0))
        for minute in range(7):
            agg.fold(_candle("A", _at(10, minute)))
        assert agg.has_closed_window("A", "5m", _at(10, 0))
        assert not agg.has_closed_window("A", "5m", _at(10, 5))


class TestRollup:
    """OHLCV and VWAP roll-up math."""

    def test_ohlcv_aggregates_over_window(self) -> None:
        """Given a full 5m window, when it closes, then OHLCV aggregates."""
        agg = CandleAggregator(["5m"])
        agg.fold(_candle("A", _at(10, 0), open_=10.0, high=12.0, low=9.0, close=11.0, volume=2.0))
        agg.fold(_candle("A", _at(10, 1), open_=11.0, high=15.0, low=10.0, close=14.0, volume=3.0))
        agg.fold(_candle("A", _at(10, 2), open_=14.0, high=14.5, low=8.0, close=9.0, volume=1.0))
        agg.fold(_candle("A", _at(10, 3), open_=9.0, high=13.0, low=8.5, close=12.0, volume=4.0))
        agg.fold(_candle("A", _at(10, 4), open_=12.0, high=12.5, low=11.0, close=11.5, volume=2.0))
        agg.fold(_candle("A", _at(10, 5)))
        emitted = agg.fold(_candle("A", _at(10, 6)))
        label, bar = emitted[0]
        assert label == "5m"
        assert bar.open == 10.0
        assert bar.close == 11.5
        assert bar.high == 15.0
        assert bar.low == 8.0
        assert bar.volume == 12.0
        assert bar.trades == 5
        assert bar.interval == 300
        assert bar.interval_begin == _at(10, 0)

    def test_vwap_is_volume_weighted(self) -> None:
        """Given two minutes, when closed, then VWAP is volume-weighted."""
        agg = CandleAggregator(["5m"])
        agg.fold(_candle("A", _at(10, 0), close=10.0, vwap=10.0, volume=1.0))
        agg.fold(_candle("A", _at(10, 1), close=20.0, vwap=20.0, volume=3.0))
        agg.fold(_candle("A", _at(10, 5)))
        emitted = agg.fold(_candle("A", _at(10, 6)))
        _label, bar = emitted[0]
        assert bar.vwap == (10.0 * 1.0 + 20.0 * 3.0) / 4.0

    def test_zero_volume_window_carries_the_close_as_vwap(self) -> None:
        """Given a window of zero-volume 1m, when closed, then VWAP is the close.

        A volume-weighted mean is undefined without volume, and zero is the
        one answer that is affirmatively wrong: the bar's OHLC still names a
        real price level, so a persisted ``vwap=0.0`` would price a valid bar
        at zero for every reader that joins on it. Carrying the close matches
        the ``_flat_fill`` bar the same aggregator writes when a window has no
        bucket at all.

        This is reached today by walutomat, whose real 1m bars all carry
        ``volume=0.0``, and by kraken once minute completion folds flat bars
        for a fully tradeless window.
        """
        agg = CandleAggregator(["5m"])
        agg.fold(_candle("A", _at(10, 0), close=10.0, volume=0.0))
        agg.fold(_candle("A", _at(10, 1), close=11.0, volume=0.0))
        agg.fold(_candle("A", _at(10, 5)))
        emitted = agg.fold(_candle("A", _at(10, 6)))
        _label, bar = emitted[0]
        assert bar.volume == 0.0
        assert bar.vwap == 11.0
        assert bar.close == 11.0


class TestEmitOnClose:
    """Buckets emit only once the per-symbol watermark passes their end."""

    def test_does_not_emit_before_window_end(self) -> None:
        """Given the window is not yet over, when folding, then no emit."""
        agg = CandleAggregator(["5m"])
        assert agg.fold(_candle("A", _at(10, 0))) == []
        assert agg.fold(_candle("A", _at(10, 1))) == []
        assert agg.fold(_candle("A", _at(10, 4))) == []
        assert agg.fold(_candle("A", _at(10, 5))) == []

    def test_emits_when_next_window_minute_finalizes(self) -> None:
        """Given the next window's minute finalizes, when folding, then emit."""
        agg = CandleAggregator(["5m"])
        for minute in (0, 1, 2, 3, 4, 5):
            assert agg.fold(_candle("A", _at(10, minute))) == []
        emitted = agg.fold(_candle("A", _at(10, 6)))
        assert [label for label, _bar in emitted] == ["5m"]

    def test_grace_delays_emission(self) -> None:
        """A positive grace withholds emission until the grace also elapses."""
        agg = CandleAggregator(["5m"], grace_seconds=120.0)
        agg.fold(_candle("A", _at(10, 0)))
        agg.fold(_candle("A", _at(10, 1)))
        agg.fold(_candle("A", _at(10, 5)))
        assert agg.fold(_candle("A", _at(10, 6))) == []

    def test_simultaneous_boundary_closes_ascending_order(self) -> None:
        """Given midnight UTC, when crossed, then 1h then 1d both emit."""
        agg = CandleAggregator(["1h", "1d"])
        agg.seed_1m("1h", _candle("A", _at(23, 0)))
        agg.seed_1m("1d", _candle("A", _BASE))
        agg.fold(_candle("A", _at(0, 0, day=15)))
        emitted = agg.fold(_candle("A", _at(0, 1, day=15)))
        assert [label for label, _bar in emitted] == ["1h", "1d"]
        labels = dict(emitted)
        assert labels["1h"].interval_begin == _at(23, 0)
        assert labels["1d"].interval_begin == _BASE

    def test_incomplete_window_joined_midstream_is_suppressed(self) -> None:
        """A pre-epoch window (mid-stream join) whose open minute was never observed is not emitted.

        Modelled with a live epoch inside the window: the window opened before the
        aggregator went live, so it is not trustworthy and is suppressed even
        though a later minute folds into it.
        """
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 2))
        agg.fold(_candle("A", _at(10, 2)))
        agg.fold(_candle("A", _at(10, 3)))
        agg.fold(_candle("A", _at(10, 5)))
        assert agg.fold(_candle("A", _at(10, 6))) == []
        assert agg.pop_seeded_incomplete_windows() == []

    def test_post_epoch_window_missing_open_minute_is_emitted(self) -> None:
        """A fully-observed post-epoch window emits even with no opening-minute trade.

        The window opened strictly after the live epoch, so every minute was
        observed; a missing opening minute is a genuine no-trade minute and the
        bar opens at its first traded minute rather than being suppressed.
        """
        agg = CandleAggregator(["5m"], live_epoch=_at(9, 59))
        emitted: list[tuple[str, CandleUpdate]] = []
        for minute in (2, 3, 4, 5, 6):
            emitted += agg.fold(_candle("A", _at(10, minute)))
        begins = [bar.interval_begin for _label, bar in emitted]
        assert _at(10, 0) in begins
        assert agg.pop_seeded_incomplete_windows() == []

    def test_seeded_window_missing_open_minute_is_suppressed(self) -> None:
        """A seeded window whose opening minute is absent is suppressed, not truncated.

        The restart seed reconstructs the current window from the durable plane;
        seeding makes it trustworthy, but if the seed does not reach the window's
        opening minute the bucket must still NOT publish a truncated bar. This
        guard must hold even though seeding runs before the live epoch is set
        (the epoch is still 0 during seeding).
        """
        agg = CandleAggregator(["1d"])
        agg.seed_1m("1d", _candle("A", _at(0, 2)))
        agg.seed_1m("1d", _candle("A", _at(0, 3)))
        agg.set_live_epoch(_at(0, 4))
        agg.fold(_candle("A", _at(0, 0, day=15)))
        emitted = agg.fold(_candle("A", _at(0, 1, day=15)))
        assert [label for label, _bar in emitted if label == "1d"] == []
        assert agg.pop_seeded_incomplete_windows() == [
            SeededIncompleteWindow("A", "1d", _BASE, 2, frozenset({_at(0, 2), _at(0, 3)}))
        ]
        assert agg.pop_seeded_incomplete_windows() == []

    def test_seeded_incomplete_expected_count_tracks_distinct_complete_minutes(self) -> None:
        """A seeded incomplete signal counts each folded complete 1m minute once."""
        agg = CandleAggregator(["1d"])
        agg.seed_1m("1d", _candle("A", _at(0, 2)))
        agg.seed_1m("1d", _candle("A", _at(0, 2), close=200.0))
        agg.seed_1m("1d", _candle("A", _at(0, 3)))
        agg.set_live_epoch(_at(0, 4))
        agg.fold(_candle("A", _at(0, 0, day=15)))
        agg.fold(_candle("A", _at(0, 1, day=15)))
        assert agg.pop_seeded_incomplete_windows() == [
            SeededIncompleteWindow("A", "1d", _BASE, 2, frozenset({_at(0, 2), _at(0, 3)}))
        ]

    def test_pre_epoch_window_suppressed_post_epoch_self_heals(self) -> None:
        """A pre-epoch window is suppressed even if its open minute folds late."""
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 2))
        emitted: list[tuple[str, CandleUpdate]] = []
        for minute in range(0, 12):
            emitted += agg.fold(_candle("A", _at(10, minute)))
        begins = [bar.interval_begin for _label, bar in emitted]
        assert _at(10, 0) not in begins
        assert _at(10, 5) in begins

    def test_minute_after_watermark_advanced_is_dropped(self) -> None:
        """A minute arriving after the watermark passed it is dropped + counted.

        Documents the bounded-reorder contract: deep reordering past a finalized
        window cannot be reconstructed.
        """
        agg = CandleAggregator(["5m"])
        agg.fold(_candle("A", _at(10, 0), volume=10.0))
        agg.fold(_candle("A", _at(10, 5), volume=1.0))
        agg.fold(_candle("A", _at(10, 4), volume=1.0))
        emitted = agg.fold(_candle("A", _at(10, 6), volume=1.0))
        assert len(emitted) == 1
        _label, bar = emitted[0]
        assert bar.volume == 11.0
        assert agg.fold(_candle("A", _at(10, 1), volume=20.0)) == []
        assert agg.late_rolls_after_close == 1


class TestPerSymbol:
    """Watermark and emission are per symbol."""

    def test_one_symbol_crossing_midnight_does_not_emit_another(self) -> None:
        """One symbol crossing midnight does not emit another's daily bar."""
        agg = CandleAggregator(["1d"])
        agg.seed_1m("1d", _candle("A", _BASE))
        agg.fold(_candle("B", _at(12, 0)))
        agg.fold(_candle("B", _at(12, 1)))
        agg.fold(_candle("A", _at(0, 0, day=15)))
        emitted = agg.fold(_candle("A", _at(0, 1, day=15)))
        assert len(emitted) == 1
        _label, bar = emitted[0]
        assert bar.symbol == "A"
        assert ("B", "1d", int(_BASE.timestamp())) in agg._buckets


class TestLate:
    """Late older-minute frames are dropped, not folded."""

    def test_late_frame_dropped_and_counted_warns_once(self) -> None:
        """A re-seen finalized minute is dropped and counted, warning once."""
        agg = CandleAggregator(["5m"])
        agg.fold(_candle("A", _at(10, 0)))
        agg.fold(_candle("A", _at(10, 1)))
        agg.fold(_candle("A", _at(10, 2)))
        assert agg.fold(_candle("A", _at(10, 0), close=999.0)) == []
        assert agg.fold(_candle("A", _at(10, 0), close=888.0)) == []
        assert agg.late_rolls_after_close == 2
        assert agg._warned_late == {("A", int(_at(10, 0).timestamp()))}

    def test_finalize_of_already_folded_minute_drops(self) -> None:
        """A stale minute finalized after the seeded watermark is dropped."""
        agg = CandleAggregator(["5m"])
        agg.seed_1m("5m", _candle("A", _at(10, 5)))
        agg.fold(_candle("A", _at(10, 3)))
        assert agg.fold(_candle("A", _at(10, 8))) == []
        assert agg.late_rolls_after_close == 1


class TestSeed:
    """Restart rebuild folds finalized 1m without emitting."""

    def test_window_start_returns_floor_for_configured_tf(self) -> None:
        """Given a configured TF, when asked, then the UTC window start."""
        agg = CandleAggregator(["1h", "1d"])
        assert agg.window_start("1h", _at(14, 37)) == _at(14, 0)
        assert agg.window_start("1d", _at(14, 37)) == _BASE

    def test_window_start_none_for_unconfigured_tf(self) -> None:
        """Given an unconfigured TF, when asked, then None."""
        agg = CandleAggregator(["1h"])
        assert agg.window_start("1d", _at(14, 37)) is None

    def test_timeframe_seconds_returns_width(self) -> None:
        """timeframe_seconds returns the configured timeframe width in seconds."""
        agg = CandleAggregator(["1h", "1d"])
        assert agg.timeframe_seconds("1h") == 3600
        assert agg.timeframe_seconds("1d") == 86400

    def test_seed_unknown_timeframe_is_noop(self) -> None:
        """Given a non-configured TF, when seeding, then nothing changes."""
        agg = CandleAggregator(["1h"])
        agg.seed_1m("1m", _candle("A", _at(10, 0)))
        agg.seed_1m("5m", _candle("A", _at(10, 0)))
        assert agg._buckets == {}

    def test_seed_reconstructs_buckets_without_emitting_or_holding(self) -> None:
        """Seeding rebuilds buckets without emitting or holding the minute."""
        agg = CandleAggregator(["1h", "1d"])
        agg.seed_1m("1d", _candle("A", _at(10, 0), volume=2.0))
        agg.seed_1m("1d", _candle("A", _at(10, 1), volume=3.0))
        agg.seed_1m("1h", _candle("A", _at(10, 0), volume=2.0))
        agg.seed_1m("1h", _candle("A", _at(10, 1), volume=3.0))
        assert "A" not in agg._open_minutes
        day_bucket = agg._buckets[("A", "1d", int(_BASE.timestamp()))]
        hour_bucket = agg._buckets[("A", "1h", int(_at(10, 0).timestamp()))]
        assert day_bucket.volume == 5.0
        assert hour_bucket.volume == 5.0

    def test_seed_out_of_order_tracks_open_and_close(self) -> None:
        """Out-of-order seeds track the earliest open and latest close."""
        agg = CandleAggregator(["1h"])
        agg.seed_1m("1h", _candle("A", _at(10, 1), open_=11.0, close=11.0))
        agg.seed_1m("1h", _candle("A", _at(10, 0), open_=10.0, close=10.0))
        agg.seed_1m("1h", _candle("A", _at(10, 2), open_=12.0, close=12.0))
        bucket = agg._buckets[("A", "1h", int(_at(10, 0).timestamp()))]
        assert bucket.open == 10.0
        assert bucket.close == 12.0

    def test_seed_does_not_lower_watermark_or_folded_minute(self) -> None:
        """An earlier seed after a later one keeps the highest watermark."""
        agg = CandleAggregator(["1h"])
        agg.seed_1m("1h", _candle("A", _at(10, 1)))
        agg.seed_1m("1h", _candle("A", _at(10, 0)))
        assert agg._watermark["A"] == _at(10, 1)
        assert agg._folded_minute["A"] == int(_at(10, 1).timestamp())

    def test_seed_then_live_emits_with_seeded_data(self) -> None:
        """A seeded open window emits with the seeded volume once live closes it."""
        agg = CandleAggregator(["1h"])
        agg.seed_1m("1h", _candle("A", _at(10, 0), volume=5.0))
        agg.fold(_candle("A", _at(10, 58), volume=1.0))
        agg.fold(_candle("A", _at(10, 59), volume=1.0))
        agg.fold(_candle("A", _at(11, 0)))
        emitted = agg.fold(_candle("A", _at(11, 1)))
        assert len(emitted) == 1
        _label, bar = emitted[0]
        assert bar.interval_begin == _at(10, 0)
        assert bar.volume == 7.0
        assert agg.pop_seeded_incomplete_windows() == []


class TestCeil:
    """Canonical boundary ceiling used to clamp forward-fill to the live region."""

    def test_ceil_returns_boundary_or_next(self) -> None:
        """Given a timestamp, when ceiling, then the first boundary at or above."""
        assert CandleAggregator._ceil(300, 300) == 300
        assert CandleAggregator._ceil(301, 300) == 600
        assert CandleAggregator._ceil(0, 300) == 0


class TestForwardFillFlush:
    """Time-driven flush: trailing-window sealing and empty-window forward-fill."""

    def test_flag_off_flush_is_noop(self) -> None:
        """With forward-fill off, flush returns nothing regardless of state."""
        agg = CandleAggregator(["5m"], forward_fill=False)
        agg.fold(_candle("A", _at(10, 0)))
        agg.fold(_candle("A", _at(10, 1)))
        assert agg.flush(_at(10, 30)) == []
        assert agg.forward_fill is False

    def test_forward_fill_property_reflects_flag(self) -> None:
        """The forward_fill property reflects the constructor flag."""
        assert CandleAggregator(["5m"], forward_fill=True).forward_fill is True

    def test_trailing_real_window_sealed_at_wall_clock(self) -> None:
        """A complete window with no later 1m is sealed by flush at wall clock."""
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 0), forward_fill=True)
        for minute in range(5):
            agg.fold(_candle("A", _at(10, minute), close=50.0))
        emitted = agg.flush(_at_s(10, 6, 30))
        assert [label for label, _bar in emitted] == ["5m"]
        _label, bar = emitted[0]
        assert bar.interval_begin == _at(10, 0)
        assert bar.volume == 5.0

    def test_empty_window_forward_filled_with_carried_close(self) -> None:
        """An empty window after real data is filled flat at the prior close."""
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 0), forward_fill=True)
        for minute in range(5):
            agg.fold(_candle("A", _at(10, minute), close=50.0))
        emitted = agg.flush(_at_s(10, 11, 30))
        bars = {bar.interval_begin: bar for _label, bar in emitted}
        assert _at(10, 0) in bars
        fill = bars[_at(10, 5)]
        assert fill.open == fill.high == fill.low == fill.close == fill.vwap == 50.0
        assert fill.volume == 0.0
        assert fill.trades == 0
        assert fill.interval == 300

    def test_gap_preserved_when_real_data_resumes(self) -> None:
        """After a forward-filled gap, a resumed window keeps its own real open."""
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 0), forward_fill=True)
        for minute in range(5):
            agg.fold(_candle("A", _at(10, minute), close=50.0))
        agg.flush(_at_s(10, 11, 30))
        for minute in range(10, 16):
            agg.fold(_candle("A", _at(10, minute), open_=60.0, close=60.0))
        emitted = agg.fold(_candle("A", _at(10, 16), close=60.0))
        assert len(emitted) == 1
        _label, bar = emitted[0]
        assert bar.interval_begin == _at(10, 10)
        assert bar.open == 60.0

    def test_multiple_consecutive_empty_windows_filled_chronologically(self) -> None:
        """Several empty windows fill in order, each carrying the same close."""
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 0), forward_fill=True)
        for minute in range(5):
            agg.fold(_candle("A", _at(10, minute), close=50.0))
        emitted = agg.flush(_at_s(10, 21, 30))
        begins = [bar.interval_begin for _label, bar in emitted]
        assert begins == [_at(10, 0), _at(10, 5), _at(10, 10), _at(10, 15)]
        fills = [bar for _label, bar in emitted][1:]
        assert all(bar.volume == 0.0 and bar.close == 50.0 for bar in fills)

    def test_flush_does_not_finalize_current_minute(self) -> None:
        """The in-progress current minute is never finalized by flush."""
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 0), forward_fill=True)
        agg.fold(_candle("A", _at(10, 5)))
        assert agg.flush(_at_s(10, 5, 30)) == []
        assert int(_at(10, 5).timestamp()) in agg._open_minutes["A"]

    def test_flush_does_not_seal_seeded_current_window(self) -> None:
        """A seeded current window is left to the data path, not sealed by flush."""
        agg = CandleAggregator(["1h"], live_epoch=_at(10, 31), forward_fill=True)
        for minute in range(31):
            agg.seed_1m("1h", _candle("A", _at(10, minute)))
        assert agg.flush(_at_s(10, 35, 30)) == []
        assert ("A", "1h", int(_at(10, 0).timestamp())) in agg._buckets

    def test_seeded_previous_window_emitted_by_flush_step_one(self) -> None:
        """A seeded just-closed previous window emits via flush's data-path step."""
        agg = CandleAggregator(["1h"], live_epoch=_at(11, 0), forward_fill=True)
        for minute in range(60):
            agg.seed_1m("1h", _candle("A", _at(10, minute), close=50.0))
        agg.fold(_candle("A", _at(11, 0)))
        emitted = agg.flush(_at_s(11, 1, 30))
        assert [label for label, _bar in emitted] == ["1h"]
        _label, bar = emitted[0]
        assert bar.interval_begin == _at(10, 0)

    def test_late_frame_for_filled_window_dropped_not_double_emitted(self) -> None:
        """A late 1m mapping into a flush-sealed window is dropped, not folded."""
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 0), forward_fill=True)
        for minute in range(5):
            agg.fold(_candle("A", _at(10, minute), close=50.0))
        agg.flush(_at_s(10, 11, 30))
        before = agg.late_rolls_after_close
        agg.fold(_candle("A", _at(10, 6)))
        emitted = agg.fold(_candle("A", _at(10, 7)))
        assert emitted == []
        assert agg.late_rolls_after_close == before + 1
        assert ("A", "5m", int(_at(10, 5).timestamp())) not in agg._buckets

    def test_data_path_emit_then_flush_does_not_re_emit(self) -> None:
        """A window the data path already emitted is not re-emitted by flush."""
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 0), forward_fill=True)
        for minute in range(7):
            agg.fold(_candle("A", _at(10, minute), close=50.0))
        emitted = agg.flush(_at_s(10, 7, 30))
        begins = [bar.interval_begin for _label, bar in emitted]
        assert _at(10, 0) not in begins

    def test_no_baseline_incomplete_window_not_filled(self) -> None:
        """A symbol with only an incomplete pre-epoch window is never filled."""
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 3), forward_fill=True)
        agg.fold(_candle("A", _at(10, 2)))
        agg.fold(_candle("A", _at(10, 3)))
        assert agg.flush(_at_s(10, 11, 30)) == []
        assert ("A", "5m") not in agg._closed_window

    def test_empty_window_without_baseline_advances_frontier_no_fill(self) -> None:
        """An empty window with no carried close advances the frontier silently."""
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 3), forward_fill=True)
        for minute in (2, 3, 4, 11, 12):
            agg.fold(_candle("A", _at(10, minute)))
        assert agg.flush(_at_s(10, 13, 30)) == []
        assert agg._closed_window[("A", "5m")] == int(_at(10, 5).timestamp())
        assert ("A", "5m") not in agg._last_close

    def test_partial_live_window_missing_open_emits_real_bar(self) -> None:
        """In forward-fill mode a fully-live window missing its open minute is real.

        Its missing 1m frames are no-trade minutes (the corpus is contiguous), so
        the partially-filled window is the true bar — not a stale flat fill.
        """
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 0), forward_fill=True)
        for minute in (0, 1, 2, 3, 4, 7):
            agg.fold(_candle("A", _at(10, minute), close=50.0))
        emitted = agg.flush(_at_s(10, 11, 30))
        bars = {bar.interval_begin: bar for _label, bar in emitted}
        assert set(bars) == {_at(10, 0), _at(10, 5)}
        partial = bars[_at(10, 5)]
        assert partial.volume == 1.0
        assert partial.close == 50.0

    def test_fill_after_partial_window_carries_its_close(self) -> None:
        """A fill after a partial real window carries that window's real close."""
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 0), forward_fill=True)
        for minute in range(5):
            agg.fold(_candle("A", _at(10, minute), close=50.0))
        agg.flush(_at_s(10, 6, 30))
        agg.fold(_candle("A", _at(10, 7), close=99.0))
        agg.flush(_at_s(10, 11, 30))
        emitted = agg.flush(_at_s(10, 16, 30))
        bars = {bar.interval_begin: bar for _label, bar in emitted}
        assert bars[_at(10, 10)].close == 99.0

    def test_stranded_pre_epoch_bucket_does_not_rewind_frontier(self) -> None:
        """Removing a stranded pre-epoch bucket never rewinds the frontier.

        A mid-window-epoch restart strands the epoch-straddle bucket; when a later
        finalize removes it, a non-monotone frontier would rewind and re-emit
        already-filled windows. The frontier must only advance. The epoch floors
        to 10:02 (CA-1) — strictly INSIDE [10:00,10:05) but past its first minute —
        so the window stays stranded (a first-minute-restart window is instead
        recovered as complete; see ``TestLiveEpochFloor``).
        """
        agg = CandleAggregator(["5m"], live_epoch=_at_s(10, 2, 30), forward_fill=True)
        for minute in range(55, 60):
            agg.seed_1m("5m", _candle("A", _at(9, minute), close=50.0))
        agg.fold(_candle("A", _at(10, 0), close=60.0))
        seen: list[datetime] = []
        for when in (_at_s(10, 1, 0), _at_s(10, 10, 0), _at_s(10, 15, 0)):
            seen += [bar.interval_begin for _label, bar in agg.flush(when)]
        agg.fold(_candle("A", _at(10, 16), close=70.0))
        agg.fold(_candle("A", _at(10, 17), close=71.0))
        seen += [bar.interval_begin for _label, bar in agg.flush(_at_s(10, 18, 0))]
        assert len(seen) == len(set(seen))
        assert _at(10, 5) in seen
        assert _at(10, 10) in seen

    def test_forward_fill_clamps_to_canonical_boundary(self) -> None:
        """A mid-window live epoch clamps the first fill to a TF boundary."""
        agg = CandleAggregator(["5m"], live_epoch=_at_s(10, 2, 30), forward_fill=True)
        for minute in range(55, 60):
            agg.seed_1m("5m", _candle("A", _at(9, minute), close=50.0))
        agg.fold(_candle("A", _at(10, 2), close=60.0))
        emitted = agg.flush(_at_s(10, 16, 30))
        begins = [int(bar.interval_begin.timestamp()) for _label, bar in emitted]
        assert all(begin % 300 == 0 for begin in begins)
        labels = [bar.interval_begin for _label, bar in emitted]
        assert _at(9, 55) in labels
        assert _at(10, 5) in labels


class TestForwardFillBounds:
    """The forward-fill window count is bounded to avoid unbounded synthesis."""

    def test_overflow_jumps_frontier_purges_and_warns(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A gap beyond the bound jumps the frontier, purges, and warns once."""
        monkeypatch.setattr(
            "snapper.messaging.publishers.candle_aggregator._FORWARD_FILL_MAX_WINDOWS", 2
        )
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 0), forward_fill=True)
        for minute in range(5):
            agg.fold(_candle("A", _at(10, minute), close=50.0))
        emitted = agg.flush(_at_s(10, 31, 30))
        assert emitted == []
        assert ("A", "5m", int(_at(10, 0).timestamp())) not in agg._buckets
        assert ("A", "5m") in agg._warned_fill_overflow

    def test_dead_symbol_stops_filling_after_max_then_resumes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A dark symbol forward-fills at most MAX windows past its last real bar.

        After the bound it stops manufacturing bars (no unbounded synthesis); when
        real data returns it resets and resumes without back-filling the dead gap.
        """
        monkeypatch.setattr(
            "snapper.messaging.publishers.candle_aggregator._FORWARD_FILL_MAX_WINDOWS", 2
        )
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 0), forward_fill=True)
        for minute in range(5):
            agg.fold(_candle("A", _at(10, minute), close=50.0))
        agg.flush(_at_s(10, 6, 30))
        fills: list[datetime] = []
        for end_minute in (11, 16, 21, 26):
            fills += [bar.interval_begin for _label, bar in agg.flush(_at_s(10, end_minute, 30))]
        assert fills == [_at(10, 5), _at(10, 10)]
        for minute in range(30, 36):
            agg.fold(_candle("A", _at(10, minute), close=60.0))
        resumed = [bar.interval_begin for _label, bar in agg.flush(_at_s(10, 41, 30))]
        assert _at(10, 30) in resumed
        assert _at(10, 20) not in resumed

    def test_overflow_warns_once_per_symbol_timeframe(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A repeated overflow for the same (symbol, timeframe) warns only once."""
        monkeypatch.setattr(
            "snapper.messaging.publishers.candle_aggregator._FORWARD_FILL_MAX_WINDOWS", 2
        )
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 0), forward_fill=True)
        for minute in range(5):
            agg.fold(_candle("A", _at(10, minute), close=50.0))
        agg.flush(_at_s(10, 6, 30))
        agg.flush(_at_s(10, 31, 30))
        agg.flush(_at_s(11, 31, 30))
        assert agg._warned_fill_overflow == {("A", "5m")}

    def test_warn_fill_overflow_set_is_bounded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The overflow warn-dedupe set clears at its cap so memory stays bounded."""
        monkeypatch.setattr(
            "snapper.messaging.publishers.candle_aggregator._FORWARD_FILL_MAX_WINDOWS", 2
        )
        monkeypatch.setattr("snapper.messaging.publishers.candle_aggregator._WARNED_LATE_CAP", 1)
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 0), forward_fill=True)
        for sym in ("A", "B"):
            for minute in range(5):
                agg.fold(_candle(sym, _at(10, minute), close=50.0))
            agg.flush(_at_s(10, 6, 30))
        agg.flush(_at_s(11, 31, 30))
        assert len(agg._warned_fill_overflow) <= 1


class TestSuspendAcrossFeedBreak:
    """Re-arming the live epoch around a feed break, failing closed.

    Before this existed, ``set_live_epoch`` had exactly one caller — the
    publisher at startup — so nothing re-armed the epoch on reconnect. The
    flush loop is process-scoped and kept running across the gap, carrying
    the pre-gap close forward and marking the results complete. These tests
    pin the three pieces of state that have to be dropped.
    """

    def test_carried_close_does_not_survive_the_break(self) -> None:
        """The whole point: a pre-gap price must not be filled into post-gap windows.

        Given real data ending at close 50.0 and then a feed break,
        When windows elapse after the break with no data,
        Then nothing is emitted at all — rather than a run of flat bars at
            50.0 asserting a price nobody observed during the gap.
        """
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 0), forward_fill=True)
        for minute in range(5):
            agg.fold(_candle("A", _at(10, minute), close=50.0))
        agg.flush(_at_s(10, 6, 30))
        agg.suspend_across_feed_break(_at(10, 10))
        assert agg.flush(_at_s(10, 31, 30)) == []

    def test_window_straddling_the_break_is_dropped_not_filled(self) -> None:
        """A window whose minutes span the gap is missing, never fabricated.

        Given a window opened before the break with only its early minutes,
        When the break is recorded and the window's end passes,
        Then it emits nothing: its ``complete`` verdict was decided when it
            was created and no longer deserves trust, and the flush walk
            starts at the new epoch so it would otherwise be orphaned rather
            than judged. A missing window is honest; a truncated one is not.
        """
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 0), forward_fill=True)
        for minute in range(2):
            agg.fold(_candle("A", _at(10, minute), close=50.0))
        agg.suspend_across_feed_break(_at(10, 3))
        emitted = agg.flush(_at_s(10, 11, 30))
        assert [bar.interval_begin for _label, bar in emitted] == []

    def test_fill_resumes_only_after_a_fresh_real_bar(self) -> None:
        """Self-healing: the first real bar after the break re-seeds the carry.

        Given a break followed by fresh real data at a NEW price,
        When later windows are empty,
        Then they fill at the new price — so the break costs only the windows
            it actually spans, and forward-fill is not disabled permanently.
        """
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 0), forward_fill=True)
        for minute in range(5):
            agg.fold(_candle("A", _at(10, minute), close=50.0))
        agg.flush(_at_s(10, 6, 30))
        agg.suspend_across_feed_break(_at(10, 10))
        for minute in range(10, 15):
            agg.fold(_candle("A", _at(10, minute), close=77.0))
        emitted = agg.flush(_at_s(10, 21, 30))
        bars = {bar.interval_begin: bar for _label, bar in emitted}
        assert bars[_at(10, 10)].close == 77.0
        fill = bars[_at(10, 15)]
        assert fill.open == fill.high == fill.low == fill.close == 77.0
        assert fill.volume == 0.0
        assert fill.trades == 0

    def test_window_opening_after_the_break_completes_normally(self) -> None:
        """The epoch moves forward, it does not poison everything after it.

        Given a break at 10:03,
        When a window opens fully after it and every minute is observed,
        Then it emits as a normal complete bar.
        """
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 0), forward_fill=True)
        agg.suspend_across_feed_break(_at(10, 3))
        for minute in range(5, 11):
            agg.fold(_candle("A", _at(10, minute), close=60.0))
        emitted = agg.flush(_at_s(10, 11, 30))
        begins = [bar.interval_begin for _label, bar in emitted]
        assert _at(10, 5) in begins
        assert _at(10, 0) not in begins

    def test_break_is_floored_to_the_minute_like_the_startup_epoch(self) -> None:
        """Re-arming reuses ``set_live_epoch``, so it inherits its flooring.

        Given a break recorded at 10:05:45,
        When a window opening at 10:05 is fully observed,
        Then it still emits — the same reasoning as a restart landing
            partway through a minute.
        """
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 0), forward_fill=True)
        agg.suspend_across_feed_break(_at_s(10, 5, 45))
        for minute in range(5, 11):
            agg.fold(_candle("A", _at(10, minute), close=60.0))
        emitted = agg.flush(_at_s(10, 11, 30))
        assert _at(10, 5) in [bar.interval_begin for _label, bar in emitted]

    def test_break_affects_every_symbol(self) -> None:
        """A socket carries every symbol, so a break darkens all of them.

        Given two symbols both carrying a close,
        When the feed breaks,
        Then neither forward-fills — the gap is a property of the connection,
            not of one instrument.
        """
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 0), forward_fill=True)
        for symbol in ("A", "B"):
            for minute in range(5):
                agg.fold(_candle(symbol, _at(10, minute), close=50.0))
        agg.flush(_at_s(10, 6, 30))
        agg.suspend_across_feed_break(_at(10, 10))
        assert agg.flush(_at_s(10, 31, 30)) == []


class TestLiveEpochFloor:
    """CA-1: the live epoch is floored to the minute so a restart mid-first-minute keeps its bar."""

    def test_epoch_floored_to_minute_emits_first_minute_window(self) -> None:
        """A restart at 10:00:30 still emits the [10:00,10:05) window it opened in."""
        agg = CandleAggregator(["5m"])
        agg.set_live_epoch(_at_s(10, 0, 30))
        emitted: list[tuple[str, CandleUpdate]] = []
        for minute in range(0, 7):
            emitted += agg.fold(_candle("A", _at(10, minute)))
        begins = [bar.interval_begin for _label, bar in emitted]
        assert _at(10, 0) in begins

    def test_epoch_floor_still_suppresses_later_minute_window(self) -> None:
        """A restart at 10:02:30 still suppresses [10:00,10:05) (earlier minutes were missed)."""
        agg = CandleAggregator(["5m"])
        agg.set_live_epoch(_at_s(10, 2, 30))
        emitted: list[tuple[str, CandleUpdate]] = []
        for minute in range(0, 12):
            emitted += agg.fold(_candle("A", _at(10, minute)))
        begins = [bar.interval_begin for _label, bar in emitted]
        assert _at(10, 0) not in begins
        assert _at(10, 5) in begins

    def test_constructor_epoch_also_floored_to_minute(self) -> None:
        """The constructor ``live_epoch`` is floored to the minute as well."""
        agg = CandleAggregator(["5m"], live_epoch=_at_s(10, 0, 45))
        emitted: list[tuple[str, CandleUpdate]] = []
        for minute in range(0, 7):
            emitted += agg.fold(_candle("A", _at(10, minute)))
        begins = [bar.interval_begin for _label, bar in emitted]
        assert _at(10, 0) in begins

    def test_forward_fill_epoch_straddle_window_emits_best_effort(self) -> None:
        """FF + CA-1 floor: the restart-epoch window emits best-effort if its open minute is absent.

        A restart at 10:00:30 floors the epoch to 10:00, so [10:00,10:05) is
        trustworthy. With forward-fill ON the first-minute requirement is dropped,
        so even with minute 10:00 genuinely absent the window emits best-effort
        from 10:01-10:04 — intended (an absent minute is no-trade; the venue
        snapshot delivers it if it traded), and strictly better than suppressing
        the whole window (dual-Codex-reviewed).
        """
        agg = CandleAggregator(["5m"], forward_fill=True)
        agg.set_live_epoch(_at_s(10, 0, 30))
        for minute in range(1, 5):
            agg.fold(_candle("A", _at(10, minute), close=100.0 + minute))
        emitted = agg.flush(_at_s(10, 5, 31))
        bars = {bar.interval_begin: bar for _label, bar in emitted}
        assert _at(10, 0) in bars
        assert bars[_at(10, 0)].close == 104.0


class TestFlushGrace:
    """FF-1: flush_grace withholds a just-ended minute/window until a late final frame can arrive."""

    def test_flush_grace_withholds_just_ended_window(self) -> None:
        """A trailing window is not sealed until the flush grace past its end has elapsed."""
        agg = CandleAggregator(
            ["5m"], live_epoch=_at(10, 0), forward_fill=True, flush_grace_seconds=5.0
        )
        for minute in range(0, 5):
            agg.fold(_candle("A", _at(10, minute)))
        assert agg.flush(_at_s(10, 5, 3)) == []
        emitted = agg.flush(_at_s(10, 5, 6))
        begins = [bar.interval_begin for _label, bar in emitted]
        assert _at(10, 0) in begins

    def test_zero_flush_grace_seals_at_boundary(self) -> None:
        """With no grace (the default) the trailing window seals as soon as the boundary passes."""
        agg = CandleAggregator(["5m"], live_epoch=_at(10, 0), forward_fill=True)
        for minute in range(0, 5):
            agg.fold(_candle("A", _at(10, minute)))
        emitted = agg.flush(_at_s(10, 5, 3))
        begins = [bar.interval_begin for _label, bar in emitted]
        assert _at(10, 0) in begins

    def test_flush_grace_seals_exactly_at_boundary_plus_grace(self) -> None:
        """At exactly boundary+grace the window seals once (inclusive: effective == boundary)."""
        agg = CandleAggregator(
            ["5m"], live_epoch=_at(10, 0), forward_fill=True, flush_grace_seconds=5.0
        )
        for minute in range(0, 5):
            agg.fold(_candle("A", _at(10, minute)))
        emitted = agg.flush(_at_s(10, 5, 5))
        begins = [bar.interval_begin for _label, bar in emitted]
        assert begins.count(_at(10, 0)) == 1


def _acquired(
    symbol: str, begin: datetime, acquired_at: datetime, *, close: float = 100.0
) -> CandleUpdate:
    """Build a trade-built 1m bar with the producer's acquisition time.

    Args:
        symbol: Native symbol.
        begin: Opening of the bar's minute.
        acquired_at: When the producer completed the bar's evidence.
        close: Bar close.

    Returns:
        A 1m :class:`CandleUpdate` carrying ``acquired_at``.
    """
    return replace(_candle(symbol, begin, close=close), acquired_at=acquired_at)


def _authoring(*, forward_fill: bool = False) -> CandleAggregator:
    """Build a 5m aggregator that authors its windows.

    Args:
        forward_fill: Whether forward-fill is enabled.

    Returns:
        A live aggregator with its epoch at 15:00 and authoring on.
    """
    agg = CandleAggregator(["5m"], live_epoch=_at(15, 0), forward_fill=forward_fill)
    agg.authoring = True
    return agg


class TestCertification:
    """Completion marks certify the windows whose end they newly reach."""

    def test_a_window_is_authored_by_its_certification_or_newest_constituent(self) -> None:
        """A closing bar acquired later never re-authors a certified window.

        Given a window whose minutes were acquired by 16:00:05 and whose end the
            16:00 mark certified at 16:00:12,
        When a bar acquired at 16:30:12 finalizes the next minute and closes it,
        Then the window is authored at the certification.
        """
        agg = _authoring()
        for minute in range(55, 60):
            agg.fold(_acquired("A", _at(15, minute), _at(15, minute) + timedelta(seconds=65)))
        agg.certify(_at(16, 0), _at_s(16, 0, 12))
        agg.fold(_acquired("A", _at(16, 0), _at_s(16, 1, 5)))

        emitted = agg.fold(_acquired("A", _at(16, 29), _at_s(16, 30, 12)))

        assert [(label, bar.interval_begin, bar.acquired_at) for label, bar in emitted] == [
            ("5m", _at(15, 55), _at_s(16, 0, 12))
        ]

    def test_a_constituent_acquired_after_the_certification_authors_the_window(self) -> None:
        """A timely certification stays when a late correction replaces its held minute.

        Given the 15:59 minute held when the 16:00 mark certified its window at
            16:00:12, then replaced by a correction acquired at 16:00:40,
        When a later bar closes the window,
        Then the window is authored at the correction, the later of the two.
        """
        agg = _authoring()
        agg.fold(_acquired("A", _at(15, 59), _at_s(16, 0, 5)))
        agg.certify(_at(16, 0), _at_s(16, 0, 12))
        agg.fold(_acquired("A", _at(15, 59), _at_s(16, 0, 40), close=101.0))
        agg.fold(_acquired("A", _at(16, 0), _at_s(16, 1, 12)))

        emitted = agg.fold(_acquired("A", _at(16, 5), _at_s(16, 6, 12)))

        assert [(bar.interval_begin, bar.close, bar.acquired_at) for _l, bar in emitted] == [
            (_at(15, 55), 101.0, _at_s(16, 0, 40))
        ]

    def test_a_held_minute_carries_its_certification_into_its_bucket(self) -> None:
        """The held-minute case, with forward-fill off.

        Given a timely bar for 15:55 held with no bucket when the 16:00 mark
            certifies its window, and later marks that pass,
        When a bar at 16:30 finalizes the held minute and another bar closes the
            window,
        Then the window is authored at the 16:00 certification, and no later
            mark re-authors it.
        """
        agg = _authoring()
        agg.fold(_acquired("A", _at(15, 55), _at_s(15, 56, 12)))
        agg.certify(_at(16, 0), _at_s(16, 0, 12))
        assert agg._held_certifications == {
            ("A", "5m", int(_at(15, 55).timestamp())): _at_s(16, 0, 12)
        }
        agg.fold(_acquired("A", _at(16, 30), _at_s(16, 31, 12)))
        assert agg._held_certifications == {}
        agg.certify(_at(16, 31), _at_s(16, 31, 12))

        emitted = agg.fold(_acquired("A", _at(16, 31), _at_s(16, 32, 12)))

        assert [(bar.interval_begin, bar.acquired_at) for _l, bar in emitted] == [
            (_at(15, 55), _at_s(16, 0, 12))
        ]

    def test_a_late_held_minute_gets_no_certification_and_a_late_created_bucket(self) -> None:
        """A late first emission is authored by its own acquisition.

        Given the 16:00 mark certified at 16:00:12 while the symbol had no input
            in the 15:55 window, then a late first emission for 15:59 acquired at
            16:00:45,
        When the 16:01 mark passes and later bars finalize and close the window,
        Then no held certification is created, the bucket is late-created, and
            the window is authored at 16:00:45, never at 16:01:12.
        """
        agg = _authoring()
        agg.fold(_acquired("A", _at(15, 50), _at_s(15, 51, 12)))
        agg.fold(_acquired("A", _at(15, 51), _at_s(15, 52, 12)))
        agg.certify(_at(16, 0), _at_s(16, 0, 12))
        agg.fold(_acquired("A", _at(15, 59), _at_s(16, 0, 45)))
        agg.certify(_at(16, 1), _at_s(16, 1, 12))
        assert agg._held_certifications == {}
        agg.fold(_acquired("A", _at(16, 0), _at_s(16, 1, 12)))
        late = agg._buckets[("A", "5m", int(_at(15, 55).timestamp()))]
        assert late.late_created
        agg.certify(_at(16, 2), _at_s(16, 2, 12))

        emitted = agg.fold(_acquired("A", _at(16, 5), _at_s(16, 6, 12)))

        assert [(bar.interval_begin, bar.acquired_at) for _l, bar in emitted] == [
            (_at(15, 55), _at_s(16, 0, 45))
        ]

    def test_a_certified_window_is_not_certified_again(self) -> None:
        """Only a newly reached end is certified, once.

        Given a bucket certified by the 16:00 mark,
        When the 16:01 mark passes,
        Then the bucket keeps the first certification.
        """
        agg = _authoring()
        agg.fold(_acquired("A", _at(15, 58), _at_s(15, 59, 12)))
        agg.fold(_acquired("A", _at(15, 59), _at_s(16, 0, 5)))
        agg.certify(_at(16, 0), _at_s(16, 0, 12))
        agg.certify(_at(16, 1), _at_s(16, 1, 12))
        agg.certify(_at(15, 59), _at_s(16, 1, 30))

        bucket = agg._buckets[("A", "5m", int(_at(15, 55).timestamp()))]
        assert bucket.certified_at == _at_s(16, 0, 12)
        assert agg._certified_through_ts == int(_at(16, 1).timestamp())

    def test_a_held_certification_is_dropped_with_a_late_minute_and_by_a_break(self) -> None:
        """No certification outlives the held input it describes.

        Given a held certification for a window the frontier has already passed,
        When its minute is finalized, and then another held certification is
            followed by a break,
        Then the first is removed with the late drop, and the break clears the
            second together with the certified boundary.
        """
        agg = _authoring()
        key = ("A", "5m", int(_at(15, 55).timestamp()))
        agg._closed_window[("A", "5m")] = key[2]
        agg.fold(_acquired("A", _at(15, 57), _at_s(15, 58, 12)))
        agg.certify(_at(16, 0), _at_s(16, 0, 12))
        assert key in agg._held_certifications
        agg.fold(_acquired("A", _at(16, 3), _at_s(16, 4, 12)))
        assert key not in agg._held_certifications
        assert agg.late_rolls_after_close == 1

        agg.certify(_at(16, 5), _at_s(16, 5, 12))
        assert agg._held_certifications
        agg.suspend_across_feed_break(_at(16, 6))

        assert agg._held_certifications == {}
        assert agg._certified_through_ts is None

    def test_a_seeded_bucket_is_never_late_created(self) -> None:
        """The restart seed predates every mark.

        Given a certified boundary past a window,
        When a seed folds a minute of that window,
        Then its bucket is neither late-created nor certified by a held entry.
        """
        agg = _authoring()
        agg.certify(_at(16, 0), _at_s(16, 0, 12))
        agg.seed_1m("5m", _candle("A", _at(15, 55)))

        bucket = agg._buckets[("A", "5m", int(_at(15, 55).timestamp()))]
        assert (bucket.late_created, bucket.certified_at) == (False, None)

    def test_without_authoring_windows_carry_no_author(self) -> None:
        """With authoring off, the publisher stamps windows at processing.

        Given certified, acquired minutes and authoring off,
        When a bar closes their window,
        Then the window carries no acquisition time.
        """
        agg = _authoring()
        agg.authoring = False
        for minute in range(55, 60):
            agg.fold(_acquired("A", _at(15, minute), _at(15, minute) + timedelta(seconds=65)))
        agg.certify(_at(16, 0), _at_s(16, 0, 12))
        agg.fold(_acquired("A", _at(16, 0), _at_s(16, 1, 5)))

        emitted = agg.fold(_acquired("A", _at(16, 1), _at_s(16, 2, 12)))

        assert [bar.acquired_at for _l, bar in emitted] == [None]

    def test_bars_without_acquisition_are_authored_by_the_certification(self) -> None:
        """A window of bars from another source still gets its certification.

        Given bars without acquisition times whose window the 16:00 mark
            certified,
        When later bars close the window,
        Then the window is authored at the certification.
        """
        agg = _authoring()
        for minute in range(55, 60):
            agg.fold(_candle("A", _at(15, minute)))
        agg.certify(_at(16, 0), _at_s(16, 0, 12))
        agg.fold(_candle("A", _at(16, 0)))

        emitted = agg.fold(_candle("A", _at(16, 1)))

        assert [bar.acquired_at for _l, bar in emitted] == [_at_s(16, 0, 12)]


class TestLongLivedCertification:
    """A certification lasts as long as its window's state."""

    def test_a_sparse_window_open_for_a_day_closes_under_its_pinned_certification(self) -> None:
        """Neither a late closing bar nor later marks re-author a long-open window.

        Given forward-fill off, a window with two traded minutes certified at
            16:00:12, and marks every hour for 25 hours without another bar,
        When a bar 25 hours later finalizes the window's last minute and another
            closes it,
        Then the window is authored at 16:00:12.
        """
        agg = _authoring()
        agg.fold(_acquired("A", _at(15, 56), _at_s(15, 57, 12)))
        agg.fold(_acquired("A", _at(15, 57), _at_s(15, 58, 12)))
        agg.certify(_at(16, 0), _at_s(16, 0, 12))
        for hour in range(1, 26):
            through = _at(16, 0) + timedelta(hours=hour)
            agg.certify(through, through + timedelta(seconds=12))

        late = _at(17, 1, day=15)
        agg.fold(_acquired("A", late, late + timedelta(minutes=1, seconds=12)))
        emitted = agg.fold(_acquired("A", late + timedelta(minutes=1), late + timedelta(minutes=2)))

        assert [(bar.interval_begin, bar.acquired_at) for _l, bar in emitted] == [
            (_at(15, 55), _at_s(16, 0, 12))
        ]


class TestFlushThrough:
    """Sealing and forward fill at a completion mark's boundary."""

    def test_flush_through_is_a_no_op_without_forward_fill(self) -> None:
        """Like the wall-clock flush, it is inert unless forward-fill is on.

        Given forward-fill off and a held minute,
        When a mark flushes through a later boundary,
        Then nothing is emitted and the minute stays held.
        """
        agg = _authoring()
        agg.fold(_acquired("A", _at(15, 55), _at_s(15, 56, 12)))

        assert agg.flush_through(_at(16, 10), _at_s(16, 10, 12)) == []
        assert agg._open_minutes["A"]

    def test_a_mark_seals_its_windows_and_fills_by_its_own_time(self) -> None:
        """Each window is sealed or filled at the first mark that reaches its end.

        Given minutes of the 15:55 window certified at 16:00:12, then nothing,
        When the 16:10 mark flushes through 16:10,
        Then the 15:55 window is sealed at its certification, and the empty
            16:00 and 16:05 windows are filled at the mark's own time, without
            subtracting a grace.
        """
        agg = _authoring(forward_fill=True)
        for minute in range(55, 60):
            agg.fold(
                _acquired("A", _at(15, minute), _at(15, minute) + timedelta(seconds=65), close=50.0)
            )
        agg.certify(_at(16, 0), _at_s(16, 0, 12))
        agg.certify(_at(16, 10), _at_s(16, 10, 12))

        emitted = agg.flush_through(_at(16, 10), _at_s(16, 10, 12))

        assert [(bar.interval_begin, bar.close, bar.acquired_at) for _l, bar in emitted] == [
            (_at(15, 55), 50.0, _at_s(16, 0, 12)),
            (_at(16, 0), 50.0, _at_s(16, 10, 12)),
            (_at(16, 5), 50.0, _at_s(16, 10, 12)),
        ]
        assert agg._last_close_author[("A", "5m")] == _at_s(16, 0, 12)

    def test_a_fill_is_never_authored_before_its_carried_close(self) -> None:
        """A fill states the carried close, so it cannot precede that close's author.

        Given a carried close authored at 16:12:00,
        When an earlier-produced mark fills the next empty window,
        Then the fill is authored at the carried close's author.
        """
        agg = _authoring(forward_fill=True)
        for minute in range(55, 60):
            agg.fold(_acquired("A", _at(15, minute), _at_s(16, 12, 0), close=50.0))
        agg.flush_through(_at(16, 0), _at_s(16, 0, 12))

        emitted = agg.flush_through(_at(16, 5), _at_s(16, 5, 12))

        assert [(bar.interval_begin, bar.acquired_at) for _l, bar in emitted] == [
            (_at(16, 0), _at_s(16, 12, 0))
        ]

    def test_a_break_forgets_the_carried_close_author(self) -> None:
        """The author travels with the close it describes.

        Given a sealed window whose close is carried,
        When the feed breaks,
        Then the carried close and its author are both forgotten.
        """
        agg = _authoring(forward_fill=True)
        for minute in range(55, 60):
            agg.fold(
                _acquired("A", _at(15, minute), _at(15, minute) + timedelta(seconds=65), close=50.0)
            )
        agg.flush_through(_at(16, 0), _at_s(16, 0, 12))
        assert ("A", "5m") in agg._last_close_author

        agg.suspend_across_feed_break(_at(16, 1))

        assert agg._last_close_author == {}
        assert agg._last_close == {}
