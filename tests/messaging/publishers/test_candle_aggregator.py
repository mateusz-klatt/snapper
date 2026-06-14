"""Unit tests for the higher-timeframe candle synthesis aggregator.

Cover the Phase-1 contract: 1m finalization (exactly-once folding of the final
value despite many in-progress frames), UTC boundary alignment, emit-on-close
via a per-symbol watermark, OHLCV/VWAP roll-up, late-frame dropping, and the
restart seed path (including the cross-timeframe non-suppression watchpoint).
"""

from datetime import UTC
from datetime import datetime

import pytest

from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.messaging.publishers.candle_aggregator import CandleAggregator

_BASE = datetime(2026, 6, 14, tzinfo=UTC)


def _at(hour: int, minute: int, *, day: int = 14) -> datetime:
    """Return a UTC datetime on the test day at the given hour/minute."""
    return datetime(2026, 6, day, hour, minute, tzinfo=UTC)


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

    def test_zero_volume_window_yields_zero_vwap(self) -> None:
        """Given a window of zero-volume 1m, when closed, then VWAP is 0."""
        agg = CandleAggregator(["5m"])
        agg.fold(_candle("A", _at(10, 0), close=10.0, volume=0.0))
        agg.fold(_candle("A", _at(10, 1), close=11.0, volume=0.0))
        agg.fold(_candle("A", _at(10, 5)))
        emitted = agg.fold(_candle("A", _at(10, 6)))
        _label, bar = emitted[0]
        assert bar.volume == 0.0
        assert bar.vwap == 0.0


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
        """A window whose opening minute was never observed is not emitted."""
        agg = CandleAggregator(["5m"])
        agg.fold(_candle("A", _at(10, 2)))
        agg.fold(_candle("A", _at(10, 3)))
        agg.fold(_candle("A", _at(10, 5)))
        assert agg.fold(_candle("A", _at(10, 6))) == []

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
