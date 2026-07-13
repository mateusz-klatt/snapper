"""Unit tests for the candle data-quality audit."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from snapper.application.data_quality.candle_audit import CandleAnomalyType
from snapper.application.data_quality.candle_audit import audit_candle_series
from snapper.data.repository_types import CandleRow

_BASE = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)


def _candle(
    open_at: datetime,
    open_price: float = 100.0,
    high: float = 101.0,
    low: float = 99.0,
    close: float = 100.5,
    volume: float = 1000.0,
) -> CandleRow:
    """Build a CandleRow with valid defaults for the audit-relevant fields."""
    return CandleRow(
        open_at=open_at,
        timeframe="1m",
        open=open_price,
        high=high,
        low=low,
        close=close,
        volume=volume,
        vwap=None,
        trades=None,
        source="native",
        complete=True,
        public_id="pub",
        timestamp=open_at,
        session_id="sess",
        sequence_id=0,
    )


def _types(anomalies: list) -> set:
    """Return the set of anomaly types present."""
    return {a.type for a in anomalies}


def test_clean_series_has_no_anomalies() -> None:
    """A well-formed series is clean.

    Given: three aligned, ordered, invariant-respecting 1m candles,
    When: audit_candle_series is called,
    Then: no anomalies are returned.
    """
    candles = [_candle(_BASE + timedelta(minutes=i)) for i in range(3)]
    assert audit_candle_series(candles, "1m") == []


def test_empty_series_has_no_anomalies() -> None:
    """An empty series is clean.

    Given: no candles,
    When: audit_candle_series is called,
    Then: no anomalies are returned.
    """
    assert audit_candle_series([], "1m") == []


def test_ohlc_invariant_violation() -> None:
    """A bar with high below low is flagged.

    Given: a candle whose high is below its low,
    When: audit_candle_series is called,
    Then: an OHLC_INVARIANT anomaly is returned.
    """
    candles = [_candle(_BASE, high=98.0, low=99.0)]
    assert _types(audit_candle_series(candles, "1m")) == {CandleAnomalyType.OHLC_INVARIANT}


def test_ohlc_high_below_open() -> None:
    """A high below the open is flagged in isolation.

    Given: a bar whose high is below open only,
    When: audit_candle_series is called,
    Then: an OHLC_INVARIANT anomaly is returned.
    """
    candles = [_candle(_BASE, open_price=100.0, high=99.5, low=99.0, close=99.2)]
    assert _types(audit_candle_series(candles, "1m")) == {CandleAnomalyType.OHLC_INVARIANT}


def test_ohlc_high_below_close() -> None:
    """A high below the close is flagged in isolation.

    Given: a bar whose high is below close only,
    When: audit_candle_series is called,
    Then: an OHLC_INVARIANT anomaly is returned.
    """
    candles = [_candle(_BASE, open_price=99.0, high=99.5, low=98.5, close=100.0)]
    assert _types(audit_candle_series(candles, "1m")) == {CandleAnomalyType.OHLC_INVARIANT}


def test_ohlc_low_above_open() -> None:
    """A low above the open is flagged in isolation.

    Given: a bar whose low is above open only,
    When: audit_candle_series is called,
    Then: an OHLC_INVARIANT anomaly is returned.
    """
    candles = [_candle(_BASE, open_price=99.0, high=101.0, low=99.5, close=100.0)]
    assert _types(audit_candle_series(candles, "1m")) == {CandleAnomalyType.OHLC_INVARIANT}


def test_ohlc_low_above_close() -> None:
    """A low above the close is flagged in isolation.

    Given: a bar whose low is above close only,
    When: audit_candle_series is called,
    Then: an OHLC_INVARIANT anomaly is returned.
    """
    candles = [_candle(_BASE, open_price=100.0, high=101.0, low=99.5, close=99.0)]
    assert _types(audit_candle_series(candles, "1m")) == {CandleAnomalyType.OHLC_INVARIANT}


def test_non_positive_price() -> None:
    """A non-positive price is flagged and suppresses the invariant check.

    Given: a candle with a zero low,
    When: audit_candle_series is called,
    Then: a NON_POSITIVE_PRICE anomaly is returned.
    """
    candles = [_candle(_BASE, low=0.0)]
    assert _types(audit_candle_series(candles, "1m")) == {CandleAnomalyType.NON_POSITIVE_PRICE}


def test_non_finite_price() -> None:
    """A non-finite price is flagged as non-positive.

    Given: a candle with an infinite high,
    When: audit_candle_series is called,
    Then: a NON_POSITIVE_PRICE anomaly is returned.
    """
    candles = [_candle(_BASE, high=float("inf"))]
    assert CandleAnomalyType.NON_POSITIVE_PRICE in _types(audit_candle_series(candles, "1m"))


def test_negative_volume() -> None:
    """Negative volume is flagged.

    Given: a candle with negative volume,
    When: audit_candle_series is called,
    Then: a NEGATIVE_VOLUME anomaly is returned.
    """
    candles = [_candle(_BASE, volume=-5.0)]
    assert _types(audit_candle_series(candles, "1m")) == {CandleAnomalyType.NEGATIVE_VOLUME}


def test_non_finite_volume() -> None:
    """Non-finite volume is flagged as negative volume.

    Given: a candle with NaN volume,
    When: audit_candle_series is called,
    Then: a NEGATIVE_VOLUME anomaly is returned.
    """
    candles = [_candle(_BASE, volume=float("nan"))]
    assert CandleAnomalyType.NEGATIVE_VOLUME in _types(audit_candle_series(candles, "1m"))


def test_misaligned_open_at() -> None:
    """A bar off the timeframe grid is flagged.

    Given: a 1m candle opening at 30 seconds past the minute,
    When: audit_candle_series is called,
    Then: a MISALIGNED_OPEN_AT anomaly is returned.
    """
    candles = [_candle(_BASE + timedelta(seconds=30))]
    assert _types(audit_candle_series(candles, "1m")) == {CandleAnomalyType.MISALIGNED_OPEN_AT}


def test_daily_alignment_is_not_checked() -> None:
    """Daily bars are not alignment-checked (venue-specific).

    Given: a 1d candle opening at a non-midnight time,
    When: audit_candle_series is called with timeframe 1d,
    Then: no MISALIGNED_OPEN_AT anomaly is returned.
    """
    candles = [_candle(_BASE + timedelta(hours=13))]
    assert CandleAnomalyType.MISALIGNED_OPEN_AT not in _types(audit_candle_series(candles, "1d"))


def test_thirty_minute_series_is_supported() -> None:
    """The 30m timeframe is a supported, first-class candle interval.

    Given: three aligned, ordered 30m candles,
    When: audit_candle_series is called with timeframe 30m,
    Then: no anomalies are returned (no ValueError is raised).
    """
    candles = [_candle(_BASE + timedelta(minutes=30 * i)) for i in range(3)]
    assert audit_candle_series(candles, "30m") == []


def test_thirty_minute_misaligned() -> None:
    """A 30m bar off the half-hour grid is flagged.

    Given: a 30m candle opening 15 minutes past the half hour,
    When: audit_candle_series is called with timeframe 30m,
    Then: a MISALIGNED_OPEN_AT anomaly is returned.
    """
    candles = [_candle(_BASE + timedelta(minutes=15))]
    assert _types(audit_candle_series(candles, "30m")) == {CandleAnomalyType.MISALIGNED_OPEN_AT}


def test_thirty_minute_gap_flagged() -> None:
    """A gap in a 30m series is flagged with the missing count.

    Given: two 30m candles 90 minutes apart,
    When: audit_candle_series is called with timeframe 30m,
    Then: a GAP anomaly noting two missing bars is returned.
    """
    candles = [_candle(_BASE), _candle(_BASE + timedelta(minutes=90))]
    gaps = [a for a in audit_candle_series(candles, "30m") if a.type is CandleAnomalyType.GAP]
    assert len(gaps) == 1
    assert "2 missing" in gaps[0].detail


def test_anchor_offset_aligns_venue_bars() -> None:
    """A venue-anchored bar aligns under an anchor offset.

    Given: a 1h bar opening at 30 minutes past the hour,
    When: audit_candle_series is called with anchor_offset_seconds=1800,
    Then: no misalignment is reported, but it is reported without the offset.
    """
    candle = [_candle(datetime(2026, 1, 1, 14, 30, 0, tzinfo=UTC))]
    aligned = audit_candle_series(candle, "1h", anchor_offset_seconds=1800)
    assert CandleAnomalyType.MISALIGNED_OPEN_AT not in _types(aligned)
    unaligned = audit_candle_series(candle, "1h")
    assert CandleAnomalyType.MISALIGNED_OPEN_AT in _types(unaligned)


def test_duplicate_open_at() -> None:
    """Two bars sharing an open_at are flagged.

    Given: two candles with the same open_at,
    When: audit_candle_series is called,
    Then: a DUPLICATE_OPEN_AT anomaly is returned.
    """
    candles = [_candle(_BASE), _candle(_BASE)]
    assert CandleAnomalyType.DUPLICATE_OPEN_AT in _types(audit_candle_series(candles, "1m"))


def test_out_of_order() -> None:
    """A bar earlier than its predecessor is flagged.

    Given: a second candle whose open_at precedes the first,
    When: audit_candle_series is called,
    Then: an OUT_OF_ORDER anomaly is returned.
    """
    candles = [_candle(_BASE + timedelta(minutes=1)), _candle(_BASE)]
    assert _types(audit_candle_series(candles, "1m")) == {CandleAnomalyType.OUT_OF_ORDER}


def test_non_adjacent_duplicate() -> None:
    """A non-adjacent duplicate is flagged as both duplicate and out of order.

    Given: a bar whose open_at repeats a non-adjacent earlier bar and so also
        precedes its immediate predecessor,
    When: audit_candle_series is called,
    Then: both DUPLICATE_OPEN_AT and OUT_OF_ORDER anomalies are returned, since
        the two facts are independent.
    """
    candles = [
        _candle(_BASE),
        _candle(_BASE + timedelta(minutes=1)),
        _candle(_BASE),
    ]
    assert _types(audit_candle_series(candles, "1m")) == {
        CandleAnomalyType.DUPLICATE_OPEN_AT,
        CandleAnomalyType.OUT_OF_ORDER,
    }


def test_permuted_complete_series_has_no_false_gap_or_split() -> None:
    """A complete-but-permuted series is flagged only as out of order.

    Given: four consecutive bars whose closes rise gradually (each chronological
        step under the split threshold) delivered in a shuffled order in which
        adjacent-delivery close moves would exceed the threshold,
    When: audit_candle_series is called,
    Then: only OUT_OF_ORDER is reported, with no spurious GAP or SPLIT_SUSPECT,
        because gap and split analysis runs over the sorted unique timestamps.
    """
    candles = [
        _candle(_BASE, open_price=100.0, high=101.0, low=99.0, close=100.0),
        _candle(_BASE + timedelta(minutes=2), open_price=140.0, high=141.0, low=139.0, close=140.0),
        _candle(_BASE + timedelta(minutes=1), open_price=120.0, high=121.0, low=119.0, close=120.0),
        _candle(_BASE + timedelta(minutes=3), open_price=160.0, high=161.0, low=159.0, close=160.0),
    ]
    result = _types(audit_candle_series(candles, "1m"))
    assert CandleAnomalyType.OUT_OF_ORDER in result
    assert CandleAnomalyType.GAP not in result
    assert CandleAnomalyType.SPLIT_SUSPECT not in result


def test_gap_flagged() -> None:
    """A gap in a continuous series is flagged with the missing count.

    Given: two 1m candles three minutes apart,
    When: audit_candle_series is called,
    Then: a GAP anomaly noting two missing bars is returned.
    """
    candles = [_candle(_BASE), _candle(_BASE + timedelta(minutes=3))]
    result = audit_candle_series(candles, "1m")
    gaps = [a for a in result if a.type is CandleAnomalyType.GAP]
    assert len(gaps) == 1
    assert "2 missing" in gaps[0].detail


def test_gap_suppressed_by_expected_gap() -> None:
    """A gap the predicate marks expected is not reported.

    Given: a gap and an expected_gap predicate returning True,
    When: audit_candle_series is called,
    Then: no GAP anomaly is returned.
    """
    candles = [_candle(_BASE), _candle(_BASE + timedelta(minutes=3))]
    result = audit_candle_series(candles, "1m", expected_gap=lambda _prev, _next: True)
    assert CandleAnomalyType.GAP not in _types(result)


def test_gap_not_suppressed_when_predicate_false() -> None:
    """A gap the predicate marks unexpected is still reported.

    Given: a gap and an expected_gap predicate returning False,
    When: audit_candle_series is called,
    Then: a GAP anomaly is returned.
    """
    candles = [_candle(_BASE), _candle(_BASE + timedelta(minutes=3))]
    result = audit_candle_series(candles, "1m", expected_gap=lambda _prev, _next: False)
    assert CandleAnomalyType.GAP in _types(result)


def test_split_suspect() -> None:
    """A large close-to-close jump is flagged.

    Given: two adjacent bars whose close halves,
    When: audit_candle_series is called,
    Then: a SPLIT_SUSPECT anomaly is returned.
    """
    candles = [
        _candle(_BASE, open_price=100.0, high=101.0, low=99.0, close=100.0),
        _candle(_BASE + timedelta(minutes=1), open_price=50.0, high=51.0, low=49.0, close=50.0),
    ]
    assert CandleAnomalyType.SPLIT_SUSPECT in _types(audit_candle_series(candles, "1m"))


def test_split_check_skipped_when_prev_close_non_positive() -> None:
    """The split check is skipped after a non-positive close.

    Given: a first bar with a zero close followed by a normal bar,
    When: audit_candle_series is called,
    Then: no SPLIT_SUSPECT anomaly is produced for the pair.
    """
    candles = [
        _candle(_BASE, open_price=1.0, high=1.0, low=0.0, close=0.0),
        _candle(_BASE + timedelta(minutes=1)),
    ]
    assert CandleAnomalyType.SPLIT_SUSPECT not in _types(audit_candle_series(candles, "1m"))


def test_duplicate_close_conflict_does_not_manufacture_split() -> None:
    """A conflicting duplicate close cannot manufacture or hide a split.

    Given: a duplicated timestamp whose two rows carry conflicting closes,
        supplied in either delivery order,
    When: audit_candle_series is called,
    Then: the duplicate is flagged and the anomaly set is identical for both
        orders, with no SPLIT_SUSPECT derived from the ambiguous close.
    """
    first_order = [
        _candle(_BASE, open_price=100.0, high=101.0, low=99.0, close=100.0),
        _candle(_BASE + timedelta(minutes=1), open_price=50.0, high=51.0, low=49.0, close=50.0),
        _candle(_BASE + timedelta(minutes=1), open_price=100.0, high=101.0, low=99.0, close=100.0),
        _candle(_BASE + timedelta(minutes=2), open_price=100.0, high=101.0, low=99.0, close=100.0),
    ]
    second_order = [
        _candle(_BASE, open_price=100.0, high=101.0, low=99.0, close=100.0),
        _candle(_BASE + timedelta(minutes=1), open_price=100.0, high=101.0, low=99.0, close=100.0),
        _candle(_BASE + timedelta(minutes=1), open_price=50.0, high=51.0, low=49.0, close=50.0),
        _candle(_BASE + timedelta(minutes=2), open_price=100.0, high=101.0, low=99.0, close=100.0),
    ]
    first = _types(audit_candle_series(first_order, "1m"))
    second = _types(audit_candle_series(second_order, "1m"))
    assert first == second == {CandleAnomalyType.DUPLICATE_OPEN_AT}


def test_gap_count_uses_grid_slots_under_jitter() -> None:
    """Gap counting assigns each bar to its nearest grid slot under jitter.

    Given: a 1m pair 20 seconds and 2m40s past the anchor (both off-grid),
    When: audit_candle_series is called,
    Then: the GAP reports two missing bars (nearest slots 0 and 3), which the
        old span-rounding would undercount as one.
    """
    candles = [
        _candle(_BASE + timedelta(seconds=20)),
        _candle(_BASE + timedelta(minutes=2, seconds=40)),
    ]
    gaps = [a for a in audit_candle_series(candles, "1m") if a.type is CandleAnomalyType.GAP]
    assert len(gaps) == 1
    assert "2 missing" in gaps[0].detail


def test_gap_count_respects_anchor_offset() -> None:
    """Gap counting honors the venue anchor offset.

    Given: two 1h bars at :30 and 2:30 past the hour under a 1800s anchor,
    When: audit_candle_series is called with anchor_offset_seconds=1800,
    Then: one missing bar is reported and neither bar is misaligned.
    """
    candles = [
        _candle(datetime(2026, 1, 1, 14, 30, 0, tzinfo=UTC)),
        _candle(datetime(2026, 1, 1, 16, 30, 0, tzinfo=UTC)),
    ]
    result = audit_candle_series(candles, "1h", anchor_offset_seconds=1800)
    gaps = [a for a in result if a.type is CandleAnomalyType.GAP]
    assert CandleAnomalyType.MISALIGNED_OPEN_AT not in _types(result)
    assert len(gaps) == 1
    assert "1 missing" in gaps[0].detail


def test_unsupported_timeframe_raises() -> None:
    """An unsupported timeframe is rejected.

    Given: a timeframe not in the supported set,
    When: audit_candle_series is called,
    Then: it raises ValueError.
    """
    with pytest.raises(ValueError, match="unsupported timeframe"):
        audit_candle_series([_candle(_BASE)], "2m")


def test_naive_open_at_is_treated_as_utc() -> None:
    """A naive open_at is coerced to UTC for the grid check.

    Given: aligned naive-datetime 1m candles,
    When: audit_candle_series is called,
    Then: no misalignment is reported.
    """
    naive = [
        _candle(datetime(2026, 1, 1, 0, 0, 0)),
        _candle(datetime(2026, 1, 1, 0, 1, 0)),
    ]
    assert audit_candle_series(naive, "1m") == []
