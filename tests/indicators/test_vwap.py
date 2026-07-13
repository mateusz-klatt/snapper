"""Unit tests for the rolling VWAP indicator."""

import pandas as pd
import pytest

from snapper.indicators.vwap import vwap


def test_vwap_known_values() -> None:
    """VWAP is the volume-weighted mean of the typical price.

    Given: a small OHLCV fixture,
    When: vwap is called with period=2,
    Then: each value equals sum(TP*vol)/sum(vol) over the window.
    """
    high = pd.Series([10.0, 11.0, 12.0])
    low = pd.Series([9.0, 10.0, 11.0])
    close = pd.Series([9.5, 10.5, 11.5])
    volume = pd.Series([100.0, 200.0, 100.0])
    result = vwap(high, low, close, volume, period=2)
    assert pd.isna(result.iloc[0])
    assert result.iloc[1] == pytest.approx(3050.0 / 300.0, rel=1e-6)
    assert result.iloc[2] == pytest.approx(3250.0 / 300.0, rel=1e-6)


def test_vwap_zero_volume_window_is_nan() -> None:
    """VWAP guards a zero-volume window.

    Given: a window with zero total volume,
    When: vwap is called,
    Then: that position is NaN rather than infinity.
    """
    high = pd.Series([10.0, 11.0, 12.0])
    low = pd.Series([9.0, 10.0, 11.0])
    close = pd.Series([9.5, 10.5, 11.5])
    volume = pd.Series([0.0, 0.0, 0.0])
    result = vwap(high, low, close, volume, period=2)
    assert result.iloc[1:].isna().all()


def test_vwap_empty_series() -> None:
    """VWAP handles an empty series.

    Given: empty inputs,
    When: vwap is called,
    Then: it returns an empty float series.
    """
    empty = pd.Series([], dtype=float)
    result = vwap(empty, empty, empty, empty, period=14)
    assert result.empty
    assert result.dtype == float


def test_vwap_length_mismatch_raises() -> None:
    """VWAP rejects mismatched input lengths.

    Given: OHLCV series of unequal length,
    When: vwap is called,
    Then: it raises ValueError.
    """
    with pytest.raises(ValueError, match="equal length"):
        vwap(pd.Series([1.0, 2.0]), pd.Series([1.0]), pd.Series([1.0, 2.0]), pd.Series([1.0, 2.0]))
