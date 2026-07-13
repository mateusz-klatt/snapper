"""Unit tests for the Money Flow Index indicator."""

import pandas as pd
import pytest

from snapper.indicators.mfi import mfi


def test_mfi_all_up_window_is_100() -> None:
    """MFI is 100 for an all-up window.

    Given: a strictly rising typical price,
    When: mfi is called with period=2,
    Then: the negative money flow is zero and MFI is 100.
    """
    high = pd.Series([10.0, 11.0, 12.0])
    low = pd.Series([9.0, 10.0, 11.0])
    close = pd.Series([9.5, 10.5, 11.5])
    volume = pd.Series([100.0, 100.0, 100.0])
    result = mfi(high, low, close, volume, period=2)
    assert pd.isna(result.iloc[0])
    assert result.iloc[1] == pytest.approx(100.0)
    assert result.iloc[2] == pytest.approx(100.0)


def test_mfi_dead_window_is_zero() -> None:
    """MFI is zero over a dead window, matching TA-Lib.

    Given: a flat window with no money flow (both sums zero),
    When: mfi is called with period=2,
    Then: the value is 0.0, not 100, matching TA-Lib.
    """
    flat = pd.Series([10.0, 10.0, 10.0])
    volume = pd.Series([100.0, 100.0, 100.0])
    result = mfi(flat, flat, flat, volume, period=2)
    assert pd.isna(result.iloc[0])
    assert result.iloc[1] == 0.0
    assert result.iloc[2] == 0.0


def test_mfi_bounded_range() -> None:
    """MFI stays within [0, 100].

    Given: an oscillating OHLCV series,
    When: mfi is called with period=3,
    Then: all non-NaN values fall within [0, 100].
    """
    close = pd.Series([10.0, 11.0, 10.5, 11.5, 10.0, 12.0, 11.0])
    high = close + 0.5
    low = close - 0.5
    volume = pd.Series([100.0, 120.0, 90.0, 110.0, 80.0, 130.0, 95.0])
    result = mfi(high, low, close, volume, period=3)
    valid = result.dropna()
    assert (valid >= 0).all()
    assert (valid <= 100).all()


def test_mfi_empty_series() -> None:
    """MFI handles an empty series.

    Given: empty inputs,
    When: mfi is called,
    Then: it returns an empty float series.
    """
    empty = pd.Series([], dtype=float)
    result = mfi(empty, empty, empty, empty, period=14)
    assert result.empty
    assert result.dtype == float


def test_mfi_length_mismatch_raises() -> None:
    """MFI rejects mismatched input lengths.

    Given: OHLCV series of unequal length,
    When: mfi is called,
    Then: it raises ValueError.
    """
    with pytest.raises(ValueError, match="equal length"):
        mfi(pd.Series([1.0, 2.0]), pd.Series([1.0]), pd.Series([1.0, 2.0]), pd.Series([1.0, 2.0]))
