"""Unit tests for the Average True Range indicator."""

import pandas as pd
import pytest

from snapper.indicators.atr import atr


def _ohlc() -> tuple[pd.Series, pd.Series, pd.Series]:
    """Return a small fixed OHLC fixture for ATR tests."""
    high = pd.Series([10.0, 11.0, 12.0, 11.5, 12.5])
    low = pd.Series([9.0, 9.5, 10.5, 10.0, 11.0])
    close = pd.Series([9.5, 10.5, 11.5, 10.5, 12.0])
    return high, low, close


def test_atr_known_values() -> None:
    """ATR matches Wilder's seed and smoothing step.

    Given: a small OHLC fixture,
    When: atr is called with period=2,
    Then: the seed excludes TR[0], starts at index period, and Wilder-smooths.
    """
    high, low, close = _ohlc()
    result = atr(high, low, close, period=2)
    assert pd.isna(result.iloc[0])
    assert pd.isna(result.iloc[1])
    assert result.iloc[2] == pytest.approx(1.5)
    assert result.iloc[3] == pytest.approx(1.5)
    assert result.iloc[4] == pytest.approx(1.75)
    assert (result.dropna() >= 0).all()


def test_atr_empty_series() -> None:
    """ATR handles empty inputs.

    Given: empty high/low/close series,
    When: atr is called,
    Then: it returns an empty float series.
    """
    empty = pd.Series([], dtype=float)
    result = atr(empty, empty, empty, period=14)
    assert result.empty
    assert result.dtype == float


def test_atr_shorter_than_period_all_nan() -> None:
    """ATR warmup covers short series.

    Given: fewer bars than the period,
    When: atr is called,
    Then: all values are NaN.
    """
    high, low, close = _ohlc()
    result = atr(high.iloc[:3], low.iloc[:3], close.iloc[:3], period=14)
    assert result.isna().all()
    assert len(result) == 3


def test_atr_length_mismatch_raises() -> None:
    """ATR rejects mismatched input lengths.

    Given: high/low/close of unequal length,
    When: atr is called,
    Then: it raises ValueError.
    """
    with pytest.raises(ValueError, match="equal length"):
        atr(pd.Series([1.0, 2.0]), pd.Series([1.0]), pd.Series([1.0, 2.0]))
