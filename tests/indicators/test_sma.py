"""Unit tests for the Simple Moving Average indicator."""

import pandas as pd

from snapper.indicators.sma import sma


def test_sma_known_values() -> None:
    """SMA returns trailing-window means.

    Given: a short price series,
    When: sma is called with period=3,
    Then: the first two values are NaN and the rest are trailing means.
    """
    series = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    result = sma(series, period=3)
    assert pd.isna(result.iloc[0])
    assert pd.isna(result.iloc[1])
    assert result.iloc[2] == 2.0
    assert result.iloc[3] == 3.0
    assert result.iloc[4] == 4.0


def test_sma_period_one_is_identity() -> None:
    """SMA with period 1 is the identity.

    Given: a price series,
    When: sma is called with period=1,
    Then: the output equals the input values.
    """
    series = pd.Series([10.0, 20.0, 30.0])
    result = sma(series, period=1)
    assert list(result) == [10.0, 20.0, 30.0]


def test_sma_empty_series() -> None:
    """SMA handles an empty series.

    Given: an empty series,
    When: sma is called,
    Then: it returns an empty float series.
    """
    result = sma(pd.Series([], dtype=float), period=3)
    assert result.empty
    assert result.dtype == float


def test_sma_shorter_than_period_all_nan() -> None:
    """SMA warmup covers short series.

    Given: fewer points than the period,
    When: sma is called,
    Then: all values are NaN.
    """
    result = sma(pd.Series([1.0, 2.0]), period=5)
    assert result.isna().all()
    assert len(result) == 2
