"""Unit tests for the Rate of Change indicator."""

import pandas as pd
import pytest

from snapper.indicators.roc import roc


def test_roc_known_values() -> None:
    """ROC returns the percent change over the period.

    Given: a rising price series,
    When: roc is called with period=1,
    Then: each value is the percent change from the prior bar.
    """
    series = pd.Series([100.0, 110.0, 121.0])
    result = roc(series, period=1)
    assert pd.isna(result.iloc[0])
    assert result.iloc[1] == pytest.approx(10.0)
    assert result.iloc[2] == pytest.approx(10.0)


def test_roc_zero_reference_is_zero() -> None:
    """ROC returns zero at a zero reference price, matching TA-Lib.

    Given: a series whose prior price is exactly zero,
    When: roc is called with period=1,
    Then: that position is 0.0 (TA-Lib's convention) rather than NaN or infinity.
    """
    result = roc(pd.Series([0.0, 5.0]), period=1)
    assert result.iloc[1] == 0.0


def test_roc_empty_series() -> None:
    """ROC handles an empty series.

    Given: an empty series,
    When: roc is called,
    Then: it returns an empty float series.
    """
    result = roc(pd.Series([], dtype=float), period=10)
    assert result.empty
    assert result.dtype == float
