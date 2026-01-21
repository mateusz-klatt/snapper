"""Unit tests for the MACD indicator module."""

import pandas as pd

from snapper.indicators.macd import macd


def test_macd_shapes() -> None:
    """Verify MACD returns DataFrame with expected columns and length.

    Given a numeric pandas Series of 10 values,
    When macd() is called with custom periods (3, 6, 3),
    Then result has columns {'macd', 'signal', 'hist'} and same length as input.
    """
    s = pd.Series([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], dtype=float)
    m = macd(s, 3, 6, 3)
    assert set(m.columns) == {"macd", "signal", "hist"}
    assert len(m) == len(s)


def test_macd_crossovers() -> None:
    """Verify MACD histogram alternates sign on oscillating data.

    Given a pandas Series with oscillating values (up-down pattern),
    When macd() is called with default parameters,
    Then histogram contains both positive and negative values (crossovers).
    """
    s = pd.Series([1, 2, 3, 2, 1, 2, 3, 4, 3, 2, 1, 2, 3], dtype=float)
    m = macd(s)
    assert (m["hist"] > 0).any()
    assert (m["hist"] < 0).any()
