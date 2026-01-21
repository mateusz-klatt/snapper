"""Unit tests for the RSI indicator module."""

import numpy as np
import pandas as pd
import pytest

from snapper.indicators.rsi import rsi


def test_rsi_known_values() -> None:
    """Verify RSI matches Wilder's published example values.

    Given the exact price series from Wilder's RSI documentation,
    When rsi() is calculated with period=14,
    Then values at indices 14, 15, 16 match ~70.53, ~66.32, ~66.62.
    """
    closes = pd.Series(
        [
            44.34,
            44.09,
            44.15,
            43.61,
            44.33,
            44.83,
            45.10,
            45.42,
            45.84,
            46.08,
            45.89,
            46.03,
            45.61,
            46.28,
            46.28,
            46.00,
            46.03,
            46.41,
            46.22,
            45.64,
            46.21,
            46.25,
            45.71,
            46.45,
            45.78,
            45.35,
            44.03,
            44.18,
            44.22,
            44.57,
            43.42,
            42.66,
            43.13,
        ]
    )
    out = rsi(closes, 14)
    assert round(out.iloc[14], 2) == pytest.approx(70.53, abs=0.1)
    assert round(out.iloc[15], 2) == pytest.approx(66.32, abs=0.1)
    assert round(out.iloc[16], 2) == pytest.approx(66.62, abs=0.2)


def test_rsi_bounds() -> None:
    """Verify RSI output is always within [0, 100] range.

    Given a trending price series (linear 1 to 2 over 100 points),
    When rsi() is calculated,
    Then all values are >= 0 and <= 100.
    """
    s = pd.Series(np.linspace(1, 2, 100))
    out = rsi(s, 14)
    assert (out >= 0).all()
    assert (out <= 100).all()


def test_rsi_empty_and_short_series() -> None:
    """Verify RSI handles edge cases gracefully.

    Given empty series, When rsi() is called, Then returns empty series.
    Given flat series (all same value) shorter than period,
    When rsi() is called, Then returns all zeros.
    """
    assert rsi(pd.Series([], dtype=float), 14).empty
    s = pd.Series([1.0] * 10)
    out = rsi(s, 14)
    assert (out == 0).all()
