"""Unit tests for the Keltner Channels indicator."""

import pandas as pd
import pytest

from snapper.indicators.keltner import keltner


def test_keltner_band_relationship() -> None:
    """Keltner bands are ordered around the EMA middle line.

    Given: a trending OHLC series,
    When: keltner is called,
    Then: lower <= middle <= upper on all non-NaN rows.
    """
    close = pd.Series([float(x) for x in range(1, 31)])
    high = close + 1.0
    low = close - 1.0
    bands = keltner(high, low, close, period=5, atr_period=5, mult=2.0)
    valid = bands.dropna()
    assert (valid["lower"] <= valid["middle"]).all()
    assert (valid["middle"] <= valid["upper"]).all()


def test_keltner_empty_series() -> None:
    """Keltner handles an empty series.

    Given: empty inputs,
    When: keltner is called,
    Then: it returns empty upper/middle/lower columns.
    """
    empty = pd.Series([], dtype=float)
    bands = keltner(empty, empty, empty)
    assert bands["upper"].empty
    assert list(bands.columns) == ["upper", "middle", "lower"]


def test_keltner_length_mismatch_raises() -> None:
    """Keltner rejects mismatched input lengths.

    Given: high/low/close of unequal length,
    When: keltner is called,
    Then: it raises ValueError.
    """
    high = pd.Series([1.0, 2.0])
    short_low = pd.Series([1.0])
    close = pd.Series([1.0, 2.0])
    with pytest.raises(ValueError, match="equal length"):
        keltner(high, short_low, close)
