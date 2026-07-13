"""Unit tests for the Bollinger Bands indicator."""

import pandas as pd

from snapper.indicators.bollinger import bollinger


def test_bollinger_band_relationship() -> None:
    """Bollinger bands are ordered.

    Given: a trending price series,
    When: bollinger is called,
    Then: lower <= middle <= upper on all non-NaN rows.
    """
    series = pd.Series([float(x) for x in range(1, 21)])
    bands = bollinger(series, period=5, num_std=2.0)
    valid = bands.dropna()
    assert (valid["lower"] <= valid["middle"]).all()
    assert (valid["middle"] <= valid["upper"]).all()


def test_bollinger_flat_window_collapses() -> None:
    """Bollinger bands collapse on a flat window.

    Given: a constant-value window,
    When: bollinger is called,
    Then: the population std is zero and all three bands equal the value.
    """
    series = pd.Series([5.0, 5.0, 5.0, 5.0])
    bands = bollinger(series, period=2, num_std=2.0)
    assert bands["middle"].iloc[1] == 5.0
    assert bands["upper"].iloc[1] == 5.0
    assert bands["lower"].iloc[1] == 5.0


def test_bollinger_empty_series() -> None:
    """Bollinger handles an empty series.

    Given: an empty series,
    When: bollinger is called,
    Then: it returns empty upper/middle/lower columns.
    """
    bands = bollinger(pd.Series([], dtype=float), period=3)
    assert bands["upper"].empty
    assert bands["middle"].empty
    assert bands["lower"].empty
    assert list(bands.columns) == ["upper", "middle", "lower"]
