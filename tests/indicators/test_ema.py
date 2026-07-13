"""Unit tests for the Exponential Moving Average indicator."""

import pandas as pd
import pytest

from snapper.indicators.ema import ema


def test_ema_seeds_from_first_value() -> None:
    """EMA seeds from the first observation.

    Given: a price series,
    When: ema is called,
    Then: the first output value equals the first input value.
    """
    series = pd.Series([10.0, 20.0, 30.0])
    result = ema(series, period=3)
    assert result.iloc[0] == 10.0


def test_ema_known_recursive_value() -> None:
    """EMA follows the adjust=False recursion.

    Given: a three-point series and period=2,
    When: ema is called,
    Then: each value matches the hand-computed recursive EMA.
    """
    series = pd.Series([1.0, 2.0, 3.0])
    result = ema(series, period=2)
    alpha = 2.0 / (2.0 + 1.0)
    expected1 = 1.0 + alpha * (2.0 - 1.0)
    expected2 = expected1 + alpha * (3.0 - expected1)
    assert result.iloc[0] == 1.0
    assert result.iloc[1] == pytest.approx(expected1)
    assert result.iloc[2] == pytest.approx(expected2)


def test_ema_empty_series() -> None:
    """EMA handles an empty series.

    Given: an empty series,
    When: ema is called,
    Then: it returns an empty float series.
    """
    result = ema(pd.Series([], dtype=float), period=3)
    assert result.empty
    assert result.dtype == float
