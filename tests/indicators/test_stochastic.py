"""Unit tests for the fast stochastic oscillator."""

import pandas as pd
import pytest

from snapper.indicators.stochastic import stochastic


def test_stochastic_known_values() -> None:
    """Stochastic %K matches hand computation and stays bounded.

    Given: a small OHLC fixture,
    When: stochastic is called with k_period=2 and d_period=2,
    Then: %K matches by hand and all values fall within [0, 100].
    """
    high = pd.Series([10.0, 11.0, 12.0, 13.0])
    low = pd.Series([9.0, 9.5, 10.5, 11.0])
    close = pd.Series([9.5, 10.5, 11.5, 12.5])
    result = stochastic(high, low, close, k_period=2, d_period=2)
    assert result["k"].iloc[1] == pytest.approx(75.0)
    assert (result["k"].dropna() >= 0).all()
    assert (result["k"].dropna() <= 100).all()


def test_stochastic_flat_window_is_zero() -> None:
    """Stochastic guards against a zero range.

    Given: a flat window where highest high equals lowest low,
    When: stochastic is called,
    Then: %K is defined as 0.0 with no division-by-zero warning.
    """
    flat = pd.Series([5.0, 5.0, 5.0])
    result = stochastic(flat, flat, flat, k_period=2, d_period=2)
    assert result["k"].iloc[1] == 0.0
    assert result["k"].iloc[2] == 0.0


def test_stochastic_empty_series() -> None:
    """Stochastic handles empty inputs.

    Given: empty high/low/close series,
    When: stochastic is called,
    Then: it returns empty k and d columns.
    """
    empty = pd.Series([], dtype=float)
    result = stochastic(empty, empty, empty)
    assert result["k"].empty
    assert result["d"].empty
    assert list(result.columns) == ["k", "d"]


def test_stochastic_length_mismatch_raises() -> None:
    """Stochastic rejects mismatched input lengths.

    Given: high/low/close of unequal length,
    When: stochastic is called,
    Then: it raises ValueError.
    """
    with pytest.raises(ValueError, match="equal length"):
        stochastic(pd.Series([1.0, 2.0]), pd.Series([1.0]), pd.Series([1.0, 2.0]))
