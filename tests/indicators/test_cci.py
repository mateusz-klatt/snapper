"""Unit tests for the Commodity Channel Index indicator."""

import pandas as pd
import pytest

from snapper.indicators.cci import cci


def test_cci_known_values() -> None:
    """CCI matches a hand-computed value.

    Given: a small OHLC fixture,
    When: cci is called with period=2,
    Then: the values match (TP - SMA) / (0.015 * mean_abs_dev).
    """
    high = pd.Series([10.0, 11.0, 12.0])
    low = pd.Series([9.0, 10.0, 11.0])
    close = pd.Series([9.5, 10.5, 11.5])
    result = cci(high, low, close, period=2)
    assert pd.isna(result.iloc[0])
    assert result.iloc[1] == pytest.approx(66.6667, rel=1e-4)
    assert result.iloc[2] == pytest.approx(66.6667, rel=1e-4)


def test_cci_flat_window_is_nan() -> None:
    """CCI guards a zero mean deviation.

    Given: a flat typical-price window,
    When: cci is called,
    Then: the zero mean-deviation position is NaN, not infinity.
    """
    flat = pd.Series([5.0, 5.0, 5.0])
    result = cci(flat, flat, flat, period=2)
    assert result.iloc[1:].isna().all()


def test_cci_empty_series() -> None:
    """CCI handles an empty series.

    Given: empty inputs,
    When: cci is called,
    Then: it returns an empty float series.
    """
    empty = pd.Series([], dtype=float)
    result = cci(empty, empty, empty, period=20)
    assert result.empty
    assert result.dtype == float


def test_cci_length_mismatch_raises() -> None:
    """CCI rejects mismatched input lengths.

    Given: high/low/close of unequal length,
    When: cci is called,
    Then: it raises ValueError.
    """
    with pytest.raises(ValueError, match="equal length"):
        cci(pd.Series([1.0, 2.0]), pd.Series([1.0]), pd.Series([1.0, 2.0]))
