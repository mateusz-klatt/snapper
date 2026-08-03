"""Unit tests for the On-Balance Volume indicator."""

import pandas as pd
import pytest

from snapper.indicators.obv import obv


def test_obv_directions() -> None:
    """OBV accumulates volume by close direction.

    Given: closes that rise, fall and stay flat,
    When: obv is called,
    Then: volume is added, subtracted or held per the close direction.
    """
    close = pd.Series([10.0, 11.0, 10.5, 10.5, 9.0])
    volume = pd.Series([100.0, 120.0, 90.0, 80.0, 70.0])
    result = obv(close, volume)
    assert list(result) == [100.0, 220.0, 130.0, 130.0, 60.0]


def test_obv_empty_series() -> None:
    """OBV handles empty inputs.

    Given: empty close and volume series,
    When: obv is called,
    Then: it returns an empty float series.
    """
    empty = pd.Series([], dtype=float)
    result = obv(empty, empty)
    assert result.empty
    assert result.dtype == float


def test_obv_length_mismatch_raises() -> None:
    """OBV rejects mismatched input lengths.

    Given: close and volume of unequal length,
    When: obv is called,
    Then: it raises ValueError.
    """
    close = pd.Series([1.0, 2.0])
    volume = pd.Series([1.0])
    with pytest.raises(ValueError, match="equal length"):
        obv(close, volume)
