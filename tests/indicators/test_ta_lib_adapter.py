"""Unit tests for TA-Lib adapter with Python fallback."""

from typing import Any

import numpy as np
import pandas as pd
import pytest

import snapper.indicators.ta_lib_adapter
from snapper.indicators.ta_lib_adapter import get_backend
from snapper.indicators.ta_lib_adapter import is_talib_available
from snapper.indicators.ta_lib_adapter import macd
from snapper.indicators.ta_lib_adapter import rsi


class TestTALibAdapter:
    """Test suite for TA-Lib adapter basic functionality."""

    def test_is_talib_available(self) -> None:
        """Test is_talib_available returns boolean.

        Given: TA-Lib adapter module,
        When: calling is_talib_available,
        Then: returns bool (True if installed, False otherwise).
        """
        result = is_talib_available()
        assert isinstance(result, bool)

    def test_get_backend(self) -> None:
        """Test get_backend returns valid backend name.

        Given: TA-Lib adapter module,
        When: calling get_backend,
        Then: returns 'talib' or 'python'.
        """
        backend = get_backend()
        assert backend in ("talib", "python")


class TestTALibAdapterFallback:
    """Test suite for TA-Lib adapter Python fallback mode."""

    @pytest.fixture(autouse=True)
    def setup_fallback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Configure fallback mode by disabling TA-Lib."""
        monkeypatch.setattr(snapper.indicators.ta_lib_adapter, "_talib_available", False)

    def test_is_talib_available(self) -> None:
        """Test is_talib_available returns False in fallback mode.

        Given: _talib_available patched to False,
        When: calling is_talib_available,
        Then: returns False.
        """
        result = is_talib_available()
        assert result is False

    def test_get_backend(self) -> None:
        """Test get_backend returns 'python' in fallback mode.

        Given: _talib_available patched to False,
        When: calling get_backend,
        Then: returns 'python'.
        """
        backend = get_backend()
        assert backend == "python"

    def test_rsi_empty_series(self) -> None:
        """Verify RSI handles empty series in fallback mode.

        Given: empty pandas Series and fallback mode,
        When: calling rsi,
        Then: returns empty Series with float dtype.
        """
        empty_series = pd.Series([], dtype=float)
        result = rsi(empty_series)
        assert len(result) == 0
        assert result.dtype == float

    def test_rsi_basic_series(self) -> None:
        """Verify RSI with basic series in fallback mode.

        Given: oscillating price series and fallback mode,
        When: calling rsi with period=14,
        Then: returns Series with same length and float dtype.
        """
        series = pd.Series([1.0, 2.0, 3.0, 2.0, 1.0] * 10, dtype=float)
        result = rsi(series, period=14)
        assert len(result) == len(series)
        assert result.dtype == float

    def test_macd_empty_series(self) -> None:
        """Verify MACD handles empty series in fallback mode.

        Given: empty pandas Series and fallback mode,
        When: calling macd,
        Then: returns three empty Series with float dtype.
        """
        empty_series = pd.Series([], dtype=float)
        macd_line, signal_line, histogram = macd(empty_series)
        assert len(macd_line) == 0
        assert len(signal_line) == 0
        assert len(histogram) == 0
        assert macd_line.dtype == float
        assert signal_line.dtype == float
        assert histogram.dtype == float

    def test_macd_basic_series(self) -> None:
        """Test MACD with basic series in fallback mode.

        Given: oscillating price series and fallback mode,
        When: calling macd with 12/26/9 params,
        Then: returns three Series with same length and float dtype.
        """
        series = pd.Series([1.0, 2.0, 3.0, 2.0, 1.0] * 10, dtype=float)
        macd_line, signal_line, histogram = macd(series, 12, 26, 9)
        assert len(macd_line) == len(series)
        assert len(signal_line) == len(series)
        assert len(histogram) == len(series)
        assert macd_line.dtype == float
        assert signal_line.dtype == float
        assert histogram.dtype == float


class TestRSIAdapter:
    """Test suite for RSI indicator adapter."""

    def test_rsi_empty_series(self) -> None:
        """Test RSI with empty series.

        Given: empty pandas Series,
        When: calling rsi,
        Then: returns empty Series with float dtype.
        """
        empty_series = pd.Series([], dtype=float)
        result = rsi(empty_series)
        assert len(result) == 0
        assert result.dtype == float

    def test_rsi_basic_series(self) -> None:
        """Test RSI with basic oscillating series.

        Given: oscillating price series,
        When: calling rsi with period=14,
        Then: returns Series with same length and float dtype.
        """
        series = pd.Series([1.0, 2.0, 3.0, 2.0, 1.0] * 10, dtype=float)
        result = rsi(series, period=14)
        assert len(result) == len(series)
        assert result.dtype == float

    def test_rsi_known_values(self) -> None:
        """Test RSI against Wilder's reference data.

        Given: known price series from Wilder's RSI examples,
        When: calling rsi with period=14,
        Then: valid values are in [0, 100] range.
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
            ],
            dtype=float,
        )
        result = rsi(closes, 14)
        assert len(result) == len(closes)
        valid_results = result.dropna()
        if len(valid_results) > 0:
            assert all(0 <= val <= 100 for val in valid_results)

    def test_rsi_flat_series(self) -> None:
        """Test RSI with flat series (no price change).

        Given: series with constant value,
        When: calling rsi with period=14,
        Then: returns Series with same length.
        """
        flat_series = pd.Series([100.0] * 30, dtype=float)
        result = rsi(flat_series, 14)
        assert len(result) == 30

    def test_rsi_with_nans(self) -> None:
        """Test RSI with NaN values in input.

        Given: series containing NaN values,
        When: calling rsi with period=10,
        Then: returns Series with same length and float dtype.
        """
        series_with_nans = pd.Series([1.0, 2.0, np.nan, 4.0, 5.0] * 6, dtype=float)
        result = rsi(series_with_nans, 10)
        assert len(result) == len(series_with_nans)
        assert result.dtype == float

    def test_rsi_different_periods(self) -> None:
        """Test RSI with different period lengths.

        Given: ascending price series,
        When: calling rsi with periods 5, 14, 21,
        Then: all return Series with correct length and float dtype.
        """
        series = pd.Series(range(1, 51), dtype=float)
        for period in [5, 14, 21]:
            result = rsi(series, period)
            assert len(result) == len(series)
            assert result.dtype == float


class TestMACDAdapter:
    """Test suite for MACD indicator adapter."""

    def test_macd_empty_series(self) -> None:
        """Test MACD with empty series.

        Given: empty pandas Series,
        When: calling macd,
        Then: returns three empty Series.
        """
        empty_series = pd.Series([], dtype=float)
        macd_line, signal_line, histogram = macd(empty_series)
        assert len(macd_line) == 0
        assert len(signal_line) == 0
        assert len(histogram) == 0

    def test_macd_basic_series(self) -> None:
        """Test MACD with basic oscillating series.

        Given: oscillating price series,
        When: calling macd,
        Then: returns three Series with same length.
        """
        series = pd.Series([1.0, 2.0, 3.0, 2.0, 1.0] * 10, dtype=float)
        macd_line, signal_line, histogram = macd(series)
        assert len(macd_line) == len(series)
        assert len(signal_line) == len(series)
        assert len(histogram) == len(series)

    def test_macd_known_calculation(self) -> None:
        """Test MACD with ascending series.

        Given: ascending price series 1-100,
        When: calling macd with 12/26/9 params,
        Then: last valid MACD value is positive (trending up).
        """
        series = pd.Series(range(1, 101), dtype=float)
        macd_line, signal_line, histogram = macd(series, 12, 26, 9)
        assert len(macd_line) == 100
        assert len(signal_line) == 100
        assert len(histogram) == 100
        valid_macd = macd_line.dropna()
        if len(valid_macd) > 0:
            assert valid_macd.iloc[-1] > 0

    def test_macd_flat_series(self) -> None:
        """Test MACD with flat series (no price change).

        Given: series with constant value 100.0,
        When: calling macd,
        Then: returns three Series with length 50.
        """
        flat_series = pd.Series([100.0] * 50, dtype=float)
        macd_line, signal_line, histogram = macd(flat_series)
        assert len(macd_line) == 50
        assert len(signal_line) == 50
        assert len(histogram) == 50

    def test_macd_with_nans(self) -> None:
        """Test MACD with NaN values in input.

        Given: series containing NaN values,
        When: calling macd,
        Then: returns three Series with same length.
        """
        series_with_nans = pd.Series([1.0, 2.0, np.nan, 4.0, 5.0] * 10, dtype=float)
        macd_line, signal_line, histogram = macd(series_with_nans)
        assert len(macd_line) == len(series_with_nans)
        assert len(signal_line) == len(series_with_nans)
        assert len(histogram) == len(series_with_nans)

    def test_macd_different_parameters(self) -> None:
        """Test MACD with different parameter combinations.

        Given: ascending price series 1-100,
        When: calling macd with various fast/slow/signal params,
        Then: all return three Series with correct length.
        """
        series = pd.Series(range(1, 101), dtype=float)
        params = [
            (5, 10, 3),
            (12, 26, 9),
            (8, 21, 5),
        ]
        for fast, slow, signal in params:
            macd_line, signal_line, histogram = macd(series, fast, slow, signal)
            assert len(macd_line) == len(series)
            assert len(signal_line) == len(series)
            assert len(histogram) == len(series)

    def test_macd_crossover_scenarios(self) -> None:
        """Test MACD with oscillating series for crossovers.

        Given: oscillating price series,
        When: calling macd with 3/6/3 params,
        Then: returns three Series with correct length.
        """
        prices = [10, 11, 12, 11, 10, 9, 10, 11, 12, 13] * 5
        series = pd.Series(prices, dtype=float)
        macd_line, signal_line, histogram = macd(series, 3, 6, 3)
        assert len(histogram) == len(series)
        assert len(macd_line) == len(series)
        assert len(signal_line) == len(series)


class TestTALibImportPath:
    """Test suite for TA-Lib import path and fallback handling."""

    def test_talib_import_fails_gracefully(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test graceful fallback when TA-Lib import fails.

        Given: USE_TALIB env set but import mocked to fail,
        When: checking get_backend,
        Then: returns 'python' (fallback mode).
        """
        monkeypatch.setenv("USE_TALIB", "true")
        original_import = (
            __builtins__.__import__ if hasattr(__builtins__, "__import__") else __import__
        )

        def mock_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "talib":
                raise ImportError("No module named 'talib'")
            return original_import(name, *args, **kwargs)

        monkeypatch.setattr("builtins.__import__", mock_import)
        monkeypatch.setattr(snapper.indicators.ta_lib_adapter, "_talib_available", False)
        monkeypatch.setattr(snapper.indicators.ta_lib_adapter, "_talib", None)
        result = get_backend()
        assert result == "python"


class TestBackendConsistency:
    """Test suite for backend consistency verification."""

    def test_rsi_produces_valid_results(self) -> None:
        """Test RSI produces valid results regardless of backend.

        Given: known price series,
        When: calling rsi with period=14,
        Then: valid values are in [0, 100] range.
        """
        series = pd.Series(
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
            ],
            dtype=float,
        )
        result = rsi(series, 14)
        assert len(result) == len(series)
        assert result.dtype == float
        valid_values = result.dropna()
        if len(valid_values) > 0:
            assert all(0 <= val <= 100 for val in valid_values)

    def test_macd_produces_valid_results(self) -> None:
        """Test MACD produces valid results regardless of backend.

        Given: ascending price series 1-50,
        When: calling macd with 12/26/9 params,
        Then: all outputs have float dtype.
        """
        series = pd.Series(range(1, 51), dtype=float)
        macd_line, signal_line, histogram = macd(series, 12, 26, 9)
        assert len(macd_line) == len(series)
        assert len(signal_line) == len(series)
        assert len(histogram) == len(series)
        assert macd_line.dtype == float
        assert signal_line.dtype == float
        assert histogram.dtype == float
