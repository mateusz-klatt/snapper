"""Technical Analysis Library adapter with fallback to pure Python.

This module provides indicator functions that use TA-Lib when available
for performance, with automatic fallback to pure Python implementations.

TA-Lib availability is controlled by:
    - USE_TALIB environment variable (default: 'true')
    - Actual TA-Lib installation presence

Example:
    Using the adapter (automatically selects backend)::

        from snapper.indicators.ta_lib_adapter import rsi, macd

        rsi_values = rsi(prices, period=14)
        macd_line, signal_line, histogram = macd(prices)

    Check which backend is active::

        from snapper.indicators.ta_lib_adapter import get_backend
        print(get_backend())  # 'talib' or 'python'
"""

import os
from typing import Any

import pandas as pd

from snapper.indicators.macd import macd as python_macd
from snapper.indicators.rsi import rsi as python_rsi

USE_TALIB = os.getenv("USE_TALIB", "true").lower() in ("true", "1", "yes")
_talib_available = False
_talib: Any = None
try:
    if USE_TALIB:
        import talib as _talib

        _talib_available = True
except ImportError:
    _talib_available = False


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Calculate RSI using TA-Lib or Python fallback.

    Args:
        series: Price series.
        period: RSI period. Default is 14.

    Returns:
        Series of RSI values.
    """
    if not _talib_available or _talib is None:
        return python_rsi(series, period)
    if len(series) == 0:
        return pd.Series([], dtype=float, index=series.index)
    values = series.astype(float).values
    rsi_values = _talib.RSI(values, timeperiod=period)
    return pd.Series(rsi_values, index=series.index, dtype=float)


def macd(
    series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """Calculate MACD using TA-Lib or Python fallback.

    Args:
        series: Price series.
        fast: Fast EMA period. Default is 12.
        slow: Slow EMA period. Default is 26.
        signal: Signal line period. Default is 9.

    Returns:
        Tuple of (macd_line, signal_line, histogram) as pandas Series.
    """
    if not _talib_available or _talib is None:
        result = python_macd(series, fast, slow, signal)
        return result["macd"], result["signal"], result["hist"]
    if len(series) == 0:
        empty_series = pd.Series([], dtype=float, index=series.index)
        return empty_series, empty_series, empty_series
    values = series.astype(float).values
    macd_line, signal_line, histogram = _talib.MACD(
        values, fastperiod=fast, slowperiod=slow, signalperiod=signal
    )
    return (
        pd.Series(macd_line, index=series.index, dtype=float),
        pd.Series(signal_line, index=series.index, dtype=float),
        pd.Series(histogram, index=series.index, dtype=float),
    )


def is_talib_available() -> bool:
    """Check if TA-Lib is available.

    Returns:
        True if TA-Lib is installed and USE_TALIB is enabled.
    """
    return _talib_available


def get_backend() -> str:
    """Get the current indicator backend name.

    Returns:
        'talib' if using TA-Lib, 'python' otherwise.
    """
    return "talib" if _talib_available else "python"
