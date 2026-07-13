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

from snapper.core.types import IndicatorBackend
from snapper.indicators.atr import atr as python_atr
from snapper.indicators.bollinger import bollinger as python_bollinger
from snapper.indicators.ema import ema as python_ema
from snapper.indicators.macd import macd as python_macd
from snapper.indicators.obv import obv as python_obv
from snapper.indicators.rsi import rsi as python_rsi
from snapper.indicators.sma import sma as python_sma
from snapper.indicators.stochastic import stochastic as python_stochastic

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


def sma(series: pd.Series, period: int = 20) -> pd.Series:
    """Calculate SMA using TA-Lib or Python fallback.

    Args:
        series: Price series.
        period: Lookback window. Default is 20.

    Returns:
        Series of SMA values.
    """
    if not _talib_available or _talib is None:
        return python_sma(series, period)
    if len(series) == 0:
        return pd.Series([], dtype=float, index=series.index)
    values = series.astype(float).values
    return pd.Series(_talib.SMA(values, timeperiod=period), index=series.index, dtype=float)


def ema(series: pd.Series, period: int = 20) -> pd.Series:
    """Calculate EMA using TA-Lib or Python fallback.

    TA-Lib seeds the first value with an SMA of the first ``period``
    samples; the Python fallback seeds from the first observation, so the
    two differ during warmup — do not assume cross-backend equality.

    Args:
        series: Price series.
        period: Span for the exponential weighting. Default is 20.

    Returns:
        Series of EMA values.
    """
    if not _talib_available or _talib is None:
        return python_ema(series, period)
    if len(series) == 0:
        return pd.Series([], dtype=float, index=series.index)
    values = series.astype(float).values
    return pd.Series(_talib.EMA(values, timeperiod=period), index=series.index, dtype=float)


def bollinger(series: pd.Series, period: int = 20, num_std: float = 2.0) -> pd.DataFrame:
    """Calculate Bollinger Bands using TA-Lib or Python fallback.

    Args:
        series: Price series.
        period: Lookback window. Default is 20.
        num_std: Number of population standard deviations. Default is 2.0.

    Returns:
        DataFrame with columns ``upper``, ``middle`` and ``lower``.
    """
    if not _talib_available or _talib is None:
        return python_bollinger(series, period, num_std)
    if len(series) == 0:
        empty = pd.Series([], dtype=float, index=series.index)
        return pd.DataFrame({"upper": empty, "middle": empty, "lower": empty})
    values = series.astype(float).values
    upper, middle, lower = _talib.BBANDS(
        values, timeperiod=period, nbdevup=num_std, nbdevdn=num_std, matype=0
    )
    return pd.DataFrame(
        {
            "upper": pd.Series(upper, index=series.index, dtype=float),
            "middle": pd.Series(middle, index=series.index, dtype=float),
            "lower": pd.Series(lower, index=series.index, dtype=float),
        }
    )


def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """Calculate ATR using TA-Lib or Python fallback.

    Args:
        high: Series of high prices.
        low: Series of low prices.
        close: Series of closing prices.
        period: Lookback window. Default is 14.

    Returns:
        Series of ATR values.
    """
    if not _talib_available or _talib is None:
        return python_atr(high, low, close, period)
    if len(close) == 0:
        return pd.Series([], dtype=float, index=close.index)
    result = _talib.ATR(
        high.astype(float).values,
        low.astype(float).values,
        close.astype(float).values,
        timeperiod=period,
    )
    return pd.Series(result, index=close.index, dtype=float)


def stochastic(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    k_period: int = 14,
    d_period: int = 3,
) -> pd.DataFrame:
    """Calculate the fast stochastic oscillator via TA-Lib or Python fallback.

    Maps to TA-Lib ``STOCHF`` (fast %K with an SMA-smoothed %D).

    Args:
        high: Series of high prices.
        low: Series of low prices.
        close: Series of closing prices.
        k_period: Lookback for the %K range. Default is 14.
        d_period: SMA smoothing window for %D. Default is 3.

    Returns:
        DataFrame with columns ``k`` and ``d``.
    """
    if not _talib_available or _talib is None:
        return python_stochastic(high, low, close, k_period, d_period)
    if len(close) == 0:
        empty = pd.Series([], dtype=float, index=close.index)
        return pd.DataFrame({"k": empty, "d": empty})
    fastk, fastd = _talib.STOCHF(
        high.astype(float).values,
        low.astype(float).values,
        close.astype(float).values,
        fastk_period=k_period,
        fastd_period=d_period,
        fastd_matype=0,
    )
    return pd.DataFrame(
        {
            "k": pd.Series(fastk, index=close.index, dtype=float),
            "d": pd.Series(fastd, index=close.index, dtype=float),
        }
    )


def obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    """Calculate OBV using TA-Lib or Python fallback.

    Args:
        close: Series of closing prices.
        volume: Series of bar volumes.

    Returns:
        Series of OBV values.
    """
    if not _talib_available or _talib is None:
        return python_obv(close, volume)
    if len(close) == 0:
        return pd.Series([], dtype=float, index=close.index)
    result = _talib.OBV(close.astype(float).values, volume.astype(float).values)
    return pd.Series(result, index=close.index, dtype=float)


def is_talib_available() -> bool:
    """Check if TA-Lib is available.

    Returns:
        True if TA-Lib is installed and USE_TALIB is enabled.
    """
    return _talib_available


def get_backend() -> IndicatorBackend:
    """Get the current indicator backend name.

    Returns:
        'talib' if using TA-Lib, 'python' otherwise.
    """
    return "talib" if _talib_available else "python"
