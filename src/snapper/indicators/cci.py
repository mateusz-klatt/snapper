"""Commodity Channel Index (CCI) indicator implementation.

Pure Python implementation of the CCI:

    TP = (high + low + close) / 3
    CCI = (TP - SMA(TP, period)) / (0.015 * mean_absolute_deviation(TP))

where the mean absolute deviation is taken over the same ``period`` window.
The 0.015 scaling constant is Lambert's original constant, matching
TA-Lib's ``CCI``. The first ``period - 1`` positions are NaN (warmup); a
window with zero mean deviation (flat typical price) yields 0, matching
TA-Lib.

Example:
    Calculate a 20-period CCI::

        import pandas as pd
        from snapper.indicators.cci import cci

        high = pd.Series([10.0, 11.0, 12.0])
        low = pd.Series([9.0, 9.5, 11.0])
        close = pd.Series([9.5, 10.5, 11.5])
        cci_values = cci(high, low, close, period=2)
"""

import numpy as np
import pandas as pd


def _mean_abs_dev(window: np.ndarray) -> float:
    """Return the mean absolute deviation of a rolling window.

    Args:
        window: Contiguous typical-price values for one window.

    Returns:
        Mean of the absolute deviations from the window mean.
    """
    return float(np.abs(window - window.mean()).mean())


def cci(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 20) -> pd.Series:
    """Calculate the Commodity Channel Index.

    Args:
        high: Series of high prices.
        low: Series of low prices.
        close: Series of closing prices.
        period: Lookback window. Default is 20.

    Returns:
        Series of CCI values with the ``close`` index. The first
        ``period - 1`` values are NaN; empty input yields an empty series.

    Raises:
        ValueError: If ``high``, ``low`` and ``close`` differ in length.
    """
    n = len(close)
    if len(high) != n or len(low) != n:
        raise ValueError("cci: high, low and close must have equal length")
    if n == 0:
        return pd.Series([], dtype=float, index=close.index)
    typical = (high.astype(float) + low.astype(float) + close.astype(float)) / 3.0
    sma_tp = typical.rolling(window=period, min_periods=period).mean()
    mad = typical.rolling(window=period, min_periods=period).apply(_mean_abs_dev, raw=True)
    safe_mad = mad.where(mad > 0.0)
    result = (typical - sma_tp) / (0.015 * safe_mad)
    return result.where((mad > 0.0) | mad.isna(), 0.0)
