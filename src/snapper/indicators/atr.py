"""Average True Range (ATR) indicator implementation.

Pure Python implementation of Wilder's Average True Range. The True Range
of a bar is the greatest of: the current high-low range, the absolute gap
from the previous close to the current high, and the absolute gap from the
previous close to the current low. ATR is Wilder's smoothing of the True
Range over ``period`` bars.

The first ``period - 1`` positions are NaN (warmup); the seed at index
``period - 1`` is the mean of the first ``period`` True Range values, after
which Wilder smoothing applies.

Example:
    Calculate a 14-period ATR::

        import pandas as pd
        from snapper.indicators.atr import atr

        high = pd.Series([10.0, 11.0, 12.0])
        low = pd.Series([9.0, 9.5, 11.0])
        close = pd.Series([9.5, 10.5, 11.5])
        atr_values = atr(high, low, close, period=2)
"""

import numpy as np
import pandas as pd


def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """Calculate the Average True Range using Wilder's smoothing.

    Args:
        high: Series of high prices.
        low: Series of low prices.
        close: Series of closing prices.
        period: Lookback window. Default is 14.

    Returns:
        Series of ATR values with the ``close`` index. The first
        ``period - 1`` values are NaN; empty input yields an empty series.

    Raises:
        ValueError: If ``high``, ``low`` and ``close`` differ in length.
    """
    n = len(close)
    if len(high) != n or len(low) != n:
        raise ValueError("atr: high, low and close must have equal length")
    if n == 0:
        return pd.Series([], dtype=float, index=close.index)
    h = high.astype(float).to_numpy()
    low_values = low.astype(float).to_numpy()
    c = close.astype(float).to_numpy()
    true_range = np.empty(n, dtype=float)
    true_range[0] = h[0] - low_values[0]
    for i in range(1, n):
        true_range[i] = max(
            h[i] - low_values[i],
            abs(h[i] - c[i - 1]),
            abs(low_values[i] - c[i - 1]),
        )
    out = np.full(n, np.nan, dtype=float)
    if n < period:
        return pd.Series(out, index=close.index, dtype=float)
    seed = float(true_range[:period].mean())
    out[period - 1] = seed
    prev = seed
    for i in range(period, n):
        prev = (prev * (period - 1) + true_range[i]) / period
        out[i] = prev
    return pd.Series(out, index=close.index, dtype=float)
