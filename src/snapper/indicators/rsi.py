"""Relative Strength Index (RSI) indicator implementation.

This module provides a pure Python implementation of the RSI indicator
using Wilder's smoothing method. The implementation uses exponential
moving averages of gains and losses.

The RSI oscillates between 0 and 100:
    - RSI > 70: Overbought condition (potential sell signal)
    - RSI < 30: Oversold condition (potential buy signal)

Example:
    Calculate RSI for a price series::

        import pandas as pd
        from snapper.indicators.rsi import rsi

        prices = pd.Series([100, 102, 101, 103, 105, 104, 106])
        rsi_values = rsi(prices, period=14)
"""

from decimal import ROUND_HALF_UP
from decimal import Decimal

import numpy as np
import pandas as pd


def _q2(x: float) -> float:
    """Quantize float to 2 decimal places with half-up rounding.

    Args:
        x: Float value to quantize.

    Returns:
        Float rounded to 2 decimal places.
    """
    return float(Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Calculate Relative Strength Index using Wilder's smoothing.

    Uses exponential smoothing of gains and losses over the specified
    period to calculate the RSI value.

    Args:
        series: Price series (typically closing prices).
        period: Lookback period for RSI calculation. Default is 14.

    Returns:
        Series of RSI values. First `period` values will be 0.
        Values range from 0 to 100.
    """
    n = len(series)
    if n == 0:
        return pd.Series([], dtype=float)
    s = series.astype(float)
    delta = s.diff()
    gain = delta.clip(lower=0.0).to_numpy()
    loss = (-delta.clip(upper=0.0)).to_numpy()
    out = np.zeros(n, dtype=float)
    if n <= period:
        return pd.Series(out, index=series.index, dtype=float)
    avg_gain = float(gain[1 : period + 1].mean())
    avg_loss = float(loss[1 : period + 1].mean())
    rs = float("inf") if avg_loss == 0 else _q2(avg_gain / avg_loss)
    out[period] = 100.0 - (100.0 / (1.0 + rs))
    prev_gain = avg_gain
    prev_loss = avg_loss
    for i in range(period + 1, n):
        prev_gain = (prev_gain * (period - 1) + gain[i]) / period
        prev_loss = (prev_loss * (period - 1) + loss[i]) / period
        rs = float("inf") if prev_loss == 0 else _q2(prev_gain / prev_loss)
        out[i] = 100.0 - (100.0 / (1.0 + rs))
    return pd.Series(out, index=series.index, dtype=float)
