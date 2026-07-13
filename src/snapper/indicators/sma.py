"""Simple Moving Average (SMA) indicator implementation.

This module provides a pure Python implementation of the SMA indicator,
the unweighted mean of the trailing ``period`` values.

The first ``period - 1`` positions are NaN (warmup), matching TA-Lib's
``SMA`` lookback convention.

Example:
    Calculate a 20-period SMA::

        import pandas as pd
        from snapper.indicators.sma import sma

        prices = pd.Series([100, 102, 101, 103, 105])
        sma_values = sma(prices, period=3)
"""

import pandas as pd


def sma(series: pd.Series, period: int = 20) -> pd.Series:
    """Calculate the Simple Moving Average.

    Args:
        series: Price series (typically closing prices).
        period: Lookback window. Default is 20.

    Returns:
        Series of SMA values with the input index. The first
        ``period - 1`` values are NaN (warmup); empty input yields an
        empty series.
    """
    if len(series) == 0:
        return pd.Series([], dtype=float, index=series.index)
    return series.astype(float).rolling(window=period, min_periods=period).mean()
