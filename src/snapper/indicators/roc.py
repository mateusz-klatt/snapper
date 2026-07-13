"""Rate of Change (ROC) momentum indicator implementation.

Pure Python implementation of the Rate of Change: the percent change of
the close versus the close ``period`` bars earlier.

    ROC = 100 * (close - close[t - period]) / close[t - period]

The first ``period`` positions are NaN (warmup). A prior close of exactly
zero yields NaN at that position (guarded to avoid division by zero).

Example:
    Calculate a 10-period ROC::

        import pandas as pd
        from snapper.indicators.roc import roc

        prices = pd.Series([100.0, 101.0, 102.0, 99.0])
        roc_values = roc(prices, period=2)
"""

import pandas as pd


def roc(series: pd.Series, period: int = 10) -> pd.Series:
    """Calculate the Rate of Change momentum indicator.

    Args:
        series: Price series (typically closing prices).
        period: Lookback for the reference price. Default is 10.

    Returns:
        Series of ROC values (percent) with the input index. The first
        ``period`` values are NaN; a zero reference price yields NaN at
        that position. Empty input yields an empty series.
    """
    if len(series) == 0:
        return pd.Series([], dtype=float, index=series.index)
    s = series.astype(float)
    prev = s.shift(period)
    safe_prev = prev.where(prev.abs() > 0.0)
    return 100.0 * (s - prev) / safe_prev
