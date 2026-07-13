"""Rolling Volume-Weighted Average Price (VWAP) indicator implementation.

Pure Python implementation of a ROLLING VWAP over a fixed ``period``
window (as opposed to a session-anchored VWAP, which would require session
boundaries this library does not carry):

    TP = (high + low + close) / 3
    VWAP = sum(TP * volume, period) / sum(volume, period)

There is no TA-Lib equivalent, so this indicator is pure-python only. The
first ``period - 1`` positions are NaN (warmup); a window with zero total
volume yields NaN (guarded).

Example:
    Calculate a 14-period rolling VWAP::

        import pandas as pd
        from snapper.indicators.vwap import vwap

        high = pd.Series([10.0, 11.0, 12.0])
        low = pd.Series([9.0, 9.5, 11.0])
        close = pd.Series([9.5, 10.5, 11.5])
        volume = pd.Series([100.0, 120.0, 90.0])
        vwap_values = vwap(high, low, close, volume, period=2)
"""

import pandas as pd


def vwap(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    volume: pd.Series,
    period: int = 14,
) -> pd.Series:
    """Calculate the rolling Volume-Weighted Average Price.

    Args:
        high: Series of high prices.
        low: Series of low prices.
        close: Series of closing prices.
        volume: Series of bar volumes.
        period: Rolling window. Default is 14.

    Returns:
        Series of rolling VWAP values with the ``close`` index. The first
        ``period - 1`` values are NaN; a zero-volume window yields NaN.
        Empty input yields an empty series.

    Raises:
        ValueError: If ``high``, ``low``, ``close`` and ``volume`` differ
            in length.
    """
    n = len(close)
    if len(high) != n or len(low) != n or len(volume) != n:
        raise ValueError("vwap: high, low, close and volume must have equal length")
    if n == 0:
        return pd.Series([], dtype=float, index=close.index)
    typical = (high.astype(float) + low.astype(float) + close.astype(float)) / 3.0
    vol = volume.astype(float)
    weighted = (typical * vol).rolling(window=period, min_periods=period).sum()
    volume_sum = vol.rolling(window=period, min_periods=period).sum()
    safe_volume = volume_sum.where(volume_sum > 0.0)
    return weighted / safe_volume
