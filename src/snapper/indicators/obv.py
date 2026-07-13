"""On-Balance Volume (OBV) indicator implementation.

Pure Python implementation of On-Balance Volume, a cumulative volume line
that adds the bar's volume when the close rises, subtracts it when the
close falls, and leaves it unchanged when the close is flat.

OBV has no lookback period: the first value seeds to the first bar's volume
(matching TA-Lib's ``OBV``), so there is no NaN warmup.

Example:
    Calculate OBV::

        import pandas as pd
        from snapper.indicators.obv import obv

        close = pd.Series([10.0, 11.0, 10.5, 10.5])
        volume = pd.Series([100.0, 120.0, 90.0, 80.0])
        obv_values = obv(close, volume)
"""

import numpy as np
import pandas as pd


def obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    """Calculate On-Balance Volume.

    Args:
        close: Series of closing prices.
        volume: Series of bar volumes.

    Returns:
        Series of cumulative OBV values with the ``close`` index, seeded at
        the first bar's volume; empty input yields an empty series.

    Raises:
        ValueError: If ``close`` and ``volume`` differ in length.
    """
    n = len(close)
    if len(volume) != n:
        raise ValueError("obv: close and volume must have equal length")
    if n == 0:
        return pd.Series([], dtype=float, index=close.index)
    c = close.astype(float).to_numpy()
    v = volume.astype(float).to_numpy()
    out = np.empty(n, dtype=float)
    out[0] = v[0]
    for i in range(1, n):
        if c[i] > c[i - 1]:
            out[i] = out[i - 1] + v[i]
        elif c[i] < c[i - 1]:
            out[i] = out[i - 1] - v[i]
        else:
            out[i] = out[i - 1]
    return pd.Series(out, index=close.index, dtype=float)
