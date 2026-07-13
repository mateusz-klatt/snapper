"""Keltner Channels indicator implementation.

Pure Python implementation of Keltner Channels: an EMA middle line with
upper and lower bands offset by a multiple of the Average True Range.

    middle = EMA(close, period)
    band = mult * ATR(high, low, close, atr_period)
    upper = middle + band
    lower = middle - band

There is no TA-Lib equivalent for Keltner Channels; the pure module composes
the pure-python EMA and ATR. The rows are NaN until both the EMA and ATR
warmups have elapsed.

Example:
    Calculate Keltner Channels::

        import pandas as pd
        from snapper.indicators.keltner import keltner

        high = pd.Series([10.0, 11.0, 12.0])
        low = pd.Series([9.0, 9.5, 11.0])
        close = pd.Series([9.5, 10.5, 11.5])
        bands = keltner(high, low, close, period=2, atr_period=2, mult=2.0)
"""

import pandas as pd

from snapper.indicators.atr import atr
from snapper.indicators.ema import ema


def keltner(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    period: int = 20,
    atr_period: int = 10,
    mult: float = 2.0,
) -> pd.DataFrame:
    """Calculate Keltner Channels.

    Args:
        high: Series of high prices.
        low: Series of low prices.
        close: Series of closing prices.
        period: EMA lookback for the middle line. Default is 20.
        atr_period: ATR lookback for the band width. Default is 10.
        mult: ATR multiplier for the band offset. Default is 2.0.

    Returns:
        DataFrame with the ``close`` index and columns ``upper``,
        ``middle`` and ``lower``. Rows are NaN until both the EMA and ATR
        warmups elapse; empty input yields an empty DataFrame with these
        columns.

    Raises:
        ValueError: If ``high``, ``low`` and ``close`` differ in length.
    """
    n = len(close)
    if len(high) != n or len(low) != n:
        raise ValueError("keltner: high, low and close must have equal length")
    if n == 0:
        empty = pd.Series([], dtype=float, index=close.index)
        return pd.DataFrame({"upper": empty, "middle": empty, "lower": empty})
    middle = ema(close, period)
    band = mult * atr(high, low, close, atr_period)
    return pd.DataFrame({"upper": middle + band, "middle": middle, "lower": middle - band})
