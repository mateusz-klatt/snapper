"""Money Flow Index (MFI) indicator implementation.

Pure Python implementation of the MFI, a volume-weighted momentum
oscillator bounded in [0, 100]:

    TP = (high + low + close) / 3
    raw money flow = TP * volume
    positive / negative money flow = raw flow on bars where TP rose / fell
    money ratio = sum(positive, period) / sum(negative, period)
    MFI = 100 - 100 / (1 + money ratio)

When the negative money flow over the window is zero (an all-up window),
MFI is 100; when there is no money flow at all (a dead or flat window, both
sums zero), MFI is 0, matching TA-Lib. The first ``period`` positions are NaN
(warmup): the typical-price direction needs a prior bar, so ``period`` complete
directional flows are only available from index ``period`` onward, matching
TA-Lib's ``MFI``. The high, low and volume series are aligned to ``close``
positionally, so a differing index cannot corrupt the result.

Example:
    Calculate a 14-period MFI::

        import pandas as pd
        from snapper.indicators.mfi import mfi

        high = pd.Series([10.0, 11.0, 12.0])
        low = pd.Series([9.0, 9.5, 11.0])
        close = pd.Series([9.5, 10.5, 11.5])
        volume = pd.Series([100.0, 120.0, 90.0])
        mfi_values = mfi(high, low, close, volume, period=2)
"""

import pandas as pd


def mfi(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    volume: pd.Series,
    period: int = 14,
) -> pd.Series:
    """Calculate the Money Flow Index.

    Args:
        high: Series of high prices.
        low: Series of low prices.
        close: Series of closing prices.
        volume: Series of bar volumes.
        period: Lookback window. Default is 14.

    Returns:
        Series of MFI values in [0, 100] with the ``close`` index. Warmup
        positions are NaN; an all-up window yields 100. Empty input yields
        an empty series.

    Raises:
        ValueError: If ``high``, ``low``, ``close`` and ``volume`` differ
            in length.
    """
    n = len(close)
    if len(high) != n or len(low) != n or len(volume) != n:
        raise ValueError("mfi: high, low, close and volume must have equal length")
    if n == 0:
        return pd.Series([], dtype=float, index=close.index)
    close = close.astype(float)
    high = pd.Series(high.astype(float).to_numpy(), index=close.index)
    low = pd.Series(low.astype(float).to_numpy(), index=close.index)
    volume = pd.Series(volume.astype(float).to_numpy(), index=close.index)
    typical = (high + low + close) / 3.0
    raw_flow = typical * volume
    direction = typical.diff()
    positive = raw_flow.where(direction > 0.0, 0.0)
    negative = raw_flow.where(direction < 0.0, 0.0)
    positive_sum = positive.rolling(window=period, min_periods=period).sum()
    negative_sum = negative.rolling(window=period, min_periods=period).sum()
    safe_negative = negative_sum.where(negative_sum > 0.0)
    money_ratio = positive_sum / safe_negative
    values = 100.0 - 100.0 / (1.0 + money_ratio)
    all_up = values.where((negative_sum > 0.0) | negative_sum.isna(), 100.0)
    flat_adjusted = all_up.where(
        (positive_sum > 0.0) | (negative_sum > 0.0) | negative_sum.isna(), 0.0
    )
    warmup = pd.Series(range(n), index=close.index) < period
    return flat_adjusted.mask(warmup)
