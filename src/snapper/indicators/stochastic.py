"""Fast Stochastic Oscillator implementation.

Pure Python implementation of the fast stochastic oscillator (equivalent
to TA-Lib's ``STOCHF`` with an SMA smoothing of ``%D``):

    %K = 100 * (close - lowest_low) / (highest_high - lowest_low)
    %D = SMA(%K, d_period)

where ``lowest_low`` / ``highest_high`` are taken over ``k_period`` bars.
When the range is zero over the window (flat market), ``%K`` is defined as
0.0 to avoid division by zero.

Both ``%K`` and ``%D`` become valid at index ``k_period + d_period - 2``,
matching TA-Lib's ``STOCHF`` (which aligns its ``%K`` output to ``%D``'s
start): ``%D`` is computed from the full ``%K`` series, then ``%K`` is masked
to the same start. The high and low series are aligned to ``close``
positionally, so a differing index cannot corrupt the result.

Example:
    Calculate a 14/3 fast stochastic::

        import pandas as pd
        from snapper.indicators.stochastic import stochastic

        high = pd.Series([10.0, 11.0, 12.0])
        low = pd.Series([9.0, 9.5, 11.0])
        close = pd.Series([9.5, 10.5, 11.5])
        stoch = stochastic(high, low, close, k_period=2, d_period=2)
        percent_k, percent_d = stoch["k"], stoch["d"]
"""

import pandas as pd


def stochastic(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    k_period: int = 14,
    d_period: int = 3,
) -> pd.DataFrame:
    """Calculate the fast stochastic oscillator (%K and %D).

    Args:
        high: Series of high prices.
        low: Series of low prices.
        close: Series of closing prices.
        k_period: Lookback for the %K high/low range. Default is 14.
        d_period: SMA smoothing window for %D. Default is 3.

    Returns:
        DataFrame with the ``close`` index and columns ``k`` and ``d``,
        each in the range [0, 100]. Warmup positions are NaN; empty input
        yields an empty DataFrame with these columns.

    Raises:
        ValueError: If ``high``, ``low`` and ``close`` differ in length.
    """
    n = len(close)
    if len(high) != n or len(low) != n:
        raise ValueError("stochastic: high, low and close must have equal length")
    if n == 0:
        empty = pd.Series([], dtype=float, index=close.index)
        return pd.DataFrame({"k": empty, "d": empty})
    c = close.astype(float)
    h = pd.Series(high.astype(float).to_numpy(), index=c.index)
    low_series = pd.Series(low.astype(float).to_numpy(), index=c.index)
    lowest_low = low_series.rolling(window=k_period, min_periods=k_period).min()
    highest_high = h.rolling(window=k_period, min_periods=k_period).max()
    span = highest_high - lowest_low
    safe_span = span.where(span > 0.0)
    percent_k_full = (100.0 * (c - lowest_low) / safe_span).where((span > 0.0) | span.isna(), 0.0)
    percent_d = percent_k_full.rolling(window=d_period, min_periods=d_period).mean()
    warmup = pd.Series(range(n), index=c.index) < (k_period + d_period - 2)
    percent_k = percent_k_full.mask(warmup)
    return pd.DataFrame({"k": percent_k, "d": percent_d})
