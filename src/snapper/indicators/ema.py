"""Exponential Moving Average (EMA) indicator implementation.

This module provides a pure Python implementation of the EMA using
``pandas`` exponential weighting with ``adjust=False`` (the same
convention used by :mod:`snapper.indicators.macd`).

NOTE: this warmup convention differs from TA-Lib's ``EMA`` (which NaNs the
first ``period - 1`` samples and seeds the value at index ``period - 1`` with
an SMA of the first ``period`` samples). Because both are recursive with the
same smoothing factor, the difference decays geometrically but never reaches
exactly zero, so the two backends are not bit-identical at any index — not
only during warmup. This is an intentional design choice shared with
:mod:`snapper.indicators.macd`; callers must not assume cross-backend equality
— see :mod:`snapper.indicators.ta_lib_adapter`.

Example:
    Calculate a 20-period EMA::

        import pandas as pd
        from snapper.indicators.ema import ema

        prices = pd.Series([100, 102, 101, 103, 105])
        ema_values = ema(prices, period=3)
"""

import pandas as pd


def ema(series: pd.Series, period: int = 20) -> pd.Series:
    """Calculate the Exponential Moving Average.

    Uses ``span=period`` weighting with ``adjust=False`` (recursive form),
    seeded from the first observation.

    Args:
        series: Price series (typically closing prices).
        period: Span for the exponential weighting. Default is 20.

    Returns:
        Series of EMA values with the input index; empty input yields an
        empty series.
    """
    if len(series) == 0:
        return pd.Series([], dtype=float, index=series.index)
    return series.astype(float).ewm(span=period, adjust=False).mean()
