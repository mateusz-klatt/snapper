"""Bollinger Bands indicator implementation.

Pure Python implementation of Bollinger Bands: a middle band (SMA) with
upper and lower bands offset by ``num_std`` population standard deviations.

The population standard deviation (``ddof=0``) is used to match TA-Lib's
``BBANDS`` (which uses ``matype=0`` SMA and a population deviation).

Example:
    Calculate 20-period, 2-std Bollinger Bands::

        import pandas as pd
        from snapper.indicators.bollinger import bollinger

        prices = pd.Series([100, 102, 101, 103, 105])
        bands = bollinger(prices, period=3, num_std=2.0)
        upper, middle, lower = bands["upper"], bands["middle"], bands["lower"]
"""

import pandas as pd


def bollinger(series: pd.Series, period: int = 20, num_std: float = 2.0) -> pd.DataFrame:
    """Calculate Bollinger Bands.

    Args:
        series: Price series (typically closing prices).
        period: Lookback window for the middle SMA and deviation. Default 20.
        num_std: Number of population standard deviations for the bands.
            Default is 2.0.

    Returns:
        DataFrame with the input index and columns:
            - ``upper``: middle + num_std * population std
            - ``middle``: SMA over ``period``
            - ``lower``: middle - num_std * population std
        The first ``period - 1`` rows are NaN (warmup); empty input yields
        an empty DataFrame with these columns.
    """
    if len(series) == 0:
        empty = pd.Series([], dtype=float, index=series.index)
        return pd.DataFrame({"upper": empty, "middle": empty, "lower": empty})
    s = series.astype(float)
    middle = s.rolling(window=period, min_periods=period).mean()
    std = s.rolling(window=period, min_periods=period).std(ddof=0)
    upper = middle + num_std * std
    lower = middle - num_std * std
    return pd.DataFrame({"upper": upper, "middle": middle, "lower": lower})
