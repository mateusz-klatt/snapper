"""Moving Average Convergence Divergence (MACD) indicator implementation.

This module provides a pure Python implementation of the MACD indicator
using exponential moving averages (EMA).

MACD consists of three components:
    - MACD Line: Fast EMA - Slow EMA
    - Signal Line: EMA of MACD Line
    - Histogram: MACD Line - Signal Line

Trading signals:
    - Bullish: MACD crosses above Signal (histogram turns positive)
    - Bearish: MACD crosses below Signal (histogram turns negative)

Example:
    Calculate MACD for a price series::

        import pandas as pd
        from snapper.indicators.macd import macd

        prices = pd.Series([100, 102, 101, 103, 105, 104, 106])
        result = macd(prices, fast=12, slow=26, signal=9)
        macd_line = result['macd']
        signal_line = result['signal']
        histogram = result['hist']
"""

import pandas as pd


def macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    """Calculate MACD indicator components.

    Args:
        series: Price series (typically closing prices).
        fast: Period for fast EMA. Default is 12.
        slow: Period for slow EMA. Default is 26.
        signal: Period for signal line EMA. Default is 9.

    Returns:
        DataFrame with columns:
            - 'macd': MACD line (fast EMA - slow EMA)
            - 'signal': Signal line (EMA of MACD)
            - 'hist': Histogram (MACD - Signal)
    """
    s = pd.Series(series).astype(float)
    ema_fast = s.ewm(span=fast, adjust=False).mean()
    ema_slow = s.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    hist = macd_line - signal_line
    return pd.DataFrame({"macd": macd_line, "signal": signal_line, "hist": hist})
