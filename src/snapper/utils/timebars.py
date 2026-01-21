"""Time bar aggregation utilities for trade-to-candle conversion.

This module provides functions for aggregating raw trades into OHLCV
candlestick (time bar) data at various timeframes.

Supported timeframes:
    - Minutes: '1m', '5m', '15m', '30m', etc.
    - Hours: '1h', '4h', etc.

Example:
    Convert trades to 5-minute candles::

        from snapper.utils.timebars import ohlc_from_trades

        trades = [
            {'ts': datetime(...), 'price': 100.0, 'size': 1.0},
            {'ts': datetime(...), 'price': 101.0, 'size': 0.5},
        ]
        candles = ohlc_from_trades(trades, '5m')
"""

from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime
from typing import Any


def floor_ts(ts: datetime, timeframe: str) -> datetime:
    """Floor timestamp to timeframe boundary.

    Args:
        ts: Timestamp to floor.
        timeframe: Timeframe string (e.g., '1m', '5m', '1h').

    Returns:
        Timestamp floored to nearest timeframe boundary.

    Raises:
        ValueError: If timeframe format is unsupported.
    """
    if timeframe.endswith("m"):
        m = int(timeframe[:-1])
        truncated = ts.replace(second=0, microsecond=0)
        minute = (truncated.minute // m) * m
        return truncated.replace(minute=minute)
    if timeframe.endswith("h"):
        h = int(timeframe[:-1])
        truncated = ts.replace(minute=0, second=0, microsecond=0)
        hour = (truncated.hour // h) * h
        return truncated.replace(hour=hour)
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def ohlc_from_trades(trades: Iterable[dict[str, Any]], timeframe: str) -> list[dict[str, Any]]:
    """Aggregate trades into OHLCV candles.

    Groups trades by floored timestamp and calculates OHLCV values
    for each time bucket.

    Args:
        trades: Iterable of trade dicts with 'ts', 'price', 'size' keys.
        timeframe: Target timeframe (e.g., '1m', '5m', '1h').

    Returns:
        List of candle dicts with keys:
            - 'ts': Candle open time
            - 'timeframe': Timeframe string
            - 'open', 'high', 'low', 'close': OHLC prices
            - 'volume': Total traded volume
    """
    buckets: dict[datetime, list[dict[str, Any]]] = defaultdict(list)
    for t in trades:
        ts = t["ts"]
        ts_floor = floor_ts(ts, timeframe)
        buckets[ts_floor].append(t)
    candles: list[dict[str, Any]] = []
    for ts, items in sorted(buckets.items()):
        prices = [float(i["price"]) for i in items]
        sizes = [float(i["size"]) for i in items]
        o = prices[0]
        h = max(prices)
        low = min(prices)
        c = prices[-1]
        v = sum(sizes)
        candles.append(
            {
                "ts": ts,
                "timeframe": timeframe,
                "open": o,
                "high": h,
                "low": low,
                "close": c,
                "volume": v,
            }
        )
    return candles
