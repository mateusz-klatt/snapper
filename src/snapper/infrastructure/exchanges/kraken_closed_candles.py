"""Validate full-field Kraken REST candles without reconstructing missing data."""

import math
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta

from snapper.infrastructure.exchanges.contracts import CandleUpdate


@dataclass(frozen=True)
class ClosedCandleRequest:
    """One bounded, canonical request ending before its acquisition minute."""

    symbol: str
    pair: str
    start: datetime
    end: datetime
    observed_at: datetime

    def __post_init__(self) -> None:
        """Reject unbounded, non-UTC, unfinished or unaligned request windows."""
        if any(not value or value != value.strip() for value in (self.symbol, self.pair)):
            raise ValueError("Kraken candle history requires a canonical product identity")
        bounds = (self.start, self.end, self.observed_at)
        if any(value.utcoffset() != timedelta(0) for value in bounds):
            raise ValueError("Kraken candle history requires UTC bounds")
        if any(value.second or value.microsecond for value in (self.start, self.end)):
            raise ValueError("Kraken candle history requires whole-minute bounds")
        if not self.start < self.end <= self.observed_at.replace(second=0, microsecond=0):
            raise ValueError("Kraken candle history must contain only closed minutes")
        if self.end - self.start > timedelta(minutes=719):
            raise ValueError("Kraken candle history exceeds its bounded recent window")


def _number(value: object) -> float:
    """Parse a finite venue number without treating booleans as prices."""
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        raise ValueError("Invalid Kraken OHLC numeric field")
    try:
        result = float(value)
    except (ValueError, OverflowError) as error:
        raise ValueError("Invalid Kraken OHLC numeric field") from error
    if not math.isfinite(result):
        raise ValueError("Non-finite Kraken OHLC numeric field")
    return result


def _candle(row: object, symbol: str) -> CandleUpdate:
    """Parse all eight native fields and reject malformed minute observations."""
    if not isinstance(row, list) or len(row) != 8:
        raise ValueError("Kraken OHLC row must contain all eight native fields")
    stamp, trades = row[0], row[7]
    if type(stamp) is not int or stamp % 60 or type(trades) is not int or trades < 0:
        raise ValueError("Invalid Kraken OHLC minute or trade count")
    opening, high, low, close, vwap, volume = [_number(value) for value in row[1:7]]
    if low <= 0 or low > min(opening, close) or high < max(opening, close, low):
        raise ValueError("Invalid Kraken OHLC price bounds")
    if volume < 0 or vwap < 0:
        raise ValueError("Invalid Kraken OHLC volume or VWAP")
    try:
        open_at = datetime.fromtimestamp(stamp, UTC)
    except (ValueError, OverflowError, OSError) as error:
        raise ValueError("Invalid Kraken OHLC minute") from error
    return CandleUpdate(
        symbol=symbol,
        open=opening,
        high=high,
        low=low,
        close=close,
        vwap=vwap,
        volume=volume,
        trades=trades,
        interval_begin=open_at,
        interval=60,
        complete=True,
    )


def parse_closed_candles(payload: object, request: ClosedCandleRequest) -> list[CandleUpdate]:
    """Return bounded closed candles with exact response identity and native fields.

    Kraken includes an unfinished final entry. It is discarded even if its
    reported business timestamp would otherwise pass the frozen bound. The
    bounded one-minute inclusive edge permits 720 closed rows plus that final
    unfinished row, as observed in frozen native captures.

    Args:
        payload: Untrusted native REST response.
        request: Frozen identity, event window and acquisition boundary.

    Returns:
        Strictly ascending closed native candles inside the requested window.

    Raises:
        ValueError: When identity, ordering, native fields or envelope disagree.
    """
    if not isinstance(payload, dict) or payload.get("error") != []:
        raise ValueError("Kraken OHLC request failed or returned an invalid envelope")
    result = payload.get("result")
    if not isinstance(result, dict) or set(result) != {request.pair, "last"}:
        raise ValueError("Kraken OHLC response identity differs from the requested product")
    rows = result[request.pair]
    if not isinstance(rows, list) or not 1 <= len(rows) <= 721:
        raise ValueError("Kraken OHLC response exceeds its bounded native contract")
    candles = [_candle(row, request.symbol) for row in rows]
    if any(
        left.interval_begin >= right.interval_begin
        for left, right in zip(candles, candles[1:], strict=False)
    ):
        raise ValueError("Kraken OHLC response contains duplicate or unordered minutes")
    return [
        candle for candle in candles[:-1] if request.start <= candle.interval_begin < request.end
    ]
