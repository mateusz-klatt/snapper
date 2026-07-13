"""Read-only data-quality audit for the historical candle store.

Surfaces the candle anomalies that silently corrupt backtests and research:
broken OHLC invariants, non-positive or non-finite prices, negative volume,
timestamps off the timeframe grid, duplicate or out-of-order bars, gaps in an
otherwise continuous series, and suspiciously large close-to-close jumps that
suggest an unhandled split or a bad tick.

This module is pure: :func:`audit_candle_series` takes the ``CandleRow`` dicts
that :meth:`Repository.get_candles` returns and produces a list of
:class:`CandleAnomaly`, with no database access or mutation. The ``candles``
unique constraint already prevents exact duplicates; this is the safety net for
everything else.
"""

from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from enum import Enum
from math import isfinite

from snapper.data.repository_types import CandleRow

_TIMEFRAME_SECONDS: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}
_ONE_DAY_SECONDS: int = 86400
_EPOCH: datetime = datetime(1970, 1, 1, tzinfo=UTC)
_MICROS_PER_SECOND: int = 1_000_000


class CandleAnomalyType(Enum):
    """Classes of candle data-quality anomaly."""

    OHLC_INVARIANT = "ohlc_invariant"
    NON_POSITIVE_PRICE = "non_positive_price"
    NEGATIVE_VOLUME = "negative_volume"
    MISALIGNED_OPEN_AT = "misaligned_open_at"
    DUPLICATE_OPEN_AT = "duplicate_open_at"
    OUT_OF_ORDER = "out_of_order"
    GAP = "gap"
    SPLIT_SUSPECT = "split_suspect"


@dataclass(frozen=True)
class CandleAnomaly:
    """A single detected candle data-quality anomaly.

    Attributes:
        type: The anomaly class.
        open_at: The bar (or the later bar of a pair) the anomaly attaches to.
        detail: Human-readable specifics for triage.
    """

    type: CandleAnomalyType
    open_at: datetime
    detail: str


def _timeframe_to_seconds(timeframe: str) -> int:
    """Return the interval length of a timeframe in seconds.

    Args:
        timeframe: One of ``1m``, ``5m``, ``15m``, ``1h``, ``4h``, ``1d``.

    Returns:
        The interval length in whole seconds.

    Raises:
        ValueError: If the timeframe is not supported.
    """
    seconds = _TIMEFRAME_SECONDS.get(timeframe)
    if seconds is None:
        raise ValueError(f"unsupported timeframe: {timeframe!r}")
    return seconds


def _as_utc(moment: datetime) -> datetime:
    """Coerce a datetime to UTC; a naive value is assumed to be UTC already."""
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


def _is_on_grid(open_at: datetime, interval_seconds: int) -> bool:
    """Return whether ``open_at`` lands on the interval grid from the epoch.

    Uses integer microseconds so float residue never causes a false misalignment.

    Args:
        open_at: The aware bar open time.
        interval_seconds: The timeframe interval in seconds.

    Returns:
        True when the bar is aligned to the grid.
    """
    delta = open_at - _EPOCH
    total_micros = (delta.days * _ONE_DAY_SECONDS + delta.seconds) * _MICROS_PER_SECOND
    total_micros += delta.microseconds
    return total_micros % (interval_seconds * _MICROS_PER_SECOND) == 0


def _row_anomalies(row: CandleRow, interval_seconds: int) -> list[CandleAnomaly]:
    """Return the row-level anomalies for a single candle.

    Args:
        row: The candle row dict.
        interval_seconds: The timeframe interval in seconds.

    Returns:
        Any OHLC-invariant, non-positive-price, negative-volume or
        misalignment anomalies found on this bar.
    """
    open_at = _as_utc(row["open_at"])
    open_price = row["open"]
    high_price = row["high"]
    low_price = row["low"]
    close_price = row["close"]
    volume = row["volume"]
    anomalies: list[CandleAnomaly] = []
    prices = (open_price, high_price, low_price, close_price)
    if any((not isfinite(price)) or price <= 0.0 for price in prices):
        anomalies.append(
            CandleAnomaly(
                CandleAnomalyType.NON_POSITIVE_PRICE,
                open_at,
                f"open={open_price} high={high_price} low={low_price} close={close_price}",
            )
        )
    elif (
        high_price < low_price
        or high_price < open_price
        or high_price < close_price
        or low_price > open_price
        or low_price > close_price
    ):
        anomalies.append(
            CandleAnomaly(
                CandleAnomalyType.OHLC_INVARIANT,
                open_at,
                f"O={open_price} H={high_price} L={low_price} C={close_price}",
            )
        )
    if (not isfinite(volume)) or volume < 0.0:
        anomalies.append(
            CandleAnomaly(CandleAnomalyType.NEGATIVE_VOLUME, open_at, f"volume={volume}")
        )
    if interval_seconds < _ONE_DAY_SECONDS and not _is_on_grid(open_at, interval_seconds):
        anomalies.append(
            CandleAnomaly(
                CandleAnomalyType.MISALIGNED_OPEN_AT,
                open_at,
                f"not aligned to the {interval_seconds}s grid",
            )
        )
    return anomalies


def audit_candle_series(
    candles: Sequence[CandleRow],
    timeframe: str,
    *,
    split_threshold: float = 0.30,
    expected_gap: Callable[[datetime, datetime], bool] | None = None,
) -> list[CandleAnomaly]:
    """Audit one instrument's candle series for one timeframe.

    Args:
        candles: Candle rows, expected in ascending ``open_at`` order.
        timeframe: The bars' timeframe (``1m``/``5m``/``15m``/``1h``/``4h``/``1d``).
        split_threshold: Fractional close-to-close move above which a bar is
            flagged as a split suspect. Default 0.30 (30%).
        expected_gap: Optional predicate ``(prev_open, next_open) -> bool``; when
            it returns True the gap between those bars is treated as expected
            (e.g. a market closure) and not reported.

    Returns:
        The detected anomalies, in scan order.

    Raises:
        ValueError: If the timeframe is not supported.
    """
    interval_seconds = _timeframe_to_seconds(timeframe)
    interval = timedelta(seconds=interval_seconds)
    anomalies: list[CandleAnomaly] = []
    previous: CandleRow | None = None
    for row in candles:
        anomalies.extend(_row_anomalies(row, interval_seconds))
        if previous is not None:
            previous_open = _as_utc(previous["open_at"])
            current_open = _as_utc(row["open_at"])
            if current_open == previous_open:
                anomalies.append(
                    CandleAnomaly(
                        CandleAnomalyType.DUPLICATE_OPEN_AT,
                        current_open,
                        f"duplicate open_at {current_open.isoformat()}",
                    )
                )
            elif current_open < previous_open:
                anomalies.append(
                    CandleAnomaly(
                        CandleAnomalyType.OUT_OF_ORDER,
                        current_open,
                        f"{current_open.isoformat()} precedes {previous_open.isoformat()}",
                    )
                )
            else:
                gap = current_open - previous_open
                if gap > interval and (
                    expected_gap is None or not expected_gap(previous_open, current_open)
                ):
                    missing = round(gap.total_seconds() / interval_seconds) - 1
                    anomalies.append(
                        CandleAnomaly(
                            CandleAnomalyType.GAP,
                            current_open,
                            f"{missing} missing bar(s) after {previous_open.isoformat()}",
                        )
                    )
                previous_close = previous["close"]
                if previous_close > 0.0:
                    change = abs(row["close"] - previous_close) / previous_close
                    if change > split_threshold:
                        anomalies.append(
                            CandleAnomaly(
                                CandleAnomalyType.SPLIT_SUSPECT,
                                current_open,
                                f"close {previous_close} -> {row['close']} ({change:.1%})",
                            )
                        )
        previous = row
    return anomalies
