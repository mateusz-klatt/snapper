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
from enum import Enum
from math import isfinite

from snapper.data.repository_types import CandleRow

_TIMEFRAME_SECONDS: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
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


@dataclass(frozen=True)
class _AuditSettings:
    """Normalized settings shared across one candle-series audit."""

    interval_seconds: int
    split_threshold: float
    anchor_offset_seconds: int
    expected_gap: Callable[[datetime, datetime], bool] | None


def _timeframe_to_seconds(timeframe: str) -> int:
    """Return the interval length of a timeframe in seconds.

    Args:
        timeframe: One of ``1m``, ``5m``, ``15m``, ``30m``, ``1h``, ``4h``, ``1d``.

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
    """Return ``moment`` in UTC; a naive value is assumed to already be UTC."""
    if moment.tzinfo is None:
        return moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC)


def _is_on_grid(open_at: datetime, interval_seconds: int, anchor_offset_seconds: int) -> bool:
    """Return whether ``open_at`` lands on the interval grid.

    Uses integer microseconds so float residue never causes a false
    misalignment. ``anchor_offset_seconds`` shifts the grid origin off the UNIX
    epoch so venue-anchored bars (e.g. a session opening at :30 past the hour)
    can be aligned by a caller that knows the schedule.

    Args:
        open_at: The aware bar open time.
        interval_seconds: The timeframe interval in seconds.
        anchor_offset_seconds: Grid-origin offset from the epoch, in seconds.

    Returns:
        True when the bar is aligned to the grid.
    """
    delta = open_at - _EPOCH
    total_micros = (delta.days * _ONE_DAY_SECONDS + delta.seconds) * _MICROS_PER_SECOND
    total_micros += delta.microseconds - anchor_offset_seconds * _MICROS_PER_SECOND
    return total_micros % (interval_seconds * _MICROS_PER_SECOND) == 0


def _grid_slot(open_at: datetime, interval_seconds: int, anchor_offset_seconds: int) -> int:
    """Return the integer grid-slot index nearest ``open_at``.

    Assigns each timestamp to its closest bar slot on the anchored grid using
    integer microseconds. Gap counting subtracts two slot indices, which is
    exact for on-grid bars and deterministic (round-half-up) for jittered ones,
    so opposing sub-interval jitter can neither hide nor invent a missing bar.

    Args:
        open_at: The aware bar open time.
        interval_seconds: The timeframe interval in seconds.
        anchor_offset_seconds: Grid-origin offset from the epoch, in seconds.

    Returns:
        The nearest grid-slot index.
    """
    delta = _as_utc(open_at) - _EPOCH
    total_micros = (delta.days * _ONE_DAY_SECONDS + delta.seconds) * _MICROS_PER_SECOND
    total_micros += delta.microseconds - anchor_offset_seconds * _MICROS_PER_SECOND
    interval_micros = interval_seconds * _MICROS_PER_SECOND
    return (total_micros + interval_micros // 2) // interval_micros


def _row_anomalies(
    row: CandleRow, interval_seconds: int, anchor_offset_seconds: int
) -> list[CandleAnomaly]:
    """Return the row-level anomalies for a single candle.

    Args:
        row: The candle row dict.
        interval_seconds: The timeframe interval in seconds.
        anchor_offset_seconds: Grid-origin offset for the alignment check.

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
    if interval_seconds < _ONE_DAY_SECONDS and not _is_on_grid(
        open_at, interval_seconds, anchor_offset_seconds
    ):
        anomalies.append(
            CandleAnomaly(
                CandleAnomalyType.MISALIGNED_OPEN_AT,
                open_at,
                f"not aligned to the {interval_seconds}s grid",
            )
        )
    return anomalies


def _delivery_anomalies(
    candles: Sequence[CandleRow],
    settings: _AuditSettings,
) -> tuple[list[CandleAnomaly], set[datetime]]:
    """Return row, duplicate, and ordering findings in delivered order."""
    anomalies: list[CandleAnomaly] = []
    seen: set[datetime] = set()
    duplicated: set[datetime] = set()
    previous_open: datetime | None = None
    for row in candles:
        anomalies.extend(
            _row_anomalies(
                row,
                settings.interval_seconds,
                settings.anchor_offset_seconds,
            )
        )
        current_open = _as_utc(row["open_at"])
        if current_open in seen:
            duplicated.add(current_open)
            anomalies.append(
                CandleAnomaly(
                    CandleAnomalyType.DUPLICATE_OPEN_AT,
                    current_open,
                    f"duplicate open_at {current_open.isoformat()}",
                )
            )
        if previous_open is not None and current_open < previous_open:
            anomalies.append(
                CandleAnomaly(
                    CandleAnomalyType.OUT_OF_ORDER,
                    current_open,
                    f"{current_open.isoformat()} precedes {previous_open.isoformat()}",
                )
            )
        seen.add(current_open)
        previous_open = current_open
    return anomalies, duplicated


def _pair_anomalies(
    previous_time: datetime,
    current_time: datetime,
    unique_rows: dict[datetime, CandleRow],
    duplicated: set[datetime],
    settings: _AuditSettings,
) -> list[CandleAnomaly]:
    """Return gap and split findings for one chronological candle pair."""
    anomalies: list[CandleAnomaly] = []
    missing = (
        _grid_slot(
            current_time,
            settings.interval_seconds,
            settings.anchor_offset_seconds,
        )
        - _grid_slot(
            previous_time,
            settings.interval_seconds,
            settings.anchor_offset_seconds,
        )
        - 1
    )
    gap_expected = (
        missing > 0
        and settings.expected_gap is not None
        and settings.expected_gap(
            previous_time,
            current_time,
        )
    )
    if missing > 0 and not gap_expected:
        anomalies.append(
            CandleAnomaly(
                CandleAnomalyType.GAP,
                current_time,
                f"{missing} missing bar(s) after {previous_time.isoformat()}",
            )
        )
    previous_close = unique_rows[previous_time]["close"]
    current_close = unique_rows[current_time]["close"]
    split_eligible = (
        previous_close > 0.0 and previous_time not in duplicated and current_time not in duplicated
    )
    if split_eligible:
        change = abs(current_close - previous_close) / previous_close
        if change > settings.split_threshold:
            anomalies.append(
                CandleAnomaly(
                    CandleAnomalyType.SPLIT_SUSPECT,
                    current_time,
                    f"close {previous_close} -> {current_close} ({change:.1%})",
                )
            )
    return anomalies


def _chronology_anomalies(
    candles: Sequence[CandleRow],
    duplicated: set[datetime],
    settings: _AuditSettings,
) -> list[CandleAnomaly]:
    """Return gap and split findings over sorted unique timestamps."""
    unique_rows = {_as_utc(row["open_at"]): row for row in candles}
    anomalies: list[CandleAnomaly] = []
    previous_time: datetime | None = None
    for current_time in sorted(unique_rows):
        if previous_time is not None:
            anomalies.extend(
                _pair_anomalies(
                    previous_time,
                    current_time,
                    unique_rows,
                    duplicated,
                    settings,
                )
            )
        previous_time = current_time
    return anomalies


def audit_candle_series(
    candles: Sequence[CandleRow],
    timeframe: str,
    *,
    split_threshold: float = 0.30,
    anchor_offset_seconds: int = 0,
    expected_gap: Callable[[datetime, datetime], bool] | None = None,
) -> list[CandleAnomaly]:
    """Audit one instrument's candle series for one timeframe.

    Args:
        candles: Candle rows, expected in ascending ``open_at`` order. Ordering
            and duplicate violations are detected on the delivered order, while
            gap and split detection runs over the chronologically-sorted unique
            timestamps so a merely-permuted-but-complete series yields no false
            gaps or split suspects. A timestamp seen more than once is reported
            as a duplicate and excluded from split detection, whose close would
            otherwise depend on arbitrary delivery order.
        timeframe: The bars' timeframe (``1m``/``5m``/``15m``/``30m``/``1h``/``4h``/``1d``).
        split_threshold: Fractional close-to-close move above which a bar is
            flagged as a split suspect. Default 0.30 (30%). This is a heuristic
            best suited to instruments that actually split (equities); on highly
            volatile or micro-priced assets (crypto) it flags normal moves, so
            tune or ignore SPLIT_SUSPECT there.
        anchor_offset_seconds: Grid-origin offset from the UNIX epoch used by the
            intraday alignment check. Default 0 aligns to the epoch (correct for
            24/7 venues); pass a session offset for venue-anchored bars.
        expected_gap: Optional predicate ``(prev_open, next_open) -> bool``; when
            it returns True the gap between those bars is treated as expected
            (e.g. a market closure) and not reported.

    Returns:
        The detected anomalies: row-level and ordering anomalies in delivered
        order, followed by gap and split anomalies in chronological order.

    Raises:
        ValueError: If the timeframe is not supported.
    """
    settings = _AuditSettings(
        interval_seconds=_timeframe_to_seconds(timeframe),
        split_threshold=split_threshold,
        anchor_offset_seconds=anchor_offset_seconds,
        expected_gap=expected_gap,
    )
    anomalies, duplicated = _delivery_anomalies(candles, settings)
    anomalies.extend(_chronology_anomalies(candles, duplicated, settings))
    return anomalies
