"""Market-time evidence shared by live candle price consumers."""

from datetime import datetime
from datetime import timedelta

from snapper.core.numeric import is_positive_finite

CANDLE_INTERVAL_SECONDS = 60
STALENESS_THRESHOLD_SECONDS = 300


def candle_evidence_timestamp(open_at: datetime, now: datetime) -> datetime:
    """Return the freshness reference for a 1m candle opened at ``open_at``.

    A 1m candle opened at ``T`` covers market activity through ``T + 60s``.
    For completed bars (``T + 60s <= now``) the effective time is ``T + 60s``.
    For an in-progress current-minute bar (``T <= now < T + 60s``), capping at
    ``now`` prevents the unelapsed portion of the minute from tripping
    caller's future-skew limit. Future-opened and naive bars retain their
    original timestamps for the caller's validation.

    Args:
        open_at: Candle interval start timestamp.
        now: Evaluation wall-clock timestamp (UTC-aware).

    Returns:
        Effective UTC timestamp for freshness checks and FX plane comparison.
    """
    if open_at.utcoffset() is None:
        return open_at
    close_at = open_at + timedelta(seconds=CANDLE_INTERVAL_SECONDS)
    return max(open_at, min(close_at, now))


def candle_mark(
    close: float | None, open_at: datetime, now: datetime
) -> tuple[float, datetime] | None:
    """Return a valid close and its evidence time, including aged closes.

    Args:
        close: Latest candle's close.
        open_at: Latest candle's interval start.
        now: Evaluation time.

    Returns:
        Price and evidence time, or None for an invalid or future candle.
    """
    if not is_positive_finite(close) or open_at.utcoffset() is None or open_at > now:
        return None
    return close, candle_evidence_timestamp(open_at, now)


def mark_is_stale(marked_at: datetime | None, now: datetime) -> bool:
    """Return whether a present mark is older than the live price window.

    Args:
        marked_at: Evidence time of the displayed mark.
        now: Evaluation time.

    Returns:
        True when the mark is more than 300 seconds old.
    """
    return marked_at is not None and (now - marked_at).total_seconds() > STALENESS_THRESHOLD_SECONDS
