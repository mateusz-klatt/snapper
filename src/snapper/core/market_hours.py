"""Shared CME trading-calendar helpers.

The Kraken Equities feed carries CME-family (CME/CBOT/NYMEX/COMEX)
FCM contracts whose scheduled closures are venue facts, not feed
defects: a daily maintenance break 21:00-22:00 UTC on trading days
and a weekend closure from Friday 21:00 UTC until Sunday 22:00 UTC.
Both the equities publisher (to suppress liveness recovery) and the
market-data watchdog (to suppress silent-exchange alerts and clamp
the silence clock to the most recent reopen) need the same calendar,
so it lives here in ``core`` below both the messaging and the
application layers.

Production candle history confirms the windows: 1m candles stop at
20:59 UTC and resume at 22:00 UTC sharp on trading days — including
Friday, where the daily break runs straight into the weekend
closure. The original publisher-private helper treated Friday
21:00-22:00 UTC as open (its Friday branch only tested the weekend
rule); that blind spot merely delayed a redundant recovery attempt
there, but it would make a freshness watchdog page every Friday
evening, so this shared version closes it.
"""

from datetime import UTC
from datetime import datetime
from datetime import time as datetime_time
from datetime import timedelta
from typing import Final

CME_DAILY_BREAK_START: Final = datetime_time(hour=21)
"""Start of the daily CME maintenance break (UTC wall time)."""

CME_DAILY_BREAK_END: Final = datetime_time(hour=22)
"""End of the daily CME maintenance break (UTC wall time)."""

_FRIDAY: Final = 4
_SATURDAY: Final = 5
_SUNDAY: Final = 6

_REOPEN_LESS_WEEKDAYS: Final = frozenset({_FRIDAY, _SATURDAY})
"""Weekdays whose 22:00 UTC boundary does NOT reopen the market.

Friday 22:00 UTC rolls the daily break straight into the weekend
closure and Saturday is closed all day, so neither day's 22:00
boundary is a closed-to-open transition.
"""


def _as_utc(now_utc: datetime) -> datetime:
    """Coerce ``now_utc`` to an aware UTC datetime.

    Args:
        now_utc: Reference instant; a naive value is assumed to
            already be UTC wall time.

    Returns:
        The same instant as an aware UTC datetime.
    """
    current = now_utc if now_utc.tzinfo is not None else now_utc.replace(tzinfo=UTC)
    return current.astimezone(UTC)


def is_cme_closed(now_utc: datetime) -> bool:
    """Return whether CME FCM contracts are in a scheduled closure window.

    Closure windows (all UTC): the daily maintenance break
    21:00-22:00 on Monday-Friday, and the weekend closure from
    Friday 21:00 (the daily break rolls straight into it) until
    Sunday 22:00.

    Args:
        now_utc: Reference instant; a naive value is assumed UTC.

    Returns:
        ``True`` inside a scheduled closure window, ``False`` when
        the venue is open.
    """
    current = _as_utc(now_utc)
    weekday = current.weekday()
    current_time = current.time()
    if weekday == _SATURDAY:
        return True
    if weekday == _SUNDAY:
        return current_time < CME_DAILY_BREAK_END
    if weekday == _FRIDAY:
        return current_time >= CME_DAILY_BREAK_START
    return CME_DAILY_BREAK_START <= current_time < CME_DAILY_BREAK_END


def last_cme_reopen(now_utc: datetime) -> datetime:
    """Return the most recent closed-to-open CME transition at or before ``now_utc``.

    Every reopen happens at 22:00 UTC — after the daily break on
    Monday-Thursday and after the weekend closure on Sunday. Friday
    and Saturday 22:00 boundaries stay closed, so they are skipped
    backwards until a true reopen day is found.

    A freshness monitor uses this to clamp its silence clock: right
    after a scheduled closure the newest candle is legitimately old,
    so silence must be measured from the reopen, not from the last
    pre-closure candle.

    Args:
        now_utc: Reference instant; a naive value is assumed UTC.

    Returns:
        Aware UTC datetime of the most recent reopen boundary at or
        before ``now_utc``.
    """
    current = _as_utc(now_utc)
    candidate = current.replace(hour=CME_DAILY_BREAK_END.hour, minute=0, second=0, microsecond=0)
    if candidate > current:
        candidate -= timedelta(days=1)
    while candidate.weekday() in _REOPEN_LESS_WEEKDAYS:
        candidate -= timedelta(days=1)
    return candidate
