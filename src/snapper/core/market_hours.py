"""Shared CME trading-calendar helpers.

The Kraken Equities feed carries CME-family (CME/CBOT/NYMEX/COMEX)
FCM contracts whose scheduled closures are venue facts, not feed
defects. Both the equities publisher (to suppress liveness recovery)
and the market-data watchdog (to suppress silent-exchange alerts and
clamp the silence clock to the most recent reopen) need the same
calendar, so it lives here in ``core`` below both the messaging and
the application layers.

The schedule is defined in Chicago WALL TIME (the venue's clock) and
converted per call: the regular session runs Sunday 17:00 CT through
Friday 16:00 CT with a daily 16:00-17:00 CT maintenance break. An
earlier revision hardcoded the break as 21:00-22:00 UTC — correct
only under daylight saving time (CDT); in winter (CST) the same wall
window is 22:00-23:00 UTC, so the UTC-anchored version would have
mis-timed every closure between November and March.

US holidays overlay the weekly schedule via two module-level tables
(``CME_FULL_CLOSURE_DATES`` and ``CME_EARLY_CLOSES``). CME finalizes
holiday hours only ~2 weeks before each holiday (with NYSE/SIFMA
input), so the tables are CURATED, not derived: they must be
refreshed periodically from
https://www.cmegroup.com/tools-information/holiday-calendar.html and
extended before each new year. An exhausted table fails OPEN (no
holiday suppression) — the watchdog then pages spuriously on a
holiday, which is the safe direction for a monitoring calendar.
"""

from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import time as datetime_time
from datetime import timedelta
from typing import Final
from zoneinfo import ZoneInfo

CME_TZ: Final = ZoneInfo("America/Chicago")
"""The venue clock: every schedule rule below is Chicago wall time."""

CME_DAILY_BREAK_START_CT: Final = datetime_time(hour=16)
"""Start of the daily CME maintenance break (Chicago wall time)."""

CME_DAILY_BREAK_END_CT: Final = datetime_time(hour=17)
"""End of the daily CME maintenance break and the reopen wall time."""

_FRIDAY: Final = 4
_SATURDAY: Final = 5
_SUNDAY: Final = 6

US_EQUITY_FULL_CLOSURES: Final[frozenset[date]] = frozenset(
    {
        date(2026, 1, 1),
        date(2026, 1, 19),
        date(2026, 2, 16),
        date(2026, 4, 3),
        date(2026, 5, 25),
        date(2026, 6, 19),
        date(2026, 7, 3),
        date(2026, 9, 7),
        date(2026, 11, 26),
        date(2026, 12, 25),
        date(2027, 1, 1),
        date(2027, 1, 18),
        date(2027, 2, 15),
        date(2027, 3, 26),
        date(2027, 5, 31),
        date(2027, 6, 18),
        date(2027, 7, 5),
        date(2027, 9, 6),
        date(2027, 11, 25),
        date(2027, 12, 24),
    }
)
"""Full-day US equity market closures covering migration and forward operation."""


def _ct(year: int, month: int, day: int, hour: int, minute: int = 0) -> datetime:
    """Build an aware America/Chicago wall-time instant for the tables below.

    Args:
        year: Calendar year.
        month: Calendar month.
        day: Calendar day.
        hour: Wall-clock hour in Chicago.
        minute: Wall-clock minute in Chicago.

    Returns:
        Aware datetime in ``CME_TZ``.
    """
    return datetime(year, month, day, hour, minute, tzinfo=CME_TZ)


CME_HOLIDAY_CLOSURES: Final[tuple[tuple[datetime, datetime], ...]] = (
    (_ct(2025, 12, 31, 16), _ct(2026, 1, 1, 17)),
    (_ct(2026, 1, 19, 12), _ct(2026, 1, 19, 17)),
    (_ct(2026, 2, 16, 12), _ct(2026, 2, 16, 17)),
    (_ct(2026, 4, 3, 8, 15), _ct(2026, 4, 5, 17)),
    (_ct(2026, 5, 25, 12), _ct(2026, 5, 25, 17)),
    (_ct(2026, 6, 19, 12), _ct(2026, 6, 21, 17)),
    (_ct(2026, 7, 3, 12), _ct(2026, 7, 5, 17)),
    (_ct(2026, 9, 7, 12), _ct(2026, 9, 7, 17)),
    (_ct(2026, 11, 26, 12), _ct(2026, 11, 26, 17)),
    (_ct(2026, 11, 27, 12, 15), _ct(2026, 11, 29, 17)),
    (_ct(2026, 12, 24, 12, 15), _ct(2026, 12, 27, 17)),
    (_ct(2026, 12, 31, 16), _ct(2027, 1, 3, 17)),
    (_ct(2027, 1, 18, 12), _ct(2027, 1, 18, 17)),
    (_ct(2027, 2, 15, 12), _ct(2027, 2, 15, 17)),
    (_ct(2027, 3, 25, 16), _ct(2027, 3, 28, 17)),
    (_ct(2027, 5, 31, 12), _ct(2027, 5, 31, 17)),
    (_ct(2027, 6, 18, 12), _ct(2027, 6, 20, 17)),
    (_ct(2027, 7, 5, 12), _ct(2027, 7, 5, 17)),
)
"""US-holiday closure intervals for CME equity-index products (CT wall time).

Curated 2026-07-05 from CME's own trading-hours API (product ES,
Wayback captures of cmegroup.com/services/trading-hours-by-product)
plus per-holiday clearing advisories. Stable venue pattern encoded:
Monday holidays and Thanksgiving Thursday HALT at 12:00 CT and
REOPEN the same day at 17:00 CT (next trade date); Friday
observances (Juneteenth, July 3) halt 12:00 CT and stay closed
through the weekend; the two half-days (day after Thanksgiving,
Christmas Eve) halt at 12:15 CT; full closures are Good Friday,
Christmas Day, and New Year's Day only. Good Friday 2026-04-03 is
the NFP-release exception: an abbreviated session halting 08:15 CT.
New Year's Eve trades a NORMAL full session (16:00 CT close, no
evening reopen). 2026 H2 entries are CME-preliminary and every 2027
entry past New Year's is pattern-projected — CME finalizes each
holiday ~2 weeks ahead, so refresh this table periodically from
https://www.cmegroup.com/tools-information/holiday-calendar.html
and EXTEND it before each new year. An exhausted table fails OPEN
(spurious watchdog pages on a holiday — the safe direction).

Intervals overlap the weekly schedule freely (the checks are OR-ed),
and none of them straddles a US DST transition.

SCOPE — this table models the KRAKEN FCM FEED, not raw CME product
groups. The feed mixes equity-index with NYMEX/COMEX contracts whose
official half-day halts differ by group (metals/energy publish later
closes than the 12:00 CT equity halt), but the venue EMPIRICALLY
halts as a whole on the equity schedule: on 2026-07-03 every carried
instrument went silent at 12:00 CT sharp (the incident this table
exists to suppress). Accepted residual: a real outage inside a
later-group window on a half-day (~6 days/year, a few hours, ends at
the next reopen check) would be suppressed — traded off against
GUARANTEED false pages every holiday without the table. If the feed
is ever observed ticking past an encoded halt, tighten that entry
instead of widening the alert window.
"""


def _as_ct(now_utc: datetime) -> datetime:
    """Coerce ``now_utc`` to the venue's Chicago wall clock.

    Args:
        now_utc: Reference instant; a naive value is assumed to
            already be UTC wall time.

    Returns:
        The same instant expressed in America/Chicago.
    """
    current = now_utc if now_utc.tzinfo is not None else now_utc.replace(tzinfo=UTC)
    return current.astimezone(CME_TZ)


def _is_closed_ct(ct: datetime) -> bool:
    """Return whether the venue is closed at a Chicago wall instant.

    Args:
        ct: Aware America/Chicago datetime.

    Returns:
        ``True`` inside the weekend closure, the daily break, or a
        holiday closure interval.
    """
    weekday = ct.weekday()
    wall = ct.time()
    if weekday == _SATURDAY:
        return True
    if weekday == _SUNDAY:
        return wall < CME_DAILY_BREAK_END_CT
    if weekday == _FRIDAY and wall >= CME_DAILY_BREAK_START_CT:
        return True
    if CME_DAILY_BREAK_START_CT <= wall < CME_DAILY_BREAK_END_CT:
        return True
    return any(start <= ct < end for start, end in CME_HOLIDAY_CLOSURES)


def is_cme_closed(now_utc: datetime) -> bool:
    """Return whether CME FCM contracts are in a scheduled closure window.

    Closure windows (all Chicago wall time): the daily maintenance
    break 16:00-17:00 on trading days, the weekend closure from
    Friday 16:00 until Sunday 17:00, full holiday closures, and
    early-close halts from the published halt time onward.

    Args:
        now_utc: Reference instant; a naive value is assumed UTC.

    Returns:
        ``True`` inside a scheduled closure window, ``False`` when
        the venue is open.
    """
    return _is_closed_ct(_as_ct(now_utc))


def is_us_equity_market_closed(day: date) -> bool:
    """Return whether US equities have no regular session on a date.

    Args:
        day: US market calendar date.

    Returns:
        True for weekends and curated full-day exchange holidays.
    """
    return day.weekday() >= _SATURDAY or day in US_EQUITY_FULL_CLOSURES


def last_cme_reopen(now_utc: datetime) -> datetime:
    """Return the most recent closed-to-open CME transition at or before ``now_utc``.

    Every reopen happens at 17:00 CT — after the daily break on
    regular days, after the weekend closure on Sunday, and after a
    holiday closure on the first evening that starts an open
    session. A candidate 17:00 CT boundary counts only when the
    venue is actually OPEN right at it: Friday and Saturday
    boundaries stay closed (weekend), and a boundary that opens into
    a full-closure evening is skipped further back.

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
    ct = _as_ct(now_utc)
    candidate = ct.replace(hour=CME_DAILY_BREAK_END_CT.hour, minute=0, second=0, microsecond=0)
    if candidate > ct:
        candidate -= timedelta(days=1)
    while _is_closed_ct(candidate):
        candidate -= timedelta(days=1)
    return candidate.astimezone(UTC)


def next_cme_open(now_utc: datetime) -> datetime:
    """Return the next closed-to-open CME transition at or after ``now_utc``.

    The forward dual of :func:`last_cme_reopen`. Every reopen happens at
    17:00 CT, so the candidate is seeded at that boundary and stepped
    forward a day at a time until it lands on an instant the venue is
    actually OPEN: the daily break resolves to today 17:00 CT, the weekend
    closure to Sunday 17:00 CT, and a holiday closure to the 17:00 CT
    evening that begins the next open session.

    A market-closed badge reads this to show when the feed will resume.

    Args:
        now_utc: Reference instant; a naive value is assumed UTC.

    Returns:
        Aware UTC datetime of the next reopen boundary at or after
        ``now_utc``.
    """
    ct = _as_ct(now_utc)
    candidate = ct.replace(hour=CME_DAILY_BREAK_END_CT.hour, minute=0, second=0, microsecond=0)
    if candidate < ct:
        candidate += timedelta(days=1)
    while _is_closed_ct(candidate):
        candidate += timedelta(days=1)
    return candidate.astimezone(UTC)
