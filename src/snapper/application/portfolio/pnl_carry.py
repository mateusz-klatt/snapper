"""Pure planning for carried P&L valuation inputs (calculation version 5A.14).

The operator decided on 2026-11-09 that a P&L point must not be withheld over a
missing one-minute candle when an earlier price or rate exists. This module holds
the arithmetic-free decisions behind that rule so they can be tested without a
database: which minutes reach back, where a read is genuinely needed, and how far
each read looks.

Reads are planned per series and gap, never per minute. Already-loaded evidence is
consulted first, so a request whose candles are healthy issues no extra query. A
gap then widens through a few bounded windows and stops at the first usable close,
which keeps a dense series to a handful of rows and a sparse one to a few reads.

No candle is written and no value is invented here. A carried input is always a
real earlier close, bounded in age and disclosed by the caller.
"""

from bisect import bisect_left
from collections.abc import Collection
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from typing import Final

from snapper.application.portfolio.fx_rates import FX_CARRY_BOUND

MARK_CARRY_BOUND: Final[timedelta] = timedelta(hours=24)
"""Oldest own close that may value an open position.

An instrument that has not printed for a full day has no defensible price. Like
:data:`~snapper.application.portfolio.fx_rates.FX_CARRY_BOUND`, this changes
published money, so it is a constant tied to the calculation version.
"""

FX_CARRY_READ_STEPS: Final[tuple[timedelta, ...]] = (
    timedelta(hours=1),
    timedelta(hours=6),
    timedelta(hours=24),
    FX_CARRY_BOUND,
)
"""Widening look-backs for one FX plane gap, ending at the carry bound."""

MARK_CARRY_READ_STEPS: Final[tuple[timedelta, ...]] = (
    timedelta(hours=1),
    timedelta(hours=6),
    MARK_CARRY_BOUND,
)
"""Widening look-backs for one instrument's leading mark gap."""

CARRY_ANCHOR_LIMIT: Final[int] = 32
"""Most FX gaps one request may search, each costing at most four reads.

Gaps collapse as they are served, so this bound is only reached by a plane with
evidence scattered thinly across months of history.
"""

_ONE_MINUTE: Final[timedelta] = timedelta(minutes=1)
"""Width of one candle, which also separates a bar's open from its close minute."""


@dataclass(frozen=True, slots=True)
class CarriedMarkSource:
    """The earlier close that values a position at a minute without its own bar.

    Attributes:
        close: Positive finite close in the instrument's quote currency.
        source_minute: Minute that bar closed.
    """

    close: float
    source_minute: datetime


def read_windows(
    anchor: datetime,
    steps: Sequence[timedelta],
) -> list[tuple[datetime, datetime]]:
    """Plan widening, non-overlapping candle-open windows behind one gap.

    The bar that opens one minute before ``anchor`` would be the anchor's own
    exact evidence, so every window ends before it.

    Args:
        anchor: First rate or mark minute of the gap.
        steps: Ascending look-backs measured from ``anchor``.

    Returns:
        Inclusive ``(lower, upper)`` candle-open bounds, newest window first.
    """
    windows: list[tuple[datetime, datetime]] = []
    upper = anchor - 2 * _ONE_MINUTE
    for step in steps:
        lower = anchor - _ONE_MINUTE - step
        windows.append((lower, upper))
        upper = lower - _ONE_MINUTE
    return windows


def _latest_before(
    ordered: Sequence[datetime],
    minute: datetime,
    bound: timedelta,
) -> datetime | None:
    """Return the newest entry strictly before ``minute`` and inside the bound."""
    index = bisect_left(ordered, minute)
    if index == 0 or ordered[index - 1] < minute - bound:
        return None
    return ordered[index - 1]


def carry_serves(
    published: Sequence[datetime],
    minute: datetime,
    bound: timedelta = FX_CARRY_BOUND,
) -> bool:
    """Return whether an ascending publication list can carry into ``minute``.

    Args:
        published: Ascending minutes with a usable close.
        minute: Minute being valued.
        bound: Oldest close a minute may use.

    Returns:
        ``True`` when an earlier usable close lies inside the bound.
    """
    return _latest_before(published, minute, bound) is not None


def carried_mark_sources(
    demand: Mapping[str, Sequence[datetime]],
    closes: Mapping[str, Mapping[datetime, float]],
    bound: timedelta = MARK_CARRY_BOUND,
) -> dict[tuple[str, datetime], CarriedMarkSource]:
    """Choose each demanded minute's latest earlier close inside the bound.

    Args:
        demand: Held minutes with no exact candle, grouped by instrument.
        closes: Positive finite closes by instrument and close minute.
        bound: Oldest close a minute may use.

    Returns:
        Sources keyed by ``(instrument, minute)``. A minute with no close inside
        the bound is absent and stays withheld.
    """
    sources: dict[tuple[str, datetime], CarriedMarkSource] = {}
    for instrument, minutes in demand.items():
        series = closes.get(instrument, {})
        ordered = sorted(series)
        for minute in minutes:
            source = _latest_before(ordered, minute, bound)
            if source is not None:
                sources[(instrument, minute)] = CarriedMarkSource(series[source], source)
    return sources


def leading_mark_gaps(
    demand: Mapping[str, Sequence[datetime]],
    closes: Mapping[str, Mapping[datetime, float]],
    bound: timedelta = MARK_CARRY_BOUND,
) -> dict[str, datetime]:
    """Find instruments whose first demanded minute needs a pre-window read.

    Later minutes are served by closes inside the already-loaded window, so one
    read per instrument is enough.

    Args:
        demand: Held minutes with no exact candle, grouped by instrument.
        closes: Positive finite closes already loaded.
        bound: Oldest close a minute may use.

    Returns:
        The first unserved minute keyed by instrument.
    """
    gaps: dict[str, datetime] = {}
    for instrument, minutes in demand.items():
        first = min(minutes)
        if _latest_before(sorted(closes.get(instrument, {})), first, bound) is None:
            gaps[instrument] = first
    return gaps


def fx_gap_anchors(
    required: Collection[datetime],
    published: Sequence[datetime],
    blocked: Collection[datetime],
    bound: timedelta = FX_CARRY_BOUND,
) -> list[datetime]:
    """Return one read anchor per contiguous run of unserved FX minutes.

    A minute with usable evidence, or with contradictory exact evidence that must
    stay withheld, needs no read. Consecutive unserved minutes share a look-back,
    so a whole run costs one search instead of one per minute.

    Args:
        required: Minutes this plane must value.
        published: Ascending minutes with a usable close already loaded.
        blocked: Minutes whose exact evidence is conflicted or invalid.
        bound: Oldest close a minute may use.

    Returns:
        Ascending first minutes of each unserved run.
    """
    published_minutes = set(published)
    anchors: list[datetime] = []
    previous: datetime | None = None
    for minute in sorted(set(required)):
        if (
            minute in published_minutes
            or minute in blocked
            or _latest_before(published, minute, bound) is not None
        ):
            previous = None
            continue
        if previous is None or minute - previous > _ONE_MINUTE:
            anchors.append(minute)
        previous = minute
    return anchors
