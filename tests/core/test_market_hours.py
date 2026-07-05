"""Tests for :mod:`snapper.core.market_hours`.

Pins the shared CME closure calendar against the production candle
baseline. The schedule is Chicago wall time (16:00-17:00 CT daily
break, Friday 16:00 CT to Sunday 17:00 CT weekend), so the UTC
windows shift with US DST: 21:00-22:00 UTC in summer (CDT), one
hour later in winter (CST) — the original UTC-anchored helper got
winter wrong. Reference week: 2026-06-29 (Monday) through
2026-07-05 (Sunday), which includes the Independence Day observance
(July 3 halt at 12:00 CT = 17:00 UTC) that produced the 2026-07-03
false-positive watchdog warnings this calendar now suppresses.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import pytest

from snapper.core.market_hours import is_cme_closed
from snapper.core.market_hours import last_cme_reopen


class TestIsCmeClosed:
    """Closure windows across the week, both boundary sides."""

    @pytest.mark.parametrize(
        ("now_utc", "expected"),
        [
            (datetime(2026, 6, 29, 20, 59, tzinfo=UTC), False),
            (datetime(2026, 6, 29, 21, 0, tzinfo=UTC), True),
            (datetime(2026, 6, 29, 21, 59, tzinfo=UTC), True),
            (datetime(2026, 6, 29, 22, 0, tzinfo=UTC), False),
            (datetime(2026, 7, 1, 12, 0, tzinfo=UTC), False),
            (datetime(2026, 7, 3, 16, 59, tzinfo=UTC), False),
            (datetime(2026, 7, 3, 17, 0, tzinfo=UTC), True),
            (datetime(2026, 7, 3, 20, 59, tzinfo=UTC), True),
            (datetime(2026, 7, 3, 21, 30, tzinfo=UTC), True),
            (datetime(2026, 7, 3, 22, 1, tzinfo=UTC), True),
            (datetime(2026, 7, 4, 12, 0, tzinfo=UTC), True),
            (datetime(2026, 7, 5, 21, 59, tzinfo=UTC), True),
            (datetime(2026, 7, 5, 22, 0, tzinfo=UTC), False),
        ],
    )
    def test_closure_windows_across_the_week(self, now_utc: datetime, expected: bool) -> None:
        """Closure verdict matches the venue schedule at each boundary.

        Given: Instants around the daily break, the Friday July 3
            early-close observance (halt 12:00 CT = 17:00 UTC), the
            Saturday full closure, and the Sunday reopen,
        When: ``is_cme_closed`` is evaluated,
        Then: It reports closed exactly inside the scheduled windows.
        """
        assert is_cme_closed(now_utc) is expected

    @pytest.mark.parametrize(
        ("now_utc", "expected"),
        [
            (datetime(2026, 1, 14, 21, 30, tzinfo=UTC), False),
            (datetime(2026, 1, 14, 22, 0, tzinfo=UTC), True),
            (datetime(2026, 1, 14, 22, 59, tzinfo=UTC), True),
            (datetime(2026, 1, 14, 23, 0, tzinfo=UTC), False),
        ],
    )
    def test_winter_break_is_one_utc_hour_later(self, now_utc: datetime, expected: bool) -> None:
        """Under CST the 16:00-17:00 CT break is 22:00-23:00 UTC.

        Given: Instants around the daily break on a winter Wednesday
            (2026-01-14, CST = UTC-6),
        When: ``is_cme_closed`` is evaluated,
        Then: The break lands one UTC hour later than in summer — the
            regression the old UTC-anchored 21:00-22:00 window carried.
        """
        assert is_cme_closed(now_utc) is expected

    @pytest.mark.parametrize(
        ("now_utc", "expected"),
        [
            (datetime(2026, 1, 19, 17, 59, tzinfo=UTC), False),
            (datetime(2026, 1, 19, 18, 0, tzinfo=UTC), True),
            (datetime(2026, 1, 19, 22, 59, tzinfo=UTC), True),
            (datetime(2026, 1, 19, 23, 0, tzinfo=UTC), False),
            (datetime(2026, 4, 3, 13, 14, tzinfo=UTC), False),
            (datetime(2026, 4, 3, 13, 15, tzinfo=UTC), True),
            (datetime(2026, 4, 4, 12, 0, tzinfo=UTC), True),
            (datetime(2026, 4, 5, 22, 0, tzinfo=UTC), False),
            (datetime(2026, 12, 24, 18, 14, tzinfo=UTC), False),
            (datetime(2026, 12, 24, 18, 15, tzinfo=UTC), True),
            (datetime(2026, 12, 24, 23, 30, tzinfo=UTC), True),
            (datetime(2026, 12, 25, 12, 0, tzinfo=UTC), True),
            (datetime(2026, 12, 27, 23, 0, tzinfo=UTC), False),
        ],
    )
    def test_holiday_closures(self, now_utc: datetime, expected: bool) -> None:
        """Holiday intervals overlay the weekly schedule.

        Given: MLK Monday (halt 12:00 CST = 18:00 UTC, same-day reopen
            17:00 CST = 23:00 UTC), Good Friday 2026 (abbreviated NFP
            session halting 08:15 CDT = 13:15 UTC, closed through the
            weekend), and Christmas Eve (halt 12:15 CST = 18:15 UTC
            with NO evening session, closed until Sunday 23:00 UTC),
        When: ``is_cme_closed`` is evaluated,
        Then: The curated closure intervals report exactly the venue's
            published windows.
        """
        assert is_cme_closed(now_utc) is expected

    def test_naive_datetime_is_assumed_utc(self) -> None:
        """A naive instant is interpreted as UTC wall time.

        Given: A naive Wednesday 21:30 (inside the daily break in UTC),
        When: ``is_cme_closed`` is evaluated,
        Then: The window verdict matches the aware-UTC equivalent.
        """
        assert is_cme_closed(datetime(2026, 7, 1, 21, 30)) is True

    def test_non_utc_timezone_is_converted(self) -> None:
        """An aware non-UTC instant is converted before window checks.

        Given: Wednesday 23:30 at UTC+2 (= 21:30 UTC, inside the break),
        When: ``is_cme_closed`` is evaluated,
        Then: It reports closed.
        """
        plus_two = timezone(timedelta(hours=2))
        assert is_cme_closed(datetime(2026, 7, 1, 23, 30, tzinfo=plus_two)) is True


class TestLastCmeReopen:
    """Most recent closed-to-open transition at or before the instant."""

    @pytest.mark.parametrize(
        ("now_utc", "expected"),
        [
            (datetime(2026, 7, 1, 12, 0, tzinfo=UTC), datetime(2026, 6, 30, 22, 0, tzinfo=UTC)),
            (datetime(2026, 7, 1, 22, 0, tzinfo=UTC), datetime(2026, 7, 1, 22, 0, tzinfo=UTC)),
            (datetime(2026, 7, 1, 23, 30, tzinfo=UTC), datetime(2026, 7, 1, 22, 0, tzinfo=UTC)),
            (datetime(2026, 7, 3, 23, 0, tzinfo=UTC), datetime(2026, 7, 2, 22, 0, tzinfo=UTC)),
            (datetime(2026, 7, 4, 12, 0, tzinfo=UTC), datetime(2026, 7, 2, 22, 0, tzinfo=UTC)),
            (datetime(2026, 7, 5, 21, 0, tzinfo=UTC), datetime(2026, 7, 2, 22, 0, tzinfo=UTC)),
            (datetime(2026, 7, 5, 22, 30, tzinfo=UTC), datetime(2026, 7, 5, 22, 0, tzinfo=UTC)),
            (datetime(2026, 7, 6, 8, 0, tzinfo=UTC), datetime(2026, 7, 5, 22, 0, tzinfo=UTC)),
        ],
    )
    def test_reopen_boundaries(self, now_utc: datetime, expected: datetime) -> None:
        """Reopen resolution skips Friday and Saturday 22:00 boundaries.

        Given: Instants midweek, right at a reopen, during the weekend
            closure, and after the Sunday reopen,
        When: ``last_cme_reopen`` is evaluated,
        Then: It returns the most recent true closed-to-open 22:00 UTC
            boundary — Friday and Saturday 22:00 stay closed and are
            skipped back to Thursday.
        """
        assert last_cme_reopen(now_utc) == expected

    @pytest.mark.parametrize(
        ("now_utc", "expected"),
        [
            (datetime(2026, 7, 6, 8, 0, tzinfo=UTC), datetime(2026, 7, 5, 22, 0, tzinfo=UTC)),
            (datetime(2026, 1, 19, 19, 0, tzinfo=UTC), datetime(2026, 1, 18, 23, 0, tzinfo=UTC)),
            (datetime(2026, 1, 20, 8, 0, tzinfo=UTC), datetime(2026, 1, 19, 23, 0, tzinfo=UTC)),
            (datetime(2026, 12, 28, 12, 0, tzinfo=UTC), datetime(2026, 12, 27, 23, 0, tzinfo=UTC)),
            (datetime(2026, 4, 6, 8, 0, tzinfo=UTC), datetime(2026, 4, 5, 22, 0, tzinfo=UTC)),
            (datetime(2026, 1, 15, 8, 0, tzinfo=UTC), datetime(2026, 1, 14, 23, 0, tzinfo=UTC)),
        ],
    )
    def test_reopen_skips_holiday_closures_and_tracks_dst(
        self, now_utc: datetime, expected: datetime
    ) -> None:
        """Reopens resolve to 17:00 CT boundaries that actually open.

        Given: Instants after the July 3 observance, during and after
            MLK Monday (whose 23:00 UTC boundary IS a reopen), after the
            Christmas cluster (Thursday-Saturday evenings all closed),
            after Good Friday weekend, and on a plain winter morning,
        When: ``last_cme_reopen`` is evaluated,
        Then: The clamp lands on the venue's true reopen — 22:00 UTC in
            summer, 23:00 UTC in winter — skipping every 17:00 CT
            boundary swallowed by a holiday interval.
        """
        assert last_cme_reopen(now_utc) == expected

    def test_naive_datetime_is_assumed_utc(self) -> None:
        """A naive instant resolves the same reopen as its UTC twin.

        Given: A naive Monday 08:00 (after the Sunday reopen),
        When: ``last_cme_reopen`` is evaluated,
        Then: Sunday 22:00 UTC is returned as an aware datetime.
        """
        result = last_cme_reopen(datetime(2026, 7, 6, 8, 0))
        assert result == datetime(2026, 7, 5, 22, 0, tzinfo=UTC)
        assert result.tzinfo is not None
