"""Tests for :mod:`snapper.core.market_hours`.

Pins the shared CME closure calendar against the production candle
baseline: 1m candles stop at 20:59 UTC and resume at 22:00 UTC sharp
on trading days (Friday included — the daily break rolls straight
into the weekend closure), and the weekend gap runs Friday 21:00 UTC
to Sunday 22:00 UTC. Reference week: 2026-06-29 (Monday) through
2026-07-05 (Sunday).
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
            (datetime(2026, 7, 3, 20, 59, tzinfo=UTC), False),
            (datetime(2026, 7, 3, 21, 0, tzinfo=UTC), True),
            (datetime(2026, 7, 3, 21, 30, tzinfo=UTC), True),
            (datetime(2026, 7, 3, 22, 1, tzinfo=UTC), True),
            (datetime(2026, 7, 4, 12, 0, tzinfo=UTC), True),
            (datetime(2026, 7, 5, 21, 59, tzinfo=UTC), True),
            (datetime(2026, 7, 5, 22, 0, tzinfo=UTC), False),
        ],
    )
    def test_closure_windows_across_the_week(self, now_utc: datetime, expected: bool) -> None:
        """Closure verdict matches the venue schedule at each boundary.

        Given: Instants around the daily break, the Friday roll-in, the
            Saturday full closure, and the Sunday reopen,
        When: ``is_cme_closed`` is evaluated,
        Then: It reports closed exactly inside the scheduled windows —
            including Friday 21:00-22:00 UTC, the blind spot of the old
            publisher-private helper.
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

    def test_naive_datetime_is_assumed_utc(self) -> None:
        """A naive instant resolves the same reopen as its UTC twin.

        Given: A naive Monday 08:00 (after the Sunday reopen),
        When: ``last_cme_reopen`` is evaluated,
        Then: Sunday 22:00 UTC is returned as an aware datetime.
        """
        result = last_cme_reopen(datetime(2026, 7, 6, 8, 0))
        assert result == datetime(2026, 7, 5, 22, 0, tzinfo=UTC)
        assert result.tzinfo is not None
