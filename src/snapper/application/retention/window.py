"""Shared retention-window helper.

Computes the per-tick ``(day_start, day_end)`` window from a
:class:`RetentionPolicy` using the formula:

.. code-block:: python

    oldest_kept_day      = today_utc - timedelta(days=policy.retain_days)
    last_eligible_day    = oldest_kept_day - timedelta(days=1)
    earliest_scanned_day = last_eligible_day - timedelta(days=policy.backlog_lookback_days)

Single source of truth for both :class:`RetentionService`
(which calls :meth:`EventArchiver.export(day_start, day_end, ...)`) and
the per-table ``archivable`` counter (which counts rows in the
same window). Drift here would mean the counter reports a different
number than the next retention cycle actually purges.
"""

from datetime import date
from datetime import timedelta

from snapper.application.retention.policies import RetentionPolicy


def compute_retention_window(today_utc: date, policy: RetentionPolicy) -> tuple[date, date]:
    """Map ``policy`` to a per-tick ``(day_start, day_end)`` window.

    Args:
        today_utc: UTC date to anchor the window against (typically
            ``datetime.now(UTC).date()``).
        policy: Retention rule with ``retain_days`` +
            ``backlog_lookback_days``.

    Returns:
        ``(earliest_scanned_day, last_eligible_day)`` — the
        ``(day_start, day_end)`` pair to pass to
        :meth:`EventArchiver.export` (whole-day inclusive). The query
        that uses these dates emits a HALF-OPEN timestamp predicate:
        ``timestamp >= datetime(day_start, 0, 0, UTC)`` AND
        ``timestamp < datetime(day_end + 1d, 0, 0, UTC)``.
    """
    oldest_kept_day = today_utc - timedelta(days=policy.retain_days)
    last_eligible_day = oldest_kept_day - timedelta(days=1)
    earliest_scanned_day = last_eligible_day - timedelta(days=policy.backlog_lookback_days)
    return earliest_scanned_day, last_eligible_day
