"""Boundary-formula tests for :func:`compute_retention_window`.

Pins the per-tick ``(day_start, day_end)`` mapping shared by Cluster C
:class:`RetentionService` and Cluster B's per-table ``archivable``
counter. Drift here is a Cluster B/C contract break — see
``proprietary/plans/plan_observability_cluster_b.md`` §7.3.
"""

from datetime import date

from snapper.application.retention.policies import RetentionPolicy
from snapper.application.retention.window import compute_retention_window


class TestComputeRetentionWindow:
    """Per-tick boundary formula (Cluster C plan §3.6)."""

    def test_telemetry_policy_today_2026_05_01(self) -> None:
        """SC#4 fixture — ``retain_days=1, backlog_lookback_days=30``."""
        policy = RetentionPolicy(table="telemetry", retain_days=1, backlog_lookback_days=30)
        day_start, day_end = compute_retention_window(date(2026, 5, 1), policy)
        assert day_start == date(2026, 3, 30)
        assert day_end == date(2026, 4, 29)

    def test_zero_lookback_yields_one_day_window(self) -> None:
        """``backlog_lookback_days=0`` → window covers exactly one day."""
        policy = RetentionPolicy(table="telemetry", retain_days=2, backlog_lookback_days=0)
        day_start, day_end = compute_retention_window(date(2026, 5, 1), policy)
        assert day_start == day_end == date(2026, 4, 28)

    def test_large_retain_pushes_window_into_past(self) -> None:
        """Large ``retain_days`` shifts both bounds proportionally."""
        policy = RetentionPolicy(table="telemetry", retain_days=365, backlog_lookback_days=30)
        day_start, day_end = compute_retention_window(date(2026, 5, 1), policy)
        assert day_end == date(2025, 4, 30)
        assert day_start == date(2025, 3, 31)

    def test_zero_retain_yesterday_is_eligible(self) -> None:
        """``retain_days=0`` → yesterday becomes the last eligible day."""
        policy = RetentionPolicy(table="telemetry", retain_days=0, backlog_lookback_days=7)
        day_start, day_end = compute_retention_window(date(2026, 5, 1), policy)
        assert day_end == date(2026, 4, 30)
        assert day_start == date(2026, 4, 23)
