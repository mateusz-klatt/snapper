"""Cluster B/C alignment integration test (plan §7.6).

Pins the exact-equality contract between Cluster B's per-table
``archivable`` counter and Cluster C's dry-run ``archived_rows``: for
the same policy + same ``today_utc`` they MUST count the same rows.
Drift here would mean operators see one number on the dashboard and a
different number in the next retention cycle's summary.

Strategy: seed an isolated SQLite DB with telemetry rows split between
in-window (policy retention range) and out-of-window (today /
yesterday). Run both:

* ``SQLAlchemyRepository.count_table_stats(telemetry_entry,
  archivable_window=compute_retention_window(today, policy))``.
* ``RetentionService.evaluate_policy(policy, dry_run=True)``.

Assert their counts agree exactly.
"""

from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from snapper.application.db_stats.snapshotter import TABLES_TO_SAMPLE
from snapper.application.retention import service as service_module
from snapper.application.retention.policies import RetentionPolicy
from snapper.application.retention.service import RetentionService
from snapper.application.retention.window import compute_retention_window
from snapper.data.db_stats_types import TableEntry
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Telemetry
from snapper.data.repository import SQLAlchemyRepository


def _telemetry_entry() -> TableEntry:
    """Resolve the canonical ``telemetry`` ``TableEntry`` from the registry."""
    for entry in TABLES_TO_SAMPLE:
        if entry.name == "telemetry":
            return entry
    raise AssertionError("telemetry entry missing from TABLES_TO_SAMPLE")


class TestClusterBCAlignment:
    """Plan §7.6 — same window, same archivable count between B and C."""

    @pytest.mark.asyncio
    async def test_archivable_count_equals_archived_rows_for_telemetry_policy(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """70 rows in window + 30 outside → archivable=70 AND archived_rows=70."""
        db_path = tmp_path / "alignment.db"
        db_url = f"sqlite+aiosqlite:///{db_path}"
        today = date(2026, 5, 1)
        policy = RetentionPolicy(table="telemetry", retain_days=1, backlog_lookback_days=30)
        day_start, day_end = compute_retention_window(today, policy)
        in_window_count = 0
        seed_repo = service_module.DatabaseRepository(db_url)
        seed_repo.create_all()
        with seed_repo.get_session() as session:
            for i in range(70):
                day = day_start + timedelta(days=i % ((day_end - day_start).days + 1))
                session.add(
                    Telemetry(
                        public_id=f"00000000-0000-7000-8000-{i:012d}",
                        session_id="align",
                        sequence_id=i,
                        timestamp=datetime(day.year, day.month, day.day, 12, 0, tzinfo=UTC),
                        known_to=KNOWN_TO_MAX,
                        transport="ws",
                        direction="in",
                        message_type="ping",
                        payload=None,
                    )
                )
                in_window_count += 1
            for i in range(15):
                session.add(
                    Telemetry(
                        public_id=f"10000000-0000-7000-8000-{i:012d}",
                        session_id="align",
                        sequence_id=70 + i,
                        timestamp=datetime(today.year, today.month, today.day, 6, 0, tzinfo=UTC),
                        known_to=KNOWN_TO_MAX,
                        transport="ws",
                        direction="in",
                        message_type="ping",
                        payload=None,
                    )
                )
            yesterday = today - timedelta(days=1)
            for i in range(15):
                session.add(
                    Telemetry(
                        public_id=f"20000000-0000-7000-8000-{i:012d}",
                        session_id="align",
                        sequence_id=85 + i,
                        timestamp=datetime(
                            yesterday.year,
                            yesterday.month,
                            yesterday.day,
                            6,
                            0,
                            tzinfo=UTC,
                        ),
                        known_to=KNOWN_TO_MAX,
                        transport="ws",
                        direction="in",
                        message_type="ping",
                        payload=None,
                    )
                )
            session.commit()
        seed_repo.dispose()

        async_repo = SQLAlchemyRepository(db_url)
        try:
            counters = await async_repo.count_table_stats(
                _telemetry_entry(),
                archivable_window=(day_start, day_end),
            )
        finally:
            engine = async_repo.engine
            await engine.dispose()

        class _FrozenDatetime(datetime):
            @classmethod
            def now(cls, tz: Any = None) -> _FrozenDatetime:
                return cls(today.year, today.month, today.day, 12, 0, tzinfo=tz or UTC)

        monkeypatch.setattr(service_module, "datetime", _FrozenDatetime)
        monkeypatch.delenv("RETENTION_DRY_RUN", raising=False)
        service = RetentionService(db_url=db_url, base_dir=tmp_path)
        try:
            result = await service.evaluate_policy(policy, dry_run=True)
        finally:
            await service.close()

        assert result["error"] is None
        assert counters.archivable == result["archived_rows"] == in_window_count == 70
        assert counters.total == 100

    @pytest.mark.asyncio
    async def test_boundary_edges_agree_between_b_and_c(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """Plan §7.6 boundary edge cases: both sides count the same edge rows.

        Seed 3 telemetry rows at the exact half-open window edges:
        ``day_start 00:00 UTC`` (INCLUDED), ``day_end 23:59:59 UTC``
        (INCLUDED), ``day_end + 1d 00:00 UTC`` (EXCLUDED). Both
        Cluster B's ``count_table_stats`` and Cluster C's
        ``evaluate_policy(dry_run=True)`` MUST count exactly 2.
        """
        db_path = tmp_path / "boundary.db"
        db_url = f"sqlite+aiosqlite:///{db_path}"
        today = date(2026, 5, 1)
        policy = RetentionPolicy(table="telemetry", retain_days=1, backlog_lookback_days=30)
        day_start, day_end = compute_retention_window(today, policy)

        seed_repo = service_module.DatabaseRepository(db_url)
        seed_repo.create_all()
        edge_rows = [
            (
                "at_start_midnight",
                datetime(day_start.year, day_start.month, day_start.day, 0, 0, tzinfo=UTC),
            ),
            (
                "at_end_last_second",
                datetime(day_end.year, day_end.month, day_end.day, 23, 59, 59, tzinfo=UTC),
            ),
            (
                "at_end_plus_one_midnight",
                datetime(
                    (day_end + timedelta(days=1)).year,
                    (day_end + timedelta(days=1)).month,
                    (day_end + timedelta(days=1)).day,
                    0,
                    0,
                    tzinfo=UTC,
                ),
            ),
        ]
        with seed_repo.get_session() as session:
            for i, (label, ts) in enumerate(edge_rows):
                session.add(
                    Telemetry(
                        public_id=f"00000000-0000-7000-8000-{i:012d}",
                        session_id=label,
                        sequence_id=i,
                        timestamp=ts,
                        known_to=KNOWN_TO_MAX,
                        transport="ws",
                        direction="in",
                        message_type="ping",
                        payload=None,
                    )
                )
            session.commit()
        seed_repo.dispose()

        async_repo = SQLAlchemyRepository(db_url)
        try:
            counters = await async_repo.count_table_stats(
                _telemetry_entry(),
                archivable_window=(day_start, day_end),
            )
        finally:
            await async_repo.engine.dispose()

        class _FrozenDatetime(datetime):
            @classmethod
            def now(cls, tz: Any = None) -> _FrozenDatetime:
                return cls(today.year, today.month, today.day, 12, 0, tzinfo=tz or UTC)

        monkeypatch.setattr(service_module, "datetime", _FrozenDatetime)
        monkeypatch.delenv("RETENTION_DRY_RUN", raising=False)
        service = RetentionService(db_url=db_url, base_dir=tmp_path)
        try:
            result = await service.evaluate_policy(policy, dry_run=True)
        finally:
            await service.close()

        assert result["error"] is None
        assert counters.archivable == result["archived_rows"] == 2
        assert counters.total == 3
