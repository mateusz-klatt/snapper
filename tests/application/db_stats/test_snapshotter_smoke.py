"""Performance smoke test for the per-table sampler.

Pins a wall-clock budget so future regressions in the COUNT path get
caught early. Seeds 100k telemetry rows + ``ix_telemetry_timestamp``
index from `metadata.create_all()`, then asserts ``_sample_once``
completes within 2s.
"""

import time
from datetime import UTC
from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy import insert

from snapper.application.db_stats.snapshotter import DbStatsSnapshotter
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Telemetry
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import SQLAlchemyRepository


def _bulk_insert_telemetry(
    seed_repo: DatabaseRepository,
    *,
    count: int,
    batch_size: int = 5_000,
) -> None:
    """Insert ``count`` telemetry rows via Core bulk insert (fast path)."""
    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    rows = [
        {
            "public_id": f"00000000-0000-7000-8000-{i:012d}",
            "session_id": "perf",
            "sequence_id": i,
            "timestamp": now,
            "known_to": KNOWN_TO_MAX,
            "transport": "ws",
            "direction": "in",
            "message_type": "ping",
            "payload": None,
        }
        for i in range(count)
    ]
    with seed_repo.get_session() as session:
        for start in range(0, count, batch_size):
            chunk = rows[start : start + batch_size]
            session.execute(insert(Telemetry), chunk)
        session.commit()


class TestSnapshotterPerformance:
    """Wall-clock budget for one full sampler tick."""

    @pytest.mark.timeout(60)
    @pytest.mark.asyncio
    async def test_sample_once_under_2s_with_100k_telemetry_rows(self, tmp_path: Path) -> None:
        """100k telemetry rows + ix_telemetry_timestamp → ``_sample_once`` < 2s."""
        db_path = tmp_path / "perf.db"
        db_url = f"sqlite+aiosqlite:///{db_path}"
        seed_repo = DatabaseRepository(db_url)
        seed_repo.create_all()
        _bulk_insert_telemetry(seed_repo, count=100_000)
        seed_repo.dispose()

        async_repo = SQLAlchemyRepository(db_url)
        try:
            snapshotter = DbStatsSnapshotter(
                repo=async_repo,
                interval_seconds=60,
                disabled=False,
            )
            start = time.monotonic()
            snapshot = await snapshotter._sample_once()
            elapsed = time.monotonic() - start
        finally:
            await async_repo.engine.dispose()
        telemetry_row = snapshot.find("telemetry")
        assert telemetry_row is not None
        assert telemetry_row.total == 100_000
        assert elapsed < 2.0, (
            f"sample_once took {elapsed:.2f}s on 100k telemetry rows "
            f"(budget 2.0s — investigate ix_telemetry_timestamp planner choice)"
        )
