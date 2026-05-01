"""Migration 0002 acceptance: ``ix_telemetry_timestamp`` planner usage (plan §7.8).

Verifies that after ``metadata.create_all()`` (which mirrors the
0002 migration via ``Telemetry.__table_args__``), SQLite's
``EXPLAIN QUERY PLAN`` references the new index for the half-open
timestamp window query Cluster B/C run on ``telemetry``.

PostgreSQL planner verification is out of scope for the in-tree test
suite (no PG fixture); this test pins the SQLite path so dev/test
deployments fail loud on any future regression.
"""

from datetime import UTC
from datetime import datetime
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy import text

from snapper.data.models import Base


class TestTelemetryTimestampIndex:
    """Plan §7.8 — SQLite planner uses ``ix_telemetry_timestamp``."""

    def test_metadata_creates_ix_telemetry_timestamp(self, tmp_path: Path) -> None:
        """``metadata.create_all()`` registers the new index alongside legacy ones."""
        db_path = tmp_path / "schema.db"
        engine = create_engine(f"sqlite:///{db_path}")
        try:
            Base.metadata.create_all(engine)
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT name FROM sqlite_master "
                        "WHERE type='index' AND tbl_name='telemetry'"
                    )
                ).all()
        finally:
            engine.dispose()
        index_names = {row.name for row in rows}
        assert "ix_telemetry_timestamp" in index_names
        assert "ix_telemetry_public_id" in index_names

    def test_window_query_plan_uses_ix_telemetry_timestamp(self, tmp_path: Path) -> None:
        """SQLite ``EXPLAIN QUERY PLAN`` mentions the new index for the window predicate."""
        db_path = tmp_path / "explain.db"
        engine = create_engine(f"sqlite:///{db_path}")
        try:
            Base.metadata.create_all(engine)
            window_start = datetime(2026, 4, 15, 0, 0, tzinfo=UTC).isoformat()
            window_end = datetime(2026, 4, 30, 0, 0, tzinfo=UTC).isoformat()
            with engine.connect() as conn:
                conn.execute(text("ANALYZE"))
                plan_rows = conn.execute(
                    text(
                        "EXPLAIN QUERY PLAN "
                        "SELECT COUNT(*) FROM telemetry "
                        "WHERE timestamp >= :window_start AND timestamp < :window_end"
                    ),
                    {"window_start": window_start, "window_end": window_end},
                ).all()
        finally:
            engine.dispose()
        plan_text = " ".join(str(row) for row in plan_rows)
        assert (
            "ix_telemetry_timestamp" in plan_text
        ), f"Expected SQLite planner to use ix_telemetry_timestamp; got plan:\n{plan_text}"
