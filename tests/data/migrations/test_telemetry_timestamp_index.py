"""Acceptance test for ``ix_telemetry_timestamp``.

Verifies that after ``metadata.create_all()`` (which mirrors the
0001 migration via ``Telemetry.__table_args__``), SQLite's
``EXPLAIN QUERY PLAN`` references the index for the half-open
timestamp window query the retention/archival paths run on
``telemetry``.

PostgreSQL planner verification is out of scope for the in-tree
test suite (no PG fixture); this test pins the SQLite path so
dev/test deployments fail loud on any future regression.

Tests use ``sqlite:///:memory:`` so the schema-creation cost stays
in-process and avoids xdist disk-write contention on WSL2.
"""

from datetime import UTC
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy import text

from snapper.data.models import Base


class TestTelemetryTimestampIndex:
    """SQLite planner uses ``ix_telemetry_timestamp`` for window queries."""

    def test_metadata_creates_ix_telemetry_timestamp(self) -> None:
        """``metadata.create_all()`` registers the index alongside legacy ones."""
        engine = create_engine("sqlite:///:memory:")
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

    def test_window_query_plan_uses_ix_telemetry_timestamp(self) -> None:
        """SQLite ``EXPLAIN QUERY PLAN`` mentions the index for the window predicate."""
        engine = create_engine("sqlite:///:memory:")
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
