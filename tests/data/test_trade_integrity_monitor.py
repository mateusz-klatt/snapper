"""Mutation tests for incremental trade-integrity monitoring."""

import sqlite3
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Literal
from typing import Protocol

import pytest
from sqlalchemy import text

from snapper.data.repository import SQLAlchemyRepository

_NOW = datetime(2026, 7, 29, 12, 0, tzinfo=UTC)
_ACTIVE = "9999-12-31 23:59:59.000000"
_INSTRUMENT = "10000000-0000-0000-0000-000000000001"

type _Monitor = Literal["m1", "m2"]


class _Finding(Protocol):
    """Finding fields asserted by the mutation suite."""

    instrument_public_id: str | None
    trade_id: str | None
    public_id: str | None
    active_count: int | None


class _RunResult(Protocol):
    """Monitor-result fields asserted by the mutation suite."""

    findings: tuple[_Finding, ...]
    sweep_rows: int
    worklog_rows: int
    cursor_timestamp: datetime
    cursor_id: int
    covered_through: datetime
    pass_completed: bool


def _create_constraint_free_monitor_db(path: Path) -> None:
    """Create the monitor schema without today's uniqueness constraints.

    Args:
        path: SQLite database path to create.
    """
    connection = sqlite3.connect(path)
    connection.executescript("""
        CREATE TABLE trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            public_id VARCHAR(36) NOT NULL,
            session_id VARCHAR(36) NOT NULL,
            sequence_id INTEGER NOT NULL,
            timestamp DATETIME NOT NULL,
            known_to DATETIME NOT NULL,
            instrument_public_id VARCHAR(36) NOT NULL,
            price FLOAT NOT NULL,
            size FLOAT NOT NULL,
            side VARCHAR(4) NOT NULL,
            trade_id VARCHAR(64),
            executed_at DATETIME
        );
        CREATE INDEX ix_trades_timestamp ON trades (timestamp);
        CREATE INDEX uq_trade_instrument_trade_id
            ON trades (instrument_public_id, trade_id);
        CREATE INDEX ix_trades_public_id
            ON trades (public_id)
            WHERE known_to = '9999-12-31 23:59:59.000000';
        CREATE TABLE trade_integrity_worklog (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            public_id VARCHAR(36) NOT NULL,
            instrument_public_id VARCHAR(36) NOT NULL,
            trade_id VARCHAR(64),
            executed_at DATETIME,
            enqueued_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            m1_pending BOOLEAN NOT NULL DEFAULT 1,
            m2_pending BOOLEAN NOT NULL DEFAULT 1
        );
        CREATE INDEX ix_trade_integrity_worklog_m1_pending
            ON trade_integrity_worklog (id)
            WHERE m1_pending = 1;
        CREATE INDEX ix_trade_integrity_worklog_m2_pending
            ON trade_integrity_worklog (id)
            WHERE m2_pending = 1;
        CREATE TABLE trade_integrity_monitor_cursors (
            monitor VARCHAR(2) PRIMARY KEY,
            covered_through DATETIME NOT NULL,
            scan_cursor_timestamp DATETIME NOT NULL,
            scan_cursor_id BIGINT NOT NULL,
            scan_end DATETIME NOT NULL,
            updated_at DATETIME NOT NULL
        );
        """)
    connection.commit()
    connection.close()


def _insert_trade(
    path: Path,
    *,
    public_id: str,
    trade_id: str | None,
    executed_at: datetime,
    timestamp: datetime,
) -> None:
    """Insert one unconstrained trade mutation row.

    Args:
        path: SQLite database path.
        public_id: Logical row identity.
        trade_id: Venue trade identity, or ``None`` when absent.
        executed_at: Venue execution time.
        timestamp: Bus time used by the sweep.
    """
    connection = sqlite3.connect(path)
    connection.execute(
        """
        INSERT INTO trades (
            public_id,
            session_id,
            sequence_id,
            timestamp,
            known_to,
            instrument_public_id,
            price,
            size,
            side,
            trade_id,
            executed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            public_id,
            "20000000-0000-0000-0000-000000000001",
            1,
            timestamp.replace(tzinfo=None).isoformat(sep=" ", timespec="microseconds"),
            _ACTIVE,
            _INSTRUMENT,
            100.0,
            1.0,
            "buy",
            trade_id,
            executed_at.replace(tzinfo=None).isoformat(sep=" ", timespec="microseconds"),
        ),
    )
    connection.commit()
    connection.close()


def _close_trade(path: Path, public_id: str, known_to: datetime) -> None:
    """Close one staged trade version.

    Args:
        path: SQLite database path.
        public_id: Logical identity to close.
        known_to: Replacement SCD2 upper bound.
    """
    connection = sqlite3.connect(path)
    connection.execute(
        "UPDATE trades SET known_to = ? WHERE public_id = ?",
        (
            known_to.replace(tzinfo=None).isoformat(sep=" ", timespec="microseconds"),
            public_id,
        ),
    )
    connection.commit()
    connection.close()


def _enqueue_work_item(
    path: Path,
    *,
    public_id: str,
    trade_id: str | None,
    executed_at: datetime,
    monitor: _Monitor,
) -> None:
    """Enqueue one exact historical identity.

    Args:
        path: SQLite database path.
        public_id: Logical row identity.
        trade_id: Venue trade identity, or ``None``.
        executed_at: Execution time written by the historical mutator.
        monitor: Monitor that must inspect the identity.
    """
    connection = sqlite3.connect(path)
    connection.execute(
        """
        INSERT INTO trade_integrity_worklog (
            public_id,
            instrument_public_id,
            trade_id,
            executed_at,
            m1_pending,
            m2_pending
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            public_id,
            _INSTRUMENT,
            trade_id,
            executed_at.replace(tzinfo=None).isoformat(
                sep=" ",
                timespec="microseconds",
            ),
            monitor == "m1",
            monitor == "m2",
        ),
    )
    connection.commit()
    connection.close()


async def _run_monitor(
    path: Path,
    monitor: _Monitor,
    *,
    sweep_limit: int = 100,
    worklog_limit: int = 100,
) -> tuple[SQLAlchemyRepository, _RunResult]:
    """Run one monitor pass against the constraint-free database.

    Args:
        path: SQLite database path.
        monitor: Monitor key, ``m1`` or ``m2``.
        sweep_limit: Maximum sweep rows for the pass.
        worklog_limit: Maximum outstanding obligations for the pass.

    Returns:
        Repository and monitor result.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{path}")
    result = await repository.run_trade_integrity_monitor(
        monitor=monitor,
        now=_NOW,
        sweep_limit=sweep_limit,
        worklog_limit=worklog_limit,
    )
    return repository, result


@pytest.mark.asyncio
async def test_m1_detects_differing_execution_times(tmp_path: Path) -> None:
    """M1 detects one venue identity carrying two execution times."""
    path = tmp_path / "m1-mutation.db"
    _create_constraint_free_monitor_db(path)
    bus_time = _NOW - timedelta(minutes=10)
    _insert_trade(
        path,
        public_id="30000000-0000-0000-0000-000000000001",
        trade_id="venue-trade-1",
        executed_at=_NOW - timedelta(minutes=20),
        timestamp=bus_time,
    )
    _insert_trade(
        path,
        public_id="30000000-0000-0000-0000-000000000002",
        trade_id="venue-trade-1",
        executed_at=_NOW - timedelta(minutes=19),
        timestamp=bus_time + timedelta(microseconds=1),
    )

    repository, result = await _run_monitor(path, "m1")

    assert len(result.findings) == 1
    assert result.findings[0].instrument_public_id == _INSTRUMENT
    assert result.findings[0].trade_id == "venue-trade-1"
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_m1_does_not_fire_on_clean_or_unidentified_rows(tmp_path: Path) -> None:
    """M1 stays quiet for distinct identities and nullable trade IDs."""
    path = tmp_path / "m1-clean.db"
    _create_constraint_free_monitor_db(path)
    bus_time = _NOW - timedelta(minutes=10)
    _insert_trade(
        path,
        public_id="31000000-0000-0000-0000-000000000001",
        trade_id="venue-clean-1",
        executed_at=_NOW - timedelta(minutes=20),
        timestamp=bus_time,
    )
    _insert_trade(
        path,
        public_id="31000000-0000-0000-0000-000000000002",
        trade_id="venue-clean-2",
        executed_at=_NOW - timedelta(minutes=19),
        timestamp=bus_time + timedelta(microseconds=1),
    )
    _insert_trade(
        path,
        public_id="31000000-0000-0000-0000-000000000003",
        trade_id=None,
        executed_at=_NOW - timedelta(minutes=18),
        timestamp=bus_time + timedelta(microseconds=2),
    )
    _insert_trade(
        path,
        public_id="31000000-0000-0000-0000-000000000004",
        trade_id=None,
        executed_at=_NOW - timedelta(minutes=17),
        timestamp=bus_time + timedelta(microseconds=3),
    )
    _insert_trade(
        path,
        public_id="31000000-0000-0000-0000-000000000005",
        trade_id="venue-clean-same-time",
        executed_at=_NOW - timedelta(minutes=16),
        timestamp=bus_time + timedelta(microseconds=4),
    )
    _insert_trade(
        path,
        public_id="31000000-0000-0000-0000-000000000006",
        trade_id="venue-clean-same-time",
        executed_at=_NOW - timedelta(minutes=16),
        timestamp=bus_time + timedelta(microseconds=5),
    )

    repository, result = await _run_monitor(path, "m1")

    assert result.findings == ()
    with pytest.raises(ValueError, match="sweep limit"):
        await repository.run_trade_integrity_monitor(
            monitor="m1",
            now=_NOW,
            sweep_limit=25_001,
            worklog_limit=100,
        )
    with pytest.raises(ValueError, match="worklog limit"):
        await repository.run_trade_integrity_monitor(
            monitor="m1",
            now=_NOW,
            sweep_limit=100,
            worklog_limit=2_001,
        )
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_m2_detects_duplicate_active_public_id(tmp_path: Path) -> None:
    """M2 detects two active placements of one public identity."""
    path = tmp_path / "m2-mutation.db"
    _create_constraint_free_monitor_db(path)
    bus_time = _NOW - timedelta(minutes=10)
    public_id = "32000000-0000-0000-0000-000000000001"
    _insert_trade(
        path,
        public_id=public_id,
        trade_id="venue-public-1",
        executed_at=_NOW - timedelta(minutes=20),
        timestamp=bus_time,
    )
    _insert_trade(
        path,
        public_id=public_id,
        trade_id="venue-public-2",
        executed_at=_NOW - timedelta(minutes=19),
        timestamp=bus_time + timedelta(microseconds=1),
    )

    repository, result = await _run_monitor(path, "m2")

    assert len(result.findings) == 1
    assert result.findings[0].public_id == public_id
    assert result.findings[0].active_count == 2
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_m2_does_not_fire_on_clean_or_closed_versions(tmp_path: Path) -> None:
    """M2 stays quiet for distinct active IDs and one closed predecessor."""
    path = tmp_path / "m2-clean.db"
    _create_constraint_free_monitor_db(path)
    bus_time = _NOW - timedelta(minutes=10)
    public_id = "33000000-0000-0000-0000-000000000001"
    _insert_trade(
        path,
        public_id=public_id,
        trade_id="venue-version-1",
        executed_at=_NOW - timedelta(minutes=20),
        timestamp=bus_time,
    )
    _close_trade(path, public_id, _NOW - timedelta(minutes=5))
    _insert_trade(
        path,
        public_id=public_id,
        trade_id="venue-version-2",
        executed_at=_NOW - timedelta(minutes=19),
        timestamp=bus_time + timedelta(microseconds=1),
    )
    _insert_trade(
        path,
        public_id="33000000-0000-0000-0000-000000000002",
        trade_id="venue-version-3",
        executed_at=_NOW - timedelta(minutes=18),
        timestamp=bus_time + timedelta(microseconds=2),
    )

    repository, result = await _run_monitor(path, "m2")

    assert result.findings == ()
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_cursor_survives_restart_and_advances_from_saved_page(tmp_path: Path) -> None:
    """A restarted repository resumes after the last durably scanned row."""
    path = tmp_path / "cursor-restart.db"
    _create_constraint_free_monitor_db(path)
    bus_time = _NOW - timedelta(minutes=10)
    for index in range(3):
        _insert_trade(
            path,
            public_id=f"34000000-0000-0000-0000-{index + 1:012d}",
            trade_id=f"cursor-{index}",
            executed_at=_NOW - timedelta(minutes=20 - index),
            timestamp=bus_time + timedelta(microseconds=index),
        )

    first_repository, first_result = await _run_monitor(path, "m1", sweep_limit=1)
    first_cursor = (first_result.cursor_timestamp, first_result.cursor_id)
    await first_repository.engine.dispose()

    second_repository, second_result = await _run_monitor(path, "m1", sweep_limit=1)
    second_cursor = (second_result.cursor_timestamp, second_result.cursor_id)

    assert second_cursor > first_cursor
    assert first_result.covered_through == _NOW - timedelta(hours=6, minutes=5)
    assert not first_result.pass_completed
    assert second_result.sweep_rows == 1
    await second_repository.engine.dispose()


@pytest.mark.asyncio
async def test_overlap_detects_late_row_behind_completed_cursor(tmp_path: Path) -> None:
    """A late commit inside the fixed overlap remains visible after coverage advances."""
    path = tmp_path / "m1-overlap.db"
    _create_constraint_free_monitor_db(path)
    first_repository, first_result = await _run_monitor(path, "m1")
    await first_repository.engine.dispose()
    late_bus_time = _NOW - timedelta(hours=1)
    _insert_trade(
        path,
        public_id="34400000-0000-0000-0000-000000000001",
        trade_id="late-overlap",
        executed_at=_NOW - timedelta(hours=2),
        timestamp=late_bus_time,
    )
    _insert_trade(
        path,
        public_id="34400000-0000-0000-0000-000000000002",
        trade_id="late-overlap",
        executed_at=_NOW - timedelta(hours=2, minutes=1),
        timestamp=late_bus_time + timedelta(microseconds=1),
    )

    second_repository, second_result = await _run_monitor(path, "m1")

    assert first_result.pass_completed
    assert len(second_result.findings) == 1
    assert second_result.findings[0].trade_id == "late-overlap"
    await second_repository.engine.dispose()


@pytest.mark.asyncio
async def test_m1_restart_drains_unscanned_historical_work(tmp_path: Path) -> None:
    """M1 cannot skip a historical obligation behind its completed cursor."""
    path = tmp_path / "m1-historical-worklog.db"
    _create_constraint_free_monitor_db(path)
    historical_bus_time = _NOW - timedelta(hours=8)
    first_public_id = "34500000-0000-0000-0000-000000000001"
    violating_public_id = "34500000-0000-0000-0000-000000000002"
    _insert_trade(
        path,
        public_id=first_public_id,
        trade_id="historical-clean",
        executed_at=_NOW - timedelta(hours=9),
        timestamp=historical_bus_time,
    )
    _insert_trade(
        path,
        public_id=violating_public_id,
        trade_id="historical-divergence",
        executed_at=_NOW - timedelta(hours=9),
        timestamp=historical_bus_time + timedelta(microseconds=1),
    )
    _insert_trade(
        path,
        public_id="34500000-0000-0000-0000-000000000003",
        trade_id="historical-divergence",
        executed_at=_NOW - timedelta(hours=9, minutes=1),
        timestamp=historical_bus_time + timedelta(microseconds=2),
    )
    _enqueue_work_item(
        path,
        public_id=first_public_id,
        trade_id="historical-clean",
        executed_at=_NOW - timedelta(hours=9),
        monitor="m1",
    )
    _enqueue_work_item(
        path,
        public_id=violating_public_id,
        trade_id="historical-divergence",
        executed_at=_NOW - timedelta(hours=9),
        monitor="m1",
    )

    first_repository, first_result = await _run_monitor(
        path,
        "m1",
        worklog_limit=1,
    )
    await first_repository.engine.dispose()
    second_repository, second_result = await _run_monitor(
        path,
        "m1",
        worklog_limit=1,
    )

    assert first_result.findings == ()
    assert first_result.sweep_rows == 0
    assert first_result.pass_completed
    assert len(second_result.findings) == 1
    assert second_result.findings[0].trade_id == "historical-divergence"
    assert second_result.worklog_rows == 1
    await second_repository.engine.dispose()


@pytest.mark.asyncio
async def test_m2_worklog_detects_historical_active_duplication(tmp_path: Path) -> None:
    """M2 probes worklog identities even when their bus time is behind the sweep."""
    path = tmp_path / "m2-historical-worklog.db"
    _create_constraint_free_monitor_db(path)
    historical_bus_time = _NOW - timedelta(hours=8)
    public_id = "34600000-0000-0000-0000-000000000001"
    _insert_trade(
        path,
        public_id=public_id,
        trade_id="historical-active-1",
        executed_at=_NOW - timedelta(hours=9),
        timestamp=historical_bus_time,
    )
    _insert_trade(
        path,
        public_id=public_id,
        trade_id="historical-active-2",
        executed_at=_NOW - timedelta(hours=9, minutes=1),
        timestamp=historical_bus_time + timedelta(microseconds=1),
    )
    _enqueue_work_item(
        path,
        public_id=public_id,
        trade_id="historical-active-1",
        executed_at=_NOW - timedelta(hours=9),
        monitor="m2",
    )

    repository, result = await _run_monitor(path, "m2")

    assert result.sweep_rows == 0
    assert len(result.findings) == 1
    assert result.findings[0].public_id == public_id
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_finding_keeps_cursor_and_worklog_obligation_pending(tmp_path: Path) -> None:
    """A finding cannot be skipped by cursor advance or worklog acknowledgement."""
    path = tmp_path / "cursor-latch.db"
    _create_constraint_free_monitor_db(path)
    bus_time = _NOW - timedelta(minutes=10)
    _insert_trade(
        path,
        public_id="35000000-0000-0000-0000-000000000001",
        trade_id="latched-identity",
        executed_at=_NOW - timedelta(minutes=20),
        timestamp=bus_time,
    )
    _insert_trade(
        path,
        public_id="35000000-0000-0000-0000-000000000002",
        trade_id="latched-identity",
        executed_at=_NOW - timedelta(minutes=19),
        timestamp=bus_time + timedelta(microseconds=1),
    )
    connection = sqlite3.connect(path)
    connection.execute(
        """
        INSERT INTO trade_integrity_worklog (
            public_id,
            instrument_public_id,
            trade_id,
            executed_at,
            m1_pending,
            m2_pending
        )
        VALUES (?, ?, ?, ?, 1, 0)
        """,
        (
            "35000000-0000-0000-0000-000000000001",
            _INSTRUMENT,
            "latched-identity",
            (_NOW - timedelta(minutes=20)).isoformat(sep=" "),
        ),
    )
    connection.commit()
    connection.close()
    repository, first_result = await _run_monitor(path, "m1")
    first_cursor = (first_result.cursor_timestamp, first_result.cursor_id)

    second_result = await repository.run_trade_integrity_monitor(
        monitor="m1",
        now=_NOW + timedelta(minutes=1),
        sweep_limit=100,
        worklog_limit=100,
    )
    async with repository.session() as session:
        pending = int(
            (
                await session.execute(
                    text("SELECT count(*) FROM trade_integrity_worklog WHERE m1_pending = 1")
                )
            ).scalar_one()
        )

    assert second_result.findings
    second_cursor = (second_result.cursor_timestamp, second_result.cursor_id)
    assert second_cursor == first_cursor
    assert pending == 1
    await repository.engine.dispose()
