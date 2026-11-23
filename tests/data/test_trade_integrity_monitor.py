"""Mutation tests for incremental trade-integrity monitoring."""

import sqlite3
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Literal
from typing import Protocol
from unittest.mock import AsyncMock

import pytest
from asyncpg.exceptions import QueryCanceledError
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from snapper.application.data_quality.trade_integrity import TradeIntegrityMonitorConfig
from snapper.application.data_quality.trade_integrity import TradeIntegrityWatchdog
from snapper.data import trade_integrity as integrity_module
from snapper.data.models import Trade
from snapper.data.models import TradeIntegrityMonitorCursor
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.trade_integrity import TradeIntegrityPassRequest

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
    expected_executed_at: datetime | None
    conflicting_executed_at: datetime | None


class _RunResult(Protocol):
    """Monitor-result fields asserted by the mutation suite."""

    findings: tuple[_Finding, ...]
    sweep_rows: int
    worklog_rows: int
    cursor_timestamp: datetime
    cursor_id: int
    covered_through: datetime
    lag_seconds: int
    lagged: bool
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
    executed_at: datetime | None,
    monitor: _Monitor,
) -> None:
    """Enqueue one exact historical identity.

    Args:
        path: SQLite database path.
        public_id: Logical row identity.
        trade_id: Venue trade identity, or ``None``.
        executed_at: Execution time written by the historical mutator, or
            ``None`` when the obligation carries no execution time.
        monitor: Monitor that must inspect the identity.
    """
    stored_executed_at = (
        None
        if executed_at is None
        else executed_at.replace(tzinfo=None).isoformat(sep=" ", timespec="microseconds")
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
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            public_id,
            _INSTRUMENT,
            trade_id,
            stored_executed_at,
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
@pytest.mark.parametrize("monitor", ["m1", "m2"])
@pytest.mark.parametrize(
    "failure",
    [
        TimeoutError(),
        DBAPIError("SELECT", None, QueryCanceledError("statement timeout")),
    ],
)
async def test_timeout_rolls_back_obligations_then_retry_commits_coverage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    monitor: _Monitor,
    failure: Exception,
) -> None:
    """Recover durable coverage and worklog progress after a cancelled transaction.

    Given: A saved cursor, two remaining trades, and a pending historical obligation.
    When: The pass fails after tentative acknowledgments and cursor mutations.
    Then: Rollback preserves durable state and a smaller retry commits full coverage.
    """
    path = tmp_path / "timeout-retry.db"
    _create_constraint_free_monitor_db(path)
    for index in range(3):
        _insert_trade(
            path,
            public_id=f"36000000-0000-0000-0000-{index + 1:012d}",
            trade_id=f"retry-{index}",
            executed_at=_NOW - timedelta(minutes=20 - index),
            timestamp=_NOW - timedelta(minutes=10 - index),
        )
    repository, initial_result = await _run_monitor(path, monitor, sweep_limit=1)
    _enqueue_work_item(
        path,
        public_id="36000000-0000-0000-0000-000000000003",
        trade_id="retry-2",
        executed_at=_NOW - timedelta(minutes=18),
        monitor=monitor,
    )
    operation = AsyncMock(wraps=repository.run_trade_integrity_monitor)
    monkeypatch.setattr(repository, "run_trade_integrity_monitor", operation)
    watchdog = TradeIntegrityWatchdog(
        repo=repository,
        config=TradeIntegrityMonitorConfig(sweep_limit=1001, worklog_limit=201),
    )
    advance_cursor = integrity_module._advance_cursor

    def fail_before_commit(
        state: TradeIntegrityMonitorCursor,
        sweep: list[Trade],
        request: TradeIntegrityPassRequest,
    ) -> bool:
        """Raise after every tentative cursor and worklog change is applied."""
        advance_cursor(state, sweep, request)
        raise failure

    with monkeypatch.context() as cancellation:
        cancellation.setattr(integrity_module, "_advance_cursor", fail_before_commit)
        assert await watchdog._run_monitor(monitor, _NOW) is None

    async with repository.session() as session:
        saved = await session.get(TradeIntegrityMonitorCursor, monitor)
        assert saved is not None
        assert (saved.scan_cursor_timestamp, saved.scan_cursor_id, saved.covered_through) == (
            initial_result.cursor_timestamp,
            initial_result.cursor_id,
            initial_result.covered_through,
        )
        assert (
            await session.execute(text("SELECT count(*) FROM trade_integrity_worklog"))
        ).scalar_one() == 1

    result = await watchdog._run_monitor(monitor, _NOW)

    assert result is not None
    assert result.sweep_rows == 2
    assert result.worklog_rows == 1
    assert result.covered_through > initial_result.covered_through
    assert operation.await_args is not None
    assert operation.await_args.kwargs["sweep_limit"] == 500
    assert operation.await_args.kwargs["worklog_limit"] == 100
    async with repository.session() as session:
        saved = await session.get(TradeIntegrityMonitorCursor, monitor)
        assert saved is not None
        assert (saved.scan_cursor_timestamp, saved.scan_cursor_id, saved.covered_through) == (
            result.cursor_timestamp,
            result.cursor_id,
            result.covered_through,
        )
        assert (
            await session.execute(text("SELECT count(*) FROM trade_integrity_worklog"))
        ).scalar_one() == 0
    await repository.engine.dispose()


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


_SWEPT_BUS_TIME = _NOW - timedelta(minutes=10)
_UNSWEPT_BUS_TIME = _NOW - timedelta(days=30)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("offset", "found"),
    [
        (timedelta(days=6), True),
        (timedelta(days=7), True),
        (-timedelta(days=7), True),
        (timedelta(days=7, microseconds=1), False),
        (-timedelta(days=7, microseconds=1), False),
    ],
)
async def test_m1_windowed_probe_reads_only_inside_the_window(
    tmp_path: Path,
    offset: timedelta,
    found: bool,
) -> None:
    """Find a swept venue identity's conflict only within seven days of its time.

    Given: A swept trade and an unswept row of the same venue identity whose
        execution time differs by the parametrized offset.
    When: M1 runs one pass.
    Then: The conflict is a finding exactly when the offset is at most seven
        days, with both execution times reported.
    """
    path = tmp_path / "m1-window.db"
    _create_constraint_free_monitor_db(path)
    executed_at = _NOW - timedelta(minutes=20)
    _insert_trade(
        path,
        public_id="37000000-0000-0000-0000-000000000001",
        trade_id="window-1",
        executed_at=executed_at,
        timestamp=_SWEPT_BUS_TIME,
    )
    _insert_trade(
        path,
        public_id="37000000-0000-0000-0000-000000000002",
        trade_id="window-1",
        executed_at=executed_at + offset,
        timestamp=_UNSWEPT_BUS_TIME,
    )

    repository, result = await _run_monitor(path, "m1")

    expected = [("window-1", executed_at, executed_at + offset)] if found else []
    assert [
        (finding.trade_id, finding.expected_executed_at, finding.conflicting_executed_at)
        for finding in result.findings
    ] == expected
    assert result.sweep_rows == 1
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_m1_owed_identity_is_probed_exactly_with_worklog_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Probe an identity that is both swept and owed only exactly, across chunks.

    Given: Single-candidate chunks, an unswept row of one venue identity eight
        days before its swept row, a third unswept row one day after the swept
        row, and a worklog obligation for the identity at the earliest time.
    When: M1 runs one pass.
    Then: One finding reports the obligation's time as expected and the swept
        row's time as conflicting, although those rows are beyond the window;
        the swept candidate is not probed separately, so its in-window conflict
        does not replace the exact finding.
    """
    monkeypatch.setattr(integrity_module, "_CANDIDATE_CHUNK_SIZE", 1)
    path = tmp_path / "m1-exact-precedence.db"
    _create_constraint_free_monitor_db(path)
    executed_at = _NOW - timedelta(minutes=20)
    historical = executed_at - timedelta(days=8)
    _insert_trade(
        path,
        public_id="38000000-0000-0000-0000-000000000001",
        trade_id="owed-and-swept",
        executed_at=historical,
        timestamp=_UNSWEPT_BUS_TIME,
    )
    _insert_trade(
        path,
        public_id="38000000-0000-0000-0000-000000000002",
        trade_id="owed-and-swept",
        executed_at=executed_at,
        timestamp=_SWEPT_BUS_TIME,
    )
    _insert_trade(
        path,
        public_id="38000000-0000-0000-0000-000000000003",
        trade_id="owed-and-swept",
        executed_at=executed_at + timedelta(days=1),
        timestamp=_UNSWEPT_BUS_TIME + timedelta(microseconds=1),
    )
    _enqueue_work_item(
        path,
        public_id="38000000-0000-0000-0000-000000000001",
        trade_id="owed-and-swept",
        executed_at=historical,
        monitor="m1",
    )

    repository, result = await _run_monitor(path, "m1")

    assert [
        (finding.trade_id, finding.expected_executed_at, finding.conflicting_executed_at)
        for finding in result.findings
    ] == [("owed-and-swept", historical, executed_at)]
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_m1_obligation_without_execution_time_finds_stored_identity(
    tmp_path: Path,
) -> None:
    """Report any stored row of an owed identity whose obligation has no time.

    Given: An unswept trade and an M1 worklog obligation for its venue identity
        that carries no execution time.
    When: M1 runs one pass.
    Then: The stored row is a finding without an expected execution time, as
        ``IS DISTINCT FROM`` reports it on PostgreSQL.
    """
    path = tmp_path / "m1-null-obligation.db"
    _create_constraint_free_monitor_db(path)
    stored = _NOW - timedelta(days=12)
    _insert_trade(
        path,
        public_id="39000000-0000-0000-0000-000000000001",
        trade_id="owed-without-time",
        executed_at=stored,
        timestamp=_UNSWEPT_BUS_TIME,
    )
    _enqueue_work_item(
        path,
        public_id="39000000-0000-0000-0000-000000000001",
        trade_id="owed-without-time",
        executed_at=None,
        monitor="m1",
    )

    repository, result = await _run_monitor(path, "m1")

    assert [
        (finding.trade_id, finding.expected_executed_at, finding.conflicting_executed_at)
        for finding in result.findings
    ] == [("owed-without-time", None, stored)]
    assert result.sweep_rows == 0
    await repository.engine.dispose()


def _stage_m2_rows_around_closed_candidate(path: Path, later_offset: timedelta) -> str:
    """Stage one closed swept row and two unswept active rows of its public id.

    Args:
        path: SQLite database path.
        later_offset: Distance of the second active row after the swept row.

    Returns:
        The shared public id.
    """
    public_id = "3a000000-0000-0000-0000-000000000001"
    candidate_time = _NOW - timedelta(minutes=20)
    _insert_trade(
        path,
        public_id=public_id,
        trade_id="m2-window-closed",
        executed_at=candidate_time,
        timestamp=_SWEPT_BUS_TIME,
    )
    _close_trade(path, public_id, _NOW - timedelta(minutes=5))
    _insert_trade(
        path,
        public_id=public_id,
        trade_id="m2-window-early",
        executed_at=candidate_time - timedelta(days=6),
        timestamp=_UNSWEPT_BUS_TIME,
    )
    _insert_trade(
        path,
        public_id=public_id,
        trade_id="m2-window-late",
        executed_at=candidate_time + later_offset,
        timestamp=_UNSWEPT_BUS_TIME + timedelta(microseconds=1),
    )
    return public_id


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("later_offset", "found"),
    [(timedelta(days=6), True), (timedelta(days=8), False)],
)
async def test_m2_windowed_probe_counts_active_rows_around_its_candidate(
    tmp_path: Path,
    later_offset: timedelta,
    found: bool,
) -> None:
    """Count only active rows within seven days of the swept candidate's time.

    Given: A closed swept row at time c and two unswept active rows of its
        public id, six days before c and the parametrized offset after c.
    When: M2 runs one pass.
    Then: Both active rows are a finding when they lie inside c's window,
        although they are twelve days apart, and none when the later row
        lies outside it.
    """
    path = tmp_path / "m2-window.db"
    _create_constraint_free_monitor_db(path)
    public_id = _stage_m2_rows_around_closed_candidate(path, later_offset)

    repository, result = await _run_monitor(path, "m2")

    expected = [(public_id, 2)] if found else []
    assert [(finding.public_id, finding.active_count) for finding in result.findings] == expected
    assert result.sweep_rows == 1
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_m2_owed_public_id_is_probed_exactly_beyond_the_window(tmp_path: Path) -> None:
    """Pair the active rows of an owed public id over every partition.

    Given: The closed swept row whose later active row lies eight days after
        it, and an M2 worklog obligation for that public id.
    When: M2 runs one pass.
    Then: The exact probe reports both active rows.
    """
    path = tmp_path / "m2-exact.db"
    _create_constraint_free_monitor_db(path)
    public_id = _stage_m2_rows_around_closed_candidate(path, timedelta(days=8))
    _enqueue_work_item(
        path,
        public_id=public_id,
        trade_id="m2-window-closed",
        executed_at=_NOW - timedelta(minutes=20),
        monitor="m2",
    )

    repository, result = await _run_monitor(path, "m2")

    assert [(finding.public_id, finding.active_count) for finding in result.findings] == [
        (public_id, 2)
    ]
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_exact_and_windowed_chunks_share_one_finding_cap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Stop at one shared cap across the exact and windowed chunks of a page.

    Given: Single-candidate chunks, a cap of three findings, two owed venue
        identities in conflict and two swept identities in conflict inside
        the window.
    When: M1 runs one pass.
    Then: The exact candidates are probed first and the page stops after the
        first windowed finding, three findings in all.
    """
    monkeypatch.setattr(integrity_module, "_CANDIDATE_CHUNK_SIZE", 1)
    monkeypatch.setattr(integrity_module, "_FINDING_LIMIT", 3)
    path = tmp_path / "shared-cap.db"
    _create_constraint_free_monitor_db(path)
    for index, trade_id in enumerate(("owed-1", "owed-2")):
        historical = _NOW - timedelta(days=20, minutes=index)
        owed_public_id = f"3b000000-0000-0000-0000-00000000000{index}"
        _insert_trade(
            path,
            public_id=owed_public_id,
            trade_id=trade_id,
            executed_at=historical,
            timestamp=_UNSWEPT_BUS_TIME + timedelta(microseconds=index),
        )
        _insert_trade(
            path,
            public_id=f"3b000000-0000-0000-0000-00000000001{index}",
            trade_id=trade_id,
            executed_at=historical - timedelta(days=9),
            timestamp=_UNSWEPT_BUS_TIME + timedelta(microseconds=10 + index),
        )
        _enqueue_work_item(
            path,
            public_id=owed_public_id,
            trade_id=trade_id,
            executed_at=historical,
            monitor="m1",
        )
    for index, trade_id in enumerate(("swept-1", "swept-2")):
        executed_at = _NOW - timedelta(minutes=20 - index)
        _insert_trade(
            path,
            public_id=f"3b000000-0000-0000-0000-00000000002{index}",
            trade_id=trade_id,
            executed_at=executed_at,
            timestamp=_SWEPT_BUS_TIME + timedelta(microseconds=index),
        )
        _insert_trade(
            path,
            public_id=f"3b000000-0000-0000-0000-00000000003{index}",
            trade_id=trade_id,
            executed_at=executed_at - timedelta(days=1),
            timestamp=_UNSWEPT_BUS_TIME + timedelta(microseconds=20 + index),
        )

    repository, result = await _run_monitor(path, "m1")

    assert [finding.trade_id for finding in result.findings] == ["owed-1", "owed-2", "swept-1"]
    await repository.engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("monitor", ["m1", "m2"])
async def test_full_page_advances_covered_through_to_strict_bound_and_shrinks_lag(
    tmp_path: Path,
    monitor: _Monitor,
) -> None:
    """Advance covered_through per clean full page up to the strict bound.

    Given: Eight clean trades spanning five hours before now, with ties at the
        boundary of each four-row page so rows with the last timestamp remain
        unscanned across page boundaries.
    When: The monitor runs two full four-row pages followed by pass completion.
    Then: Each full page advances covered_through only to the greatest
        timestamp strictly below its last row, shrinking lag_seconds and
        clearing lagged once within three hours, and completion reaches scan_end.
    """
    path = tmp_path / f"strict-bound-{monitor}.db"
    _create_constraint_free_monitor_db(path)
    timestamps = (
        _NOW - timedelta(hours=5),
        _NOW - timedelta(hours=4),
        _NOW - timedelta(hours=2),
        _NOW - timedelta(hours=2),
        _NOW - timedelta(hours=2),
        _NOW - timedelta(hours=1),
        _NOW - timedelta(minutes=30),
        _NOW - timedelta(minutes=30),
    )
    for index, timestamp in enumerate(timestamps):
        _insert_trade(
            path,
            public_id=f"3c000000-0000-0000-0000-{index + 1:012d}",
            trade_id=f"strict-{index}",
            executed_at=timestamp - timedelta(minutes=1),
            timestamp=timestamp,
        )

    repository, first_result = await _run_monitor(path, monitor, sweep_limit=4)
    second_result = await repository.run_trade_integrity_monitor(
        monitor=monitor,
        now=_NOW,
        sweep_limit=4,
        worklog_limit=100,
    )
    final_result = await repository.run_trade_integrity_monitor(
        monitor=monitor,
        now=_NOW,
        sweep_limit=4,
        worklog_limit=100,
    )

    assert first_result.sweep_rows == 4
    assert not first_result.pass_completed
    assert first_result.covered_through == _NOW - timedelta(hours=4)
    assert first_result.lag_seconds == int(timedelta(hours=3, minutes=55).total_seconds())
    assert first_result.lagged

    assert second_result.sweep_rows == 4
    assert not second_result.pass_completed
    assert second_result.covered_through == _NOW - timedelta(hours=1)
    assert second_result.lag_seconds == int(timedelta(minutes=55).total_seconds())
    assert not second_result.lagged

    assert final_result.sweep_rows == 0
    assert final_result.pass_completed
    assert final_result.covered_through == _NOW - timedelta(minutes=5)
    assert final_result.lag_seconds == 0
    assert not final_result.lagged
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_single_timestamp_page_does_not_advance_covered_through(tmp_path: Path) -> None:
    """Keep covered_through unchanged when all rows in a full page share one timestamp.

    Given: A full initial page whose rows share one timestamp, followed by a
        full page with mixed timestamps, followed by another single-timestamp
        full page.
    When: M1 advances across those pages with sweep_limit equal to two.
    Then: Both single-timestamp pages advance the durable cursor without moving
        covered_through, while the mixed-timestamp page advances it.
    """
    path = tmp_path / "same-timestamp-page.db"
    _create_constraint_free_monitor_db(path)
    t_initial = _NOW - timedelta(hours=5)
    t_later = _NOW - timedelta(hours=3)
    t_flat = _NOW - timedelta(hours=1)
    for index, timestamp in enumerate((t_initial, t_initial, t_initial, t_later, t_flat, t_flat)):
        _insert_trade(
            path,
            public_id=f"3d000000-0000-0000-0000-{index + 1:012d}",
            trade_id=f"flat-{index}",
            executed_at=timestamp - timedelta(minutes=1),
            timestamp=timestamp,
        )

    repository, first_result = await _run_monitor(path, "m1", sweep_limit=2)
    second_result = await repository.run_trade_integrity_monitor(
        monitor="m1",
        now=_NOW,
        sweep_limit=2,
        worklog_limit=100,
    )
    third_result = await repository.run_trade_integrity_monitor(
        monitor="m1",
        now=_NOW,
        sweep_limit=2,
        worklog_limit=100,
    )

    assert not first_result.pass_completed
    assert first_result.covered_through == _NOW - timedelta(hours=6, minutes=5)
    assert first_result.cursor_timestamp == t_initial
    assert first_result.cursor_id == 2

    assert not second_result.pass_completed
    assert second_result.covered_through == t_initial

    assert not third_result.pass_completed
    assert third_result.covered_through == t_initial
    assert (third_result.cursor_timestamp, third_result.cursor_id) == (t_flat, 6)
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_covered_through_remains_monotone_across_overlap_restart_and_completion(
    tmp_path: Path,
) -> None:
    """Never lower covered_through when a pass restarts inside the six-hour overlap.

    Given: Clean trades inside the initial overlap window and newer trades
        settling before the following pass starts.
    When: Pass one completes at scan_end, pass two pages through the overlap
        and sets a newer scan_end on completion, and pass three pages from
        the restarted overlap to the newer scan_end.
    Then: Covered_through stays at pass one's scan_end throughout every page
        inside the overlap and advances monotonically past it only once a page
        strict bound or completion exceeds it.
    """
    path = tmp_path / "monotone-restart.db"
    _create_constraint_free_monitor_db(path)
    overlap_times = (
        _NOW - timedelta(hours=4),
        _NOW - timedelta(hours=3),
        _NOW - timedelta(hours=2),
    )
    for index, timestamp in enumerate(overlap_times):
        _insert_trade(
            path,
            public_id=f"3e000000-0000-0000-0000-{index + 1:012d}",
            trade_id=f"overlap-{index}",
            executed_at=timestamp - timedelta(minutes=1),
            timestamp=timestamp,
        )

    repository, pass_one = await _run_monitor(path, "m1", sweep_limit=10)
    first_scan_end = _NOW - timedelta(minutes=5)
    assert pass_one.pass_completed
    assert pass_one.covered_through == first_scan_end

    later_now = _NOW + timedelta(minutes=3)
    second_scan_end = later_now - timedelta(minutes=5)
    for index, offset_minutes in enumerate((2, 1, 0), start=4):
        timestamp = second_scan_end - timedelta(minutes=offset_minutes)
        _insert_trade(
            path,
            public_id=f"3e000000-0000-0000-0000-{index:012d}",
            trade_id=f"new-{index}",
            executed_at=timestamp - timedelta(minutes=1),
            timestamp=timestamp,
        )

    pass_two_pages = [
        await repository.run_trade_integrity_monitor(
            monitor="m1",
            now=later_now,
            sweep_limit=2,
            worklog_limit=100,
        )
        for _ in range(2)
    ]
    pass_three_pages = [
        await repository.run_trade_integrity_monitor(
            monitor="m1",
            now=later_now,
            sweep_limit=2,
            worklog_limit=100,
        )
        for _ in range(4)
    ]

    assert [page.pass_completed for page in pass_two_pages] == [False, True]
    assert [page.covered_through for page in pass_two_pages] == [
        first_scan_end,
        first_scan_end,
    ]
    assert [page.pass_completed for page in pass_three_pages] == [False, False, False, True]
    assert [page.covered_through for page in pass_three_pages] == [
        first_scan_end,
        first_scan_end,
        second_scan_end - timedelta(minutes=1),
        second_scan_end,
    ]
    await repository.engine.dispose()


@pytest.mark.asyncio
async def test_findings_do_not_advance_covered_through(tmp_path: Path) -> None:
    """Keep covered_through pinned when a sweep page detects a finding.

    Given: Two clean trades on page one and a violating trade pair on page two
        with distinct timestamps inside the page.
    When: Page one commits cleanly and page two encounters the violation.
    Then: Page one advances covered_through to its strict bound, while page
        two holds both the cursor and covered_through unchanged.
    """
    path = tmp_path / "finding-freezes-coverage.db"
    _create_constraint_free_monitor_db(path)
    _insert_trade(
        path,
        public_id="3f000000-0000-0000-0000-000000000001",
        trade_id="clean-1",
        executed_at=_NOW - timedelta(hours=5, minutes=1),
        timestamp=_NOW - timedelta(hours=5),
    )
    _insert_trade(
        path,
        public_id="3f000000-0000-0000-0000-000000000002",
        trade_id="clean-2",
        executed_at=_NOW - timedelta(hours=4, minutes=1),
        timestamp=_NOW - timedelta(hours=4),
    )
    _insert_trade(
        path,
        public_id="3f000000-0000-0000-0000-000000000003",
        trade_id="conflict-1",
        executed_at=_NOW - timedelta(hours=3, minutes=1),
        timestamp=_NOW - timedelta(hours=3),
    )
    _insert_trade(
        path,
        public_id="3f000000-0000-0000-0000-000000000004",
        trade_id="conflict-1",
        executed_at=_NOW - timedelta(hours=3, minutes=2),
        timestamp=_NOW - timedelta(hours=2),
    )

    repository, clean_page = await _run_monitor(path, "m1", sweep_limit=2)
    violating_page = await repository.run_trade_integrity_monitor(
        monitor="m1",
        now=_NOW,
        sweep_limit=2,
        worklog_limit=100,
    )

    assert clean_page.findings == ()
    assert clean_page.covered_through == _NOW - timedelta(hours=5)
    assert len(violating_page.findings) == 1
    assert not violating_page.pass_completed
    assert violating_page.covered_through == clean_page.covered_through
    assert (violating_page.cursor_timestamp, violating_page.cursor_id) == (
        clean_page.cursor_timestamp,
        clean_page.cursor_id,
    )
    await repository.engine.dispose()
