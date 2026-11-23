"""Verify read-only guards and bit-exact resume comparison in the operator probe script."""

import asyncio
import os
import resource
import sqlite3
import subprocess
import sys
from collections.abc import Mapping
from contextlib import asynccontextmanager
from contextlib import closing
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import event
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.orm import Session

from scripts import pnl_checkpoint_resume_probe as probe
from snapper.application.portfolio import pnl_snapshotter
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import PnlCheckpointFallback
from snapper.application.portfolio.pnl_snapshotter import PortfolioPnlSnapshotter
from snapper.application.portfolio.pnl_timeline import PnlTimelinePoint
from snapper.application.portfolio.pnl_timeline_service import PnlFxRateSource
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReadPolicy
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayOptions
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.data.pnl_correction_state import PnlCorrectionFrontier
from snapper.data.pnl_correction_state import PnlCorrectionKey
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PNL_SAMPLE_CALC_VERSION
from snapper.data.repository_types import PnlTimelineFillWitnessCut
from tests.application.portfolio import test_pnl_snapshotter_checkpoint_e2e as e2e
from tests.application.portfolio import test_pnl_snapshotter_lower_id_rewind as idless
from tests.data import test_pnl_crypto_usd_plane_repository as prices
from tests.data import test_pnl_opening_checkpoint_repository as checkpoint_repository
from tests.data import test_pnl_timeline_execution_prefix_repository as prefix
from tests.data import test_pnl_timeline_sample_repository as samples

_MINUTE = timedelta(minutes=1)


@pytest.fixture(autouse=True)
def _contain_address_space_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the probe's ``RLIMIT_AS`` from leaking into the pytest worker process.

    ``main()`` applies ``resource.setrlimit(RLIMIT_AS, ...)`` to the current
    process on Linux; without this fixture the cap would persist for the rest
    of an xdist worker's lifetime and crash unrelated tests with MemoryError.
    Tests that verify ``_apply_address_space_limit`` override ``setrlimit`` inside
    the test itself, while RSS checks measure growth over startup baseline and
    require no ``getrusage`` stub.
    """
    monkeypatch.setattr(probe.resource, "setrlimit", lambda _kind, _bounds: None)


class _RecordingCursor:
    """Minimal DBAPI cursor recording executed SQL and returning mapped rows."""

    def __init__(self, row_map: Mapping[str, tuple[object, ...] | None] | None = None) -> None:
        """Initialize an empty SQL command log, close state, and fetch lookup."""
        self.commands: list[str] = []
        self.closed = False
        self._row_map = row_map or {}

    def execute(self, statement: str) -> None:
        """Record one executed SQL statement."""
        self.commands.append(statement)

    def fetchone(self) -> tuple[object, ...] | None:
        """Return mapped row for the most recently executed query."""
        return self._row_map.get(self.commands[-1]) if self.commands else None

    def close(self) -> None:
        """Mark the cursor closed."""
        self.closed = True


class _RecordingConnection:
    """Minimal DBAPI connection yielding a recording cursor."""

    def __init__(self, row_map: Mapping[str, tuple[object, ...] | None] | None = None) -> None:
        """Create one reusable recording cursor."""
        self.cursor_obj = _RecordingCursor(row_map)

    def cursor(self) -> _RecordingCursor:
        """Return the recording cursor."""
        return self.cursor_obj


class _ScalarResult:
    """Minimal SQLAlchemy result stub supporting ``scalar_one`` and ``first``."""

    def __init__(self, value: object) -> None:
        """Store the configured result payload."""
        self._value = value

    def scalar_one(self) -> object:
        """Return the configured scalar value."""
        return self._value

    def first(self) -> object:
        """Return the configured row tuple or ``None``."""
        return self._value


class _PreflightSession:
    """Minimal async context-manager session returning mapped preflight scalars."""

    def __init__(self, responses: Mapping[str, object]) -> None:
        """Store SQL-to-scalar responses emitted by ``session.execute``."""
        self._responses = responses

    async def __aenter__(self) -> _PreflightSession:
        """Enter the async preflight session context."""
        return self

    async def __aexit__(self, *_exc: object) -> None:
        """Exit the async preflight session context."""
        return None

    async def execute(self, statement: object) -> _ScalarResult:
        """Return the configured scalar result matching ``statement``."""
        return _ScalarResult(self._responses.get(str(statement)))


class _DialectProbeRepo(SQLAlchemyRepository):
    """Test-only ``SQLAlchemyRepository`` subclass overriding dialect and session scalars."""

    def __init__(self, dialect_name: str, responses: Mapping[str, object]) -> None:
        """Bind an in-memory SQLite engine while reporting ``dialect_name`` cleanly."""
        super().__init__("sqlite+aiosqlite:///:memory:")
        self._override_dialect = dialect_name
        self._responses = responses

    @property
    def dialect_name(self) -> str:
        """Return the overridden SQLAlchemy dialect name for preflight tests."""
        return self._override_dialect

    @asynccontextmanager
    async def session(self) -> _PreflightSession:
        """Yield a lightweight preflight session returning ``_responses``."""
        yield _PreflightSession(self._responses)


def _sample_key() -> PnlCorrectionKey:
    """Build a canonical correction key matching the real SQLite fixture."""
    return PnlCorrectionKey(
        wallet_public_id=prefix._WALLET,
        mode="live",
        valuation_ccy="USD",
        epoch_public_id="00000000-0000-7000-8000-000000000e01",
        calc_version=PNL_SAMPLE_CALC_VERSION,
    )


def _sample_frontier(
    *, has_fill_gap: bool = False, complete_cut: bool = True
) -> PnlCorrectionFrontier:
    """Build a synthetic frontier covering normal, gap, and incomplete-cut scenarios."""
    tip = samples._T0 + 5 * _MINUTE
    return PnlCorrectionFrontier(
        key=_sample_key(),
        generation=1,
        revision=5,
        window_start=samples._T0 + _MINUTE,
        next_minute=None,
        tip=tip,
        as_of=tip + 2 * _MINUTE,
        baseline_watermarks={},
        pending_watermarks={"kraken": 1},
        prior_peak=0.0,
        repair_from=None,
        raw_execution_watermarks={"kraken": 1} if complete_cut else None,
        execution_manifest_digest="a" * 64 if complete_cut else None,
        has_fill_gap=has_fill_gap,
        expected_venues=frozenset({"kraken"}) if complete_cut else None,
        venue_event_watermarks={"kraken": 1} if complete_cut else None,
        fill_witness_digest="b" * 64 if complete_cut else None,
    )


def _sample_point(point_time: datetime, value: float) -> PnlTimelinePoint:
    """Construct a minimal valid ``PnlTimelinePoint`` for comparison tests."""
    return PnlTimelinePoint(
        point_time=point_time,
        realized_pnl=value,
        fee_pnl=0.0,
        accrual_pnl=0.0,
        unrealized_pnl=0.0,
        net_pnl=value,
        valuation_status="complete",
        incompleteness_reasons=(),
        per_instrument=(),
        attribution=(),
    )


def _sample_result(
    points: tuple[PnlTimelinePoint, ...],
    rate_sources: tuple[PnlFxRateSource, ...] = (),
) -> PnlWalletSeriesResult:
    """Build a synthetic ``PnlWalletSeriesResult`` for unit tests."""
    return PnlWalletSeriesResult(
        points=points,
        granularity="1m",
        valuation_ccy="USD",
        rate_sources=rate_sources,
    )


async def _seed_and_catchup(
    db_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[str, PortfolioPnlSampleScope]:
    """Seed ten minutes of real SQLite history with deferred and suffix fills, then run off catch-up."""
    monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", 2)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "off")
    monkeypatch.delenv("PNL_FX_SHADOW_PINNING_ENABLED", raising=False)
    url = f"sqlite+aiosqlite:///{db_path.as_posix()}"
    scope = await idless._seed(url, complete=False)
    async with idless._repository(url) as repo:
        await e2e._add_valuation_minutes(repo, 6, 10)
        async with repo.session() as session:
            for sequence, minute, side in ((2, 7, "buy"), (3, 9, "sell")):
                session.add_all(e2e._matched_execution(sequence, minute, side))
            await session.commit()
        as_of = samples._T0 + 12 * _MINUTE
        for _ in range(5):
            snap = PortfolioPnlSnapshotter(repo=repo, disabled=False, clock=lambda: as_of)
            await snap._tick_once()
    return url, scope


async def test_real_sqlite_probe_matches_cold_bit_for_bit_with_zero_db_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prove probe computes cold, in-memory capture, and resume bit-identically with no writes.

    Given: A caught-up 10-minute SQLite scope advanced strictly in ``off`` mode with zero
        persisted opening checkpoints.
    When: ``probe_wallet_checkpoint_resume`` runs with explicit DB URL and via bootstrap settings.
    Then: Cold vs resumed points and rate_sources match bit-for-bit, timings are positive,
        and no rows are written or modified in the database.

    Args:
        tmp_path: Pytest isolated directory for the SQLite fixture database.
        monkeypatch: Pytest environment and settings seam.
    """
    db_path = tmp_path / "probe.db"
    url, scope = await _seed_and_catchup(db_path, monkeypatch)
    report = await probe.probe_wallet_checkpoint_resume(
        probe.CheckpointResumeProbeConfig(
            wallet_public_id=scope.wallet_public_id,
            mode="live",
            valuation_ccy="USD",
            window_minutes=4,
            db_url=url,
        )
    )
    assert report.matched is True
    assert report.mismatch_detail is None
    assert report.calc_version == PNL_SAMPLE_CALC_VERSION
    assert report.checkpoint_minute == samples._T0 + 6 * _MINUTE
    assert report.frontier_tip == samples._T0 + 10 * _MINUTE
    assert report.point_count == 4
    assert report.cold_seconds > 0.0
    assert report.capture_seconds > 0.0
    assert report.verify_seconds > 0.0
    assert report.resume_seconds > 0.0

    monkeypatch.setattr(
        probe,
        "get_bootstrap_settings",
        lambda: SimpleNamespace(db_url=url),
    )
    default_report = await probe.probe_wallet_checkpoint_resume(
        probe.CheckpointResumeProbeConfig(
            wallet_public_id=scope.wallet_public_id,
            window_minutes=4,
        )
    )
    assert default_report.matched is True

    async with idless._repository(url) as repo:
        assert await repo.get_portfolio_pnl_opening_checkpoint(scope) is None
        rows = await repo.get_portfolio_pnl_samples(
            e2e._query(scope),
            samples._T0 + _MINUTE,
            samples._T0 + 10 * _MINUTE,
        )
        assert len(rows) == 10


def test_cli_main_and_subprocess_succeed_on_real_sqlite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify operator CLI ``main`` and standalone process execution print timings and exit zero.

    Given: A seeded and caught-up 10-minute SQLite scope on disk.
    When: ``probe.main`` runs in-process and via ``subprocess.run``.
    Then: Both return exit code 0 and print timings plus ``bit_identical=True``.

    Args:
        tmp_path: Pytest isolated directory for the CLI SQLite database.
        monkeypatch: Pytest environment and chunk-size fixture.
        capsys: Stdout/stderr capture verifying operator CLI output.
    """
    db_path = tmp_path / "cli_probe.db"
    url, scope = asyncio.run(_seed_and_catchup(db_path, monkeypatch))
    rc = probe.main(
        [
            "--wallet",
            scope.wallet_public_id,
            "--window-minutes",
            "4",
            "--statement-timeout-ms",
            "15000",
            "--max-runtime-seconds",
            "30",
            "--db-url",
            url,
        ]
    )
    assert rc == 0
    captured = capsys.readouterr()
    assert "bit_identical=True" in captured.out
    assert "cold_seconds=" in captured.out
    assert "resume_seconds=" in captured.out
    assert not any(line.startswith("WARNING:") for line in captured.out.splitlines())
    assert captured.out.strip().splitlines()[-1] == "RESULT: PASS"

    repo_root = Path(probe.__file__).resolve().parents[1]
    env = dict(os.environ)
    env.pop("PNL_PROBE_DB_URL", None)
    env["PYTHONPATH"] = str(repo_root / "src")
    proc = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts/pnl_checkpoint_resume_probe.py"),
            "--wallet",
            scope.wallet_public_id,
            "--window-minutes",
            "4",
            "--db-url",
            url,
        ],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert proc.returncode == 0, proc.stderr
    assert "bit_identical=True" in proc.stdout
    assert proc.stdout.strip().splitlines()[-1] == "RESULT: PASS"


def test_postgres_shaped_statement_allowlist_and_duration_parsing() -> None:
    """Allow production Postgres FX/tx read statements while blocking locking/writes and bad SETs.

    Given: A PostgreSQL-shaped read statement sequence from FX/timeline reads, mutating
        statements, ``SELECT ... FOR UPDATE``, unsafe ``SET``/``PRAGMA``, and PG durations.
    When: Checked by ``_assert_read_only_sql`` and ``_parse_pg_duration_ms``.
    Then: Every legitimate read-only session setting passes, every write or row-locking query
        raises ``CheckpointResumeProbeError``, and timeout durations parse accurately.
    """
    postgres_read_statements = (
        "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "SET TRANSACTION READ ONLY",
        "SET LOCAL jit = off",
        "SET LOCAL jit = on",
        "SET LOCAL statement_timeout = '30000ms'",
        "SET LOCAL statement_timeout = '20s'",
        "SET LOCAL statement_timeout = 20000",
        "SET LOCAL statement_timeout = 15000",
        "SET LOCAL lock_timeout = '1s'",
        "SET LOCAL lock_timeout = 1000",
        "SAVEPOINT sa_savepoint_1",
        "RELEASE SAVEPOINT sa_savepoint_1",
        "ROLLBACK TO SAVEPOINT sa_savepoint_1",
        "PRAGMA busy_timeout = 1000",
        "SELECT id, close FROM candles WHERE label = 'FOR UPDATE; DELETE'",
    )
    for statement in postgres_read_statements:
        probe._assert_read_only_sql(statement)

    for forbidden in (
        "COPY candles TO STDOUT",
        "CREATE TABLE tmp_x (id INT)",
        "ALTER TABLE symbols ADD COLUMN x TEXT",
        "DROP TABLE symbols",
        "SET work_mem = '1GB'",
        "SET LOCAL role = snapper_admin",
        "PRAGMA busy_timeout = 5000",
        "SELECT * FROM orders FOR UPDATE",
        "SELECT * FROM orders FOR SHARE",
        "SELECT set_config('statement_timeout', '0', true)",
        "SELECT set_config ('lock_timeout', '0', true)",
        "SELECT pg_sleep(5)",
        "SELECT pg_advisory_lock(1)",
        "SELECT pg_advisory_unlock(1)",
        "SELECT pg_try_advisory_lock(1)",
        "SELECT pg_try_advisory_xact_lock_shared(1)",
        'SELECT "pg_try_advisory_lock"(1)',
        'SELECT pg_catalog."pg_advisory_lock"(1)',
        "SELECT \"setval\"('candles_id_seq', 1)",
        "SELECT setval('candles_id_seq', 1)",
        "SELECT 1 INTO TEMP probe_mutation",
        "WITH s AS (SELECT pg_sleep(1)) SELECT * FROM s",
        "WITH x AS (SELECT * FROM orders FOR UPDATE) SELECT * FROM x",
        "EXPLAIN ANALYZE SELECT 1",
        "EXPLAIN (ANALYZE, COSTS) SELECT 1",
    ):
        with pytest.raises(probe.CheckpointResumeProbeError, match="write_sql_refused"):
            probe._assert_read_only_sql(forbidden)

    assert probe._parse_pg_duration_ms("0") == 0
    assert probe._parse_pg_duration_ms("500ms") == 500
    assert probe._parse_pg_duration_ms("1s") == 1_000
    assert probe._parse_pg_duration_ms("30s") == 30_000
    assert probe._parse_pg_duration_ms("2min") == 120_000
    assert probe._parse_pg_duration_ms("1h") == 3_600_000
    assert probe._parse_pg_duration_ms("1min 30s") == 90_000
    assert probe._parse_pg_duration_ms("") is None
    assert probe._parse_pg_duration_ms("off") is None


def test_local_timeout_allowlist_enforces_positive_caps() -> None:
    """Enforce bounded ``0 < value <= cap`` parsing on original ``SET LOCAL`` timeout values.

    Given: Valid and unbounded or over-cap ``SET LOCAL statement_timeout`` and ``lock_timeout``
        values (0, '0', '1h', 0ms, 20000, '20s', 31000, plus lock and custom-cap overrides).
    When: Checked by ``_assert_read_only_sql`` under default and custom timeout caps.
    Then: Values within bounds pass while zero, unsupported units, or cap-exceeding values
        refuse with ``write_or_unbounded_sql``.
    """
    for allowed in (
        "SET LOCAL statement_timeout = 20000",
        "SET LOCAL statement_timeout = '20s'",
        "SET LOCAL statement_timeout = '20000'",
        "SET LOCAL statement_timeout = '30000ms'",
        "SET LOCAL lock_timeout = 500",
        "SET LOCAL lock_timeout = '500ms'",
        "SET LOCAL lock_timeout = '1s'",
        "SET LOCAL lock_timeout = 1000",
    ):
        probe._assert_read_only_sql(allowed)

    for unbounded_or_over_cap in (
        "SET LOCAL statement_timeout = 0",
        "SET LOCAL statement_timeout = '0'",
        "SET LOCAL statement_timeout = '1h'",
        "SET LOCAL statement_timeout = 0ms",
        "SET LOCAL statement_timeout = 31000",
        "SET LOCAL statement_timeout = '1min'",
        "SET LOCAL statement_timeout = DEFAULT",
        "SET LOCAL statement_timeout",
        "SET LOCAL lock_timeout = 0",
        "SET LOCAL lock_timeout = '0'",
        "SET LOCAL lock_timeout = '1h'",
        "SET LOCAL lock_timeout = 0ms",
        "SET LOCAL lock_timeout = 1001",
        "SET LOCAL lock_timeout = '2s'",
    ):
        with pytest.raises(probe.CheckpointResumeProbeError, match="write_or_unbounded_sql"):
            probe._assert_read_only_sql(unbounded_or_over_cap)

    with pytest.raises(probe.CheckpointResumeProbeError, match="write_or_unbounded_sql"):
        probe._assert_read_only_sql(
            "SET LOCAL statement_timeout = 20000", statement_timeout_ms=15_000
        )
    probe._assert_read_only_sql("SET LOCAL statement_timeout = 31000", statement_timeout_ms=40_000)


def test_read_only_sql_dialect_and_orm_guards_reject_every_mutation() -> None:
    """Enforce SQL verb checks, dialect timeout setup, and ORM dirty-session guards.

    Given: Read-only and mutating SQL statements, dialect cursors, and clean vs dirty sessions.
    When: Checked by ``_assert_read_only_sql``, ``_enforce_connection_read_only``, and
        ``_guard_session_write``.
    Then: Read-only and timeout setup succeeds while any mutation, writable CTE, unsafe
        PRAGMA/SET, unsupported dialect, bad timeout, or staged ORM write refuses cleanly.
    """
    for safe_sql in (
        "",
        " ; ",
        "SELECT 1; BEGIN; COMMIT; ROLLBACK; SAVEPOINT sp1; RELEASE sp1",
        "SELECT 'DELETE FROM t'; -- INSERT INTO t VALUES (1)\nSELECT 1",
        "WITH prefix AS (SELECT 1) SELECT * FROM prefix",
        "EXPLAIN SELECT 1",
        "PRAGMA query_only",
        "PRAGMA query_only = ON",
        "PRAGMA table_info(symbols)",
        "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY",
    ):
        probe._assert_read_only_sql(safe_sql)

    for unsafe_sql in (
        "INSERT INTO symbols VALUES (1)",
        "UPDATE symbols SET name = 'x'",
        "DELETE FROM symbols",
        "WITH gone AS (DELETE FROM symbols) SELECT * FROM gone",
        "PRAGMA query_only = OFF",
        "PRAGMA writable_schema = ON",
        "SET TRANSACTION READ WRITE",
        "SET work_mem = '1GB'",
    ):
        with pytest.raises(probe.CheckpointResumeProbeError, match="write_sql_refused"):
            probe._assert_read_only_sql(unsafe_sql)

    sqlite_conn = _RecordingConnection()
    probe._enforce_connection_read_only(sqlite_conn, "sqlite")
    assert sqlite_conn.cursor_obj.commands == [
        "PRAGMA foreign_keys = ON",
        "PRAGMA query_only = ON",
        "PRAGMA busy_timeout = 1000",
    ]
    assert sqlite_conn.cursor_obj.closed is True

    pg_conn = _RecordingConnection()
    probe._enforce_connection_read_only(pg_conn, "postgresql", statement_timeout_ms=15_000)
    assert pg_conn.cursor_obj.commands == [
        "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
        "SET SESSION statement_timeout = 15000",
        "SET SESSION lock_timeout = 1000",
        "SET SESSION idle_in_transaction_session_timeout = 10000",
    ]
    assert pg_conn.cursor_obj.closed is True

    with pytest.raises(probe.CheckpointResumeProbeError, match="invalid_statement_timeout_ms:0"):
        probe._enforce_connection_read_only(_RecordingConnection(), "postgresql", 0)
    with pytest.raises(
        probe.CheckpointResumeProbeError, match="invalid_statement_timeout_ms:60001"
    ):
        probe._enforce_connection_read_only(_RecordingConnection(), "postgresql", 60_001)

    with pytest.raises(probe.CheckpointResumeProbeError, match="unsupported_dialect:mysql"):
        probe._enforce_connection_read_only(_RecordingConnection(), "mysql")

    sqlite_check_conn = _RecordingConnection(
        {"PRAGMA query_only": (1,), "PRAGMA busy_timeout": (1000,)}
    )
    probe._verify_dbapi_checkout_state(sqlite_check_conn, "sqlite", 30_000)
    assert sqlite_check_conn.cursor_obj.closed is True

    pg_check_conn = _RecordingConnection(
        {
            "SHOW transaction_read_only": ("on",),
            "SHOW statement_timeout": ("25s",),
            "SHOW lock_timeout": ("1s",),
            "SHOW idle_in_transaction_session_timeout": ("10s",),
        }
    )
    probe._verify_dbapi_checkout_state(pg_check_conn, "postgresql", 25_000)
    with pytest.raises(probe.CheckpointResumeProbeError, match="database_write_would_commit"):
        probe._verify_dbapi_checkout_state(_RecordingConnection(), "sqlite", 30_000)
    with pytest.raises(probe.CheckpointResumeProbeError, match="unsupported_dialect:mysql"):
        probe._verify_dbapi_checkout_state(_RecordingConnection(), "mysql", 30_000)

    probe._guard_session_write(object())
    sync_session = Session()
    try:
        probe._guard_session_write(sync_session)
        sync_session.add(prefix._symbol())
        with pytest.raises(probe.CheckpointResumeProbeError, match="write_commit_detected"):
            probe._guard_session_write(sync_session)
    finally:
        sync_session.close()


async def test_sqlite_preflight_and_runtime_guards_refuse_mutations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse non-SQLAlchemy repos, permissive policies, missing hooks, and writable SQLite.

    Given: Non-SQLAlchemy repositories, permissive read policies, disabled listeners,
        writable SQLite connections, and pooled connections checked out under guard.
    When: ``probe_with_repository`` or ``_preflight_read_only`` executes.
    Then: Every unsafe configuration raises ``CheckpointResumeProbeError`` before proceeding,
        and live SQLite rejects both raw DML and ORM commits before commit completes.

    Args:
        tmp_path: Pytest directory for temporary SQLite database.
        monkeypatch: Pytest fixture patching connection read-only enforcement.
    """
    with pytest.raises(probe.CheckpointResumeProbeError, match="sqlalchemy_repository_required"):
        await probe.probe_with_repository(
            SimpleNamespace(),
            probe.CheckpointResumeProbeConfig(wallet_public_id=prefix._WALLET),
        )

    url, scope = await _seed_and_catchup(tmp_path / "guard.db", monkeypatch)
    async with idless._repository(url) as repo:
        with pytest.raises(
            probe.CheckpointResumeProbeError, match="write_policy_allow_anchor_creation"
        ):
            await probe.probe_with_repository(
                repo,
                probe.CheckpointResumeProbeConfig(wallet_public_id=scope.wallet_public_id),
                read_policy=PnlSeriesReadPolicy(allow_anchor_creation=True),
            )
        with pytest.raises(probe.CheckpointResumeProbeError, match="read_only_guard_missing"):
            await probe._preflight_read_only(repo, PnlSeriesReadPolicy(allow_anchor_creation=False))
        async with repo.session() as warmup:
            await warmup.execute(text("SELECT 1"))
        with probe._read_only_guards(repo, statement_timeout_ms=15_000):
            async with repo.session() as session:
                assert (await session.execute(text("PRAGMA query_only"))).scalar_one() == 1
                with pytest.raises(probe.CheckpointResumeProbeError, match="write_sql_refused"):
                    await session.execute(text("DELETE FROM symbols"))
                with pytest.raises(
                    probe.CheckpointResumeProbeError, match="write_or_unbounded_sql"
                ):
                    await session.execute(text("SET LOCAL statement_timeout = '20s'"))
            with pytest.raises(probe.CheckpointResumeProbeError, match="write_commit_detected"):
                async with repo.session() as session:
                    session.add(prefix._symbol())
                    await session.commit()

        monkeypatch.setattr(
            probe, "_enforce_connection_read_only", lambda _conn, _dialect, _timeout=30_000: None
        )
        repo_writable = SQLAlchemyRepository(url)
        try:
            with (
                probe._read_only_guards(repo_writable),
                pytest.raises(
                    probe.CheckpointResumeProbeError, match="database_write_would_commit"
                ),
            ):
                await probe._preflight_read_only(
                    repo_writable,
                    PnlSeriesReadPolicy(allow_anchor_creation=False),
                )
        finally:
            await repo_writable.engine.dispose()

        sqlite_bad_timeout = _DialectProbeRepo(
            "sqlite",
            {"PRAGMA query_only": 1, "PRAGMA busy_timeout": 5000},
        )
        try:
            with (
                probe._read_only_guards(sqlite_bad_timeout),
                pytest.raises(
                    probe.CheckpointResumeProbeError, match="database_lock_timeout_mismatch"
                ),
            ):
                await probe._preflight_read_only(
                    sqlite_bad_timeout,
                    PnlSeriesReadPolicy(allow_anchor_creation=False),
                )
        finally:
            await sqlite_bad_timeout.engine.dispose()


async def test_postgresql_and_unsupported_dialect_preflight_guards() -> None:
    """Verify PostgreSQL read-only, timeout, and role preflight accepts valid sessions and rejects others.

    Given: Simulated PostgreSQL sessions with valid or mismatched read-only/timeout settings,
        writable vs read-only role privileges, and an unsupported MySQL dialect.
    When: ``_preflight_read_only`` inspects each repository under read-only guards.
    Then: Valid PostgreSQL read-only + timeouts pass, writable roles require explicit opt-in,
        and mismatched read-only or timeout settings refuse cleanly.
    """
    policy = PnlSeriesReadPolicy(allow_anchor_creation=False)
    role_sql = str(probe._ROLE_PRIVILEGE_QUERY)
    base_pg: dict[str, object] = {
        "SHOW transaction_read_only": "on",
        "SHOW statement_timeout": "30s",
        "SHOW lock_timeout": "1s",
        "SHOW idle_in_transaction_session_timeout": "10s",
        role_sql: None,
    }
    pg_ok = _DialectProbeRepo("postgresql", base_pg)
    try:
        with probe._read_only_guards(pg_ok):
            await probe._preflight_read_only(pg_ok, policy)
    finally:
        await pg_ok.engine.dispose()

    pg_writable_role = _DialectProbeRepo(
        "postgresql",
        {**base_pg, role_sql: ("executions", "INSERT")},
    )
    try:
        with (
            probe._read_only_guards(pg_writable_role),
            pytest.raises(
                probe.CheckpointResumeProbeError,
                match="writable_role_refused:executions:INSERT",
            ),
        ):
            await probe._preflight_read_only(pg_writable_role, policy, allow_writable_role=False)
        with probe._read_only_guards(pg_writable_role):
            await probe._preflight_read_only(pg_writable_role, policy, allow_writable_role=True)
    finally:
        await pg_writable_role.engine.dispose()

    bad_cases: tuple[tuple[str, dict[str, object], str], ...] = (
        (
            "postgresql",
            {**base_pg, "SHOW transaction_read_only": "off"},
            "database_write_would_commit",
        ),
        (
            "postgresql",
            {**base_pg, "SHOW statement_timeout": "10s"},
            "database_statement_timeout_mismatch",
        ),
        ("postgresql", {**base_pg, "SHOW lock_timeout": "5s"}, "database_lock_timeout_mismatch"),
        (
            "postgresql",
            {**base_pg, "SHOW idle_in_transaction_session_timeout": "0"},
            "database_idle_timeout_mismatch",
        ),
        ("mysql", {}, "unsupported_dialect:mysql"),
    )
    for dialect_name, responses, expected_error in bad_cases:
        repo = _DialectProbeRepo(dialect_name, responses)
        try:
            with (
                probe._read_only_guards(repo),
                pytest.raises(probe.CheckpointResumeProbeError, match=expected_error),
            ):
                await probe._preflight_read_only(repo, policy)
        finally:
            await repo.engine.dispose()


class _FakeAsyncpgTx:
    """Minimal asyncpg transaction stub."""

    async def start(self) -> None:
        """Start fake transaction."""

    async def commit(self) -> None:
        """Commit fake transaction."""

    async def rollback(self) -> None:
        """Roll back fake transaction."""


class _FakeAsyncpgState:
    """Shared recorder and dynamic role flag for fake asyncpg driver connections."""

    def __init__(self, *, statement_timeout: str = "25s") -> None:
        """Initialize empty connect/query logs, timeout value, and read-only role state."""
        self.connects: list[dict[str, object]] = []
        self.queries: list[str] = []
        self.role_can_write = False
        self.statement_timeout = statement_timeout


_FAKE_PG_STATIC_RESPONSES: tuple[tuple[str, list[tuple[object, ...]]], ...] = (
    ("version()", [("PostgreSQL 16.2",)]),
    ("current_schema", [("public",)]),
    ("transaction isolation level", [("read committed",)]),
    ("standard_conforming_strings", [("on",)]),
    ("transaction_read_only", [("on",)]),
    ("lock_timeout", [("1s",)]),
    ("idle_in_transaction_session_timeout", [("10s",)]),
)


def _split_top_level_commas(projection: str) -> list[str]:
    """Split a SQL projection list on depth-0 commas."""
    columns: list[str] = []
    start = 0
    depth = 0
    for index, ch in enumerate(projection):
        if ch in "()":
            depth += 1 if ch == "(" else -1
        elif ch == "," and depth == 0:
            columns.append(projection[start:index].strip())
            start = index + 1
    columns.append(projection[start:].strip())
    return columns


def _select_column_names(sql: str) -> list[str]:
    """Extract top-level SQLAlchemy projection aliases for fake asyncpg column metadata."""
    collapsed = " ".join(sql.split())
    upper = collapsed.upper()
    depth = 0
    select_end = -1
    from_start = len(collapsed)
    for index, ch in enumerate(collapsed):
        if ch in "()":
            depth += 1 if ch == "(" else -1
            continue
        if depth == 0 and select_end == -1 and upper.startswith("SELECT ", index):
            select_end = index + 7
        elif depth == 0 and select_end != -1 and upper.startswith(" FROM ", index):
            from_start = index
            break
    if select_end == -1:
        return ["col"]
    projection = collapsed[select_end:from_start].strip()
    if projection.upper().startswith("DISTINCT "):
        projection = projection[9:].strip()
    return [
        (
            col.rsplit(" AS ", 1)[-1].rsplit(" as ", 1)[-1]
            if " AS " in col.upper()
            else col.rsplit(".", 1)[-1]
        )
        .strip()
        .strip('"')
        for col in _split_top_level_commas(projection)
    ]


class _FakeAsyncpgPrepared:
    """Minimal asyncpg prepared statement stub."""

    def __init__(self, sql: str, state: _FakeAsyncpgState) -> None:
        """Bind query text and shared driver state."""
        self._sql = sql
        self._state = state

    def get_attributes(self) -> list[SimpleNamespace]:
        """Return typed column attribute descriptors."""
        if "has_table_privilege" in self._sql:
            return [
                SimpleNamespace(name="table_name", type=SimpleNamespace(oid=25)),
                SimpleNamespace(name="privilege", type=SimpleNamespace(oid=25)),
            ]
        return [
            SimpleNamespace(name=col_name, type=SimpleNamespace(oid=701))
            for col_name in _select_column_names(self._sql)
        ]

    def get_parameters(self) -> list[object]:
        """Return empty parameter descriptors."""
        return []

    def get_statusmsg(self) -> str:
        """Return status command tag."""
        return "SELECT 1"

    async def fetch(self, *_args: object) -> list[tuple[object, ...]]:
        """Return query-specific PG metadata, SHOW settings, and privilege rows."""
        self._state.queries.append(self._sql)
        if "has_table_privilege" in self._sql:
            return [("candles", "DELETE")] if self._state.role_can_write else []
        if "statement_timeout" in self._sql and "set_config" not in self._sql:
            return [(self._state.statement_timeout,)]
        lowered = self._sql.lower()
        if "count(" in lowered and "sum(" in lowered and "group by" not in lowered:
            return [(0, None, None, None)]
        for pattern, rows in _FAKE_PG_STATIC_RESPONSES:
            if pattern in self._sql:
                return rows
        return [("1",)] if self._sql.strip() == "SELECT 1" else []


class _FakeAsyncpgConn:
    """Minimal fake asyncpg connection capturing startup ``server_settings``."""

    def __init__(self, state: _FakeAsyncpgState, **kwargs: object) -> None:
        """Record connect keyword arguments."""
        state.connects.append(kwargs)
        self._state = state
        self.closed = False

    async def set_type_codec(self, *_a: object, **_kw: object) -> None:
        """Accept codec registration."""

    async def set_builtin_type_codec(self, *_a: object, **_kw: object) -> None:
        """Accept builtin codec registration."""

    def is_closed(self) -> bool:
        """Report open/closed state."""
        return self.closed

    def transaction(self, **_kw: object) -> _FakeAsyncpgTx:
        """Create a transaction stub."""
        return _FakeAsyncpgTx()

    async def prepare(self, sql: str, **_kw: object) -> _FakeAsyncpgPrepared:
        """Prepare a statement stub."""
        return _FakeAsyncpgPrepared(sql, self._state)

    async def fetchrow(self, _sql: str) -> None:
        """Support SQLAlchemy ``pool_pre_ping`` connectivity check."""
        return None

    async def close(self, timeout: float | None = None) -> None:
        """Mark connection closed."""
        del timeout
        self.closed = True

    def terminate(self) -> None:
        """Force-close connection."""
        self.closed = True

    def add_termination_listener(self, _fn: object) -> None:
        """Accept termination listener."""

    def remove_termination_listener(self, _fn: object) -> None:
        """Remove termination listener."""


async def test_fake_asyncpg_records_startup_server_settings_and_verifies_checkouts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify probe builds its own PG engine with startup ``server_settings`` and checks every checkout.

    Given: A fake asyncpg driver recording ``connect`` kwargs and answering PG queries.
    When: ``_build_probe_engine`` connects and checks out sessions repeatedly under guards.
    Then: Startup ``server_settings`` pin read-only, timeouts, and application name on connect,
        and every pooled checkout verifies read-only mode, timeouts, and role privileges.

    Args:
        monkeypatch: Pytest fixture patching ``asyncpg.connect``.
    """
    state = _FakeAsyncpgState()

    async def _fake_connect(*_args: object, **kwargs: object) -> _FakeAsyncpgConn:
        """Construct a fake asyncpg connection."""
        return _FakeAsyncpgConn(state, **kwargs)

    monkeypatch.setattr(asyncpg, "connect", _fake_connect)
    dsn = "postgresql+asyncpg://probe_ro:secret@127.0.0.1:5432/snapper"
    probe_engine = probe._build_probe_engine(dsn, 25_000)
    repo = SQLAlchemyRepository(dsn)
    default_engine = repo.engine
    repo.engine = probe_engine
    repo.session_factory = probe.async_sessionmaker(
        probe_engine,
        expire_on_commit=False,
        class_=probe.AsyncSession,
    )
    policy = PnlSeriesReadPolicy(allow_anchor_creation=False)
    try:
        await default_engine.dispose()
        with probe._read_only_guards(repo, statement_timeout_ms=25_000):
            await probe._preflight_read_only(
                repo,
                policy,
                statement_timeout_ms=25_000,
                allow_writable_role=False,
            )
            async with repo.session() as second_checkout:
                assert (await second_checkout.execute(text("SELECT 1"))).scalar_one() == "1"
            state.role_can_write = True
            with pytest.raises(
                probe.CheckpointResumeProbeError,
                match="writable_role_refused:candles:DELETE",
            ):
                await probe._preflight_read_only(
                    repo,
                    policy,
                    statement_timeout_ms=25_000,
                    allow_writable_role=False,
                )
    finally:
        await probe_engine.dispose()

    assert len(state.connects) == 1
    assert state.connects[0]["server_settings"] == {
        "timezone": "UTC",
        "default_transaction_read_only": "on",
        "statement_timeout": "25000",
        "lock_timeout": "1000",
        "idle_in_transaction_session_timeout": "10000",
        "application_name": "pnl_checkpoint_resume_probe",
    }
    assert sum(1 for sql in state.queries if sql == "SHOW transaction_read_only") >= 3


async def test_probe_enforces_hard_bounds_max_executions_and_memory_caps(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Enforce hard timeout caps, max executions, memory caps, and ``RLIMIT_AS`` setup.

    Given: Out-of-bounds timeout/cap configs, oversized scope counts, RSS bounds, and timeouts.
    When: ``probe_with_repository`` and memory helpers enforce limits.
    Then: Invalid/upper-capped bounds fail immediately, oversized history refuses before
        buffering, RSS/RLIMIT helpers cover darwin/linux cleanly, and stalled execution times out.

    Args:
        tmp_path: Pytest isolated directory for temporary SQLite fixture.
        monkeypatch: Pytest fixture patching counts, platform, rlimit, and runtime sleep.
    """
    url, scope = await _seed_and_catchup(tmp_path / "timeout.db", monkeypatch)
    async with idless._repository(url) as repo:
        bad_configs = (
            (
                probe.CheckpointResumeProbeConfig(
                    wallet_public_id=scope.wallet_public_id, statement_timeout_ms=0
                ),
                "invalid_statement_timeout_ms:0",
            ),
            (
                probe.CheckpointResumeProbeConfig(
                    wallet_public_id=scope.wallet_public_id, statement_timeout_ms=60_001
                ),
                "invalid_statement_timeout_ms:60001",
            ),
            (
                probe.CheckpointResumeProbeConfig(
                    wallet_public_id=scope.wallet_public_id, max_runtime_seconds=0.0
                ),
                "invalid_max_runtime_seconds:0",
            ),
            (
                probe.CheckpointResumeProbeConfig(
                    wallet_public_id=scope.wallet_public_id, max_runtime_seconds=601.0
                ),
                "invalid_max_runtime_seconds:601.0",
            ),
            (
                probe.CheckpointResumeProbeConfig(
                    wallet_public_id=scope.wallet_public_id, max_executions=0
                ),
                "invalid_max_executions:0",
            ),
            (
                probe.CheckpointResumeProbeConfig(
                    wallet_public_id=scope.wallet_public_id, max_rss_mb=0
                ),
                "invalid_max_rss_mb:0",
            ),
        )
        for bad_config, expected_error in bad_configs:
            with pytest.raises(probe.CheckpointResumeProbeError, match=expected_error):
                await probe.probe_with_repository(repo, bad_config)

        orig_current_rss = probe._current_rss_mb
        rss_values = iter((3000.0, 3000.0, 5500.0))
        monkeypatch.setattr(probe, "_current_rss_mb", lambda: next(rss_values))
        try:
            with pytest.raises(
                probe.CheckpointResumeProbeError, match="max_rss_exceeded:2500.0>2048"
            ):
                await probe.probe_with_repository(
                    repo,
                    probe.CheckpointResumeProbeConfig(wallet_public_id=scope.wallet_public_id),
                )
        finally:
            monkeypatch.setattr(probe, "_current_rss_mb", orig_current_rss)

        orig_counts = repo.get_pnl_execution_prefix_counts
        monkeypatch.setattr(
            repo,
            "get_pnl_execution_prefix_counts",
            AsyncMock(return_value={"kraken": (5, 5)}),
        )
        with pytest.raises(
            probe.CheckpointResumeProbeError, match="^caps_exceeded:executions$"
        ) as exc:
            await probe.probe_with_repository(
                repo,
                probe.CheckpointResumeProbeConfig(
                    wallet_public_id=scope.wallet_public_id,
                    max_executions=2,
                ),
            )
        assert probe._classify_failure(exc.value) == (3, "RESULT: REFUSED caps_exceeded:executions")
        monkeypatch.setattr(repo, "get_pnl_execution_prefix_counts", orig_counts)

        with pytest.raises(probe.CheckpointResumeProbeError, match="^caps_exceeded:executions$"):
            await probe.probe_with_repository(
                repo,
                probe.CheckpointResumeProbeConfig(
                    wallet_public_id=scope.wallet_public_id,
                    max_executions=2,
                ),
            )

        async def slow_series(*_args: object, **_kwargs: object) -> PnlWalletSeriesResult:
            """Simulate a stalled database query triggering the overall runtime ceiling."""
            await asyncio.sleep(0.2)
            return _sample_result(())

        monkeypatch.setattr(probe, "build_wallet_pnl_series", slow_series)
        with pytest.raises(probe.CheckpointResumeProbeError, match="^timeout$"):
            await probe.probe_with_repository(
                repo,
                probe.CheckpointResumeProbeConfig(
                    wallet_public_id=scope.wallet_public_id,
                    window_minutes=4,
                    max_runtime_seconds=0.02,
                ),
            )


async def test_probe_refuses_invalid_configs_and_incomplete_frontiers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse invalid probe arguments and missing or incomplete frontier prerequisites.

    Given: Invalid probe parameters, missing anchor/frontier rows, fill gaps, and short windows.
    When: ``probe_with_repository`` evaluates each broken precondition.
    Then: Every case raises ``CheckpointResumeProbeError`` naming the exact refusal reason.

    Args:
        tmp_path: Pytest directory for the isolated SQLite fixture.
        monkeypatch: Pytest fixture injecting each failure mode in turn.
    """
    url, scope = await _seed_and_catchup(tmp_path / "errors_frontiers.db", monkeypatch)
    async with idless._repository(url) as repo:
        with pytest.raises(probe.CheckpointResumeProbeError, match="invalid_mode:margin"):
            await probe.probe_with_repository(
                repo,
                probe.CheckpointResumeProbeConfig(
                    wallet_public_id=scope.wallet_public_id,
                    mode="margin",
                ),
            )
        with pytest.raises(probe.CheckpointResumeProbeError, match="invalid_window_minutes:0"):
            await probe.probe_with_repository(
                repo,
                probe.CheckpointResumeProbeConfig(
                    wallet_public_id=scope.wallet_public_id,
                    window_minutes=0,
                ),
            )
        with pytest.raises(probe.CheckpointResumeProbeError, match="no_anchor"):
            await probe.probe_with_repository(
                repo,
                probe.CheckpointResumeProbeConfig(
                    wallet_public_id="00000000-0000-7000-8000-00000000ffff"
                ),
            )

        base_config = probe.CheckpointResumeProbeConfig(
            wallet_public_id=scope.wallet_public_id,
            window_minutes=4,
        )
        monkeypatch.setattr(
            repo, "get_portfolio_pnl_correction_frontier", AsyncMock(return_value=None)
        )
        with pytest.raises(probe.CheckpointResumeProbeError, match="no_frontier"):
            await probe.probe_with_repository(repo, base_config)

        monkeypatch.setattr(
            repo,
            "get_portfolio_pnl_correction_frontier",
            AsyncMock(return_value=_sample_frontier(has_fill_gap=True)),
        )
        with pytest.raises(probe.CheckpointResumeProbeError, match="frontier_has_fill_gap"):
            await probe.probe_with_repository(repo, base_config)

        monkeypatch.setattr(
            repo,
            "get_portfolio_pnl_correction_frontier",
            AsyncMock(return_value=_sample_frontier(complete_cut=False)),
        )
        with pytest.raises(probe.CheckpointResumeProbeError, match="frontier_incomplete_cut"):
            await probe.probe_with_repository(repo, base_config)

        short_frontier = replace(_sample_frontier(), tip=samples._T0 + _MINUTE)
        monkeypatch.setattr(
            repo,
            "get_portfolio_pnl_correction_frontier",
            AsyncMock(return_value=short_frontier),
        )
        with pytest.raises(probe.CheckpointResumeProbeError, match="frontier_window_too_short"):
            await probe.probe_with_repository(repo, base_config)


async def test_probe_refuses_capture_verification_and_resume_fallbacks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse whenever capture gap, capture refusal, verify fallback, or resume fallback occurs.

    Given: A valid SQLite scope patched sequentially with capture fill gap, missing witness cut,
        soft capture refusal, verifier fallback, and service resume fallback.
    When: ``probe_with_repository`` evaluates each refusal trigger.
    Then: Every case raises ``CheckpointResumeProbeError`` naming the fallback or refusal.

    Args:
        tmp_path: Pytest directory for the isolated SQLite fixture.
        monkeypatch: Pytest fixture injecting each fallback trigger in turn.
    """
    url, scope = await _seed_and_catchup(tmp_path / "errors_fallbacks.db", monkeypatch)
    base_config = probe.CheckpointResumeProbeConfig(
        wallet_public_id=scope.wallet_public_id,
        window_minutes=4,
    )
    async with idless._repository(url) as repo:
        orig_gap = repo.pnl_timeline_scope_has_fill_gap
        monkeypatch.setattr(repo, "pnl_timeline_scope_has_fill_gap", AsyncMock(return_value=True))
        with pytest.raises(probe.CheckpointResumeProbeError, match="capture_fill_gap"):
            await probe.probe_with_repository(repo, base_config)

        monkeypatch.setattr(repo, "pnl_timeline_scope_has_fill_gap", AsyncMock(return_value=False))
        orig_prefix = repo.get_pnl_timeline_execution_prefix
        monkeypatch.setattr(
            repo,
            "get_pnl_timeline_execution_prefix",
            AsyncMock(return_value={"watermarks": {}, "executions": [], "annulments": []}),
        )
        with pytest.raises(
            probe.CheckpointResumeProbeError, match="missing_capture_fill_witness_cut"
        ):
            await probe.probe_with_repository(repo, base_config)

        monkeypatch.setattr(repo, "pnl_timeline_scope_has_fill_gap", orig_gap)
        monkeypatch.setattr(repo, "get_pnl_timeline_execution_prefix", orig_prefix)
        real_series = probe.build_wallet_pnl_series

        async def refuse_capture(
            *args: object,
            valuation_ccy: str = "USD",
            policy: PnlSeriesReadPolicy,
            options: PnlSeriesReplayOptions,
        ) -> PnlWalletSeriesResult:
            """Simulate a soft capture refusal when checkpoint_capture is supplied."""
            res = await real_series(
                *args,
                valuation_ccy=valuation_ccy,
                policy=policy,
                options=options,
            )
            if options.checkpoint_capture is not None:
                return replace(
                    res, checkpoint_write=None, checkpoint_capture_refusal="global_withhold"
                )
            return res

        monkeypatch.setattr(probe, "build_wallet_pnl_series", refuse_capture)
        with pytest.raises(
            probe.CheckpointResumeProbeError,
            match="checkpoint_capture_refused:global_withhold",
        ):
            await probe.probe_with_repository(repo, base_config)

        monkeypatch.setattr(probe, "build_wallet_pnl_series", real_series)
        orig_verify = probe.verify_checkpoint_for_resume
        monkeypatch.setattr(
            probe,
            "verify_checkpoint_for_resume",
            AsyncMock(return_value=PnlCheckpointFallback("checkpoint_late_execution", True)),
        )
        with pytest.raises(
            probe.CheckpointResumeProbeError,
            match="checkpoint_verify_fallback:checkpoint_late_execution",
        ):
            await probe.probe_with_repository(repo, base_config)

        monkeypatch.setattr(probe, "verify_checkpoint_for_resume", orig_verify)

        async def resume_fallback(
            *args: object,
            valuation_ccy: str = "USD",
            policy: PnlSeriesReadPolicy,
            options: PnlSeriesReplayOptions,
        ) -> PnlWalletSeriesResult:
            """Simulate a runtime resume fallback on the resumed branch."""
            res = await real_series(
                *args,
                valuation_ccy=valuation_ccy,
                policy=policy,
                options=options,
            )
            if options.checkpoint_resume is not None:
                return replace(res, checkpoint_resume_fallback="checkpoint_fx_election_changed")
            return res

        monkeypatch.setattr(probe, "build_wallet_pnl_series", resume_fallback)
        with pytest.raises(
            probe.CheckpointResumeProbeError,
            match="checkpoint_resume_fallback:checkpoint_fx_election_changed",
        ):
            await probe.probe_with_repository(repo, base_config)


def test_canonical_and_sequence_comparison() -> None:
    """Cover NaN/signed-zero canonicalization and sequence mismatch detection.

    Given: Scalar/float/datetime/dataclass inputs and cold vs resumed series pairs.
    When: ``_canonical`` and ``_compare_series`` evaluate equality, NaN, -0.0, and rate sources.
    Then: Bit-level differences are pinpointed and unsupported objects raise ``TypeError``.
    """
    t1 = samples._T0 + _MINUTE
    assert probe._canonical(None) is None
    assert probe._canonical("ok") == "ok"
    assert probe._canonical(True) is True
    assert probe._canonical(7) == 7
    assert probe._canonical(float("nan")) == "nan"
    assert probe._canonical(-0.0) != probe._canonical(0.0)
    assert probe._canonical(t1) == t1.isoformat()
    src = PnlFxRateSource("BTC", "USD", "BTC", "USD", "kraken")
    assert probe._canonical((src,)) == [
        {
            "source_currency": "BTC",
            "valuation_currency": "USD",
            "base_currency": "BTC",
            "quote_currency": "USD",
            "exchange": "kraken",
        }
    ]
    with pytest.raises(TypeError):
        probe._canonical(object())

    p_nan = _sample_point(t1, float("nan"))
    p_zero = _sample_point(t1, 0.0)
    p_neg_zero = _sample_point(t1, -0.0)
    assert (
        probe._compare_series(_sample_result((p_nan,), (src,)), _sample_result((p_nan,), (src,)))
        is None
    )
    assert (
        probe._compare_series(_sample_result((p_zero,)), _sample_result((p_zero, p_zero)))
        == "points.length:1!=2"
    )
    assert (
        probe._compare_series(_sample_result((p_zero,)), _sample_result((p_neg_zero,)))
        == "points[0]"
    )
    other_src = PnlFxRateSource("BTC", "USD", "BTC", "USD", "coinbase")
    assert (
        probe._compare_series(
            _sample_result((p_zero,), (src,)),
            _sample_result((p_zero,), (other_src,)),
        )
        == "rate_sources[0]"
    )


def test_cli_exit_codes_and_result_lines(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify exact CLI exit codes 1/2/3, RESULT formatting, and single-line DSN redaction.

    Given: Mismatch report, closed refusals, missing frontier, timeout, interrupt, and DSN error.
    When: ``main`` executes each outcome path.
    Then: Exit codes and ``RESULT:`` lines match contract with credentials redacted everywhere.

    Args:
        monkeypatch: Pytest fixture patching CLI coroutine results.
        capsys: Stdout/stderr capture verifying error and mismatch output.
    """
    monkeypatch.delenv("PNL_PROBE_DB_URL", raising=False)
    t1 = samples._T0 + _MINUTE

    mismatch_report = probe.CheckpointResumeProbeReport(
        wallet_public_id=prefix._WALLET,
        mode="live",
        valuation_ccy="USD",
        calc_version=PNL_SAMPLE_CALC_VERSION,
        checkpoint_minute=t1,
        frontier_tip=t1 + _MINUTE,
        point_count=1,
        rate_source_count=0,
        cold_seconds=0.1,
        capture_seconds=0.1,
        verify_seconds=0.05,
        resume_seconds=0.02,
        matched=False,
        mismatch_detail="points[0]",
    )
    monkeypatch.setattr(
        probe, "probe_wallet_checkpoint_resume", AsyncMock(return_value=mismatch_report)
    )
    assert probe.main(["--wallet", prefix._WALLET]) == 1
    mismatch_out = capsys.readouterr().out
    assert "bit_identical=False mismatch_detail=points[0]" in mismatch_out
    assert mismatch_out.strip().splitlines()[-1] == "RESULT: FAIL points[0]"

    monkeypatch.setattr(
        probe,
        "probe_wallet_checkpoint_resume",
        AsyncMock(
            side_effect=probe.CheckpointResumeProbeError(
                "checkpoint_verify_fallback:checkpoint_late_execution"
            )
        ),
    )
    assert probe.main(["--wallet", prefix._WALLET, "--require-readonly-role"]) == 3
    refusal_io = capsys.readouterr()
    assert refusal_io.err == ""
    assert sum(1 for line in refusal_io.out.splitlines() if line.startswith("WARNING:")) == 1
    assert (
        refusal_io.out.strip().splitlines()[-1]
        == "RESULT: REFUSED checkpoint_verify_fallback:checkpoint_late_execution"
    )

    monkeypatch.setattr(
        probe,
        "probe_wallet_checkpoint_resume",
        AsyncMock(side_effect=probe.CheckpointResumeProbeError("capture_fill_gap")),
    )
    assert probe.main(["--wallet", prefix._WALLET]) == 3
    assert capsys.readouterr().out.strip().splitlines()[-1] == "RESULT: REFUSED capture_fill_gap"

    monkeypatch.setattr(
        probe,
        "probe_wallet_checkpoint_resume",
        AsyncMock(side_effect=probe.CheckpointResumeProbeError("no_frontier")),
    )
    assert probe.main(["--wallet", prefix._WALLET]) == 2
    assert capsys.readouterr().out.strip().splitlines()[-1] == "RESULT: FAIL no_frontier"

    monkeypatch.setattr(
        probe,
        "probe_wallet_checkpoint_resume",
        AsyncMock(side_effect=TimeoutError()),
    )
    assert probe.main(["--wallet", prefix._WALLET]) == 2
    assert capsys.readouterr().out.strip().splitlines()[-1] == "RESULT: FAIL timeout"

    monkeypatch.setattr(
        probe,
        "probe_wallet_checkpoint_resume",
        AsyncMock(side_effect=KeyboardInterrupt()),
    )
    assert probe.main(["--wallet", prefix._WALLET]) == 2
    assert capsys.readouterr().out.strip().splitlines()[-1] == "RESULT: FAIL interrupted"

    monkeypatch.setattr(
        probe,
        "probe_wallet_checkpoint_resume",
        AsyncMock(
            side_effect=SQLAlchemyError(
                "connect failed postgresql+asyncpg://alice:supersecret@db.internal:5432/prod password=topsecret"
            )
        ),
    )
    assert (
        probe.main(["--wallet", prefix._WALLET, "--allow-writable-role-with-readonly-session"]) == 2
    )
    secret_io = capsys.readouterr()
    assert secret_io.err == ""
    assert "supersecret" not in secret_io.out
    assert "topsecret" not in secret_io.out
    assert (
        secret_io.out.strip().splitlines()[-1]
        == "RESULT: FAIL connect failed postgresql+asyncpg://***:***@db.internal:5432/prod password=***"
    )


async def test_dsn_redaction_engine_errors_and_interrupt_dispose(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Redact credentials everywhere and dispose both engines on Ctrl-C or startup failure.

    Given: DSN/password strings, failing bootstrap settings, malformed/unsupported URLs,
        and an interrupted probe execution.
    When: ``_redact_secrets``, ``_build_probe_engine``, and ``probe_wallet_checkpoint_resume`` run.
    Then: Credentials never appear unredacted and engines are disposed cleanly even on interrupt.

    Args:
        monkeypatch: Pytest fixture injecting bootstrap/repo failures and interrupts.
    """
    monkeypatch.delenv("PNL_PROBE_DB_URL", raising=False)
    redacted = probe._redact_secrets(
        "postgresql+asyncpg://alice:s3cr3t@10.0.1.5:5432/prod "
        "host=db password=topsecret token='abc def'"
    )
    assert "s3cr3t" not in redacted
    assert "topsecret" not in redacted
    assert "abc def" not in redacted
    assert "postgresql+asyncpg://***:***@10.0.1.5:5432/prod" in redacted
    assert "password=*** token=***" in redacted

    monkeypatch.setattr(
        probe,
        "get_bootstrap_settings",
        lambda: (_ for _ in ()).throw(
            ValueError("Unknown key in .env: postgresql://bob:envsecret@localhost:5432/db")
        ),
    )
    with pytest.raises(probe.CheckpointResumeProbeError) as boot_exc:
        await probe.probe_wallet_checkpoint_resume(
            probe.CheckpointResumeProbeConfig(wallet_public_id=prefix._WALLET)
        )
    assert "envsecret" not in str(boot_exc.value)
    assert "***:***@localhost:5432/db" in str(boot_exc.value)

    with pytest.raises(probe.CheckpointResumeProbeError, match="invalid_database_url"):
        probe._build_probe_engine("not-a-valid-url", 30_000)
    with pytest.raises(probe.CheckpointResumeProbeError) as driver_exc:
        probe._build_probe_engine("postgresql+missingdriver://snapper_ro:s3cr3t@h/d", 30_000)
    assert "s3cr3t" not in str(driver_exc.value)
    assert "invalid_database_url:" in str(driver_exc.value)
    with pytest.raises(probe.CheckpointResumeProbeError, match="unsupported_dialect:mysql"):
        probe._build_probe_engine("mysql+aiomysql://u:pw@h/d", 30_000)

    mem_engine = probe._build_probe_engine("sqlite+aiosqlite:///:memory:", 30_000)
    await mem_engine.dispose()

    disposed_ids: list[int] = []
    orig_dispose = AsyncEngine.dispose

    async def track_dispose(engine: AsyncEngine) -> None:
        """Record disposed async engine identities cleanly."""
        disposed_ids.append(id(engine))
        await orig_dispose(engine)

    monkeypatch.setattr(AsyncEngine, "dispose", track_dispose)
    probe_engine = probe._build_probe_engine("sqlite+aiosqlite:///:memory:", 30_000)
    monkeypatch.setattr(probe, "_build_probe_engine", lambda _u, _t: probe_engine)

    def fail_repo(_url: str) -> SQLAlchemyRepository:
        """Simulate failure before ``default_engine`` assignment."""
        raise SQLAlchemyError("postgresql+asyncpg://u:pw@h/d")

    monkeypatch.setattr(probe, "SQLAlchemyRepository", fail_repo)
    with pytest.raises(SQLAlchemyError):
        await probe.probe_wallet_checkpoint_resume(
            probe.CheckpointResumeProbeConfig(
                wallet_public_id=prefix._WALLET,
                db_url="sqlite+aiosqlite:///:memory:",
            )
        )
    assert disposed_ids == [id(probe_engine)]

    disposed_ids.clear()
    probe_engine2 = probe._build_probe_engine("sqlite+aiosqlite:///:memory:", 30_000)
    monkeypatch.setattr(probe, "_build_probe_engine", lambda _u, _t: probe_engine2)
    monkeypatch.setattr(probe, "SQLAlchemyRepository", SQLAlchemyRepository)
    monkeypatch.setattr(probe, "probe_with_repository", AsyncMock(side_effect=KeyboardInterrupt()))
    with pytest.raises(KeyboardInterrupt):
        await probe.probe_wallet_checkpoint_resume(
            probe.CheckpointResumeProbeConfig(
                wallet_public_id=prefix._WALLET,
                db_url="sqlite+aiosqlite:///:memory:",
            )
        )
    assert id(probe_engine2) in disposed_ids
    assert len(disposed_ids) == 2


def test_db_url_flag_and_env_override_app_url_and_warning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify ``--db-url`` and ``PNL_PROBE_DB_URL`` override app DB_URL with warning control.

    Given: A seeded SQLite scope, app ``DB_URL``, ``PNL_PROBE_DB_URL``, and ``--db-url``.
    When: Running CLI ``main`` with ``--db-url`` overriding env, with ``PNL_PROBE_DB_URL``
        overriding app ``DB_URL``, and with neither set.
    Then: Override precedence is ``--db-url > PNL_PROBE_DB_URL > DB_URL``, app ``DB_URL`` env stays
        unchanged, no warning prints when overridden, and exactly one ``WARNING`` line prints
        before continuing when neither override is set.

    Args:
        tmp_path: Pytest isolated directory for the SQLite scope database.
        monkeypatch: Pytest fixture controlling environment variables and bootstrap settings.
        capsys: Stdout/stderr capture checking warnings and RESULT verdicts.
    """
    url, scope = asyncio.run(_seed_and_catchup(tmp_path / "override.db", monkeypatch))
    monkeypatch.setenv("DB_URL", "postgresql+asyncpg://app_rw:appsecret@app-db:5432/app")
    monkeypatch.setattr(
        probe,
        "get_bootstrap_settings",
        lambda: (_ for _ in ()).throw(AssertionError("app DB_URL should not be loaded")),
    )

    monkeypatch.setenv("PNL_PROBE_DB_URL", "postgresql+asyncpg://env_ro:envpass@127.0.0.1:1/wrong")
    assert (
        probe.main(["--wallet", scope.wallet_public_id, "--window-minutes", "4", "--db-url", url])
        == 0
    )
    cli_io = capsys.readouterr()
    assert cli_io.err == ""
    assert not any(line.startswith("WARNING:") for line in cli_io.out.splitlines())
    assert cli_io.out.strip().splitlines()[-1] == "RESULT: PASS"
    assert os.environ["DB_URL"] == "postgresql+asyncpg://app_rw:appsecret@app-db:5432/app"

    monkeypatch.setenv("PNL_PROBE_DB_URL", url)
    assert probe.main(["--wallet", scope.wallet_public_id, "--window-minutes", "4"]) == 0
    env_io = capsys.readouterr()
    assert env_io.err == ""
    assert not any(line.startswith("WARNING:") for line in env_io.out.splitlines())
    assert env_io.out.strip().splitlines()[-1] == "RESULT: PASS"

    monkeypatch.setenv("PNL_PROBE_DB_URL", "   ")
    monkeypatch.setattr(probe, "get_bootstrap_settings", lambda: SimpleNamespace(db_url=url))
    assert probe.main(["--wallet", scope.wallet_public_id, "--window-minutes", "4"]) == 0
    warn_io = capsys.readouterr()
    assert warn_io.err == ""
    warning_lines = [line for line in warn_io.out.splitlines() if line.startswith("WARNING:")]
    assert len(warning_lines) == 1
    assert "snapper_ro" in warning_lines[0]
    assert warn_io.out.strip().splitlines()[-1] == "RESULT: PASS"


def test_db_url_flag_and_env_errors_redact_credentials(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify invalid driver errors and connect exceptions redact ``--db-url`` and env DSNs.

    Given: Credentialed ``--db-url`` and ``PNL_PROBE_DB_URL`` values triggering driver/connect errors.
    When: Running CLI ``main`` for bad drivers and failing ``asyncpg.connect`` calls.
    Then: No password or secret token appears unredacted in stdout or stderr.

    Args:
        monkeypatch: Pytest fixture patching environment variables and ``asyncpg.connect``.
        capsys: Stdout/stderr capture verifying redacted failure output.
    """
    monkeypatch.delenv("PNL_PROBE_DB_URL", raising=False)
    assert (
        probe.build_parser().parse_args(["--wallet", prefix._WALLET]).allow_writable_role is False
    )
    bad_driver_dsn = "postgresql+missing://snapper_ro:driversec@prod.internal:5432/snapper"
    assert probe.main(["--wallet", prefix._WALLET, "--db-url", bad_driver_dsn]) == 2
    bad_driver_io = capsys.readouterr()
    assert "driversec" not in bad_driver_io.out
    assert "invalid_database_url:" in bad_driver_io.out

    bad_flag_dsn = "postgresql+asyncpg://snapper_ro:flagsecret@prod.internal:5432/snapper"

    async def _fail_flag_connect(*_args: object, **_kwargs: object) -> object:
        """Raise an exception containing the flag DSN and password parameter."""
        raise OSError(f"connect failed {bad_flag_dsn} password=qsecret")

    monkeypatch.setattr(asyncpg, "connect", _fail_flag_connect)
    assert probe.main(["--wallet", prefix._WALLET, "--db-url", bad_flag_dsn]) == 2
    bad_flag_io = capsys.readouterr()
    assert bad_flag_io.err == ""
    assert "flagsecret" not in bad_flag_io.out
    assert "qsecret" not in bad_flag_io.out
    assert "postgresql+asyncpg://***:***@prod.internal:5432/snapper" in bad_flag_io.out
    assert "password=***" in bad_flag_io.out

    bad_env_dsn = "postgresql+asyncpg://snapper_ro:envsecret@prod.internal:5432/snapper"

    async def _fail_env_connect(*_args: object, **_kwargs: object) -> object:
        """Raise an exception containing the environment override DSN."""
        raise OSError(f"connect failed {bad_env_dsn}")

    monkeypatch.setattr(asyncpg, "connect", _fail_env_connect)
    monkeypatch.setenv("PNL_PROBE_DB_URL", bad_env_dsn)
    assert probe.main(["--wallet", prefix._WALLET]) == 2
    bad_env_io = capsys.readouterr()
    assert bad_env_io.err == ""
    assert "envsecret" not in bad_env_io.out
    assert "postgresql+asyncpg://***:***@prod.internal:5432/snapper" in bad_env_io.out


def test_pg_override_url_passes_readonly_role_without_override_and_warns_when_unset(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Verify PG role check passes cleanly under ``snapper_ro`` override and warns when unset.

    Given: A fake PostgreSQL asyncpg driver simulating ``snapper_ro`` vs a writable role.
    When: Invoking ``main`` with ``--db-url``, with ``PNL_PROBE_DB_URL``, and without either.
    Then: Overrides pass the writable-role preflight with no override flag under ``snapper_ro``,
        refuse writable roles without the override flag, and emit one ``WARNING`` before running
        all existing checks when neither override is set.

    Args:
        monkeypatch: Pytest fixture patching asyncpg, bootstrap settings, and scope load.
        capsys: Stdout/stderr capture checking warnings and RESULT lines.
    """
    state = _FakeAsyncpgState(statement_timeout="30s")

    async def _fake_connect(*_args: object, **_kwargs: object) -> _FakeAsyncpgConn:
        """Return a fake asyncpg connection bound to ``state``."""
        return _FakeAsyncpgConn(state)

    monkeypatch.setattr(asyncpg, "connect", _fake_connect)
    monkeypatch.setattr(
        probe,
        "_load_prepared_scope",
        AsyncMock(side_effect=probe.CheckpointResumeProbeError("no_frontier")),
    )
    monkeypatch.setattr(
        probe,
        "get_bootstrap_settings",
        lambda: (_ for _ in ()).throw(AssertionError("app DB_URL should not be called")),
    )

    ro_dsn = "postgresql+asyncpg://snapper_ro:rosecret@db.internal:5432/snapper"
    assert probe.main(["--wallet", prefix._WALLET, "--db-url", ro_dsn]) == 2
    flag_io = capsys.readouterr()
    assert flag_io.err == ""
    assert not any(line.startswith("WARNING:") for line in flag_io.out.splitlines())
    assert flag_io.out.strip().splitlines()[-1] == "RESULT: FAIL no_frontier"
    assert any("has_table_privilege" in sql for sql in state.queries)

    state.queries.clear()
    monkeypatch.setenv("PNL_PROBE_DB_URL", ro_dsn)
    assert probe.main(["--wallet", prefix._WALLET]) == 2
    env_io = capsys.readouterr()
    assert env_io.err == ""
    assert not any(line.startswith("WARNING:") for line in env_io.out.splitlines())
    assert env_io.out.strip().splitlines()[-1] == "RESULT: FAIL no_frontier"
    assert any("has_table_privilege" in sql for sql in state.queries)

    state.role_can_write = True
    assert probe.main(["--wallet", prefix._WALLET]) == 2
    writable_io = capsys.readouterr()
    assert (
        writable_io.out.strip().splitlines()[-1]
        == "RESULT: FAIL writable_role_refused:candles:DELETE"
    )
    assert (
        probe.main(["--wallet", prefix._WALLET, "--allow-writable-role-with-readonly-session"]) == 2
    )
    override_io = capsys.readouterr()
    assert override_io.out.strip().splitlines()[-1] == "RESULT: FAIL no_frontier"

    monkeypatch.delenv("PNL_PROBE_DB_URL", raising=False)
    monkeypatch.setattr(
        probe,
        "get_bootstrap_settings",
        lambda: SimpleNamespace(
            db_url="postgresql+asyncpg://app_rw:appsecret@db.internal:5432/snapper"
        ),
    )
    assert probe.main(["--wallet", prefix._WALLET]) == 2
    fallback_writable_io = capsys.readouterr()
    assert (
        sum(1 for line in fallback_writable_io.out.splitlines() if line.startswith("WARNING:")) == 1
    )
    assert (
        fallback_writable_io.out.strip().splitlines()[-1]
        == "RESULT: FAIL writable_role_refused:candles:DELETE"
    )

    state.role_can_write = False
    assert probe.main(["--wallet", prefix._WALLET]) == 2
    fallback_ro_io = capsys.readouterr()
    assert sum(1 for line in fallback_ro_io.out.splitlines() if line.startswith("WARNING:")) == 1
    assert fallback_ro_io.out.strip().splitlines()[-1] == "RESULT: FAIL no_frontier"


async def _record_pg_production_read_queries(
    monkeypatch: pytest.MonkeyPatch,
) -> list[str]:
    """Execute production P&L read paths on fake asyncpg and collect all SQL emitted."""
    state = _FakeAsyncpgState(statement_timeout="30s")

    async def _fake_connect(*_args: object, **_kwargs: object) -> _FakeAsyncpgConn:
        """Return a fake asyncpg connection bound to ``state``."""
        return _FakeAsyncpgConn(state)

    monkeypatch.setattr(asyncpg, "connect", _fake_connect)
    dsn = "postgresql+asyncpg://snapper_ro:rosec@db.internal:5432/snapper"
    pg_engine = probe._build_probe_engine(dsn, 30_000)
    repo = SQLAlchemyRepository(dsn)
    default_engine = repo.engine
    repo.engine = pg_engine
    repo.session_factory = probe.async_sessionmaker(
        pg_engine, expire_on_commit=False, class_=probe.AsyncSession
    )
    scope = _sample_frontier().key
    sample_scope = PortfolioPnlSampleScope(
        wallet_public_id=scope.wallet_public_id,
        mode=scope.mode,
        valuation_ccy=scope.valuation_ccy,
        epoch_public_id=scope.epoch_public_id,
        anchor_point_time=samples._T0,
    )
    t1, t2 = samples._T0 + _MINUTE, samples._T0 + 2 * _MINUTE
    cut: PnlTimelineFillWitnessCut = {"watermarks": {"kraken": 1}, "digest": "a" * 64}
    try:
        await default_engine.dispose()
        await repo.get_portfolio_pnl_anchor(scope.wallet_public_id, scope.mode, "USD", None)
        await repo.get_portfolio_pnl_correction_frontier(sample_scope)
        await repo.get_pnl_execution_prefix_counts(scope.wallet_public_id, "live", {"kraken": 1})
        await repo.get_pnl_timeline_execution_prefix(
            scope.wallet_public_id, "live", t2, capture_fill_witness_cut=True
        )
        await repo.get_pnl_timeline_execution_prefix_at_watermarks(
            scope.wallet_public_id, "live", {}, t2
        )
        await repo.pnl_timeline_scope_has_fill_gap(scope.wallet_public_id, "live", t2)
        await repo.get_accruals_for_pnl(scope.wallet_public_id, "live", t2)
        await repo.get_pnl_accrual_prefix_fingerprint(
            scope.wallet_public_id, "live", samples._T0, t1, t2
        )
        await repo.get_instrument_symbol_refs([prefix._INSTRUMENT], t2)
        await repo.get_pnl_timeline_candles(
            [
                {
                    "instrument_public_id": prefix._INSTRUMENT,
                    "native_symbol": "BTC/USD",
                    "exchange": "kraken",
                    "instrument_exchange": "kraken",
                    "base_currency": "BTC",
                    "quote_currency": "USD",
                    "valid_from": samples._T0,
                    "valid_to": t2,
                }
            ],
            t1,
            t2,
            t2,
        )
        await repo.get_pnl_fx_rate_exchanges([("EUR", "USD")], t1, t2, t2)
        await repo.get_pnl_fx_rate_exchanges_at([("EUR", "USD")], [t1], t2)
        await repo.get_pnl_fx_rate_candles([("EUR", "USD", "kraken")], t1, t2, t2)
        await repo.get_pnl_fx_rate_candles_at([("EUR", "USD", "kraken")], [t1], t2)
        await repo.get_pnl_event_fx_candle_fingerprint([("EUR", "USD", "kraken")], [t1], t2)
        await repo.get_pnl_event_fx_candle_fingerprints_by_plane(
            [("EUR", "USD", "kraken")], [t1], t2
        )
        await repo.get_pnl_timeline_execution_lineage([prefix._ORDER], t2)
        await repo.get_spot_execution_chain_tip(
            scope.wallet_public_id, "kraken", "live", 0, "0" * 64, 0
        )
        assert cut["digest"]
    finally:
        await pg_engine.dispose()
    return list(state.queries)


async def test_bound_pg_timeout_set_config_and_production_read_audit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Allow exact ``_bound_pnl_correction_transaction`` SQL and verify all probe reads pass guard.

    Given: Exact production ``set_config`` timeout SQL, tampered/over-cap variants, and live
        SQLite + PostgreSQL-dialect production read paths exercised against an SQL recorder.
    When: ``_assert_read_only_sql`` checks every direct case and every recorded statement.
    Then: Exact ``_bound_pnl_correction_transaction`` SQL passes within caps, over-cap or
        tampered ``set_config`` refuses cleanly, and every recorded read statement passes.

    Args:
        tmp_path: Pytest directory for the temporary SQLite probe database.
        monkeypatch: Pytest fixture patching fake asyncpg for PG SQL recording.
    """
    pg_bound_sql = (
        "SELECT set_config('statement_timeout', CASE WHEN "
        "current_setting('statement_timeout')::interval = interval '0' OR "
        "current_setting('statement_timeout')::interval > interval '20 seconds' "
        "THEN '20s' ELSE current_setting('statement_timeout') END, true), "
        "set_config('lock_timeout', CASE WHEN "
        "current_setting('lock_timeout')::interval = interval '0' OR "
        "current_setting('lock_timeout')::interval > interval '1 second' "
        "THEN '1s' ELSE current_setting('lock_timeout') END, true)"
    )
    probe._assert_read_only_sql(pg_bound_sql, statement_timeout_ms=30_000)
    probe._assert_read_only_sql(pg_bound_sql, statement_timeout_ms=5_000)

    with pytest.raises(probe.CheckpointResumeProbeError, match="write_or_unbounded_sql:set_config"):
        probe._assert_read_only_sql(pg_bound_sql.replace("'20s'", "'15s'"))
    with pytest.raises(probe.CheckpointResumeProbeError, match="write_or_unbounded_sql:set_config"):
        probe._assert_read_only_sql(pg_bound_sql.replace("'20 seconds'", "'1 minute'"))
    over_hard_cap = pg_bound_sql.replace("'20 seconds'", "'61 seconds'").replace("'20s'", "'61s'")
    with pytest.raises(probe.CheckpointResumeProbeError, match="write_or_unbounded_sql:set_config"):
        probe._assert_read_only_sql(over_hard_cap, statement_timeout_ms=5_000)
    for bad_set_config in (
        pg_bound_sql.replace(", true)", ", false)"),
        pg_bound_sql.replace("'statement_timeout'", "'work_mem'"),
        "SELECT set_config('statement_timeout', '20s', true)",
        "WITH c AS (" + pg_bound_sql + ") SELECT * FROM c",
    ):
        with pytest.raises(
            probe.CheckpointResumeProbeError, match="write_sql_refused:forbidden_function"
        ):
            probe._assert_read_only_sql(bad_set_config)

    url, scope = await _seed_and_catchup(tmp_path / "audit.db", monkeypatch)
    async with idless._repository(url) as sqlite_repo:
        async with sqlite_repo.session() as session:
            session.add(
                checkpoint_repository._accrual_orm(
                    samples._T0 + _MINUTE, 1, scope=(scope.wallet_public_id, scope.mode)
                )
            )
            await session.commit()
        recorded_sqlite: list[str] = []
        event.listen(
            sqlite_repo.engine.sync_engine,
            "before_cursor_execute",
            lambda _c, _cur, stmt, *_r: recorded_sqlite.append(stmt),
        )
        await probe.probe_with_repository(
            sqlite_repo,
            probe.CheckpointResumeProbeConfig(
                wallet_public_id=scope.wallet_public_id, window_minutes=4
            ),
        )
        assert recorded_sqlite
        for sql in recorded_sqlite:
            probe._assert_read_only_sql(sql)

    recorded_pg = await _record_pg_production_read_queries(monkeypatch)
    assert any("set_config('statement_timeout'" in sql for sql in recorded_pg)
    assert any("SET LOCAL jit = off" in sql for sql in recorded_pg)
    assert any("REPEATABLE READ, READ ONLY" in sql for sql in recorded_pg)
    assert any("WITH pnl_fx_candidate_instruments" in sql for sql in recorded_pg)
    for sql in recorded_pg:
        probe._assert_read_only_sql(sql, statement_timeout_ms=30_000)


async def test_candle_cap_counts_only_wallet_marks_and_candidate_fx(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exclude unrelated instruments without dropping relevant candidate-FX candles.

    Given: A caught-up wallet, an unrelated crypto series, and a candidate forex series.
    When: Scope and direct candle counts run before and after adding those rows.
    Then: The scope count is unchanged while the repository-proof FX count sees its rows.

    Args:
        tmp_path: Pytest isolated directory for the SQLite fixture.
        monkeypatch: Pytest fixture configuring the bounded off-mode seed.
    """
    url, scope = await _seed_and_catchup(tmp_path / "scoped_candles.db", monkeypatch)
    async with idless._repository(url) as repo:
        frontier = await repo.get_portfolio_pnl_correction_frontier(scope)
        assert frontier is not None
        before = await probe._count_scope_candles(repo, scope, frontier)
        assert before > 0
        unrelated_symbol = "00000000-0000-7000-8000-00000000f101"
        unrelated_instrument = "00000000-0000-7000-8000-00000000f102"
        fx_symbol_id = "00000000-0000-7000-8000-00000000f103"
        fx_instrument_id = "00000000-0000-7000-8000-00000000f104"
        fx_symbol = prices._symbol(fx_symbol_id, "USD-EUR", "USD", "EUR")
        fx_symbol.asset_type = "forex"
        async with repo.session() as session:
            session.add_all(
                (
                    prices._symbol(unrelated_symbol, "ETH-USD", "ETH", "USD"),
                    prices._instrument(unrelated_instrument, unrelated_symbol, "kraken"),
                    fx_symbol,
                    prices._instrument(fx_instrument_id, fx_symbol_id, "kraken"),
                )
            )
            for offset in range(25):
                minute = samples._T0 + offset * _MINUTE
                for instrument_id in (unrelated_instrument, fx_instrument_id):
                    session.add(prices._candle(instrument_id, minute, 1.1))
            await session.commit()

        assert await probe._count_scope_candles(repo, scope, frontier) == before
        fx_refs = await repo.get_instrument_symbol_refs([fx_instrument_id], frontier.as_of)
        assert probe._fx_candidate_pairs(fx_refs, "USD") == [("EUR", "USD"), ("USD", "EUR")]
        assert (
            await probe._count_mark_candles(
                repo, [], scope.anchor_point_time, frontier.tip, frontier.as_of
            )
            == 0
        )
        assert (
            await probe._count_fx_candles(
                repo, [], scope.anchor_point_time, frontier.tip, frontier.as_of
            )
            == 0
        )
        assert (
            await probe._count_fx_candles(
                repo,
                [("USD", "EUR", "kraken")],
                scope.anchor_point_time,
                frontier.tip,
                frontier.as_of,
            )
            == 11
        )


def test_accrual_candle_and_cut_moved_caps_refuse_with_exit_three(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Refuse oversized accruals, candles, or moved capture cuts with exit code 3.

    Given: Valid and zero cap configs, seeded accruals/candles, and a capture cut that grows
        beyond ``max_executions`` between precheck and fresh capture.
    When: Probe validation and execution evaluate caps and cut stability.
    Then: Zero caps fail validation cleanly, accrual/candle caps return exit code 3
        ``RESULT: REFUSED caps_exceeded:<kind>``, and moved cuts return exit code 3
        ``RESULT: REFUSED cut_moved``.

    Args:
        tmp_path: Pytest isolated directory for temporary SQLite fixture.
        monkeypatch: Pytest fixture patching CLI/cut growth scenarios.
    """
    url, scope = asyncio.run(_seed_and_catchup(tmp_path / "caps.db", monkeypatch))

    async def _seed_accruals_and_check_invalid() -> None:
        """Validate zero-cap failures, seed accruals, and verify empty witness count."""
        async with idless._repository(url) as repo:
            for bad_cfg, expected in (
                (
                    probe.CheckpointResumeProbeConfig(
                        wallet_public_id=scope.wallet_public_id, max_accruals=0
                    ),
                    "invalid_max_accruals:0",
                ),
                (
                    probe.CheckpointResumeProbeConfig(
                        wallet_public_id=scope.wallet_public_id, max_candles=0
                    ),
                    "invalid_max_candles:0",
                ),
            ):
                with pytest.raises(probe.CheckpointResumeProbeError, match=expected):
                    await probe.probe_with_repository(repo, bad_cfg)

            async with repo.session() as session:
                for row_id in (1, 2):
                    session.add(
                        checkpoint_repository._accrual_orm(
                            samples._T0 + row_id * _MINUTE,
                            row_id,
                            scope=(scope.wallet_public_id, scope.mode),
                        )
                    )
                await session.commit()

            assert await probe._count_witness_fill_events(repo, scope, {}) == 0

    asyncio.run(_seed_accruals_and_check_invalid())

    assert (
        probe.main(
            [
                "--wallet",
                scope.wallet_public_id,
                "--window-minutes",
                "4",
                "--db-url",
                url,
                "--max-accruals",
                "1",
            ]
        )
        == 3
    )
    assert (
        probe.main(
            [
                "--wallet",
                scope.wallet_public_id,
                "--window-minutes",
                "4",
                "--db-url",
                url,
                "--max-candles",
                "1",
            ]
        )
        == 3
    )

    async def _check_cut_moved() -> None:
        """Verify typed cap errors and ``cut_moved`` refusal when captured prefix expands."""
        async with idless._repository(url) as repo:
            with pytest.raises(probe.CheckpointResumeProbeError, match="^caps_exceeded:accruals$"):
                await probe.probe_with_repository(
                    repo,
                    probe.CheckpointResumeProbeConfig(
                        wallet_public_id=scope.wallet_public_id, window_minutes=4, max_accruals=1
                    ),
                )
            with pytest.raises(probe.CheckpointResumeProbeError, match="^caps_exceeded:candles$"):
                await probe.probe_with_repository(
                    repo,
                    probe.CheckpointResumeProbeConfig(
                        wallet_public_id=scope.wallet_public_id, window_minutes=4, max_candles=1
                    ),
                )

            orig_get_prefix = repo.get_pnl_timeline_execution_prefix

            async def _grown_cut_prefix(
                wallet_id: str,
                mode: str,
                as_of: datetime | None,
                *,
                capture_fill_witness_cut: bool = False,
            ) -> object:
                """Commit a new matched execution + fill via raw sqlite3 outside ORM guards."""
                with closing(sqlite3.connect(tmp_path / "caps.db")) as raw_conn:
                    raw_conn.row_factory = sqlite3.Row
                    ex_row = dict(
                        raw_conn.execute(
                            "SELECT * FROM executions WHERE scope_sequence = 2"
                        ).fetchone()
                    )
                    ex_row.pop("id")
                    ex_row.update(
                        scope_sequence=4,
                        public_id="00000000-0000-7000-8000-000000000904",
                        exec_id="exec-kraken-4",
                        trade_id="trade-kraken-4",
                        sequence_id=4,
                        timestamp=ex_row["timestamp"],
                    )
                    ex_cols = ", ".join(ex_row.keys())
                    ex_q = ", ".join("?" for _ in ex_row)
                    raw_conn.execute(
                        f"INSERT INTO executions ({ex_cols}) VALUES ({ex_q})",
                        tuple(ex_row.values()),
                    )
                    ve_row = dict(
                        raw_conn.execute(
                            "SELECT * FROM venue_events WHERE exec_id = 'exec-kraken-2'"
                        ).fetchone()
                    )
                    ve_row.pop("id")
                    ve_row.update(
                        public_id="00000000-0000-7000-8000-000000000914",
                        exec_id="exec-kraken-4",
                        trade_id="trade-kraken-4",
                        sequence_id=4,
                    )
                    ve_cols = ", ".join(ve_row.keys())
                    ve_q = ", ".join("?" for _ in ve_row)
                    raw_conn.execute(
                        f"INSERT INTO venue_events ({ve_cols}) VALUES ({ve_q})",
                        tuple(ve_row.values()),
                    )
                    raw_conn.commit()
                return await orig_get_prefix(
                    wallet_id, mode, as_of, capture_fill_witness_cut=capture_fill_witness_cut
                )

            monkeypatch.setattr(repo, "get_pnl_timeline_execution_prefix", _grown_cut_prefix)
            with pytest.raises(probe.CheckpointResumeProbeError, match="^cut_moved$") as cut_exc:
                await probe.probe_with_repository(
                    repo,
                    probe.CheckpointResumeProbeConfig(
                        wallet_public_id=scope.wallet_public_id, window_minutes=4, max_executions=3
                    ),
                )
            assert probe._classify_failure(cut_exc.value) == (3, "RESULT: REFUSED cut_moved")

    asyncio.run(_check_cut_moved())


def test_argparse_error_and_exit_redact_dsn_and_multiline_collapse(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Redact DSNs on mistyped CLI flags/parser exits and collapse multi-line DB errors.

    Given: Mistyped CLI arguments bearing credentials, direct parser ``exit`` calls, and
        multi-line / empty exception messages.
    When: Parsing arguments fails or ``_classify_failure`` / ``main`` formats errors.
    Then: Parser ``error`` and ``exit`` scrub credentials, multi-line DB errors collapse into a
        single terminal ``RESULT:`` line, and empty exceptions report their class name cleanly.

    Args:
        monkeypatch: Pytest fixture patching CLI execution results.
        capsys: Stdout/stderr capture inspecting parser stderr and final stdout line.
    """
    with pytest.raises(SystemExit) as parse_exit:
        probe.main(
            [
                "--wallet",
                prefix._WALLET,
                "--db-url",
                "postgresql+asyncpg://ro:argvsec@db.prod:5432/snapper",
                "--bogus=postgresql+asyncpg://ro:flagsec@db.prod:5432/snapper",
            ]
        )
    assert parse_exit.value.code == 2
    parse_io = capsys.readouterr()
    assert "argvsec" not in parse_io.err
    assert "flagsec" not in parse_io.err
    assert "postgresql+asyncpg://***:***@db.prod:5432/snapper" in parse_io.err

    parser = probe.build_parser()
    with pytest.raises(SystemExit) as msg_exit:
        parser.exit(2, "fatal postgresql+asyncpg://ro:exitsec@db.prod:5432/snapper\n")
    assert msg_exit.value.code == 2
    exit_io = capsys.readouterr()
    assert "exitsec" not in exit_io.err
    assert "postgresql+asyncpg://***:***@db.prod:5432/snapper" in exit_io.err

    with pytest.raises(SystemExit) as quiet_exit:
        parser.exit(0)
    assert quiet_exit.value.code == 0

    monkeypatch.setattr(
        probe,
        "probe_wallet_checkpoint_resume",
        AsyncMock(
            side_effect=SQLAlchemyError(
                "line1 postgresql+asyncpg://ro:linesec@db.prod:5432/snapper\n[SQL: SELECT 1]\nline3"
            )
        ),
    )
    assert (
        probe.main(
            [
                "--wallet",
                prefix._WALLET,
                "--db-url",
                "postgresql+asyncpg://ro:linesec@db.prod:5432/snapper",
            ]
        )
        == 2
    )
    multiline_io = capsys.readouterr()
    assert multiline_io.err == ""
    assert "linesec" not in multiline_io.out
    lines = multiline_io.out.strip().splitlines()
    assert len(lines) == 1
    assert (
        lines[-1]
        == "RESULT: FAIL line1 postgresql+asyncpg://***:***@db.prod:5432/snapper [SQL: SELECT 1] line3"
    )
    assert probe._classify_failure(ValueError()) == (2, "RESULT: FAIL ValueError")
    assert probe._classify_failure(probe.CheckpointResumeProbeError("timeout")) == (
        2,
        "RESULT: FAIL timeout",
    )


async def _provision_and_seed_pg_db(
    target_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> PortfolioPnlSampleScope:
    """Create schema, seed a 10-minute wallet with matched fills, and catch up in off mode."""
    monkeypatch.setattr(pnl_snapshotter, "MAX_CORRECTION_CHUNK_MINUTES", 2)
    monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "off")
    monkeypatch.delenv("PNL_FX_SHADOW_PINNING_ENABLED", raising=False)
    scope = await idless._seed(target_url, complete=False)
    async with idless._repository(target_url) as repo:
        await e2e._add_valuation_minutes(repo, 6, 10)
        async with repo.session() as session:
            for sequence, minute, side in ((2, 7, "buy"), (3, 9, "sell")):
                execution, fill_event = e2e._matched_execution(sequence, minute, side)
                fill_event.id = 100 + sequence
                session.add_all((execution, fill_event))
            await session.commit()
        as_of = samples._T0 + 12 * _MINUTE
        for _ in range(5):
            snap = PortfolioPnlSnapshotter(repo=repo, disabled=False, clock=lambda: as_of)
            await snap._tick_once()
    return scope


async def _pg_create_ro_db_and_seed(
    urls: tuple[str, str],
    creds: tuple[str, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> PortfolioPnlSampleScope:
    """Create temporary Postgres DB and dedicated read-only role, then seed scope."""
    async_base, target_async = urls
    db_name, role_name, role_pw = creds
    maintenance = SQLAlchemyRepository(async_base)
    try:
        async with maintenance.engine.connect() as conn:
            admin = await conn.execution_options(isolation_level="AUTOCOMMIT")
            await admin.exec_driver_sql(f'CREATE DATABASE "{db_name}"')
            await admin.exec_driver_sql(f"CREATE ROLE \"{role_name}\" LOGIN PASSWORD '{role_pw}'")
            await admin.exec_driver_sql(f'GRANT CONNECT ON DATABASE "{db_name}" TO "{role_name}"')
    finally:
        await maintenance.engine.dispose()
    scope = await _provision_and_seed_pg_db(target_async, monkeypatch)
    grant_repo = SQLAlchemyRepository(target_async)
    try:
        async with grant_repo.engine.begin() as conn:
            await conn.exec_driver_sql(f'GRANT USAGE ON SCHEMA public TO "{role_name}"')
            await conn.exec_driver_sql(
                f'GRANT SELECT ON ALL TABLES IN SCHEMA public TO "{role_name}"'
            )
            await conn.exec_driver_sql(
                f'ALTER ROLE "{role_name}" SET default_transaction_read_only = on'
            )
    finally:
        await grant_repo.engine.dispose()
    return scope


async def _pg_drop_ro_db(async_base: str, db_name: str, role_name: str) -> None:
    """Drop disposable Postgres database and temporary read-only role."""
    maintenance = SQLAlchemyRepository(async_base)
    try:
        async with maintenance.engine.connect() as conn:
            admin = await conn.execution_options(isolation_level="AUTOCOMMIT")
            await admin.exec_driver_sql(f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)')
            await admin.exec_driver_sql(f'DROP ROLE IF EXISTS "{role_name}"')
    finally:
        await maintenance.engine.dispose()


def test_opt_in_real_postgres_probe_e2e(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Run ``main()`` against an isolated real PostgreSQL database when ``PNL_TEST_POSTGRES_URL`` is set.

    Given: An opt-in ``PNL_TEST_POSTGRES_URL`` PostgreSQL server URL with isolated database
        and dedicated ``snapper_ro``-style read-only test role.
    When: Seeding a small caught-up wallet and running ``probe.main()`` against the DB.
    Then: Probe passes read-only preflight, ``_bound_pnl_correction_transaction``, and FX/mark
        replay, returning exit code 0 and ``RESULT: PASS`` with zero persisted checkpoints.

    Args:
        monkeypatch: Pytest fixture setting ``off`` catch-up mode for seeding.
        capsys: Stdout/stderr capture asserting ``RESULT: PASS``.
    """
    raw_pg = os.environ.get("PNL_TEST_POSTGRES_URL", "").strip()
    if not raw_pg:
        pytest.skip("PNL_TEST_POSTGRES_URL is not set; skipping opt-in Postgres probe e2e test")
    parsed = make_url(raw_pg)
    assert parsed.get_backend_name() == "postgresql"
    async_base = parsed.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)
    db_name = f"pnl_probe_e2e_{uuid4().hex}"
    role_name = f"pnl_probe_ro_{uuid4().hex[:12]}"
    role_pw = f"probe_{uuid4().hex}"
    target_async = parsed.set(drivername="postgresql+asyncpg", database=db_name).render_as_string(
        hide_password=False
    )
    ro_async = parsed.set(
        drivername="postgresql+asyncpg",
        database=db_name,
        username=role_name,
        password=role_pw,
    ).render_as_string(hide_password=False)
    try:
        scope = asyncio.run(
            _pg_create_ro_db_and_seed(
                (async_base, target_async), (db_name, role_name, role_pw), monkeypatch
            )
        )
        rc = probe.main(
            [
                "--wallet",
                scope.wallet_public_id,
                "--mode",
                scope.mode,
                "--valuation-ccy",
                scope.valuation_ccy,
                "--window-minutes",
                "4",
                "--db-url",
                ro_async,
            ]
        )
        assert rc == 0
        io = capsys.readouterr()
        assert role_pw not in io.out
        assert io.out.strip().splitlines()[-1] == "RESULT: PASS"
    finally:
        asyncio.run(_pg_drop_ro_db(async_base, db_name, role_name))


def test_rss_readers_statm_darwin_fallback_and_baseline_growth(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Measure current RSS on Linux statm, macOS ``ru_maxrss``, and unsupported fallbacks cleanly.

    Given: A fake ``/proc/self/statm`` file on Linux, ``darwin`` ``getrusage``, an unsupported OS,
        and large startup RSS baselines.
    When: ``_current_rss_mb`` and ``_check_rss_mb`` evaluate current memory and delta over start.
    Then: Linux computes ``resident_pages * SC_PAGE_SIZE`` in MiB, macOS converts bytes, fallback
        returns ``0.0``, high baselines with low growth pass, and excessive growth refuses.

    Args:
        tmp_path: Pytest isolated directory hosting a fake ``/proc/self/statm`` file.
        monkeypatch: Pytest fixture overriding platform, proc path, ``sysconf``, and ``getrusage``.
    """
    statm_file = tmp_path / "statm"
    statm_file.write_text("10000 2048 500 10 0 8000 0\n", encoding="utf-8")
    monkeypatch.setattr(probe, "_PROC_STATM_PATH", statm_file)
    monkeypatch.setattr(probe.os, "sysconf", lambda _name: 4096)
    monkeypatch.setattr(probe.sys, "platform", "linux")
    assert probe._current_rss_mb() == pytest.approx(8.0)

    statm_file.write_text("", encoding="utf-8")
    assert probe._current_rss_mb() == 0.0

    monkeypatch.setattr(probe.sys, "platform", "darwin")
    monkeypatch.setattr(
        probe.resource,
        "getrusage",
        lambda _who: SimpleNamespace(ru_maxrss=64 * 1024 * 1024),
    )
    assert probe._current_rss_mb() == pytest.approx(64.0)

    monkeypatch.setattr(probe.sys, "platform", "win32")
    assert probe._current_rss_mb() == 0.0

    monkeypatch.setattr(probe, "_current_rss_mb", lambda: 3600.0)
    probe._check_rss_mb(200, baseline_rss_mb=3500.0)
    monkeypatch.setattr(probe, "_current_rss_mb", lambda: 3400.0)
    probe._check_rss_mb(1, baseline_rss_mb=3500.0)
    monkeypatch.setattr(probe, "_current_rss_mb", lambda: 3800.0)
    with pytest.raises(probe.CheckpointResumeProbeError, match="max_rss_exceeded:300.0>200"):
        probe._check_rss_mb(200, baseline_rss_mb=3500.0)


def test_apply_address_space_limit_real_getrlimit_and_non_linux_skip(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exercise relative ``RLIMIT_AS`` calculation, hard-limit clamp/skip, and ``/proc`` skips.

    Given: Real ``resource.getrlimit(RLIMIT_AS)``, a fake ``/proc/self/status`` file holding
        ``VmSize``, a local ``setrlimit`` recorder overriding the autouse stub, erroring
        ``setrlimit``, and non-Linux or unreadable-``/proc`` inputs.
    When: Calling ``_apply_address_space_limit`` across all Linux, clamp/skip, and fallback paths.
    Then: Computed soft limit equals ``current_vmsize + max(max_rss_mb, 4 * max_rss_mb)`` MiB,
        never exceeds existing hard limits, skips when hard limit is below target or ``/proc``
        fails, handles ``setrlimit`` errors, and skips on non-Linux platforms.

    Args:
        tmp_path: Pytest isolated directory hosting a fake ``/proc/self/status`` file.
        monkeypatch: Pytest fixture patching platform, status path, and ``setrlimit``.
    """
    recorded: list[tuple[int, tuple[int, int]]] = []
    monkeypatch.setattr(
        probe.resource,
        "setrlimit",
        lambda kind, bounds: recorded.append((kind, bounds)),
    )

    vmsize_kb = 131_072
    vmsize_bytes = vmsize_kb * 1024
    status_file = tmp_path / "status"
    status_file.write_text(f"Name:\tpython\nVmSize:\t{vmsize_kb} kB\n", encoding="utf-8")
    monkeypatch.setattr(probe, "_PROC_STATUS_PATH", status_file)

    monkeypatch.setattr(probe.sys, "platform", "darwin")
    probe._apply_address_space_limit(256)
    monkeypatch.setattr(probe.sys, "platform", "win32")
    probe._apply_address_space_limit(256)
    assert recorded == []

    monkeypatch.setattr(probe.sys, "platform", "linux")
    _soft, real_hard = resource.getrlimit(resource.RLIMIT_AS)
    headroom_bytes = (probe.ADDRESS_SPACE_HEADROOM_FACTOR * 256) * 1024 * 1024
    desired_soft = vmsize_bytes + headroom_bytes
    probe._apply_address_space_limit(256)
    if real_hard == resource.RLIM_INFINITY or real_hard >= desired_soft:
        expected_real = (
            desired_soft if real_hard == resource.RLIM_INFINITY else min(desired_soft, real_hard)
        )
        assert recorded[-1] == (resource.RLIMIT_AS, (expected_real, real_hard))
    else:
        assert recorded == []

    recorded.clear()
    monkeypatch.setattr(probe.resource, "getrlimit", lambda _kind: (0, resource.RLIM_INFINITY))
    probe._apply_address_space_limit(256)
    high_finite_hard = desired_soft + 32 * 1024 * 1024
    monkeypatch.setattr(probe.resource, "getrlimit", lambda _kind: (0, high_finite_hard))
    probe._apply_address_space_limit(256)
    low_finite_hard = vmsize_bytes + 64 * 1024 * 1024
    monkeypatch.setattr(probe.resource, "getrlimit", lambda _kind: (0, low_finite_hard))
    probe._apply_address_space_limit(256)
    assert recorded == [
        (resource.RLIMIT_AS, (desired_soft, resource.RLIM_INFINITY)),
        (resource.RLIMIT_AS, (desired_soft, high_finite_hard)),
    ]

    recorded.clear()
    monkeypatch.setattr(probe.resource, "getrlimit", lambda _kind: (0, resource.RLIM_INFINITY))
    status_file.write_text("Name:\tpython\nVmRSS:\t4096 kB\n", encoding="utf-8")
    probe._apply_address_space_limit(256)
    monkeypatch.setattr(probe, "_PROC_STATUS_PATH", tmp_path / "missing_status")
    probe._apply_address_space_limit(256)
    assert recorded == []

    monkeypatch.setattr(probe, "_PROC_STATUS_PATH", status_file)
    status_file.write_text(f"VmSize:\t{vmsize_kb} kB\n", encoding="utf-8")
    monkeypatch.setattr(
        probe.resource,
        "setrlimit",
        lambda _kind, _bounds: (_ for _ in ()).throw(ValueError("not supported")),
    )
    probe._apply_address_space_limit(256)

    recorded.clear()
    monkeypatch.delattr(probe.resource, "RLIMIT_AS", raising=False)
    probe._apply_address_space_limit(256)
    assert recorded == []


def test_linux_real_rlimit_as_subprocess_smoke() -> None:
    """Apply unstubbed relative ``RLIMIT_AS`` in a Linux subprocess and allocate ``max_rss_mb/2``.

    Given: A Linux host running a clean child Python subprocess with no ``setrlimit`` stubs.
    When: Importing ``pnl_checkpoint_resume_probe``, applying ``_apply_address_space_limit(64)``,
        and allocating and touching a 32 MiB buffer (``max_rss_mb / 2``).
    Then: The child applies the relative virtual limit without dying on library mappings and exits 0.
    """
    if not sys.platform.startswith("linux"):
        pytest.skip("Linux-only real RLIMIT_AS subprocess smoke test")
    repo_root = Path(probe.__file__).resolve().parents[1]
    env = dict(os.environ)
    env["PYTHONPATH"] = str(repo_root / "src")
    child_code = (
        "import resource; "
        "from scripts import pnl_checkpoint_resume_probe as probe; "
        "max_rss_mb = 64; "
        "vmsize = probe._read_linux_vmsize_bytes(); "
        "assert vmsize is not None and vmsize > 0; "
        "_, hard_before = resource.getrlimit(resource.RLIMIT_AS); "
        "expected = vmsize + max(max_rss_mb, probe.ADDRESS_SPACE_HEADROOM_FACTOR * max_rss_mb) * 1024 * 1024; "
        "probe._apply_address_space_limit(max_rss_mb); "
        "soft_after, hard_after = resource.getrlimit(resource.RLIMIT_AS); "
        "assert hard_after == hard_before; "
        "assert (soft_after == expected) if (hard_before == resource.RLIM_INFINITY or hard_before >= expected) else True; "
        "buf = bytearray((max_rss_mb // 2) * 1024 * 1024); "
        "mv = memoryview(buf); "
        "mv[::4096] = b'\\x01' * len(mv[::4096]); "
        "assert len(buf) == (max_rss_mb // 2) * 1024 * 1024"
    )
    proc = subprocess.run(
        [sys.executable, "-c", child_code],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert proc.returncode == 0, proc.stderr


async def test_probe_with_repository_timeout_raises_timeout_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Translate ``TimeoutError`` inside ``probe_with_repository`` to ``CheckpointResumeProbeError``.

    Given: A SQLite repository and an ``_execute_probe_pipeline`` coroutine raising ``TimeoutError``.
    When: ``probe_with_repository`` executes inside ``asyncio.timeout``.
    Then: ``CheckpointResumeProbeError('timeout')`` is raised cleanly with guards removed.

    Args:
        monkeypatch: Pytest fixture replacing ``_execute_probe_pipeline``.
    """
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    monkeypatch.setattr(
        probe,
        "_execute_probe_pipeline",
        AsyncMock(side_effect=TimeoutError("deadline exceeded")),
    )
    try:
        with pytest.raises(probe.CheckpointResumeProbeError, match="^timeout$"):
            await probe.probe_with_repository(
                repo,
                probe.CheckpointResumeProbeConfig(
                    wallet_public_id=prefix._WALLET,
                    max_runtime_seconds=1.0,
                ),
            )
    finally:
        await repo.engine.dispose()


def test_mark_series_sort_tolerates_null_historical_quote() -> None:
    """Sort historical mark series whose quote metadata is partly null.

    Given: Two references to one native symbol, exchange and base whose
        historical quote is ``None`` in one version and ``"USD"`` in the other.
    When: The series set is sorted with the probe's null-safe key.
    Then: Sorting does not raise and the concrete quote orders before the null one.
    """
    series = {
        ("XBTUSD", "kraken", "BTC", None),
        ("XBTUSD", "kraken", "BTC", "USD"),
    }
    with pytest.raises(TypeError):
        sorted(series)
    assert sorted(series, key=probe._null_safe_series_key) == [
        ("XBTUSD", "kraken", "BTC", "USD"),
        ("XBTUSD", "kraken", "BTC", None),
    ]


def test_dsn_redaction_covers_slash_in_password() -> None:
    """Redact a DSN password that contains a raw slash.

    Given: A DSN whose password contains ``/``, which SQLAlchemy accepts unescaped.
    When: ``_redact_secrets`` scrubs it, as argparse errors and diagnostics do.
    Then: No fragment of the password survives in the output.
    """
    slashed = probe._redact_secrets("postgresql+asyncpg://alice:pa/ss@10.0.1.5:5432/prod")
    assert "pa/ss" not in slashed
    assert "ss@" not in slashed
    assert slashed == "postgresql+asyncpg://***:***@10.0.1.5:5432/prod"
    sqlite_path = "sqlite+aiosqlite:////srv/snap@shot/probe.db"
    assert probe._redact_secrets(sqlite_path) == sqlite_path
