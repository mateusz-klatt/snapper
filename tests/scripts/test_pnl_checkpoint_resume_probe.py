"""Verify read-only guards and bit-exact resume comparison in the operator probe script."""

import asyncio
import os
import subprocess
import sys
from collections.abc import Mapping
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text
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
from tests.application.portfolio import test_pnl_snapshotter_checkpoint_e2e as e2e
from tests.application.portfolio import test_pnl_snapshotter_lower_id_rewind as idless
from tests.data import test_pnl_timeline_execution_prefix_repository as prefix
from tests.data import test_pnl_timeline_sample_repository as samples

_MINUTE = timedelta(minutes=1)


class _RecordingCursor:
    """Minimal DBAPI cursor recording executed read-only session setup SQL."""

    def __init__(self) -> None:
        """Initialize an empty SQL command log and close state."""
        self.commands: list[str] = []
        self.closed = False

    def execute(self, statement: str) -> None:
        """Record one executed SQL statement."""
        self.commands.append(statement)

    def close(self) -> None:
        """Mark the cursor closed."""
        self.closed = True


class _RecordingConnection:
    """Minimal DBAPI connection yielding a recording cursor."""

    def __init__(self) -> None:
        """Create one reusable recording cursor."""
        self.cursor_obj = _RecordingCursor()

    def cursor(self) -> _RecordingCursor:
        """Return the recording cursor."""
        return self.cursor_obj


class _ScalarResult:
    """Minimal SQLAlchemy scalar result stub for preflight dialect tests."""

    def __init__(self, value: object) -> None:
        """Store the single preflight query scalar value."""
        self._value = value

    def scalar_one(self) -> object:
        """Return the configured scalar value."""
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
    assert captured.out.strip().splitlines()[-1] == "RESULT: PASS"

    repo_root = Path(probe.__file__).resolve().parents[1]
    env = dict(os.environ)
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
        "WITH x AS (SELECT * FROM orders FOR UPDATE) SELECT * FROM x",
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
    ]
    assert pg_conn.cursor_obj.closed is True

    with pytest.raises(probe.CheckpointResumeProbeError, match="invalid_statement_timeout_ms:0"):
        probe._enforce_connection_read_only(_RecordingConnection(), "postgresql", 0)

    with pytest.raises(probe.CheckpointResumeProbeError, match="unsupported_dialect:mysql"):
        probe._enforce_connection_read_only(_RecordingConnection(), "mysql")

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
    """Verify PostgreSQL read-only and timeout preflight accepts valid sessions and rejects others.

    Given: Simulated PostgreSQL sessions with valid or mismatched read-only/timeout settings
        and an unsupported MySQL dialect.
    When: ``_preflight_read_only`` inspects each repository under read-only guards.
    Then: Valid PostgreSQL read-only + timeouts pass, while writable or mismatched timeouts refuse.
    """
    policy = PnlSeriesReadPolicy(allow_anchor_creation=False)
    pg_ok = _DialectProbeRepo(
        "postgresql",
        {
            "SHOW transaction_read_only": "on",
            "SHOW statement_timeout": "30s",
            "SHOW lock_timeout": "1s",
        },
    )
    try:
        with probe._read_only_guards(pg_ok):
            await probe._preflight_read_only(pg_ok, policy)
    finally:
        await pg_ok.engine.dispose()

    bad_cases: tuple[tuple[str, dict[str, object], str], ...] = (
        (
            "postgresql",
            {
                "SHOW transaction_read_only": "off",
                "SHOW statement_timeout": "30s",
                "SHOW lock_timeout": "1s",
            },
            "database_write_would_commit",
        ),
        (
            "postgresql",
            {
                "SHOW transaction_read_only": "on",
                "SHOW statement_timeout": "10s",
                "SHOW lock_timeout": "1s",
            },
            "database_statement_timeout_mismatch",
        ),
        (
            "postgresql",
            {
                "SHOW transaction_read_only": "on",
                "SHOW statement_timeout": "30s",
                "SHOW lock_timeout": "5s",
            },
            "database_lock_timeout_mismatch",
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


async def test_probe_enforces_max_runtime_seconds_and_validates_bounds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Enforce ``statement_timeout_ms`` and ``max_runtime_seconds`` bounds on every probe run.

    Given: Out-of-bounds timeout configs and a slow series call exceeding ``max_runtime_seconds``.
    When: ``probe_with_repository`` evaluates the config or runs under ``asyncio.timeout``.
    Then: Invalid bounds fail immediately and over-budget execution raises
        ``probe_max_runtime_exceeded``.

    Args:
        tmp_path: Pytest isolated directory for temporary SQLite fixture.
        monkeypatch: Pytest fixture injecting a slow series delay.
    """
    url, scope = await _seed_and_catchup(tmp_path / "timeout.db", monkeypatch)
    async with idless._repository(url) as repo:
        with pytest.raises(
            probe.CheckpointResumeProbeError, match="invalid_statement_timeout_ms:0"
        ):
            await probe.probe_with_repository(
                repo,
                probe.CheckpointResumeProbeConfig(
                    wallet_public_id=scope.wallet_public_id,
                    statement_timeout_ms=0,
                ),
            )
        with pytest.raises(probe.CheckpointResumeProbeError, match="invalid_max_runtime_seconds:0"):
            await probe.probe_with_repository(
                repo,
                probe.CheckpointResumeProbeConfig(
                    wallet_public_id=scope.wallet_public_id,
                    max_runtime_seconds=0.0,
                ),
            )

        async def slow_series(*_args: object, **_kwargs: object) -> PnlWalletSeriesResult:
            """Simulate a stalled database query triggering the overall runtime ceiling."""
            await asyncio.sleep(0.2)
            return _sample_result(())

        monkeypatch.setattr(probe, "build_wallet_pnl_series", slow_series)
        with pytest.raises(
            probe.CheckpointResumeProbeError,
            match="probe_max_runtime_exceeded:0.02",
        ):
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
        with pytest.raises(probe.CheckpointResumeProbeError, match="missing_active_anchor"):
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
        with pytest.raises(probe.CheckpointResumeProbeError, match="missing_correction_frontier"):
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


def test_canonical_sequence_comparison_and_cli_exit_codes(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Cover NaN/signed-zero canonicalization, mismatch detection, and CLI status codes.

    Given: Scalar/float/datetime/dataclass inputs and cold vs resumed series pairs.
    When: ``_canonical``, ``_compare_series``, and ``main`` evaluate equality, NaN, -0.0,
        rate-source divergence, mismatches, and raised errors.
    Then: Bit-level differences are pinpointed, unsupported objects raise ``TypeError``, and
        ``main`` returns 1 on mismatch and 2 on probe error.

    Args:
        monkeypatch: Pytest fixture patching CLI coroutine results.
        capsys: Stdout/stderr capture verifying error and mismatch output.
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
    assert probe.main(["--wallet", prefix._WALLET]) == 2
    refusal_io = capsys.readouterr()
    assert "checkpoint resume probe refused:" in refusal_io.err
    assert (
        refusal_io.out.strip().splitlines()[-1]
        == "RESULT: PASS(refused:checkpoint_verify_fallback:checkpoint_late_execution)"
    )

    monkeypatch.setattr(
        probe,
        "probe_wallet_checkpoint_resume",
        AsyncMock(side_effect=probe.CheckpointResumeProbeError("capture_fill_gap")),
    )
    assert probe.main(["--wallet", prefix._WALLET]) == 2
    assert (
        capsys.readouterr().out.strip().splitlines()[-1] == "RESULT: PASS(refused:capture_fill_gap)"
    )

    monkeypatch.setattr(
        probe,
        "probe_wallet_checkpoint_resume",
        AsyncMock(side_effect=probe.CheckpointResumeProbeError("database_write_would_commit")),
    )
    assert probe.main(["--wallet", prefix._WALLET]) == 2
    fail_io = capsys.readouterr()
    assert "checkpoint resume probe refused: database_write_would_commit" in fail_io.err
    assert fail_io.out.strip().splitlines()[-1] == "RESULT: FAIL database_write_would_commit"
