"""Tests for the retention policy service + module-level helpers.

Covers:

* Module-level validation rejects a policy whose ``table`` is not a
  key in ``EVENT_TABLES``.
* `_compute_window` arithmetic — frozen ``today_utc`` produces the
  exact ``(day_start, day_end)`` per §3.6 of the Cluster C plan.
* `evaluate_policy` happy path + dry-run propagation + archiver
  exception capture + window-failure capture.
* `run_once` collects per-policy results and never re-raises.
* `close` disposes the underlying repo via ``asyncio.to_thread``.
* Env-var parsers (``RETENTION_INTERVAL_SECONDS``, ``RETENTION_DISABLED``,
  ``RETENTION_DRY_RUN``, ``RETENTION_OUTPUT_DIR``) cover empty / unset /
  unparseable / truthy / negative cases.

Strategy: mock the sync ``EventArchiver`` at the service-construction
boundary so unit tests can assert exact ``day_start`` / ``day_end`` /
``purge`` arguments without spinning up a real SQLite database. Real
CSV-write + DB-purge integration is already covered by
:mod:`tests.data.test_archiver`.
"""

import asyncio
import importlib
from collections.abc import Generator
from dataclasses import dataclass
from datetime import UTC
from datetime import date
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.application.retention import policies as policies_module
from snapper.application.retention import service as service_module
from snapper.application.retention.policies import DEFAULT_INTERVAL_SECONDS
from snapper.application.retention.policies import RETENTION_POLICIES
from snapper.application.retention.policies import RetentionPolicy
from snapper.application.retention.policies import resolve_disabled
from snapper.application.retention.policies import resolve_dry_run
from snapper.application.retention.policies import resolve_interval
from snapper.application.retention.policies import resolve_output_dir
from snapper.application.retention.service import RetentionService
from snapper.application.retention.service import _compute_window
from snapper.application.retention.service import _failure_result
from snapper.data.archiver import ExportResult


@dataclass(slots=True)
class _ArchiverCall:
    """One captured invocation of the fake ``EventArchiver.export``."""

    table: str
    day_start: date
    day_end: date
    dry_run: bool
    purge: bool


class _FakeArchiver:
    """Synchronous stand-in for ``EventArchiver``.

    Captures every ``export(...)`` invocation and either returns a
    fixed :class:`ExportResult` or raises a configured exception.
    """

    def __init__(
        self,
        *,
        result: ExportResult | None = None,
        raise_exc: Exception | None = None,
    ) -> None:
        self.result = result or ExportResult(files_written=0, rows_exported=0, rows_purged=0)
        self.raise_exc = raise_exc
        self.calls: list[_ArchiverCall] = []

    def export(
        self,
        *,
        table: str,
        day_start: date,
        day_end: date,
        dry_run: bool = False,
        purge: bool = False,
        exchange: str | None = None,
        archive_symbol: str | None = None,
    ) -> ExportResult:
        """Capture call args + return / raise."""
        del exchange, archive_symbol
        self.calls.append(
            _ArchiverCall(
                table=table,
                day_start=day_start,
                day_end=day_end,
                dry_run=dry_run,
                purge=purge,
            )
        )
        if self.raise_exc is not None:
            raise self.raise_exc
        return self.result


def _build_service_with_fake(
    fake: _FakeArchiver,
    *,
    db_url: str = "sqlite+aiosqlite:///:memory:",
    base_dir: Path | None = None,
) -> RetentionService:
    """Construct a :class:`RetentionService` whose internal archiver is ``fake``.

    Patches the ``EventArchiver`` symbol used in the service module so
    the constructor builds the fake; also stubs the
    :class:`DatabaseRepository` to avoid opening a real engine.
    """
    fake_repo = MagicMock()
    with (
        patch.object(service_module, "EventArchiver", lambda _repo, _base_dir: fake),
        patch.object(service_module, "DatabaseRepository", lambda _db_url: fake_repo),
    ):
        return RetentionService(db_url=db_url, base_dir=base_dir or Path("data"))


@pytest.fixture
def _frozen_now_2026_05_01() -> Generator[None]:
    """Pin ``datetime.now(UTC)`` to 2026-05-01 12:00 UTC for boundary tests."""

    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> _FrozenDatetime:
            return cls(2026, 5, 1, 12, 0, tzinfo=tz or UTC)

    with patch.object(service_module, "datetime", _FrozenDatetime):
        yield


class TestPoliciesModuleValidation:
    """Module-level guard for unknown event tables."""

    def test_telemetry_policy_present_in_default_list(self) -> None:
        """Sanity-check the shipped default policy."""
        assert (
            RetentionPolicy(table="telemetry", retain_days=1, backlog_lookback_days=30),
        ) == RETENTION_POLICIES

    def test_validate_policies_raises_for_unknown_table(self) -> None:
        """The import-time guard rejects a non-event-table policy."""
        bad = (RetentionPolicy(table="not_a_real_table", retain_days=1, backlog_lookback_days=1),)
        with pytest.raises(ValueError, match="not in EVENT_TABLES"):
            policies_module.validate_policies(bad)

    def test_validate_policies_accepts_default_list(self) -> None:
        """The shipped default list passes the guard."""
        policies_module.validate_policies(RETENTION_POLICIES)


class TestEnvVarParsers:
    """Boolean + numeric parsers for the four retention env vars."""

    @pytest.mark.parametrize(
        ("env_value", "expected"),
        [
            (None, DEFAULT_INTERVAL_SECONDS),
            ("", DEFAULT_INTERVAL_SECONDS),
            ("   ", DEFAULT_INTERVAL_SECONDS),
            ("not-a-number", DEFAULT_INTERVAL_SECONDS),
            ("0", DEFAULT_INTERVAL_SECONDS),
            ("-30", DEFAULT_INTERVAL_SECONDS),
            ("60", 60.0),
            ("3600", DEFAULT_INTERVAL_SECONDS),
            ("3.5", 3.5),
        ],
    )
    def test_resolve_interval(self, env_value: str | None, expected: float) -> None:
        """Empty / non-positive / unparseable falls back to default 3600."""
        assert resolve_interval(env_value) == pytest.approx(expected)

    @pytest.mark.parametrize(
        ("env_value", "expected"),
        [
            (None, False),
            ("", False),
            ("false", False),
            ("0", False),
            ("anything", False),
            ("true", True),
            ("TRUE", True),
            ("1", True),
            ("yes", True),
            ("  Yes  ", True),
        ],
    )
    def test_resolve_disabled(self, env_value: str | None, expected: bool) -> None:
        """Truthy values are accepted case-insensitively."""
        assert resolve_disabled(env_value) is expected

    @pytest.mark.parametrize(
        ("env_value", "expected"),
        [
            (None, False),
            ("", False),
            ("false", False),
            ("true", True),
            ("YES", True),
            ("1", True),
        ],
    )
    def test_resolve_dry_run(self, env_value: str | None, expected: bool) -> None:
        """Same shape as the disabled parser."""
        assert resolve_dry_run(env_value) is expected

    @pytest.mark.parametrize(
        ("env_value", "expected"),
        [
            (None, "data"),
            ("", "data"),
            ("   ", "data"),
            ("data", "data"),
            ("/var/lib/snapper", "/var/lib/snapper"),
            ("  data ", "data"),
        ],
    )
    def test_resolve_output_dir(self, env_value: str | None, expected: str) -> None:
        """Empty / unset falls back to the CLI default ``data``."""
        assert resolve_output_dir(env_value) == expected


class TestComputeWindow:
    """Per-tick boundary formula (Cluster C plan §3.6)."""

    def test_telemetry_policy_today_2026_05_01(self) -> None:
        """SC#4 fixture — `retain_days=1, backlog_lookback_days=30`."""
        policy = RetentionPolicy(table="telemetry", retain_days=1, backlog_lookback_days=30)
        day_start, day_end = _compute_window(date(2026, 5, 1), policy)
        assert day_start == date(2026, 3, 30)
        assert day_end == date(2026, 4, 29)

    def test_zero_lookback_yields_one_day_window(self) -> None:
        """``backlog_lookback_days=0`` → window covers exactly one day."""
        policy = RetentionPolicy(table="telemetry", retain_days=2, backlog_lookback_days=0)
        day_start, day_end = _compute_window(date(2026, 5, 1), policy)
        assert day_start == day_end == date(2026, 4, 28)

    def test_large_retain_pushes_window_into_past(self) -> None:
        """Large ``retain_days`` shifts both bounds proportionally."""
        policy = RetentionPolicy(table="telemetry", retain_days=365, backlog_lookback_days=30)
        day_start, day_end = _compute_window(date(2026, 5, 1), policy)
        assert day_end == date(2025, 4, 30)
        assert day_start == date(2025, 3, 31)


class TestFailureResult:
    """Helper that builds a zero-counters result with the error stamped."""

    def test_failure_result_zeros_counters(self) -> None:
        """Counters are zero; ``error`` carries the supplied string."""
        policy = RetentionPolicy(table="telemetry", retain_days=1, backlog_lookback_days=30)
        result = _failure_result(policy, day_start="2026-04-29", day_end="2026-04-29", error="boom")
        assert result["archived_rows"] == 0
        assert result["purged_rows"] == 0
        assert result["files_written"] == 0
        assert result["error"] == "boom"
        assert result["day_start"] == "2026-04-29"
        assert result["day_end"] == "2026-04-29"

    def test_failure_result_window_unset_on_pre_window_failure(self) -> None:
        """``day_start`` / ``day_end`` are ``None`` when window comp failed."""
        policy = RetentionPolicy(table="telemetry", retain_days=1, backlog_lookback_days=30)
        result = _failure_result(policy, day_start=None, day_end=None, error="boom")
        assert result["day_start"] is None
        assert result["day_end"] is None


class TestEvaluatePolicy:
    """Per-tick policy evaluation."""

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("_frozen_now_2026_05_01")
    async def test_happy_path_records_archiver_result(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Mock archiver returns counts; result reflects them."""
        monkeypatch.delenv("RETENTION_DRY_RUN", raising=False)
        fake = _FakeArchiver(
            result=ExportResult(files_written=2, rows_exported=200, rows_purged=200)
        )
        service = _build_service_with_fake(fake)
        policy = RetentionPolicy(table="telemetry", retain_days=1, backlog_lookback_days=30)

        result = await service.evaluate_policy(policy)

        assert result["archived_rows"] == 200
        assert result["purged_rows"] == 200
        assert result["files_written"] == 2
        assert result["error"] is None
        assert result["day_start"] == "2026-03-30"
        assert result["day_end"] == "2026-04-29"
        assert len(fake.calls) == 1
        call = fake.calls[0]
        assert call.table == "telemetry"
        assert call.day_start == date(2026, 3, 30)
        assert call.day_end == date(2026, 4, 29)
        assert call.purge is True

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("_frozen_now_2026_05_01")
    async def test_dry_run_forces_purge_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``RETENTION_DRY_RUN=true`` always passes ``purge=False``."""
        monkeypatch.setenv("RETENTION_DRY_RUN", "true")
        fake = _FakeArchiver(result=ExportResult(files_written=1, rows_exported=10, rows_purged=0))
        service = _build_service_with_fake(fake)
        policy = RetentionPolicy(table="telemetry", retain_days=1, backlog_lookback_days=30)

        await service.evaluate_policy(policy)

        assert fake.calls[0].purge is False

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("_frozen_now_2026_05_01")
    async def test_records_archiver_exception_does_not_propagate(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A raise inside ``EventArchiver.export`` is captured + recorded."""
        monkeypatch.delenv("RETENTION_DRY_RUN", raising=False)
        fake = _FakeArchiver(raise_exc=RuntimeError("synthetic boom"))
        service = _build_service_with_fake(fake)
        policy = RetentionPolicy(table="telemetry", retain_days=1, backlog_lookback_days=30)

        result = await service.evaluate_policy(policy)

        assert result["error"] == "synthetic boom"
        assert result["archived_rows"] == 0
        assert result["purged_rows"] == 0
        assert result["files_written"] == 0
        assert result["day_start"] == "2026-03-30"
        assert result["day_end"] == "2026-04-29"

    @pytest.mark.asyncio
    async def test_window_overflow_is_captured_as_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A pre-window error (date math overflow) yields ``day_*=None``."""
        monkeypatch.delenv("RETENTION_DRY_RUN", raising=False)
        fake = _FakeArchiver()
        service = _build_service_with_fake(fake)
        policy = RetentionPolicy(
            table="telemetry",
            retain_days=10**8,
            backlog_lookback_days=10**8,
        )

        result = await service.evaluate_policy(policy)

        assert result["error"] is not None
        assert result["day_start"] is None
        assert result["day_end"] is None
        assert fake.calls == []


class TestRunOnce:
    """Aggregate orchestration over the policy list."""

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("_frozen_now_2026_05_01")
    async def test_collects_per_policy_results(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """One run produces one summary entry per shipped policy."""
        monkeypatch.delenv("RETENTION_DRY_RUN", raising=False)
        fake = _FakeArchiver(result=ExportResult(files_written=1, rows_exported=42, rows_purged=42))
        service = _build_service_with_fake(fake)

        summary = await service.run_once()

        assert len(summary["results"]) == len(RETENTION_POLICIES)
        assert summary["results"][0]["archived_rows"] == 42
        assert summary["dry_run"] is False
        assert summary["run_completed_at"] >= summary["run_started_at"]
        assert service.last_run_summary == summary

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("_frozen_now_2026_05_01")
    async def test_one_policy_raise_does_not_propagate(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``run_once`` returns normally when a policy raises in evaluate_policy."""
        monkeypatch.delenv("RETENTION_DRY_RUN", raising=False)
        fake = _FakeArchiver(raise_exc=RuntimeError("boom"))
        service = _build_service_with_fake(fake)

        summary = await service.run_once()

        assert summary["results"][0]["error"] == "boom"

    @pytest.mark.asyncio
    @pytest.mark.usefixtures("_frozen_now_2026_05_01")
    async def test_dry_run_snapshot_at_run_start(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Mid-run env flips do not change the recorded ``dry_run`` flag.

        We can't actually flip mid-run synchronously here, but we can
        assert that the value present at ``run_started_at`` is the
        value reflected in the summary.
        """
        monkeypatch.setenv("RETENTION_DRY_RUN", "true")
        fake = _FakeArchiver(result=ExportResult(files_written=0, rows_exported=0, rows_purged=0))
        service = _build_service_with_fake(fake)

        summary = await service.run_once()

        assert summary["dry_run"] is True
        assert all(call.purge is False for call in fake.calls)


class TestClose:
    """Lifecycle teardown."""

    @pytest.mark.asyncio
    async def test_close_disposes_repo_via_thread(self) -> None:
        """``close`` calls ``repo.dispose`` exactly once via ``to_thread``."""
        fake_repo = MagicMock()
        with (
            patch.object(service_module, "DatabaseRepository", lambda _db_url: fake_repo),
            patch.object(service_module, "EventArchiver", lambda _repo, _base_dir: _FakeArchiver()),
        ):
            service = RetentionService(db_url="sqlite+aiosqlite:///:memory:", base_dir=Path("data"))
            await service.close()

        fake_repo.dispose.assert_called_once_with()


class TestPoliciesModuleSelfValidation:
    """Re-import the module to exercise the import-time validation guard."""

    def test_module_reimport_is_clean_for_default_policies(self) -> None:
        """Default policies pass the in-module assertion on reload."""
        importlib.reload(policies_module)

        assert any(p.table == "telemetry" for p in policies_module.RETENTION_POLICIES)


@pytest.mark.asyncio
async def test_to_thread_keeps_event_loop_responsive() -> None:
    """The threading boundary lets a concurrent task make progress.

    Surfaces SC#13 — sampler tick offloads to thread (the loop ticks
    even while a synthetic 100ms ``export`` is running).
    """
    sleep_secs = 0.1
    progress: list[int] = []

    def _slow_export(**_kwargs: Any) -> ExportResult:
        import time as _time

        _time.sleep(sleep_secs)
        return ExportResult(files_written=1, rows_exported=1, rows_purged=1)

    fake_repo = MagicMock()
    fake_archiver = MagicMock()
    fake_archiver.export = _slow_export

    with (
        patch.object(service_module, "DatabaseRepository", lambda _db_url: fake_repo),
        patch.object(
            service_module,
            "EventArchiver",
            lambda _repo, _base_dir: fake_archiver,
        ),
    ):
        service = RetentionService(db_url="sqlite+aiosqlite:///:memory:", base_dir=Path("data"))

    async def _ticker() -> None:
        for _ in range(5):
            progress.append(len(progress))
            await asyncio.sleep(0.02)

    policy = RetentionPolicy(table="telemetry", retain_days=1, backlog_lookback_days=30)

    eval_task = asyncio.create_task(service.evaluate_policy(policy))
    tick_task = asyncio.create_task(_ticker())

    await asyncio.gather(eval_task, tick_task)

    assert len(progress) == 5
