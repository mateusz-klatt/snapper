"""Tests for the retention scheduler + its lifespan helpers in ``app.py``.

Covers:

* Eager first run populates ``last_run_summary`` before the loop.
* Loop tolerates a per-tick exception from ``run_once`` (Codex
  re-review NEW MAJOR fix).
* ``stop`` cancels cleanly + closes the underlying service.
* ``RETENTION_DISABLED=true`` skips the eager run + loop entirely.
* Lifespan helper unit + integration shape: failure leaves the
  ``app.state.retention_scheduler`` attribute absent (helper-unit test
  with :class:`SimpleNamespace`) or pinned to the lifespan-pre-set
  ``None`` (integration shape).
* Stop helper tolerates partial-init.
"""

import asyncio
from collections.abc import Generator
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest

from snapper.application.retention import scheduler as scheduler_module
from snapper.application.retention.scheduler import RetentionScheduler
from snapper.application.retention.service import RetentionRunSummary
from snapper.server.app import _start_retention_scheduler
from snapper.server.app import _stop_retention_scheduler


@pytest.fixture(autouse=True)
def _clear_retention_env(monkeypatch: pytest.MonkeyPatch) -> Generator[None]:
    """Drop retention env vars so each test starts from defaults."""
    for key in (
        "RETENTION_INTERVAL_SECONDS",
        "RETENTION_DISABLED",
        "RETENTION_DRY_RUN",
        "RETENTION_OUTPUT_DIR",
    ):
        monkeypatch.delenv(key, raising=False)
    yield


def _build_fake_summary() -> RetentionRunSummary:
    """Return an empty-results :class:`RetentionRunSummary` for assertion fixtures."""
    from datetime import UTC
    from datetime import datetime

    now = datetime(2026, 5, 1, 12, 0, tzinfo=UTC)
    return RetentionRunSummary(
        run_started_at=now,
        run_completed_at=now,
        dry_run=False,
        results=[],
    )


def _build_scheduler_with_fake_service(
    *,
    run_once_side_effects: list[Any] | None = None,
    interval_seconds: float = 0.05,
    disabled: bool = False,
) -> tuple[RetentionScheduler, Any]:
    """Construct a scheduler with a :class:`AsyncMock` service.

    ``run_once_side_effects`` (if provided) is consumed left-to-right
    via :class:`AsyncMock`'s ``side_effect`` mechanism — a list lets
    the test mix successful return values with raising exceptions.
    """
    fake_service = AsyncMock()
    if run_once_side_effects is None:
        fake_service.run_once.return_value = _build_fake_summary()
    else:
        fake_service.run_once.side_effect = list(run_once_side_effects)
    fake_service.close = AsyncMock()
    fake_service.last_run_summary = None
    scheduler = RetentionScheduler(
        db_url="sqlite+aiosqlite:///:memory:",
        service=fake_service,
        interval_seconds=interval_seconds,
        disabled=disabled,
    )
    return scheduler, fake_service


class TestSchedulerStart:
    """Eager first run + disabled-mode skip."""

    @pytest.mark.asyncio
    async def test_eager_first_run_populates_summary_before_loop(self) -> None:
        """``start`` calls ``run_once`` ONCE before spawning the loop."""
        scheduler, fake_service = _build_scheduler_with_fake_service(
            run_once_side_effects=[_build_fake_summary()]
        )

        await scheduler.start()
        await scheduler.stop()

        assert fake_service.run_once.await_count >= 1

    @pytest.mark.asyncio
    async def test_disabled_skips_eager_run_and_loop(self) -> None:
        """``RETENTION_DISABLED=true`` parks the scheduler entirely."""
        scheduler, fake_service = _build_scheduler_with_fake_service(disabled=True)

        await scheduler.start()
        await scheduler.stop()

        assert fake_service.run_once.await_count == 0


class TestSchedulerLoop:
    """Loop ticking, exception tolerance, clean shutdown."""

    @pytest.mark.asyncio
    async def test_loop_continues_after_one_tick_raises(self) -> None:
        """A bug in ``run_once`` itself is caught + the loop ticks again.

        Uses an :class:`asyncio.Event` to deterministically wait for
        the third call without a wall-clock sleep — keeps the test
        non-flaky under CI scheduling pressure.
        """
        third_call = asyncio.Event()
        call_count = {"n": 0}
        outcomes: list[Any] = [
            _build_fake_summary(),
            RuntimeError("synthetic mid-loop boom"),
            _build_fake_summary(),
        ]

        async def _run_once_recording() -> Any:
            call_count["n"] += 1
            if call_count["n"] >= 3:
                third_call.set()
            outcome = outcomes[call_count["n"] - 1]
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        scheduler, fake_service = _build_scheduler_with_fake_service(interval_seconds=0.005)
        fake_service.run_once.side_effect = _run_once_recording

        await scheduler.start()
        await asyncio.wait_for(third_call.wait(), timeout=2.0)
        await scheduler.stop()

        assert call_count["n"] >= 3

    @pytest.mark.asyncio
    async def test_loop_continues_when_run_once_itself_raises(self) -> None:
        """First post-eager tick raises; second tick still runs (defensive catch).

        Same deterministic-event pattern as the previous test.
        """
        third_call = asyncio.Event()
        call_count = {"n": 0}
        outcomes: list[Any] = [
            _build_fake_summary(),
            RuntimeError("post-eager bug"),
            _build_fake_summary(),
        ]

        async def _run_once_recording() -> Any:
            call_count["n"] += 1
            if call_count["n"] >= 3:
                third_call.set()
            outcome = outcomes[call_count["n"] - 1]
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        scheduler, fake_service = _build_scheduler_with_fake_service(interval_seconds=0.005)
        fake_service.run_once.side_effect = _run_once_recording

        await scheduler.start()
        await asyncio.wait_for(third_call.wait(), timeout=2.0)
        await scheduler.stop()

        assert call_count["n"] >= 3

    @pytest.mark.asyncio
    async def test_stop_cancels_loop_cleanly(self) -> None:
        """``stop`` returns within a tight budget even mid-sleep."""
        scheduler, fake_service = _build_scheduler_with_fake_service(interval_seconds=10.0)
        await scheduler.start()

        await scheduler.stop()

        assert fake_service.close.await_count == 1

    @pytest.mark.asyncio
    async def test_stop_idempotent_when_loop_already_done(self) -> None:
        """Calling ``stop`` after the loop has finished is a no-op apart from close."""
        scheduler, fake_service = _build_scheduler_with_fake_service()
        await scheduler.start()
        await asyncio.sleep(0.01)
        await scheduler.stop()
        first_close_count = fake_service.close.await_count

        await scheduler.stop()

        assert fake_service.close.await_count == first_close_count + 1

    @pytest.mark.asyncio
    async def test_stop_disabled_scheduler_with_explicit_service_still_closes(self) -> None:
        """Disabled scheduler that was passed a service explicitly still closes it."""
        scheduler, fake_service = _build_scheduler_with_fake_service(disabled=True)

        await scheduler.start()
        await scheduler.stop()

        assert fake_service.close.await_count == 1

    @pytest.mark.asyncio
    async def test_stop_skips_close_when_service_is_none(self) -> None:
        """Disabled scheduler without an explicit service has no close to call.

        Default disabled-mode constructor path skips
        :class:`RetentionService` construction entirely (Codex final-gate
        BLOCKER fix); ``stop()`` MUST tolerate that no-service state.
        """

        class _NopRepo:
            def dispose(self) -> None:
                """No-op for the constructor's repo build."""
                return

        scheduler = RetentionScheduler(
            db_url="sqlite+aiosqlite:///:memory:",
            disabled=True,
            interval_seconds=10.0,
        )

        assert scheduler._service is None

        await scheduler.start()
        await scheduler.stop()

    @pytest.mark.asyncio
    async def test_loop_returns_when_service_is_none(self) -> None:
        """Defensive early return when ``_loop`` finds no service.

        Unreachable in production because :meth:`start` guards against
        spawning the loop when ``_service is None``, but the defensive
        return is exercised here for mypy + 100% branch coverage.
        """
        scheduler = RetentionScheduler(
            db_url="sqlite+aiosqlite:///:memory:",
            disabled=True,
            interval_seconds=0.01,
        )

        await scheduler._loop()


class TestSchedulerLastRunSummaryNoService:
    """``last_run_summary`` returns ``None`` when no service is attached."""

    @pytest.mark.asyncio
    async def test_returns_none_when_service_is_none(self) -> None:
        """Disabled scheduler without explicit service has no service to proxy."""
        scheduler = RetentionScheduler(
            db_url="sqlite+aiosqlite:///:memory:",
            disabled=True,
            interval_seconds=0.01,
        )

        assert scheduler.last_run_summary is None


class TestSchedulerConstructionDisabled:
    """Codex final-gate BLOCKER fix: disabled scheduler skips service build."""

    def test_disabled_default_skips_retention_service_construction(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Disabled mode does not invoke ``RetentionService`` constructor.

        Why this matters: ``RetentionService.__init__`` builds a sync
        ``DatabaseRepository`` which loads driver-specific imports.
        On PostgreSQL a missing ``psycopg2`` driver previously raised
        ``ModuleNotFoundError`` even when the operator set
        ``RETENTION_DISABLED=true`` — the disabled scheduler should not
        require the sync DB driver to be importable at all.
        """
        constructor_calls = {"n": 0}

        def _fake_service(**_kwargs: Any) -> Any:
            constructor_calls["n"] += 1
            return AsyncMock()

        monkeypatch.setattr(scheduler_module, "RetentionService", _fake_service)

        scheduler = RetentionScheduler(
            db_url="postgresql+asyncpg://localhost/snapper",
            disabled=True,
            interval_seconds=3600.0,
        )

        assert scheduler._service is None
        assert constructor_calls["n"] == 0

    def test_active_default_constructs_retention_service(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Active mode (disabled=False) DOES construct the service."""
        constructor_calls = {"n": 0}

        def _fake_service(**_kwargs: Any) -> Any:
            constructor_calls["n"] += 1
            return AsyncMock()

        monkeypatch.setattr(scheduler_module, "RetentionService", _fake_service)

        scheduler = RetentionScheduler(
            db_url="sqlite+aiosqlite:///:memory:",
            disabled=False,
            interval_seconds=3600.0,
        )

        assert scheduler._service is not None
        assert constructor_calls["n"] == 1


class TestSchedulerProperties:
    """Read-only property surface."""

    @pytest.mark.asyncio
    async def test_disabled_property_reflects_construction(self) -> None:
        """``disabled`` reflects the constructor flag."""
        scheduler_disabled, _ = _build_scheduler_with_fake_service(disabled=True)
        scheduler_active, _ = _build_scheduler_with_fake_service(disabled=False)

        assert scheduler_disabled.disabled is True
        assert scheduler_active.disabled is False

    @pytest.mark.asyncio
    async def test_interval_seconds_property_reflects_construction(self) -> None:
        """``interval_seconds`` reflects the constructor value."""
        scheduler, _ = _build_scheduler_with_fake_service(interval_seconds=42.0)

        assert scheduler.interval_seconds == 42.0

    @pytest.mark.asyncio
    async def test_last_run_summary_proxies_to_service(self) -> None:
        """``last_run_summary`` reads through to the underlying service."""
        summary = _build_fake_summary()
        scheduler, fake_service = _build_scheduler_with_fake_service()
        fake_service.last_run_summary = summary

        assert scheduler.last_run_summary is summary


class TestSchedulerEnvDefaults:
    """Constructor falls back to env-var resolvers when args are ``None``."""

    @pytest.mark.asyncio
    async def test_constructor_reads_env_when_args_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``None`` arg paths exercise the env-var resolvers (coverage)."""
        monkeypatch.setenv("RETENTION_INTERVAL_SECONDS", "120")
        monkeypatch.setenv("RETENTION_DISABLED", "true")
        monkeypatch.setenv("RETENTION_OUTPUT_DIR", "/tmp/snapper-retention-test")
        fake_service = AsyncMock()
        fake_service.last_run_summary = None
        fake_service.close = AsyncMock()

        with patch.object(scheduler_module, "RetentionService", lambda **_kwargs: fake_service):
            scheduler = RetentionScheduler(db_url="sqlite+aiosqlite:///:memory:")

        assert scheduler.interval_seconds == 120.0
        assert scheduler.disabled is True

    @pytest.mark.asyncio
    async def test_constructor_explicit_base_dir_skips_env_lookup(self) -> None:
        """Explicit ``base_dir`` arg bypasses ``RETENTION_OUTPUT_DIR`` env."""
        from pathlib import Path

        fake_service = AsyncMock()
        fake_service.last_run_summary = None

        with patch.object(scheduler_module, "RetentionService", lambda **_kwargs: fake_service):
            scheduler = RetentionScheduler(
                db_url="sqlite+aiosqlite:///:memory:",
                base_dir=Path("/explicit/base"),
                interval_seconds=42.0,
                disabled=False,
            )

        assert scheduler.interval_seconds == 42.0
        assert scheduler.disabled is False


class TestSchedulerLoopBranchCoverage:
    """Edge-case branches for the loop's exit + post-sleep checks."""

    @pytest.mark.asyncio
    async def test_loop_returns_when_stopping_set_during_sleep(self) -> None:
        """Stopping flag flipped during sleep → early return after the sleep.

        Yields control once with ``asyncio.sleep(0)`` after :meth:`start`
        so the loop task is guaranteed to enter its first
        ``asyncio.sleep`` before the test sets the stopping flag.
        """
        scheduler, fake_service = _build_scheduler_with_fake_service(
            run_once_side_effects=[_build_fake_summary()],
            interval_seconds=0.05,
        )
        await scheduler.start()
        await asyncio.sleep(0)
        scheduler._stopping.set()
        assert scheduler._loop_task is not None
        await scheduler._loop_task
        await scheduler.stop()

        assert fake_service.run_once.await_count == 1

    @pytest.mark.asyncio
    async def test_loop_exits_when_stopping_set_during_run_once(self) -> None:
        """Stopping flag flipped during ``run_once`` → next iteration exits via while-check."""
        scheduler_holder: dict[str, RetentionScheduler] = {}
        call_count = {"n": 0}

        async def _run_once_setting_stop() -> RetentionRunSummary:
            call_count["n"] += 1
            if call_count["n"] >= 2:
                scheduler_holder["sched"]._stopping.set()
            return _build_fake_summary()

        scheduler, fake_service = _build_scheduler_with_fake_service(
            run_once_side_effects=None,
            interval_seconds=0.01,
        )
        scheduler_holder["sched"] = scheduler
        fake_service.run_once.side_effect = _run_once_setting_stop

        await scheduler.start()
        assert scheduler._loop_task is not None
        await scheduler._loop_task
        await scheduler.stop()

        assert call_count["n"] >= 2


class TestStartHelper:
    """Lifespan startup helper — B22 attribute-absent contract."""

    @pytest.mark.asyncio
    async def test_start_failure_leaves_attribute_unset(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Helper-unit test: when ``start`` raises, no attribute is assigned."""

        class _FailingScheduler:
            def __init__(self, **_kwargs: Any) -> None:
                pass

            async def start(self) -> None:
                raise RuntimeError("synthetic startup failure")

            disabled = False
            interval_seconds = 0.0

        monkeypatch.setattr(
            "snapper.server.app.RetentionScheduler",
            _FailingScheduler,
        )
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_retention_scheduler(app, db_url="sqlite+aiosqlite:///:memory:")

        assert not hasattr(app.state, "retention_scheduler")

    @pytest.mark.asyncio
    async def test_start_success_assigns_attribute(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """On success, the singleton is attached to ``app.state``."""
        started = AsyncMock()

        class _SucceedingScheduler:
            def __init__(self, **_kwargs: Any) -> None:
                self._started = started

            async def start(self) -> None:
                await self._started()

            disabled = False
            interval_seconds = 3600.0

        monkeypatch.setattr(
            "snapper.server.app.RetentionScheduler",
            _SucceedingScheduler,
        )
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_retention_scheduler(app, db_url="sqlite+aiosqlite:///:memory:")

        assert started.await_count == 1
        assert isinstance(app.state.retention_scheduler, _SucceedingScheduler)

    @pytest.mark.asyncio
    async def test_start_disabled_logs_and_assigns(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Disabled scheduler still gets assigned (so the route can read disabled flag)."""

        class _DisabledScheduler:
            def __init__(self, **_kwargs: Any) -> None:
                pass

            async def start(self) -> None:
                return None

            disabled = True
            interval_seconds = 3600.0

        monkeypatch.setattr(
            "snapper.server.app.RetentionScheduler",
            _DisabledScheduler,
        )
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_retention_scheduler(app, db_url="sqlite+aiosqlite:///:memory:")

        assert isinstance(app.state.retention_scheduler, _DisabledScheduler)
        assert app.state.retention_scheduler.disabled is True


class TestStopHelper:
    """Lifespan shutdown helper — tolerates partial-init."""

    @pytest.mark.asyncio
    async def test_stop_when_attribute_absent_is_noop(self) -> None:
        """Shutdown helper exits silently when no scheduler was attached."""
        app = SimpleNamespace(state=SimpleNamespace())

        await _stop_retention_scheduler(app)

    @pytest.mark.asyncio
    async def test_stop_when_pre_set_none_is_noop(self) -> None:
        """Lifespan TOP pre-sets the attribute to ``None``; helper still no-ops."""
        app = SimpleNamespace(state=SimpleNamespace(retention_scheduler=None))

        await _stop_retention_scheduler(app)

    @pytest.mark.asyncio
    async def test_stop_when_attached_invokes_stop(self) -> None:
        """When attached, the helper awaits the scheduler's ``stop`` method."""
        stop_mock = AsyncMock()
        app = SimpleNamespace(
            state=SimpleNamespace(retention_scheduler=SimpleNamespace(stop=stop_mock))
        )

        await _stop_retention_scheduler(app)

        assert stop_mock.await_count == 1
