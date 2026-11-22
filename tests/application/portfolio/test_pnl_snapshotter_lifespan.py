"""Lifespan helper tests for the Phase-5B portfolio P&L snapshotter wiring.

Mirrors ``tests/application/db_stats/test_app_lifespan_helpers.py`` so the
attribute-absent, instance-0-gated, disabled-mode contract of
``_start_pnl_snapshotter`` / ``_stop_pnl_snapshotter`` is exercised in the same
shape as the other lifespan singletons.
"""

from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock

import pytest
from loguru import logger as loguru_logger

from snapper.application.portfolio import pnl_snapshotter_config
from snapper.application.portfolio.pnl_snapshotter import PortfolioPnlSnapshotter
from snapper.application.services.settings import SettingsService
from snapper.config.settings import AppSettings
from snapper.server.app import _start_background_writers
from snapper.server.app import _start_pnl_snapshotter
from snapper.server.app import _stop_background_writers
from snapper.server.app import _stop_pnl_snapshotter


def _settings(*, instance_id: int = 0, instance_count: int = 1) -> AppSettings:
    """Build a settings stub carrying the coordinator partition identity."""
    return cast(
        AppSettings,
        SimpleNamespace(
            coordinator_instance_id=instance_id,
            coordinator_instance_count=instance_count,
        ),
    )


def _settings_service() -> SettingsService:
    """Build an identity-only settings-service stub."""
    return cast(SettingsService, SimpleNamespace())


class TestStartHelper:
    """Startup helper — instance gate, disabled mode, fail-closed contract."""

    @pytest.mark.asyncio
    async def test_enabled_on_instance_zero_attaches(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """An enabled snapshotter on instance 0 is started and attached."""
        started = AsyncMock()

        class _Snapshotter:
            def __init__(self, **kwargs: object) -> None:
                self._kwargs = kwargs

            async def start(self) -> None:
                await started()

            disabled = False
            interval_seconds = 60
            checkpoint_mode = "off"
            configured_checkpoint_mode = "off"

        def _repository(_db_url: str) -> SimpleNamespace:
            return SimpleNamespace()

        monkeypatch.setattr("snapper.server.app.PortfolioPnlSnapshotter", _Snapshotter)
        monkeypatch.setattr("snapper.server.app.get_repository", _repository)
        monkeypatch.setenv("PNL_SNAPSHOTTER_ENABLED", "true")
        settings_service = _settings_service()
        state_settings_service = _settings_service()
        app = SimpleNamespace(state=SimpleNamespace(settings_service=state_settings_service))

        await _start_pnl_snapshotter(
            app,
            db_url="sqlite+aiosqlite:///:memory:",
            settings=_settings(),
            settings_service=settings_service,
        )

        assert started.await_count == 1
        assert isinstance(app.state.pnl_snapshotter, _Snapshotter)
        assert app.state.pnl_snapshotter._kwargs["disabled"] is False
        assert app.state.pnl_snapshotter._kwargs["settings_service"] is settings_service
        assert app.state.pnl_snapshotter._kwargs["settings_service"] is not state_settings_service

    @pytest.mark.asyncio
    async def test_non_coordinator_instance_skips(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A non-zero partition instance never starts the single-writer job."""
        calls: list[str] = []

        def _record_repository(db_url: str) -> None:
            calls.append(db_url)

        monkeypatch.setattr("snapper.server.app.get_repository", _record_repository)
        monkeypatch.setenv("PNL_SNAPSHOTTER_ENABLED", "true")
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_pnl_snapshotter(
            app,
            db_url="sqlite+aiosqlite:///:memory:",
            settings=_settings(instance_id=1, instance_count=3),
            settings_service=_settings_service(),
        )

        assert calls == []
        assert not hasattr(app.state, "pnl_snapshotter")

    @pytest.mark.asyncio
    async def test_disabled_by_default_attaches_without_repo(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With no enabling flag the snapshotter attaches parked, no repo built."""
        repo_calls: list[str] = []

        class _Snapshotter:
            def __init__(self, **kwargs: object) -> None:
                self._kwargs = kwargs

            async def start(self) -> None:
                return None

            disabled = True
            interval_seconds = 60

        def _record_repository(db_url: str) -> None:
            repo_calls.append(db_url)

        monkeypatch.setattr("snapper.server.app.PortfolioPnlSnapshotter", _Snapshotter)
        monkeypatch.setattr("snapper.server.app.get_repository", _record_repository)
        monkeypatch.delenv("PNL_SNAPSHOTTER_ENABLED", raising=False)
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_pnl_snapshotter(
            app,
            db_url="sqlite+aiosqlite:///:memory:",
            settings=_settings(),
            settings_service=_settings_service(),
        )

        assert repo_calls == []
        assert app.state.pnl_snapshotter._kwargs["repo"] is None
        assert app.state.pnl_snapshotter._kwargs["disabled"] is True

    @pytest.mark.asyncio
    async def test_start_failure_leaves_attribute_absent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A startup exception is swallowed and no attribute is attached."""

        class _Failing:
            def __init__(self, **_kwargs: object) -> None:
                pass

            async def start(self) -> None:
                raise RuntimeError("synthetic startup failure")

            disabled = False
            interval_seconds = 60
            checkpoint_mode = "off"
            configured_checkpoint_mode = "off"

        def _repository(_db_url: str) -> SimpleNamespace:
            return SimpleNamespace()

        monkeypatch.setattr("snapper.server.app.PortfolioPnlSnapshotter", _Failing)
        monkeypatch.setattr("snapper.server.app.get_repository", _repository)
        monkeypatch.setenv("PNL_SNAPSHOTTER_ENABLED", "true")
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_pnl_snapshotter(
            app,
            db_url="sqlite+aiosqlite:///:memory:",
            settings=_settings(),
            settings_service=_settings_service(),
        )

        assert not hasattr(app.state, "pnl_snapshotter")

    @pytest.mark.parametrize(
        "case",
        [
            (None, None, 60, "off", "off"),
            ("30", "shadow", 30, "shadow", "shadow"),
            ("120", "active", 120, "active", "active"),
        ],
    )
    @pytest.mark.asyncio
    async def test_startup_logs_resolved_interval_and_checkpoint_mode(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        case: tuple[str | None, str | None, int, str, str],
    ) -> None:
        """Log interval plus effective and configured checkpoint modes when started.

        Given: An enabled snapshotter environment with interval and checkpoint mode.
        When: ``_start_pnl_snapshotter`` starts and attaches the snapshotter.
        Then: An INFO log line reports ``interval=<N>s checkpoint_mode=<effective> configured_checkpoint_mode=<configured>``.
        """
        interval_env, checkpoint_env, expected_interval, expected_effective, expected_configured = (
            case
        )
        monkeypatch.setattr("snapper.server.app.get_repository", lambda _url: SimpleNamespace())
        monkeypatch.setattr(PortfolioPnlSnapshotter, "start", AsyncMock())
        monkeypatch.setenv("PNL_SNAPSHOTTER_ENABLED", "true")
        if interval_env is None:
            monkeypatch.delenv("PNL_SNAPSHOTTER_INTERVAL_SECONDS", raising=False)
        else:
            monkeypatch.setenv("PNL_SNAPSHOTTER_INTERVAL_SECONDS", interval_env)
        if checkpoint_env is None:
            monkeypatch.delenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", raising=False)
        else:
            monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", checkpoint_env)
        app = SimpleNamespace(state=SimpleNamespace())

        handler_id = loguru_logger.add(caplog.handler, format="{message}", level="INFO")
        try:
            await _start_pnl_snapshotter(
                app,
                db_url="sqlite+aiosqlite:///:memory:",
                settings=_settings(),
                settings_service=_settings_service(),
            )
        finally:
            loguru_logger.remove(handler_id)

        expected = (
            f"PortfolioPnlSnapshotter started: "
            f"interval={expected_interval}s "
            f"checkpoint_mode={expected_effective} "
            f"configured_checkpoint_mode={expected_configured}"
        )
        assert any(
            record.levelname == "INFO" and record.getMessage() == expected
            for record in caplog.records
        )

    @pytest.mark.asyncio
    async def test_startup_logs_kill_switch_effective_and_configured_checkpoint_mode(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Log downgraded effective mode when ``CHECKPOINT_MODES_ENABLED`` restricts config.

        Given: ``CHECKPOINT_MODES_ENABLED`` narrowed to ``{"off"}``.
        When: ``_start_pnl_snapshotter`` starts with ``PNL_SNAPSHOTTER_CHECKPOINT_MODE=shadow``.
        Then: The INFO line reports ``checkpoint_mode=off`` and ``configured_checkpoint_mode=shadow``.
        """
        monkeypatch.setattr(
            pnl_snapshotter_config,
            "CHECKPOINT_MODES_ENABLED",
            frozenset({"off"}),
        )
        monkeypatch.setattr("snapper.server.app.get_repository", lambda _url: SimpleNamespace())
        monkeypatch.setattr(PortfolioPnlSnapshotter, "start", AsyncMock())
        monkeypatch.setenv("PNL_SNAPSHOTTER_ENABLED", "true")
        monkeypatch.setenv("PNL_SNAPSHOTTER_INTERVAL_SECONDS", "45")
        monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "shadow")
        app = SimpleNamespace(state=SimpleNamespace())

        handler_id = loguru_logger.add(caplog.handler, format="{message}", level="INFO")
        try:
            await _start_pnl_snapshotter(
                app,
                db_url="sqlite+aiosqlite:///:memory:",
                settings=_settings(),
                settings_service=_settings_service(),
            )
        finally:
            loguru_logger.remove(handler_id)

        expected = (
            "PortfolioPnlSnapshotter started: interval=45s "
            "checkpoint_mode=off configured_checkpoint_mode=shadow"
        )
        assert any(
            record.levelname == "INFO" and record.getMessage() == expected
            for record in caplog.records
        )

    @pytest.mark.asyncio
    async def test_invalid_checkpoint_mode_is_isolated_at_server_startup(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Keep invalid checkpoint-mode configuration from crashing server startup.

        Given: ``PNL_SNAPSHOTTER_CHECKPOINT_MODE`` set to an unrecognised token.
        When: ``_start_pnl_snapshotter`` initializes the writer singleton.
        Then: Startup returns cleanly without attaching ``app.state.pnl_snapshotter``.
        """
        monkeypatch.setattr("snapper.server.app.get_repository", lambda _url: SimpleNamespace())
        monkeypatch.setenv("PNL_SNAPSHOTTER_ENABLED", "true")
        monkeypatch.setenv("PNL_SNAPSHOTTER_CHECKPOINT_MODE", "bogus")
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_pnl_snapshotter(
            app,
            db_url="sqlite+aiosqlite:///:memory:",
            settings=_settings(),
            settings_service=_settings_service(),
        )

        assert not hasattr(app.state, "pnl_snapshotter")


class TestStopHelper:
    """Shutdown helper — tolerates partial init."""

    @pytest.mark.asyncio
    async def test_stop_no_op_when_absent(self) -> None:
        """Stop returns cleanly when no snapshotter is attached."""
        app = SimpleNamespace(state=SimpleNamespace())
        await _stop_pnl_snapshotter(app)

    @pytest.mark.asyncio
    async def test_stop_calls_attached_snapshotter(self) -> None:
        """An attached snapshotter receives the stop call."""
        stopped = AsyncMock()
        app = SimpleNamespace(state=SimpleNamespace(pnl_snapshotter=SimpleNamespace(stop=stopped)))
        await _stop_pnl_snapshotter(app)
        assert stopped.await_count == 1


class TestBackgroundWriters:
    """The combined start/stop grouping the DB-stats and P&L writers (D3)."""

    @pytest.mark.asyncio
    async def test_start_delegates_to_both_writers_in_order(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The combined start runs the DB-stats writer before the P&L writer."""
        order: list[str] = []

        async def _start_db(_app: object, *, db_url: str) -> None:
            assert db_url == "sqlite+aiosqlite:///:memory:"
            order.append("db_stats")

        async def _start_pnl(
            _app: object,
            *,
            db_url: str,
            settings: AppSettings,
            settings_service: SettingsService,
        ) -> None:
            assert db_url == "sqlite+aiosqlite:///:memory:"
            assert settings is expected_settings
            assert settings_service is expected_settings_service
            order.append("pnl")

        start_db = AsyncMock(side_effect=_start_db)
        start_pnl = AsyncMock(side_effect=_start_pnl)
        monkeypatch.setattr("snapper.server.app._start_db_stats_snapshotter", start_db)
        monkeypatch.setattr("snapper.server.app._start_pnl_snapshotter", start_pnl)
        app = SimpleNamespace(state=SimpleNamespace())
        expected_settings = _settings()
        expected_settings_service = _settings_service()

        await _start_background_writers(
            app,
            db_url="sqlite+aiosqlite:///:memory:",
            settings=expected_settings,
            settings_service=expected_settings_service,
        )

        assert order == ["db_stats", "pnl"]
        start_db.assert_awaited_once_with(app, db_url="sqlite+aiosqlite:///:memory:")
        start_pnl.assert_awaited_once_with(
            app,
            db_url="sqlite+aiosqlite:///:memory:",
            settings=expected_settings,
            settings_service=expected_settings_service,
        )

    @pytest.mark.asyncio
    async def test_stop_delegates_in_reverse_order(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The combined stop tears the writers down in reverse start order."""
        order: list[str] = []

        async def _stop_pnl(_app: object) -> None:
            order.append("pnl")

        async def _stop_db(_app: object) -> None:
            order.append("db_stats")

        stop_pnl = AsyncMock(side_effect=_stop_pnl)
        stop_db = AsyncMock(side_effect=_stop_db)
        monkeypatch.setattr("snapper.server.app._stop_pnl_snapshotter", stop_pnl)
        monkeypatch.setattr("snapper.server.app._stop_db_stats_snapshotter", stop_db)
        app = SimpleNamespace(state=SimpleNamespace())

        await _stop_background_writers(app)

        assert order == ["pnl", "db_stats"]
        stop_pnl.assert_awaited_once_with(app)
        stop_db.assert_awaited_once_with(app)
