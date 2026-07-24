"""Lifespan helper tests for the Phase-5B portfolio P&L snapshotter wiring.

Mirrors ``tests/application/db_stats/test_app_lifespan_helpers.py`` so the
attribute-absent, instance-0-gated, disabled-mode contract of
``_start_pnl_snapshotter`` / ``_stop_pnl_snapshotter`` is exercised in the same
shape as the other lifespan singletons.
"""

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from snapper.server.app import _start_background_writers
from snapper.server.app import _start_pnl_snapshotter
from snapper.server.app import _stop_background_writers
from snapper.server.app import _stop_pnl_snapshotter


def _settings(*, instance_id: int = 0, instance_count: int = 1) -> Any:
    """Build a settings stub carrying the coordinator partition identity."""
    return SimpleNamespace(
        coordinator_instance_id=instance_id, coordinator_instance_count=instance_count
    )


class TestStartHelper:
    """Startup helper — instance gate, disabled mode, fail-closed contract."""

    @pytest.mark.asyncio
    async def test_enabled_on_instance_zero_attaches(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """An enabled snapshotter on instance 0 is started and attached."""
        started = AsyncMock()

        class _Snapshotter:
            def __init__(self, **kwargs: Any) -> None:
                self._kwargs = kwargs

            async def start(self) -> None:
                await started()

            disabled = False
            interval_seconds = 60

        monkeypatch.setattr("snapper.server.app.PortfolioPnlSnapshotter", _Snapshotter)
        monkeypatch.setattr("snapper.server.app.get_repository", lambda db_url: SimpleNamespace())
        monkeypatch.setenv("PNL_SNAPSHOTTER_ENABLED", "true")
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_pnl_snapshotter(
            app, db_url="sqlite+aiosqlite:///:memory:", settings=_settings()
        )

        assert started.await_count == 1
        assert isinstance(app.state.pnl_snapshotter, _Snapshotter)
        assert app.state.pnl_snapshotter._kwargs["disabled"] is False

    @pytest.mark.asyncio
    async def test_non_coordinator_instance_skips(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A non-zero partition instance never starts the single-writer job."""
        calls: list[str] = []
        monkeypatch.setattr(
            "snapper.server.app.get_repository", lambda db_url: calls.append(db_url)
        )
        monkeypatch.setenv("PNL_SNAPSHOTTER_ENABLED", "true")
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_pnl_snapshotter(
            app,
            db_url="sqlite+aiosqlite:///:memory:",
            settings=_settings(instance_id=1, instance_count=3),
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
            def __init__(self, **kwargs: Any) -> None:
                self._kwargs = kwargs

            async def start(self) -> None:
                return None

            disabled = True
            interval_seconds = 60

        monkeypatch.setattr("snapper.server.app.PortfolioPnlSnapshotter", _Snapshotter)
        monkeypatch.setattr(
            "snapper.server.app.get_repository", lambda db_url: repo_calls.append(db_url)
        )
        monkeypatch.delenv("PNL_SNAPSHOTTER_ENABLED", raising=False)
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_pnl_snapshotter(
            app, db_url="sqlite+aiosqlite:///:memory:", settings=_settings()
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
            def __init__(self, **_kwargs: Any) -> None:
                pass

            async def start(self) -> None:
                raise RuntimeError("synthetic startup failure")

            disabled = False
            interval_seconds = 60

        monkeypatch.setattr("snapper.server.app.PortfolioPnlSnapshotter", _Failing)
        monkeypatch.setattr("snapper.server.app.get_repository", lambda db_url: SimpleNamespace())
        monkeypatch.setenv("PNL_SNAPSHOTTER_ENABLED", "true")
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_pnl_snapshotter(
            app, db_url="sqlite+aiosqlite:///:memory:", settings=_settings()
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
        start_db = AsyncMock(side_effect=lambda *a, **k: order.append("db_stats"))
        start_pnl = AsyncMock(side_effect=lambda *a, **k: order.append("pnl"))
        monkeypatch.setattr("snapper.server.app._start_db_stats_snapshotter", start_db)
        monkeypatch.setattr("snapper.server.app._start_pnl_snapshotter", start_pnl)
        app = SimpleNamespace(state=SimpleNamespace())
        settings = _settings()

        await _start_background_writers(
            app, db_url="sqlite+aiosqlite:///:memory:", settings=settings
        )

        assert order == ["db_stats", "pnl"]
        start_db.assert_awaited_once_with(app, db_url="sqlite+aiosqlite:///:memory:")
        start_pnl.assert_awaited_once_with(
            app, db_url="sqlite+aiosqlite:///:memory:", settings=settings
        )

    @pytest.mark.asyncio
    async def test_stop_delegates_in_reverse_order(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The combined stop tears the writers down in reverse start order."""
        order: list[str] = []
        stop_pnl = AsyncMock(side_effect=lambda *a, **k: order.append("pnl"))
        stop_db = AsyncMock(side_effect=lambda *a, **k: order.append("db_stats"))
        monkeypatch.setattr("snapper.server.app._stop_pnl_snapshotter", stop_pnl)
        monkeypatch.setattr("snapper.server.app._stop_db_stats_snapshotter", stop_db)
        app = SimpleNamespace(state=SimpleNamespace())

        await _stop_background_writers(app)

        assert order == ["pnl", "db_stats"]
        stop_pnl.assert_awaited_once_with(app)
        stop_db.assert_awaited_once_with(app)
