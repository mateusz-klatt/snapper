"""Lifespan helper tests for ``_start_db_stats_snapshotter`` / ``_stop_db_stats_snapshotter``.

Mirrors the test pattern used by retention
(``tests/application/retention/test_scheduler.py::TestStartHelper`` /
``TestStopHelper``) so the B22 attribute-absent contract is exercised
across all snapshotter helpers in the same shape.
"""

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from snapper.server.app import _start_db_stats_snapshotter
from snapper.server.app import _stop_db_stats_snapshotter


class TestStartHelper:
    """Lifespan startup helper — B22 attribute-absent contract."""

    @pytest.mark.asyncio
    async def test_start_success_assigns_attribute(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """On success, the singleton is attached to ``app.state``."""
        started = AsyncMock()

        class _SucceedingSnapshotter:
            def __init__(self, **_kwargs: Any) -> None:
                self._started = started

            async def start(self) -> None:
                await self._started()

            disabled = False
            interval_seconds = 60.0

        monkeypatch.setattr("snapper.server.app.DbStatsSnapshotter", _SucceedingSnapshotter)
        monkeypatch.delenv("DB_METRICS_DISABLED", raising=False)
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_db_stats_snapshotter(app, db_url="sqlite+aiosqlite:///:memory:")

        assert started.await_count == 1
        assert isinstance(app.state.db_stats_snapshotter, _SucceedingSnapshotter)

    @pytest.mark.asyncio
    async def test_start_failure_leaves_attribute_absent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Helper swallows startup exceptions; ``app.state`` keeps no attribute."""

        class _FailingSnapshotter:
            def __init__(self, **_kwargs: Any) -> None:
                pass

            async def start(self) -> None:
                raise RuntimeError("synthetic startup failure")

            disabled = False
            interval_seconds = 0.0

        monkeypatch.setattr("snapper.server.app.DbStatsSnapshotter", _FailingSnapshotter)
        monkeypatch.delenv("DB_METRICS_DISABLED", raising=False)
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_db_stats_snapshotter(app, db_url="sqlite+aiosqlite:///:memory:")

        assert not hasattr(app.state, "db_stats_snapshotter")

    @pytest.mark.asyncio
    async def test_start_disabled_via_env_skips_repo_construction(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``DB_METRICS_DISABLED=true`` resolves before ``get_repository`` is called."""

        class _DisabledSnapshotter:
            def __init__(self, **kwargs: Any) -> None:
                self._kwargs = kwargs

            async def start(self) -> None:
                return None

            disabled = True
            interval_seconds = 60.0

        get_repository_calls: list[str] = []

        def fake_get_repository(db_url: str) -> Any:
            get_repository_calls.append(db_url)
            return SimpleNamespace()

        monkeypatch.setattr("snapper.server.app.DbStatsSnapshotter", _DisabledSnapshotter)
        monkeypatch.setattr("snapper.server.app.get_repository", fake_get_repository)
        monkeypatch.setenv("DB_METRICS_DISABLED", "true")
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_db_stats_snapshotter(app, db_url="sqlite+aiosqlite:///:memory:")

        assert get_repository_calls == []
        assert isinstance(app.state.db_stats_snapshotter, _DisabledSnapshotter)
        assert app.state.db_stats_snapshotter._kwargs["repo"] is None
        assert app.state.db_stats_snapshotter._kwargs["disabled"] is True


class TestStopHelper:
    """Lifespan shutdown helper — tolerates partial-init."""

    @pytest.mark.asyncio
    async def test_stop_no_op_when_attribute_absent(self) -> None:
        """``_stop_db_stats_snapshotter`` returns cleanly when no snapshotter is attached."""
        app = SimpleNamespace(state=SimpleNamespace())
        await _stop_db_stats_snapshotter(app)

    @pytest.mark.asyncio
    async def test_stop_calls_attached_snapshotter(self) -> None:
        """Attached snapshotter receives the ``stop()`` call."""
        stopped = AsyncMock()
        snapshotter = SimpleNamespace(stop=stopped)
        app = SimpleNamespace(state=SimpleNamespace(db_stats_snapshotter=snapshotter))
        await _stop_db_stats_snapshotter(app)
        assert stopped.await_count == 1
