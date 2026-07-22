"""Tests for AI-review maintenance FastAPI lifespan wiring."""

from collections.abc import Callable
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.server.app import _start_ai_review_maintenance
from snapper.server.app import _stop_ai_review_maintenance


class TestStartAiReviewMaintenance:
    """Startup helper attach-on-success behavior."""

    @pytest.mark.asyncio
    async def test_start_failure_leaves_attribute_absent(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A maintenance startup failure is isolated from the lifespan."""

        class FailingMaintenance:
            """Stand-in that fails during startup."""

            def __init__(self, **_kwargs: object) -> None:
                """Accept production constructor keywords."""

            async def start(self) -> None:
                """Raise a synthetic startup failure."""
                raise RuntimeError("startup failed")

        monkeypatch.setattr("snapper.server.app.AiReviewMaintenanceService", FailingMaintenance)
        monkeypatch.setattr("snapper.server.app.get_ai_review_service", MagicMock())
        monkeypatch.setattr("snapper.server.app.get_repository", MagicMock())
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_ai_review_maintenance(app, db_url="sqlite+aiosqlite:///:memory:")

        assert not hasattr(app.state, "ai_review_maintenance")

    @pytest.mark.asyncio
    async def test_start_success_attaches_wired_driver(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Successful startup injects the singleton and repository factory."""
        started = AsyncMock()
        service = MagicMock()
        repository = MagicMock()
        get_service = MagicMock(return_value=service)
        get_repository = MagicMock(return_value=repository)
        constructed: list[dict[str, object]] = []

        class SucceedingMaintenance:
            """Stand-in recording constructor and startup calls."""

            def __init__(
                self,
                *,
                service: object,
                repository_factory: Callable[[], object],
            ) -> None:
                """Capture production dependency injection.

                Args:
                    service: AI-review singleton sentinel.
                    repository_factory: Repository provider under test.
                """
                constructed.append(
                    {
                        "service": service,
                        "repository": repository_factory(),
                    }
                )

            async def start(self) -> None:
                """Record successful startup."""
                await started()

        monkeypatch.setattr("snapper.server.app.AiReviewMaintenanceService", SucceedingMaintenance)
        monkeypatch.setattr("snapper.server.app.get_ai_review_service", get_service)
        monkeypatch.setattr("snapper.server.app.get_repository", get_repository)
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_ai_review_maintenance(
            app,
            db_url="sqlite+aiosqlite:///:memory:",
        )

        assert started.await_count == 1
        assert constructed == [{"service": service, "repository": repository}]
        get_service.assert_called_once_with()
        get_repository.assert_called_once_with("sqlite+aiosqlite:///:memory:")
        assert isinstance(app.state.ai_review_maintenance, SucceedingMaintenance)


class TestStopAiReviewMaintenance:
    """Shutdown helper partial-start behavior."""

    @pytest.mark.asyncio
    async def test_stop_without_attribute_is_noop(self) -> None:
        """Missing startup state is tolerated."""
        app = SimpleNamespace(state=SimpleNamespace())

        await _stop_ai_review_maintenance(app)

    @pytest.mark.asyncio
    async def test_stop_awaits_attached_driver(self) -> None:
        """An attached maintenance driver receives one stop call."""
        maintenance = MagicMock(stop=AsyncMock())
        app = SimpleNamespace(state=SimpleNamespace(ai_review_maintenance=maintenance))

        await _stop_ai_review_maintenance(app)

        maintenance.stop.assert_awaited_once()
