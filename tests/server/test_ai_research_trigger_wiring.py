"""Tests for AI-research trigger FastAPI lifespan wiring."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.server.app import _start_ai_research_trigger
from snapper.server.app import _stop_ai_research_trigger


class TestStartAiResearchTrigger:
    """Startup helper attach-on-success behavior."""

    @pytest.mark.asyncio
    async def test_start_failure_leaves_attribute_absent(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A trigger failure is isolated from the remaining lifespan."""

        class FailingTrigger:
            """Stand-in that fails during startup."""

            def __init__(self, **_kwargs: object) -> None:
                """Accept production constructor keywords."""

            async def start(self) -> None:
                """Raise a synthetic startup failure."""
                raise RuntimeError("startup failed")

        monkeypatch.setattr("snapper.server.app.AiResearchTriggerService", FailingTrigger)
        monkeypatch.setattr("snapper.server.app.get_repository", MagicMock())
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_ai_research_trigger(app, db_url="sqlite+aiosqlite:///:memory:")

        assert not hasattr(app.state, "ai_research_trigger")

    @pytest.mark.asyncio
    async def test_start_success_attaches_wired_trigger(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Successful startup injects the repository and shared publisher."""
        started = AsyncMock()
        repository = MagicMock()
        publisher = MagicMock()
        get_repository = MagicMock(return_value=repository)
        constructed: list[dict[str, object]] = []

        class SucceedingTrigger:
            """Stand-in recording constructor and startup calls."""

            def __init__(
                self,
                *,
                repo: object,
                msg_publisher: object | None = None,
            ) -> None:
                """Capture production dependency injection.

                Args:
                    repo: Repository sentinel.
                    msg_publisher: Publisher sentinel.
                """
                constructed.append({"repo": repo, "msg_publisher": msg_publisher})

            async def start(self) -> None:
                """Record successful startup."""
                await started()

        monkeypatch.setattr("snapper.server.app.AiResearchTriggerService", SucceedingTrigger)
        monkeypatch.setattr("snapper.server.app.get_repository", get_repository)
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_ai_research_trigger(
            app,
            db_url="sqlite+aiosqlite:///:memory:",
            msg_publisher=publisher,
        )

        assert started.await_count == 1
        assert constructed == [{"repo": repository, "msg_publisher": publisher}]
        get_repository.assert_called_once_with("sqlite+aiosqlite:///:memory:")
        assert isinstance(app.state.ai_research_trigger, SucceedingTrigger)


class TestStopAiResearchTrigger:
    """Shutdown helper partial-start behavior."""

    @pytest.mark.asyncio
    async def test_stop_without_attribute_is_noop(self) -> None:
        """Missing startup state is tolerated."""
        app = SimpleNamespace(state=SimpleNamespace())

        await _stop_ai_research_trigger(app)

    @pytest.mark.asyncio
    async def test_stop_awaits_attached_trigger(self) -> None:
        """An attached trigger receives one stop call."""
        trigger = MagicMock(stop=AsyncMock())
        app = SimpleNamespace(state=SimpleNamespace(ai_research_trigger=trigger))

        await _stop_ai_research_trigger(app)

        trigger.stop.assert_awaited_once()
