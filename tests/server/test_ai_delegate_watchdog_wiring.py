"""Tests for AI-delegate watchdog FastAPI lifespan wiring."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.server.app import _start_ai_delegate_watchdog
from snapper.server.app import _stop_ai_delegate_watchdog


class TestStartAiDelegateWatchdog:
    """Startup helper attach-on-success behavior."""

    def test_start_failure_leaves_attribute_absent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A startup failure is isolated from the rest of lifespan.

        Given: A watchdog stand-in whose start raises,
        When: The startup helper runs,
        Then: No state attribute is attached and no exception escapes.
        """

        class FailingWatchdog:
            """Stand-in that fails during startup."""

            def __init__(self, **_kwargs: object) -> None:
                """Accept the production constructor keywords."""

            def start(self) -> None:
                """Raise a synthetic startup failure."""
                raise RuntimeError("startup failed")

        monkeypatch.setattr("snapper.server.app.AiDelegateWatchdog", FailingWatchdog)
        monkeypatch.setattr("snapper.server.app.get_repository", MagicMock())
        app = SimpleNamespace(state=SimpleNamespace())

        _start_ai_delegate_watchdog(app, db_url="sqlite+aiosqlite:///:memory:")

        assert not hasattr(app.state, "ai_delegate_watchdog")

    def test_start_success_attaches_wired_watchdog(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Successful startup wires the shared repository and publisher.

        Given: A recording watchdog, repository, and publisher,
        When: The startup helper succeeds,
        Then: The started instance is attached with both dependencies.
        """
        started = MagicMock()
        repository = MagicMock()
        publisher = MagicMock()
        get_repository = MagicMock(return_value=repository)
        constructed: list[dict[str, object]] = []

        class SucceedingWatchdog:
            """Stand-in recording constructor and start calls."""

            def __init__(self, *, repo: object, msg_publisher: object | None = None) -> None:
                """Capture production dependency injection.

                Args:
                    repo: Repository sentinel.
                    msg_publisher: Publisher sentinel.
                """
                constructed.append({"repo": repo, "msg_publisher": msg_publisher})

            def start(self) -> None:
                """Record successful startup."""
                started()

        monkeypatch.setattr("snapper.server.app.AiDelegateWatchdog", SucceedingWatchdog)
        monkeypatch.setattr("snapper.server.app.get_repository", get_repository)
        app = SimpleNamespace(state=SimpleNamespace())

        _start_ai_delegate_watchdog(
            app,
            db_url="sqlite+aiosqlite:///:memory:",
            msg_publisher=publisher,
        )

        started.assert_called_once_with()
        assert constructed == [{"repo": repository, "msg_publisher": publisher}]
        get_repository.assert_called_once_with("sqlite+aiosqlite:///:memory:")
        assert isinstance(app.state.ai_delegate_watchdog, SucceedingWatchdog)


class TestStopAiDelegateWatchdog:
    """Shutdown helper partial-start behavior."""

    @pytest.mark.asyncio
    async def test_stop_without_attribute_is_noop(self) -> None:
        """Missing startup state is tolerated.

        Given: An app whose watchdog never attached,
        When: The shutdown helper runs,
        Then: It returns without raising.
        """
        app = SimpleNamespace(state=SimpleNamespace())

        await _stop_ai_delegate_watchdog(app)

    @pytest.mark.asyncio
    async def test_stop_awaits_attached_watchdog(self) -> None:
        """An attached watchdog receives one stop call.

        Given: An app with a started watchdog,
        When: The shutdown helper runs,
        Then: Its asynchronous stop method is awaited once.
        """
        watchdog = MagicMock(stop=AsyncMock())
        app = SimpleNamespace(state=SimpleNamespace(ai_delegate_watchdog=watchdog))

        await _stop_ai_delegate_watchdog(app)

        watchdog.stop.assert_awaited_once()
