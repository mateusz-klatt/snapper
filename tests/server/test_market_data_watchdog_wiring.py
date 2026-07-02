"""Tests for the market-data watchdog lifespan wiring in :mod:`snapper.server.app`.

Pins the attach-on-success-only contract shared by every in-API
monitor helper: the ``app.state`` attribute exists ONLY after a
successful ``start()``, startup failure logs without blocking the
rest of the lifespan, and the stop helper tolerates the absent
attribute left behind by a failed start.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.server.app import _start_market_data_watchdog
from snapper.server.app import _stop_market_data_watchdog


class TestStartMarketDataWatchdogHelper:
    """Lifespan startup helper — attach-on-success-only."""

    @pytest.mark.asyncio
    async def test_start_failure_leaves_attribute_absent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failing watchdog start leaves no app.state attribute.

        Given: A watchdog stand-in whose ``start`` raises,
        When: The startup helper runs,
        Then: No exception propagates and ``app.state`` has no
            watchdog attribute.
        """

        class FailingWatchdog:
            """Stand-in that fails its start call."""

            def __init__(self, **_kwargs: object) -> None:
                """Accept the helper's keyword wiring."""

            async def start(self) -> None:
                """Raise to simulate a boot-time failure."""
                raise RuntimeError("synthetic startup failure")

        monkeypatch.setattr("snapper.server.app.MarketDataWatchdog", FailingWatchdog)
        monkeypatch.setattr("snapper.server.app.get_repository", MagicMock())
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_market_data_watchdog(app, db_url="sqlite+aiosqlite:///:memory:")

        assert not hasattr(app.state, "market_data_watchdog")

    @pytest.mark.asyncio
    async def test_start_success_attaches_watchdog_with_wiring(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """On success the watchdog is attached with repo + publisher wired.

        Given: A watchdog stand-in recording its constructor wiring,
        When: The startup helper runs with a publisher,
        Then: The repository from ``get_repository(db_url)`` and the
            publisher are injected, ``start`` ran once, and the
            instance hangs off ``app.state``.
        """
        started = AsyncMock()
        repo_sentinel = MagicMock()
        get_repository = MagicMock(return_value=repo_sentinel)
        publisher = MagicMock()
        constructed: list[dict[str, object]] = []

        class SucceedingWatchdog:
            """Stand-in that records constructor injection."""

            def __init__(self, *, repo: object, msg_publisher: object | None = None) -> None:
                """Capture the helper's wiring."""
                constructed.append({"repo": repo, "msg_publisher": msg_publisher})

            async def start(self) -> None:
                """Record the call without doing real I/O."""
                await started()

        monkeypatch.setattr("snapper.server.app.MarketDataWatchdog", SucceedingWatchdog)
        monkeypatch.setattr("snapper.server.app.get_repository", get_repository)
        app = SimpleNamespace(state=SimpleNamespace())

        await _start_market_data_watchdog(
            app, db_url="sqlite+aiosqlite:///:memory:", msg_publisher=publisher
        )

        assert started.await_count == 1
        assert constructed == [{"repo": repo_sentinel, "msg_publisher": publisher}]
        get_repository.assert_called_once_with("sqlite+aiosqlite:///:memory:")
        assert isinstance(app.state.market_data_watchdog, SucceedingWatchdog)


class TestStopMarketDataWatchdogHelper:
    """Lifespan shutdown helper — tolerates partial-init."""

    @pytest.mark.asyncio
    async def test_stop_when_attribute_absent_is_noop(self) -> None:
        """Shutdown helper exits silently when no watchdog was attached.

        Given: An app whose startup helper never attached a watchdog,
        When: The stop helper runs,
        Then: It returns without raising.
        """
        app = SimpleNamespace(state=SimpleNamespace())

        await _stop_market_data_watchdog(app)

    @pytest.mark.asyncio
    async def test_stop_awaits_attached_watchdog(self) -> None:
        """Shutdown helper stops the attached watchdog.

        Given: An app with an attached watchdog stand-in,
        When: The stop helper runs,
        Then: The watchdog's ``stop`` is awaited exactly once.
        """
        watchdog = MagicMock(stop=AsyncMock())
        app = SimpleNamespace(state=SimpleNamespace(market_data_watchdog=watchdog))

        await _stop_market_data_watchdog(app)

        watchdog.stop.assert_awaited_once()
