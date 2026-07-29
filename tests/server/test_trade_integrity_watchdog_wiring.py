"""Tests for trade-integrity watchdog FastAPI lifespan wiring."""

from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI

from snapper.config.app import AppSettings
from snapper.server.app import _start_trade_integrity_watchdog
from snapper.server.app import _stop_trade_integrity_watchdog


def _app() -> FastAPI:
    """Build a minimal typed application stand-in."""
    return cast(FastAPI, SimpleNamespace(state=SimpleNamespace()))


def _settings(instance_id: int, instance_count: int) -> AppSettings:
    """Build typed coordinator identity settings."""
    return cast(
        AppSettings,
        SimpleNamespace(
            coordinator_instance_id=instance_id,
            coordinator_instance_count=instance_count,
        ),
    )


class TestStartTradeIntegrityWatchdog:
    """Coordinator ownership and attach-on-success behavior."""

    def test_nonzero_coordinator_skips_watchdog(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Only coordinator instance zero owns the shared monitor cursors."""
        watchdog_type = MagicMock()
        get_repository = MagicMock()
        monkeypatch.setattr("snapper.server.app.TradeIntegrityWatchdog", watchdog_type)
        monkeypatch.setattr("snapper.server.app.get_repository", get_repository)
        app = _app()

        _start_trade_integrity_watchdog(
            app,
            db_url="sqlite+aiosqlite:///:memory:",
            settings=_settings(1, 2),
        )

        watchdog_type.assert_not_called()
        get_repository.assert_not_called()
        assert not hasattr(app.state, "trade_integrity_watchdog")

    def test_instance_zero_attaches_started_watchdog(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The owner receives the shared repository and heartbeat publisher."""
        started = MagicMock()
        repository = MagicMock()
        publisher = MagicMock()
        get_repository = MagicMock(return_value=repository)
        constructed: list[dict[str, object]] = []

        class SucceedingWatchdog:
            """Stand-in recording constructor and start calls."""

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
                constructed.append(
                    {
                        "repo": repo,
                        "msg_publisher": msg_publisher,
                    }
                )

            def start(self) -> None:
                """Record successful non-blocking startup."""
                started()

        monkeypatch.setattr("snapper.server.app.TradeIntegrityWatchdog", SucceedingWatchdog)
        monkeypatch.setattr("snapper.server.app.get_repository", get_repository)
        app = _app()

        _start_trade_integrity_watchdog(
            app,
            db_url="sqlite+aiosqlite:///:memory:",
            settings=_settings(0, 2),
            msg_publisher=publisher,
        )

        started.assert_called_once_with()
        get_repository.assert_called_once_with("sqlite+aiosqlite:///:memory:")
        assert constructed == [
            {
                "repo": repository,
                "msg_publisher": publisher,
            }
        ]
        assert isinstance(app.state.trade_integrity_watchdog, SucceedingWatchdog)

    def test_start_failure_leaves_watchdog_absent(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A local startup failure cannot block the rest of API startup."""

        class FailingWatchdog:
            """Stand-in whose start method fails."""

            def __init__(self, **_kwargs: object) -> None:
                """Accept production constructor arguments."""

            def start(self) -> None:
                """Raise a synthetic startup failure."""
                raise RuntimeError("startup failed")

        monkeypatch.setattr("snapper.server.app.TradeIntegrityWatchdog", FailingWatchdog)
        monkeypatch.setattr("snapper.server.app.get_repository", MagicMock())
        app = _app()

        _start_trade_integrity_watchdog(
            app,
            db_url="sqlite+aiosqlite:///:memory:",
            settings=_settings(0, 1),
        )

        assert not hasattr(app.state, "trade_integrity_watchdog")


class TestStopTradeIntegrityWatchdog:
    """Shutdown helper behavior after partial and successful startup."""

    @pytest.mark.asyncio
    async def test_stop_without_watchdog_is_noop(self) -> None:
        """A skipped or failed startup leaves shutdown safe."""
        await _stop_trade_integrity_watchdog(_app())

    @pytest.mark.asyncio
    async def test_stop_awaits_attached_watchdog(self) -> None:
        """The attached owner task is stopped exactly once."""
        watchdog = MagicMock(stop=AsyncMock())
        app = _app()
        app.state.trade_integrity_watchdog = watchdog

        await _stop_trade_integrity_watchdog(app)

        watchdog.stop.assert_awaited_once_with()
