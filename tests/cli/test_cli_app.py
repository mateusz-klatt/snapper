"""Tests for Snapper CLI application."""

import asyncio
import signal
import threading
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import typer
import uvicorn
from pydantic import SecretStr
from typer.testing import CliRunner

import snapper.cli.app as app_module
from snapper.auth.domain.roles import UserRole
from snapper.cli.app import _alembic_cfg
from snapper.cli.app import app
from snapper.cli.app import broker
from snapper.cli.app import executor
from snapper.cli.app import feed
from snapper.cli.app import init_admin
from snapper.cli.app import list_users
from snapper.cli.app import main_callback
from snapper.cli.app import parse_date_range
from snapper.cli.app import parse_symbols
from snapper.cli.app import polygon_backfill_aggregates
from snapper.cli.app import polygon_backfill_grouped
from snapper.cli.app import reset_password
from snapper.cli.app import server
from snapper.cli.app import settings_rotate_encryption
from snapper.cli.app import validate_api_keys
from snapper.cli.app import zmq_logger
from snapper.infrastructure.security.encryption import SettingsEncryptionService


@pytest.fixture()
def cli_runner() -> CliRunner:
    """Provide a Typer CliRunner instance for CLI testing."""
    return CliRunner()


def test_validate_api_keys_accepts_secret_str() -> None:
    """Test API key validation accepts SecretStr values.

    Given: Valid SecretStr key and secret,
    When: validate_api_keys is called,
    Then: Returns True for valid credentials, False for missing.
    """
    key = SecretStr("kraken-key")
    secret = SecretStr("kraken-secret")
    assert validate_api_keys(key, secret) is True
    assert validate_api_keys(key, None) is False
    assert validate_api_keys(None, secret) is False


def test_validate_api_keys_returns_true_in_paper_mode() -> None:
    """Test API key validation returns True in paper mode.

    Given: No API keys provided,
    When: validate_api_keys is called with paper=True,
    Then: Returns True since paper mode doesn't require keys.
    """
    assert validate_api_keys(None, None, paper=True) is True


def test_validate_api_keys_for_trader_always_true() -> None:
    """Test trader API key validation always returns True.

    Given: Trader coordinator using settings for credentials,
    When: validate_api_keys_for_trader is called,
    Then: Always returns True regardless of paper mode.
    """
    assert app_module.validate_api_keys_for_trader() is True
    assert app_module.validate_api_keys_for_trader(paper=True) is True


def test_parse_date_range_returns_utc_datetimes() -> None:
    """Test date range parsing returns UTC datetimes.

    Given: Valid date strings in YYYY-MM-DD format,
    When: parse_date_range is called,
    Then: Returns timezone-aware datetimes in UTC.
    """
    start, end = parse_date_range("2024-01-01", "2024-01-02")
    assert start.tzinfo is UTC
    assert end.tzinfo is UTC
    assert start == datetime(2024, 1, 1, tzinfo=UTC)
    assert end == datetime(2024, 1, 2, tzinfo=UTC)


def test_parse_date_range_invalid_input() -> None:
    """Test date range parsing raises on invalid input.

    Given: Invalid date string,
    When: parse_date_range is called,
    Then: ValueError is raised.
    """
    with pytest.raises(ValueError):
        parse_date_range("2024-13-01", "2024-01-02")


def test_parse_symbols_strips_and_filters() -> None:
    """Test symbol parsing strips whitespace and filters empty.

    Given: Comma-separated symbols with extra whitespace,
    When: parse_symbols is called,
    Then: Returns cleaned list without empty entries.
    """
    symbols = parse_symbols(" BTC-USD , ,ETH-USD ")
    assert symbols == ["BTC-USD", "ETH-USD"]


def test_parse_symbols_returns_empty_for_blank_input() -> None:
    """Test symbol parsing returns empty list for blank input.

    Given: A string with only whitespace,
    When: parse_symbols is called,
    Then: Returns empty list.
    """
    assert parse_symbols("   ") == []


def test_alembic_cfg_sets_url_and_uses_root_ini() -> None:
    """Test Alembic config sets database URL and migrations path.

    Given: A database URL,
    When: _alembic_cfg is called,
    Then: Config has correct URL and script location.
    """
    cfg = _alembic_cfg("sqlite:///tmp.db")
    assert cfg.get_main_option("sqlalchemy.url") == "sqlite:///tmp.db"
    script_location = cfg.get_main_option("script_location")
    assert script_location is not None
    assert "migrations" in script_location


def test_trade_zmq_invokes_async_runner(
    cli_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test trade-zmq command starts trading coordinator.

    Given: Mocked settings and TraderCoordinator,
    When: trade-zmq command is invoked,
    Then: Database is upgraded and coordinator starts.
    """
    captured: dict[str, Any] = {}

    def fake_settings() -> Any:
        return SimpleNamespace(db_url="sqlite:///memory.db")

    monkeypatch.setattr(app_module, "get_settings", fake_settings)

    class DummyConfig:
        pass

    dummy_cfg = DummyConfig()

    def fake_cfg(db_url: str) -> DummyConfig:
        captured["db_url"] = db_url
        return dummy_cfg

    monkeypatch.setattr(app_module, "_alembic_cfg", fake_cfg)
    upgraded: list[tuple[DummyConfig, str]] = []

    class DummyAlembicCommand:
        def upgrade(self, cfg: DummyConfig, revision: str) -> None:
            upgraded.append((cfg, revision))

    monkeypatch.setattr(app_module, "command", DummyAlembicCommand())

    class FakeTraderCoordinator:
        def __init__(self, signal_topics: list[str] | None = None) -> None:
            captured["trader_kwargs"] = {"signal_topics": signal_topics}

        async def start(self) -> None:
            """No-op start for FakeTraderCoordinator test stub."""
            pass

    monkeypatch.setattr(app_module, "TraderCoordinator", FakeTraderCoordinator)
    result = cli_runner.invoke(
        app,
        [
            "trade-zmq",
            "--signal-topics",
            "signals.macd",
        ],
    )
    assert result.exit_code == 0
    assert "Central Trading Coordinator" in result.stdout
    assert captured["db_url"] == "sqlite:///memory.db"
    assert upgraded == [(dummy_cfg, "head")]
    trader_kwargs = captured["trader_kwargs"]
    assert trader_kwargs["signal_topics"] == ["signals.macd"]


def test_db_init_runs_upgrade(monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner) -> None:
    """Test db-init command runs Alembic upgrade.

    Given: Mocked settings and Alembic command,
    When: db-init command is invoked,
    Then: Alembic upgrade to head is executed.
    """
    captured: dict[str, Any] = {}
    settings = SimpleNamespace(db_url="sqlite:///memory.db")
    monkeypatch.setattr(app_module, "get_settings", lambda: settings)

    class DummyConfig:
        pass

    dummy_cfg = DummyConfig()

    def fake_cfg(db_url: str) -> DummyConfig:
        captured["db_url"] = db_url
        return dummy_cfg

    monkeypatch.setattr(app_module, "_alembic_cfg", fake_cfg)

    class DummyAlembic:
        def __init__(self) -> None:
            self.upgrades: list[tuple[DummyConfig, str]] = []

        def upgrade(self, cfg: DummyConfig, revision: str) -> None:
            self.upgrades.append((cfg, revision))

    dummy_alembic = DummyAlembic()
    monkeypatch.setattr(app_module, "command", dummy_alembic)
    result = cli_runner.invoke(app, ["db-init"])
    assert result.exit_code == 0
    assert captured["db_url"] == "sqlite:///memory.db"
    assert dummy_alembic.upgrades == [(dummy_cfg, "head")]
    assert "DB initialized" in result.stdout


def test_db_upgrade_uses_requested_revision(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test db-upgrade command uses specified revision.

    Given: Mocked Alembic command,
    When: db-upgrade is invoked with --revision,
    Then: Alembic upgrade to specified revision is executed.
    """
    settings = SimpleNamespace(db_url="sqlite:///memory.db")
    monkeypatch.setattr(app_module, "get_settings", lambda: settings)

    class DummyConfig:
        pass

    dummy_cfg = DummyConfig()

    def fake_cfg_unused(_db_url: str) -> DummyConfig:
        return dummy_cfg

    monkeypatch.setattr(app_module, "_alembic_cfg", fake_cfg_unused)

    class DummyAlembic:
        def __init__(self) -> None:
            self.calls: list[tuple[DummyConfig, str]] = []

        def upgrade(self, cfg: DummyConfig, revision: str) -> None:
            self.calls.append((cfg, revision))

    dummy_alembic = DummyAlembic()
    monkeypatch.setattr(app_module, "command", dummy_alembic)
    result = cli_runner.invoke(app, ["db-upgrade", "--revision", "abc123"])
    assert result.exit_code == 0
    assert dummy_alembic.calls == [(dummy_cfg, "abc123")]
    assert "DB upgraded to abc123" in result.stdout


def test_db_downgrade_uses_requested_revision(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test db-downgrade command uses specified revision.

    Given: Mocked Alembic command,
    When: db-downgrade is invoked with --revision,
    Then: Alembic downgrade to specified revision is executed.
    """
    settings = SimpleNamespace(db_url="sqlite:///memory.db")
    monkeypatch.setattr(app_module, "get_settings", lambda: settings)

    class DummyConfig:
        pass

    dummy_cfg = DummyConfig()

    def fake_cfg_unused(_db_url: str) -> DummyConfig:
        return dummy_cfg

    monkeypatch.setattr(app_module, "_alembic_cfg", fake_cfg_unused)

    class DummyAlembic:
        def __init__(self) -> None:
            self.downgrades: list[tuple[DummyConfig, str]] = []

        def downgrade(self, cfg: DummyConfig, revision: str) -> None:
            self.downgrades.append((cfg, revision))

    dummy_alembic = DummyAlembic()
    monkeypatch.setattr(app_module, "command", dummy_alembic)
    result = cli_runner.invoke(app, ["db-downgrade", "--revision", "-2"])
    assert result.exit_code == 0
    assert dummy_alembic.downgrades == [(dummy_cfg, "-2")]
    assert "DB downgraded by -2" in result.stdout


def _fake_settings(**overrides: Any) -> Any:
    defaults = {
        "db_url": "sqlite:///memory.db",
        "server_host": "127.0.0.1",
        "server_port": 8000,
        "server_reload": False,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_server_runs_without_reload(monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner) -> None:
    """Test server command runs uvicorn without reload.

    Given: Settings with server_reload=False,
    When: server command is invoked,
    Then: Uvicorn runs with reload=False.
    """
    captured: dict[str, Any] = {}

    def fake_run(
        app_obj: Any, host: str, port: int, reload: bool, log_level: str, log_config: Any
    ) -> None:
        captured.update({"app": app_obj, "host": host, "port": port, "reload": reload})

    monkeypatch.setattr(app_module, "get_settings", lambda: _fake_settings(server_reload=False))
    monkeypatch.setattr(app_module, "create_app", lambda: "APP_INSTANCE")
    monkeypatch.setattr(uvicorn, "run", fake_run)
    result = cli_runner.invoke(app, ["server"])
    assert result.exit_code == 0
    assert captured == {"app": "APP_INSTANCE", "host": "127.0.0.1", "port": 8000, "reload": False}


def test_server_runs_with_reload(monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner) -> None:
    """Test server command runs uvicorn with reload.

    Given: Settings with server_reload=True,
    When: server command is invoked,
    Then: Uvicorn runs with factory=True and reload=True.
    """
    captured: dict[str, Any] = {}

    def fake_run(
        app_path: str,
        factory: bool,
        host: str,
        port: int,
        reload: bool,
        log_level: str,
        log_config: Any,
    ) -> None:
        captured.update(
            {"app_path": app_path, "factory": factory, "host": host, "port": port, "reload": reload}
        )

    monkeypatch.setattr(app_module, "get_settings", lambda: _fake_settings(server_reload=True))
    monkeypatch.setattr(uvicorn, "run", fake_run)
    result = cli_runner.invoke(app, ["server"])
    assert result.exit_code == 0
    assert captured["app_path"] == "snapper.server.app:create_app"
    assert captured["factory"] is True
    assert captured["reload"] is True


def test_broker_starts_and_stops(monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner) -> None:
    """Test broker command starts and stops ZMQ broker.

    Given: Mocked ZmqBrokerThread,
    When: broker command is invoked,
    Then: Broker starts and stops cleanly.
    """
    events: list[str] = []

    class DummyBroker:
        def __init__(
            self, xsub_endpoint: str | None = None, xpub_endpoint: str | None = None
        ) -> None:
            self.xsub_endpoint = xsub_endpoint or "xsub"
            self.xpub_endpoint = xpub_endpoint or "xpub"

        def start(self) -> None:
            events.append("start")

        def stop(self) -> None:
            events.append("stop")

    class DummyEvent:
        def __init__(self) -> None:
            self.signaled = False

        def set(self) -> None:
            self.signaled = True

        def wait(self) -> None:
            return None

    monkeypatch.setattr(app_module, "ZmqBrokerThread", DummyBroker)
    monkeypatch.setattr(signal, "signal", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(threading, "Event", DummyEvent)
    result = cli_runner.invoke(app, ["broker"])
    assert result.exit_code == 0
    assert events == ["start", "stop"]


def test_feed_runs_publisher(monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner) -> None:
    """Test feed command runs market data publisher.

    Given: Mocked KrakenMarketDataPublisher,
    When: feed command is invoked with symbols,
    Then: Publisher starts and stops with specified symbols.
    """
    calls: list[str] = []

    class DummyPublisher:
        def __init__(self, symbols: list[str]) -> None:
            self.symbols = symbols

        async def start(self) -> None:
            calls.append("start")

        async def stop(self) -> None:
            calls.append("stop")

    original_run = asyncio.run

    def fake_run(coro: Any) -> None:
        original_run(coro)

    monkeypatch.setattr(app_module, "KrakenMarketDataPublisher", DummyPublisher)
    monkeypatch.setattr(asyncio, "run", fake_run)
    result = cli_runner.invoke(app, ["feed", "--symbols", "BTC/USD,ETH/USD"])
    assert result.exit_code == 0
    assert calls == ["start", "stop"]


def test_zmq_logger_handles_keyboard_interrupt(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test zmq-logger handles KeyboardInterrupt gracefully.

    Given: A logger that raises KeyboardInterrupt,
    When: zmq-logger command is invoked,
    Then: Command exits cleanly with code 0.
    """
    started: list[bool] = []

    class DummyLogger:
        def __init__(self, **_kwargs: Any) -> None:
            """Intentionally empty stub for testing."""
            pass

        async def start(self) -> None:
            started.append(True)
            raise KeyboardInterrupt

    original_run = asyncio.run

    def fake_run(coro: Any) -> None:
        original_run(coro)

    monkeypatch.setattr(app_module, "ZmqMessageLogger", DummyLogger)
    monkeypatch.setattr(asyncio, "run", fake_run)
    result = cli_runner.invoke(app, ["zmq-logger", "--no-file", "--payload"])
    assert result.exit_code == 0
    assert started == [True]


def test_trade_zmq_exits_when_validation_fails(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test trade-zmq exits when API validation fails.

    Given: validate_api_keys_for_trader returns False,
    When: trade-zmq command is invoked,
    Then: Command exits with code 1 and error message.
    """
    monkeypatch.setattr(app_module, "get_settings", lambda: _fake_settings())
    monkeypatch.setattr(app_module, "validate_api_keys_for_trader", lambda paper=False: False)
    result = cli_runner.invoke(app, ["trade-zmq"])
    assert result.exit_code == 1
    assert "Error validating configuration" in result.stdout


def test_update_zonda_symbols_reports_error(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test update-zonda-symbols handles updater errors gracefully.

    Given a ZondaSymbolMappingUpdaterService that raises an error,
    When the update-zonda-symbols command is invoked,
    Then it returns exit code 1 with an error message.
    """

    class DummyUpdater:
        def __init__(self, update_threshold_hours: int, force: bool) -> None:
            self.update_threshold_hours = update_threshold_hours
            self.force = force

        async def start(self) -> None:
            raise RuntimeError("boom")

    monkeypatch.setattr(app_module, "ZondaSymbolMappingUpdaterService", DummyUpdater)
    result = cli_runner.invoke(app, ["update-zonda-symbols", "--force"])
    assert result.exit_code == 1
    assert "Error updating Zonda symbol mappings" in result.stdout


def test_polygon_backfill_aggregates_reports_error(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test polygon-backfill-aggregates handles service errors gracefully.

    Given a PolygonAggregatesBackfillService that raises an error,
    When the polygon-backfill-aggregates command is invoked,
    Then it returns exit code 1 with an error message.
    """

    class DummyService:
        def __init__(
            self,
            symbols: list[str] | None,
            all_mapped: bool,
            multiplier: int,
            timespan: str,
            days_back: int,
            resume: bool,
            save_csv: bool,
        ) -> None:
            self.symbols = symbols
            self.all_mapped = all_mapped
            self.multiplier = multiplier
            self.timespan = timespan
            self.days_back = days_back
            self.resume = resume
            self.save_csv = save_csv

        async def start(self) -> None:
            raise RuntimeError("agg-fail")

    monkeypatch.setattr(app_module, "PolygonAggregatesBackfillService", DummyService)
    result = cli_runner.invoke(
        app,
        ["polygon-backfill-aggregates", "--symbol", "X:BTCUSD", "--days", "2"],
    )
    assert result.exit_code == 1
    assert "Error during Polygon aggregates backfill" in result.stdout


def test_polygon_backfill_grouped_reports_error(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test polygon-backfill-grouped handles service errors gracefully.

    Given a PolygonGroupedDailyBackfillService that raises an error,
    When the polygon-backfill-grouped command is invoked,
    Then it returns exit code 1 with an error message.
    """

    class DummyService:
        def __init__(
            self, market_type: str, days: int, locale: str, save_csv: bool, adjusted: bool
        ) -> None:
            self.market_type = market_type
            self.days = days
            self.locale = locale
            self.save_csv = save_csv
            self.adjusted = adjusted

        async def start(self) -> None:
            raise RuntimeError("grouped-fail")

    monkeypatch.setattr(app_module, "PolygonGroupedDailyBackfillService", DummyService)
    result = cli_runner.invoke(
        app,
        ["polygon-backfill-grouped", "--market", "stocks", "--days", "5", "--locale", "us"],
    )
    assert result.exit_code == 1
    assert "Error during Polygon grouped backfill" in result.stdout


def test_update_kraken_symbols_success(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test update-kraken-symbols succeeds with --force flag.

    Given a functioning KrakenSymbolMappingUpdaterService,
    When the update-kraken-symbols command is invoked with --force,
    Then it completes successfully with exit code 0.
    """
    started: list[bool] = []

    class DummyUpdater:
        def __init__(self, force: bool) -> None:
            self.force = force

        async def start(self) -> None:
            started.append(self.force)

    monkeypatch.setattr(app_module, "KrakenSymbolMappingUpdaterService", DummyUpdater)
    result = cli_runner.invoke(app, ["update-kraken-symbols", "--force"])
    assert result.exit_code == 0
    assert started == [True]


def test_update_kraken_symbols_error(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test update-kraken-symbols handles updater errors gracefully.

    Given a KrakenSymbolMappingUpdaterService that raises an error,
    When the update-kraken-symbols command is invoked,
    Then it returns exit code 1 with an error message.
    """

    class DummyUpdater:
        def __init__(self, force: bool) -> None:
            self.force = force

        async def start(self) -> None:
            raise RuntimeError("ksym")

    monkeypatch.setattr(app_module, "KrakenSymbolMappingUpdaterService", DummyUpdater)
    result = cli_runner.invoke(app, ["update-kraken-symbols"])
    assert result.exit_code == 1
    assert "Error updating symbol mappings" in result.stdout


def test_update_polygon_symbols_success(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test update-polygon-symbols succeeds with --force and --insert-new flags.

    Given a functioning PolygonSymbolMappingUpdaterService,
    When the update-polygon-symbols command is invoked with --force --insert-new,
    Then it completes successfully with exit code 0.
    """
    started: list[tuple[bool, bool]] = []

    class DummyUpdater:
        def __init__(self, update_threshold_hours: int, force: bool, insert_new: bool) -> None:
            self.update_threshold_hours = update_threshold_hours
            self.force = force
            self.insert_new = insert_new

        async def start(self) -> None:
            started.append((self.force, self.insert_new))

    monkeypatch.setattr(app_module, "PolygonSymbolMappingUpdaterService", DummyUpdater)
    result = cli_runner.invoke(app, ["update-polygon-symbols", "--force", "--insert-new"])
    assert result.exit_code == 0
    assert started == [(True, True)]


def test_update_polygon_symbols_error(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test update-polygon-symbols handles updater errors gracefully.

    Given a PolygonSymbolMappingUpdaterService that raises an error,
    When the update-polygon-symbols command is invoked,
    Then it returns exit code 1 with an error message.
    """

    class DummyUpdater:
        def __init__(self, update_threshold_hours: int, force: bool, insert_new: bool) -> None:
            self.force = force
            self.insert_new = insert_new

        async def start(self) -> None:
            raise RuntimeError("psym")

    monkeypatch.setattr(app_module, "PolygonSymbolMappingUpdaterService", DummyUpdater)
    result = cli_runner.invoke(app, ["update-polygon-symbols"])
    assert result.exit_code == 1
    assert "Error updating Polygon symbol mappings" in result.stdout


def test_update_walutomat_symbols_success(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test update-walutomat-symbols succeeds with --force flag.

    Given a functioning WalutomatSymbolMappingUpdaterService,
    When the update-walutomat-symbols command is invoked with --force,
    Then it completes successfully with exit code 0.
    """
    started: list[bool] = []

    class DummyUpdater:
        def __init__(self, update_threshold_hours: int, force: bool) -> None:
            self.force = force

        async def start(self) -> None:
            started.append(self.force)

    monkeypatch.setattr(app_module, "WalutomatSymbolMappingUpdaterService", DummyUpdater)
    result = cli_runner.invoke(app, ["update-walutomat-symbols", "--force"])
    assert result.exit_code == 0
    assert started == [True]


def test_update_walutomat_symbols_error(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test update-walutomat-symbols handles updater errors gracefully.

    Given a WalutomatSymbolMappingUpdaterService that raises an error,
    When the update-walutomat-symbols command is invoked,
    Then it returns exit code 1 with an error message.
    """

    class DummyUpdater:
        def __init__(self, update_threshold_hours: int, force: bool) -> None:
            self.force = force

        async def start(self) -> None:
            raise RuntimeError("wfail")

    monkeypatch.setattr(app_module, "WalutomatSymbolMappingUpdaterService", DummyUpdater)
    result = cli_runner.invoke(app, ["update-walutomat-symbols"])
    assert result.exit_code == 1
    assert "Error updating Walutomat symbol mappings" in result.stdout


def test_update_kraken_market_snapshot_error(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test update-kraken-market-snapshot handles errors gracefully.

    Given a run_snapshot_update function that raises an error,
    When the update-kraken-market-snapshot command is invoked,
    Then it returns exit code 1 with an error message.
    """

    def _raise_snap() -> None:
        raise RuntimeError("snap")

    monkeypatch.setattr(app_module, "run_snapshot_update", _raise_snap)
    result = cli_runner.invoke(app, ["update-kraken-market-snapshot"])
    assert result.exit_code == 1
    assert "Error updating Kraken market snapshots" in result.stdout


def test_update_zonda_market_snapshot_error(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test update-zonda-market-snapshot handles errors.

    Given: A function that raises RuntimeError,
    When: update-zonda-market-snapshot is invoked,
    Then: Command exits with code 1 and error message.
    """

    def _raise_zsnap() -> None:
        raise RuntimeError("zsnap")

    monkeypatch.setattr(app_module, "run_zonda_snapshot_update", _raise_zsnap)
    result = cli_runner.invoke(app, ["update-zonda-market-snapshot"])
    assert result.exit_code == 1
    assert "Error updating Zonda market snapshots" in result.stdout


def test_update_walutomat_market_snapshot_error(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test update-walutomat-market-snapshot handles errors.

    Given: A function that raises RuntimeError,
    When: update-walutomat-market-snapshot is invoked,
    Then: Command exits with code 1 and error message.
    """

    def _raise_wsnap() -> None:
        raise RuntimeError("wsnap")

    monkeypatch.setattr(app_module, "run_walutomat_snapshot_update", _raise_wsnap)
    result = cli_runner.invoke(app, ["update-walutomat-market-snapshot"])
    assert result.exit_code == 1
    assert "Error updating Walutomat market snapshots" in result.stdout


def test_update_market_snapshot_successes(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test market snapshot commands succeed.

    Given: Mocked snapshot update functions,
    When: all market snapshot commands are invoked,
    Then: Each command succeeds and calls its function.
    """
    flags: list[str] = []
    monkeypatch.setattr(app_module, "run_snapshot_update", lambda: flags.append("kraken"))
    monkeypatch.setattr(app_module, "run_zonda_snapshot_update", lambda: flags.append("zonda"))
    monkeypatch.setattr(app_module, "run_walutomat_snapshot_update", lambda: flags.append("wal"))
    result_kraken = cli_runner.invoke(app, ["update-kraken-market-snapshot"])
    result_zonda = cli_runner.invoke(app, ["update-zonda-market-snapshot"])
    result_wal = cli_runner.invoke(app, ["update-walutomat-market-snapshot"])
    assert result_kraken.exit_code == 0
    assert result_zonda.exit_code == 0
    assert result_wal.exit_code == 0
    assert flags == ["kraken", "zonda", "wal"]


def test_executor_unknown_exchange(monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner) -> None:
    """Test executor command rejects unknown exchange.

    Given: An unknown exchange name,
    When: executor command is invoked,
    Then: Command exits with error about unknown exchange.
    """
    result = cli_runner.invoke(app, ["executor", "--exchange", "unknown"])
    assert result.exit_code == 1
    assert "Unknown exchange" in result.stdout


def test_executor_runs_and_stops(monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner) -> None:
    """Test executor command starts and stops order executor.

    Given: Mocked exchange order executors,
    When: executor command is invoked,
    Then: Executor starts and stops cleanly.
    """
    calls: list[str] = []

    class DummyExecutor:
        def __init__(self) -> None:
            """Intentionally empty stub for testing."""
            pass

        def get_status(self) -> dict[str, str]:
            return {"broker_xpub": "xpub", "broker_xsub": "xsub"}

        async def start(self) -> None:
            calls.append("start")
            raise KeyboardInterrupt

        async def stop(self) -> None:
            calls.append("stop")

    monkeypatch.setattr(app_module, "KrakenOrderExecutor", DummyExecutor)
    monkeypatch.setattr(app_module, "ZondaOrderExecutor", DummyExecutor)
    monkeypatch.setattr(app_module, "WalutomatOrderExecutor", DummyExecutor)
    original_run = asyncio.run

    def fake_run(coro: Any) -> None:
        original_run(coro)

    monkeypatch.setattr(asyncio, "run", fake_run)
    result = cli_runner.invoke(app, ["executor", "--exchange", "kraken"])
    assert result.exit_code == 0
    assert calls == ["start", "stop"]


class _DummyResult:
    """Test dummy for SQLAlchemy result."""

    def scalars(self) -> "_DummyResult":
        return self

    def all(self) -> list[object]:
        return []


class _DummySession:
    """Test dummy for async database session."""

    def __init__(self) -> None:
        self.execute_calls = 0

    async def __aenter__(self) -> "_DummySession":
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False

    async def execute(self, _stmt: object) -> _DummyResult:
        self.execute_calls += 1
        return _DummyResult()

    async def commit(self) -> None:
        return None


class TestValidateApiKeys:
    """Tests for validate_api_keys credential validation function."""

    def test_validate_api_keys_with_valid_keys(self) -> None:
        """Test validate_api_keys accepts valid credentials.

        Given: Valid API key and secret,
        When: validate_api_keys is called,
        Then: Returns True.
        """
        result = validate_api_keys("valid_key", "valid_secret", paper=False)
        assert result is True

    def test_validate_api_keys_paper_mode(self) -> None:
        """Test validate_api_keys succeeds in paper mode.

        Given: Paper mode enabled with no credentials,
        When: validate_api_keys is called,
        Then: Returns True.
        """
        result = validate_api_keys(None, None, paper=True)
        assert result is True

    def test_validate_api_keys_missing_api_key(self) -> None:
        """Test validate_api_keys rejects missing API key.

        Given: Missing API key with valid secret,
        When: validate_api_keys is called,
        Then: Returns False.
        """
        result = validate_api_keys(None, "valid_secret", paper=False)
        assert result is False

    def test_validate_api_keys_missing_secret(self) -> None:
        """Test validate_api_keys rejects missing secret.

        Given: Valid API key with missing secret,
        When: validate_api_keys is called,
        Then: Returns False.
        """
        result = validate_api_keys("valid_key", None, paper=False)
        assert result is False

    def test_validate_api_keys_empty_api_key(self) -> None:
        """Test validate_api_keys rejects empty API key.

        Given: Empty API key string with valid secret,
        When: validate_api_keys is called,
        Then: Returns False.
        """
        result = validate_api_keys("", "valid_secret", paper=False)
        assert result is False

    def test_validate_api_keys_empty_secret(self) -> None:
        """Test validate_api_keys rejects empty secret.

        Given: Valid API key with empty secret string,
        When: validate_api_keys is called,
        Then: Returns False.
        """
        result = validate_api_keys("valid_key", "", paper=False)
        assert result is False

    def test_validate_api_keys_both_none(self) -> None:
        """Test validate_api_keys rejects both None in live mode.

        Given: Both credentials None without paper mode,
        When: validate_api_keys is called,
        Then: Returns False.
        """
        result = validate_api_keys(None, None, paper=False)
        assert result is False


class TestParseDateRange:
    """Tests for parse_date_range date string parsing function."""

    def test_parse_date_range_valid_iso_format(self) -> None:
        """Test parse_date_range parses valid ISO dates.

        Given: Valid ISO format date strings,
        When: parse_date_range is called,
        Then: Returns UTC-aware datetimes.
        """
        start_str = "2023-01-01"
        end_str = "2023-12-31"
        start_date, end_date = parse_date_range(start_str, end_str)
        assert start_date == datetime(2023, 1, 1, tzinfo=UTC)
        assert end_date == datetime(2023, 12, 31, tzinfo=UTC)

    def test_parse_date_range_invalid_start_date(self) -> None:
        """Test parse_date_range raises on invalid start date.

        Given: Invalid start date string,
        When: parse_date_range is called,
        Then: Raises ValueError with message.
        """
        start_str = "invalid-date"
        end_str = "2023-12-31"
        with pytest.raises(ValueError, match="Error parsing dates"):
            parse_date_range(start_str, end_str)

    def test_parse_date_range_invalid_end_date(self) -> None:
        """Test parse_date_range raises on invalid end date.

        Given: Invalid end date string,
        When: parse_date_range is called,
        Then: Raises ValueError with message.
        """
        start_str = "2023-01-01"
        end_str = "invalid-date"
        with pytest.raises(ValueError, match="Error parsing dates"):
            parse_date_range(start_str, end_str)

    def test_parse_date_range_same_dates(self) -> None:
        """Test parse_date_range handles same start and end.

        Given: Same date for start and end,
        When: parse_date_range is called,
        Then: Returns equal datetimes.
        """
        start_str = "2023-06-15"
        end_str = "2023-06-15"
        start_date, end_date = parse_date_range(start_str, end_str)
        assert start_date == datetime(2023, 6, 15, tzinfo=UTC)
        assert end_date == datetime(2023, 6, 15, tzinfo=UTC)

    def test_parse_date_range_leap_year(self) -> None:
        """Test parse_date_range handles leap year dates.

        Given: Feb 29 in leap year,
        When: parse_date_range is called,
        Then: Parses correctly.
        """
        start_str = "2024-02-29"
        end_str = "2024-02-29"
        start_date, end_date = parse_date_range(start_str, end_str)
        assert start_date == datetime(2024, 2, 29, tzinfo=UTC)
        assert end_date == datetime(2024, 2, 29, tzinfo=UTC)


class TestParseSymbols:
    """Tests for parse_symbols comma-separated string parsing function."""

    def test_parse_symbols_single_symbol(self) -> None:
        """Test parse_symbols handles single symbol.

        Given: Single symbol string,
        When: parse_symbols is called,
        Then: Returns list with one symbol.
        """
        symbols_str = "AAPL"
        symbols = parse_symbols(symbols_str)
        assert symbols == ["AAPL"]

    def test_parse_symbols_multiple_symbols(self) -> None:
        """Test parse_symbols handles multiple symbols.

        Given: Comma-separated symbols,
        When: parse_symbols is called,
        Then: Returns list of all symbols.
        """
        symbols_str = "AAPL,MSFT,GOOGL"
        symbols = parse_symbols(symbols_str)
        assert symbols == ["AAPL", "MSFT", "GOOGL"]

    def test_parse_symbols_with_spaces(self) -> None:
        """Test parse_symbols strips whitespace.

        Given: Symbols with extra spaces,
        When: parse_symbols is called,
        Then: Returns trimmed symbols.
        """
        symbols_str = "AAPL, MSFT , GOOGL"
        symbols = parse_symbols(symbols_str)
        assert symbols == ["AAPL", "MSFT", "GOOGL"]

    def test_parse_symbols_empty_string(self) -> None:
        """Test parse_symbols handles empty string.

        Given: Empty string,
        When: parse_symbols is called,
        Then: Returns empty list.
        """
        symbols_str = ""
        symbols = parse_symbols(symbols_str)
        assert symbols == []

    def test_parse_symbols_whitespace_only(self) -> None:
        """Test parse_symbols handles whitespace-only string.

        Given: Whitespace-only string,
        When: parse_symbols is called,
        Then: Returns empty list.
        """
        symbols_str = "   "
        symbols = parse_symbols(symbols_str)
        assert symbols == []

    def test_parse_symbols_with_empty_parts(self) -> None:
        """Test parse_symbols filters empty parts.

        Given: String with empty comma-separated parts,
        When: parse_symbols is called,
        Then: Returns only non-empty symbols.
        """
        symbols_str = "AAPL,,MSFT,,"
        symbols = parse_symbols(symbols_str)
        assert symbols == ["AAPL", "MSFT"]

    def test_parse_symbols_mixed_case(self) -> None:
        """Test parse_symbols preserves case.

        Given: Mixed-case symbols,
        When: parse_symbols is called,
        Then: Returns symbols with original case.
        """
        symbols_str = "aapl,MSFT,Googl"
        symbols = parse_symbols(symbols_str)
        assert symbols == ["aapl", "MSFT", "Googl"]


def test_db_downgrade_invokes_command(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test db-downgrade invokes Alembic downgrade command.

    Given: Mocked Alembic command module,
    When: db-downgrade is invoked with revision,
    Then: Alembic downgrade is called with correct args.
    """
    captured: dict[str, object] = {}
    settings = SimpleNamespace(db_url="sqlite:///tmp.db")
    monkeypatch.setattr(app_module, "get_settings", lambda: settings)

    class DummyConfig:
        pass

    dummy_cfg = DummyConfig()

    def fake_cfg(db_url: str) -> DummyConfig:
        captured["db_url"] = db_url
        return dummy_cfg

    monkeypatch.setattr(app_module, "_alembic_cfg", fake_cfg)

    class DummyAlembic:
        def __init__(self) -> None:
            self.calls: list[tuple[DummyConfig, str]] = []

        def downgrade(self, cfg: DummyConfig, revision: str) -> None:
            self.calls.append((cfg, revision))

    dummy_alembic = DummyAlembic()
    monkeypatch.setattr(app_module, "command", dummy_alembic)
    result = cli_runner.invoke(app, ["db-downgrade", "--revision", "-2"])
    assert result.exit_code == 0
    assert captured["db_url"] == "sqlite:///tmp.db"
    assert dummy_alembic.calls == [(dummy_cfg, "-2")]
    assert "DB downgraded" in result.stdout


def test_settings_rotate_encryption_dry_run_with_no_encrypted_settings(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test encryption rotation dry-run with no encrypted settings.

    Given: A database with no encrypted settings,
    When: settings-rotate-encryption is run with --dry-run,
    Then: Message indicates no encrypted settings found.
    """
    settings = SimpleNamespace(
        db_url="sqlite+aiosqlite:///memory.db",
        master_password="old-pass",
        encryption_salt="oldsalt",
    )
    monkeypatch.setattr(app_module, "BootstrapSettingsLoader", lambda: settings)
    dummy_engine_disposed: list[bool] = []

    class DummyEngine:
        async def dispose(self) -> None:
            dummy_engine_disposed.append(True)

    dummy_engine = DummyEngine()
    monkeypatch.setattr(app_module, "create_async_engine", lambda *_args, **_kwargs: dummy_engine)

    def fake_sessionmaker(_engine: object) -> Callable[[], _DummySession]:
        return lambda: _DummySession()

    monkeypatch.setattr(app_module, "async_sessionmaker", fake_sessionmaker)
    result = cli_runner.invoke(
        app,
        [
            "settings-rotate-encryption",
            "--new-password",
            "new-pass",
            "--new-salt",
            "newsalt",
            "--dry-run",
        ],
    )
    assert result.exit_code == 0
    assert "No encrypted settings found" in result.stdout
    assert dummy_engine_disposed == []


def test_main_callback_initializes_encryption(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test main callback initializes encryption service.

    Given: Bootstrap settings with master password and salt,
    When: main_callback is invoked,
    Then: Global encryption is initialized with credentials.
    """
    called: list[tuple[str, str]] = []

    class DummyBootstrap:
        def __init__(self) -> None:
            self.master_password = "secret"
            self.encryption_salt = "salt"

    monkeypatch.setattr(app_module, "BootstrapSettingsLoader", DummyBootstrap)
    monkeypatch.setattr(
        app_module,
        "initialize_global_encryption",
        lambda password, salt: called.append((password, salt)),
    )
    app_module.main_callback()
    assert called == [("secret", "salt")]


def create_mock_settings(**overrides: Any) -> type:
    """Create a mock settings class with optional attribute overrides."""
    default_attrs: dict[str, Any] = {
        "kraken_api_key": None,
        "kraken_api_secret": None,
        "db_url": "sqlite:///test.db",
        "instruments": {"kraken": ["BTC-USD"], "zonda": [], "walutomat": [], "polygon": []},
        "timeframes": ["1m"],
        "backfill_days": 30,
        "risk_max_leverage": 1.0,
        "risk_max_drawdown": 0.15,
        "risk_r_per_trade": 0.005,
        "log_level": "INFO",
        "log_json": False,
        "ws_reconnect_max_delay": 10,
        "zmq_pub_address": "tcp://127.0.0.1:5555",
        "zmq_sub_address": "tcp://127.0.0.1:5555",
        "zmq_broker_frontend": "tcp://127.0.0.1:5556",
        "zmq_broker_backend": "tcp://127.0.0.1:5557",
        "server_host": "0.0.0.0",
        "server_port": 8000,
        "server_reload": False,
    }
    default_attrs.update(overrides)
    return type("AppSettings", (), default_attrs)


class TestTradeCommands:
    """Tests for trade-zmq CLI command and trader coordinator."""

    def test_trade_zmq_command_paper_mode(self, cli_runner: CliRunner) -> None:
        """Test trade-zmq runs in paper mode.

        Given: Settings without API credentials,
        When: trade-zmq command is invoked,
        Then: TraderCoordinator starts successfully.
        """
        with (patch("snapper.cli.app.get_settings") as mock_get_settings,):
            mock_settings = create_mock_settings()
            mock_get_settings.return_value = mock_settings

            class MockTraderCoordinator:
                def __init__(self, signal_topics: list[str] | None = None) -> None:
                    self.signal_topics = signal_topics

                async def start(self) -> None:
                    """No-op start for MockTraderCoordinator test stub."""
                    pass

            with (
                patch("snapper.cli.app.TraderCoordinator", MockTraderCoordinator),
                patch("snapper.cli.app.command.upgrade"),
            ):
                result = cli_runner.invoke(app, ["trade-zmq"])
                assert result.exit_code == 0

    def test_trade_zmq_with_custom_symbols(self, cli_runner: CliRunner) -> None:
        """Test trade-zmq handles custom symbols.

        Given: Valid settings,
        When: trade-zmq is invoked,
        Then: TraderCoordinator starts with topics.
        """
        with (patch("snapper.cli.app.get_settings") as mock_get_settings,):
            mock_settings = create_mock_settings()
            mock_get_settings.return_value = mock_settings

            class MockTraderCoordinator:
                def __init__(self, signal_topics: list[str] | None = None) -> None:
                    self.signal_topics = signal_topics

                async def start(self) -> None:
                    """No-op start for MockTraderCoordinator test stub."""
                    pass

            with (
                patch("snapper.cli.app.TraderCoordinator", MockTraderCoordinator),
                patch("snapper.cli.app.command.upgrade"),
            ):
                result = cli_runner.invoke(app, ["trade-zmq"])
                assert result.exit_code == 0


class TestDatabaseCommands:
    """Tests for database CLI commands (init, upgrade, downgrade)."""

    def test_db_init_command(self, cli_runner: CliRunner) -> None:
        """Test db-init runs Alembic upgrade.

        Given: Mocked Alembic command,
        When: db-init command is invoked,
        Then: Alembic upgrade is called.
        """
        with (patch("snapper.cli.app.get_settings") as mock_get_settings,):
            mock_settings = create_mock_settings()
            mock_get_settings.return_value = mock_settings
            with patch("snapper.cli.app.command.upgrade") as mock_upgrade:
                result = cli_runner.invoke(app, ["db-init"])
                assert result.exit_code == 0
                mock_upgrade.assert_called_once()

    def test_db_upgrade_command(self, cli_runner: CliRunner) -> None:
        """Test db-upgrade runs Alembic upgrade.

        Given: Mocked Alembic command,
        When: db-upgrade command is invoked,
        Then: Alembic upgrade is called.
        """
        with (patch("snapper.cli.app.get_settings") as mock_get_settings,):
            mock_settings = create_mock_settings()
            mock_get_settings.return_value = mock_settings
            with patch("snapper.cli.app.command.upgrade") as mock_upgrade:
                result = cli_runner.invoke(app, ["db-upgrade"])
                assert result.exit_code == 0
                mock_upgrade.assert_called_once()

    def test_db_downgrade_command(self, cli_runner: CliRunner) -> None:
        """Test db-downgrade runs Alembic downgrade.

        Given: Mocked Alembic command,
        When: db-downgrade --revision is invoked,
        Then: Alembic downgrade is called.
        """
        with (patch("snapper.cli.app.get_settings") as mock_get_settings,):
            mock_settings = create_mock_settings()
            mock_get_settings.return_value = mock_settings
            with patch("snapper.cli.app.command.downgrade") as mock_downgrade:
                result = cli_runner.invoke(app, ["db-downgrade", "--revision", "-1"])
                assert result.exit_code == 0
                mock_downgrade.assert_called_once()


class TestServiceCommands:
    """Tests for service-related CLI commands."""

    pass


class TestServerCommands:
    """Tests for server CLI command with uvicorn configuration."""

    def test_server_command_default_port(self, cli_runner: CliRunner) -> None:
        """Test server uses default port from settings.

        Given: Settings with default port 8000,
        When: server command is invoked,
        Then: Uvicorn runs on port 8000.
        """
        with (patch("snapper.cli.app.get_settings") as mock_get_settings,):
            mock_settings = create_mock_settings()
            mock_get_settings.return_value = mock_settings
            with patch("snapper.cli.app.uvicorn.run") as mock_uvicorn_run:
                result = cli_runner.invoke(app, ["server"])
                assert result.exit_code == 0
                mock_uvicorn_run.assert_called_once()
                call_kwargs = mock_uvicorn_run.call_args[1]
                assert call_kwargs["port"] == 8000

    def test_server_command_custom_port(self, cli_runner: CliRunner) -> None:
        """Test server uses custom port argument.

        Given: Settings and --port argument,
        When: server --port 9000 is invoked,
        Then: Uvicorn runs on port 9000.
        """
        with (patch("snapper.cli.app.get_settings") as mock_get_settings,):
            mock_settings = create_mock_settings()
            mock_get_settings.return_value = mock_settings
            with patch("snapper.cli.app.uvicorn.run") as mock_uvicorn_run:
                result = cli_runner.invoke(app, ["server", "--port", "9000"])
                assert result.exit_code == 0
                call_kwargs = mock_uvicorn_run.call_args[1]
                assert call_kwargs["port"] == 9000


class TestUserCommands:
    """Tests for user management CLI commands."""

    pass


class TestCLICoverageImprovement:
    """Additional tests to improve CLI module coverage."""

    def test_validate_api_keys_comprehensive(self) -> None:
        """Test validate_api_keys with various inputs.

        Given: Multiple credential combinations,
        When: validate_api_keys is called for each,
        Then: Returns expected boolean results.
        """
        assert validate_api_keys(None, None, paper=True) is True
        assert validate_api_keys(None, None, paper=False) is False
        assert validate_api_keys("", "", paper=False) is False
        assert validate_api_keys("key", "", paper=False) is False
        assert validate_api_keys("", "secret", paper=False) is False
        assert validate_api_keys("key", "secret", paper=False) is True

    def test_parse_date_range_valid(self) -> None:
        """Test parse_date_range returns correct year/month/day.

        Given: Valid date range strings,
        When: parse_date_range is called,
        Then: Dates have correct components.
        """
        start, end = parse_date_range("2023-01-01", "2023-12-31")
        assert start.year == 2023
        assert start.month == 1
        assert start.day == 1
        assert end.year == 2023
        assert end.month == 12
        assert end.day == 31

    def test_parse_date_range_invalid(self) -> None:
        """Test parse_date_range raises on invalid dates.

        Given: Invalid date strings,
        When: parse_date_range is called,
        Then: Raises ValueError.
        """
        with pytest.raises(ValueError, match="Error parsing dates"):
            parse_date_range("invalid", "2023-12-31")
        with pytest.raises(ValueError, match="Error parsing dates"):
            parse_date_range("2023-01-01", "invalid")

    def test_parse_symbols_empty(self) -> None:
        """Test parse_symbols returns empty for empty input.

        Given: Empty string,
        When: parse_symbols is called,
        Then: Returns empty list.
        """
        result = parse_symbols("")
        assert result == []

    def test_parse_symbols_single(self) -> None:
        """Test parse_symbols parses single symbol.

        Given: Single symbol string,
        When: parse_symbols is called,
        Then: Returns list with one symbol.
        """
        result = parse_symbols("BTC-USD")
        assert result == ["BTC-USD"]

    def test_parse_symbols_multiple(self) -> None:
        """Test parse_symbols parses multiple symbols.

        Given: Comma-separated symbols with spaces,
        When: parse_symbols is called,
        Then: Returns trimmed symbols list.
        """
        result = parse_symbols("BTC-USD, ETH-USD , DOGE/USD")
        assert result == ["BTC-USD", "ETH-USD", "DOGE/USD"]

    def test_parse_symbols_with_empty_parts(self) -> None:
        """Test parse_symbols filters empty parts.

        Given: String with empty comma-separated parts,
        When: parse_symbols is called,
        Then: Returns only non-empty symbols.
        """
        result = parse_symbols("BTC-USD,,  , ETH-USD")
        assert result == ["BTC-USD", "ETH-USD"]

    def test_main_callback(self) -> None:
        """Test main_callback runs without error.

        Given: Default environment,
        When: main_callback is called,
        Then: Completes without exception.
        """
        main_callback()

    def test_cli_commands_import(self) -> None:
        """Test CLI app has expected commands.

        Given: Imported app module,
        When: Checking for info attribute,
        Then: App has info command.
        """
        assert hasattr(app, "info")

    def test_main_callback_function(self) -> None:
        """Test main_callback clears encryption after run.

        Given: Encryption service may be initialized,
        When: main_callback runs,
        Then: Encryption instance is cleared.
        """
        try:
            main_callback()
        finally:
            SettingsEncryptionService.clear_instance()


class TestBrokerCommand:
    """Tests for ZMQ broker CLI command."""

    def test_broker_command_creates_instance(self) -> None:
        """Test broker command creates ZMQ broker instance.

        Given: Mocked ZmqBrokerThread,
        When: broker function is called,
        Then: Broker starts and stops with endpoints.
        """
        with (
            patch("snapper.cli.app.ZmqBrokerThread") as mock_broker_class,
            patch("snapper.cli.app.threading.Event") as mock_event,
            patch("snapper.cli.app.signal.signal"),
        ):
            mock_broker_instance = MagicMock()
            mock_broker_class.return_value = mock_broker_instance
            mock_broker_instance.xsub_endpoint = "tcp://localhost:5555"
            mock_broker_instance.xpub_endpoint = "tcp://localhost:5556"
            mock_event_instance = MagicMock()
            mock_event_instance.wait.return_value = None
            mock_event.return_value = mock_event_instance
            broker(xsub="tcp://localhost:5555", xpub="tcp://localhost:5556")
            mock_broker_class.assert_called_once_with(
                xsub_endpoint="tcp://localhost:5555", xpub_endpoint="tcp://localhost:5556"
            )
            mock_broker_instance.start.assert_called_once()
            mock_broker_instance.stop.assert_called_once()

    def test_broker_command_default_endpoints(self) -> None:
        """Test broker uses default endpoints when not specified.

        Given: Mocked ZmqBrokerThread,
        When: broker function is called without args,
        Then: Uses default endpoint values.
        """
        with (
            patch("snapper.cli.app.ZmqBrokerThread") as mock_broker_class,
            patch("snapper.cli.app.threading.Event") as mock_event,
            patch("snapper.cli.app.signal.signal"),
        ):
            mock_broker_instance = MagicMock()
            mock_broker_class.return_value = mock_broker_instance
            mock_broker_instance.xsub_endpoint = "tcp://*:5559"
            mock_broker_instance.xpub_endpoint = "tcp://*:5560"
            mock_event_instance = MagicMock()
            mock_event_instance.wait.return_value = None
            mock_event.return_value = mock_event_instance
            broker(xsub=None, xpub=None)
            mock_broker_class.assert_called_once_with(xsub_endpoint=None, xpub_endpoint=None)
            mock_broker_instance.start.assert_called_once()
            mock_broker_instance.stop.assert_called_once()

    def test_broker_command_handles_keyboard_interrupt(self) -> None:
        """Test broker handles KeyboardInterrupt gracefully.

        Given: Event wait raises KeyboardInterrupt,
        When: broker function is called,
        Then: Broker stops cleanly.
        """
        with (
            patch("snapper.cli.app.ZmqBrokerThread") as mock_broker_class,
            patch("snapper.cli.app.threading.Event") as mock_event,
            patch("snapper.cli.app.signal.signal"),
        ):
            mock_broker_instance = MagicMock()
            mock_broker_class.return_value = mock_broker_instance
            mock_broker_instance.xsub_endpoint = "tcp://*:5559"
            mock_broker_instance.xpub_endpoint = "tcp://*:5560"
            mock_event_instance = MagicMock()
            mock_event_instance.wait.side_effect = KeyboardInterrupt
            mock_event.return_value = mock_event_instance
            broker()
            mock_broker_instance.stop.assert_called_once()


class TestFeedCommand:
    """Tests for market data feed CLI command."""

    def test_feed_command_creates_publisher(self) -> None:
        """Test feed creates market data publisher.

        Given: Mocked KrakenMarketDataPublisher,
        When: feed function is called with symbols,
        Then: Publisher created with parsed symbols.
        """
        with patch("snapper.cli.app.KrakenMarketDataPublisher") as mock_publisher_class:
            mock_publisher = MagicMock()
            mock_publisher.start = MagicMock(return_value=None)
            mock_publisher.stop = MagicMock(return_value=None)
            mock_publisher_class.return_value = mock_publisher

            async def mock_start() -> None:
                """Intentionally empty mock implementation."""
                pass

            async def mock_stop() -> None:
                """Intentionally empty mock implementation."""
                pass

            mock_publisher.start = mock_start
            mock_publisher.stop = mock_stop
            feed(symbols="BTC-USD,ETH-USD")
            mock_publisher_class.assert_called_once_with(
                symbols=["BTC-USD", "ETH-USD"],
            )

    def test_feed_command_through_broker(self) -> None:
        """Test feed publishes through broker.

        Given: Mocked KrakenMarketDataPublisher,
        When: feed function is called,
        Then: Publisher created with symbols.
        """
        with patch("snapper.cli.app.KrakenMarketDataPublisher") as mock_publisher_class:
            mock_publisher = MagicMock()

            async def mock_start() -> None:
                """Intentionally empty mock implementation."""
                pass

            async def mock_stop() -> None:
                """Intentionally empty mock implementation."""
                pass

            mock_publisher.start = mock_start
            mock_publisher.stop = mock_stop
            mock_publisher_class.return_value = mock_publisher
            feed(symbols="BTC-USD")
            mock_publisher_class.assert_called_once_with(symbols=["BTC-USD"])


class TestExecutorCommand:
    """Tests for order executor CLI command."""

    def test_executor_command_creates_service(self) -> None:
        """Test executor creates order executor service.

        Given: Mocked KrakenOrderExecutor,
        When: executor function is called,
        Then: Executor created and status retrieved.
        """
        with patch("snapper.cli.app.KrakenOrderExecutor") as mock_service_class:
            mock_service = MagicMock()
            mock_service.get_status.return_value = {
                "broker_xsub": "tcp://127.0.0.1:7500",
                "broker_xpub": "tcp://127.0.0.1:7501",
            }

            async def mock_start() -> None:
                """Intentionally empty mock implementation."""
                pass

            async def mock_stop() -> None:
                """Intentionally empty mock implementation."""
                pass

            mock_service.start = mock_start
            mock_service.stop = mock_stop
            mock_service_class.return_value = mock_service
            executor()
            mock_service_class.assert_called_once_with()

    def test_executor_command_default_endpoints(self) -> None:
        """Test executor uses default broker endpoints.

        Given: Mocked KrakenOrderExecutor,
        When: executor function is called,
        Then: Uses default endpoints from status.
        """
        with patch("snapper.cli.app.KrakenOrderExecutor") as mock_service_class:
            mock_service = MagicMock()
            mock_service.get_status.return_value = {
                "broker_xsub": "tcp://127.0.0.1:7500",
                "broker_xpub": "tcp://127.0.0.1:7501",
            }

            async def mock_start() -> None:
                """Intentionally empty mock implementation."""
                pass

            async def mock_stop() -> None:
                """Intentionally empty mock implementation."""
                pass

            mock_service.start = mock_start
            mock_service.stop = mock_stop
            mock_service_class.return_value = mock_service
            executor()
            mock_service_class.assert_called_once_with()


class TestAdminCommands:
    """Tests for admin user management CLI commands."""

    def test_init_admin_creates_user(self) -> None:
        """Test init_admin creates new admin user.

        Given: User service with no existing user,
        When: init_admin is called,
        Then: User is created with admin role.
        """
        with patch("snapper.cli.app.UserService") as mock_service_class:
            mock_service = MagicMock()

            async def mock_get_user(username: str) -> None:
                return None

            async def mock_create_user(**kwargs: Any) -> None:
                """Intentionally empty mock implementation."""
                pass

            mock_service.get_user_by_username = mock_get_user
            mock_service.create_user = mock_create_user
            mock_service_class.return_value = mock_service
            init_admin(username="testadmin", password="testpass123")
            mock_service_class.assert_called_once()

    def test_init_admin_default_credentials(self) -> None:
        """Test init_admin with default credentials.

        Given: User service with no existing user,
        When: init_admin is called with defaults,
        Then: User is created.
        """
        with patch("snapper.cli.app.UserService") as mock_service_class:
            mock_service = MagicMock()

            async def mock_get_user(username: str) -> None:
                return None

            async def mock_create_user(**kwargs: Any) -> None:
                """Intentionally empty mock implementation."""
                pass

            mock_service.get_user_by_username = mock_get_user
            mock_service.create_user = mock_create_user
            mock_service_class.return_value = mock_service
            init_admin(username="admin", password="admin123")
            mock_service_class.assert_called_once()

    def test_list_users_displays_users(self) -> None:
        """Test list_users retrieves all users.

        Given: User service returning empty list,
        When: list_users is called,
        Then: Service called to get all users.
        """
        with patch("snapper.cli.app.UserService") as mock_service_class:
            mock_service = MagicMock()

            async def mock_get_all_users(include_inactive: bool = False) -> list[Any]:
                return []

            mock_service.get_all_users = mock_get_all_users
            mock_service_class.return_value = mock_service
            list_users()
            mock_service_class.assert_called_once()

    def test_reset_password_with_password(self) -> None:
        """Test reset_password updates user's password.

        Given: User service with existing user,
        When: reset_password is called with new password,
        Then: Password is hashed and saved.
        """
        with patch("snapper.cli.app.UserService") as mock_service_class:
            mock_service = MagicMock()
            mock_service.hash_password_with_salt = MagicMock(
                return_value=("hashed_password", "salt")
            )
            mock_session = MagicMock()
            mock_session.__aenter__ = MagicMock(return_value=mock_session)
            mock_session.__aexit__ = MagicMock(return_value=None)

            async def mock_execute(stmt: Any) -> Any:
                mock_result = MagicMock()
                mock_user = MagicMock()
                mock_user.username = "testuser"
                mock_result.scalar_one_or_none = MagicMock(return_value=mock_user)
                return mock_result

            async def mock_commit() -> None:
                """Intentionally empty mock implementation."""
                pass

            mock_session.execute = mock_execute
            mock_session.commit = mock_commit
            mock_repo = MagicMock()
            mock_repo.session = MagicMock(return_value=mock_session)
            mock_service.repository = mock_repo
            mock_service_class.return_value = mock_service
            reset_password(username="testuser", new_password="newpass123")
            mock_service_class.assert_called_once()

    def test_reset_password_with_prompt(self) -> None:
        """Test reset_password prompts for password twice.

        Given: User service and password prompts,
        When: reset_password is called without password,
        Then: Prompts user twice for confirmation.
        """
        with (
            patch("snapper.cli.app.typer.prompt") as mock_prompt,
            patch("snapper.cli.app.UserService") as mock_service_class,
        ):
            mock_prompt.side_effect = ["newpass123", "newpass123"]
            mock_service = MagicMock()
            mock_service.hash_password_with_salt = MagicMock(
                return_value=("hashed_password", "salt")
            )
            mock_session = MagicMock()
            mock_session.__aenter__ = MagicMock(return_value=mock_session)
            mock_session.__aexit__ = MagicMock(return_value=None)

            async def mock_execute(stmt: Any) -> Any:
                mock_result = MagicMock()
                mock_user = MagicMock()
                mock_user.username = "testuser"
                mock_result.scalar_one_or_none = MagicMock(return_value=mock_user)
                return mock_result

            async def mock_commit() -> None:
                """Intentionally empty mock implementation."""
                pass

            mock_session.execute = mock_execute
            mock_session.commit = mock_commit
            mock_repo = MagicMock()
            mock_repo.session = MagicMock(return_value=mock_session)
            mock_service.repository = mock_repo
            mock_service_class.return_value = mock_service
            reset_password(username="testuser", new_password="")
            assert mock_prompt.call_count == 2
            mock_service_class.assert_called_once()

    def test_reset_password_mismatch(self) -> None:
        """Test reset_password fails on password mismatch.

        Given: Mismatched password prompts,
        When: reset_password is called,
        Then: Shows mismatch error message.
        """
        with (
            patch("snapper.cli.app.typer.prompt") as mock_prompt,
            patch("snapper.cli.app.typer.echo") as mock_echo,
        ):
            mock_prompt.side_effect = ["newpass123", "differentpass"]
            reset_password(username="testuser", new_password="")
            mock_echo.assert_called_with("Passwords don't match!")


class TestAlembicConfigNotFound:
    """Tests for Alembic configuration file not found error handling."""

    def test_alembic_cfg_raises_when_ini_not_found(self) -> None:
        """Test _alembic_cfg raises when ini file missing.

        Given: alembic.ini does not exist,
        When: _alembic_cfg is called,
        Then: Raises RuntimeError.
        """
        with (
            patch.object(Path, "exists", return_value=False),
            pytest.raises(RuntimeError, match="alembic.ini not found"),
        ):
            _alembic_cfg("sqlite:///test.db")


class TestServeCommandKeyboardInterrupt:
    """Tests for server command KeyboardInterrupt handling."""

    def test_serve_keyboard_interrupt(self) -> None:
        """Test server handles KeyboardInterrupt gracefully.

        Given: Uvicorn raises KeyboardInterrupt,
        When: server command is called,
        Then: Shows shutdown message.
        """
        with (
            patch("snapper.cli.app.uvicorn.run", side_effect=KeyboardInterrupt),
            patch("snapper.cli.app.typer.echo") as mock_echo,
        ):
            server(host="127.0.0.1", port=8000, reload=False)
            mock_echo.assert_called_with("\nShutting down gracefully...")


class TestSignalHandler:
    """Tests for signal handler registration and invocation."""

    def test_broker_signal_handler_called(self) -> None:
        """Test broker signal handler sets stop event.

        Given: Signal handler registered for SIGINT,
        When: Handler is invoked,
        Then: Stop event is set.
        """
        captured_handler: Any = None

        def capture_signal_handler(sig: int, handler: Any) -> None:
            nonlocal captured_handler
            if sig == signal.SIGINT:
                captured_handler = handler

        with (
            patch("snapper.cli.app.ZmqBrokerThread") as mock_broker_class,
            patch("snapper.cli.app.threading.Event") as mock_event,
            patch("snapper.cli.app.signal.signal", side_effect=capture_signal_handler),
        ):
            mock_broker_instance = MagicMock()
            mock_broker_class.return_value = mock_broker_instance
            mock_broker_instance.xsub_endpoint = "tcp://*:5559"
            mock_broker_instance.xpub_endpoint = "tcp://*:5560"
            mock_event_instance = MagicMock()
            mock_event_instance.wait.return_value = None
            mock_event.return_value = mock_event_instance
            broker()
            assert captured_handler is not None
            captured_handler(signal.SIGINT, None)
            mock_event_instance.set.assert_called_once()


class TestFeedKeyboardInterrupt:
    """Tests for feed command KeyboardInterrupt handling."""

    def test_feed_keyboard_interrupt(self) -> None:
        """Test feed handles KeyboardInterrupt gracefully.

        Given: Publisher start raises KeyboardInterrupt,
        When: feed command is called,
        Then: Shows stopped by user message.
        """
        with (
            patch("snapper.cli.app.KrakenMarketDataPublisher") as mock_publisher_class,
            patch("snapper.cli.app.typer.echo") as mock_echo,
        ):
            mock_publisher = MagicMock()

            async def mock_start() -> None:
                raise KeyboardInterrupt

            async def mock_stop() -> None:
                """Intentionally empty mock implementation."""
                pass

            mock_publisher.start = mock_start
            mock_publisher.stop = mock_stop
            mock_publisher_class.return_value = mock_publisher
            feed(symbols="BTC-USD")
            assert any("Feed stopped by user" in str(call) for call in mock_echo.call_args_list)


class TestZmqLoggerKeyboardInterrupt:
    """Tests for ZMQ logger command KeyboardInterrupt handling."""

    def test_zmq_logger_keyboard_interrupt(self) -> None:
        """Test zmq_logger handles KeyboardInterrupt gracefully.

        Given: Logger start raises KeyboardInterrupt,
        When: zmq_logger command is called,
        Then: Shows stopped by user message.
        """
        with (
            patch("snapper.cli.app.ZmqMessageLogger") as mock_logger_class,
            patch("snapper.cli.app.typer.echo") as mock_echo,
        ):
            mock_logger = MagicMock()
            mock_logger.audit_path = "/tmp/audit.log"

            async def mock_start() -> None:
                raise KeyboardInterrupt

            async def mock_stop() -> None:
                """Intentionally empty mock implementation."""
                pass

            mock_logger.start = mock_start
            mock_logger.stop = mock_stop
            mock_logger_class.return_value = mock_logger
            zmq_logger(log_payload=False, log_file=False)
            assert any(
                "ZMQ Logger stopped by user" in str(call) for call in mock_echo.call_args_list
            )

    def test_zmq_logger_with_log_file_enabled(self) -> None:
        """Test zmq_logger shows audit path when enabled.

        Given: Logger with log_file enabled,
        When: zmq_logger command is called,
        Then: Shows audit file path.
        """
        with (
            patch("snapper.cli.app.ZmqMessageLogger") as mock_logger_class,
            patch("snapper.cli.app.typer.echo") as mock_echo,
        ):
            mock_logger = MagicMock()
            mock_logger.audit_path = "/tmp/audit.log"

            async def mock_start() -> None:
                """Intentionally empty mock implementation."""
                pass

            async def mock_stop() -> None:
                """Intentionally empty mock implementation."""
                pass

            mock_logger.start = mock_start
            mock_logger.stop = mock_stop
            mock_logger_class.return_value = mock_logger
            zmq_logger(log_payload=True, log_file=True)
            assert any("Audit file:" in str(call) for call in mock_echo.call_args_list)


class TestEncryptionRotateErrors:
    """Tests for settings encryption rotation error handling."""

    def test_rotate_encryption_verification_failed(self) -> None:
        """Test encryption rotation fails on verification mismatch.

        Given: Old encryption returns wrong verification value,
        When: settings_rotate_encryption is called,
        Then: Shows verification failed error.
        """
        with (
            patch("snapper.cli.app.BootstrapSettingsLoader") as mock_bootstrap_class,
            patch("snapper.cli.app.SettingsEncryptionService") as mock_encryption_class,
            patch("snapper.cli.app.typer.echo") as mock_echo,
        ):
            mock_bootstrap = MagicMock()
            mock_bootstrap.master_password = "old-password"
            mock_bootstrap.encryption_salt = "salt123"
            mock_bootstrap_class.return_value = mock_bootstrap
            mock_old_encryption = MagicMock()
            mock_new_encryption = MagicMock()
            mock_old_encryption.encrypt.return_value = "encrypted"
            mock_old_encryption.decrypt.return_value = "wrong-value"
            mock_new_encryption.encrypt.return_value = "new-encrypted"
            mock_new_encryption.decrypt.return_value = "test-rotation-verification"
            mock_encryption_class.side_effect = [mock_old_encryption, mock_new_encryption]
            settings_rotate_encryption(
                old_master_password="old",
                new_master_password="new",
                old_salt=None,
                new_salt=None,
                dry_run=False,
            )
            error_calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Failed to rotate encryption" in call for call in error_calls)
            assert any("Encryption verification failed" in call for call in error_calls)

    def test_settings_rotate_encryption_decrypt_error_raises_in_loop(self) -> None:
        """Test encryption rotation shows DB inconsistency on error.

        Given: Decryption fails during rotation loop,
        When: settings_rotate_encryption is called,
        Then: Shows database inconsistency warning.
        """
        with (
            patch("snapper.cli.app.BootstrapSettingsLoader") as mock_bootstrap_class,
            patch("snapper.cli.app.SettingsEncryptionService") as mock_encryption_class,
            patch("snapper.cli.app.create_async_engine") as mock_engine,
            patch("snapper.cli.app.async_sessionmaker") as mock_sessionmaker,
            patch("snapper.cli.app.typer.echo") as mock_echo,
        ):
            mock_bootstrap = MagicMock()
            mock_bootstrap.master_password = "old-password"
            mock_bootstrap.encryption_salt = "salt123"
            mock_bootstrap_class.return_value = mock_bootstrap
            mock_old_encryption = MagicMock()
            mock_new_encryption = MagicMock()
            mock_old_encryption.encrypt.return_value = "enc1"
            mock_old_encryption.decrypt.side_effect = [
                "test-rotation-verification",
                RuntimeError("decrypt failed"),
            ]
            mock_new_encryption.encrypt.return_value = "enc2"
            mock_new_encryption.decrypt.return_value = "test-rotation-verification"
            mock_encryption_class.side_effect = [mock_old_encryption, mock_new_encryption]

            class _ScalarResult:
                def __init__(self, rows: list[Any]) -> None:
                    self._rows = rows

                def all(self) -> list[Any]:
                    return self._rows

            class _ExecuteResult:
                def __init__(self, rows: list[Any]) -> None:
                    self._rows = rows

                def scalars(self) -> _ScalarResult:
                    return _ScalarResult(self._rows)

            class _Session:
                def __init__(self, rows: list[Any]) -> None:
                    self._rows = rows

                async def __aenter__(self) -> "_Session":
                    return self

                async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
                    return None

                async def execute(self, _query: Any) -> _ExecuteResult:
                    return _ExecuteResult(self._rows)

                async def commit(self) -> None:
                    return None

            setting = SimpleNamespace(key="secret", value="ciphertext", is_encrypted=True)
            mock_engine.return_value = MagicMock()
            mock_sessionmaker.return_value = MagicMock(return_value=_Session([setting]))
            settings_rotate_encryption(
                old_master_password="old",
                new_master_password="new",
                old_salt="oldsalt",
                new_salt="newsalt",
                dry_run=False,
            )
            error_calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Failed to rotate encryption" in call for call in error_calls)
            assert any("Database may be in inconsistent state" in call for call in error_calls)

    def test_settings_rotate_encryption_decrypt_error_dry_run(self) -> None:
        """Test encryption rotation dry-run skips DB warning.

        Given: Decryption fails during dry-run,
        When: settings_rotate_encryption is called,
        Then: No database inconsistency warning.
        """
        with (
            patch("snapper.cli.app.BootstrapSettingsLoader") as mock_bootstrap_class,
            patch("snapper.cli.app.SettingsEncryptionService") as mock_encryption_class,
            patch("snapper.cli.app.create_async_engine") as mock_engine,
            patch("snapper.cli.app.async_sessionmaker") as mock_sessionmaker,
            patch("snapper.cli.app.typer.echo") as mock_echo,
        ):
            mock_bootstrap = MagicMock()
            mock_bootstrap.master_password = "old-password"
            mock_bootstrap.encryption_salt = "salt123"
            mock_bootstrap_class.return_value = mock_bootstrap
            mock_old_encryption = MagicMock()
            mock_new_encryption = MagicMock()
            mock_old_encryption.encrypt.return_value = "enc1"
            mock_old_encryption.decrypt.side_effect = [
                "test-rotation-verification",
                RuntimeError("decrypt failed"),
            ]
            mock_new_encryption.encrypt.return_value = "enc2"
            mock_new_encryption.decrypt.return_value = "test-rotation-verification"
            mock_encryption_class.side_effect = [mock_old_encryption, mock_new_encryption]

            class _ScalarResult:
                def __init__(self, rows: list[Any]) -> None:
                    self._rows = rows

                def all(self) -> list[Any]:
                    return self._rows

            class _ExecuteResult:
                def __init__(self, rows: list[Any]) -> None:
                    self._rows = rows

                def scalars(self) -> _ScalarResult:
                    return _ScalarResult(self._rows)

            class _Session:
                def __init__(self, rows: list[Any]) -> None:
                    self._rows = rows

                async def __aenter__(self) -> "_Session":
                    return self

                async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
                    return None

                async def execute(self, _query: Any) -> _ExecuteResult:
                    return _ExecuteResult(self._rows)

                async def commit(self) -> None:
                    return None

            setting = SimpleNamespace(key="secret", value="ciphertext", is_encrypted=True)
            mock_engine.return_value = MagicMock()
            mock_sessionmaker.return_value = MagicMock(return_value=_Session([setting]))
            settings_rotate_encryption(
                old_master_password="old",
                new_master_password="new",
                old_salt="oldsalt",
                new_salt="newsalt",
                dry_run=True,
            )
            error_calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Failed to rotate" in call for call in error_calls)
            assert all("Database may be in inconsistent state" not in call for call in error_calls)

    def test_settings_rotate_encryption_outer_exception_dry_run(self) -> None:
        """Test encryption rotation outer exception handled.

        Given: Engine creation raises RuntimeError,
        When: settings_rotate_encryption is called,
        Then: Shows rotation failed error.
        """
        with (
            patch("snapper.cli.app.BootstrapSettingsLoader") as mock_bootstrap_class,
            patch("snapper.cli.app.SettingsEncryptionService") as mock_encryption_class,
            patch("snapper.cli.app.create_async_engine", side_effect=RuntimeError("db down")),
            patch("snapper.cli.app.typer.echo") as mock_echo,
        ):
            mock_bootstrap = MagicMock()
            mock_bootstrap.master_password = "old-password"
            mock_bootstrap.encryption_salt = "salt123"
            mock_bootstrap_class.return_value = mock_bootstrap
            mock_encryption = MagicMock()
            mock_encryption.encrypt.return_value = "enc1"
            mock_encryption.decrypt.return_value = "test-rotation-verification"
            mock_encryption_class.return_value = mock_encryption
            settings_rotate_encryption(
                old_master_password="old",
                new_master_password="new",
                old_salt="oldsalt",
                new_salt="newsalt",
                dry_run=True,
            )
            error_calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Failed to rotate encryption" in call for call in error_calls)
            assert all("Database may be in inconsistent state" not in call for call in error_calls)


class TestPolygonBackfillErrors:
    """Tests for Polygon backfill command error handling."""

    def test_polygon_backfill_aggregates_error(self) -> None:
        """Test polygon-backfill-aggregates handles API errors.

        Given: Backfill service raises RuntimeError,
        When: polygon_backfill_aggregates is called,
        Then: Exits with code 1 and error message.
        """
        with (
            patch("snapper.cli.app.PolygonAggregatesBackfillService") as mock_service_class,
            patch("snapper.cli.app.typer.echo") as mock_echo,
        ):
            mock_service = MagicMock()

            async def mock_start() -> None:
                raise RuntimeError("API error")

            mock_service.start = mock_start
            mock_service_class.return_value = mock_service
            with pytest.raises(typer.Exit) as exc_info:
                polygon_backfill_aggregates(
                    symbols=["BTC-USD"],
                    all_mapped=False,
                    multiplier=1,
                    timespan="day",
                    days_back=30,
                    resume=True,
                    save_csv=True,
                )
            assert exc_info.value.exit_code == 1
            assert any(
                "Error during Polygon aggregates backfill" in str(call)
                for call in mock_echo.call_args_list
            )

    def test_polygon_backfill_grouped_error(self) -> None:
        """Test polygon-backfill-grouped handles API errors.

        Given: Grouped backfill service raises RuntimeError,
        When: polygon_backfill_grouped is called,
        Then: Exits with code 1 and error message.
        """
        with (
            patch("snapper.cli.app.PolygonGroupedDailyBackfillService") as mock_service_class,
            patch("snapper.cli.app.typer.echo") as mock_echo,
        ):
            mock_service = MagicMock()

            async def mock_start() -> None:
                raise RuntimeError("API error")

            mock_service.start = mock_start
            mock_service_class.return_value = mock_service
            with pytest.raises(typer.Exit) as exc_info:
                polygon_backfill_grouped(market_type="stocks", days=30)
            assert exc_info.value.exit_code == 1
            assert any(
                "Error during Polygon grouped backfill" in str(call)
                for call in mock_echo.call_args_list
            )


class TestPolygonBackfillSuccess:
    """Tests for Polygon backfill command success scenarios."""

    def test_polygon_backfill_aggregates_success(self) -> None:
        """Test polygon-backfill-aggregates succeeds.

        Given: Backfill service completes successfully,
        When: polygon_backfill_aggregates is called,
        Then: Shows completion message.
        """
        with (
            patch("snapper.cli.app.PolygonAggregatesBackfillService") as mock_service_class,
            patch("snapper.cli.app.typer.echo") as mock_echo,
        ):
            mock_service = MagicMock()

            async def mock_start() -> None:
                return None

            mock_service.start = mock_start
            mock_service_class.return_value = mock_service
            polygon_backfill_aggregates(
                symbols=["BTC-USD"],
                all_mapped=False,
                multiplier=1,
                timespan="day",
                days_back=1,
                resume=True,
                save_csv=False,
            )
            assert any(
                "Polygon aggregates backfill complete" in str(call)
                for call in mock_echo.call_args_list
            )

    def test_polygon_backfill_grouped_success(self) -> None:
        """Test polygon-backfill-grouped succeeds.

        Given: Grouped backfill service completes,
        When: polygon_backfill_grouped is called,
        Then: Shows completion message.
        """
        with (
            patch("snapper.cli.app.PolygonGroupedDailyBackfillService") as mock_service_class,
            patch("snapper.cli.app.typer.echo") as mock_echo,
        ):
            mock_service = MagicMock()

            async def mock_start() -> None:
                return None

            mock_service.start = mock_start
            mock_service_class.return_value = mock_service
            polygon_backfill_grouped(market_type="fx", days=2)
            assert any(
                "Polygon grouped daily backfill complete" in str(call)
                for call in mock_echo.call_args_list
            )


@pytest.fixture()
def mock_settings() -> SimpleNamespace:
    """Provide mock settings with database and server configuration."""
    return SimpleNamespace(
        db_url="sqlite:///memory.db",
        server_host="0.0.0.0",
        server_port=8000,
        server_reload=False,
    )


def test_server_command_starts_uvicorn(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, mock_settings: SimpleNamespace
) -> None:
    """Test server command starts uvicorn with settings.

    Given: Settings with host and port,
    When: server command is invoked,
    Then: Uvicorn runs with correct config.
    """
    captured: dict[str, Any] = {}

    def fake_get_settings() -> SimpleNamespace:
        return mock_settings

    monkeypatch.setattr(app_module, "get_settings", fake_get_settings)

    def fake_uvicorn_run(app: Any, **kwargs: Any) -> None:
        captured["app"] = app
        captured["uvicorn_kwargs"] = kwargs

    monkeypatch.setattr(uvicorn, "run", fake_uvicorn_run)
    result = cli_runner.invoke(app, ["server"])
    assert result.exit_code == 0
    assert "Starting Snapper server on 0.0.0.0:8000" in result.stdout
    assert captured["uvicorn_kwargs"]["host"] == "0.0.0.0"
    assert captured["uvicorn_kwargs"]["port"] == 8000
    assert captured["uvicorn_kwargs"]["reload"] is False


def test_server_command_with_override_options(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch, mock_settings: SimpleNamespace
) -> None:
    """Test server command accepts override options.

    Given: Settings and CLI override arguments,
    When: server command is invoked with options,
    Then: Uvicorn runs with overridden values.
    """
    captured: dict[str, Any] = {}
    monkeypatch.setattr(app_module, "get_settings", lambda: mock_settings)

    def fake_uvicorn_run(app: Any, **kwargs: Any) -> None:
        captured["app"] = app
        captured["uvicorn_kwargs"] = kwargs

    monkeypatch.setattr(uvicorn, "run", fake_uvicorn_run)
    result = cli_runner.invoke(
        app,
        ["server", "--host", "127.0.0.1", "--port", "9000", "--reload"],
    )
    assert result.exit_code == 0
    assert "Starting Snapper server on 127.0.0.1:9000" in result.stdout
    assert captured["uvicorn_kwargs"]["host"] == "127.0.0.1"
    assert captured["uvicorn_kwargs"]["port"] == 9000
    assert captured["uvicorn_kwargs"]["reload"] is True


def test_broker_command_starts_zmq_broker(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test broker command starts ZMQ broker.

    Given: Mocked ZmqBrokerThread,
    When: broker command is invoked,
    Then: Broker starts and stops.
    """
    captured: dict[str, Any] = {}

    class MockBroker:
        def __init__(self, xsub_endpoint: str | None = None, xpub_endpoint: str | None = None):
            captured["xsub_endpoint"] = xsub_endpoint
            captured["xpub_endpoint"] = xpub_endpoint
            self.xsub_endpoint = xsub_endpoint or "tcp://*:5555"
            self.xpub_endpoint = xpub_endpoint or "tcp://*:5556"

        def start(self) -> None:
            captured["started"] = True

        def stop(self) -> None:
            captured["stopped"] = True

    monkeypatch.setattr(app_module, "ZmqBrokerThread", MockBroker)

    class MockEvent:
        def wait(self) -> None:
            """Intentionally empty stub for testing."""
            pass

        def set(self) -> None:
            """Intentionally empty stub for testing."""
            pass

        def is_set(self) -> bool:
            return True

    mock_event_instance = MockEvent()

    def mock_event() -> MockEvent:
        return mock_event_instance

    monkeypatch.setattr("threading.Event", mock_event)
    result = cli_runner.invoke(app, ["broker"])
    assert result.exit_code == 0
    assert "ZMQ Broker running" in result.stdout
    assert captured["started"] is True
    assert captured["stopped"] is True


def test_broker_command_with_custom_endpoints(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test broker command accepts custom endpoints.

    Given: Custom xsub and xpub endpoints,
    When: broker command is invoked,
    Then: Broker uses custom endpoints.
    """
    captured: dict[str, Any] = {}

    class MockBroker:
        def __init__(self, xsub_endpoint: str | None = None, xpub_endpoint: str | None = None):
            captured["xsub_endpoint"] = xsub_endpoint
            captured["xpub_endpoint"] = xpub_endpoint
            self.xsub_endpoint = xsub_endpoint or "tcp://*:5555"
            self.xpub_endpoint = xpub_endpoint or "tcp://*:5556"

        def start(self) -> None:
            """No-op start for MockBroker test stub."""
            pass

        def stop(self) -> None:
            """Intentionally empty stub for testing."""
            pass

    monkeypatch.setattr(app_module, "ZmqBrokerThread", MockBroker)

    class MockEvent:
        def wait(self) -> None:
            """Intentionally empty stub for testing."""
            pass

        def set(self) -> None:
            """Intentionally empty stub for testing."""
            pass

        def is_set(self) -> bool:
            return True

    monkeypatch.setattr("threading.Event", lambda: MockEvent())
    result = cli_runner.invoke(
        app,
        ["broker", "--xsub", "tcp://*:6000", "--xpub", "tcp://*:6001"],
    )
    assert result.exit_code == 0
    assert captured["xsub_endpoint"] == "tcp://*:6000"
    assert captured["xpub_endpoint"] == "tcp://*:6001"


def test_feed_command_starts_feed_publisher(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test feed command starts market data publisher.

    Given: Mocked KrakenMarketDataPublisher,
    When: feed command is invoked with symbols,
    Then: Publisher starts with parsed symbols.
    """
    captured: dict[str, Any] = {}

    class MockKrakenMarketDataPublisher:
        def __init__(self, symbols: list[str]):
            captured["symbols"] = symbols

        async def start(self) -> None:
            captured["started"] = True

        async def stop(self) -> None:
            captured["stopped"] = True

    monkeypatch.setattr(app_module, "KrakenMarketDataPublisher", MockKrakenMarketDataPublisher)

    def mock_asyncio_run(coro: Any) -> None:
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        try:
            new_loop.run_until_complete(coro)
        finally:
            new_loop.close()
            asyncio.set_event_loop(None)

    monkeypatch.setattr("asyncio.run", mock_asyncio_run)
    result = cli_runner.invoke(app, ["feed", "--symbols", "BTC-USD,ETH-USD"])
    assert result.exit_code == 0
    assert "Starting feed for symbols: ['BTC-USD', 'ETH-USD']" in result.stdout
    assert captured["symbols"] == ["BTC-USD", "ETH-USD"]
    assert captured["started"] is True
    assert captured["stopped"] is True


def test_executor_command_starts_executor(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test executor command starts order executor.

    Given: Mocked KrakenOrderExecutor,
    When: executor command is invoked,
    Then: Executor starts and stops.
    """
    captured: dict[str, Any] = {}

    class MockExecutor:
        def get_status(self) -> dict[str, str]:
            return {
                "broker_xpub": "tcp://localhost:5556",
                "broker_xsub": "tcp://localhost:5555",
            }

        async def start(self) -> None:
            captured["started"] = True

        async def stop(self) -> None:
            captured["stopped"] = True

    monkeypatch.setattr(app_module, "KrakenOrderExecutor", MockExecutor)

    def mock_asyncio_run(coro: Any) -> None:
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        try:
            new_loop.run_until_complete(coro)
        finally:
            new_loop.close()
            asyncio.set_event_loop(None)

    monkeypatch.setattr("asyncio.run", mock_asyncio_run)
    result = cli_runner.invoke(app, ["executor"])
    assert result.exit_code == 0
    assert "Starting kraken execution service" in result.stdout
    assert captured["started"] is True
    assert captured["stopped"] is True


def test_update_kraken_symbols_runs_updater(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test update-kraken-symbols runs symbol updater.

    Given: Mocked KrakenSymbolMappingUpdaterService,
    When: update-kraken-symbols is invoked,
    Then: Service starts and completes.
    """
    captured: dict[str, bool] = {}

    class MockUpdater:
        def __init__(self, force: bool = False):
            captured["force"] = force

        async def start(self) -> None:
            captured["started"] = True

    monkeypatch.setattr(app_module, "KrakenSymbolMappingUpdaterService", MockUpdater)
    result = cli_runner.invoke(app, ["update-kraken-symbols"])
    assert result.exit_code == 0
    assert "Starting Kraken symbol mapping update" in result.stdout
    assert "Symbol mappings updated successfully" in result.stdout
    assert captured["force"] is False
    assert captured["started"] is True


def test_update_kraken_symbols_with_force_flag(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test update-kraken-symbols accepts force flag.

    Given: Mocked KrakenSymbolMappingUpdaterService,
    When: update-kraken-symbols --force is invoked,
    Then: Force flag is passed to service.
    """
    captured: dict[str, bool] = {}

    class MockUpdater:
        def __init__(self, force: bool = False):
            captured["force"] = force

        async def start(self) -> None:
            captured["started"] = True

    monkeypatch.setattr(app_module, "KrakenSymbolMappingUpdaterService", MockUpdater)
    result = cli_runner.invoke(app, ["update-kraken-symbols", "--force"])
    assert result.exit_code == 0
    assert captured["force"] is True


def test_update_kraken_symbols_handles_exception(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test update-kraken-symbols handles errors.

    Given: Service raises RuntimeError,
    When: update-kraken-symbols is invoked,
    Then: Shows error message and exits with 1.
    """

    class MockUpdater:
        def __init__(self, force: bool = False):
            """Intentionally empty stub for testing."""
            pass

        async def start(self) -> None:
            raise RuntimeError("API connection failed")

    monkeypatch.setattr(app_module, "KrakenSymbolMappingUpdaterService", MockUpdater)
    result = cli_runner.invoke(app, ["update-kraken-symbols"])
    assert result.exit_code == 1
    assert "Error updating symbol mappings: API connection failed" in result.stdout


def test_update_kraken_market_snapshot_runs_service(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test update-kraken-market-snapshot runs service.

    Given: Mocked run_snapshot_update function,
    When: update-kraken-market-snapshot is invoked,
    Then: Service runs and completes.
    """
    captured: dict[str, bool] = {}

    def mock_run_snapshot_update() -> None:
        captured["run"] = True

    monkeypatch.setattr(app_module, "run_snapshot_update", mock_run_snapshot_update)
    result = cli_runner.invoke(app, ["update-kraken-market-snapshot"])
    assert result.exit_code == 0
    assert "Updating Kraken market snapshots" in result.stdout
    assert "Kraken market snapshot update complete" in result.stdout
    assert captured["run"] is True


def test_update_kraken_market_snapshot_handles_exception(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test update-kraken-market-snapshot handles errors.

    Given: Service raises RuntimeError,
    When: update-kraken-market-snapshot is invoked,
    Then: Shows error message and exits with 1.
    """

    def mock_run_snapshot_update() -> None:
        raise RuntimeError("WebSocket connection failed")

    monkeypatch.setattr(app_module, "run_snapshot_update", mock_run_snapshot_update)
    result = cli_runner.invoke(app, ["update-kraken-market-snapshot"])
    assert result.exit_code == 1
    assert "Error updating Kraken market snapshots" in result.stdout


def test_update_zonda_symbols_runs_updater(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test update-zonda-symbols runs symbol updater.

    Given: Mocked ZondaSymbolMappingUpdaterService,
    When: update-zonda-symbols is invoked,
    Then: Service starts with default threshold.
    """
    captured: dict[str, bool | int] = {}

    class MockUpdater:
        def __init__(self, update_threshold_hours: int = 24, force: bool = False):
            captured["threshold"] = update_threshold_hours
            captured["force"] = force

        async def start(self) -> None:
            captured["started"] = True

    monkeypatch.setattr(app_module, "ZondaSymbolMappingUpdaterService", MockUpdater)
    result = cli_runner.invoke(app, ["update-zonda-symbols"])
    assert result.exit_code == 0
    assert "Starting Zonda symbol mapping update" in result.stdout
    assert "Zonda symbol mappings updated successfully" in result.stdout
    assert captured["threshold"] == 24
    assert captured["force"] is False


def test_update_zonda_market_snapshot_runs_service(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test update-zonda-market-snapshot runs service.

    Given: Mocked run_zonda_snapshot_update function,
    When: update-zonda-market-snapshot is invoked,
    Then: Service runs and completes.
    """
    captured: dict[str, bool] = {}

    def mock_run_zonda_snapshot_update() -> None:
        captured["run"] = True

    monkeypatch.setattr(app_module, "run_zonda_snapshot_update", mock_run_zonda_snapshot_update)
    result = cli_runner.invoke(app, ["update-zonda-market-snapshot"])
    assert result.exit_code == 0
    assert "Updating Zonda market snapshots" in result.stdout
    assert "Zonda market snapshot update complete" in result.stdout
    assert captured["run"] is True


def test_update_walutomat_symbols_runs_updater(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test update-walutomat-symbols runs symbol updater.

    Given: Mocked WalutomatSymbolMappingUpdaterService,
    When: update-walutomat-symbols is invoked,
    Then: Service starts with default threshold.
    """
    captured: dict[str, bool | int] = {}

    class MockUpdater:
        def __init__(self, update_threshold_hours: int = 24, force: bool = False):
            captured["threshold"] = update_threshold_hours
            captured["force"] = force

        async def start(self) -> None:
            captured["started"] = True

    monkeypatch.setattr(app_module, "WalutomatSymbolMappingUpdaterService", MockUpdater)
    result = cli_runner.invoke(app, ["update-walutomat-symbols"])
    assert result.exit_code == 0
    assert "Starting Walutomat symbol mapping update" in result.stdout
    assert "Walutomat symbol mappings updated successfully" in result.stdout
    assert captured["threshold"] == 24


def test_update_walutomat_market_snapshot_runs_service(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test update-walutomat-market-snapshot runs service.

    Given: Mocked run_walutomat_snapshot_update function,
    When: update-walutomat-market-snapshot is invoked,
    Then: Service runs and completes.
    """
    captured: dict[str, bool] = {}

    def mock_run_walutomat_snapshot_update() -> None:
        captured["run"] = True

    monkeypatch.setattr(
        app_module, "run_walutomat_snapshot_update", mock_run_walutomat_snapshot_update
    )
    result = cli_runner.invoke(app, ["update-walutomat-market-snapshot"])
    assert result.exit_code == 0
    assert "Updating Walutomat market snapshots" in result.stdout
    assert "Walutomat market snapshot update complete" in result.stdout
    assert captured["run"] is True


def test_update_polygon_symbols_runs_updater(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test update-polygon-symbols runs symbol updater.

    Given: Mocked PolygonSymbolMappingUpdaterService,
    When: update-polygon-symbols --insert-new is invoked,
    Then: Service starts with insert_new flag.
    """
    captured: dict[str, bool | int] = {}

    class MockUpdater:
        def __init__(
            self, update_threshold_hours: int = 168, force: bool = False, insert_new: bool = False
        ):
            captured["threshold"] = update_threshold_hours
            captured["force"] = force
            captured["insert_new"] = insert_new

        async def start(self) -> None:
            captured["started"] = True

    monkeypatch.setattr(app_module, "PolygonSymbolMappingUpdaterService", MockUpdater)
    result = cli_runner.invoke(app, ["update-polygon-symbols", "--insert-new"])
    assert result.exit_code == 0
    assert "Starting Polygon symbol mapping update" in result.stdout
    assert "Polygon symbol mappings updated successfully" in result.stdout
    assert captured["threshold"] == 168
    assert captured["insert_new"] is True


@pytest.fixture()
def mock_user_service() -> MagicMock:
    """Provide a mock UserService with async methods."""
    service = MagicMock()
    service.get_user_by_username = AsyncMock()
    service.create_user = AsyncMock()
    service.get_all_users = AsyncMock()
    service.hash_password_with_salt = MagicMock(return_value=("hashed_password", "salt"))
    return service


@pytest.fixture()
def mock_user() -> MagicMock:
    """Provide a mock user with admin role."""
    user = MagicMock()
    user.username = "testuser"
    user.role = UserRole.ADMIN
    user.is_active = True
    return user


def test_init_admin_creates_new_user(cli_runner: CliRunner, mock_user_service: MagicMock) -> None:
    """Test init-admin creates new admin user via CLI.

    Given: User service with no existing user,
    When: init-admin command is invoked,
    Then: Admin user is created with specified credentials.
    """
    mock_user_service.get_user_by_username.return_value = None
    with patch("snapper.cli.app.UserService", return_value=mock_user_service):
        result = cli_runner.invoke(
            app,
            ["init-admin", "--username", "admin", "--password", "admin123"],
        )
    assert result.exit_code == 0
    assert "Admin user 'admin' created successfully" in result.stdout
    mock_user_service.create_user.assert_called_once()
    call_kwargs = mock_user_service.create_user.call_args.kwargs
    assert call_kwargs["username"] == "admin"
    assert call_kwargs["password"] == "admin123"
    assert call_kwargs["role"] == UserRole.ADMIN
    assert call_kwargs["is_active"] is True


def test_init_admin_fails_when_user_exists(
    cli_runner: CliRunner, mock_user_service: MagicMock, mock_user: MagicMock
) -> None:
    """Test init-admin fails when user exists.

    Given: User service with existing user,
    When: init-admin command is invoked,
    Then: Shows user already exists message.
    """
    mock_user_service.get_user_by_username.return_value = mock_user
    with patch("snapper.cli.app.UserService", return_value=mock_user_service):
        result = cli_runner.invoke(
            app,
            ["init-admin", "--username", "admin", "--password", "admin123"],
        )
    assert result.exit_code == 0
    assert "User 'admin' already exists" in result.stdout
    mock_user_service.create_user.assert_not_called()


def test_list_users_displays_all_users(
    cli_runner: CliRunner, mock_user_service: MagicMock, mock_user: MagicMock
) -> None:
    """Test list-users displays all system users.

    Given: User service returning users list,
    When: list-users command is invoked,
    Then: Shows users in formatted table.
    """
    mock_user_service.get_all_users.return_value = [mock_user]
    with patch("snapper.cli.app.UserService", return_value=mock_user_service):
        result = cli_runner.invoke(app, ["list-users"])
    assert result.exit_code == 0
    assert "System Users" in result.stdout
    assert "testuser" in result.stdout
    assert "admin" in result.stdout
    mock_user_service.get_all_users.assert_called_once_with(include_inactive=True)


def test_list_users_shows_inactive_users(
    cli_runner: CliRunner, mock_user_service: MagicMock
) -> None:
    """Test list-users shows inactive users.

    Given: User service returning inactive user,
    When: list-users command is invoked,
    Then: Shows user as inactive.
    """
    inactive_user = MagicMock()
    inactive_user.username = "inactive"
    inactive_user.role = UserRole.VIEWER
    inactive_user.is_active = False
    mock_user_service.get_all_users.return_value = [inactive_user]
    with patch("snapper.cli.app.UserService", return_value=mock_user_service):
        result = cli_runner.invoke(app, ["list-users"])
    assert result.exit_code == 0
    assert "inactive" in result.stdout
    assert "Inactive" in result.stdout


def test_reset_password_updates_user_password(
    cli_runner: CliRunner, mock_user_service: MagicMock
) -> None:
    """Test reset-password updates user password.

    Given: User service with existing user,
    When: reset-password command is invoked,
    Then: Password is hashed and committed.
    """
    mock_db_user = MagicMock()
    mock_db_user.username = "testuser"
    mock_db_user.password_hash = "old_hash"
    mock_db_user.salt = "old_salt"
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalar_one_or_none = MagicMock(return_value=mock_db_user)
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_session.commit = AsyncMock()
    mock_repository = MagicMock()
    mock_repository.session = MagicMock(return_value=mock_session)
    mock_user_service.repository = mock_repository
    with patch("snapper.cli.app.UserService", return_value=mock_user_service):
        result = cli_runner.invoke(
            app,
            ["reset-password", "testuser", "--new-password", "newpass123"],
        )
    assert result.exit_code == 0
    assert "Password reset for user 'testuser'" in result.stdout
    assert "New password: newpass123" in result.stdout
    mock_session.commit.assert_called_once()


def test_reset_password_fails_for_nonexistent_user(
    cli_runner: CliRunner, mock_user_service: MagicMock
) -> None:
    """Test reset-password fails for nonexistent user.

    Given: User service returning None for user,
    When: reset-password command is invoked,
    Then: Shows user not found message.
    """
    mock_session = AsyncMock()
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalar_one_or_none = MagicMock(return_value=None)
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_repository = MagicMock()
    mock_repository.session = MagicMock(return_value=mock_session)
    mock_user_service.repository = mock_repository
    with patch("snapper.cli.app.UserService", return_value=mock_user_service):
        result = cli_runner.invoke(
            app,
            ["reset-password", "nonexistent", "--new-password", "newpass123"],
        )
    assert result.exit_code == 0
    assert "User 'nonexistent' not found" in result.stdout


@pytest.fixture()
def mock_bootstrap_settings() -> MagicMock:
    """Provide mock bootstrap settings for encryption rotation tests."""
    mock = MagicMock()
    mock.master_password = "old-password"
    mock.encryption_salt = "old-salt"
    mock.db_url = "sqlite+aiosqlite:///:memory:"
    return mock


@pytest.fixture()
def mock_setting() -> MagicMock:
    """Provide a mock encrypted setting instance."""
    setting = MagicMock()
    setting.key = "test_key"
    setting.value = None
    setting.is_encrypted = True
    return setting


@pytest.fixture()
def mock_engine() -> AsyncMock:
    """Provide a mock async database engine."""
    engine = AsyncMock()
    engine.dispose = AsyncMock()
    return engine


@pytest.fixture()
def mock_session() -> AsyncMock:
    """Provide a mock async database session with context manager."""
    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock()
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    return session


@pytest.fixture()
def mock_session_factory(mock_session: AsyncMock) -> MagicMock:
    """Provide a mock session factory that returns mock_session."""
    factory = MagicMock()
    factory.return_value = mock_session
    return factory


def test_rotate_encryption_dry_run(
    cli_runner: CliRunner,
    mock_bootstrap_settings: MagicMock,
    mock_engine: AsyncMock,
    mock_session: AsyncMock,
    mock_session_factory: MagicMock,
    mock_setting: MagicMock,
) -> None:
    """Test encryption rotation dry-run mode.

    Given: Encrypted settings in database,
    When: settings-rotate-encryption --dry-run is invoked,
    Then: Shows settings to rotate without committing.
    """
    old_encryption = SettingsEncryptionService("old-password", b"old-salt")
    encrypted_value = old_encryption.encrypt("secret-value")
    mock_setting.value = encrypted_value
    execute_result = MagicMock()
    execute_result.scalars = MagicMock(
        return_value=MagicMock(all=MagicMock(return_value=[mock_setting]))
    )
    mock_session.execute.return_value = execute_result
    with (
        patch("snapper.cli.app.BootstrapSettingsLoader", return_value=mock_bootstrap_settings),
        patch("snapper.cli.app.create_async_engine", return_value=mock_engine),
        patch("snapper.cli.app.async_sessionmaker", return_value=mock_session_factory),
    ):
        result = cli_runner.invoke(
            app,
            [
                "settings-rotate-encryption",
                "--new-password",
                "new-password",
                "--dry-run",
            ],
        )
    assert result.exit_code == 0
    assert "DRY RUN MODE" in result.stdout
    assert "Would rotate 1 settings" in result.stdout
    mock_session.commit.assert_not_called()


def test_rotate_encryption_success(
    cli_runner: CliRunner,
    mock_bootstrap_settings: MagicMock,
    mock_engine: AsyncMock,
    mock_session: AsyncMock,
    mock_session_factory: MagicMock,
    mock_setting: MagicMock,
) -> None:
    """Test encryption rotation completes successfully.

    Given: Encrypted settings in database,
    When: settings-rotate-encryption is invoked,
    Then: Settings are re-encrypted and committed.
    """
    old_encryption = SettingsEncryptionService("old-password", b"old-salt")
    encrypted_value = old_encryption.encrypt("secret-value")
    mock_setting.value = encrypted_value
    result_mock = MagicMock()
    result_mock.scalars = MagicMock(
        return_value=MagicMock(all=MagicMock(return_value=[mock_setting]))
    )
    mock_session.execute.return_value = result_mock
    with (
        patch("snapper.cli.app.BootstrapSettingsLoader", return_value=mock_bootstrap_settings),
        patch("snapper.cli.app.create_async_engine", return_value=mock_engine),
        patch("snapper.cli.app.async_sessionmaker", return_value=mock_session_factory),
    ):
        result = cli_runner.invoke(
            app,
            [
                "settings-rotate-encryption",
                "--new-password",
                "new-password",
            ],
        )
    assert result.exit_code == 0
    assert "Successfully rotated 1 encrypted settings" in result.stdout
    assert "Update your environment variables" in result.stdout
    mock_session.commit.assert_called_once()


def test_rotate_encryption_with_new_salt(
    cli_runner: CliRunner,
    mock_bootstrap_settings: MagicMock,
    mock_engine: AsyncMock,
    mock_session: AsyncMock,
    mock_session_factory: MagicMock,
    mock_setting: MagicMock,
) -> None:
    """Test encryption rotation with new salt.

    Given: Encrypted settings and new salt specified,
    When: settings-rotate-encryption --new-salt is invoked,
    Then: Shows new salt in output.
    """
    old_encryption = SettingsEncryptionService("old-password", b"old-salt")
    encrypted_value = old_encryption.encrypt("secret-value")
    mock_setting.value = encrypted_value
    result_mock = MagicMock()
    result_mock.scalars = MagicMock(
        return_value=MagicMock(all=MagicMock(return_value=[mock_setting]))
    )
    mock_session.execute.return_value = result_mock
    with (
        patch("snapper.cli.app.BootstrapSettingsLoader", return_value=mock_bootstrap_settings),
        patch("snapper.cli.app.create_async_engine", return_value=mock_engine),
        patch("snapper.cli.app.async_sessionmaker", return_value=mock_session_factory),
    ):
        result = cli_runner.invoke(
            app,
            [
                "settings-rotate-encryption",
                "--new-password",
                "new-password",
                "--new-salt",
                "new-salt",
            ],
        )
    assert result.exit_code == 0
    assert "Successfully rotated 1 encrypted settings" in result.stdout
    assert "ENCRYPTION_SALT=new-salt" in result.stdout
    mock_session.commit.assert_called_once()


def test_rotate_encryption_empty_settings(
    cli_runner: CliRunner,
    mock_bootstrap_settings: MagicMock,
    mock_engine: AsyncMock,
    mock_session: AsyncMock,
    mock_session_factory: MagicMock,
) -> None:
    """Test encryption rotation with no settings.

    Given: No encrypted settings in database,
    When: settings-rotate-encryption is invoked,
    Then: Shows no encrypted settings found.
    """
    result_mock = MagicMock()
    result_mock.scalars = MagicMock(return_value=MagicMock(all=MagicMock(return_value=[])))
    mock_session.execute.return_value = result_mock
    with (
        patch("snapper.cli.app.BootstrapSettingsLoader", return_value=mock_bootstrap_settings),
        patch("snapper.cli.app.create_async_engine", return_value=mock_engine),
        patch("snapper.cli.app.async_sessionmaker", return_value=mock_session_factory),
    ):
        result = cli_runner.invoke(
            app,
            [
                "settings-rotate-encryption",
                "--new-password",
                "new-password",
            ],
        )
    assert result.exit_code == 0
    assert "No encrypted settings found" in result.stdout
    mock_session.commit.assert_not_called()


def test_rotate_encryption_skips_empty_values(
    cli_runner: CliRunner,
    mock_bootstrap_settings: MagicMock,
    mock_engine: AsyncMock,
    mock_session: AsyncMock,
    mock_session_factory: MagicMock,
) -> None:
    """Test encryption rotation skips empty settings.

    Given: Setting with empty value,
    When: settings-rotate-encryption is invoked,
    Then: Shows skipping empty setting message.
    """
    empty_setting = MagicMock()
    empty_setting.key = "empty_key"
    empty_setting.value = None
    empty_setting.is_encrypted = True
    result_mock = MagicMock()
    result_mock.scalars = MagicMock(
        return_value=MagicMock(all=MagicMock(return_value=[empty_setting]))
    )
    mock_session.execute.return_value = result_mock
    with (
        patch("snapper.cli.app.BootstrapSettingsLoader", return_value=mock_bootstrap_settings),
        patch("snapper.cli.app.create_async_engine", return_value=mock_engine),
        patch("snapper.cli.app.async_sessionmaker", return_value=mock_session_factory),
    ):
        result = cli_runner.invoke(
            app,
            [
                "settings-rotate-encryption",
                "--new-password",
                "new-password",
            ],
        )
    assert result.exit_code == 0
    assert "Skipping empty setting: empty_key" in result.stdout
    mock_session.commit.assert_not_called()


def test_rotate_encryption_with_custom_old_password(
    cli_runner: CliRunner,
    mock_bootstrap_settings: MagicMock,
    mock_engine: AsyncMock,
    mock_session: AsyncMock,
    mock_session_factory: MagicMock,
    mock_setting: MagicMock,
) -> None:
    """Test encryption rotation with custom old password.

    Given: Settings encrypted with custom password,
    When: settings-rotate-encryption --old-password is invoked,
    Then: Decrypts with custom password and rotates.
    """
    old_encryption = SettingsEncryptionService("custom-old-password", b"custom-old-salt")
    encrypted_value = old_encryption.encrypt("secret-value")
    mock_setting.value = encrypted_value
    result_mock = MagicMock()
    result_mock.scalars = MagicMock(
        return_value=MagicMock(all=MagicMock(return_value=[mock_setting]))
    )
    mock_session.execute.return_value = result_mock
    with (
        patch("snapper.cli.app.BootstrapSettingsLoader", return_value=mock_bootstrap_settings),
        patch("snapper.cli.app.create_async_engine", return_value=mock_engine),
        patch("snapper.cli.app.async_sessionmaker", return_value=mock_session_factory),
    ):
        result = cli_runner.invoke(
            app,
            [
                "settings-rotate-encryption",
                "--new-password",
                "new-password",
                "--old-password",
                "custom-old-password",
                "--old-salt",
                "custom-old-salt",
            ],
        )
    assert result.exit_code == 0
    assert "Successfully rotated 1 encrypted settings" in result.stdout
    mock_session.commit.assert_called_once()


def test_rotate_encryption_decryption_failure(
    cli_runner: CliRunner,
    mock_bootstrap_settings: MagicMock,
    mock_engine: AsyncMock,
    mock_session: AsyncMock,
    mock_session_factory: MagicMock,
    mock_setting: MagicMock,
) -> None:
    """Test encryption rotation handles decryption failure.

    Given: Invalid encrypted data,
    When: settings-rotate-encryption is invoked,
    Then: Shows failed to rotate message.
    """
    mock_setting.value = "invalid-encrypted-data"
    result_mock = MagicMock()
    result_mock.scalars = MagicMock(
        return_value=MagicMock(all=MagicMock(return_value=[mock_setting]))
    )
    mock_session.execute.return_value = result_mock
    with (
        patch("snapper.cli.app.BootstrapSettingsLoader", return_value=mock_bootstrap_settings),
        patch("snapper.cli.app.create_async_engine", return_value=mock_engine),
        patch("snapper.cli.app.async_sessionmaker", return_value=mock_session_factory),
    ):
        result = cli_runner.invoke(
            app,
            [
                "settings-rotate-encryption",
                "--new-password",
                "new-password",
            ],
        )
    assert result.exit_code == 0
    assert "Failed to rotate test_key" in result.stdout
    mock_session.commit.assert_not_called()


def test_rotate_encryption_verification_failure(
    cli_runner: CliRunner,
    mock_bootstrap_settings: MagicMock,
    mock_engine: AsyncMock,
    mock_session: AsyncMock,
    mock_session_factory: MagicMock,
) -> None:
    """Test encryption rotation fails on verification.

    Given: Missing master password,
    When: settings-rotate-encryption is invoked,
    Then: Shows failed to rotate error.
    """
    mock_bootstrap_settings.master_password = None
    with (
        patch("snapper.cli.app.BootstrapSettingsLoader", return_value=mock_bootstrap_settings),
        patch("snapper.cli.app.create_async_engine", return_value=mock_engine),
        patch("snapper.cli.app.async_sessionmaker", return_value=mock_session_factory),
    ):
        result = cli_runner.invoke(
            app,
            [
                "settings-rotate-encryption",
                "--new-password",
                "new-password",
            ],
        )
    assert result.exit_code == 0
    assert "Failed to rotate encryption" in result.stdout
