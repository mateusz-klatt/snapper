"""Tests for Snapper CLI application."""

import asyncio
import builtins
import importlib
import signal
import sys
import threading
from collections.abc import Callable
from datetime import UTC
from datetime import date as date_type_local
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import typer
import uvicorn
from pydantic import SecretStr
from typer.testing import CliRunner

import snapper.cli.app as app_module
import snapper.messaging.infrastructure.publisher as publisher_module
from snapper.application.process_manager.launcher import CoreProcessStartupError
from snapper.application.process_manager.run_recorder import ProcessRunRecorder
from snapper.application.services.continuous_contract_builder import BuildResult
from snapper.application.services.continuous_contract_builder import RollPointInfo
from snapper.application.updaters.historical.split_repair import SplitRepairCandidate
from snapper.application.updaters.historical.split_repair import SplitRepairSummary
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
from snapper.core.types import ProcessAutostartProfileEnum
from snapper.infrastructure.exchanges.implementations.polygon import PolygonSplitEvent
from snapper.infrastructure.security.encryption import SettingsEncryptionService

SYNC_MEMORY_DB_URL = "sqlite:///:memory:"
ASYNC_MEMORY_DB_URL = "sqlite+aiosqlite:///:memory:"


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
    cfg = _alembic_cfg(SYNC_MEMORY_DB_URL)
    assert cfg.get_main_option("sqlalchemy.url") == SYNC_MEMORY_DB_URL
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
        return SimpleNamespace(db_url=SYNC_MEMORY_DB_URL)

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
    assert captured["db_url"] == SYNC_MEMORY_DB_URL
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
    settings = SimpleNamespace(db_url=SYNC_MEMORY_DB_URL)
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
    assert captured["db_url"] == SYNC_MEMORY_DB_URL
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
    settings = SimpleNamespace(db_url=SYNC_MEMORY_DB_URL)
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
    settings = SimpleNamespace(db_url=SYNC_MEMORY_DB_URL)
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
        "db_url": SYNC_MEMORY_DB_URL,
        "server_host": "127.0.0.1",
        "server_port": 8000,
        "server_reload": False,
        "server_proxy_headers": True,
        "server_forwarded_allow_ips": "127.0.0.1",
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
        app_obj: Any,
        host: str,
        port: int,
        reload: bool,
        log_level: str,
        log_config: Any,
        proxy_headers: bool,
        forwarded_allow_ips: str,
        loop: str,
    ) -> None:
        captured.update(
            {
                "app": app_obj,
                "host": host,
                "port": port,
                "reload": reload,
                "proxy_headers": proxy_headers,
                "forwarded_allow_ips": forwarded_allow_ips,
                "loop": loop,
            }
        )

    monkeypatch.setattr(app_module, "get_settings", lambda: _fake_settings(server_reload=False))
    monkeypatch.setattr(app_module, "create_app", lambda: "APP_INSTANCE")
    monkeypatch.setattr(uvicorn, "run", fake_run)
    result = cli_runner.invoke(app, ["server"])
    assert result.exit_code == 0
    assert captured == {
        "app": "APP_INSTANCE",
        "host": "127.0.0.1",
        "port": 8000,
        "reload": False,
        "proxy_headers": True,
        "forwarded_allow_ips": "127.0.0.1",
        "loop": "uvloop",
    }


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
        proxy_headers: bool,
        forwarded_allow_ips: str,
        loop: str,
    ) -> None:
        captured.update(
            {
                "app_path": app_path,
                "factory": factory,
                "host": host,
                "port": port,
                "reload": reload,
                "proxy_headers": proxy_headers,
                "forwarded_allow_ips": forwarded_allow_ips,
                "loop": loop,
            }
        )

    monkeypatch.setattr(app_module, "get_settings", lambda: _fake_settings(server_reload=True))
    monkeypatch.setattr(uvicorn, "run", fake_run)
    result = cli_runner.invoke(app, ["server"])
    assert result.exit_code == 0
    assert captured["app_path"] == "snapper.server.app:create_app"
    assert captured["factory"] is True
    assert captured["reload"] is True
    assert captured["proxy_headers"] is True
    assert captured["forwarded_allow_ips"] == "127.0.0.1"
    assert captured["loop"] == "uvloop"


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

        def get_run_result(self) -> dict[str, int]:
            return {}

    monkeypatch.setattr(app_module, "PolygonAggregatesBackfillService", DummyService)
    result = cli_runner.invoke(
        app,
        ["polygon-backfill-aggregates", "--symbol", "X:BTCUSD", "--days", "2"],
    )
    assert result.exit_code == 1
    assert "Error during Polygon aggregates backfill" in result.stdout


def test_polygon_backfill_aggregates_writes_process_run(
    monkeypatch: pytest.MonkeyPatch,
    cli_runner: CliRunner,
) -> None:
    """Persist a completed process run for the direct daily CLI path.

    Given: A backfill service replaced by an offline successful fake,
    When: the Polygon aggregates CLI command runs,
    Then: the isolated test database contains a succeeded process run with
        observable symbol outcome counters.
    """

    async def offline_start(self: Any) -> None:
        self._run_stats.selected_symbols = 163
        self._run_stats.fetched_symbols = 163
        self._run_stats.symbols_without_data = 163

    monkeypatch.setattr(
        app_module.PolygonAggregatesBackfillService,
        "start",
        offline_start,
    )
    result = cli_runner.invoke(
        app,
        ["polygon-backfill-aggregates", "--symbol", "AAPL", "--days", "1"],
    )

    async def load_run() -> dict[str, Any]:
        recorder = ProcessRunRecorder(app_module.get_settings())
        runs = await recorder.get_recent_runs(name="polygon_aggregates_backfill", limit=1)
        return runs[0]

    run = asyncio.run(load_run())
    assert result.exit_code == 0
    assert run["status"] == "succeeded"
    assert run["result"] == {
        "selected_symbols": 163,
        "fetched_symbols": 163,
        "symbols_with_data": 0,
        "symbols_without_data": 163,
        "skipped_symbols": 0,
    }


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

    Given a functioning KrakenSymbolUpdaterService,
    When the update-kraken-symbols command is invoked with --force,
    Then it completes successfully with exit code 0.
    """
    started: list[bool] = []

    class DummyUpdater:
        def __init__(self, force: bool) -> None:
            self.force = force

        async def start(self) -> None:
            started.append(self.force)

    monkeypatch.setattr(app_module, "KrakenSymbolUpdaterService", DummyUpdater)
    result = cli_runner.invoke(app, ["update-kraken-symbols", "--force"])
    assert result.exit_code == 0
    assert started == [True]


def test_update_kraken_symbols_error(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test update-kraken-symbols handles updater errors gracefully.

    Given a KrakenSymbolUpdaterService that raises an error,
    When the update-kraken-symbols command is invoked,
    Then it returns exit code 1 with an error message.
    """

    class DummyUpdater:
        def __init__(self, force: bool) -> None:
            self.force = force

        async def start(self) -> None:
            raise RuntimeError("ksym")

    monkeypatch.setattr(app_module, "KrakenSymbolUpdaterService", DummyUpdater)
    result = cli_runner.invoke(app, ["update-kraken-symbols"])
    assert result.exit_code == 1
    assert "Error updating symbol mappings" in result.stdout


def test_update_polygon_symbols_success(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test update-polygon-symbols succeeds with --force and --insert-new flags.

    Given a functioning PolygonSymbolUpdaterService,
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

    monkeypatch.setattr(app_module, "PolygonSymbolUpdaterService", DummyUpdater)
    result = cli_runner.invoke(app, ["update-polygon-symbols", "--force", "--insert-new"])
    assert result.exit_code == 0
    assert started == [(True, True)]


def test_update_polygon_symbols_error(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test update-polygon-symbols handles updater errors gracefully.

    Given a PolygonSymbolUpdaterService that raises an error,
    When the update-polygon-symbols command is invoked,
    Then it returns exit code 1 with an error message.
    """

    class DummyUpdater:
        def __init__(self, update_threshold_hours: int, force: bool, insert_new: bool) -> None:
            self.force = force
            self.insert_new = insert_new

        async def start(self) -> None:
            raise RuntimeError("psym")

    monkeypatch.setattr(app_module, "PolygonSymbolUpdaterService", DummyUpdater)
    result = cli_runner.invoke(app, ["update-polygon-symbols"])
    assert result.exit_code == 1
    assert "Error updating Polygon symbol mappings" in result.stdout


def test_update_walutomat_symbols_success(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test update-walutomat-symbols succeeds with --force flag.

    Given a functioning WalutomatSymbolUpdaterService,
    When the update-walutomat-symbols command is invoked with --force,
    Then it completes successfully with exit code 0.
    """
    started: list[bool] = []

    class DummyUpdater:
        def __init__(self, update_threshold_hours: int, force: bool) -> None:
            self.force = force

        async def start(self) -> None:
            started.append(self.force)

    monkeypatch.setattr(app_module, "WalutomatSymbolUpdaterService", DummyUpdater)
    result = cli_runner.invoke(app, ["update-walutomat-symbols", "--force"])
    assert result.exit_code == 0
    assert started == [True]


def test_update_walutomat_symbols_error(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test update-walutomat-symbols handles updater errors gracefully.

    Given a WalutomatSymbolUpdaterService that raises an error,
    When the update-walutomat-symbols command is invoked,
    Then it returns exit code 1 with an error message.
    """

    class DummyUpdater:
        def __init__(self, update_threshold_hours: int, force: bool) -> None:
            self.force = force

        async def start(self) -> None:
            raise RuntimeError("wfail")

    monkeypatch.setattr(app_module, "WalutomatSymbolUpdaterService", DummyUpdater)
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


def test_update_kraken_futures_market_snapshot_error(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test update-kraken-futures-market-snapshot handles errors gracefully.

    Given a run_kraken_futures_snapshot_update function that raises an error,
    When the update-kraken-futures-market-snapshot command is invoked,
    Then it returns exit code 1 with an error message.
    """

    def _raise_snap() -> None:
        raise RuntimeError("futures_snap")

    monkeypatch.setattr(app_module, "run_kraken_futures_snapshot_update", _raise_snap)
    result = cli_runner.invoke(app, ["update-kraken-futures-market-snapshot"])
    assert result.exit_code == 1
    assert "Error updating Kraken Futures market snapshots" in result.stdout


def test_update_kraken_equities_market_snapshot_error(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test update-kraken-equities-market-snapshot handles errors gracefully.

    Given a run_kraken_equities_snapshot_update function that raises an error,
    When the update-kraken-equities-market-snapshot command is invoked,
    Then it returns exit code 1 with an error message.
    """

    def _raise_snap() -> None:
        raise RuntimeError("equities_snap")

    monkeypatch.setattr(app_module, "run_kraken_equities_snapshot_update", _raise_snap)
    result = cli_runner.invoke(app, ["update-kraken-equities-market-snapshot"])
    assert result.exit_code == 1
    assert "Error updating Kraken Equities market snapshots" in result.stdout


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
    monkeypatch.setattr(app_module, "run_walutomat_snapshot_update", lambda: flags.append("wal"))
    result_kraken = cli_runner.invoke(app, ["update-kraken-market-snapshot"])
    result_wal = cli_runner.invoke(app, ["update-walutomat-market-snapshot"])
    assert result_kraken.exit_code == 0
    assert result_wal.exit_code == 0
    assert flags == ["kraken", "wal"]


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

    def scalars(self) -> _DummyResult:
        return self

    def all(self) -> list[object]:
        return []


class _DummySession:
    """Test dummy for async database session."""

    def __init__(self) -> None:
        self.execute_calls = 0

    async def __aenter__(self) -> _DummySession:
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
    settings = SimpleNamespace(db_url=SYNC_MEMORY_DB_URL)
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
    assert captured["db_url"] == SYNC_MEMORY_DB_URL
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
        db_url=ASYNC_MEMORY_DB_URL,
        master_password="old-pass",
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
            "--dry-run",
        ],
    )
    assert result.exit_code == 0
    assert "No encrypted settings found" in result.stdout
    assert dummy_engine_disposed == []


def test_main_callback_initializes_encryption(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test main callback initializes encryption service.

    Given: Bootstrap settings with master password,
    When: main_callback is invoked,
    Then: get_encryption_service is called.
    """
    called: list[bool] = []
    monkeypatch.setattr(
        app_module,
        "get_encryption_service",
        lambda: called.append(True),
    )
    app_module.main_callback()
    assert called == [True]


def create_mock_settings(**overrides: Any) -> type:
    """Create a mock settings class with optional attribute overrides."""
    default_attrs: dict[str, Any] = {
        "db_url": SYNC_MEMORY_DB_URL,
        "instruments": {"kraken": ["BTC-USD"], "walutomat": [], "polygon": []},
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
        "server_proxy_headers": True,
        "server_forwarded_allow_ips": "127.0.0.1",
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
    """Tests for database CLI commands (init, upgrade, downgrade, seed)."""

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

    def test_db_seed_default_profile(self, cli_runner: CliRunner) -> None:
        """Test db-seed with default dev profile.

        Given: Mocked run_seed returning counts,
        When: db-seed command is invoked without arguments,
        Then: run_seed is called with 'dev' profile.
        """
        with patch("snapper.cli.app.run_seed", return_value=(3, 1)) as mock_run:
            result = cli_runner.invoke(app, ["db-seed"])
            assert result.exit_code == 0
            mock_run.assert_called_once_with("dev")
            assert "3 users" in result.stdout
            assert "1 settings" in result.stdout

    def test_db_seed_custom_profile(self, cli_runner: CliRunner) -> None:
        """Test db-seed with custom profile.

        Given: Mocked run_seed,
        When: db-seed --profile prod is invoked,
        Then: run_seed is called with 'prod' profile.
        """
        with patch("snapper.cli.app.run_seed", return_value=(1, 7)) as mock_run:
            result = cli_runner.invoke(app, ["db-seed", "--profile", "prod"])
            assert result.exit_code == 0
            mock_run.assert_called_once_with("prod")
            assert "1 users" in result.stdout
            assert "7 settings" in result.stdout
            assert "prod" in result.stdout

    def test_db_seed_missing_profile_error(self, cli_runner: CliRunner) -> None:
        """Test db-seed with missing profile shows error.

        Given: run_seed raises FileNotFoundError,
        When: db-seed --profile nonexistent is invoked,
        Then: error message is displayed and exit code is 1.
        """
        with patch(
            "snapper.cli.app.run_seed",
            side_effect=FileNotFoundError("Seed profile 'nonexistent' not found"),
        ):
            result = cli_runner.invoke(app, ["db-seed", "--profile", "nonexistent"])
            assert result.exit_code == 1
            assert "nonexistent" in result.stdout


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
                assert call_kwargs["proxy_headers"] is True
                assert call_kwargs["forwarded_allow_ips"] == "127.0.0.1"

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
                assert call_kwargs["proxy_headers"] is True
                assert call_kwargs["forwarded_allow_ips"] == "127.0.0.1"

    def test_server_command_uses_uvloop(self, cli_runner: CliRunner) -> None:
        """Server explicitly opts in to uvloop instead of the asyncio default.

        Given: production server config (no --reload),
        When: ``server`` is invoked,
        Then: ``uvicorn.run`` receives ``loop="uvloop"`` so the asyncio
            main thread runs on the faster libuv-based loop. Falling
            back to ``_UnixSelectorEventLoop`` under prod tick volume
            (~1500 msgs/s + PG tick-writer) saturated the event loop
            and made even simple endpoints ~200ms+; uvloop is the
            cheapest mitigation that doesn't require restructuring the
            publisher pipeline.
        """
        with patch("snapper.cli.app.get_settings") as mock_get_settings:
            mock_settings = create_mock_settings()
            mock_get_settings.return_value = mock_settings
            with patch("snapper.cli.app.uvicorn.run") as mock_uvicorn_run:
                result = cli_runner.invoke(app, ["server"])
                assert result.exit_code == 0
                call_kwargs = mock_uvicorn_run.call_args[1]
                assert call_kwargs["loop"] == "uvloop"

    def test_server_command_reload_uses_uvloop(self, cli_runner: CliRunner) -> None:
        """The reload branch also routes through uvloop.

        Given: ``server_reload`` is True,
        When: ``server`` is invoked,
        Then: ``uvicorn.run`` still receives ``loop="uvloop"`` — the
            faster loop is desired in both prod and developer-reload
            modes; the only difference between the two branches is
            ``factory=True`` for hot reload.
        """
        with patch("snapper.cli.app.get_settings") as mock_get_settings:
            mock_settings = create_mock_settings()
            mock_settings.server_reload = True
            mock_get_settings.return_value = mock_settings
            with patch("snapper.cli.app.uvicorn.run") as mock_uvicorn_run:
                result = cli_runner.invoke(app, ["server"])
                assert result.exit_code == 0
                call_kwargs = mock_uvicorn_run.call_args[1]
                assert call_kwargs["loop"] == "uvloop"


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

            async def mock_get_all_users(
                include_inactive: bool = False,
                as_of: datetime | None = None,
            ) -> list[Any]:
                return []

            mock_service.get_all_users = mock_get_all_users
            mock_service_class.return_value = mock_service
            list_users()
            mock_service_class.assert_called_once()

    def test_reset_password_with_password(self) -> None:
        """Test reset_password updates user's password.

        Given: User service with existing user,
        When: reset_password is called with new password,
        Then: Delegates to reset_password_by_username.
        """
        with patch("snapper.cli.app.UserService") as mock_service_class:
            mock_service = MagicMock()
            mock_service.reset_password_by_username = AsyncMock()
            mock_service_class.return_value = mock_service
            reset_password(username="testuser", new_password="newpass123")
            mock_service_class.assert_called_once()
            mock_service.reset_password_by_username.assert_called_once_with(
                "testuser", "newpass123"
            )

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
            mock_service.reset_password_by_username = AsyncMock()
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
            _alembic_cfg(SYNC_MEMORY_DB_URL)


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

                async def __aenter__(self) -> _Session:
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

                async def __aenter__(self) -> _Session:
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
            mock_bootstrap_class.return_value = mock_bootstrap
            mock_encryption = MagicMock()
            mock_encryption.encrypt.return_value = "enc1"
            mock_encryption.decrypt.return_value = "test-rotation-verification"
            mock_encryption_class.return_value = mock_encryption
            settings_rotate_encryption(
                old_master_password="old",
                new_master_password="new",
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
            mock_service.get_run_result.return_value = {}
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
            mock_service.get_run_result.return_value = {}
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
        db_url=SYNC_MEMORY_DB_URL,
        server_host="0.0.0.0",
        server_port=8000,
        server_reload=False,
        server_proxy_headers=True,
        server_forwarded_allow_ips="127.0.0.1",
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
    assert captured["uvicorn_kwargs"]["proxy_headers"] is True
    assert captured["uvicorn_kwargs"]["forwarded_allow_ips"] == "127.0.0.1"


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
    assert captured["uvicorn_kwargs"]["proxy_headers"] is True
    assert captured["uvicorn_kwargs"]["forwarded_allow_ips"] == "127.0.0.1"


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

    Given: Mocked KrakenSymbolUpdaterService,
    When: update-kraken-symbols is invoked,
    Then: Service starts and completes.
    """
    captured: dict[str, bool] = {}

    class MockUpdater:
        def __init__(self, force: bool = False):
            captured["force"] = force

        async def start(self) -> None:
            captured["started"] = True

    monkeypatch.setattr(app_module, "KrakenSymbolUpdaterService", MockUpdater)
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

    Given: Mocked KrakenSymbolUpdaterService,
    When: update-kraken-symbols --force is invoked,
    Then: Force flag is passed to service.
    """
    captured: dict[str, bool] = {}

    class MockUpdater:
        def __init__(self, force: bool = False):
            captured["force"] = force

        async def start(self) -> None:
            captured["started"] = True

    monkeypatch.setattr(app_module, "KrakenSymbolUpdaterService", MockUpdater)
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

    monkeypatch.setattr(app_module, "KrakenSymbolUpdaterService", MockUpdater)
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


def test_update_kraken_futures_symbols_runs_updater(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test update-kraken-futures-symbols runs symbol updater.

    Given: Mocked KrakenFuturesSymbolUpdaterService,
    When: update-kraken-futures-symbols is invoked with --force,
    Then: Updater is created with force=True and started.
    """
    started: list[bool] = []

    class DummyUpdater:
        def __init__(self, force: bool) -> None:
            self.force = force

        async def start(self) -> None:
            started.append(self.force)

    monkeypatch.setattr(app_module, "KrakenFuturesSymbolUpdaterService", DummyUpdater)
    result = cli_runner.invoke(app, ["update-kraken-futures-symbols", "--force"])
    assert result.exit_code == 0
    assert started == [True]


def test_update_kraken_futures_symbols_handles_exception(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test update-kraken-futures-symbols handles updater errors.

    Given: KrakenFuturesSymbolUpdaterService that raises,
    When: update-kraken-futures-symbols is invoked,
    Then: Returns exit code 1.
    """

    class DummyUpdater:
        def __init__(self, force: bool) -> None:
            self.force = force

        async def start(self) -> None:
            raise RuntimeError("futures error")

    monkeypatch.setattr(app_module, "KrakenFuturesSymbolUpdaterService", DummyUpdater)
    result = cli_runner.invoke(app, ["update-kraken-futures-symbols"])
    assert result.exit_code == 1


def test_update_kraken_equities_symbols_runs_updater(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test update-kraken-equities-symbols runs symbol updater.

    Given: Mocked KrakenEquitiesSymbolUpdaterService,
    When: update-kraken-equities-symbols is invoked with --force,
    Then: Updater is created with force=True and started.
    """
    started: list[bool] = []

    class DummyUpdater:
        def __init__(self, force: bool) -> None:
            self.force = force

        async def start(self) -> None:
            started.append(self.force)

    monkeypatch.setattr(app_module, "KrakenEquitiesSymbolUpdaterService", DummyUpdater)
    result = cli_runner.invoke(app, ["update-kraken-equities-symbols", "--force"])
    assert result.exit_code == 0
    assert started == [True]


def test_update_kraken_equities_symbols_handles_exception(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test update-kraken-equities-symbols handles updater errors.

    Given: KrakenEquitiesSymbolUpdaterService that raises,
    When: update-kraken-equities-symbols is invoked,
    Then: Returns exit code 1.
    """

    class DummyUpdater:
        def __init__(self, force: bool) -> None:
            self.force = force

        async def start(self) -> None:
            raise RuntimeError("equities error")

    monkeypatch.setattr(app_module, "KrakenEquitiesSymbolUpdaterService", DummyUpdater)
    result = cli_runner.invoke(app, ["update-kraken-equities-symbols"])
    assert result.exit_code == 1


def test_update_kraken_futures_market_snapshot_runs_service(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test update-kraken-futures-market-snapshot runs service.

    Given: Mocked run_kraken_futures_snapshot_update function,
    When: update-kraken-futures-market-snapshot is invoked,
    Then: Service runs and completes.
    """
    captured: dict[str, bool] = {}

    def mock_run() -> None:
        captured["run"] = True

    monkeypatch.setattr(app_module, "run_kraken_futures_snapshot_update", mock_run)
    result = cli_runner.invoke(app, ["update-kraken-futures-market-snapshot"])
    assert result.exit_code == 0
    assert "Updating Kraken Futures market snapshots" in result.stdout
    assert "Kraken Futures market snapshot update complete" in result.stdout
    assert captured["run"] is True


def test_update_kraken_equities_market_snapshot_runs_service(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test update-kraken-equities-market-snapshot runs service.

    Given: Mocked run_kraken_equities_snapshot_update function,
    When: update-kraken-equities-market-snapshot is invoked,
    Then: Service runs and completes.
    """
    captured: dict[str, bool] = {}

    def mock_run() -> None:
        captured["run"] = True

    monkeypatch.setattr(app_module, "run_kraken_equities_snapshot_update", mock_run)
    result = cli_runner.invoke(app, ["update-kraken-equities-market-snapshot"])
    assert result.exit_code == 0
    assert "Updating Kraken Equities market snapshots" in result.stdout
    assert "Kraken Equities market snapshot update complete" in result.stdout
    assert captured["run"] is True


def test_update_walutomat_symbols_runs_updater(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test update-walutomat-symbols runs symbol updater.

    Given: Mocked WalutomatSymbolUpdaterService,
    When: update-walutomat-symbols is invoked,
    Then: Service starts with default threshold.
    """
    captured: dict[str, bool | int] = {}

    class MockUpdater:
        def __init__(self, update_threshold_hours: int = 168, force: bool = False) -> None:
            captured["threshold"] = update_threshold_hours
            captured["force"] = force

        async def start(self) -> None:
            captured["started"] = True

    monkeypatch.setattr(app_module, "WalutomatSymbolUpdaterService", MockUpdater)
    result = cli_runner.invoke(app, ["update-walutomat-symbols"])
    assert result.exit_code == 0
    assert "Starting Walutomat symbol mapping update" in result.stdout
    assert "Walutomat symbol mappings updated successfully" in result.stdout
    assert captured["threshold"] == 168


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

    Given: Mocked PolygonSymbolUpdaterService,
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

    monkeypatch.setattr(app_module, "PolygonSymbolUpdaterService", MockUpdater)
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
    service.hash_password = MagicMock(return_value="hashed_password")
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
    Then: Delegates to reset_password_by_username and shows success.
    """
    mock_user_service.reset_password_by_username = AsyncMock()
    with patch("snapper.cli.app.UserService", return_value=mock_user_service):
        result = cli_runner.invoke(
            app,
            ["reset-password", "testuser", "--new-password", "newpass123"],
        )
    assert result.exit_code == 0
    assert "Password reset for user 'testuser'" in result.stdout
    assert "newpass123" not in result.stdout
    mock_user_service.reset_password_by_username.assert_called_once_with("testuser", "newpass123")


def test_reset_password_fails_for_nonexistent_user(
    cli_runner: CliRunner, mock_user_service: MagicMock
) -> None:
    """Test reset-password fails for nonexistent user.

    Given: User service raising ValueError for missing user,
    When: reset-password command is invoked,
    Then: Shows user not found message.
    """
    mock_user_service.reset_password_by_username = AsyncMock(
        side_effect=ValueError("User 'nonexistent' not found")
    )
    with patch("snapper.cli.app.UserService", return_value=mock_user_service):
        result = cli_runner.invoke(
            app,
            ["reset-password", "nonexistent", "--new-password", "newpass123"],
        )
    assert result.exit_code == 0
    assert "User 'nonexistent' not found" in result.stdout


def test_reset_password_handles_generic_exception(
    cli_runner: CliRunner, mock_user_service: MagicMock
) -> None:
    """Test reset-password handles unexpected exceptions.

    Given: User service raising RuntimeError,
    When: reset-password command is invoked,
    Then: Shows failure message.
    """
    mock_user_service.reset_password_by_username = AsyncMock(side_effect=RuntimeError("db down"))
    with patch("snapper.cli.app.UserService", return_value=mock_user_service):
        result = cli_runner.invoke(
            app,
            ["reset-password", "testuser", "--new-password", "newpass123"],
        )
    assert result.exit_code == 0
    assert "Failed to reset password: db down" in result.stdout


@pytest.fixture()
def mock_bootstrap_settings() -> MagicMock:
    """Provide mock bootstrap settings for encryption rotation tests."""
    mock = MagicMock()
    mock.master_password = "old-password"
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
    session.add = MagicMock()
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
    old_encryption = SettingsEncryptionService("old-password")
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
    old_encryption = SettingsEncryptionService("old-password")
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
    assert "Update MASTER_PASSWORD in your .env" in result.stdout
    assert "intentionally NOT printed" in result.stdout
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
    old_encryption = SettingsEncryptionService("custom-old-password")
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

    Given: Verification raises ValueError,
    When: settings-rotate-encryption is invoked,
    Then: Shows failed to rotate error.
    """
    with (
        patch("snapper.cli.app.BootstrapSettingsLoader", return_value=mock_bootstrap_settings),
        patch("snapper.cli.app.create_async_engine", return_value=mock_engine),
        patch("snapper.cli.app.async_sessionmaker", return_value=mock_session_factory),
        patch(
            "snapper.cli.app._verify_encryption_services",
            side_effect=ValueError("Encryption verification failed"),
        ),
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


def test_update_underlyings_success(cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test update-underlyings runs updater.

    Given: Mocked UnderlyingUpdater,
    When: update-underlyings is invoked with --force,
    Then: Service runs and completes.
    """
    captured: dict[str, object] = {}

    class MockUpdater:
        def __init__(self, db_url: str, force: bool = False) -> None:
            captured["db_url"] = db_url
            captured["force"] = force

        async def run(self) -> None:
            captured["ran"] = True

    monkeypatch.setattr(app_module, "UnderlyingUpdater", MockUpdater)
    result = cli_runner.invoke(app, ["update-underlyings", "--force"])
    assert result.exit_code == 0
    assert "Updating underlying asset mappings" in result.stdout
    assert "updated successfully" in result.stdout
    assert captured["force"] is True
    assert captured["ran"] is True


def test_update_underlyings_no_force(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test update-underlyings default (no --force).

    Given: Mocked UnderlyingUpdater,
    When: update-underlyings is invoked without flags,
    Then: force is False.
    """
    captured: dict[str, object] = {}

    class MockUpdater:
        def __init__(self, db_url: str, force: bool = False) -> None:
            captured["force"] = force

        async def run(self) -> None:
            pass

    monkeypatch.setattr(app_module, "UnderlyingUpdater", MockUpdater)
    result = cli_runner.invoke(app, ["update-underlyings"])
    assert result.exit_code == 0
    assert captured["force"] is False


def test_build_continuous_success(cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test build-continuous command runs builder and displays output.

    Given: Mocked BootstrapSettingsLoader, repository, and ContinuousContractBuilder,
    When: build-continuous is invoked with required arguments,
    Then: Builder runs and output shows contracts used, bar count, and roll points.
    """
    mock_underlying = {
        "public_id": "ua-1",
        "ticker": "SPX",
        "name": "S&P 500",
    }
    roll_point = RollPointInfo(
        from_contract="ESM6",
        to_contract="ESU6",
        roll_at=datetime(2026, 1, 3, tzinfo=UTC),
        adjustment=10.0,
    )
    build_result = BuildResult(
        candles=[],
        contracts_used=["ESM6", "ESU6"],
        roll_points=[roll_point],
        failed_roll=None,
    )

    mock_repo = AsyncMock()
    mock_repo.get_underlying_by_ticker = AsyncMock(return_value=mock_underlying)

    mock_builder = AsyncMock()
    mock_builder.build = AsyncMock(return_value=build_result)

    mock_bootstrap = MagicMock()
    mock_bootstrap.db_url = "sqlite:///:memory:"

    monkeypatch.setattr(app_module, "BootstrapSettingsLoader", lambda: mock_bootstrap)
    monkeypatch.setattr(app_module, "get_repository", lambda db_url: mock_repo)
    monkeypatch.setattr(app_module, "ContinuousContractBuilder", lambda repository: mock_builder)

    result = cli_runner.invoke(app, ["build-continuous", "SPX", "kraken_equities", "ES"])
    assert result.exit_code == 0
    assert "Contracts used: 2" in result.stdout
    assert "ESM6" in result.stdout
    assert "ESU6" in result.stdout
    assert "Total bars: 0" in result.stdout
    assert "Roll points: 1" in result.stdout
    assert "adj=10.0" in result.stdout


def test_build_continuous_underlying_not_found(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test build-continuous exits with error when underlying not found.

    Given: Repository returns None for get_underlying_by_ticker,
    When: build-continuous is invoked,
    Then: Exit code 1 and error message printed.
    """
    mock_repo = AsyncMock()
    mock_repo.get_underlying_by_ticker = AsyncMock(return_value=None)

    mock_bootstrap = MagicMock()
    mock_bootstrap.db_url = "sqlite:///:memory:"

    monkeypatch.setattr(app_module, "BootstrapSettingsLoader", lambda: mock_bootstrap)
    monkeypatch.setattr(app_module, "get_repository", lambda db_url: mock_repo)

    result = cli_runner.invoke(app, ["build-continuous", "NOPE", "kraken_equities", "ES"])
    assert result.exit_code == 1
    assert "Underlying not found: NOPE" in result.output


def test_build_continuous_with_failed_roll(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test build-continuous shows warning when series is truncated.

    Given: Builder returns result with failed_roll,
    When: build-continuous is invoked,
    Then: Output includes WARNING about truncated series.
    """
    mock_underlying = {"public_id": "ua-1", "ticker": "SPX", "name": "S&P 500"}
    failed = RollPointInfo(
        from_contract="ESM6",
        to_contract="ESU6",
        roll_at=datetime(2026, 1, 3, tzinfo=UTC),
        adjustment=None,
    )
    build_result = BuildResult(
        candles=[],
        contracts_used=["ESM6", "ESU6"],
        roll_points=[],
        failed_roll=failed,
    )

    mock_repo = AsyncMock()
    mock_repo.get_underlying_by_ticker = AsyncMock(return_value=mock_underlying)

    mock_builder = AsyncMock()
    mock_builder.build = AsyncMock(return_value=build_result)

    mock_bootstrap = MagicMock()
    mock_bootstrap.db_url = "sqlite:///:memory:"

    monkeypatch.setattr(app_module, "BootstrapSettingsLoader", lambda: mock_bootstrap)
    monkeypatch.setattr(app_module, "get_repository", lambda db_url: mock_repo)
    monkeypatch.setattr(app_module, "ContinuousContractBuilder", lambda repository: mock_builder)

    result = cli_runner.invoke(app, ["build-continuous", "SPX", "kraken_equities", "ES"])
    assert result.exit_code == 0
    assert "WARNING" in result.stdout


def test_build_continuous_with_explicit_dates(
    cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test build-continuous parses --start/--end dates and normalizes to UTC.

    Given: --start and --end as ISO strings (one naive, one offset-aware),
    When: build-continuous is invoked,
    Then: builder.build receives UTC-normalized datetimes.
    """
    captured_kwargs: dict[str, Any] = {}
    mock_result = MagicMock()
    mock_result.candles = []
    mock_result.contracts_used = []
    mock_result.roll_points = []
    mock_result.failed_roll = None

    async def _capture_build(**kwargs: Any) -> MagicMock:
        captured_kwargs.update(kwargs)
        return mock_result

    mock_builder = MagicMock()
    mock_builder.build = _capture_build
    mock_repo = AsyncMock()
    mock_repo.get_underlying_by_ticker = AsyncMock(
        return_value={"public_id": "u1", "ticker": "SPX"}
    )
    mock_bootstrap = MagicMock()
    mock_bootstrap.db_url = "sqlite:///:memory:"

    monkeypatch.setattr(app_module, "BootstrapSettingsLoader", lambda: mock_bootstrap)
    monkeypatch.setattr(app_module, "get_repository", lambda db_url: mock_repo)
    monkeypatch.setattr(app_module, "ContinuousContractBuilder", lambda repository: mock_builder)

    result = cli_runner.invoke(
        app,
        [
            "build-continuous",
            "SPX",
            "kraken_equities",
            "ES",
            "--start",
            "2026-01-01",
            "--end",
            "2026-06-01T00:00:00+02:00",
        ],
    )
    assert result.exit_code == 0

    assert captured_kwargs["start"].tzinfo == UTC
    assert captured_kwargs["end"].tzinfo == UTC


def test_build_continuous_rejects_negative_rollover_days(cli_runner: CliRunner) -> None:
    """R2: --rollover-days=-1 must exit non-zero before builder is invoked.

    Given: build-continuous CLI command with typer.Option(min=0, max=365),
    When: invoked with --rollover-days=-1,
    Then: typer exits non-zero at parse time with the rollover field
        named in the error output, without invoking the builder.
    """
    with patch("snapper.cli.app.ContinuousContractBuilder") as mock_builder_cls:
        result = cli_runner.invoke(
            app,
            [
                "build-continuous",
                "SPX",
                "kraken_equities",
                "ES",
                "--rollover-days",
                "-1",
            ],
        )

    assert result.exit_code == 2
    mock_builder_cls.assert_not_called()


def test_build_continuous_rejects_excessive_rollover_days(cli_runner: CliRunner) -> None:
    """R2: --rollover-days=366 must exit non-zero before builder is invoked.

    Given: build-continuous CLI command with typer.Option(min=0, max=365),
    When: invoked with --rollover-days=366,
    Then: typer exits non-zero at parse time with the rollover field
        named in the error output, without invoking the builder.
    """
    with patch("snapper.cli.app.ContinuousContractBuilder") as mock_builder_cls:
        result = cli_runner.invoke(
            app,
            [
                "build-continuous",
                "SPX",
                "kraken_equities",
                "ES",
                "--rollover-days",
                "366",
            ],
        )

    assert result.exit_code == 2
    mock_builder_cls.assert_not_called()


class TestBacktestList:
    """Tests for backtest-list CLI command."""

    def test_list_empty(self, cli_runner: CliRunner) -> None:
        """No runs shows empty message."""
        mock_bt_repo = AsyncMock()
        mock_bt_repo.list_runs = AsyncMock(return_value=[])

        with (
            patch("snapper.cli.app.BootstrapSettingsLoader") as mock_boot,
            patch("snapper.cli.app.get_repository") as mock_get_repo,
            patch("snapper.cli.app.BacktestRepository", return_value=mock_bt_repo),
        ):
            mock_boot.return_value.db_url = ASYNC_MEMORY_DB_URL
            mock_repo = MagicMock()
            mock_repo.session_factory = MagicMock()
            mock_get_repo.return_value = mock_repo

            result = cli_runner.invoke(app, ["backtest-list"])
            assert result.exit_code == 0
            assert "No backtest runs found" in result.output

    def test_list_with_runs(self, cli_runner: CliRunner) -> None:
        """Runs are displayed in table format."""
        mock_bt_repo = AsyncMock()
        mock_bt_repo.list_runs = AsyncMock(
            return_value=[
                {
                    "public_id": "run-123456789012",
                    "strategy_name": "sma_cross",
                    "status": "completed",
                    "instrument_public_id": "BTC-USD",
                    "exchange": "kraken",
                    "start_date": datetime(2026, 1, 1, tzinfo=UTC),
                    "end_date": datetime(2026, 6, 1, tzinfo=UTC),
                }
            ]
        )

        with (
            patch("snapper.cli.app.BootstrapSettingsLoader") as mock_boot,
            patch("snapper.cli.app.get_repository") as mock_get_repo,
            patch("snapper.cli.app.BacktestRepository", return_value=mock_bt_repo),
        ):
            mock_boot.return_value.db_url = ASYNC_MEMORY_DB_URL
            mock_repo = MagicMock()
            mock_repo.session_factory = MagicMock()
            mock_get_repo.return_value = mock_repo

            result = cli_runner.invoke(app, ["backtest-list"])
            assert result.exit_code == 0
            assert "sma_cross" in result.output
            assert "1 run(s)" in result.output


class TestBacktestCancel:
    """Tests for backtest-cancel CLI command."""

    def test_cancel_running(self, cli_runner: CliRunner) -> None:
        """Running run gets cancel_requested."""
        mock_bt_repo = AsyncMock()
        mock_bt_repo.get_run = AsyncMock(return_value={"status": "running", "public_id": "run-1"})
        mock_bt_repo.update_run_status = AsyncMock(return_value=1)

        with (
            patch("snapper.cli.app.BootstrapSettingsLoader") as mock_boot,
            patch("snapper.cli.app.get_repository") as mock_get_repo,
            patch("snapper.cli.app.BacktestRepository", return_value=mock_bt_repo),
        ):
            mock_boot.return_value.db_url = ASYNC_MEMORY_DB_URL
            mock_repo = MagicMock()
            mock_repo.session_factory = MagicMock()
            mock_get_repo.return_value = mock_repo

            result = cli_runner.invoke(app, ["backtest-cancel", "run-1"])
            assert result.exit_code == 0
            assert "Cancel requested" in result.output

    def test_cancel_not_found(self, cli_runner: CliRunner) -> None:
        """Missing run exits with error."""
        mock_bt_repo = AsyncMock()
        mock_bt_repo.get_run = AsyncMock(return_value=None)

        with (
            patch("snapper.cli.app.BootstrapSettingsLoader") as mock_boot,
            patch("snapper.cli.app.get_repository") as mock_get_repo,
            patch("snapper.cli.app.BacktestRepository", return_value=mock_bt_repo),
        ):
            mock_boot.return_value.db_url = ASYNC_MEMORY_DB_URL
            mock_repo = MagicMock()
            mock_repo.session_factory = MagicMock()
            mock_get_repo.return_value = mock_repo

            result = cli_runner.invoke(app, ["backtest-cancel", "nonexistent"])
            assert result.exit_code == 1
            assert "not found" in result.output.lower()

    def test_cancel_completed_exits_error(self, cli_runner: CliRunner) -> None:
        """Completed run cannot be cancelled."""
        mock_bt_repo = AsyncMock()
        mock_bt_repo.get_run = AsyncMock(return_value={"status": "completed", "public_id": "run-1"})

        with (
            patch("snapper.cli.app.BootstrapSettingsLoader") as mock_boot,
            patch("snapper.cli.app.get_repository") as mock_get_repo,
            patch("snapper.cli.app.BacktestRepository", return_value=mock_bt_repo),
        ):
            mock_boot.return_value.db_url = ASYNC_MEMORY_DB_URL
            mock_repo = MagicMock()
            mock_repo.session_factory = MagicMock()
            mock_get_repo.return_value = mock_repo

            result = cli_runner.invoke(app, ["backtest-cancel", "run-1"])
            assert result.exit_code == 1
            assert "Cannot cancel" in result.output


class TestBacktestRerun:
    """Tests for backtest-rerun CLI command."""

    def test_rerun_not_found(self, cli_runner: CliRunner) -> None:
        """Missing original run exits with error."""
        with (
            patch("snapper.cli.app.BootstrapSettingsLoader") as mock_boot,
            patch("snapper.cli.app.get_repository") as mock_get_repo,
            patch("snapper.cli.app.BacktestRepository") as mock_bt_cls,
        ):
            mock_boot.return_value.db_url = ASYNC_MEMORY_DB_URL
            mock_repo = MagicMock()
            mock_repo.session_factory = MagicMock()
            mock_get_repo.return_value = mock_repo
            mock_bt = AsyncMock()
            mock_bt.get_run = AsyncMock(return_value=None)
            mock_bt_cls.return_value = mock_bt

            result = cli_runner.invoke(app, ["backtest-rerun", "nonexistent"])
            assert result.exit_code == 1
            assert "not found" in result.output.lower()

    @patch.dict(
        "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", {"sma_cross": MagicMock()}
    )
    def test_rerun_success(self, cli_runner: CliRunner) -> None:
        """Found run is re-run with same config."""
        original = {
            "strategy_name": "sma_cross",
            "instrument_public_id": "BTC-USD",
            "exchange": "kraken",
            "timeframe": "1h",
            "start_date": datetime(2026, 1, 1, tzinfo=UTC),
            "end_date": datetime(2026, 6, 1, tzinfo=UTC),
            "initial_cash": 10000.0,
            "wallet_public_id": "w-1",
            "execution_mode": "direct_db",
            "fill_model": "market",
            "slippage_bps": 0.0,
            "commission_bps": 0.0,
        }
        mock_bt_repo = AsyncMock()
        mock_bt_repo.get_run = AsyncMock(return_value=original)
        mock_bt_repo.create_run = AsyncMock(return_value=(1, "run-rerun"))
        mock_bt_repo.insert_signals_batch = AsyncMock()
        mock_bt_repo.insert_trades_batch = AsyncMock()
        mock_bt_repo.insert_equity_points_batch = AsyncMock()
        mock_bt_repo.insert_result = AsyncMock(return_value="res-1")
        mock_bt_repo.update_run_status = AsyncMock(return_value=1)

        mock_engine = AsyncMock()
        mock_engine.run = AsyncMock(return_value=(MagicMock(), {}))

        with (
            patch("snapper.cli.app.BootstrapSettingsLoader") as mock_boot,
            patch("snapper.cli.app.get_repository") as mock_get_repo,
            patch("snapper.cli.app.BacktestRepository", return_value=mock_bt_repo),
            patch("snapper.cli.app.DirectDbEngine", return_value=mock_engine),
        ):
            mock_boot.return_value.db_url = ASYNC_MEMORY_DB_URL
            mock_repo = MagicMock()
            mock_repo.session_factory = MagicMock()
            mock_repo.get_instrument_public_id_by_symbol = AsyncMock(
                return_value="instrument-uuid-1"
            )
            mock_get_repo.return_value = mock_repo

            result = cli_runner.invoke(app, ["backtest-rerun", "run-1"])
            assert result.exit_code == 0
            assert "Re-running" in result.output
            assert "sma_cross" in result.output
            create_call = mock_bt_repo.create_run.await_args
            assert create_call is not None
            row_arg = create_call.kwargs.get("row") or create_call.args[0]
            assert row_arg["execution_mode"] == "direct_db"
            assert row_arg["fill_model"] == "market"
            assert row_arg["slippage_bps"] == pytest.approx(0.0)
            assert row_arg["commission_bps"] == pytest.approx(0.0)


class TestBacktestRerunPreservesPhase2Fields:
    """backtest-rerun must propagate non-default execution-config fields."""

    @patch.dict(
        "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES",
        {"sma_cross": MagicMock()},
    )
    def test_rerun_propagates_non_default_fields(self, cli_runner: CliRunner) -> None:
        """Non-default execution_mode / slippage / commission survive a rerun."""
        original = {
            "strategy_name": "sma_cross",
            "instrument_public_id": "BTC-USD",
            "exchange": "kraken",
            "timeframe": "1h",
            "start_date": datetime(2026, 1, 1, tzinfo=UTC),
            "end_date": datetime(2026, 6, 1, tzinfo=UTC),
            "initial_cash": 10000.0,
            "wallet_public_id": "w-1",
            "execution_mode": "zmq_replay",
            "fill_model": "market",
            "slippage_bps": 12.5,
            "commission_bps": 7.5,
        }
        mock_bt_repo = AsyncMock()
        mock_bt_repo.get_run = AsyncMock(return_value=original)
        mock_bt_repo.create_run = AsyncMock(return_value=(1, "run-rerun"))
        mock_bt_repo.insert_signals_batch = AsyncMock()
        mock_bt_repo.insert_trades_batch = AsyncMock()
        mock_bt_repo.insert_equity_points_batch = AsyncMock()
        mock_bt_repo.insert_result = AsyncMock(return_value="res-1")
        mock_bt_repo.update_run_status = AsyncMock(return_value=1)
        mock_engine = AsyncMock()
        mock_engine.run = AsyncMock(return_value=(MagicMock(), {}))
        with (
            patch("snapper.cli.app.BootstrapSettingsLoader") as mock_boot,
            patch("snapper.cli.app.get_repository") as mock_get_repo,
            patch("snapper.cli.app.BacktestRepository", return_value=mock_bt_repo),
            patch("snapper.cli.app.DirectDbEngine", return_value=mock_engine),
        ):
            mock_boot.return_value.db_url = ASYNC_MEMORY_DB_URL
            mock_repo = MagicMock()
            mock_repo.session_factory = MagicMock()
            mock_repo.get_instrument_public_id_by_symbol = AsyncMock(
                return_value="instrument-uuid-1"
            )
            mock_get_repo.return_value = mock_repo
            result = cli_runner.invoke(app, ["backtest-rerun", "run-1"])
            assert result.exit_code == 0
            create_call = mock_bt_repo.create_run.await_args
            assert create_call is not None
            row_arg = create_call.kwargs.get("row") or create_call.args[0]
            assert row_arg["execution_mode"] == "zmq_replay"
            assert row_arg["slippage_bps"] == pytest.approx(12.5)
            assert row_arg["commission_bps"] == pytest.approx(7.5)


class TestBacktestRun:
    """Tests for backtest-run CLI command."""

    @patch.dict(
        "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", {"sma_cross": MagicMock()}
    )
    def test_run_success(self, cli_runner: CliRunner) -> None:
        """Successful backtest run prints metrics."""
        mock_bt_repo = AsyncMock()
        mock_bt_repo.create_run = AsyncMock(return_value=(1, "run-new"))
        mock_bt_repo.insert_signals_batch = AsyncMock()
        mock_bt_repo.insert_trades_batch = AsyncMock()
        mock_bt_repo.insert_equity_points_batch = AsyncMock()
        mock_bt_repo.insert_result = AsyncMock(return_value="res-1")
        mock_bt_repo.update_run_status = AsyncMock(return_value=1)

        mock_engine = AsyncMock()
        mock_engine.run = AsyncMock(return_value=(MagicMock(), {}))

        mock_collector = MagicMock()
        mock_collector.signals = [{"s": 1}]
        mock_collector.trades = [{"t": 1}]
        mock_collector.equity_points = [{"e": 1}]

        mock_metrics = MagicMock()
        mock_metrics.total_trades = 5
        mock_metrics.total_pnl = 500.0
        mock_metrics.sharpe_ratio = 1.5
        mock_metrics.max_drawdown = 0.1
        mock_metrics.winning_trades = 3
        mock_metrics.losing_trades = 2
        mock_metrics.win_rate = 0.6
        mock_metrics.profit_factor = 3.0
        mock_metrics.final_equity = 10500.0
        mock_metrics.max_equity = 11000.0

        with (
            patch("snapper.cli.app.BootstrapSettingsLoader") as mock_boot,
            patch("snapper.cli.app.get_repository") as mock_get_repo,
            patch("snapper.cli.app.BacktestRepository", return_value=mock_bt_repo),
            patch("snapper.cli.app.DirectDbEngine", return_value=mock_engine),
            patch("snapper.cli.app.ResultCollector", return_value=mock_collector),
            patch("snapper.cli.app.compute_metrics", return_value=mock_metrics),
        ):
            mock_boot.return_value.db_url = ASYNC_MEMORY_DB_URL
            mock_repo = MagicMock()
            mock_repo.session_factory = MagicMock()
            mock_repo.get_instrument_public_id_by_symbol = AsyncMock(
                return_value="instrument-uuid-1"
            )
            mock_get_repo.return_value = mock_repo

            result = cli_runner.invoke(
                app,
                [
                    "backtest-run",
                    "--strategy",
                    "sma_cross",
                    "--instrument",
                    "BTC-USD",
                    "--exchange",
                    "kraken",
                    "--start",
                    "2026-01-01",
                    "--end",
                    "2026-06-01",
                ],
            )
            assert result.exit_code == 0
            assert "completed" in result.output.lower()
            mock_bt_repo.insert_signals_batch.assert_called_once()
            mock_bt_repo.insert_trades_batch.assert_called_once()
            mock_bt_repo.insert_equity_points_batch.assert_called_once()

    @patch.dict(
        "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", {"sma_cross": MagicMock()}
    )
    def test_run_engine_failure(self, cli_runner: CliRunner) -> None:
        """Engine failure marks run as failed and exits 1."""
        mock_bt_repo = AsyncMock()
        mock_bt_repo.create_run = AsyncMock(return_value=(1, "run-fail"))
        mock_bt_repo.update_run_status = AsyncMock(return_value=1)

        mock_engine = AsyncMock()
        mock_engine.run = AsyncMock(side_effect=RuntimeError("no data"))

        with (
            patch("snapper.cli.app.BootstrapSettingsLoader") as mock_boot,
            patch("snapper.cli.app.get_repository") as mock_get_repo,
            patch("snapper.cli.app.BacktestRepository", return_value=mock_bt_repo),
            patch("snapper.cli.app.DirectDbEngine", return_value=mock_engine),
        ):
            mock_boot.return_value.db_url = ASYNC_MEMORY_DB_URL
            mock_repo = MagicMock()
            mock_repo.session_factory = MagicMock()
            mock_repo.get_instrument_public_id_by_symbol = AsyncMock(
                return_value="instrument-uuid-1"
            )
            mock_get_repo.return_value = mock_repo

            result = cli_runner.invoke(
                app,
                [
                    "backtest-run",
                    "--strategy",
                    "sma_cross",
                    "--instrument",
                    "BTC-USD",
                    "--exchange",
                    "kraken",
                    "--start",
                    "2026-01-01",
                    "--end",
                    "2026-06-01",
                ],
            )
            assert result.exit_code == 1
            assert "failed" in result.output.lower()

    @patch.dict(
        "snapper.strategies.factory.StrategyFactory.STRATEGY_CLASSES", {"sma_cross": MagicMock()}
    )
    def test_run_unknown_instrument_exits_1(self, cli_runner: CliRunner) -> None:
        """An unresolvable instrument symbol exits 1 before touching the engine."""
        mock_bt_repo = AsyncMock()
        mock_bt_repo.create_run = AsyncMock(return_value=(1, "run-new"))

        with (
            patch("snapper.cli.app.BootstrapSettingsLoader") as mock_boot,
            patch("snapper.cli.app.get_repository") as mock_get_repo,
            patch("snapper.cli.app.BacktestRepository", return_value=mock_bt_repo),
        ):
            mock_boot.return_value.db_url = ASYNC_MEMORY_DB_URL
            mock_repo = MagicMock()
            mock_repo.session_factory = MagicMock()
            mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=None)
            mock_get_repo.return_value = mock_repo

            result = cli_runner.invoke(
                app,
                [
                    "backtest-run",
                    "--strategy",
                    "sma_cross",
                    "--instrument",
                    "NOPE-USD",
                    "--exchange",
                    "kraken",
                    "--start",
                    "2026-01-01",
                    "--end",
                    "2026-06-01",
                ],
            )
            assert result.exit_code == 1
            assert "no active instrument resolves" in result.output.lower()
            mock_bt_repo.create_run.assert_not_called()


class TestNotifyCommand:
    """``snapper notify`` — iOS Push Foundation sidecar CLI.

    The command wires four external collaborators (bootstrap
    settings, repository, SettingsService, ApnsClientPool) and
    spawns the sidecar inside an ``asyncio.run``. We mock every
    boundary so the test exercises the CLI plumbing without touching
    a real DB, real ZMQ broker, or real APNs connection.
    """

    def test_notify_runs_until_sidecar_stops(
        self, cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The notify command wires the sidecar and runs to completion."""
        calls: list[str] = []

        class DummySidecar:
            def __init__(self, **_: Any) -> None:
                calls.append("sidecar_init")

            async def start(self) -> None:
                calls.append("start")

            async def stop(self) -> None:
                calls.append("stop")

        bootstrap = SimpleNamespace(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xsub="tcp://127.0.0.1:7500",
            zmq_broker_xpub="tcp://127.0.0.1:7501",
        )
        apns_config = SimpleNamespace(
            topic="ie.klatt.snapper",
            environment="sandbox",
        )

        monkeypatch.setattr(app_module, "NotifySidecar", DummySidecar)
        monkeypatch.setattr(app_module, "get_bootstrap_settings", lambda: bootstrap)
        monkeypatch.setattr(app_module, "get_repository", lambda _url: MagicMock())
        monkeypatch.setattr(
            app_module,
            "get_settings_service",
            AsyncMock(return_value=MagicMock()),
        )
        monkeypatch.setattr(app_module, "load_apns_config", lambda _s: apns_config)
        monkeypatch.setattr(app_module, "build_apns_client_pool", lambda _c: MagicMock())
        monkeypatch.setattr(app_module, "ValidatedSubscriber", lambda _sock: MagicMock())
        monkeypatch.setattr(app_module, "apply_hwm", lambda *a, **k: None)

        fake_ctx = MagicMock()
        fake_sock = MagicMock()
        fake_sock.close = MagicMock()
        fake_ctx.socket = MagicMock(return_value=fake_sock)
        fake_ctx.term = MagicMock()
        monkeypatch.setattr(app_module.zmq.asyncio, "Context", lambda: fake_ctx)

        result = cli_runner.invoke(app_module.app, ["notify"])

        assert result.exit_code == 0
        assert "start" in calls
        assert "stop" in calls
        assert fake_sock.close.called
        assert fake_ctx.term.called

    def test_notify_catches_keyboard_interrupt(
        self, cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """KeyboardInterrupt during start() prints a stop message and exits 0."""

        class InterruptingSidecar:
            def __init__(self, **_: Any) -> None:
                pass

            async def start(self) -> None:
                raise KeyboardInterrupt()

            async def stop(self) -> None:
                return None

        bootstrap = SimpleNamespace(
            db_url="sqlite+aiosqlite:///:memory:",
            zmq_broker_xsub="tcp://127.0.0.1:7500",
            zmq_broker_xpub="tcp://127.0.0.1:7501",
        )
        apns_config = SimpleNamespace(
            topic="ie.klatt.snapper",
            environment="sandbox",
        )

        monkeypatch.setattr(app_module, "NotifySidecar", InterruptingSidecar)
        monkeypatch.setattr(app_module, "get_bootstrap_settings", lambda: bootstrap)
        monkeypatch.setattr(app_module, "get_repository", lambda _url: MagicMock())
        monkeypatch.setattr(
            app_module,
            "get_settings_service",
            AsyncMock(return_value=MagicMock()),
        )
        monkeypatch.setattr(app_module, "load_apns_config", lambda _s: apns_config)
        monkeypatch.setattr(app_module, "build_apns_client_pool", lambda _c: MagicMock())
        monkeypatch.setattr(app_module, "ValidatedSubscriber", lambda _sock: MagicMock())
        monkeypatch.setattr(app_module, "apply_hwm", lambda *a, **k: None)

        fake_ctx = MagicMock()
        fake_sock = MagicMock()
        fake_ctx.socket = MagicMock(return_value=fake_sock)
        monkeypatch.setattr(app_module.zmq.asyncio, "Context", lambda: fake_ctx)

        result = cli_runner.invoke(app_module.app, ["notify"])

        assert result.exit_code == 0
        assert "stopped by user" in result.output


class TestEgressCommand:
    """Tests for the `snapper egress` CLI subcommand.

    The subcommand is a thin shim that forwards extra args to
    :func:`snapper.egress.__main__.main` and propagates its integer
    return code as the Typer process exit code. Tests lock the four
    invariants of the shim:

    1. Default invocation passes ``argv=[]`` (NOT ``argv=None``) so
       the egress argparse parser doesn't receive the literal
       ``"egress"`` token via ``sys.argv[1:]`` fallback.
    2. Extra flags (e.g. ``--instance-id``) are forwarded verbatim.
    3. Non-zero return propagates as the process exit code via
       ``typer.Exit(code=...)``; a bare ``return rc`` would NOT.
    4. Zero return propagates symmetrically.

    The shim patches the IMPORTED alias ``snapper.cli.app._egress_main``,
    not ``snapper.egress.__main__.main``, because the shim binds to the
    alias at import time (see ``src/snapper/cli/app.py``).
    """

    def test_egress_command_passes_empty_argv_by_default(
        self, cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — no extras → ``_egress_main`` called with ``argv=[]``.

        Given the operator runs ``snapper egress`` with no extra args,
        When the CLI dispatches,
        Then ``_egress_main`` is invoked with ``argv=[]``, NOT
            ``argv=None``. This protects against the ``sys.argv[1:]``
            fallback in ``snapper.egress.__main__.main`` that would
            otherwise receive the literal ``"egress"`` token under
            the unified ``ENTRYPOINT ["snapper"]``.
        """
        mock_egress_main = MagicMock(return_value=0)
        monkeypatch.setattr(app_module, "_egress_main", mock_egress_main)

        result = cli_runner.invoke(app_module.app, ["egress"])

        assert result.exit_code == 0
        mock_egress_main.assert_called_once_with([])

    def test_egress_command_forwards_instance_id_flag(
        self, cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — extras forwarded to ``_egress_main`` verbatim.

        Given the operator runs ``snapper egress --instance-id name``,
        When the CLI dispatches,
        Then ``_egress_main`` is invoked with
            ``argv=["--instance-id", "name"]``. Locks the
            ``allow_extra_args=True`` + ``ignore_unknown_options=True``
            context-settings invariant — without them Typer would
            consume ``--instance-id`` into its own parser and reject
            it as an unknown option.
        """
        mock_egress_main = MagicMock(return_value=0)
        monkeypatch.setattr(app_module, "_egress_main", mock_egress_main)

        result = cli_runner.invoke(app_module.app, ["egress", "--instance-id", "custom-name"])

        assert result.exit_code == 0
        mock_egress_main.assert_called_once_with(["--instance-id", "custom-name"])

    def test_egress_command_propagates_nonzero_exit(
        self, cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — non-zero return from ``_egress_main`` → CLI exit code.

        Given ``_egress_main`` returns ``2`` (invalid private key),
        When the CLI dispatches,
        Then the CliRunner result exit_code is ``2``. Locks the
            ``raise typer.Exit(code=...)`` invariant — a bare
            ``return rc`` would NOT propagate the exit code through
            Typer's invocation machinery.
        """
        mock_egress_main = MagicMock(return_value=2)
        monkeypatch.setattr(app_module, "_egress_main", mock_egress_main)

        result = cli_runner.invoke(app_module.app, ["egress"])

        assert result.exit_code == 2

    def test_egress_command_propagates_zero_exit(
        self, cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — zero return from ``_egress_main`` → CLI exit code 0.

        Symmetric to the non-zero test, locking the success path.
        """
        mock_egress_main = MagicMock(return_value=0)
        monkeypatch.setattr(app_module, "_egress_main", mock_egress_main)

        result = cli_runner.invoke(app_module.app, ["egress"])

        assert result.exit_code == 0

    def test_cli_module_import_tolerates_missing_egress_dependency(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — CLI import survives when the egress dependency is unavailable.

        Given importing ``snapper.egress.__main__`` raises ``ImportError``,
        When ``snapper.cli.app`` is imported fresh,
        Then the module still imports and stores the error for later
            reporting by the ``egress`` command path.
        """
        real_import = builtins.__import__
        original_module = sys.modules.get("snapper.cli.app")
        sys.modules.pop("snapper.cli.app", None)
        sys.modules.pop("snapper.egress.__main__", None)

        def guarded_import(
            name: str,
            globals_dict: dict[str, object] | None = None,
            locals_dict: dict[str, object] | None = None,
            fromlist: tuple[str, ...] = (),
            level: int = 0,
        ) -> object:
            if name == "snapper.egress.__main__":
                raise ImportError("No module named 'fcntl'")
            return real_import(name, globals_dict, locals_dict, fromlist, level)

        monkeypatch.setattr(builtins, "__import__", guarded_import)

        try:
            imported_module = importlib.import_module("snapper.cli.app")
        finally:
            if original_module is None:
                sys.modules.pop("snapper.cli.app", None)
            else:
                sys.modules["snapper.cli.app"] = original_module

        assert imported_module._egress_main is None
        assert isinstance(imported_module._egress_import_error, ImportError)

    def test_egress_command_reports_unavailable_dependency(
        self, cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Spec — egress command reports stored import failure cleanly.

        Given the optional egress import failed during module import,
        When the operator invokes ``snapper egress``,
        Then the CLI prints the stored import error and exits with code 1.
        """
        monkeypatch.setattr(app_module, "_egress_main", None)
        monkeypatch.setattr(
            app_module,
            "_egress_import_error",
            ImportError("No module named 'fcntl'"),
        )

        result = cli_runner.invoke(app_module.app, ["egress"])

        assert result.exit_code == 1
        assert "snapper egress is unavailable in this environment" in result.output
        assert "No module named 'fcntl'" in result.output


class TestReconcileSymbolAliasesCommand:
    """Tests for the ``reconcile-symbol-aliases`` CLI backfill.

    Pins the operator-facing surface of the alias reconcile backfill.
    The CLI is a one-shot backfill for SCD2 alias rows whose owning
    capability is already deactivated. Steady-state reconcile is
    handled by each symbol updater in its own ``run-static`` flow;
    this CLI exists for one-time historical cleanup.
    """

    def test_all_exchanges_default_reports_per_exchange_counts(
        self,
        cli_runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Spec — ``--exchange all`` invokes the helper once per registered exchange.

        Given: A monkey-patched ``SymbolUpdaterService._reconcile_aliases``
            that returns a non-zero count per exchange,
        When: ``reconcile-symbol-aliases`` is invoked with no
            ``--exchange`` flag (defaults to ``all``),
        Then: The helper is called once per entry in
            ``_RECONCILE_ALIASES_EXCHANGES``, the per-exchange line is
            printed for each, the total line is emitted, and the
            process exits 0. Verifies the iteration contract and the
            single-transaction commit pattern.
        """
        call_log: list[tuple[str, object]] = []

        def _fake_reconcile(session: object, exchange: str, _now: object) -> int:
            call_log.append((exchange, session))
            return 7

        monkeypatch.setattr(app_module.SymbolUpdaterService, "_reconcile_aliases", _fake_reconcile)
        session_holder: list[object] = []

        class _StubSession:
            def __enter__(self) -> _StubSession:
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def commit(self) -> None:
                session_holder.append(self)

        class _StubRepo:
            def __init__(self, _db_url: str) -> None:
                pass

            def get_session(self) -> _StubSession:
                return _StubSession()

        monkeypatch.setattr(app_module, "DatabaseRepository", _StubRepo)
        monkeypatch.setattr(
            app_module,
            "BootstrapSettingsLoader",
            type(
                "_StubBootstrap",
                (),
                {"__init__": lambda self: None, "db_url": "sqlite:///:memory:"},
            ),
        )
        result = cli_runner.invoke(app_module.app, ["reconcile-symbol-aliases"])
        assert result.exit_code == 0
        called_exchanges = sorted(exchange for exchange, _ in call_log)
        registered_exchanges = sorted(app_module._RECONCILE_ALIASES_EXCHANGES.keys())
        assert called_exchanges == registered_exchanges
        assert "Total: closed" in result.output
        assert f"{7 * len(registered_exchanges)} alias rows" in result.output
        assert len(session_holder) == 1

    def test_single_exchange_scopes_to_one_call(
        self,
        cli_runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Spec — ``--exchange kraken`` calls the helper exactly once.

        Given: A monkey-patched ``_reconcile_aliases`` that returns 3
            for ``kraken``,
        When: ``reconcile-symbol-aliases --exchange kraken`` runs,
        Then: The helper is invoked exactly once with
            ``ExchangeEnum.KRAKEN`` and the per-exchange + total lines
            both report 3. Verifies the per-exchange scoping contract
            so operators can target a single venue without affecting
            others.
        """
        call_log: list[str] = []

        def _fake_reconcile(_session: object, exchange: str, _now: object) -> int:
            call_log.append(exchange)
            return 3

        monkeypatch.setattr(app_module.SymbolUpdaterService, "_reconcile_aliases", _fake_reconcile)

        class _StubSession:
            def __enter__(self) -> _StubSession:
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def commit(self) -> None:
                return None

        class _StubRepo:
            def __init__(self, _db_url: str) -> None:
                pass

            def get_session(self) -> _StubSession:
                return _StubSession()

        monkeypatch.setattr(app_module, "DatabaseRepository", _StubRepo)
        monkeypatch.setattr(
            app_module,
            "BootstrapSettingsLoader",
            type(
                "_StubBootstrap",
                (),
                {"__init__": lambda self: None, "db_url": "sqlite:///:memory:"},
            ),
        )
        result = cli_runner.invoke(
            app_module.app, ["reconcile-symbol-aliases", "--exchange", "kraken"]
        )
        assert result.exit_code == 0
        assert len(call_log) == 1
        assert call_log[0] == "kraken"
        assert "kraken: closed 3 alias rows" in result.output

    def test_unknown_exchange_exits_with_error_code(
        self,
        cli_runner: CliRunner,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Spec — an unrecognised ``--exchange`` value exits with code 1.

        Given: A ``--exchange`` value that does not match any
            registered entry in ``_RECONCILE_ALIASES_EXCHANGES``,
        When: ``reconcile-symbol-aliases --exchange bogus`` runs,
        Then: The CLI exits with code 1, the helper is never invoked,
            and the error output names the rejected value plus the
            list of acceptable choices. Protects against silent no-op
            when an operator misspells an exchange name.
        """
        invocations: list[str] = []

        def _fake_reconcile(_session: object, exchange: str, _now: object) -> int:
            invocations.append(exchange)
            return 0

        monkeypatch.setattr(app_module.SymbolUpdaterService, "_reconcile_aliases", _fake_reconcile)

        class _StubRepo:
            def __init__(self, _db_url: str) -> None:
                pass

            def get_session(self) -> object:
                raise AssertionError("get_session must not be called when exchange is invalid")

        monkeypatch.setattr(app_module, "DatabaseRepository", _StubRepo)
        monkeypatch.setattr(
            app_module,
            "BootstrapSettingsLoader",
            type(
                "_StubBootstrap",
                (),
                {"__init__": lambda self: None, "db_url": "sqlite:///:memory:"},
            ),
        )
        result = cli_runner.invoke(
            app_module.app, ["reconcile-symbol-aliases", "--exchange", "bogus"]
        )
        assert result.exit_code == 1
        assert "Unknown exchange" in result.output
        assert "'bogus'" in result.output
        assert invocations == []


def test_feed_engine_starts_publishers_and_shuts_down(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Test feed-engine boots publishers and tears down on shutdown.

    Given: mocked settings service, registry discovery, launcher, and a
        no-op shutdown wait,
    When: the feed-engine command is invoked,
    Then: the launcher syncs the registry, starts publishers, and on the
        finally path stops all processes and disposes the service — in
        order.
    """
    calls: list[str] = []
    captured: dict[str, object] = {}

    class DummyService:
        async def shutdown(self) -> None:
            calls.append("shutdown")

    class DummyLauncher:
        def __init__(self, settings: Any) -> None:
            calls.append("init")

        def set_msg_publisher(self, publisher: object) -> None:
            calls.append("set_publisher")
            captured["publisher"] = publisher

        async def sync_registry_to_database(self) -> None:
            calls.append("sync")

        async def start_feed_publishers(self) -> None:
            calls.append("start")

        async def stop_all_processes(self) -> None:
            calls.append("stop")

    closed: dict[str, bool] = {"sock": False, "ctx": False}
    sockopts: list[tuple[int, int]] = []

    class FakeSocket:
        def setsockopt(self, option: int, value: int) -> None:
            sockopts.append((option, value))

        def connect(self, addr: str) -> None:
            captured["connect_addr"] = addr

        def close(self) -> None:
            calls.append("sock_close")
            closed["sock"] = True

    class FakeContext:
        def socket(self, kind: object) -> FakeSocket:
            return FakeSocket()

        def term(self) -> None:
            calls.append("ctx_term")
            closed["ctx"] = True

    class FakeZmqAsyncio:
        @staticmethod
        def Context() -> FakeContext:
            return FakeContext()

    fake_zmq = SimpleNamespace(PUB="PUB", LINGER=17, asyncio=FakeZmqAsyncio())

    async def _fake_get_service(db_url: str, xsub: str) -> DummyService:
        calls.append("get_service")
        return DummyService()

    async def _no_wait(launcher: Any) -> None:
        calls.append("wait")
        return None

    monkeypatch.setattr(publisher_module, "zmq", fake_zmq)
    monkeypatch.setattr(publisher_module, "apply_hwm", lambda sock, **kwargs: None)
    monkeypatch.setattr(publisher_module, "ValidatedPublisher", lambda sock: sock)
    monkeypatch.setattr(publisher_module, "MessagePublisher", lambda validated, tracker: validated)
    monkeypatch.setattr(app_module, "get_settings_service", _fake_get_service)
    monkeypatch.setattr(
        app_module,
        "get_settings_with_service",
        lambda svc: SimpleNamespace(zmq_broker_xsub="tcp://broker:7500"),
    )
    monkeypatch.setattr(app_module, "discover_processes", lambda: calls.append("discover"))
    monkeypatch.setattr(app_module, "ProcessLauncherService", DummyLauncher)
    monkeypatch.setattr(
        app_module,
        "ProcessCommandListener",
        lambda launcher: SimpleNamespace(start=AsyncMock(), stop=AsyncMock()),
    )
    monkeypatch.setattr(app_module, "_await_feed_shutdown_or_failure", _no_wait)
    result = cli_runner.invoke(app, ["feed-engine"])
    assert result.exit_code == 0
    assert calls == [
        "get_service",
        "discover",
        "init",
        "set_publisher",
        "sync",
        "start",
        "wait",
        "stop",
        "shutdown",
        "sock_close",
        "ctx_term",
    ]
    publisher_obj = captured["publisher"]
    assert isinstance(publisher_obj, FakeSocket)
    assert captured["connect_addr"] == "tcp://broker:7500"
    assert closed == {"sock": True, "ctx": True}
    assert (fake_zmq.LINGER, 0) in sockopts


def test_strategies_engine_starts_and_shuts_down(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """strategies-engine boots, wires the decision listener, and tears down.

    Given: mocked settings service, discovery, launcher, AI-review
        service, and a no-op shutdown wait,
    When: the strategies-engine command is invoked,
    Then: the decision-only bus listener starts BEFORE strategies and the
        finally path stops processes, the listener, the service, and the
        publisher socket — in order.
    """
    calls: list[str] = []

    class DummyService:
        async def shutdown(self) -> None:
            calls.append("shutdown")

    class DummyAiService:
        def set_msg_publisher(self, publisher: object) -> None:
            calls.append(f"ai_set_publisher:{publisher is not None}")

        async def start_bus_listener(self, xpub: str, *, topics: tuple[str, ...]) -> None:
            calls.append(f"listener_start:{','.join(topics)}")

        async def stop_bus_listener(self) -> None:
            calls.append("listener_stop")

    class DummyLauncher:
        def __init__(self, settings: Any) -> None:
            calls.append("init")

        def set_msg_publisher(self, publisher: object) -> None:
            calls.append("set_publisher")

        async def sync_registry_to_database(self) -> None:
            calls.append("sync")

        async def start_all_processes(self) -> None:
            calls.append("start_all")

        async def emit_summary_snapshot(self) -> None:
            calls.append("summary")

        async def stop_all_processes(self) -> None:
            calls.append("stop")

    class FakeSocket:
        def setsockopt(self, option: int, value: int) -> None:
            del option, value

        def connect(self, addr: str) -> None:
            del addr

        def close(self) -> None:
            calls.append("sock_close")

    class FakeContext:
        def socket(self, kind: object) -> FakeSocket:
            del kind
            return FakeSocket()

        def term(self) -> None:
            calls.append("ctx_term")

    fake_zmq = SimpleNamespace(PUB="PUB", LINGER=17, asyncio=SimpleNamespace(Context=FakeContext))

    async def _fake_get_service(db_url: str, xsub: str) -> DummyService:
        calls.append("get_service")
        return DummyService()

    async def _no_wait() -> None:
        calls.append("wait")

    monkeypatch.setattr(publisher_module, "zmq", fake_zmq)
    monkeypatch.setattr(publisher_module, "apply_hwm", lambda sock, **kwargs: None)
    monkeypatch.setattr(publisher_module, "ValidatedPublisher", lambda sock: sock)
    monkeypatch.setattr(publisher_module, "MessagePublisher", lambda validated, tracker: validated)
    monkeypatch.setattr(app_module, "get_settings_service", _fake_get_service)
    monkeypatch.setattr(
        app_module,
        "get_settings_with_service",
        lambda svc: SimpleNamespace(
            zmq_broker_xsub="tcp://broker:7500", zmq_broker_xpub="tcp://broker:7501"
        ),
    )
    monkeypatch.setattr(app_module, "discover_processes", lambda: calls.append("discover"))
    monkeypatch.setattr(app_module, "ProcessLauncherService", DummyLauncher)
    monkeypatch.setattr(
        app_module,
        "ProcessCommandListener",
        lambda launcher: SimpleNamespace(start=AsyncMock(), stop=AsyncMock()),
    )
    monkeypatch.setattr(app_module, "get_ai_review_service", lambda: DummyAiService())
    monkeypatch.setattr(app_module, "_await_shutdown_signal", _no_wait)
    result = cli_runner.invoke(app, ["strategies-engine"])
    assert result.exit_code == 0
    assert calls[:6] == [
        "get_service",
        "discover",
        "init",
        "set_publisher",
        "ai_set_publisher:True",
        "listener_start:bus.ai_review_decision",
    ]
    assert calls[6:9] == ["sync", "start_all", "wait"]
    assert calls[9:] == ["stop", "listener_stop", "shutdown", "sock_close", "ctx_term"]


def test_strategies_engine_exits_nonzero_on_core_failure(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """A CORE startup failure exits the container non-zero.

    Given: a launcher whose start_all_processes raises
        CoreProcessStartupError,
    When: the strategies-engine command is invoked,
    Then: the exit code is 1 and teardown still ran.
    """
    calls: list[str] = []

    class DummyService:
        async def shutdown(self) -> None:
            calls.append("shutdown")

    class DummyAiService:
        def set_msg_publisher(self, publisher: object) -> None:
            calls.append(f"ai_set_publisher:{publisher is not None}")

        async def start_bus_listener(self, xpub: str, *, topics: tuple[str, ...]) -> None:
            calls.append("listener_start")

        async def stop_bus_listener(self) -> None:
            calls.append("listener_stop")

    class DummyLauncher:
        def __init__(self, settings: Any) -> None:
            pass

        def set_msg_publisher(self, publisher: object) -> None:
            pass

        async def sync_registry_to_database(self) -> None:
            pass

        async def start_all_processes(self) -> None:
            raise CoreProcessStartupError(["zmq_broker"])

        async def stop_all_processes(self) -> None:
            calls.append("stop")

    async def _fake_get_service(db_url: str, xsub: str) -> DummyService:
        return DummyService()

    monkeypatch.setattr(
        publisher_module, "build_audit_publisher", lambda xsub: (None, None), raising=False
    )
    monkeypatch.setattr(app_module, "build_audit_publisher", lambda xsub: (None, None))
    monkeypatch.setattr(app_module, "get_settings_service", _fake_get_service)
    monkeypatch.setattr(
        app_module,
        "get_settings_with_service",
        lambda svc: SimpleNamespace(
            zmq_broker_xsub="tcp://broker:7500", zmq_broker_xpub="tcp://broker:7501"
        ),
    )
    monkeypatch.setattr(app_module, "discover_processes", lambda: None)
    monkeypatch.setattr(app_module, "ProcessLauncherService", DummyLauncher)
    monkeypatch.setattr(
        app_module,
        "ProcessCommandListener",
        lambda launcher: SimpleNamespace(start=AsyncMock(), stop=AsyncMock()),
    )
    monkeypatch.setattr(app_module, "get_ai_review_service", lambda: DummyAiService())
    result = cli_runner.invoke(app, ["strategies-engine"])
    assert result.exit_code == 1
    assert "stop" in calls
    assert "listener_stop" in calls
    assert "shutdown" in calls


def test_strategies_engine_degrades_without_publisher(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """A broker hiccup during publisher build degrades to no summary emission.

    Given: build_audit_publisher raising,
    When: the strategies-engine command is invoked,
    Then: the engine still boots and exits cleanly (publisher None).
    """
    captured: dict[str, object] = {}

    class DummyService:
        async def shutdown(self) -> None:
            pass

    class DummyAiService:
        def set_msg_publisher(self, publisher: object) -> None:
            captured["ai_publisher_is_none"] = publisher is None

        async def start_bus_listener(self, xpub: str, *, topics: tuple[str, ...]) -> None:
            pass

        async def stop_bus_listener(self) -> None:
            pass

    class DummyLauncher:
        def __init__(self, settings: Any) -> None:
            pass

        def set_msg_publisher(self, publisher: object) -> None:
            captured["publisher"] = publisher

        async def sync_registry_to_database(self) -> None:
            pass

        async def start_all_processes(self) -> None:
            pass

        async def stop_all_processes(self) -> None:
            pass

    async def _fake_get_service(db_url: str, xsub: str) -> DummyService:
        return DummyService()

    async def _no_wait() -> None:
        pass

    def _boom(xsub: str) -> tuple[None, None]:
        raise RuntimeError("broker away")

    monkeypatch.setattr(app_module, "build_audit_publisher", _boom)
    monkeypatch.setattr(app_module, "get_settings_service", _fake_get_service)
    monkeypatch.setattr(
        app_module,
        "get_settings_with_service",
        lambda svc: SimpleNamespace(
            zmq_broker_xsub="tcp://broker:7500", zmq_broker_xpub="tcp://broker:7501"
        ),
    )
    monkeypatch.setattr(app_module, "discover_processes", lambda: None)
    monkeypatch.setattr(app_module, "ProcessLauncherService", DummyLauncher)
    monkeypatch.setattr(
        app_module,
        "ProcessCommandListener",
        lambda launcher: SimpleNamespace(start=AsyncMock(), stop=AsyncMock()),
    )
    monkeypatch.setattr(app_module, "get_ai_review_service", lambda: DummyAiService())
    monkeypatch.setattr(app_module, "_await_shutdown_signal", _no_wait)
    result = cli_runner.invoke(app, ["strategies-engine"])
    assert result.exit_code == 0
    assert captured["publisher"] is None


def test_strategies_engine_skips_listener_without_xpub(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """An empty XPUB endpoint skips the decision listener entirely.

    Given: settings without a broker XPUB endpoint,
    When: the strategies-engine command is invoked,
    Then: no listener starts or stops and the engine exits cleanly.
    """
    listener_calls: list[str] = []

    class DummyService:
        async def shutdown(self) -> None:
            pass

    class DummyAiService:
        def set_msg_publisher(self, publisher: object) -> None:
            listener_calls.append(f"ai_set_publisher:{publisher is not None}")

        async def start_bus_listener(self, xpub: str, *, topics: tuple[str, ...]) -> None:
            listener_calls.append("start")

        async def stop_bus_listener(self) -> None:
            listener_calls.append("stop")

    class DummyLauncher:
        def __init__(self, settings: Any) -> None:
            pass

        def set_msg_publisher(self, publisher: object) -> None:
            pass

        async def sync_registry_to_database(self) -> None:
            pass

        async def start_all_processes(self) -> None:
            pass

        async def stop_all_processes(self) -> None:
            pass

    async def _fake_get_service(db_url: str, xsub: str) -> DummyService:
        return DummyService()

    async def _no_wait() -> None:
        pass

    monkeypatch.setattr(app_module, "build_audit_publisher", lambda xsub: (None, None))
    monkeypatch.setattr(app_module, "get_settings_service", _fake_get_service)
    monkeypatch.setattr(
        app_module,
        "get_settings_with_service",
        lambda svc: SimpleNamespace(zmq_broker_xsub="tcp://broker:7500", zmq_broker_xpub=""),
    )
    monkeypatch.setattr(app_module, "discover_processes", lambda: None)
    monkeypatch.setattr(app_module, "ProcessLauncherService", DummyLauncher)
    monkeypatch.setattr(
        app_module,
        "ProcessCommandListener",
        lambda launcher: SimpleNamespace(start=AsyncMock(), stop=AsyncMock()),
    )
    monkeypatch.setattr(app_module, "get_ai_review_service", lambda: DummyAiService())
    monkeypatch.setattr(app_module, "_await_shutdown_signal", _no_wait)
    result = cli_runner.invoke(app, ["strategies-engine"])
    assert result.exit_code == 0
    assert listener_calls == ["ai_set_publisher:False"]


def test_delegate_engine_starts_and_shuts_down(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """delegate-engine boots its management services and tears them down.

    Given: Delegate-profile settings, a mocked launcher, and an in-memory publisher,
    When: The delegate-engine command runs through one scheduling turn,
    Then: Processes, summaries, reconciliation, and the command listener run before cleanup.
    """
    calls: list[str] = []

    class DummyService:
        async def shutdown(self) -> None:
            calls.append("shutdown")

    class DummyLauncher:
        def __init__(self, settings: object) -> None:
            del settings
            calls.append("init")

        def set_msg_publisher(self, publisher: object) -> None:
            calls.append(f"set_publisher:{publisher is not None}")

        async def sync_registry_to_database(self) -> None:
            calls.append("sync")

        async def start_all_processes(self) -> None:
            calls.append("start_all")

        async def emit_summary_snapshot(self) -> None:
            calls.append("summary")

        async def reconcile_desired_state(self) -> None:
            calls.append("reconcile")

        async def stop_all_processes(self) -> None:
            calls.append("stop")

    class DummyCommandListener:
        def __init__(self, launcher: object) -> None:
            del launcher
            calls.append("listener_init")

        async def start(self, endpoint: str) -> None:
            calls.append(f"listener_start:{endpoint}")

        async def stop(self) -> None:
            calls.append("listener_stop")

    class FakeSocket:
        def setsockopt(self, option: int, value: int) -> None:
            del option, value

        def connect(self, addr: str) -> None:
            del addr

        def close(self) -> None:
            calls.append("sock_close")

    class FakeContext:
        def socket(self, kind: object) -> FakeSocket:
            del kind
            return FakeSocket()

        def term(self) -> None:
            calls.append("ctx_term")

    fake_zmq = SimpleNamespace(PUB="PUB", LINGER=17, asyncio=SimpleNamespace(Context=FakeContext))

    def _fake_get_settings() -> SimpleNamespace:
        return SimpleNamespace(
            db_url=ASYNC_MEMORY_DB_URL,
            zmq_broker_xsub="tcp://broker:7500",
            process_autostart_profile=ProcessAutostartProfileEnum.DELEGATE,
        )

    async def _fake_get_service(db_url: str, xsub: str) -> DummyService:
        del db_url, xsub
        calls.append("get_service")
        return DummyService()

    def _fake_get_app_settings(service: DummyService) -> SimpleNamespace:
        del service
        return SimpleNamespace(
            zmq_broker_xsub="tcp://broker:7500",
            zmq_broker_xpub="tcp://broker:7501",
            process_autostart_profile=ProcessAutostartProfileEnum.DELEGATE,
        )

    def _apply_hwm(socket: FakeSocket, *, sndhwm: int) -> None:
        del socket, sndhwm

    def _validated_publisher(socket: FakeSocket) -> FakeSocket:
        return socket

    def _message_publisher(socket: FakeSocket, tracker: object) -> FakeSocket:
        del tracker
        return socket

    def _discover_processes() -> None:
        calls.append("discover")

    async def _no_wait() -> None:
        calls.append("wait")
        await asyncio.sleep(0)

    monkeypatch.setattr(publisher_module, "zmq", fake_zmq)
    monkeypatch.setattr(publisher_module, "apply_hwm", _apply_hwm)
    monkeypatch.setattr(publisher_module, "ValidatedPublisher", _validated_publisher)
    monkeypatch.setattr(publisher_module, "MessagePublisher", _message_publisher)
    monkeypatch.setattr(app_module, "get_settings", _fake_get_settings)
    monkeypatch.setattr(app_module, "get_settings_service", _fake_get_service)
    monkeypatch.setattr(app_module, "get_settings_with_service", _fake_get_app_settings)
    monkeypatch.setattr(app_module, "discover_processes", _discover_processes)
    monkeypatch.setattr(app_module, "ProcessLauncherService", DummyLauncher)
    monkeypatch.setattr(app_module, "ProcessCommandListener", DummyCommandListener)
    monkeypatch.setattr(app_module, "_await_shutdown_signal", _no_wait)
    result = cli_runner.invoke(app, ["delegate-engine"])
    assert result.exit_code == 0
    assert calls == [
        "get_service",
        "discover",
        "init",
        "set_publisher:True",
        "sync",
        "start_all",
        "listener_init",
        "listener_start:tcp://broker:7501",
        "wait",
        "summary",
        "reconcile",
        "listener_stop",
        "stop",
        "shutdown",
        "sock_close",
        "ctx_term",
    ]


def test_delegate_engine_exits_nonzero_on_core_failure(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """A delegate CORE startup failure exits after unconditional teardown.

    Given: A delegate-profile launcher whose core process cannot start,
    When: The delegate-engine command invokes the launcher,
    Then: It exits with code one without constructing scheduled management services.
    """
    calls: list[str] = []

    class DummyService:
        async def shutdown(self) -> None:
            calls.append("shutdown")

    class DummyLauncher:
        def __init__(self, settings: object) -> None:
            del settings

        def set_msg_publisher(self, publisher: object) -> None:
            del publisher

        async def sync_registry_to_database(self) -> None:
            calls.append("sync")

        async def start_all_processes(self) -> None:
            raise CoreProcessStartupError(["delegate_runner"])

        async def stop_all_processes(self) -> None:
            calls.append("stop")

    class DummyCommandListener:
        def __init__(self, launcher: object) -> None:
            del launcher
            calls.append("listener_init")

    def _fake_get_settings() -> SimpleNamespace:
        return SimpleNamespace(
            db_url=ASYNC_MEMORY_DB_URL,
            zmq_broker_xsub="tcp://broker:7500",
            process_autostart_profile=ProcessAutostartProfileEnum.DELEGATE,
        )

    async def _fake_get_service(db_url: str, xsub: str) -> DummyService:
        del db_url, xsub
        return DummyService()

    def _fake_get_app_settings(service: DummyService) -> SimpleNamespace:
        del service
        return SimpleNamespace(
            zmq_broker_xsub="tcp://broker:7500",
            zmq_broker_xpub="tcp://broker:7501",
            process_autostart_profile=ProcessAutostartProfileEnum.DELEGATE,
        )

    def _build_publisher(xsub: str) -> tuple[None, None]:
        del xsub
        return None, None

    def _discover_processes() -> None:
        calls.append("discover")

    monkeypatch.setattr(app_module, "get_settings", _fake_get_settings)
    monkeypatch.setattr(app_module, "build_audit_publisher", _build_publisher)
    monkeypatch.setattr(app_module, "get_settings_service", _fake_get_service)
    monkeypatch.setattr(app_module, "get_settings_with_service", _fake_get_app_settings)
    monkeypatch.setattr(app_module, "discover_processes", _discover_processes)
    monkeypatch.setattr(app_module, "ProcessLauncherService", DummyLauncher)
    monkeypatch.setattr(app_module, "ProcessCommandListener", DummyCommandListener)
    result = cli_runner.invoke(app, ["delegate-engine"])
    assert result.exit_code == 1
    assert "Delegate engine startup failed" in result.output
    assert calls == ["discover", "sync", "stop", "shutdown"]


def test_delegate_engine_degrades_without_publisher(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """A delegate publisher failure leaves process management operational.

    Given: A delegate profile whose summary publisher cannot be constructed,
    When: The delegate-engine command starts and stops normally,
    Then: The launcher receives no publisher and both cleanup paths remain safe.
    """
    captured: dict[str, object] = {}
    publisher_shutdowns: list[tuple[object | None, object | None]] = []

    class DummyService:
        async def shutdown(self) -> None:
            captured["settings_shutdown"] = True

    class DummyLauncher:
        def __init__(self, settings: object) -> None:
            del settings

        def set_msg_publisher(self, publisher: object) -> None:
            captured["publisher"] = publisher

        async def sync_registry_to_database(self) -> None:
            return None

        async def start_all_processes(self) -> None:
            return None

        async def emit_summary_snapshot(self) -> None:
            return None

        async def reconcile_desired_state(self) -> None:
            return None

        async def stop_all_processes(self) -> None:
            captured["processes_stopped"] = True

    class DummyCommandListener:
        def __init__(self, launcher: object) -> None:
            del launcher

        async def start(self, endpoint: str) -> None:
            del endpoint

        async def stop(self) -> None:
            captured["listener_stopped"] = True

    def _fake_get_settings() -> SimpleNamespace:
        return SimpleNamespace(
            db_url=ASYNC_MEMORY_DB_URL,
            zmq_broker_xsub="tcp://broker:7500",
            process_autostart_profile=ProcessAutostartProfileEnum.DELEGATE,
        )

    async def _fake_get_service(db_url: str, xsub: str) -> DummyService:
        del db_url, xsub
        return DummyService()

    def _fake_get_app_settings(service: DummyService) -> SimpleNamespace:
        del service
        return SimpleNamespace(
            zmq_broker_xsub="tcp://broker:7500",
            zmq_broker_xpub="tcp://broker:7501",
            process_autostart_profile=ProcessAutostartProfileEnum.DELEGATE,
        )

    def _build_publisher(xsub: str) -> tuple[None, None]:
        del xsub
        raise RuntimeError("broker away")

    def _shutdown_publisher(publisher: object | None, context: object | None) -> None:
        publisher_shutdowns.append((publisher, context))

    async def _no_wait() -> None:
        return None

    monkeypatch.setattr(app_module, "get_settings", _fake_get_settings)
    monkeypatch.setattr(app_module, "build_audit_publisher", _build_publisher)
    monkeypatch.setattr(app_module, "shutdown_audit_publisher", _shutdown_publisher)
    monkeypatch.setattr(app_module, "get_settings_service", _fake_get_service)
    monkeypatch.setattr(app_module, "get_settings_with_service", _fake_get_app_settings)
    monkeypatch.setattr(app_module, "discover_processes", lambda: None)
    monkeypatch.setattr(app_module, "ProcessLauncherService", DummyLauncher)
    monkeypatch.setattr(app_module, "ProcessCommandListener", DummyCommandListener)
    monkeypatch.setattr(app_module, "_await_shutdown_signal", _no_wait)
    result = cli_runner.invoke(app, ["delegate-engine"])
    assert result.exit_code == 0
    assert "summary publisher unavailable" in result.output
    assert captured == {
        "publisher": None,
        "listener_stopped": True,
        "processes_stopped": True,
        "settings_shutdown": True,
    }
    assert publisher_shutdowns == [(None, None), (None, None)]


def test_delegate_engine_rejects_non_delegate_profile(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """The delegate command fails closed before creating services on profile mismatch.

    Given: Raw settings assigning the delegate command to the API profile,
    When: The delegate-engine command validates coordinator ownership,
    Then: It exits with code one without constructing the settings service.
    """
    service_calls: list[tuple[str, str]] = []

    class DummyService:
        async def shutdown(self) -> None:
            return None

    def _fake_get_settings() -> SimpleNamespace:
        return SimpleNamespace(
            db_url=ASYNC_MEMORY_DB_URL,
            zmq_broker_xsub="tcp://broker:7500",
            process_autostart_profile=ProcessAutostartProfileEnum.API,
        )

    async def _fake_get_service(db_url: str, xsub: str) -> DummyService:
        service_calls.append((db_url, xsub))
        return DummyService()

    monkeypatch.setattr(app_module, "get_settings", _fake_get_settings)
    monkeypatch.setattr(app_module, "get_settings_service", _fake_get_service)
    result = cli_runner.invoke(app, ["delegate-engine"])
    assert result.exit_code == 1
    assert "requires PROCESS_AUTOSTART_PROFILE=delegate" in result.output
    assert service_calls == []


@pytest.mark.asyncio
async def test_strategies_summary_loop_ticks_and_survives_errors() -> None:
    """The summary loop emits every tick and swallows emission errors.

    Given: a launcher whose snapshot raises once then succeeds,
    When: the loop runs two ticks (patched sleep),
    Then: both attempts happened and the error never escaped.
    """
    attempts: list[int] = []

    class DummyLauncher:
        async def emit_summary_snapshot(self) -> None:
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeError("publisher blip")

    sleeps: list[float] = []

    async def _fast_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        if len(sleeps) >= 2:
            raise asyncio.CancelledError

    with (
        patch.object(app_module.asyncio, "sleep", _fast_sleep),
        pytest.raises(asyncio.CancelledError),
    ):
        await app_module._strategies_summary_loop(cast(Any, DummyLauncher()))
    assert len(attempts) == 2
    assert sleeps == [5.0, 5.0]


@pytest.mark.asyncio
async def test_reconcile_loop_ticks_and_survives_errors() -> None:
    """The reconcile loop converges every tick and swallows a pass error.

    Given: a launcher whose reconcile raises once then succeeds,
    When: the loop runs two ticks (patched sleep),
    Then: both attempts happened, the error never escaped, and it sleeps the
        reconcile interval between ticks.
    """
    attempts: list[int] = []

    class DummyLauncher:
        async def reconcile_desired_state(self) -> None:
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeError("reconcile blip")

    sleeps: list[float] = []

    async def _fast_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        if len(sleeps) >= 2:
            raise asyncio.CancelledError

    with (
        patch.object(app_module.asyncio, "sleep", _fast_sleep),
        pytest.raises(asyncio.CancelledError),
    ):
        await app_module._reconcile_loop(cast(Any, DummyLauncher()))
    assert len(attempts) == 2
    assert sleeps == [10.0, 10.0]


def test_feed_engine_spawns_reconcile_loop_when_profile_feed(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """With PROCESS_AUTOSTART_PROFILE=feed the reconcile loop is spawned and torn down.

    Given: a feed profile and a launcher that boots and shuts down cleanly,
    When: the feed-engine command runs,
    Then: the reconcile loop is spawned (not the profile-mismatch disable
        path) and the finally cancels it, exiting cleanly.
    """

    class DummyService:
        async def shutdown(self) -> None:
            return None

    class DummyLauncher:
        def __init__(self, settings: Any) -> None:
            return None

        def set_msg_publisher(self, publisher: object) -> None:
            return None

        async def sync_registry_to_database(self) -> None:
            return None

        async def start_feed_publishers(self) -> None:
            return None

        async def stop_all_processes(self) -> None:
            return None

        async def reconcile_desired_state(self) -> None:
            return None

    async def _fake_get_service(db_url: str, xsub: str) -> DummyService:
        return DummyService()

    async def _clean_shutdown(launcher: Any) -> str | None:
        return None

    monkeypatch.setattr(app_module, "get_settings_service", _fake_get_service)
    monkeypatch.setattr(
        app_module,
        "get_settings_with_service",
        lambda svc: SimpleNamespace(
            zmq_broker_xsub="tcp://broker:7500",
            process_autostart_profile=ProcessAutostartProfileEnum.FEED,
        ),
    )
    monkeypatch.setattr(app_module, "discover_processes", lambda: None)
    monkeypatch.setattr(app_module, "ProcessLauncherService", DummyLauncher)
    monkeypatch.setattr(
        app_module,
        "ProcessCommandListener",
        lambda launcher: SimpleNamespace(start=AsyncMock(), stop=AsyncMock()),
    )
    monkeypatch.setattr(app_module, "_await_feed_shutdown_or_failure", _clean_shutdown)
    result = cli_runner.invoke(app, ["feed-engine"])
    assert result.exit_code == 0
    assert "reconcile loop disabled" not in result.output


def test_strategies_engine_spawns_reconcile_loop_when_profile_strategy(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """With PROCESS_AUTOSTART_PROFILE=strategy the reconcile loop is spawned and torn down.

    Given: a strategy profile and a launcher that boots and shuts down cleanly,
    When: the strategies-engine command runs,
    Then: the reconcile loop is spawned (not the profile-mismatch disable
        path) and the finally cancels it, exiting cleanly.
    """

    class DummyService:
        async def shutdown(self) -> None:
            return None

    class DummyAiService:
        def set_msg_publisher(self, publisher: object) -> None:
            return None

        async def start_bus_listener(self, xpub: str, *, topics: tuple[str, ...]) -> None:
            return None

        async def stop_bus_listener(self) -> None:
            return None

    class DummyLauncher:
        def __init__(self, settings: Any) -> None:
            return None

        def set_msg_publisher(self, publisher: object) -> None:
            return None

        async def sync_registry_to_database(self) -> None:
            return None

        async def start_all_processes(self) -> None:
            return None

        async def emit_summary_snapshot(self) -> None:
            return None

        async def reconcile_desired_state(self) -> None:
            return None

        async def stop_all_processes(self) -> None:
            return None

    async def _fake_get_service(db_url: str, xsub: str) -> DummyService:
        return DummyService()

    async def _no_wait() -> None:
        return None

    monkeypatch.setattr(app_module, "get_settings_service", _fake_get_service)
    monkeypatch.setattr(
        app_module,
        "get_settings_with_service",
        lambda svc: SimpleNamespace(
            zmq_broker_xsub="tcp://broker:7500",
            zmq_broker_xpub="",
            process_autostart_profile=ProcessAutostartProfileEnum.STRATEGY,
        ),
    )
    monkeypatch.setattr(app_module, "discover_processes", lambda: None)
    monkeypatch.setattr(app_module, "ProcessLauncherService", DummyLauncher)
    monkeypatch.setattr(
        app_module,
        "ProcessCommandListener",
        lambda launcher: SimpleNamespace(start=AsyncMock(), stop=AsyncMock()),
    )
    monkeypatch.setattr(app_module, "get_ai_review_service", lambda: DummyAiService())
    monkeypatch.setattr(app_module, "_await_shutdown_signal", _no_wait)
    result = cli_runner.invoke(app, ["strategies-engine"])
    assert result.exit_code == 0
    assert "reconcile loop disabled" not in result.output


def test_feed_engine_disables_reconcile_loop_when_profile_not_feed(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """§8 driver-not-on-API: a non-FEED profile installs NO reconcile driver.

    Given: a non-feed (API) autostart profile on the feed-engine node,
    When: the feed-engine command runs,
    Then: the else branch echoes 'reconcile loop disabled' and constructs NO
        ProcessCommandListener, so no stray reconcile driver runs where the node
        does not own reconcile.
    """

    class DummyService:
        async def shutdown(self) -> None:
            return None

    class DummyLauncher:
        def __init__(self, settings: Any) -> None:
            return None

        def set_msg_publisher(self, publisher: object) -> None:
            return None

        async def sync_registry_to_database(self) -> None:
            return None

        async def start_feed_publishers(self) -> None:
            return None

        async def stop_all_processes(self) -> None:
            return None

        async def reconcile_desired_state(self) -> None:
            return None

    async def _fake_get_service(db_url: str, xsub: str) -> DummyService:
        return DummyService()

    async def _clean_shutdown(launcher: Any) -> str | None:
        return None

    constructed: list[Any] = []

    def _spy_listener(launcher: Any) -> SimpleNamespace:
        constructed.append(launcher)
        return SimpleNamespace(start=AsyncMock(), stop=AsyncMock())

    monkeypatch.setattr(app_module, "get_settings_service", _fake_get_service)
    monkeypatch.setattr(
        app_module,
        "get_settings_with_service",
        lambda svc: SimpleNamespace(
            zmq_broker_xsub="tcp://broker:7500",
            process_autostart_profile=ProcessAutostartProfileEnum.API,
        ),
    )
    monkeypatch.setattr(app_module, "discover_processes", lambda: None)
    monkeypatch.setattr(app_module, "ProcessLauncherService", DummyLauncher)
    monkeypatch.setattr(app_module, "ProcessCommandListener", _spy_listener)
    monkeypatch.setattr(app_module, "_await_feed_shutdown_or_failure", _clean_shutdown)
    result = cli_runner.invoke(app, ["feed-engine"])
    assert result.exit_code == 0
    assert "reconcile loop disabled" in result.output
    assert constructed == []


def test_strategies_engine_disables_reconcile_loop_when_profile_not_strategy(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """§8 driver-not-on-API: a non-STRATEGY profile installs NO reconcile driver.

    Given: a non-strategy (API) autostart profile on the strategies-engine node,
    When: the strategies-engine command runs,
    Then: the else branch echoes 'reconcile loop disabled' and constructs NO
        ProcessCommandListener.
    """

    class DummyService:
        async def shutdown(self) -> None:
            return None

    class DummyAiService:
        def set_msg_publisher(self, publisher: object) -> None:
            return None

        async def start_bus_listener(self, xpub: str, *, topics: tuple[str, ...]) -> None:
            return None

        async def stop_bus_listener(self) -> None:
            return None

    class DummyLauncher:
        def __init__(self, settings: Any) -> None:
            return None

        def set_msg_publisher(self, publisher: object) -> None:
            return None

        async def sync_registry_to_database(self) -> None:
            return None

        async def start_all_processes(self) -> None:
            return None

        async def emit_summary_snapshot(self) -> None:
            return None

        async def reconcile_desired_state(self) -> None:
            return None

        async def stop_all_processes(self) -> None:
            return None

    async def _fake_get_service(db_url: str, xsub: str) -> DummyService:
        return DummyService()

    async def _no_wait() -> None:
        return None

    constructed: list[Any] = []

    def _spy_listener(launcher: Any) -> SimpleNamespace:
        constructed.append(launcher)
        return SimpleNamespace(start=AsyncMock(), stop=AsyncMock())

    monkeypatch.setattr(app_module, "get_settings_service", _fake_get_service)
    monkeypatch.setattr(
        app_module,
        "get_settings_with_service",
        lambda svc: SimpleNamespace(
            zmq_broker_xsub="tcp://broker:7500",
            zmq_broker_xpub="",
            process_autostart_profile=ProcessAutostartProfileEnum.API,
        ),
    )
    monkeypatch.setattr(app_module, "discover_processes", lambda: None)
    monkeypatch.setattr(app_module, "ProcessLauncherService", DummyLauncher)
    monkeypatch.setattr(app_module, "ProcessCommandListener", _spy_listener)
    monkeypatch.setattr(app_module, "get_ai_review_service", lambda: DummyAiService())
    monkeypatch.setattr(app_module, "_await_shutdown_signal", _no_wait)
    result = cli_runner.invoke(app, ["strategies-engine"])
    assert result.exit_code == 0
    assert "reconcile loop disabled" in result.output
    assert constructed == []


def test_feed_engine_exits_nonzero_on_publisher_crash(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """feed-engine exits non-zero when a supervised publisher crashes.

    Given: the shutdown/failure wait resolves with a crashed publisher
        name (rather than a clean shutdown signal),
    When: the feed-engine command is invoked,
    Then: the finally path still tears down, and the command exits with
        code 1 so the orchestrator restarts the whole container.
    """

    class DummyService:
        async def shutdown(self) -> None:
            return None

    class DummyLauncher:
        def __init__(self, settings: Any) -> None:
            return None

        def set_msg_publisher(self, publisher: object) -> None:
            return None

        async def sync_registry_to_database(self) -> None:
            return None

        async def start_feed_publishers(self) -> None:
            return None

        async def stop_all_processes(self) -> None:
            return None

    async def _fake_get_service(db_url: str, xsub: str) -> DummyService:
        return DummyService()

    async def _crash(launcher: Any) -> str:
        return "kraken_equities_feed_publisher"

    monkeypatch.setattr(app_module, "get_settings_service", _fake_get_service)
    monkeypatch.setattr(
        app_module,
        "get_settings_with_service",
        lambda svc: SimpleNamespace(zmq_broker_xsub="tcp://broker:7500"),
    )
    monkeypatch.setattr(app_module, "discover_processes", lambda: None)
    monkeypatch.setattr(app_module, "ProcessLauncherService", DummyLauncher)
    monkeypatch.setattr(
        app_module,
        "ProcessCommandListener",
        lambda launcher: SimpleNamespace(start=AsyncMock(), stop=AsyncMock()),
    )
    monkeypatch.setattr(app_module, "_await_feed_shutdown_or_failure", _crash)
    result = cli_runner.invoke(app, ["feed-engine"])
    assert result.exit_code == 1
    assert "kraken_equities_feed_publisher" in result.output


def test_feed_engine_degrades_when_publisher_build_fails(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """A broker hiccup during publisher build degrades to no-metrics, not a crash.

    Given: ZMQ socket creation succeeds but ``connect`` raises (a
        momentarily-unavailable broker / bad endpoint),
    When: the feed-engine command runs,
    Then: the partially-built socket is closed and the context terminated,
        the launcher is wired with ``None`` (metrics emission disabled),
        feed startup still proceeds, and the command exits cleanly without
        crash-looping the container.
    """
    calls: list[str] = []
    captured: dict[str, object] = {}
    closed: dict[str, bool] = {"sock": False, "ctx": False}
    sockopts: list[tuple[int, int]] = []

    class DummyService:
        async def shutdown(self) -> None:
            calls.append("shutdown")

    class DummyLauncher:
        def __init__(self, settings: Any) -> None:
            calls.append("init")

        def set_msg_publisher(self, publisher: object) -> None:
            captured["publisher"] = publisher

        async def sync_registry_to_database(self) -> None:
            calls.append("sync")

        async def start_feed_publishers(self) -> None:
            calls.append("start")

        async def stop_all_processes(self) -> None:
            calls.append("stop")

    class FakeSocket:
        def setsockopt(self, option: int, value: int) -> None:
            sockopts.append((option, value))

        def connect(self, addr: str) -> None:
            raise RuntimeError("broker down")

        def close(self) -> None:
            closed["sock"] = True

    class FakeContext:
        def socket(self, kind: object) -> FakeSocket:
            return FakeSocket()

        def term(self) -> None:
            closed["ctx"] = True

    class FakeZmqAsyncio:
        @staticmethod
        def Context() -> FakeContext:
            return FakeContext()

    fake_zmq = SimpleNamespace(PUB="PUB", LINGER=17, asyncio=FakeZmqAsyncio())

    async def _fake_get_service(db_url: str, xsub: str) -> DummyService:
        return DummyService()

    async def _no_wait(launcher: Any) -> None:
        return None

    monkeypatch.setattr(publisher_module, "zmq", fake_zmq)
    monkeypatch.setattr(publisher_module, "apply_hwm", lambda sock, **kwargs: None)
    monkeypatch.setattr(app_module, "get_settings_service", _fake_get_service)
    monkeypatch.setattr(
        app_module,
        "get_settings_with_service",
        lambda svc: SimpleNamespace(zmq_broker_xsub="tcp://broker:7500"),
    )
    monkeypatch.setattr(app_module, "discover_processes", lambda: None)
    monkeypatch.setattr(app_module, "ProcessLauncherService", DummyLauncher)
    monkeypatch.setattr(
        app_module,
        "ProcessCommandListener",
        lambda launcher: SimpleNamespace(start=AsyncMock(), stop=AsyncMock()),
    )
    monkeypatch.setattr(app_module, "_await_feed_shutdown_or_failure", _no_wait)
    result = cli_runner.invoke(app, ["feed-engine"])
    assert result.exit_code == 0
    assert "summary publisher unavailable" in result.output
    assert captured["publisher"] is None
    assert closed == {"sock": True, "ctx": True}
    assert (fake_zmq.LINGER, 0) in sockopts
    assert calls == ["init", "sync", "start", "stop", "shutdown"]


def test_feed_engine_degrades_when_context_creation_fails(
    monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
) -> None:
    """Context creation failure degrades cleanly with nothing to close.

    Given: ``zmq.asyncio.Context()`` itself raises before any socket
        exists,
    When: the feed-engine command runs,
    Then: the except path skips both the socket-close and context-term
        guards (neither was created), wires the launcher with ``None``,
        and feed startup still proceeds to a clean exit.
    """
    captured: dict[str, object] = {}

    class DummyService:
        async def shutdown(self) -> None:
            return None

    class DummyLauncher:
        def __init__(self, settings: Any) -> None:
            return None

        def set_msg_publisher(self, publisher: object) -> None:
            captured["publisher"] = publisher

        async def sync_registry_to_database(self) -> None:
            return None

        async def start_feed_publishers(self) -> None:
            return None

        async def stop_all_processes(self) -> None:
            return None

    class FailingZmqAsyncio:
        @staticmethod
        def Context() -> object:
            raise RuntimeError("no zmq context")

    fake_zmq = SimpleNamespace(PUB="PUB", LINGER=17, asyncio=FailingZmqAsyncio())

    async def _fake_get_service(db_url: str, xsub: str) -> DummyService:
        return DummyService()

    async def _no_wait(launcher: Any) -> None:
        return None

    monkeypatch.setattr(publisher_module, "zmq", fake_zmq)
    monkeypatch.setattr(app_module, "get_settings_service", _fake_get_service)
    monkeypatch.setattr(
        app_module,
        "get_settings_with_service",
        lambda svc: SimpleNamespace(zmq_broker_xsub="tcp://broker:7500"),
    )
    monkeypatch.setattr(app_module, "discover_processes", lambda: None)
    monkeypatch.setattr(app_module, "ProcessLauncherService", DummyLauncher)
    monkeypatch.setattr(
        app_module,
        "ProcessCommandListener",
        lambda launcher: SimpleNamespace(start=AsyncMock(), stop=AsyncMock()),
    )
    monkeypatch.setattr(app_module, "_await_feed_shutdown_or_failure", _no_wait)
    result = cli_runner.invoke(app, ["feed-engine"])
    assert result.exit_code == 0
    assert "summary publisher unavailable" in result.output
    assert captured["publisher"] is None


def test_await_feed_shutdown_or_failure_returns_crash_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The combinator returns the crashed publisher name when failure wins.

    Given: the shutdown signal never fires but a supervised publisher
        failure resolves,
    When: _await_feed_shutdown_or_failure is awaited,
    Then: it returns the crashed publisher's name and leaves no pending
        task dangling.
    """

    async def _never() -> None:
        await asyncio.Event().wait()

    class DummyLauncher:
        async def wait_for_feed_publisher_failure(self) -> str:
            return "kraken_equities_feed_publisher"

    async def _drive() -> str | None:
        monkeypatch.setattr(app_module, "_await_shutdown_signal", _never)
        return await app_module._await_feed_shutdown_or_failure(cast(Any, DummyLauncher()))

    result = asyncio.run(_drive())
    assert result == "kraken_equities_feed_publisher"


def test_await_feed_shutdown_or_failure_returns_none_on_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The combinator returns None when the shutdown signal wins.

    Given: the shutdown signal resolves immediately while the publisher
        failure never fires,
    When: _await_feed_shutdown_or_failure is awaited,
    Then: it returns None (clean teardown) and cancels the pending
        failure waiter.
    """

    async def _immediate() -> None:
        return None

    class DummyLauncher:
        async def wait_for_feed_publisher_failure(self) -> str:
            await asyncio.Event().wait()
            return "never"

    async def _drive() -> str | None:
        monkeypatch.setattr(app_module, "_await_shutdown_signal", _immediate)
        return await app_module._await_feed_shutdown_or_failure(cast(Any, DummyLauncher()))

    result = asyncio.run(_drive())
    assert result is None


def test_await_shutdown_signal_returns_when_signalled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_await_shutdown_signal`` returns once a registered signal fires.

    Given: the running loop's ``add_signal_handler`` is patched to invoke
        its callback immediately (simulating SIGINT/SIGTERM delivery),
    When: ``_await_shutdown_signal`` is awaited,
    Then: it completes without blocking.
    """

    async def _drive() -> None:
        loop = asyncio.get_running_loop()

        def _immediate(sig: int, callback: Callable[[], None]) -> None:
            callback()

        monkeypatch.setattr(loop, "add_signal_handler", _immediate)
        await app_module._await_shutdown_signal()

    asyncio.run(_drive())


class TestPolygonRepairSplitsCommand:
    """CLI surface of the split-repair orchestrator."""

    @staticmethod
    def _summary_with_break(unverified: bool, undetectable: bool = False) -> object:
        """Build a run summary carrying one confirmed candidate."""
        candidate = SplitRepairCandidate(
            event=PolygonSplitEvent(
                ticker="NFLX",
                execution_date=date_type_local(2025, 11, 17),
                split_from=1.0,
                split_to=10.0,
            ),
            native_symbol="NFLX",
            instrument_public_id="i-1",
            symbol_public_id="s-1",
            break_day=date_type_local(2023, 12, 18),
        )
        summary = SplitRepairSummary(splits_seen=3)
        summary.candidates.append(candidate)
        if undetectable:
            summary.undetectable.append("HON")
        if unverified:
            summary.unverified.append("NFLX")
        else:
            summary.repaired.append("NFLX")
        return summary

    def test_success_reports_summary(
        self, monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
    ) -> None:
        """A verified repair prints the summary and exits cleanly.

        Given: A patched repair run returning one repaired symbol,
        When: polygon-repair-splits is invoked,
        Then: The summary lines and the break detail are echoed with
            exit code 0.
        """
        captured: dict[str, object] = {}

        async def fake_run(**kwargs: object) -> object:
            captured.update(kwargs)
            return self._summary_with_break(unverified=False, undetectable=True)

        monkeypatch.setattr(app_module, "run_polygon_split_repair", fake_run)
        result = cli_runner.invoke(
            app,
            ["polygon-repair-splits", "-s", "NFLX", "--lookback-days", "30", "--dry-run"],
        )
        assert result.exit_code == 0
        assert captured == {
            "symbols": ["NFLX"],
            "lookback_days": 30,
            "window_days": 730,
            "dry_run": True,
        }
        assert "repaired: 1" in result.stdout
        assert "undetectable: 1" in result.stdout
        assert "Micro-splits skipped (verify manually): HON" in result.stdout
        assert "break at 2023-12-18" in result.stdout
        assert "Polygon split repair complete!" in result.stdout

    def test_unverified_symbols_exit_nonzero(
        self, monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
    ) -> None:
        """A repair that fails verification exits with code 1.

        Given: A patched repair run leaving NFLX unverified,
        When: polygon-repair-splits is invoked,
        Then: The unverified line is echoed and the exit code is 1.
        """

        async def fake_run(**kwargs: object) -> object:
            return self._summary_with_break(unverified=True)

        monkeypatch.setattr(app_module, "run_polygon_split_repair", fake_run)
        result = cli_runner.invoke(app, ["polygon-repair-splits"])
        assert result.exit_code == 1
        assert "UNVERIFIED after repair: NFLX" in result.stdout

    def test_error_reports_and_exits_nonzero(
        self, monkeypatch: pytest.MonkeyPatch, cli_runner: CliRunner
    ) -> None:
        """A failing repair run is reported gracefully.

        Given: A patched repair run raising an error,
        When: polygon-repair-splits is invoked,
        Then: The error message is echoed with exit code 1.
        """

        async def fake_run(**kwargs: object) -> object:
            raise RuntimeError("split-fail")

        monkeypatch.setattr(app_module, "run_polygon_split_repair", fake_run)
        result = cli_runner.invoke(app, ["polygon-repair-splits"])
        assert result.exit_code == 1
        assert "Error during polygon split repair" in result.stdout
