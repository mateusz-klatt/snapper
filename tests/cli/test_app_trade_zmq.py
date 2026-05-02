"""Tests for the ``--instance-id`` / ``--instance-count`` CLI flags.

These tests use :mark:`real_settings` to opt out of the autouse
``mock_settings_for_tests`` fixture (defined at
``tests/conftest.py:345-359``) so that the cache-clear discipline in
``cli.app.trade_zmq`` actually affects a real cached
:func:`get_settings` result — the mocked autouse path intercepts
``get_settings`` at the module-level, defeating cache-clear spies.

A function-scope autouse ``_reset_cli_environment`` fixture resets
the partitioning env vars AND clears both factory caches before and
after every test, making the suite deterministic under any xdist
worker ordering.
"""

import os as os_mod
from collections.abc import Generator
from typing import Any
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

import snapper.cli.app as app_module
from snapper.cli.app import app
from snapper.config.settings import get_bootstrap_settings
from snapper.config.settings import get_settings

pytestmark = pytest.mark.real_settings

SYNC_MEMORY_DB_URL = "sqlite:///:memory:"


@pytest.fixture(autouse=True)
def _reset_cli_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> Generator[None]:
    """Reset env + caches before and after each test.

    Pre-test: delete the three partitioning env vars, then clear both
    factory caches so the next ``get_settings()`` call sees a clean
    baseline.

    Post-test: clear caches again because the command-under-test
    mutates :data:`os.environ` and may have cached a coordinator id
    that would leak into the next test. ``monkeypatch`` auto-restores
    :data:`os.environ` on teardown.
    """
    for var in (
        "SNAPPER_COORDINATOR_INSTANCE_ID",
        "SNAPPER_COORDINATOR_INSTANCE_COUNT",
        "SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS",
    ):
        monkeypatch.delenv(var, raising=False)
    get_bootstrap_settings.cache_clear()
    get_settings.cache_clear()
    yield
    get_bootstrap_settings.cache_clear()
    get_settings.cache_clear()


@pytest.fixture()
def cli_runner() -> CliRunner:
    """Provide a Typer :class:`CliRunner` for CLI invocation."""
    return CliRunner()


@pytest.fixture()
def _stub_trade_zmq_runtime(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Stub out validate/alembic/runner so ``trade-zmq`` short-circuits.

    Leaves the flag-handler + settings path intact so the test
    observes the exact CLI mutation behavior.

    Returns:
        A dict the test reads post-invoke:
            - ``trader_kwargs``: kwargs the CLI passed to TraderCoordinator.
            - ``settings_instance_id``: ``s.coordinator_instance_id`` as
              observed inside the command body (post cache_clear).
            - ``settings_instance_count``: same for instance_count.
    """
    captured: dict[str, Any] = {}

    def fake_validate(paper: bool = False) -> bool:
        return True

    monkeypatch.setattr(app_module, "validate_api_keys_for_trader", fake_validate)

    class DummyConfig:
        pass

    def fake_alembic_cfg(db_url: str) -> DummyConfig:
        captured["db_url"] = db_url
        return DummyConfig()

    monkeypatch.setattr(app_module, "_alembic_cfg", fake_alembic_cfg)

    class DummyAlembicCommand:
        def upgrade(self, cfg: Any, revision: str) -> None:
            captured["upgraded"] = revision

    monkeypatch.setattr(app_module, "command", DummyAlembicCommand())

    class FakeTraderCoordinator:
        def __init__(self, signal_topics: list[str] | None = None) -> None:
            captured["trader_kwargs"] = {"signal_topics": signal_topics}
            s = get_settings()
            captured["settings_instance_id"] = s.coordinator_instance_id
            captured["settings_instance_count"] = s.coordinator_instance_count

        async def start(self) -> None:
            """Stub start — the test does not exercise the trading loop."""

    monkeypatch.setattr(app_module, "TraderCoordinator", FakeTraderCoordinator)
    return captured


class TestTradeZmqFlags:
    """CLI flag precedence + cache-clear + env mutation matrix."""

    def test_no_flags_no_env_preserves_defaults(
        self,
        cli_runner: CliRunner,
        _stub_trade_zmq_runtime: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """No flags + no env → defaults (0, 1); cache_clear NOT called."""
        bootstrap_cc = MagicMock(wraps=get_bootstrap_settings.cache_clear)
        settings_cc = MagicMock(wraps=get_settings.cache_clear)
        monkeypatch.setattr(app_module.get_bootstrap_settings, "cache_clear", bootstrap_cc)
        monkeypatch.setattr(app_module.get_settings, "cache_clear", settings_cc)

        result = cli_runner.invoke(app, ["trade-zmq"])
        assert result.exit_code == 0
        assert _stub_trade_zmq_runtime["settings_instance_id"] == 0
        assert _stub_trade_zmq_runtime["settings_instance_count"] == 1
        assert bootstrap_cc.call_count == 0
        assert settings_cc.call_count == 0

    def test_flags_only_populate_env_and_clear_cache(
        self,
        cli_runner: CliRunner,
        _stub_trade_zmq_runtime: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Flags set env vars + fire cache_clear + command reads fresh settings."""
        bootstrap_cc = MagicMock(wraps=get_bootstrap_settings.cache_clear)
        settings_cc = MagicMock(wraps=get_settings.cache_clear)
        monkeypatch.setattr(app_module.get_bootstrap_settings, "cache_clear", bootstrap_cc)
        monkeypatch.setattr(app_module.get_settings, "cache_clear", settings_cc)

        result = cli_runner.invoke(
            app, ["trade-zmq", "--instance-id", "1", "--instance-count", "2"]
        )
        assert result.exit_code == 0
        assert os_mod.environ["SNAPPER_COORDINATOR_INSTANCE_ID"] == "1"
        assert os_mod.environ["SNAPPER_COORDINATOR_INSTANCE_COUNT"] == "2"
        assert bootstrap_cc.call_count == 1
        assert settings_cc.call_count == 1
        assert _stub_trade_zmq_runtime["settings_instance_id"] == 1
        assert _stub_trade_zmq_runtime["settings_instance_count"] == 2

    def test_flags_override_preexisting_env(
        self,
        cli_runner: CliRunner,
        _stub_trade_zmq_runtime: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Pre-set env + flags → flags win + cache_clear fires."""
        monkeypatch.setenv("SNAPPER_COORDINATOR_INSTANCE_ID", "0")
        monkeypatch.setenv("SNAPPER_COORDINATOR_INSTANCE_COUNT", "1")
        bootstrap_cc = MagicMock(wraps=get_bootstrap_settings.cache_clear)
        settings_cc = MagicMock(wraps=get_settings.cache_clear)
        monkeypatch.setattr(app_module.get_bootstrap_settings, "cache_clear", bootstrap_cc)
        monkeypatch.setattr(app_module.get_settings, "cache_clear", settings_cc)

        result = cli_runner.invoke(
            app, ["trade-zmq", "--instance-id", "3", "--instance-count", "4"]
        )
        assert result.exit_code == 0
        assert bootstrap_cc.call_count == 1
        assert settings_cc.call_count == 1
        assert _stub_trade_zmq_runtime["settings_instance_id"] == 3
        assert _stub_trade_zmq_runtime["settings_instance_count"] == 4

    def test_env_only_no_flags_no_cache_clear(
        self,
        cli_runner: CliRunner,
        _stub_trade_zmq_runtime: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Env pre-set + no flags → command reads env verbatim; no cache_clear."""
        monkeypatch.setenv("SNAPPER_COORDINATOR_INSTANCE_ID", "2")
        monkeypatch.setenv("SNAPPER_COORDINATOR_INSTANCE_COUNT", "3")
        bootstrap_cc = MagicMock(wraps=get_bootstrap_settings.cache_clear)
        settings_cc = MagicMock(wraps=get_settings.cache_clear)
        monkeypatch.setattr(app_module.get_bootstrap_settings, "cache_clear", bootstrap_cc)
        monkeypatch.setattr(app_module.get_settings, "cache_clear", settings_cc)

        result = cli_runner.invoke(app, ["trade-zmq"])
        assert result.exit_code == 0
        assert bootstrap_cc.call_count == 0
        assert settings_cc.call_count == 0
        assert _stub_trade_zmq_runtime["settings_instance_id"] == 2
        assert _stub_trade_zmq_runtime["settings_instance_count"] == 3

    def test_only_instance_id_flag_fires_cache_clear(
        self,
        cli_runner: CliRunner,
        _stub_trade_zmq_runtime: dict[str, Any],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Single ``--instance-id`` flag still triggers cache_clear."""
        bootstrap_cc = MagicMock(wraps=get_bootstrap_settings.cache_clear)
        settings_cc = MagicMock(wraps=get_settings.cache_clear)
        monkeypatch.setattr(app_module.get_bootstrap_settings, "cache_clear", bootstrap_cc)
        monkeypatch.setattr(app_module.get_settings, "cache_clear", settings_cc)

        result = cli_runner.invoke(app, ["trade-zmq", "--instance-id", "0"])
        assert result.exit_code == 0
        assert bootstrap_cc.call_count == 1
        assert settings_cc.call_count == 1
