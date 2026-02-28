"""Pytest configuration and shared fixtures for the Snapper test suite.

This module provides the foundational test infrastructure for all Snapper tests,
including:

- **Network isolation**: Blocks external network requests to ensure tests only
  communicate with localhost, preventing accidental API calls during testing.
- **Settings mocking**: Provides a comprehensive mock of AppSettings with sensible
  defaults for all configuration values (API keys, auth settings, ZMQ endpoints).
- **Symbol mapper mocking**: Pre-populates symbol mappings for common instruments
  (BTC-USD, ETH-USD, EUR-PLN, etc.) across all supported exchanges.
- **Database isolation**: Copies the template SQLite database to a temporary
  directory for each test session, ensuring test isolation.
- **Singleton cleanup**: Automatically clears all singleton instances (TokenManager,
  CSRFManager, WsTokenService, etc.) between tests to prevent state leakage.
- **ZMQ socket cleanup**: Ensures all ZMQ sockets are properly closed after each
  test to prevent resource leaks and hanging connections.

Fixtures:
    block_external_requests: Auto-use fixture that raises AssertionError for
        any network request to non-localhost hosts.
    mock_settings_for_tests: Auto-use fixture that patches get_settings() to
        return a mock AppSettings object with test defaults.
    isolated_sqlite_db: Session-scoped fixture that creates an isolated copy
        of the SQLite database for the test run.
    cleanup_all: Auto-use fixture that cleans up singletons and repositories
        after each test.
    cleanup_zmq_sockets: Auto-use fixture that closes all ZMQ sockets after
        each test function.

Note:
    Tests can opt out of settings mocking by using the ``@pytest.mark.real_settings``
    marker when they need to test actual settings loading behavior.
"""

import asyncio
import os
import shutil
import socket
import sys
import tracemalloc
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest import mock
from unittest.mock import Mock

import pytest
import zmq
import zmq.asyncio

from snapper.api.auth.services.ws_token_service import WsTokenService
from snapper.application.services.settings import SettingsService
from snapper.auth.dependencies import CSRFManager
from snapper.auth.tokens import TokenManager
from snapper.auth.tokens import WebSocketTokenRotator
from snapper.auth.user_service import UserService
from snapper.auth.websocket_auth import WebSocketAuthManager
from snapper.config import settings
from snapper.config.app import AppSettings
from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.data.repository import clear_repository_cache
from snapper.data.repository import dispose_repositories
from snapper.data.seed.loader import run_seed
from snapper.infrastructure.security.encryption import SettingsEncryptionService
from snapper.infrastructure.symbols.mapper import CapabilityInfo
from snapper.infrastructure.symbols.mapper import SymbolMapperService
from snapper.server.rate_limiting import limiter

_background_tasks: set[asyncio.Task[None]] = set()


@pytest.fixture(autouse=True)
def block_external_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    """Block external network requests allowing only localhost."""
    original_getaddrinfo = socket.getaddrinfo
    allowed_hosts = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}

    def assert_only_localhost(host: str, *args: Any, **kwargs: Any) -> list[Any]:
        if host not in allowed_hosts:
            raise AssertionError(f"External network request to {host!r} blocked")
        return original_getaddrinfo(host, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", assert_only_localhost)


@pytest.fixture(autouse=True)
def disable_rate_limiting() -> Generator[None, None, None]:
    """Disable slowapi rate limiting during tests to prevent 429 responses."""
    limiter.enabled = False
    yield
    limiter.enabled = True


_SINGLETONS_TO_CLEAR: tuple[type[Any], ...] = (
    TokenManager,
    CSRFManager,
    WsTokenService,
    WebSocketAuthManager,
    WebSocketTokenRotator,
)

_EXCHANGE_MAP_ATTRS: tuple[tuple[str, str, str, str], ...] = (
    ("native_to_kraken_ws", "kraken_ws_to_native", "kraken", "ws"),
    ("native_to_kraken_rest", "kraken_rest_to_native", "kraken", "rest"),
    ("native_to_ccxt", "ccxt_to_native", "kraken", "ccxt"),
    ("native_to_zonda_ws", "zonda_ws_to_native", "zonda", "ws"),
    ("native_to_walutomat_ws", "walutomat_ws_to_native", "walutomat", "ws"),
    ("native_to_walutomat_rest", "walutomat_rest_to_native", "walutomat", "rest"),
    ("native_to_polygon_rest", "polygon_rest_to_native", "polygon", "rest"),
)

_TEST_EXCHANGE_CAPABILITIES: dict[str, tuple[bool, bool]] = {
    "kraken": (True, True),
    "zonda": (True, True),
    "walutomat": (True, True),
    "polygon": (True, False),
}

_TEST_SYMBOL_MAPPINGS: dict[str, tuple[str | None, ...]] = {
    "BTC-USD": ("BTC/USD", "XXBTZUSD", "BTC/USD", "BTC-USD", None, None, "X:BTCUSD"),
    "ETH-USD": ("ETH/USD", "XETHZUSD", "ETH/USD", "ETH-USD", None, None, "X:ETHUSD"),
    "BTC-EUR": ("BTC/EUR", "XXBTZEUR", "BTC/EUR", "BTC-EUR", None, None, "X:BTCEUR"),
    "EUR-USD": ("EUR/USD", "ZEURZUSD", "EUR/USD", None, "EUR_USD", "EURUSD", "C:EURUSD"),
    "EUR-PLN": (None, None, None, None, "EUR_PLN", "EURPLN", "C:EURPLN"),
    "BTC-PLN": (None, None, "BTC/PLN", "BTC-PLN", None, None, None),
    "USD-PLN": (None, None, None, None, "USD_PLN", "USDPLN", "C:USDPLN"),
    "AAPL": ("AAPLx/USD", "AAPLxUSD", None, None, None, None, "AAPL"),
}

_SETTINGS_WITH_SERVICE_PATHS: tuple[str, ...] = (
    "snapper.config.settings.get_settings_with_service",
    "snapper.auth.tokens.get_settings_with_service",
    "snapper.auth.dependencies.get_settings_with_service",
    "snapper.api.auth.services.ws_token_service.get_settings_with_service",
)


def _clear_auth_singletons() -> None:
    """Clear all authentication-related singleton instances."""
    try:
        for cls in _SINGLETONS_TO_CLEAR:
            cls.clear_instance()
    except Exception:
        pass


def _build_mock_settings() -> Mock:
    """Build a Mock object with all AppSettings attributes set to test defaults.

    Returns:
        Configured Mock instance.
    """
    mock_settings = Mock()
    bootstrap = BootstrapSettingsLoader()
    mock_settings.db_url = bootstrap.db_url
    mock_settings.server_host = bootstrap.server_host
    mock_settings.server_port = bootstrap.server_port
    mock_settings.server_reload = bootstrap.server_reload
    mock_settings.zmq_broker_xsub = bootstrap.zmq_broker_xsub
    mock_settings.zmq_broker_xpub = bootstrap.zmq_broker_xpub
    mock_settings.kraken_api_key = ""
    mock_settings.kraken_api_secret = ""
    mock_settings.polygon_api_key = ""
    mock_settings.walutomat_api_key = ""
    mock_settings.walutomat_private_key = ""
    mock_settings.auth_secret_key = "test-secret-key-for-testing-only-32-bytes-long"
    mock_settings.csrf_secret_key = "test-csrf-secret-key-for-testing"
    mock_settings.auth_access_token_expire_minutes = 15
    mock_settings.auth_refresh_token_expire_days = 7
    mock_settings.auth_refresh_token_expire_days_extended = 30
    mock_settings.auth_algorithm = "HS256"
    mock_settings.csrf_token_expire_minutes = 60
    mock_settings.instruments = {
        "kraken": ["BTC-USD", "EUR-USD", "BTC-EUR"],
        "zonda": [],
        "walutomat": [],
        "polygon": [],
    }
    mock_settings.timeframes = ["1m"]
    mock_settings.backfill_days = 30
    mock_settings.risk_max_leverage = 1.0
    mock_settings.risk_max_drawdown = 0.15
    mock_settings.risk_r_per_trade = 0.005
    mock_settings.log_level = "INFO"
    mock_settings.log_json = False
    mock_settings.ws_token_ttl_seconds = 900
    mock_settings.ws_reconnect_max_delay = 10
    mock_settings.rest_retry_max_attempts = 5
    mock_settings.rest_retry_backoff_base = 0.2
    mock_settings.rest_retry_max_delay = 2.0
    mock_settings.rest_circuit_failure_threshold = 5
    mock_settings.rest_circuit_reset_timeout = 30
    mock_settings.zmq_heartbeat_interval_ms = 1000
    mock_settings.session_secure = False
    mock_settings.ui_origin = ""
    mock_settings.session_same_site = "lax"
    mock_settings.session_domain = ""
    return mock_settings


def _mock_load_cache_if_needed(self: Any, fail_fast: bool = False) -> None:
    """Populate symbol mapper cache with test mappings and capabilities."""
    if self._cache_loaded:
        return
    seen_caps: set[tuple[str, str]] = set()
    for native, exchange_symbols in _TEST_SYMBOL_MAPPINGS.items():
        for idx, (fwd_attr, rev_attr, exchange, channel) in enumerate(_EXCHANGE_MAP_ATTRS):
            exchange_symbol = exchange_symbols[idx]
            if exchange_symbol:
                getattr(self, fwd_attr)[native] = exchange_symbol
                getattr(self, rev_attr)[exchange_symbol] = native
                key = (exchange, channel)
                self.forward.setdefault(key, {})[native] = exchange_symbol
                self.reverse.setdefault(key, {})[exchange_symbol] = native
                cap_key = (native, exchange)
                if cap_key not in seen_caps:
                    seen_caps.add(cap_key)
                    can_md, can_trade = _TEST_EXCHANGE_CAPABILITIES[exchange]
                    self.capabilities[cap_key] = CapabilityInfo(
                        can_market_data=can_md,
                        can_trade=can_trade,
                        source="test",
                        reason=None,
                    )
    self._cache_loaded = True


def _patch_settings_functions(
    monkeypatch: pytest.MonkeyPatch,
    mock_get_settings: Any,
    mock_get_settings_with_service: Any,
) -> None:
    """Patch all get_settings and get_settings_with_service references.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        mock_get_settings: Replacement for get_settings.
        mock_get_settings_with_service: Replacement for get_settings_with_service.
    """
    monkeypatch.setattr("snapper.config.settings.get_settings", mock_get_settings)
    for path in _SETTINGS_WITH_SERVICE_PATHS:
        monkeypatch.setattr(path, mock_get_settings_with_service)
    for module_name in tuple(sys.modules):
        if not module_name.startswith("snapper."):
            continue
        module = sys.modules[module_name]
        if hasattr(module, "get_settings"):
            monkeypatch.setattr(f"{module_name}.get_settings", mock_get_settings)


@pytest.fixture(autouse=True)
def mock_settings_for_tests(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Provide mock application settings for tests."""
    if "real_settings" in request.keywords:
        return
    _clear_auth_singletons()
    clear_repository_cache()
    mock_settings = _build_mock_settings()

    def mock_get_settings() -> AppSettings:
        return mock_settings

    def mock_get_settings_with_service(settings_service: object) -> AppSettings:
        return mock_settings

    _patch_settings_functions(monkeypatch, mock_get_settings, mock_get_settings_with_service)
    monkeypatch.setattr(
        "snapper.infrastructure.symbols.mapper._get_bootstrap_settings",
        mock_get_settings,
    )
    monkeypatch.setattr(
        "snapper.infrastructure.symbols.mapper.SymbolMapperService.load_cache_if_needed",
        _mock_load_cache_if_needed,
    )
    mock_settings_service = Mock(spec=SettingsService)
    token_manager = TokenManager()
    token_manager.set_settings_service(mock_settings_service)
    csrf_manager = CSRFManager()
    csrf_manager.set_settings_service(mock_settings_service)
    ws_token_service = WsTokenService()
    ws_token_service.set_settings_service(mock_settings_service)


@pytest.fixture(autouse=True, scope="session")
def isolated_sqlite_db(tmp_path_factory: pytest.TempPathFactory) -> Generator[None, None, None]:
    """Provide an isolated SQLite database copy for the test session."""
    template_path = Path(__file__).resolve().parent.parent / "data" / "snapper.db"
    if not template_path.exists():
        yield
        return
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "session")
    temp_dir = tmp_path_factory.mktemp(f"sqlite-{worker_id}")
    db_path = temp_dir / "snapper.db"
    shutil.copy2(template_path, db_path)
    original_db_url = os.environ.get("DB_URL")
    os.environ["DB_URL"] = f"sqlite+aiosqlite:///{db_path.as_posix()}"
    settings.get_bootstrap_settings.cache_clear()
    settings.get_settings.cache_clear()
    run_seed("dev")
    try:
        yield
    finally:
        if original_db_url is not None:
            os.environ["DB_URL"] = original_db_url
        else:
            os.environ.pop("DB_URL", None)
        settings.get_bootstrap_settings.cache_clear()
        settings.get_settings.cache_clear()


@pytest.fixture(scope="session", autouse=True)
def cleanup_tmp_path_factory(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[None, None, None]:
    """Clean up temporary paths at the end of the test session."""
    base_temp = tmp_path_factory.getbasetemp()
    try:
        yield
    finally:
        shutil.rmtree(base_temp, ignore_errors=True)


def pytest_configure(config: pytest.Config) -> None:
    """Configure pytest: start tracemalloc and set event loop policy."""
    tracemalloc.start()
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:

        def safe_del(self: zmq.Context) -> None:
            """Intentionally empty to suppress ZMQ context cleanup errors."""
            pass

        zmq.Context.__del__ = safe_del
    except (ImportError, AttributeError):
        """Intentionally suppressed: zmq may not be installed."""


@pytest.fixture(autouse=True)
def cleanup_all() -> Generator[None, None, None]:
    """Clean up singletons and repository caches after each test."""
    yield
    mock.patch.stopall()
    try:
        CSRFManager.clear_instance()
        SymbolMapperService.clear_instance()
        SettingsEncryptionService.clear_instance()
        UserService.clear_instance()
        WebSocketAuthManager.clear_instance()
        TokenManager.clear_instance()
        WebSocketTokenRotator.clear_instance()
        WsTokenService.clear_instance()
        SettingsService.clear_instance()
    except Exception:
        pass
    try:
        loop = None
        try:
            loop = asyncio.get_running_loop()
            task = asyncio.create_task(dispose_repositories())
            _background_tasks.add(task)
            task.add_done_callback(_background_tasks.discard)
        except RuntimeError:
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(dispose_repositories())
            finally:
                loop.close()
    except Exception:
        pass
    finally:
        clear_repository_cache()


@pytest.fixture(autouse=True, scope="function")
def cleanup_zmq_sockets() -> Generator[None, None, None]:
    """Close all ZMQ sockets after each test function."""
    yield
    try:
        for ctx_module in [zmq, zmq.asyncio]:
            try:
                ctx = ctx_module.Context.instance()
                for socket in tuple(ctx.sockets):
                    try:
                        socket.setsockopt(zmq.LINGER, 0)
                        socket.close()
                    except Exception:
                        pass
            except Exception:
                pass
    except ImportError:
        pass
