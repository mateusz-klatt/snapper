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
import contextlib
import gc
import json
import os
import shutil
import socket
import sqlite3
import sys
import tracemalloc
import types
import weakref
from collections import Counter
from collections.abc import AsyncGenerator
from collections.abc import Generator
from pathlib import Path
from typing import Any
from typing import cast
from unittest import mock
from unittest.mock import Mock

import bcrypt
import pytest
import pytest_asyncio
import zmq
import zmq.asyncio
from alembic.config import Config
from alembic.script import ScriptDirectory
from fastapi.testclient import TestClient
from sqlalchemy import create_engine

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
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Base
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import _repository_cache
from snapper.data.repository import dispose_repositories
from snapper.data.seed.loader import _build_credential_envelope
from snapper.data.seed.loader import load_seed_profile
from snapper.infrastructure.security.encryption import SettingsEncryptionService
from snapper.infrastructure.security.encryption import get_encryption_service
from snapper.infrastructure.symbols.mapper import CapabilityInfo
from snapper.infrastructure.symbols.mapper import SymbolMapperService
from snapper.server.rate_limiting import limiter


def _install_windows_pyroute2_stub() -> None:
    """Install a minimal pyroute2 import shim for Windows test collection.

    The snapper-egress unit tests mock every kernel-facing pyroute2
    handle, but importing pyroute2 itself pulls in the POSIX-only
    ``fcntl`` module on Windows. This shim preserves the import surface
    those tests need while making accidental runtime use fail loudly.
    """
    if sys.platform != "win32":
        return
    try:
        __import__("pyroute2")
    except ModuleNotFoundError as exc:
        if exc.name != "fcntl":
            raise
    else:
        return

    class _StubNetlinkError(Exception):
        """Small replacement for pyroute2.netlink.exceptions.NetlinkError."""

        def __init__(self, code: int) -> None:
            """Store the numeric netlink errno used by wg_control."""
            super().__init__(code)
            self.code = code

    class _UnavailablePyroute2Handle:
        """Failing stand-in for unpatched pyroute2 handles on Windows."""

        def __init__(self) -> None:
            """Raise if a test accidentally reaches the real handle path."""
            raise RuntimeError("pyroute2 kernel handles are unavailable on Windows")

    pyroute2_module = types.ModuleType("pyroute2")
    netlink_module = types.ModuleType("pyroute2.netlink")
    exceptions_module = types.ModuleType("pyroute2.netlink.exceptions")

    pyroute2_module.__dict__["IPRoute"] = _UnavailablePyroute2Handle
    pyroute2_module.__dict__["WireGuard"] = _UnavailablePyroute2Handle
    pyroute2_module.__dict__["netlink"] = netlink_module
    netlink_module.__dict__["exceptions"] = exceptions_module
    exceptions_module.__dict__["NetlinkError"] = _StubNetlinkError

    sys.modules["pyroute2"] = pyroute2_module
    sys.modules["pyroute2.netlink"] = netlink_module
    sys.modules["pyroute2.netlink.exceptions"] = exceptions_module


_install_windows_pyroute2_stub()

_ORIGINAL_SQLALCHEMY_CREATE_ALL = SQLAlchemyRepository.create_all
_ORIGINAL_DATABASE_CREATE_ALL = DatabaseRepository.create_all

_background_tasks: set[asyncio.Task[None]] = set()
_tracked_zmq_contexts: weakref.WeakSet[object] = weakref.WeakSet()
_tracked_sqlite_connections: weakref.WeakSet[sqlite3.Connection] = weakref.WeakSet()
_tracking_install_state = {"zmq": False, "sqlite": False}


class _TrackedSQLiteConnection(sqlite3.Connection):
    """sqlite3 connection subclass that supports weakref-based tracking."""


def _close_test_client_on_gc(self: TestClient) -> None:
    """Close TestClient during garbage collection to release lifespan resources."""
    try:
        self.close()
    except Exception:
        return


def _cleanup_zmq_contexts() -> None:
    """Close tracked sockets and destroy shared ZMQ contexts."""
    contexts = list(_tracked_zmq_contexts)
    for ctx_module in (zmq.asyncio, zmq):
        try:
            contexts.append(ctx_module.Context.instance())
        except Exception:
            continue
    for ctx in contexts:
        sockets = getattr(ctx, "sockets", ())
        try:
            socket_iterable = tuple(sockets)
        except Exception:
            socket_iterable = ()
        for socket_obj in socket_iterable:
            try:
                socket_obj.setsockopt(zmq.LINGER, 0)
                socket_obj.close()
            except Exception:
                pass
        with contextlib.suppress(Exception):
            ctx.destroy(linger=0)


def _install_zmq_context_tracking() -> None:
    """Track every created ZMQ context so teardown can destroy it."""
    if _tracking_install_state["zmq"]:
        return
    original_sync_init = zmq.Context.__init__
    original_async_init = zmq.asyncio.Context.__init__

    def tracked_sync_init(self: zmq.Context, *args: Any, **kwargs: Any) -> None:
        original_sync_init(self, *args, **kwargs)
        _tracked_zmq_contexts.add(self)

    def tracked_async_init(self: zmq.asyncio.Context, *args: Any, **kwargs: Any) -> None:
        original_async_init(self, *args, **kwargs)
        _tracked_zmq_contexts.add(self)

    zmq.Context.__init__ = tracked_sync_init
    zmq.asyncio.Context.__init__ = tracked_async_init
    _tracking_install_state["zmq"] = True


def _install_sqlite_connection_tracking() -> None:
    """Track sqlite connections so session teardown can close stragglers."""
    if _tracking_install_state["sqlite"]:
        return
    original_connect = sqlite3.connect

    def tracked_connect(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        kwargs.setdefault("factory", _TrackedSQLiteConnection)
        connection = cast(sqlite3.Connection, original_connect(*args, **kwargs))
        with contextlib.suppress(TypeError):
            _tracked_sqlite_connections.add(connection)
        return connection

    sqlite3.connect = tracked_connect
    sqlite3.dbapi2.connect = tracked_connect
    _tracking_install_state["sqlite"] = True


def _close_tracked_sqlite_connections() -> None:
    """Close any sqlite connections left open by the current test."""
    for connection in tuple(_tracked_sqlite_connections):
        with contextlib.suppress(Exception):
            connection.close()


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
def disable_rate_limiting() -> Generator[None]:
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
    ("native_to_walutomat_ws", "walutomat_ws_to_native", "walutomat", "ws"),
    ("native_to_walutomat_rest", "walutomat_rest_to_native", "walutomat", "rest"),
    ("native_to_polygon_rest", "polygon_rest_to_native", "polygon", "rest"),
)

_TEST_EXCHANGE_CAPABILITIES: dict[str, tuple[bool, bool]] = {
    "kraken": (True, True),
    "walutomat": (True, True),
    "polygon": (True, False),
}

_TEST_SYMBOL_MAPPINGS: dict[str, tuple[str | None, ...]] = {
    "BTC-USD": ("BTC/USD", "XXBTZUSD", "BTC/USD", None, None, "X:BTCUSD"),
    "ETH-USD": ("ETH/USD", "XETHZUSD", "ETH/USD", None, None, "X:ETHUSD"),
    "BTC-EUR": ("BTC/EUR", "XXBTZEUR", "BTC/EUR", None, None, "X:BTCEUR"),
    "EUR-USD": ("EUR/USD", "ZEURZUSD", "EUR/USD", "EUR_USD", "EURUSD", "C:EURUSD"),
    "EUR-PLN": (None, None, None, "EUR_PLN", "EURPLN", "C:EURPLN"),
    "USD-PLN": (None, None, None, "USD_PLN", "USDPLN", "C:USDPLN"),
    "AAPL": ("AAPLx/USD", "AAPLxUSD", None, None, None, "AAPL"),
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
    mock_settings.server_api_only = True
    mock_settings.zmq_broker_xsub = bootstrap.zmq_broker_xsub
    mock_settings.zmq_broker_xpub = bootstrap.zmq_broker_xpub
    mock_settings.telemetry_recording_enabled = bootstrap.telemetry_recording_enabled
    mock_settings.polygon_api_key = ""
    mock_settings.auth_secret_key = "test-secret-key-for-testing-only-32-bytes-long"
    mock_settings.csrf_secret_key = "test-csrf-secret-key-for-testing"
    mock_settings.auth_access_token_expire_minutes = 15
    mock_settings.auth_refresh_token_expire_days = 7
    mock_settings.auth_refresh_token_expire_days_extended = 30
    mock_settings.auth_algorithm = "HS256"
    mock_settings.csrf_token_expire_minutes = 60
    mock_settings.instruments = {
        "kraken": ["BTC-USD", "EUR-USD", "BTC-EUR"],
        "walutomat": [],
        "polygon": [],
    }
    mock_settings.timeframes = ["1m"]
    mock_settings.candle_forward_fill = False
    mock_settings.candle_minute_completion = False
    mock_settings.spot_trade_built_shadow_enabled = False
    mock_settings.spot_candle_source = "native"
    mock_settings.trade_built_finalize_grace_seconds = 12
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
    mock_settings.coordinator_instance_id = 0
    mock_settings.coordinator_instance_count = 1
    mock_settings.coordinator_outbox_max_scan_rows = 1000
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
    _repository_cache.clear()
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


def _extract_sqlite_path(db_url: str) -> Path | None:
    """Return the file path from a SQLite URL, or None for other engines."""
    for prefix in ("sqlite+aiosqlite:///", "sqlite:///"):
        if db_url.startswith(prefix):
            return Path(db_url[len(prefix) :]).resolve()
    return None


def _sqlite_contract_rows(
    connection: sqlite3.Connection,
    statement: str,
    parameters: dict[str, object] | None = None,
) -> Counter[tuple[object, ...]]:
    """Return one fixture-contract query while preserving row multiplicity."""
    return Counter(tuple(row) for row in connection.execute(statement, parameters or {}).fetchall())


def _require_fresh_seed_fixture(db_path: Path) -> None:
    """Verify an isolated SQLite copy exactly matches the resolved dev seed."""
    profile = load_seed_profile("dev")
    declared_wallets = profile.wallets
    expected_wallets = Counter(
        (
            wallet.label,
            int(wallet.is_paper),
            wallet.description,
            1,
        )
        for wallet in declared_wallets
    )
    expected_credentials = Counter(
        (
            wallet.label,
            int(wallet.is_paper),
            credential.exchange,
            credential.credential_type,
            credential.label,
            json.dumps(
                json.loads(_build_credential_envelope(credential)),
                sort_keys=True,
            ),
            1,
            1,
        )
        for wallet in declared_wallets
        for credential in wallet.credentials
    )
    expected_methods = Counter(
        (
            wallet.label,
            int(wallet.is_paper),
            credential.exchange,
            "live",
            credential.reconciliation_method,
            1,
            1,
        )
        for wallet in declared_wallets
        for credential in wallet.credentials
        if credential.reconciliation_method != "unclassified"
    )
    if not declared_wallets:
        expected_wallets = Counter(
            {
                (
                    "default",
                    1,
                    "Default paper-mode wallet seeded for single-user deployment",
                    1,
                ): 1
            }
        )
        expected_credentials = Counter(
            {
                (
                    "default",
                    1,
                    "paper",
                    "paper",
                    "default paper bootstrap",
                    '{"initial_balance": "10000.0"}',
                    1,
                    1,
                ): 1
            }
        )
    alembic_config = Config(str(Path(__file__).resolve().parents[1] / "alembic.ini"))
    expected_heads = Counter(
        (head,) for head in ScriptDirectory.from_config(alembic_config).get_heads()
    )
    expected_passwords = {user.username: user.password for user in profile.users}
    active_parameters: dict[str, object] = {
        "known_to": KNOWN_TO_MAX.replace(tzinfo=None).isoformat(
            sep=" ",
            timespec="microseconds",
        )
    }
    connection_resources = contextlib.ExitStack()
    try:
        connection = connection_resources.enter_context(
            contextlib.closing(sqlite3.connect(db_path))
        )
        credential_rows = connection.execute(
            "SELECT w.label, w.is_paper, c.exchange, c.credential_type, c.label, "
            "c.encrypted_payload, "
            "(c.known_to = :known_to), "
            "(w.known_to = :known_to) "
            "FROM wallet_credentials c "
            "LEFT JOIN wallets w ON w.public_id = c.wallet_public_id",
            active_parameters,
        ).fetchall()
        actual_credentials = Counter(
            (
                *tuple(row[:5]),
                json.dumps(
                    json.loads(get_encryption_service().decrypt(str(row[5]))),
                    sort_keys=True,
                ),
                row[6],
                row[7],
            )
            for row in credential_rows
        )
        password_rows = connection.execute("SELECT username, password_hash FROM users").fetchall()
        password_contract: Counter[tuple[object, ...]] = Counter()
        for username, password_hash in password_rows:
            expected_password = expected_passwords.get(str(username))
            valid = False
            if expected_password is not None:
                with contextlib.suppress(ValueError):
                    valid = bcrypt.checkpw(
                        expected_password.encode(),
                        str(password_hash).encode(),
                    )
            password_contract[(username, valid)] += 1
        setting_rows = connection.execute(
            "SELECT key, value, category, description, is_encrypted, "
            "(known_to = :known_to) FROM settings",
            active_parameters,
        ).fetchall()
        actual_settings = Counter(
            (
                key,
                get_encryption_service().decrypt(str(value)) if is_encrypted else value,
                category,
                description,
                int(is_encrypted),
                int(active),
            )
            for key, value, category, description, is_encrypted, active in setting_rows
        )
        actual = {
            "users": _sqlite_contract_rows(
                connection,
                "SELECT username, email, role, is_active, (known_to = :known_to) FROM users",
                active_parameters,
            ),
            "passwords": password_contract,
            "operators": _sqlite_contract_rows(
                connection,
                "SELECT label, description, (known_to = :known_to) FROM operators",
                active_parameters,
            ),
            "memberships": _sqlite_contract_rows(
                connection,
                "SELECT u.username, o.label, m.is_primary, "
                "(u.known_to = :known_to), "
                "(o.known_to = :known_to), "
                "(m.known_to = :known_to) "
                "FROM user_operator_memberships m "
                "LEFT JOIN users u ON u.public_id = m.user_public_id "
                "LEFT JOIN operators o ON o.public_id = m.operator_public_id",
                active_parameters,
            ),
            "wallets": _sqlite_contract_rows(
                connection,
                "SELECT label, is_paper, description, (known_to = :known_to) FROM wallets",
                active_parameters,
            ),
            "credentials": actual_credentials,
            "methods": _sqlite_contract_rows(
                connection,
                "SELECT w.label, w.is_paper, c.exchange, c.mode, c.method, "
                "(c.known_to = :known_to), "
                "(w.known_to = :known_to) "
                "FROM portfolio_reconciliation_method_configs c "
                "LEFT JOIN wallets w ON w.public_id = c.wallet_public_id",
                active_parameters,
            ),
            "settings": actual_settings,
            "grants": _sqlite_contract_rows(
                connection,
                "SELECT "
                "(SELECT COUNT(*) FROM wallet_operator_scope_grants), "
                "(SELECT COUNT(*) FROM wallet_user_read_grants)",
            ),
            "alembic_heads": _sqlite_contract_rows(
                connection,
                "SELECT version_num FROM alembic_version",
            ),
        }
    except sqlite3.OperationalError as exc:
        raise RuntimeError(
            "SQLite test fixture is missing required schema; run `make migrate-dev-sqlite`"
        ) from exc
    finally:
        connection_resources.close()
    expected = {
        "users": Counter((user.username, user.email, user.role, 1, 1) for user in profile.users),
        "passwords": Counter((user.username, True) for user in profile.users),
        "operators": Counter(
            (operator.label, operator.description, 1) for operator in profile.operators
        ),
        "memberships": Counter(
            (
                user.username,
                operator_label,
                int(operator_label == user.primary_operator),
                1,
                1,
                1,
            )
            for user in profile.users
            for operator_label in user.operators
        ),
        "wallets": expected_wallets,
        "credentials": expected_credentials,
        "methods": expected_methods,
        "settings": Counter(
            (
                setting.key,
                setting.value,
                setting.category,
                setting.description,
                int(SettingsEncryptionService.is_sensitive_setting(setting.key)),
                1,
            )
            for setting in profile.settings
        ),
        "grants": Counter({(0, 0): 1}),
        "alembic_heads": expected_heads,
    }
    mismatches = sorted(key for key in expected if actual[key] != expected[key])
    if mismatches:
        raise RuntimeError(
            "SQLite test fixture is stale or partial for "
            f"{', '.join(mismatches)}; run `make migrate-dev-sqlite`"
        )


def _clear_lru_cache(func: object) -> None:
    """Clear a cache-enabled callable when the cache API is available."""
    cache_clear = getattr(func, "cache_clear", None)
    if callable(cache_clear):
        cache_clear()


_db_template_state: dict[str, Path | None] = {"path": None}


@pytest.fixture(scope="session", autouse=True)
def db_template_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Create a WAL-mode template SQLite DB with full schema once per worker.

    Subsequent create_all() calls on SQLAlchemyRepository/DatabaseRepository
    copy this template (~1ms) instead of running metadata.create_all (~3-4s).
    Persisting WAL mode before those copies preserves the connection-level
    initialization that the create_all replacement otherwise bypasses.
    """
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
    template_dir = tmp_path_factory.mktemp(f"db-template-{worker_id}")
    template_path = template_dir / "template.db"

    engine = create_engine(f"sqlite:///{template_path}")
    Base.metadata.create_all(engine)
    with engine.connect() as connection:
        journal_mode = connection.exec_driver_sql("PRAGMA journal_mode=WAL").scalar_one()
    engine.dispose()
    if journal_mode != "wal":
        raise RuntimeError(f"SQLite test template did not enter WAL mode: {journal_mode}")
    _db_template_state["path"] = template_path
    return template_path


def _try_copy_template(db_url: str) -> bool:
    """Copy template DB file if URL is file-based SQLite and template exists.

    Returns True if copy succeeded, False if fallback to real create_all needed.
    In-memory SQLite (`:memory:`) always falls back.
    """
    if _db_template_state["path"] is None or not _db_template_state["path"].exists():
        return False
    if ":memory:" in db_url:
        return False
    for prefix in ("sqlite:///", "sqlite+aiosqlite:///"):
        if db_url.startswith(prefix):
            raw_path = db_url[len(prefix) :]
            if not raw_path or raw_path == ":memory:":
                return False
            target = Path(raw_path)
            if not target.exists():
                shutil.copy2(_db_template_state["path"], target)
                return True
    return False


async def _patched_create_all_async(self: Any) -> None:
    """Async create_all that copies template DB instead of full schema creation."""
    if _try_copy_template(str(self.engine.url)):
        return
    async with self.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


def _patched_create_all_sync(self: Any) -> None:
    """Sync create_all that copies template DB instead of full schema creation."""
    if _try_copy_template(str(self.engine.url)):
        return
    Base.metadata.create_all(self.engine)


_patched_create_all_async.__wrapped__ = _ORIGINAL_SQLALCHEMY_CREATE_ALL
_patched_create_all_sync.__wrapped__ = _ORIGINAL_DATABASE_CREATE_ALL


@pytest.fixture(autouse=True, scope="session")
def _patch_create_all(db_template_path: Path) -> Generator[None]:
    """Monkeypatch create_all to use template copy for all test DBs."""
    SQLAlchemyRepository.create_all = _patched_create_all_async
    DatabaseRepository.create_all = _patched_create_all_sync
    yield
    SQLAlchemyRepository.create_all = _ORIGINAL_SQLALCHEMY_CREATE_ALL
    DatabaseRepository.create_all = _ORIGINAL_DATABASE_CREATE_ALL


@pytest.fixture(autouse=True, scope="session")
def isolated_sqlite_db(tmp_path_factory: pytest.TempPathFactory) -> Generator[None]:
    """Provide an isolated database for the test session.

    SQLite: copies the complete fresh-seeded template to a temp directory
    so the original is never modified, then verifies its exact seed and
    migration contract. Other engines (Postgres, etc.) keep DB_URL as-is
    and are not required to equal the disposable dev profile.
    """
    original_db_url = os.environ.get("DB_URL")
    _clear_lru_cache(settings.get_bootstrap_settings)
    configured_url = BootstrapSettingsLoader().db_url
    sqlite_path = _extract_sqlite_path(configured_url)

    if sqlite_path and sqlite_path.exists():
        worker_id = os.environ.get("PYTEST_XDIST_WORKER", "session")
        temp_dir = tmp_path_factory.mktemp(f"sqlite-{worker_id}")
        db_path = temp_dir / "snapper.db"
        shutil.copy2(sqlite_path, db_path)
        os.environ["DB_URL"] = f"sqlite+aiosqlite:///{db_path.as_posix()}"
        _clear_lru_cache(settings.get_bootstrap_settings)
        _require_fresh_seed_fixture(db_path)

    _clear_lru_cache(settings.get_settings)
    try:
        yield
    finally:
        if original_db_url is not None:
            os.environ["DB_URL"] = original_db_url
        else:
            os.environ.pop("DB_URL", None)
        _clear_lru_cache(settings.get_bootstrap_settings)
        _clear_lru_cache(settings.get_settings)


@pytest.fixture(scope="session", autouse=True)
def cleanup_tmp_path_factory(
    tmp_path_factory: pytest.TempPathFactory,
) -> Generator[None]:
    """Clean up temporary paths at the end of the test session."""
    base_temp = tmp_path_factory.getbasetemp()
    try:
        yield
    finally:
        shutil.rmtree(base_temp, ignore_errors=True)


@pytest.fixture(scope="session", autouse=True)
def cleanup_session_resources() -> Generator[None]:
    """Dispose lingering repositories and contexts before pytest final GC."""
    yield
    with contextlib.suppress(Exception):
        _cleanup_zmq_contexts()
    _close_tracked_sqlite_connections()
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(dispose_repositories())
    finally:
        loop.close()
    _repository_cache.clear()
    gc.collect()


def pytest_configure(config: pytest.Config) -> None:
    """Configure pytest: start tracemalloc and set event loop policy."""
    tracemalloc.start()
    TestClient.__del__ = _close_test_client_on_gc
    _install_zmq_context_tracking()
    _install_sqlite_connection_tracking()
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:

        def safe_del(self: zmq.Context) -> None:
            """Intentionally empty to suppress ZMQ context cleanup errors."""
            pass

        zmq.Context.__del__ = safe_del
    except (ImportError, AttributeError):
        """Intentionally suppressed: zmq may not be installed."""


@pytest_asyncio.fixture(autouse=True)
async def cleanup_all() -> AsyncGenerator[None]:
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
        await dispose_repositories()
    except Exception:
        pass
    finally:
        _close_tracked_sqlite_connections()
        _cleanup_zmq_contexts()
        _repository_cache.clear()


@pytest.fixture(autouse=True)
def cleanup_zmq_sockets() -> Generator[None]:
    """Close all ZMQ sockets after each test function."""
    yield
    with contextlib.suppress(ImportError):
        _cleanup_zmq_contexts()
