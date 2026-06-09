"""Unit tests for AppSettings configuration facade."""

from typing import Any

import pytest

from snapper.config.app import AppSettings
from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.core.types import ProcessAutostartProfileEnum


class _DummyService:
    """Test dummy for settings service."""

    def __init__(self, return_value: Any) -> None:
        self.return_value = return_value
        self.calls: list[tuple[str, Any]] = []

    def get_setting(self, key: str, default: Any) -> Any:
        self.calls.append((key, default))
        return self.return_value


def test_db_setting_raises_without_service() -> None:
    """Verify accessing DB setting without service raises RuntimeError.

    Given AppSettings with settings_service=None,
    When accessing polygon_api_key (a DB-backed property),
    Then RuntimeError is raised with appropriate message.
    """
    bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
    settings = AppSettings(bootstrap, settings_service=None)
    with pytest.raises(RuntimeError, match="SettingsService not initialized"):
        _ = settings.polygon_api_key


def test_db_setting_returns_default_when_none_from_service() -> None:
    """Verify default is returned when service returns None.

    Given AppSettings with service returning None,
    When accessing auth_secret_key,
    Then default value is returned.
    """
    bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
    service = _DummyService(return_value=None)
    settings = AppSettings(bootstrap, settings_service=service)
    value = settings.auth_secret_key
    assert value == "change-me-in-production-use-openssl-rand-hex-32"
    assert service.calls == [("auth_secret_key", "change-me-in-production-use-openssl-rand-hex-32")]


class MockSettingsService:
    """Mock settings service for testing AppSettings properties."""

    def __init__(self, values: dict[str, Any] | None = None) -> None:
        """Initialize the instance."""
        self.values = values or {}

    def get_setting(self, key: str, default: Any) -> Any:
        """Return setting value or default if not found."""
        return self.values.get(key, default)


class TestAppSettingsMarketDataProperties:
    """Tests for AppSettings market-data API property accessors.

    The per-exchange trading credential properties (``kraken_api_key``,
    ``walutomat_api_key`` etc.) were removed because wallet-scoped
    credentials live in the ``wallet_credentials`` table and are
    loaded by ``CredentialResolver`` during per-wallet executor
    startup. ``polygon_api_key`` stays on ``AppSettings`` because
    Polygon is a shared market-data provider, not a wallet.
    """

    def test_polygon_api_key_returns_value(self) -> None:
        """Verify polygon_api_key returns configured value.

        Given service with polygon_api_key set,
        When accessing settings.polygon_api_key,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"polygon_api_key": "poly-key"})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.polygon_api_key == "poly-key"


class TestAppSettingsFeedEgress:
    """Tests for the feed_egress_enabled gate (default off, robust coercion)."""

    def test_defaults_off_when_absent(self) -> None:
        """Verify an absent setting reads as False (feeds stay direct)."""
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        settings = AppSettings(bootstrap, settings_service=MockSettingsService({}))
        assert settings.feed_egress_enabled is False

    def test_true_bool_enables(self) -> None:
        """Verify a boolean True enables feed egress."""
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"feed_egress_enabled": True})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.feed_egress_enabled is True

    def test_truthy_string_enables(self) -> None:
        """Verify a 'true' string value enables feed egress."""
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"feed_egress_enabled": "true"})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.feed_egress_enabled is True

    def test_false_string_does_not_enable(self) -> None:
        """Verify a 'false' string value does NOT read as truthy (footgun guard)."""
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"feed_egress_enabled": "false"})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.feed_egress_enabled is False

    def test_nonstring_nonbool_coerced(self) -> None:
        """Verify a non-bool/non-string truthy value is coerced via bool()."""
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"feed_egress_enabled": 1})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.feed_egress_enabled is True


class TestAppSettingsAuthProperties:
    """Tests for AppSettings authentication property accessors."""

    def test_auth_secret_key_returns_value(self) -> None:
        """Verify auth_secret_key returns configured value.

        Given service with auth_secret_key set,
        When accessing settings.auth_secret_key,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"auth_secret_key": "custom-secret"})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.auth_secret_key == "custom-secret"

    def test_csrf_secret_key_returns_value(self) -> None:
        """Verify csrf_secret_key returns configured value.

        Given service with csrf_secret_key set,
        When accessing settings.csrf_secret_key,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"csrf_secret_key": "csrf-secret"})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.csrf_secret_key == "csrf-secret"

    def test_ws_token_ttl_seconds_returns_value(self) -> None:
        """Verify ws_token_ttl_seconds returns configured value.

        Given service with ws_token_ttl_seconds set,
        When accessing settings.ws_token_ttl_seconds,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"ws_token_ttl_seconds": 1800})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.ws_token_ttl_seconds == 1800

    def test_auth_algorithm_returns_value(self) -> None:
        """Verify auth_algorithm returns configured value.

        Given service with auth_algorithm set,
        When accessing settings.auth_algorithm,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"auth_algorithm": "HS512"})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.auth_algorithm == "HS512"

    def test_auth_access_token_expire_minutes_returns_value(self) -> None:
        """Verify auth_access_token_expire_minutes returns configured value.

        Given service with auth_access_token_expire_minutes set,
        When accessing settings.auth_access_token_expire_minutes,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"auth_access_token_expire_minutes": 30})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.auth_access_token_expire_minutes == 30

    def test_auth_refresh_token_expire_days_returns_value(self) -> None:
        """Verify auth_refresh_token_expire_days returns configured value.

        Given service with auth_refresh_token_expire_days set,
        When accessing settings.auth_refresh_token_expire_days,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"auth_refresh_token_expire_days": 14})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.auth_refresh_token_expire_days == 14

    def test_auth_refresh_token_expire_days_extended_returns_value(self) -> None:
        """Verify auth_refresh_token_expire_days_extended returns configured value.

        Given service with auth_refresh_token_expire_days_extended set,
        When accessing settings.auth_refresh_token_expire_days_extended,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"auth_refresh_token_expire_days_extended": 60})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.auth_refresh_token_expire_days_extended == 60

    def test_csrf_token_expire_minutes_returns_value(self) -> None:
        """Verify csrf_token_expire_minutes returns configured value.

        Given service with csrf_token_expire_minutes set,
        When accessing settings.csrf_token_expire_minutes,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"csrf_token_expire_minutes": 120})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.csrf_token_expire_minutes == 120


class TestAppSettingsTradingProperties:
    """Tests for AppSettings trading configuration property accessors."""

    def test_instruments_returns_value(self) -> None:
        """Verify instruments returns configured exchange instrument map.

        Given service with instruments dict set,
        When accessing settings.instruments,
        Then configured dict is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        custom_instruments = {"kraken": ["BTC-USD"], "walutomat": ["EUR-PLN"]}
        service = MockSettingsService({"instruments": custom_instruments})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.instruments == custom_instruments

    def test_instruments_default_is_wildcard_for_every_exchange(self) -> None:
        """Verify the fresh-DB default is wildcard on every exchange.

        Given service with no ``instruments`` setting persisted,
        When accessing settings.instruments,
        Then every exchange (kraken / kraken_futures / kraken_equities
            / walutomat / polygon) maps to the wildcard sentinel
            ``["*"]``. Live-WS publishers expand via
            ``get_available_*_symbols()`` in ``_validate_symbols``;
            historical backfill services recognise the sentinel and
            delegate to ``_get_all_mapped_symbols()`` /
            ``get_available_*_symbols()`` (same path the ``--all`` CLI
            flag takes). The wildcard is therefore semantically
            equivalent to ``--all`` across publishers + backfill so a
            single sentinel covers the full venue universe everywhere.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService()
        settings = AppSettings(bootstrap, settings_service=service)
        default_instruments = settings.instruments
        assert default_instruments["kraken"] == ["*"]
        assert default_instruments["kraken_futures"] == ["*"]
        assert default_instruments["kraken_equities"] == ["*"]
        assert default_instruments["walutomat"] == ["*"]
        assert default_instruments["polygon"] == ["*"]

    def test_timeframes_returns_value(self) -> None:
        """Verify timeframes returns configured list.

        Given service with timeframes list set,
        When accessing settings.timeframes,
        Then configured list is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"timeframes": ["1m", "5m", "1h"]})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.timeframes == ["1m", "5m", "1h"]

    def test_paper_instruments_returns_value(self) -> None:
        """Verify paper_instruments returns configured source map.

        Given service with paper_instruments dict set,
        When accessing settings.paper_instruments,
        Then configured dict is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        custom_sources = {"kraken": ["BTC-USD"], "polygon": ["AAPL"]}
        service = MockSettingsService({"paper_instruments": custom_sources})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.paper_instruments == custom_sources

    def test_backfill_days_returns_value(self) -> None:
        """Verify backfill_days returns configured value.

        Given service with backfill_days set,
        When accessing settings.backfill_days,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"backfill_days": 60})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.backfill_days == 60


class TestAppSettingsRiskProperties:
    """Tests for AppSettings risk management property accessors."""

    def test_risk_max_leverage_returns_value(self) -> None:
        """Verify risk_max_leverage returns configured value.

        Given service with risk_max_leverage set,
        When accessing settings.risk_max_leverage,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"risk_max_leverage": 2.0})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.risk_max_leverage == pytest.approx(2.0)

    def test_risk_max_drawdown_returns_value(self) -> None:
        """Verify risk_max_drawdown returns configured value.

        Given service with risk_max_drawdown set,
        When accessing settings.risk_max_drawdown,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"risk_max_drawdown": 0.20})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.risk_max_drawdown == pytest.approx(0.20)

    def test_risk_r_per_trade_returns_value(self) -> None:
        """Verify risk_r_per_trade returns configured value.

        Given service with risk_r_per_trade set,
        When accessing settings.risk_r_per_trade,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"risk_r_per_trade": 0.01})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.risk_r_per_trade == pytest.approx(0.01)

    def test_has_db_access_false_without_service(self) -> None:
        """Verify has_db_access returns False without settings service.

        Given AppSettings without database service,
        When accessing has_db_access,
        Then False is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        settings = AppSettings(bootstrap)
        assert settings.has_db_access is False

    def test_has_db_access_true_with_service(self) -> None:
        """Verify has_db_access returns True with settings service.

        Given AppSettings with MockSettingsService,
        When accessing has_db_access,
        Then True is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.has_db_access is True


class TestAppSettingsLoggingProperties:
    """Tests for AppSettings logging configuration property accessors."""

    def test_log_level_returns_value(self) -> None:
        """Verify log_level returns configured value.

        Given service with log_level set,
        When accessing settings.log_level,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"log_level": "DEBUG"})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.log_level == "DEBUG"

    def test_log_json_returns_value(self) -> None:
        """Verify log_json returns configured value.

        Given service with log_json set,
        When accessing settings.log_json,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"log_json": True})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.log_json is True


class TestAppSettingsRestApiProperties:
    """Tests for AppSettings REST API configuration property accessors."""

    def test_ws_reconnect_max_delay_returns_value(self) -> None:
        """Verify ws_reconnect_max_delay returns configured value.

        Given service with ws_reconnect_max_delay set,
        When accessing settings.ws_reconnect_max_delay,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"ws_reconnect_max_delay": 30})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.ws_reconnect_max_delay == 30

    def test_rest_retry_max_attempts_returns_value(self) -> None:
        """Verify rest_retry_max_attempts returns configured value.

        Given service with rest_retry_max_attempts set,
        When accessing settings.rest_retry_max_attempts,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"rest_retry_max_attempts": 10})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.rest_retry_max_attempts == 10

    def test_rest_retry_backoff_base_returns_value(self) -> None:
        """Verify rest_retry_backoff_base returns configured value.

        Given service with rest_retry_backoff_base set,
        When accessing settings.rest_retry_backoff_base,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"rest_retry_backoff_base": 0.5})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.rest_retry_backoff_base == pytest.approx(0.5)

    def test_rest_retry_max_delay_returns_value(self) -> None:
        """Verify rest_retry_max_delay returns configured value.

        Given service with rest_retry_max_delay set,
        When accessing settings.rest_retry_max_delay,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"rest_retry_max_delay": 5.0})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.rest_retry_max_delay == pytest.approx(5.0)

    def test_rest_circuit_failure_threshold_returns_value(self) -> None:
        """Verify rest_circuit_failure_threshold returns configured value.

        Given service with rest_circuit_failure_threshold set,
        When accessing settings.rest_circuit_failure_threshold,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"rest_circuit_failure_threshold": 10})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.rest_circuit_failure_threshold == 10

    def test_rest_circuit_reset_timeout_returns_value(self) -> None:
        """Verify rest_circuit_reset_timeout returns configured value.

        Given service with rest_circuit_reset_timeout set,
        When accessing settings.rest_circuit_reset_timeout,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"rest_circuit_reset_timeout": 60})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.rest_circuit_reset_timeout == 60


class TestAppSettingsZmqProperties:
    """Tests for AppSettings ZMQ messaging property accessors."""

    def test_zmq_heartbeat_interval_ms_returns_value(self) -> None:
        """Verify zmq_heartbeat_interval_ms returns configured value.

        Given service with zmq_heartbeat_interval_ms set,
        When accessing settings.zmq_heartbeat_interval_ms,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"zmq_heartbeat_interval_ms": 2000})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.zmq_heartbeat_interval_ms == 2000


class TestAppSettingsSessionProperties:
    """Tests for AppSettings session configuration property accessors."""

    def test_session_secure_returns_value(self) -> None:
        """Verify session_secure returns configured value.

        Given service with session_secure set,
        When accessing settings.session_secure,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"session_secure": True})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.session_secure is True

    def test_ui_origin_returns_value(self) -> None:
        """Verify ui_origin returns configured value.

        Given service with ui_origin set,
        When accessing settings.ui_origin,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"ui_origin": "https://app.example.com"})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.ui_origin == "https://app.example.com"

    def test_session_same_site_returns_value(self) -> None:
        """Verify session_same_site returns configured value.

        Given service with session_same_site set,
        When accessing settings.session_same_site,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"session_same_site": "strict"})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.session_same_site == "strict"

    def test_session_domain_returns_value(self) -> None:
        """Verify session_domain returns configured value.

        Given service with session_domain set,
        When accessing settings.session_domain,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"session_domain": ".example.com"})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.session_domain == ".example.com"


class TestAppSettingsPublicApi:
    """Tests for AppSettings public API methods."""

    def test_get_setting_returns_custom_value(self) -> None:
        """Verify get_setting returns value from service.

        Given service with custom_setting set,
        When calling settings.get_setting(),
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"custom_setting": "custom_value"})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.get_setting("custom_setting", "default") == "custom_value"

    def test_get_setting_returns_default_when_not_found(self) -> None:
        """Verify get_setting returns default when key missing.

        Given service with empty values,
        When calling settings.get_setting() with missing key,
        Then fallback default is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.get_setting("missing_key", "fallback") == "fallback"


class TestServerReloadProperty:
    """Tests for AppSettings server_reload bootstrap property."""

    def test_server_reload_returns_false_by_default(self) -> None:
        """Verify server_reload defaults to False.

        Given bootstrap with SERVER_RELOAD=False,
        When accessing settings.server_reload,
        Then False is returned.
        """
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            SERVER_RELOAD=False,
        )
        settings = AppSettings(bootstrap, settings_service=None)
        assert settings.server_reload is False

    def test_server_reload_returns_true_when_enabled(self) -> None:
        """Verify server_reload returns True when enabled.

        Given bootstrap with SERVER_RELOAD=True,
        When accessing settings.server_reload,
        Then True is returned.
        """
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            SERVER_RELOAD=True,
        )
        settings = AppSettings(bootstrap, settings_service=None)
        assert settings.server_reload is True


class TestServerProxyProperties:
    """Tests for AppSettings reverse proxy bootstrap properties."""

    def test_server_proxy_headers_returns_true_by_default(self) -> None:
        """Verify server_proxy_headers defaults to True.

        Given bootstrap without override,
        When accessing settings.server_proxy_headers,
        Then True is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        settings = AppSettings(bootstrap, settings_service=None)
        assert settings.server_proxy_headers is True

    def test_server_proxy_headers_returns_false_when_disabled(self) -> None:
        """Verify server_proxy_headers can be disabled explicitly.

        Given bootstrap with SERVER_PROXY_HEADERS=False,
        When accessing settings.server_proxy_headers,
        Then False is returned.
        """
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            SERVER_PROXY_HEADERS=False,
        )
        settings = AppSettings(bootstrap, settings_service=None)
        assert settings.server_proxy_headers is False

    def test_server_forwarded_allow_ips_returns_bootstrap_value(self) -> None:
        """Verify trusted forwarded proxy list is exposed.

        Given bootstrap with SERVER_FORWARDED_ALLOW_IPS configured,
        When accessing settings.server_forwarded_allow_ips,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            SERVER_FORWARDED_ALLOW_IPS="127.0.0.1,172.17.0.1",
        )
        settings = AppSettings(bootstrap, settings_service=None)
        assert settings.server_forwarded_allow_ips == "127.0.0.1,172.17.0.1"

    def test_recon_balance_threshold_defaults_to_one(self) -> None:
        """Verify recon_balance_threshold defaults to 1.0.

        Given service without recon_balance_threshold set,
        When accessing settings.recon_balance_threshold,
        Then 1.0 is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.recon_balance_threshold == pytest.approx(1.0)


class TestCoordinatorPartitioningProperties:
    """``coordinator_*`` delegate properties on :class:`AppSettings`.

    The bootstrap field types differ from the :class:`AppSettings`
    return types for ``coordinator_outbox_max_scan_rows`` (bootstrap
    stores raw ``str | None`` for pydantic-settings compatibility;
    ``AppSettings`` parses to ``int | None``). The other two are
    straight int passthroughs. None of these consult the
    :class:`SettingsService` — verified by constructing ``AppSettings``
    with ``settings_service=None``.
    """

    def test_instance_id_delegates_to_bootstrap(self) -> None:
        """``coordinator_instance_id`` passes through from bootstrap."""
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            SNAPPER_COORDINATOR_INSTANCE_ID="2",
        )
        settings = AppSettings(bootstrap, settings_service=None)
        assert settings.coordinator_instance_id == 2

    def test_instance_id_defaults_to_zero(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Default bootstrap value surfaces as ``0``."""
        monkeypatch.delenv("SNAPPER_COORDINATOR_INSTANCE_ID", raising=False)
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        settings = AppSettings(bootstrap, settings_service=None)
        assert settings.coordinator_instance_id == 0

    def test_instance_count_delegates_to_bootstrap(self) -> None:
        """``coordinator_instance_count`` passes through from bootstrap."""
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            SNAPPER_COORDINATOR_INSTANCE_COUNT="5",
        )
        settings = AppSettings(bootstrap, settings_service=None)
        assert settings.coordinator_instance_count == 5

    def test_instance_count_defaults_to_one(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Default bootstrap value surfaces as ``1``."""
        monkeypatch.delenv("SNAPPER_COORDINATOR_INSTANCE_COUNT", raising=False)
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        settings = AppSettings(bootstrap, settings_service=None)
        assert settings.coordinator_instance_count == 1

    def test_max_scan_rows_parses_numeric_string(self) -> None:
        """Numeric env string is parsed to an int."""
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS="50",
        )
        settings = AppSettings(bootstrap, settings_service=None)
        assert settings.coordinator_outbox_max_scan_rows == 50

    def test_max_scan_rows_default_is_1000(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The bootstrap default ``"1000"`` parses to the int ``1000``."""
        monkeypatch.delenv("SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS", raising=False)
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        settings = AppSettings(bootstrap, settings_service=None)
        assert settings.coordinator_outbox_max_scan_rows == 1000

    def test_max_scan_rows_empty_sentinel_is_none(self) -> None:
        """Empty string disables the cap (``None``)."""
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS="",
        )
        settings = AppSettings(bootstrap, settings_service=None)
        assert settings.coordinator_outbox_max_scan_rows is None

    def test_max_scan_rows_unbounded_sentinel_is_none(self) -> None:
        """``"unbounded"`` disables the cap (``None``)."""
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS="unbounded",
        )
        settings = AppSettings(bootstrap, settings_service=None)
        assert settings.coordinator_outbox_max_scan_rows is None

    def test_max_scan_rows_none_sentinel_is_none(self) -> None:
        """``"none"`` (case-insensitive) disables the cap (``None``)."""
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS="NONE",
        )
        settings = AppSettings(bootstrap, settings_service=None)
        assert settings.coordinator_outbox_max_scan_rows is None

    def test_max_scan_rows_whitespace_wrapped_unbounded(self) -> None:
        """``"  unbounded  "`` with surrounding whitespace → ``None``.

        Regression guard: the accessor documents a trim-then-match
        contract. A future refactor that drops ``.strip()`` would only
        be caught by this case (the non-whitespace cases would still
        pass via raw-literal match).
        """
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS="  unbounded  ",
        )
        settings = AppSettings(bootstrap, settings_service=None)
        assert settings.coordinator_outbox_max_scan_rows is None

    def test_max_scan_rows_whitespace_wrapped_none(self) -> None:
        """``"  NONE  "`` combines case-insensitive + trim contracts."""
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS="  NONE  ",
        )
        settings = AppSettings(bootstrap, settings_service=None)
        assert settings.coordinator_outbox_max_scan_rows is None

    def test_max_scan_rows_non_int_raises(self) -> None:
        """A non-numeric non-sentinel value raises :class:`ValueError`."""
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS="abc",
        )
        settings = AppSettings(bootstrap, settings_service=None)
        with pytest.raises(ValueError, match="must be positive int or 'unbounded'"):
            _ = settings.coordinator_outbox_max_scan_rows

    def test_max_scan_rows_negative_raises(self) -> None:
        """A negative integer string raises :class:`ValueError`."""
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS="-5",
        )
        settings = AppSettings(bootstrap, settings_service=None)
        with pytest.raises(ValueError, match="must be >= 1 or 'unbounded'"):
            _ = settings.coordinator_outbox_max_scan_rows

    def test_max_scan_rows_zero_raises(self) -> None:
        """``"0"`` is non-positive and raises :class:`ValueError`."""
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS="0",
        )
        settings = AppSettings(bootstrap, settings_service=None)
        with pytest.raises(ValueError, match="must be >= 1 or 'unbounded'"):
            _ = settings.coordinator_outbox_max_scan_rows


class TestProcessAutostartProfileProperty:
    """``process_autostart_profile`` delegates straight to bootstrap.

    Does not consult the :class:`SettingsService` — verified by
    constructing ``AppSettings`` with ``settings_service=None``.
    """

    def test_default_is_all(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Default bootstrap value surfaces as ``ALL``."""
        monkeypatch.delenv("PROCESS_AUTOSTART_PROFILE", raising=False)
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        settings = AppSettings(bootstrap, settings_service=None)
        assert settings.process_autostart_profile is ProcessAutostartProfileEnum.ALL

    def test_delegates_api_profile(self) -> None:
        """``api`` env value passes through unchanged."""
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            PROCESS_AUTOSTART_PROFILE="api",
        )
        settings = AppSettings(bootstrap, settings_service=None)
        assert settings.process_autostart_profile is ProcessAutostartProfileEnum.API

    def test_delegates_feed_profile(self) -> None:
        """``feed`` env value passes through unchanged."""
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            PROCESS_AUTOSTART_PROFILE="feed",
        )
        settings = AppSettings(bootstrap, settings_service=None)
        assert settings.process_autostart_profile is ProcessAutostartProfileEnum.FEED


class TestZmqBrokerBindProperties:
    """Broker bind endpoints fall back to connect endpoints when unset."""

    def test_bind_falls_back_to_connect_when_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Empty bind endpoints surface the connect endpoints."""
        monkeypatch.delenv("ZMQ_BROKER_BIND_XSUB", raising=False)
        monkeypatch.delenv("ZMQ_BROKER_BIND_XPUB", raising=False)
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            ZMQ_BROKER_XSUB="tcp://127.0.0.1:7500",
            ZMQ_BROKER_XPUB="tcp://127.0.0.1:7501",
        )
        settings = AppSettings(bootstrap, settings_service=None)
        assert settings.zmq_broker_bind_xsub == "tcp://127.0.0.1:7500"
        assert settings.zmq_broker_bind_xpub == "tcp://127.0.0.1:7501"

    def test_bind_overrides_take_precedence(self) -> None:
        """When set, the bind endpoints are returned verbatim."""
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            ZMQ_BROKER_XSUB="tcp://snapper:7500",
            ZMQ_BROKER_XPUB="tcp://snapper:7501",
            ZMQ_BROKER_BIND_XSUB="tcp://0.0.0.0:7500",
            ZMQ_BROKER_BIND_XPUB="tcp://0.0.0.0:7501",
        )
        settings = AppSettings(bootstrap, settings_service=None)
        assert settings.zmq_broker_bind_xsub == "tcp://0.0.0.0:7500"
        assert settings.zmq_broker_bind_xpub == "tcp://0.0.0.0:7501"
