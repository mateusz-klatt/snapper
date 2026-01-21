"""Unit tests for AppSettings configuration facade."""

from typing import Any

import pytest

from snapper.config.app import AppSettings
from snapper.config.bootstrap import BootstrapSettingsLoader


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
    When accessing kraken_api_key,
    Then RuntimeError is raised with appropriate message.
    """
    bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
    settings = AppSettings(bootstrap, settings_service=None)
    with pytest.raises(RuntimeError, match="SettingsService not initialized"):
        _ = settings.kraken_api_key


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


class TestAppSettingsCredentialProperties:
    """Tests for AppSettings API credential property accessors."""

    def test_kraken_api_key_returns_value(self) -> None:
        """Verify kraken_api_key returns configured value.

        Given service with kraken_api_key set,
        When accessing settings.kraken_api_key,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"kraken_api_key": "test-kraken-key"})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.kraken_api_key == "test-kraken-key"

    def test_kraken_api_secret_returns_value(self) -> None:
        """Verify kraken_api_secret returns configured value.

        Given service with kraken_api_secret set,
        When accessing settings.kraken_api_secret,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"kraken_api_secret": "test-secret"})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.kraken_api_secret == "test-secret"

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

    def test_walutomat_api_key_returns_value(self) -> None:
        """Verify walutomat_api_key returns configured value.

        Given service with walutomat_api_key set,
        When accessing settings.walutomat_api_key,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"walutomat_api_key": "walut-key"})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.walutomat_api_key == "walut-key"

    def test_walutomat_private_key_returns_value(self) -> None:
        """Verify walutomat_private_key returns configured value.

        Given service with walutomat_private_key set,
        When accessing settings.walutomat_private_key,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"walutomat_private_key": "private-key"})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.walutomat_private_key == "private-key"

    def test_zonda_api_key_returns_value(self) -> None:
        """Verify zonda_api_key returns configured value.

        Given service with zonda_api_key set,
        When accessing settings.zonda_api_key,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"zonda_api_key": "zonda-key"})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.zonda_api_key == "zonda-key"

    def test_zonda_api_secret_returns_value(self) -> None:
        """Verify zonda_api_secret returns configured value.

        Given service with zonda_api_secret set,
        When accessing settings.zonda_api_secret,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"zonda_api_secret": "zonda-secret"})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.zonda_api_secret == "zonda-secret"


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
        custom_instruments = {"kraken": ["BTC-USD"], "zonda": ["BTC-PLN"]}
        service = MockSettingsService({"instruments": custom_instruments})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.instruments == custom_instruments

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
        assert settings.risk_max_leverage == 2.0

    def test_risk_max_drawdown_returns_value(self) -> None:
        """Verify risk_max_drawdown returns configured value.

        Given service with risk_max_drawdown set,
        When accessing settings.risk_max_drawdown,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"risk_max_drawdown": 0.20})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.risk_max_drawdown == 0.20

    def test_risk_r_per_trade_returns_value(self) -> None:
        """Verify risk_r_per_trade returns configured value.

        Given service with risk_r_per_trade set,
        When accessing settings.risk_r_per_trade,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"risk_r_per_trade": 0.01})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.risk_r_per_trade == 0.01


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
        assert settings.rest_retry_backoff_base == 0.5

    def test_rest_retry_max_delay_returns_value(self) -> None:
        """Verify rest_retry_max_delay returns configured value.

        Given service with rest_retry_max_delay set,
        When accessing settings.rest_retry_max_delay,
        Then configured value is returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"rest_retry_max_delay": 5.0})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.rest_retry_max_delay == 5.0

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
