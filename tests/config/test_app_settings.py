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


def test_bootstrap_accessors_return_values() -> None:
    """Bootstrap-backed accessors forward values from the loader.

    Given: Bootstrap settings with non-default server and ZMQ values,
    When: AppSettings reads bootstrap-backed properties,
    Then: The facade returns the configured loader values.
    """
    bootstrap = BootstrapSettingsLoader(
        DB_URL="sqlite:///:memory:",
        SERVER_HOST="0.0.0.0",
        SERVER_PORT=9001,
        SERVER_API_ONLY=True,
        ZMQ_BROKER_XSUB="tcp://127.0.0.1:7600",
        ZMQ_BROKER_XPUB="tcp://127.0.0.1:7601",
        TELEMETRY_RECORDING_ENABLED=True,
    )
    settings = AppSettings(bootstrap, settings_service=MockSettingsService({}))
    assert settings.db_url == "sqlite:///:memory:"
    assert settings.server_host == "0.0.0.0"
    assert settings.server_port == 9001
    assert settings.server_api_only is True
    assert settings.zmq_broker_xsub == "tcp://127.0.0.1:7600"
    assert settings.zmq_broker_xpub == "tcp://127.0.0.1:7601"
    assert settings.telemetry_recording_enabled is True


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


class TestAppSettingsWriteBufferBatchSizes:
    """Tests for the publisher write-buffer batch-size and flush-age getters."""

    def test_write_buffer_getters_return_defaults_when_absent(self) -> None:
        """Verify the write-buffer getters return their documented defaults.

        Given: AppSettings backed by an empty settings service,
        When: reading the candle/tick/trade batch-size and flush-age getters,
        Then: each returns its default (50ms flush, 100 candle, 500 tick, 500 trade).
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        settings = AppSettings(bootstrap, settings_service=MockSettingsService({}))
        assert settings.write_buffer_flush_ms == 50
        assert settings.write_buffer_candle_max_rows == 100
        assert settings.write_buffer_tick_max_rows == 500
        assert settings.write_buffer_trade_max_rows == 500


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


class TestAppSettingsKrakenEquitiesRealtimeWs:
    """Tests for the Kraken Equities realtime WS feature gate."""

    def test_defaults_off_when_absent(self) -> None:
        """Absent settings keep Kraken Equities on the public delayed feed."""
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        settings = AppSettings(bootstrap, settings_service=MockSettingsService({}))
        assert settings.kraken_equities_realtime_ws_enabled is False
        assert settings.kraken_equities_realtime_wallet_public_id == ""

    def test_true_bool_enables(self) -> None:
        """A boolean True enables the authenticated realtime feed."""
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"kraken_equities_realtime_ws_enabled": True})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.kraken_equities_realtime_ws_enabled is True

    def test_truthy_string_enables(self) -> None:
        """A truthy string enables the authenticated realtime feed."""
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"kraken_equities_realtime_ws_enabled": "on"})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.kraken_equities_realtime_ws_enabled is True

    def test_false_string_does_not_enable(self) -> None:
        """A false string keeps the authenticated realtime feed disabled."""
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"kraken_equities_realtime_ws_enabled": "false"})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.kraken_equities_realtime_ws_enabled is False

    def test_nonstring_nonbool_coerced(self) -> None:
        """A non-string value is coerced the same way as feed egress."""
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"kraken_equities_realtime_ws_enabled": 1})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.kraken_equities_realtime_ws_enabled is True

    def test_wallet_public_id_returns_value(self) -> None:
        """The realtime token wallet id is DB-backed and defaults separately."""
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        service = MockSettingsService({"kraken_equities_realtime_wallet_public_id": "wallet-1"})
        settings = AppSettings(bootstrap, settings_service=service)
        assert settings.kraken_equities_realtime_wallet_public_id == "wallet-1"


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

    def test_auth_secret_key_production_rejects_missing_value(self) -> None:
        """Production mode refuses the development JWT placeholder."""
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            SNAPPER_ENV="production",
            MASTER_PASSWORD="custom-master-password",
        )
        service = MockSettingsService({})
        settings = AppSettings(bootstrap, settings_service=service)
        with pytest.raises(RuntimeError, match="auth_secret_key"):
            _ = settings.auth_secret_key

    def test_auth_secret_key_production_rejects_placeholder_value(self) -> None:
        """Production mode refuses an explicitly stored JWT placeholder."""
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            SNAPPER_ENV="production",
            MASTER_PASSWORD="custom-master-password",
        )
        service = MockSettingsService(
            {"auth_secret_key": "change-me-in-production-use-openssl-rand-hex-32"}
        )
        settings = AppSettings(bootstrap, settings_service=service)
        with pytest.raises(RuntimeError, match="auth_secret_key"):
            _ = settings.auth_secret_key

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

    def test_csrf_secret_key_production_rejects_missing_value(self) -> None:
        """Production mode refuses the development CSRF placeholder."""
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            SNAPPER_ENV="production",
            MASTER_PASSWORD="custom-master-password",
        )
        service = MockSettingsService({})
        settings = AppSettings(bootstrap, settings_service=service)
        with pytest.raises(RuntimeError, match="csrf_secret_key"):
            _ = settings.csrf_secret_key

    def test_master_password_returns_bootstrap_value(self) -> None:
        """The facade exposes the validated bootstrap master password."""
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            MASTER_PASSWORD="custom-master-password",
        )
        settings = AppSettings(bootstrap, settings_service=MockSettingsService({}))
        assert settings.master_password == "custom-master-password"

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

    def test_candle_forward_fill_defaults_false_and_reads_value(self) -> None:
        """Verify candle_forward_fill defaults to False and reads the DB value.

        Given a service without and with candle_forward_fill set,
        When accessing settings.candle_forward_fill,
        Then the default False and the configured value are returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        default = AppSettings(bootstrap, settings_service=MockSettingsService({}))
        assert default.candle_forward_fill is False
        enabled = AppSettings(
            bootstrap, settings_service=MockSettingsService({"candle_forward_fill": True})
        )
        assert enabled.candle_forward_fill is True

    def test_persist_intermediate_candles_defaults_false_and_coerces(self) -> None:
        """Verify persist_intermediate_candles defaults False and coerces values.

        Given a service with the setting absent, as a bool, as truthy/falsey
            strings, and as an int,
        When accessing settings.persist_intermediate_candles,
        Then it defaults False and coerces each representation correctly.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        default = AppSettings(bootstrap, settings_service=MockSettingsService({}))
        assert default.persist_intermediate_candles is False

        def _value(raw: object) -> bool:
            settings = AppSettings(
                bootstrap,
                settings_service=MockSettingsService({"persist_intermediate_candles": raw}),
            )
            return settings.persist_intermediate_candles

        assert _value(True) is True
        assert _value("on") is True
        assert _value("false") is False
        assert _value(1) is True
        assert _value(0) is False

    def test_spot_trade_built_shadow_enabled_defaults_false_and_coerces(self) -> None:
        """Verify spot_trade_built_shadow_enabled defaults False and coerces values.

        Given a service with the setting absent, as a bool, as truthy/falsey
            strings, and as an int,
        When accessing settings.spot_trade_built_shadow_enabled,
        Then it defaults False and coerces each representation correctly.

        Returns:
            None.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        default = AppSettings(bootstrap, settings_service=MockSettingsService({}))
        assert default.spot_trade_built_shadow_enabled is False

        def _value(raw: object) -> bool:
            """Read the shadow setting for one raw service value.

            Args:
                raw: Raw settings-service value.

            Returns:
                Coerced boolean property value.
            """
            settings = AppSettings(
                bootstrap,
                settings_service=MockSettingsService({"spot_trade_built_shadow_enabled": raw}),
            )
            return settings.spot_trade_built_shadow_enabled

        assert _value(True) is True
        assert _value("yes") is True
        assert _value("false") is False
        assert _value(1) is True
        assert _value(0) is False

    def test_spot_candle_source_defaults_native_and_only_accepts_trade_built(self) -> None:
        """Verify the Kraken Spot live candle source is opt-in.

        Given a service with the setting absent, explicitly trade_built, and
            several fallback values,
        When accessing settings.spot_candle_source,
        Then only the normalized trade_built string switches the source.

        Returns:
            None.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        default = AppSettings(bootstrap, settings_service=MockSettingsService({}))
        assert default.spot_candle_source == "native"

        def _value(raw: object) -> str:
            """Read the Spot candle source for one raw service value.

            Args:
                raw: Raw settings-service value.

            Returns:
                Normalized Spot candle source.
            """
            settings = AppSettings(
                bootstrap,
                settings_service=MockSettingsService({"spot_candle_source": raw}),
            )
            return settings.spot_candle_source

        assert _value("trade_built") == "trade_built"
        assert _value(" TRADE_BUILT ") == "trade_built"
        assert _value("native") == "native"
        assert _value("trades") == "native"
        assert _value(True) == "native"

    def test_trade_built_finalize_grace_seconds_defaults_and_coerces(self) -> None:
        """Verify the trade-built finalization grace setting parses seconds.

        Given a service with the setting absent, numeric, string, and negative,
        When accessing settings.trade_built_finalize_grace_seconds,
        Then it defaults to twelve seconds and clamps negative values to zero.

        Returns:
            None.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        default = AppSettings(bootstrap, settings_service=MockSettingsService({}))
        assert default.trade_built_finalize_grace_seconds == 12

        def _value(raw: object) -> int:
            """Read the trade-built grace for one raw service value.

            Args:
                raw: Raw settings-service value.

            Returns:
                Parsed non-negative grace seconds.
            """
            settings = AppSettings(
                bootstrap,
                settings_service=MockSettingsService({"trade_built_finalize_grace_seconds": raw}),
            )
            return settings.trade_built_finalize_grace_seconds

        assert _value(20) == 20
        assert _value("7") == 7
        assert _value(-4) == 0

    def test_candle_single_source_defaults_false_and_reads_value(self) -> None:
        """Verify candle_single_source defaults to False and reads the DB value.

        Given a service without and with candle_single_source set,
        When accessing settings.candle_single_source,
        Then the default False and the configured value are returned.
        """
        bootstrap = BootstrapSettingsLoader(DB_URL="sqlite:///:memory:")
        default = AppSettings(bootstrap, settings_service=MockSettingsService({}))
        assert default.candle_single_source is False
        enabled = AppSettings(
            bootstrap, settings_service=MockSettingsService({"candle_single_source": True})
        )
        assert enabled.candle_single_source is True

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

    def test_container_split_flags_pass_through(self) -> None:
        """The embedded/extra-package split flags pass through bootstrap."""
        bootstrap = BootstrapSettingsLoader(
            DB_URL="sqlite:///:memory:",
            ZMQ_BROKER_EMBEDDED=False,
            STRATEGIES_EMBEDDED=False,
            STRATEGY_EXTRA_PACKAGES="strategies,extras",
        )
        settings = AppSettings(bootstrap, settings_service=None)
        assert settings.zmq_broker_embedded is False
        assert settings.strategies_embedded is False
        assert settings.strategy_extra_packages == "strategies,extras"


def test_trade_command_dispatch_ttl_passthrough() -> None:
    """The dispatch TTL passes through from bootstrap untouched.

    Given AppSettings over a bootstrap with an explicit TTL,
    When reading trade_command_dispatch_ttl_s,
    Then the bootstrap float is returned as-is.
    """
    bootstrap = BootstrapSettingsLoader(
        DB_URL="sqlite:///:memory:", TRADE_COMMAND_DISPATCH_TTL_S=17.0
    )
    settings = AppSettings(bootstrap, settings_service=None)
    assert settings.trade_command_dispatch_ttl_s == pytest.approx(17.0)
