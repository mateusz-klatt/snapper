"""Application settings facade with two-tier configuration.

This module provides the AppSettings class which unifies configuration from
two sources:

1. **Bootstrap settings** - Environment variables and .env file (via BootstrapSettingsLoader)
   - Database connection URL
   - Master password and encryption salt
   - Server host/port configuration
   - Reverse proxy trust configuration
   - ZMQ broker endpoints

2. **Database settings** - Runtime configuration stored in DB (via SettingsService)
   - API keys for exchanges (Kraken, Polygon, Walutomat, Zonda)
   - Trading parameters (instruments, timeframes, risk limits)
   - Authentication settings (token expiry, CSRF configuration)

The two-tier approach allows safe storage of secrets in DB with encryption,
while keeping infrastructure config in environment variables.

Example:
    Basic usage (bootstrap only)::

        settings = get_settings()
        db_url = settings.db_url  # From env/bootstrap

    With database access::

        settings_service = await get_settings_service(db_url, ...)
        settings = get_settings_with_service(settings_service)
        api_key = settings.kraken_api_key  # From encrypted DB
"""

from typing import Any

from snapper.application.services.settings import SettingsService
from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.core.types import ExchangeEnum

__all__ = ["AppSettings"]


class AppSettings:
    """Unified application settings facade.

    Provides a single interface to access configuration from both bootstrap
    (environment) and database sources. Database settings are accessed through
    an optional SettingsService which provides caching and ZMQ synchronization.

    Attributes:
        _bootstrap: Bootstrap settings from environment variables.
        _settings_service: Optional service for database-backed settings.
    """

    def __init__(
        self,
        bootstrap_settings: BootstrapSettingsLoader,
        settings_service: SettingsService | None = None,
    ) -> None:
        """Initialize AppSettings with bootstrap and optional database access.

        Args:
            bootstrap_settings: Loader for environment-based configuration.
            settings_service: Optional service for database settings. If None,
                attempting to access database-backed settings will raise RuntimeError.
        """
        self._bootstrap = bootstrap_settings
        self._settings_service = settings_service

    @property
    def db_url(self) -> str:
        """Return database connection URL from bootstrap settings.

        Returns:
            Database connection URL string.
        """
        return self._bootstrap.db_url

    @property
    def server_host(self) -> str:
        """Return server host address from bootstrap settings.

        Returns:
            Server host address string.
        """
        return self._bootstrap.server_host

    @property
    def server_port(self) -> int:
        """Return server port number from bootstrap settings.

        Returns:
            Server port number.
        """
        return self._bootstrap.server_port

    @property
    def server_reload(self) -> bool:
        """Return whether server auto-reload is enabled.

        Returns:
            True if auto-reload is enabled, False otherwise.
        """
        return self._bootstrap.server_reload

    @property
    def server_api_only(self) -> bool:
        """Return whether the server should skip engine autostart.

        When True the lifespan does not call ``start_all_processes()``.
        The ZMQ-WebSocket bridge still starts so the frontend receives
        live data from a separately-running engine.

        Returns:
            True if API-only mode is enabled, False otherwise.
        """
        return self._bootstrap.server_api_only

    @property
    def server_proxy_headers(self) -> bool:
        """Return whether uvicorn should parse proxy headers.

        Returns:
            True if proxy header parsing is enabled, False otherwise.
        """
        return self._bootstrap.server_proxy_headers

    @property
    def server_forwarded_allow_ips(self) -> str:
        """Return trusted proxy source IP list for forwarded headers.

        Returns:
            Comma-separated trusted proxy IP addresses or CIDRs.
        """
        return self._bootstrap.server_forwarded_allow_ips

    @property
    def zmq_broker_xsub(self) -> str:
        """Return ZMQ broker XSUB endpoint from bootstrap settings.

        Returns:
            ZMQ XSUB endpoint URL.
        """
        return self._bootstrap.zmq_broker_xsub

    @property
    def zmq_broker_xpub(self) -> str:
        """Return ZMQ broker XPUB endpoint from bootstrap settings.

        Returns:
            ZMQ XPUB endpoint URL.
        """
        return self._bootstrap.zmq_broker_xpub

    @property
    def telemetry_recording_enabled(self) -> bool:
        """Return whether data-plane telemetry recording is enabled.

        When False (default), telemetry counters still increment but
        rows are not persisted to the telemetry table.

        Returns:
            True if telemetry recording is enabled, False otherwise.
        """
        return self._bootstrap.telemetry_recording_enabled

    def _get_db_setting[T](self, key: str, default: T) -> T:
        """Retrieve a setting value from database with fallback to default.

        Args:
            key: The setting key to look up in the database.
            default: Value to return if setting is not found or is None.

        Returns:
            The setting value from database, or default if not found.

        Raises:
            RuntimeError: If SettingsService was not initialized.
        """
        if self._settings_service is None:
            raise RuntimeError(
                f"Cannot access database setting '{key}' - SettingsService not initialized. "
                f"This AppSettings instance was created without database access "
                f"(via get_settings()). "
                f"Use get_settings_with_service(service) instead, or ensure your component "
                f"has called start()/initialize() to set up database access."
            )
        result = self._settings_service.get_setting(key, default)
        return result if result is not None else default

    @property
    def kraken_api_key(self) -> str:
        """Return Kraken exchange API key from database settings.

        Returns:
            Kraken API key string, empty if not configured.
        """
        return self._get_db_setting("kraken_api_key", "")

    @property
    def kraken_api_secret(self) -> str:
        """Return Kraken exchange API secret from database settings.

        Returns:
            Kraken API secret string, empty if not configured.
        """
        return self._get_db_setting("kraken_api_secret", "")

    @property
    def kraken_futures_api_key(self) -> str:
        """Return Kraken Futures API key from database settings.

        Returns:
            Kraken Futures API key string, empty if not configured.
        """
        return self._get_db_setting("kraken_futures_api_key", "")

    @property
    def kraken_futures_api_secret(self) -> str:
        """Return Kraken Futures API secret from database settings.

        Returns:
            Kraken Futures API secret string, empty if not configured.
        """
        return self._get_db_setting("kraken_futures_api_secret", "")

    @property
    def polygon_api_key(self) -> str:
        """Return Polygon.io API key from database settings.

        Returns:
            Polygon API key string, empty if not configured.
        """
        return self._get_db_setting("polygon_api_key", "")

    @property
    def walutomat_api_key(self) -> str:
        """Return Walutomat API key from database settings.

        Returns:
            Walutomat API key string, empty if not configured.
        """
        return self._get_db_setting("walutomat_api_key", "")

    @property
    def walutomat_private_key(self) -> str:
        """Return Walutomat private key from database settings.

        Returns:
            Walutomat private key string, empty if not configured.
        """
        return self._get_db_setting("walutomat_private_key", "")

    @property
    def zonda_api_key(self) -> str:
        """Return Zonda exchange API key from database settings.

        Returns:
            Zonda API key string, empty if not configured.
        """
        return self._get_db_setting("zonda_api_key", "")

    @property
    def zonda_api_secret(self) -> str:
        """Return Zonda exchange API secret from database settings.

        Returns:
            Zonda API secret string, empty if not configured.
        """
        return self._get_db_setting("zonda_api_secret", "")

    @property
    def auth_secret_key(self) -> str:
        """Return JWT authentication secret key from database settings.

        Returns:
            JWT secret key string for token signing.
        """
        return self._get_db_setting(
            "auth_secret_key", "change-me-in-production-use-openssl-rand-hex-32"
        )

    @property
    def csrf_secret_key(self) -> str:
        """Return CSRF protection secret key from database settings.

        Returns:
            CSRF secret key string for token generation.
        """
        return self._get_db_setting("csrf_secret_key", "change-me-in-production-csrf-key")

    @property
    def ws_token_ttl_seconds(self) -> int:
        """Return WebSocket token time-to-live in seconds.

        Returns:
            Token TTL in seconds.
        """
        return self._get_db_setting("ws_token_ttl_seconds", 900)

    @property
    def instruments(self) -> dict[str, list[str]]:
        """Return configured trading instruments per exchange.

        Returns:
            Dictionary mapping exchange names to lists of instrument symbols.
        """
        return self._get_db_setting(
            "instruments",
            {
                ExchangeEnum.KRAKEN: [
                    "BTC-USD",
                    "BTC-EUR",
                    "BTC-USDC",
                    "BTC-EURC",
                    "EUR-USD",
                    "ETH-USD",
                    "SPY",
                ],
                ExchangeEnum.KRAKEN_FUTURES: [],
                ExchangeEnum.ZONDA: ["BTC-PLN", "USDC-PLN", "ETH-PLN", "SOL-USDC", "BTC-EUR"],
                ExchangeEnum.WALUTOMAT: ["EUR-PLN", "USD-PLN", "EUR-USD"],
                ExchangeEnum.POLYGON: ["BTC-USD", "BTC-EUR", "EUR-USD", "EUR-PLN", "USD-PLN"],
            },
        )

    @property
    def paper_instruments(self) -> dict[str, list[str]]:
        """Return configured market data sources for paper replay.

        Returns:
            Dictionary mapping source exchanges to lists of instrument symbols.
        """
        return self._get_db_setting(
            "paper_instruments",
            {
                ExchangeEnum.KRAKEN: ["BTC-USD", "EUR-USD"],
                ExchangeEnum.KRAKEN_FUTURES: [],
                ExchangeEnum.ZONDA: ["BTC-PLN"],
                ExchangeEnum.WALUTOMAT: ["EUR-PLN", "USD-PLN"],
            },
        )

    @property
    def timeframes(self) -> list[str]:
        """Return configured trading timeframes.

        Returns:
            List of timeframe strings.
        """
        return self._get_db_setting("timeframes", ["1m"])

    @property
    def backfill_days(self) -> int:
        """Return number of days for historical data backfill.

        Returns:
            Number of days to backfill.
        """
        return self._get_db_setting("backfill_days", 30)

    @property
    def risk_max_leverage(self) -> float:
        """Return maximum allowed trading leverage.

        Returns:
            Maximum leverage multiplier.
        """
        return self._get_db_setting("risk_max_leverage", 1.0)

    @property
    def risk_max_drawdown(self) -> float:
        """Return maximum allowed portfolio drawdown as decimal fraction.

        Returns:
            Maximum drawdown as decimal (e.g., 0.15 for 15%).
        """
        return self._get_db_setting("risk_max_drawdown", 0.15)

    @property
    def risk_r_per_trade(self) -> float:
        """Return risk per trade as decimal fraction of portfolio.

        Returns:
            Risk per trade as decimal (e.g., 0.005 for 0.5%).
        """
        return self._get_db_setting("risk_r_per_trade", 0.005)

    @property
    def log_level(self) -> str:
        """Return application logging level.

        Returns:
            Logging level string (e.g., INFO, DEBUG).
        """
        return self._get_db_setting("log_level", "INFO")

    @property
    def log_json(self) -> bool:
        """Return whether JSON logging format is enabled.

        Returns:
            True if JSON logging enabled, False otherwise.
        """
        return self._get_db_setting("log_json", False)

    @property
    def ws_reconnect_max_delay(self) -> int:
        """Return maximum WebSocket reconnection delay in seconds.

        Returns:
            Maximum reconnection delay in seconds.
        """
        return self._get_db_setting("ws_reconnect_max_delay", 10)

    @property
    def rest_retry_max_attempts(self) -> int:
        """Return maximum REST API retry attempts.

        Returns:
            Maximum number of retry attempts.
        """
        return self._get_db_setting("rest_retry_max_attempts", 5)

    @property
    def rest_retry_backoff_base(self) -> float:
        """Return REST API retry backoff base delay in seconds.

        Returns:
            Base delay in seconds for exponential backoff.
        """
        return self._get_db_setting("rest_retry_backoff_base", 0.2)

    @property
    def rest_retry_max_delay(self) -> float:
        """Return maximum REST API retry delay in seconds.

        Returns:
            Maximum retry delay in seconds.
        """
        return self._get_db_setting("rest_retry_max_delay", 2.0)

    @property
    def rest_circuit_failure_threshold(self) -> int:
        """Return circuit breaker failure threshold count.

        Returns:
            Number of failures before circuit opens.
        """
        return self._get_db_setting("rest_circuit_failure_threshold", 5)

    @property
    def rest_circuit_reset_timeout(self) -> int:
        """Return circuit breaker reset timeout in seconds.

        Returns:
            Timeout in seconds before circuit attempts reset.
        """
        return self._get_db_setting("rest_circuit_reset_timeout", 30)

    @property
    def zmq_heartbeat_interval_ms(self) -> int:
        """Return ZMQ heartbeat interval in milliseconds.

        Returns:
            Heartbeat interval in milliseconds.
        """
        return self._get_db_setting("zmq_heartbeat_interval_ms", 1000)

    @property
    def write_buffer_flush_ms(self) -> int:
        """Return flush age threshold for publisher micro-batch in milliseconds.

        Batched DB writes are flushed when the oldest item exceeds this age.
        Not hot-reloaded; publisher caches at start, requires restart to change.

        Returns:
            Flush age threshold in milliseconds.
        """
        return self._get_db_setting("write_buffer_flush_ms", 50)

    @property
    def write_buffer_candle_max_rows(self) -> int:
        """Return candle batch size trigger for publisher micro-batch.

        Returns:
            Maximum candle rows before flush is triggered.
        """
        return self._get_db_setting("write_buffer_candle_max_rows", 100)

    @property
    def write_buffer_tick_max_rows(self) -> int:
        """Return tick batch size trigger for publisher micro-batch.

        Returns:
            Maximum tick rows before flush is triggered.
        """
        return self._get_db_setting("write_buffer_tick_max_rows", 500)

    @property
    def write_buffer_trade_max_rows(self) -> int:
        """Return trade batch size trigger for publisher micro-batch.

        Returns:
            Maximum trade rows before flush is triggered.
        """
        return self._get_db_setting("write_buffer_trade_max_rows", 500)

    @property
    def auth_algorithm(self) -> str:
        """Return JWT signing algorithm.

        Returns:
            Algorithm name string (e.g., HS256).
        """
        return self._get_db_setting("auth_algorithm", "HS256")

    @property
    def auth_access_token_expire_minutes(self) -> int:
        """Return access token expiration time in minutes.

        Returns:
            Token expiration time in minutes.
        """
        return self._get_db_setting("auth_access_token_expire_minutes", 15)

    @property
    def auth_refresh_token_expire_days(self) -> int:
        """Return refresh token expiration time in days.

        Returns:
            Token expiration time in days.
        """
        return self._get_db_setting("auth_refresh_token_expire_days", 7)

    @property
    def auth_refresh_token_expire_days_extended(self) -> int:
        """Return extended refresh token expiration time in days.

        Returns:
            Extended token expiration time in days.
        """
        return self._get_db_setting("auth_refresh_token_expire_days_extended", 30)

    @property
    def csrf_token_expire_minutes(self) -> int:
        """Return CSRF token expiration time in minutes.

        Returns:
            Token expiration time in minutes.
        """
        return self._get_db_setting("csrf_token_expire_minutes", 60)

    @property
    def session_secure(self) -> bool:
        """Return whether session cookies require HTTPS.

        Returns:
            True if HTTPS required, False otherwise.
        """
        return self._get_db_setting("session_secure", False)

    @property
    def ui_origin(self) -> str:
        """Return allowed UI origin for CORS configuration.

        Returns:
            Allowed origin URL string, empty if not configured.
        """
        return self._get_db_setting("ui_origin", "")

    @property
    def session_same_site(self) -> str:
        """Return session cookie SameSite attribute value.

        Returns:
            SameSite attribute value (strict, lax, or none).
        """
        return self._get_db_setting("session_same_site", "lax")

    @property
    def session_domain(self) -> str:
        """Return session cookie domain.

        Returns:
            Cookie domain string, empty if not configured.
        """
        return self._get_db_setting("session_domain", "")

    @property
    def use_durable_commands(self) -> bool:
        """Return whether durable command mode is enabled.

        When True, engine delegates order publishing to the outbox
        dispatcher and VenueEvent writes are fail-closed.

        Returns:
            True if durable mode enabled, False (default) for dual-write.
        """
        return self._get_db_setting("use_durable_commands", False)

    def get_setting(self, key: str, default: Any = None) -> Any:
        """Retrieve a setting value from database by key.

        Args:
            key: The setting key to look up.
            default: Value to return if setting is not found.

        Returns:
            The setting value or default if not found.
        """
        return self._get_db_setting(key, default)
