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
   - API keys for exchanges (Kraken, Polygon, Walutomat)
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
        polygon_key = settings.polygon_api_key  # From encrypted DB

    Wallet-scoped exchange credentials (kraken, walutomat,
    kraken_futures) do NOT live on AppSettings. They are loaded from
    the ``wallet_credentials`` table by ``CredentialResolver`` during
    per-wallet executor startup.
"""

from typing import Any

from snapper.application.services.settings import SettingsService
from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.core.types import ExchangeEnum
from snapper.core.types import ProcessAutostartProfileEnum

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
    def process_autostart_profile(self) -> ProcessAutostartProfileEnum:
        """Return the container autostart profile from bootstrap settings.

        Selects which registered processes this node autostarts so the
        market-data ingest tier can run in its own container off the
        FastAPI event loop. ``ALL`` starts everything, ``API`` skips
        market-data publishers, ``FEED`` starts only them.

        Returns:
            The configured :class:`ProcessAutostartProfileEnum` member.
        """
        return self._bootstrap.process_autostart_profile

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
    def zmq_broker_bind_xsub(self) -> str:
        """Return the interface the broker binds its XSUB socket to.

        Falls back to :attr:`zmq_broker_xsub` (the connectors' endpoint)
        when the dedicated bind endpoint is unset, preserving
        single-container behaviour where bind and connect coincide. A
        cross-container deployment sets ``ZMQ_BROKER_BIND_XSUB`` to a
        routable interface (e.g. ``tcp://0.0.0.0:7500``) because ZMQ
        bind rejects the hostname connectors use.

        Returns:
            The XSUB bind endpoint, or the connect endpoint if unset.
        """
        return self._bootstrap.zmq_broker_bind_xsub or self._bootstrap.zmq_broker_xsub

    @property
    def zmq_broker_bind_xpub(self) -> str:
        """Return the interface the broker binds its XPUB socket to.

        Falls back to :attr:`zmq_broker_xpub` when unset. See
        :attr:`zmq_broker_bind_xsub`.

        Returns:
            The XPUB bind endpoint, or the connect endpoint if unset.
        """
        return self._bootstrap.zmq_broker_bind_xpub or self._bootstrap.zmq_broker_xpub

    @property
    def coordinator_instance_id(self) -> int:
        """Return this coordinator's zero-based instance identifier.

        Sourced from ``SNAPPER_COORDINATOR_INSTANCE_ID`` env var (or
        ``--instance-id`` CLI flag) via :class:`BootstrapSettingsLoader`.
        NOT DB-backed — per-coordinator identity cannot live in shared
        DB settings.

        Returns:
            Zero-based coordinator instance id. Default ``0``.
        """
        return self._bootstrap.coordinator_instance_id

    @property
    def coordinator_instance_count(self) -> int:
        """Return the total number of coordinator instances.

        Sourced from ``SNAPPER_COORDINATOR_INSTANCE_COUNT`` env var (or
        ``--instance-count`` CLI flag). Operators raise this and
        restart all coordinators during a full-cutover scale-up.

        Returns:
            Total coordinator instance count. Default ``1``.
        """
        return self._bootstrap.coordinator_instance_count

    @property
    def coordinator_outbox_max_scan_rows(self) -> int | None:
        """Return the outbox scan cap as parsed ``int | None``.

        The bootstrap field stores the raw env form (``str | None``)
        because pydantic-settings parses env vars as strings. This
        accessor parses it to ``int``, or returns ``None`` when the
        operator sets the sentinel ``"unbounded"`` / ``""`` / ``"none"``
        to disable the cap.

        Returns:
            Positive int scan cap, or ``None`` for unbounded scan.

        Raises:
            ValueError: If the raw value is not parseable as an int, or
                parses to a non-positive value (and is not one of the
                unbounded sentinels).
        """
        raw = self._bootstrap.coordinator_outbox_max_scan_rows
        if raw is None or raw.strip().lower() in {"", "unbounded", "none"}:
            return None
        try:
            value = int(raw)
        except ValueError as exc:
            raise ValueError(
                f"coordinator_outbox_max_scan_rows must be positive int or "
                f"'unbounded', got {raw!r}"
            ) from exc
        if value <= 0:
            raise ValueError(
                f"coordinator_outbox_max_scan_rows must be >= 1 or 'unbounded', got {value}"
            )
        return value

    @property
    def trade_command_dispatch_ttl_s(self) -> float:
        """Return the dispatch max-age TTL for trade commands.

        Gates both the outbox dispatch (stale CREATED submits expire to
        EXPIRED) and the executor submit path (stale frames reject
        before any venue call). ``<= 0`` disables both gates.

        Returns:
            TTL in seconds; values ``<= 0`` mean disabled.
        """
        return self._bootstrap.trade_command_dispatch_ttl_s

    @property
    def telemetry_recording_enabled(self) -> bool:
        """Return whether data-plane telemetry recording is enabled.

        When False (default), telemetry counters still increment but
        rows are not persisted to the telemetry table.

        Returns:
            True if telemetry recording is enabled, False otherwise.
        """
        return self._bootstrap.telemetry_recording_enabled

    @property
    def has_db_access(self) -> bool:
        """Check whether this settings instance has database access.

        Returns:
            True if SettingsService was initialized, False otherwise.
        """
        return self._settings_service is not None

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
    def polygon_api_key(self) -> str:
        """Return Polygon.io API key from database settings.

        Polygon is a shared market-data provider, not a wallet-scoped
        exchange, so the key stays in the ``settings`` table. Per-wallet
        trading credentials (kraken, walutomat, kraken_futures)
        live in ``wallet_credentials``.

        Returns:
            Polygon API key string, empty if not configured.
        """
        return self._get_db_setting("polygon_api_key", "")

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
    def feed_egress_enabled(self) -> bool:
        """Return whether feed publishers route market data through the egress pool.

        Default off: feed publishers connect directly to the exchanges. When
        enabled, each publisher process initializes the egress pool at startup
        so the Kraken connect shim and Walutomat's pooled HTTP transport route
        through the configured WireGuard/SOCKS tunnels (direct stays the
        fallback). Coerced defensively so a string ``"false"`` setting value
        does not read as truthy.

        Returns:
            True when the feed should use the egress pool, else False.
        """
        raw = self._get_db_setting("feed_egress_enabled", False)
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, str):
            return raw.strip().lower() in ("true", "1", "yes", "on")
        return bool(raw)

    @property
    def instruments(self) -> dict[str, list[str]]:
        """Return configured trading instruments per exchange.

        Default for every exchange is the wildcard sentinel ``["*"]``.

        For live-WS publisher exchanges (Kraken spot, Kraken Futures,
        Kraken Equities, Walutomat) each publisher's
        ``_validate_symbols`` expands the wildcard against
        ``get_available_*_symbols()`` at runtime so a fresh DB
        subscribes to the full venue universe without any operator
        configuration. Persistence is decoupled — the
        ``market_persist_*`` settings (seeded as
        ``{"mode":"explicit","exchanges":{}}`` in
        ``proprietary/data/seed/{dev,prod}.toml``) keep every wildcard
        tick out of the DB write path; data flows into the in-process
        ``MarketCacheService`` + ZMQ broadcast and is dropped at the
        publisher's persist gate.

        For ``ExchangeEnum.POLYGON`` (REST-only backfill, no live WS)
        and the three Kraken historical backfill services, each
        ``_resolve_symbols`` recognises ``["*"]`` and delegates to
        ``_get_all_mapped_symbols()`` / ``get_available_*_symbols()`` —
        the same path the ``--all`` CLI flag takes. The setting
        wildcard is therefore semantically equivalent to ``--all``
        across publishers + backfill, so the per-target Makefile
        ``-all`` variants are redundant when wildcard is in settings.

        Operators that want a narrower allowlist (e.g. for a curated
        backfill / strategy universe) override this setting via the
        ``instruments`` DB row — Settings UI or
        ``POST /api/settings`` ``{"key":"instruments", "value": {...}}``.

        FCM index-futures expire quarterly; when narrowing
        ``KRAKEN_EQUITIES`` to an explicit list, operators must rotate
        the entries ~2 weeks before the maturity column in
        ``SymbolExchangeCapability`` reaches ``<= 14 days`` (see
        ``docs/operations.md`` "Kraken Equities (TradFi) market data"
        section for the rotation playbook + monitoring alert).

        Returns:
            Dictionary mapping exchange names to lists of instrument symbols.
        """
        return self._get_db_setting(
            "instruments",
            {
                ExchangeEnum.KRAKEN: ["*"],
                ExchangeEnum.KRAKEN_FUTURES: ["*"],
                ExchangeEnum.KRAKEN_EQUITIES: ["*"],
                ExchangeEnum.WALUTOMAT: ["*"],
                ExchangeEnum.POLYGON: ["*"],
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
    def recon_balance_threshold(self) -> float:
        """Return the absolute threshold for balance mismatch warnings.

        Reconciliation logs a WARNING when the difference between
        exchange-reported balance and expected balance exceeds this
        value (in base currency).

        Returns:
            Threshold in base currency units. Default 1.0.
        """
        return self._get_db_setting("recon_balance_threshold", 1.0)

    def get_setting(self, key: str, default: Any = None) -> Any:
        """Retrieve a setting value from database by key.

        Args:
            key: The setting key to look up.
            default: Value to return if setting is not found.

        Returns:
            The setting value or default if not found.
        """
        return self._get_db_setting(key, default)
