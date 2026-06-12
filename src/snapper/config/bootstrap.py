"""Bootstrap settings loader from environment variables.

This module provides the BootstrapSettingsLoader class which loads
fundamental configuration required before database access is available.
These settings are loaded from environment variables and/or a .env file.

Bootstrap settings include:
    - Database connection URL
    - Master password for settings encryption
    - HTTP server configuration (host, port, reload, API-only mode)
    - Reverse proxy trust configuration (proxy headers, trusted proxy IPs)
    - Process autostart profile
    - ZMQ broker connect and bind endpoints
    - Telemetry recording toggle
    - Coordinator partitioning controls
    - Paired-execution guard and timeout controls
    - Trade-command dispatch TTL

Example:
    Load settings from environment::

        loader = BootstrapSettingsLoader()
        db_url = loader.db_url
        zmq_xpub = loader.zmq_broker_xpub

    Environment variables::

        DB_URL=postgresql+asyncpg://user:pass@host/db
        MASTER_PASSWORD=secure-password
        SERVER_HOST=0.0.0.0
        SERVER_PORT=8000
        SERVER_API_ONLY=false
        PROCESS_AUTOSTART_PROFILE=all
        SERVER_PROXY_HEADERS=true
        SERVER_FORWARDED_ALLOW_IPS=127.0.0.1
        ZMQ_BROKER_XSUB=tcp://127.0.0.1:7500
        ZMQ_BROKER_XPUB=tcp://127.0.0.1:7501
        ZMQ_BROKER_BIND_XSUB=tcp://0.0.0.0:7500
        ZMQ_BROKER_BIND_XPUB=tcp://0.0.0.0:7501
        TELEMETRY_RECORDING_ENABLED=false
        SNAPPER_COORDINATOR_INSTANCE_ID=0
        SNAPPER_COORDINATOR_INSTANCE_COUNT=1
        SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS=1000
        PAIRED_EXECUTION_GUARD_ENABLED=false
        PAIRED_EXECUTION_ASSEMBLY_TIMEOUT_S=5.0
        PAIRED_EXECUTION_FILL_TIMEOUT_S=30.0
        TRADE_COMMAND_DISPATCH_TTL_S=30.0
"""

from typing import Any
from typing import Self

from pydantic import Field
from pydantic import field_validator
from pydantic import model_validator
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict

from snapper.config.env_contract import validate_env_file
from snapper.core.types import ProcessAutostartProfileEnum

__all__ = ["BootstrapSettingsLoader"]


DEFAULT_MASTER_PASSWORD = "snapper_default_master_password_v1"
DEVELOPMENT_ENVIRONMENTS = frozenset({"development", "dev", "test", "testing", "ci"})
PRODUCTION_ENVIRONMENTS = frozenset({"production", "prod", "staging"})
SUPPORTED_ENVIRONMENTS = DEVELOPMENT_ENVIRONMENTS | PRODUCTION_ENVIRONMENTS


class BootstrapSettingsLoader(BaseSettings):
    """Pydantic settings loader for bootstrap configuration.

    Loads configuration from environment variables and .env file.
    These are infrastructure-level settings required before the application
    can connect to the database.

    Attributes:
        db_url: SQLAlchemy async database URL.
        snapper_env: Deployment environment. Development/test-like modes
            allow local defaults; production-like modes fail fast on
            placeholder secrets.
        master_password: Password for encrypting sensitive settings in DB.
        server_host: HTTP server bind address.
        server_port: HTTP server port.
        server_reload: Enable uvicorn auto-reload for development.
        server_api_only: When True, the server starts without launching
            background processes (broker, publishers, strategies, executors).
            The ZMQ-WebSocket bridge still starts so the frontend receives
            live data from a separately-running engine.  Useful for
            production deployments where the engine runs on a different
            host. Multi-uvicorn-worker deployments are supported —
            set ``SNAPPER_COORDINATOR_INSTANCE_ID`` and
            ``SNAPPER_COORDINATOR_INSTANCE_COUNT`` per worker so the
            ``ShardOwnership`` partitioning gates the AI-review
            caps_violation external WS fanout to exactly one worker per
            review row. See ``docs/architecture.md`` Deployment Modes
            for the full per-bus-topic dedup contract.
        process_autostart_profile: Selects which registered processes a
            node autostarts. ``all`` (default) starts everything; ``api``
            starts everything EXCEPT market-data publishers (backend
            container); ``feed`` starts ONLY market-data publishers
            (dedicated feed container). Splits the ingest tier off the
            FastAPI event loop so publishers no longer share CPU with the
            API under NYSE burst. Orthogonal to ``server_api_only`` (which
            skips ALL autostart). See
            :class:`snapper.core.types.ProcessAutostartProfileEnum`.
        server_proxy_headers: Enable parsing proxy headers in uvicorn.
        server_forwarded_allow_ips: Trusted proxy source IP list for
            forwarded headers.
        zmq_broker_xsub: ZMQ XSUB endpoint (publishers connect here).
        zmq_broker_xpub: ZMQ XPUB endpoint (subscribers connect here).
        zmq_broker_bind_xsub: Interface the broker BINDS its XSUB socket
            to. Empty (default) means bind the same endpoint connectors
            use (``zmq_broker_xsub``) — correct for single-container.
            For cross-container the broker must bind a routable
            interface (``tcp://0.0.0.0:7500``) while connectors target
            the broker host by service name (``tcp://snapper:7500``); ZMQ
            bind rejects hostnames, so the two endpoints must differ.
        zmq_broker_bind_xpub: Interface the broker BINDS its XPUB socket
            to. Empty (default) falls back to ``zmq_broker_xpub``. See
            ``zmq_broker_bind_xsub``.
        telemetry_recording_enabled: When True, data-plane messages
            (ping/pong, heartbeat, GET reads) are persisted to the
            telemetry table. Default is False (counters still increment).
        coordinator_instance_id: Zero-based identifier for this
            ``TraderCoordinator`` instance in a multi-instance
            deployment. Default ``0``. Used with
            ``coordinator_instance_count`` by
            ``snapper.core.partitioning.ShardOwnership`` to decide
            static-hash shard ownership.
        coordinator_instance_count: Total number of coordinator
            instances sharing the same DB/broker. Default ``1``
            (single-instance; every shard is owned, partitioning is a
            no-op). Operators raise this and restart all coordinators
            during a full-cutover scale-up.
        coordinator_outbox_max_scan_rows: Raw environment form of the
            cap on how many rows ``OutboxDispatcher._dispatch_batch``
            scans per poll while filtering for owned shards. Stored as
            ``str | None`` because pydantic-settings parses env vars as
            strings and the sentinel values ``""``, ``"unbounded"``,
            and ``"none"`` need to survive round-trip. The parsed
            ``int | None`` form lives on ``AppSettings``.
        paired_execution_guard_enabled: Fail-closed gate for real-money
            multi-leg strategies. Default ``False`` makes a strategy that
            emits a multi-leg signal group on a LIVE (non-paper) exchange
            refuse to publish, because paired EMISSION (Issue A) is not
            venue-execution-atomic on its own — one leg could reject or
            fill late and leave one-sided exposure. Operators flip this to
            ``True`` only once the full paired-execution guard
            (group-armed dispatch + cross-coordinator halt + compensating
            flatten) is deployed. Paper multi-leg is always allowed.
        paired_execution_assembly_timeout_s: Seconds a paired-execution
            group waits for ALL sibling legs to register a durable command
            before it is considered stalled. Each new group's
            ``assembly_deadline = created_at + this``; a group cannot arm
            after its assembly_deadline, and the guard scanner
            (``PairedExecutionGuardScanner``) breaks groups still
            assembling past it — no venue order was sent.
        paired_execution_fill_timeout_s: Seconds an armed paired-execution
            group waits for its legs to fill before the guard treats a
            laggard as a breakage. Sets ``fill_deadline = created_at + this``.
        trade_command_dispatch_ttl_s: Maximum command age before outbox
            dispatch and executor order processing refuse a stale trade
            command. ``<= 0`` disables both gates.
    """

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False, extra="ignore"
    )
    db_url: str = Field(default="sqlite+aiosqlite:///./data/snapper.db", alias="DB_URL")
    snapper_env: str = Field(default="development", alias="SNAPPER_ENV")
    master_password: str = Field(default=DEFAULT_MASTER_PASSWORD, alias="MASTER_PASSWORD")
    server_host: str = Field(default="127.0.0.1", alias="SERVER_HOST")
    server_port: int = Field(default=8000, alias="SERVER_PORT")
    server_reload: bool = Field(default=False, alias="SERVER_RELOAD")
    server_api_only: bool = Field(default=False, alias="SERVER_API_ONLY")
    process_autostart_profile: ProcessAutostartProfileEnum = Field(
        default=ProcessAutostartProfileEnum.ALL, alias="PROCESS_AUTOSTART_PROFILE"
    )
    server_proxy_headers: bool = Field(default=True, alias="SERVER_PROXY_HEADERS")
    server_forwarded_allow_ips: str = Field(default="127.0.0.1", alias="SERVER_FORWARDED_ALLOW_IPS")
    zmq_broker_xsub: str = Field(default="tcp://127.0.0.1:7500", alias="ZMQ_BROKER_XSUB")
    zmq_broker_xpub: str = Field(default="tcp://127.0.0.1:7501", alias="ZMQ_BROKER_XPUB")
    zmq_broker_bind_xsub: str = Field(default="", alias="ZMQ_BROKER_BIND_XSUB")
    zmq_broker_bind_xpub: str = Field(default="", alias="ZMQ_BROKER_BIND_XPUB")
    telemetry_recording_enabled: bool = Field(default=False, alias="TELEMETRY_RECORDING_ENABLED")
    coordinator_instance_id: int = Field(default=0, alias="SNAPPER_COORDINATOR_INSTANCE_ID")
    coordinator_instance_count: int = Field(default=1, alias="SNAPPER_COORDINATOR_INSTANCE_COUNT")
    coordinator_outbox_max_scan_rows: str | None = Field(
        default="1000", alias="SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS"
    )
    paired_execution_guard_enabled: bool = Field(
        default=False, alias="PAIRED_EXECUTION_GUARD_ENABLED"
    )
    paired_execution_assembly_timeout_s: float = Field(
        default=5.0, alias="PAIRED_EXECUTION_ASSEMBLY_TIMEOUT_S"
    )
    paired_execution_fill_timeout_s: float = Field(
        default=30.0, alias="PAIRED_EXECUTION_FILL_TIMEOUT_S"
    )
    trade_command_dispatch_ttl_s: float = Field(default=30.0, alias="TRADE_COMMAND_DISPATCH_TTL_S")
    """Max age of a trade command before dispatch refuses it.

    Gates BOTH the outbox fetch (stale CREATED submits expire to the
    terminal EXPIRED status, never published) and the executor's
    ``_process_order`` (stale frames reject before any venue call), so
    an outage backlog cannot fire MARKET orders priced off old signals.
    Must stay BELOW the engine's 60s in-flight timeout valve minus
    coordinator/executor clock skew (gates compare wall-clock
    ``created_at`` across hosts; NTP assumed) — past 60s the engine may
    re-emit a replacement and a late-executing stale original would
    double exposure. ``<= 0`` disables both gates.
    """

    @model_validator(mode="before")
    @classmethod
    def _validate_env_contract(cls, data: Any) -> Any:
        """Validate the ``.env`` file against the shared allowlist.

        Runs before pydantic field parsing. ``extra='ignore'`` on this
        loader lets subsystem-owned keys (``RETENTION_*``,
        ``SYSTEM_METRICS_*``, ``DB_METRICS_*``) pass through without
        tripping bootstrap parsing, so the typo defence has to live
        one layer up — :mod:`env_contract` enforces the shared
        allowlist and surfaces :mod:`difflib` suggestions on typos.
        """
        validate_env_file()
        return data

    @field_validator("snapper_env")
    @classmethod
    def _normalize_snapper_env(cls, value: str) -> str:
        """Normalize and validate the deployment environment switch."""
        normalized = value.strip().lower()
        if normalized not in SUPPORTED_ENVIRONMENTS:
            supported = ", ".join(sorted(SUPPORTED_ENVIRONMENTS))
            raise ValueError(f"SNAPPER_ENV must be one of {supported}, got {value!r}")
        return normalized

    @model_validator(mode="after")
    def _validate_production_secret_defaults(self) -> Self:
        """Refuse bootstrap placeholder secrets in production-like modes."""
        if self.requires_explicit_secrets and self.master_password == DEFAULT_MASTER_PASSWORD:
            raise ValueError(
                "MASTER_PASSWORD must be explicitly set when SNAPPER_ENV is "
                f"{self.snapper_env!r}; refusing the development placeholder"
            )
        return self

    @property
    def requires_explicit_secrets(self) -> bool:
        """Return whether this environment must reject placeholder secrets.

        Returns:
            True when placeholder secrets are refused.
        """
        return self.snapper_env not in DEVELOPMENT_ENVIRONMENTS
