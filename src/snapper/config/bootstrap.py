"""Bootstrap settings loader from environment variables.

This module provides the BootstrapSettingsLoader class which loads
fundamental configuration required before database access is available.
These settings are loaded from environment variables and/or a .env file.

Bootstrap settings include:
    - Database connection URL
    - Master password for settings encryption
    - HTTP server configuration (host, port, reload)
    - Reverse proxy trust configuration (proxy headers, trusted proxy IPs)
    - ZMQ broker endpoints

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
        SERVER_PROXY_HEADERS=true
        SERVER_FORWARDED_ALLOW_IPS=127.0.0.1
        ZMQ_BROKER_XSUB=tcp://127.0.0.1:7500
        ZMQ_BROKER_XPUB=tcp://127.0.0.1:7501
"""

from pydantic import Field
from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict

__all__ = ["BootstrapSettingsLoader"]


class BootstrapSettingsLoader(BaseSettings):
    """Pydantic settings loader for bootstrap configuration.

    Loads configuration from environment variables and .env file.
    These are infrastructure-level settings required before the application
    can connect to the database.

    Attributes:
        db_url: SQLAlchemy async database URL.
        master_password: Password for encrypting sensitive settings in DB.
        server_host: HTTP server bind address.
        server_port: HTTP server port.
        server_reload: Enable uvicorn auto-reload for development.
        server_api_only: When True, the server starts without launching
            background processes (broker, publishers, strategies, executors).
            The ZMQ-WebSocket bridge still starts so the frontend receives
            live data from a separately-running engine.  Useful for
            production deployments or multiple uvicorn workers.
        server_proxy_headers: Enable parsing proxy headers in uvicorn.
        server_forwarded_allow_ips: Trusted proxy source IP list for
            forwarded headers.
        zmq_broker_xsub: ZMQ XSUB endpoint (publishers connect here).
        zmq_broker_xpub: ZMQ XPUB endpoint (subscribers connect here).
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
    """

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", case_sensitive=False
    )
    db_url: str = Field(default="sqlite+aiosqlite:///./data/snapper.db", alias="DB_URL")
    master_password: str = Field(
        default="snapper_default_master_password_v1", alias="MASTER_PASSWORD"
    )
    server_host: str = Field(default="127.0.0.1", alias="SERVER_HOST")
    server_port: int = Field(default=8000, alias="SERVER_PORT")
    server_reload: bool = Field(default=False, alias="SERVER_RELOAD")
    server_api_only: bool = Field(default=False, alias="SERVER_API_ONLY")
    server_proxy_headers: bool = Field(default=True, alias="SERVER_PROXY_HEADERS")
    server_forwarded_allow_ips: str = Field(default="127.0.0.1", alias="SERVER_FORWARDED_ALLOW_IPS")
    zmq_broker_xsub: str = Field(default="tcp://127.0.0.1:7500", alias="ZMQ_BROKER_XSUB")
    zmq_broker_xpub: str = Field(default="tcp://127.0.0.1:7501", alias="ZMQ_BROKER_XPUB")
    telemetry_recording_enabled: bool = Field(default=False, alias="TELEMETRY_RECORDING_ENABLED")
    coordinator_instance_id: int = Field(default=0, alias="SNAPPER_COORDINATOR_INSTANCE_ID")
    coordinator_instance_count: int = Field(default=1, alias="SNAPPER_COORDINATOR_INSTANCE_COUNT")
    coordinator_outbox_max_scan_rows: str | None = Field(
        default="1000", alias="SNAPPER_COORDINATOR_OUTBOX_MAX_SCAN_ROWS"
    )
