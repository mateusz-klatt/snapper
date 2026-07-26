"""Snapper CLI application built with Typer.

This module provides the command-line interface for the Snapper trading
platform. Available commands include:

Server & Infrastructure:
    - ``server``: Start the FastAPI server with dashboard
    - ``broker``: Run the ZMQ message broker
    - ``trade-zmq``: Start the central trading coordinator
    - ``executor``: Run exchange order executor service
    - ``zmq-logger``: Monitor ZMQ message traffic
    - ``feed``: Run market data publisher
    - ``delegate-engine``: Run managed delegate workloads

Database:
    - ``db-init``: Initialize database schema
    - ``db-upgrade``: Run Alembic migrations
    - ``db-downgrade``: Rollback migrations

User Management:
    - ``init-admin``: Create initial admin user
    - ``list-users``: List all system users
    - ``reset-password``: Reset user password
    - ``token preflight``: Prove a live credential still authenticates

Ledger Maintenance:
    - ``annulment inspect``: Report what blocks one scope's certification
    - ``annulment annul``: Append one guarded execution annulment
    - ``annulment complete-visibility``: Complete pending durability observations

Data Updates:
    - ``update-kraken-symbols``: Sync Kraken symbol mappings
    - ``update-polygon-symbols``: Sync Polygon symbol mappings
    - ``polygon-backfill-aggregates``: Backfill historical data
    - ``kraken-futures-backfill-candles``: Backfill Kraken Futures OHLCV
    - ``kraken-equities-backfill-candles``: Backfill Kraken Equities (FCM) OHLCV
    - ``backfill-candles-from-trades``: Backfill calculated 1m candles from trades
    - ``backfill-synthesized-candles``: Backfill synthesized higher-timeframe candles
    - ``update-kraken-futures-funding-rates``: Backfill funding rates

Example:
    Run the server::

        snapper server --host 0.0.0.0 --port 8000

    Initialize database::

        snapper db-init
"""

import asyncio
import contextlib
import json as json_mod
import os
import signal
import threading
from collections.abc import Callable
from datetime import UTC
from datetime import date as date_type
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Annotated
from typing import Any
from typing import cast

import sqlalchemy as sa
import typer
import uvicorn
import zmq
import zmq.asyncio
from alembic import command
from alembic.config import Config
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from snapper.application.ai_review.service import _BUS_AI_REVIEW_DECISION_TOPIC
from snapper.application.ai_review.service import get_ai_review_service
from snapper.application.backtest.config import BacktestConfig
from snapper.application.backtest.config import BacktestExecutionMode
from snapper.application.backtest.config import BacktestFillModel
from snapper.application.backtest.config import compute_fingerprint
from snapper.application.backtest.direct_engine import DirectDbEngine
from snapper.application.backtest.metrics import compute_metrics
from snapper.application.backtest.result_collector import ResultCollector
from snapper.application.engine.trader import TraderCoordinator
from snapper.application.notify.apns_client import build_apns_client_pool
from snapper.application.notify.apns_config import load_apns_config
from snapper.application.notify.push_beta import PUSH_BETA_SETTING_KEY
from snapper.application.notify.push_beta import parse_push_beta_config
from snapper.application.notify.sidecar import NotifySidecar
from snapper.application.process_manager.command_listener import ProcessCommandListener
from snapper.application.process_manager.launcher import CoreProcessStartupError
from snapper.application.process_manager.launcher import ProcessLauncherService
from snapper.application.process_manager.registry import discover_processes
from snapper.application.services.candle_coverage import VERIFIABLE_TIMEFRAMES
from snapper.application.services.candle_coverage import CoverageReport
from snapper.application.services.candle_coverage import verify_candle_coverage
from snapper.application.services.continuous_contract_builder import ContinuousContractBuilder
from snapper.application.services.settings import get_settings_service
from snapper.application.updaters.historical.aggregates import PolygonAggregatesBackfillService
from snapper.application.updaters.historical.csv_loader import PolygonCsvLoaderService
from snapper.application.updaters.historical.grouped import PolygonGroupedDailyBackfillService
from snapper.application.updaters.historical.grouped_candle_loader import (
    PolygonGroupedCandleLoaderService,
)
from snapper.application.updaters.historical.kraken_equities_aggregates import (
    KrakenEquitiesAggregatesBackfillService,
)
from snapper.application.updaters.historical.kraken_futures_aggregates import (
    KrakenFuturesAggregatesBackfillService,
)
from snapper.application.updaters.historical.kraken_futures_funding import (
    KrakenFuturesFundingBackfillService,
)
from snapper.application.updaters.historical.split_repair import run_polygon_split_repair
from snapper.application.updaters.historical.synthesized_candle_backfill import (
    SynthesizedCandleBackfillService,
)
from snapper.application.updaters.historical.trade_candle_backfill import TradeCandleBackfillService
from snapper.application.updaters.symbols.base import SymbolUpdaterService
from snapper.application.updaters.symbols.kraken import KrakenSymbolUpdaterService
from snapper.application.updaters.symbols.kraken_equities import KrakenEquitiesSymbolUpdaterService
from snapper.application.updaters.symbols.kraken_futures import KrakenFuturesSymbolUpdaterService
from snapper.application.updaters.symbols.polygon import PolygonSymbolUpdaterService
from snapper.application.updaters.symbols.walutomat import WalutomatSymbolUpdaterService
from snapper.application.updaters.underlying_updater import UnderlyingUpdater
from snapper.auth.domain.roles import UserRole
from snapper.auth.user_service import UserService
from snapper.cli.dev_pat import dev_mint_pat
from snapper.cli.execution_annulment import annulment_app
from snapper.cli.token_preflight import token_app
from snapper.config.settings import BootstrapSettingsLoader
from snapper.config.settings import get_bootstrap_settings
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_with_service
from snapper.core.types import ExchangeEnum
from snapper.core.types import ProcessAutostartProfileEnum
from snapper.data.archive_symbols import safe_path
from snapper.data.archiver import EVENT_TABLES
from snapper.data.archiver import STATE_TABLES
from snapper.data.archiver import ArchiveRestorer
from snapper.data.archiver import CandleAuditArchiver
from snapper.data.archiver import CandleCacheArchiver
from snapper.data.archiver import EventArchiver
from snapper.data.archiver import ExportResult
from snapper.data.archiver import StateArchiver
from snapper.data.backtest_repository import BacktestRepository
from snapper.data.models import Setting
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import close_and_insert
from snapper.data.repository import get_repository
from snapper.data.repository import where_active_now
from snapper.data.repository_types import BacktestResultInsertRow
from snapper.data.repository_types import BacktestRunRow
from snapper.data.seed.loader import run_seed
from snapper.infrastructure.market_data.kraken import run_snapshot_update
from snapper.infrastructure.market_data.kraken_equities import run_kraken_equities_snapshot_update
from snapper.infrastructure.market_data.kraken_futures import run_kraken_futures_snapshot_update
from snapper.infrastructure.market_data.walutomat import run_walutomat_snapshot_update
from snapper.infrastructure.security.encryption import SettingsEncryptionService
from snapper.infrastructure.security.encryption import get_encryption_service
from snapper.messaging.executors.kraken import KrakenOrderExecutor
from snapper.messaging.executors.walutomat import WalutomatOrderExecutor
from snapper.messaging.infrastructure.broker import ZmqBrokerThread
from snapper.messaging.infrastructure.logger import ZmqMessageLogger
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.publisher import build_audit_publisher
from snapper.messaging.infrastructure.publisher import shutdown_audit_publisher
from snapper.messaging.infrastructure.validated_socket import HWM_AUDIT
from snapper.messaging.infrastructure.validated_socket import HWM_ORDER_FLOW
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.publishers.kraken import KrakenMarketDataPublisher
from snapper.server.app import create_app
from snapper.strategies.cointegration import CointegrationPairs
from snapper.strategies.heartbeat_consult import HeartbeatConsult
from snapper.strategies.macd import MACDCrossover
from snapper.strategies.rsi import RSIReversion

_egress_main: Callable[[list[str] | None], int] | None = None
_egress_import_error: ImportError | None = None
try:
    from snapper.egress.__main__ import main as _egress_main
except ImportError as exc:
    _egress_import_error = exc

_REGISTERED_STRATEGIES = (RSIReversion, MACDCrossover, CointegrationPairs, HeartbeatConsult)

_FORCE_UPDATE_HELP = "Force update even if recently updated"
_CLI_SYMBOL_SOURCE_ALL_MAPPED = "all mapped"
_CLI_SYMBOL_SOURCE_SETTINGS = "specified/settings"
_CLI_RESUME_FLAG = "--resume/--no-resume"

app = typer.Typer(add_completion=False, help="Snapper CLI")


def validate_api_keys_for_trader(paper: bool = False) -> bool:
    """Validate API keys configuration for trader coordinator.

    Args:
        paper: If True, skip validation for paper trading mode.

    Returns:
        True if configuration is valid.
    """
    return True


def _parse_utc(value: str) -> datetime:
    """Parse an ISO datetime string and normalize to UTC.

    Naive inputs get UTC attached. Offset-aware inputs are converted
    to UTC via astimezone.

    Args:
        value: ISO 8601 datetime string.

    Returns:
        Timezone-aware datetime in UTC.
    """
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def validate_api_keys(kraken_api_key: Any, kraken_api_secret: Any, paper: bool = False) -> bool:
    """Validate Kraken API credentials.

    Args:
        kraken_api_key: Kraken API key.
        kraken_api_secret: Kraken API secret.
        paper: If True, skip validation for paper trading mode.

    Returns:
        True if credentials are valid or paper mode is enabled.
    """
    if paper:
        return True
    key_str = str(kraken_api_key) if kraken_api_key else None
    secret_str = str(kraken_api_secret) if kraken_api_secret else None
    return bool(key_str and secret_str)


def parse_date_range(from_str: str, to_str: str) -> tuple[datetime, datetime]:
    """Parse date range strings into datetime objects.

    Args:
        from_str: Start date in YYYY-MM-DD format.
        to_str: End date in YYYY-MM-DD format.

    Returns:
        Tuple of (start_datetime, end_datetime) with UTC timezone.

    Raises:
        ValueError: If date strings are not in valid format.
    """
    try:
        from_date = datetime.strptime(from_str, "%Y-%m-%d").date()
        to_date = datetime.strptime(to_str, "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError(f"Error parsing dates: {e}") from e
    start_dt = datetime(from_date.year, from_date.month, from_date.day, tzinfo=UTC)
    end_dt = datetime(to_date.year, to_date.month, to_date.day, tzinfo=UTC)
    return start_dt, end_dt


def parse_symbols(symbols: str) -> list[str]:
    """Parse comma-separated symbols string into a list.

    Args:
        symbols: Comma-separated string of trading symbols.

    Returns:
        List of trimmed symbol strings.
    """
    if not symbols:
        return []
    return [s.strip() for s in symbols.split(",") if s.strip()]


def _alembic_cfg(db_url: str) -> Config:
    """Create Alembic configuration with database URL.

    Args:
        db_url: SQLAlchemy database connection URL.

    Returns:
        Configured Alembic Config object.

    Raises:
        RuntimeError: If alembic.ini is not found.
    """
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent.parent.parent
    candidates = [
        project_root / "alembic.ini",
        Path.cwd() / "alembic.ini",
    ]
    for alembic_ini_path in candidates:
        if alembic_ini_path.exists():
            cfg = Config(str(alembic_ini_path))
            cfg.set_main_option("sqlalchemy.url", db_url)
            return cfg
    raise RuntimeError(f"alembic.ini not found (tried {candidates})")


@app.callback()
def main_callback() -> None:
    """Initialize global encryption before any CLI command runs."""
    get_encryption_service()


@app.command(name="trade-zmq")
def trade_zmq(
    signal_topics: str | None = typer.Option(
        None, help="Comma-separated signal topics to subscribe (default: signals.)"
    ),
    instance_id: int | None = typer.Option(
        None,
        "--instance-id",
        help=(
            "Coordinator instance id (zero-based). Overrides the "
            "SNAPPER_COORDINATOR_INSTANCE_ID env var when set. Default 0."
        ),
    ),
    instance_count: int | None = typer.Option(
        None,
        "--instance-count",
        help=(
            "Total coordinator instance count. Overrides the "
            "SNAPPER_COORDINATOR_INSTANCE_COUNT env var when set. Default 1."
        ),
    ),
) -> None:
    """Start the central trading coordinator.

    Args:
        signal_topics: Comma-separated list of ZMQ signal topics to subscribe.
        instance_id: Zero-based identifier for this coordinator in a
            multi-instance deployment. When provided, it is written to
            ``os.environ`` so that the bootstrap settings loader picks
            it up during :class:`BootstrapSettingsLoader` construction.
        instance_count: Total coordinator count across the deployment.
            Same env precedence as ``instance_id``.

    Precedence: CLI flag > env var > default. When either flag is
    passed, the settings caches are invalidated via ``cache_clear``
    before any :func:`get_settings` call in this command body so the
    fresh env value is observed (the factories at
    ``snapper.config.settings`` are :func:`functools.lru_cache`-backed).
    """
    flag_mutated = False
    if instance_id is not None:
        os.environ["SNAPPER_COORDINATOR_INSTANCE_ID"] = str(instance_id)
        flag_mutated = True
    if instance_count is not None:
        os.environ["SNAPPER_COORDINATOR_INSTANCE_COUNT"] = str(instance_count)
        flag_mutated = True
    if flag_mutated:
        get_bootstrap_settings.cache_clear()
        get_settings.cache_clear()
    s = get_settings()
    if not validate_api_keys_for_trader(paper=False):
        typer.echo("Error validating configuration")
        raise typer.Exit(code=1)
    cfg = _alembic_cfg(s.db_url)
    command.upgrade(cfg, "head")
    topics = [t.strip() for t in signal_topics.split(",")] if signal_topics else ["signals."]
    typer.echo("Starting Central Trading Coordinator (ONE per system)")
    typer.echo("   Execution: EXTERNAL (via ExchangeExecutorService - kill switch enabled)")
    typer.echo(f"   Signal Topics: {topics}")
    typer.echo("")
    typer.echo("Note: ExchangeExecutorService must be running separately!")
    typer.echo("   Start it with: snapper executor")
    typer.echo("")
    typer.echo("Note: Strategies must be started separately.")
    typer.echo("   Use 'snapper server' to manage strategy processes.")
    typer.echo("")

    async def run_trader() -> None:
        trader = TraderCoordinator(signal_topics=topics)
        await trader.start()

    asyncio.run(run_trader())


@app.command(name="db-init")
def db_init() -> None:
    """Initialize the database schema using Alembic migrations."""
    s = get_settings()
    cfg = _alembic_cfg(s.db_url)
    command.upgrade(cfg, "head")
    typer.echo("DB initialized")


@app.command(name="db-upgrade")
def db_upgrade(revision: str = typer.Option("head", help="Revision to upgrade to")) -> None:
    """Run Alembic migrations to upgrade the database.

    Args:
        revision: Target revision identifier (default: head).
    """
    s = get_settings()
    cfg = _alembic_cfg(s.db_url)
    command.upgrade(cfg, revision)
    typer.echo(f"DB upgraded to {revision}")


@app.command(name="db-downgrade")
def db_downgrade(revision: str = typer.Option("-1", help="Revision to downgrade to")) -> None:
    """Run Alembic migrations to downgrade the database.

    Args:
        revision: Target revision identifier (default: -1 for one step back).
    """
    s = get_settings()
    cfg = _alembic_cfg(s.db_url)
    command.downgrade(cfg, revision)
    typer.echo(f"DB downgraded by {revision}")


@app.command(name="db-seed")
def db_seed(
    profile: str = typer.Option("dev", help="Seed profile name (e.g. dev, prod)"),
) -> None:
    """Seed the database with environment-specific data from TOML profiles.

    Loads users and settings from a TOML seed file. Uses three-tier lookup:
    ``data/seed/{profile}.toml`` -> ``proprietary/data/seed/{profile}.toml``
    -> package-bundled ``snapper/data/seed/{profile}.toml``.

    Args:
        profile: Seed profile name.
    """
    try:
        users_count, settings_count = run_seed(profile)
        typer.echo(f"Seeded {users_count} users and {settings_count} settings (profile: {profile})")
    except FileNotFoundError as e:
        typer.echo(f"Error: {e}")
        raise typer.Exit(code=1) from e


@app.command()
def server(
    host: str | None = typer.Option(None, help="Server host (overrides SERVER_HOST env var)"),
    port: int | None = typer.Option(None, help="Server port (overrides SERVER_PORT env var)"),
    reload: bool | None = typer.Option(
        None, help="Enable auto-reload (overrides SERVER_RELOAD env var)"
    ),
) -> None:
    """Start the FastAPI server with dashboard.

    Set ``SERVER_API_ONLY=true`` to skip engine process autostart while
    keeping the ZMQ-WebSocket bridge (useful for multi-worker deployments
    or when the engine runs as a separate process).

    Args:
        host: Server host address.
        port: Server port number.
        reload: Enable auto-reload on file changes.
    """
    s = get_settings()
    server_host = host or s.server_host
    server_port = port or s.server_port
    server_reload = reload if reload is not None else s.server_reload
    server_proxy_headers = s.server_proxy_headers
    server_forwarded_allow_ips = s.server_forwarded_allow_ips
    typer.echo(f"Starting Snapper server on {server_host}:{server_port}")
    typer.echo(f"Dashboard available at: http://{server_host}:{server_port}")
    if server_reload:
        typer.echo("Reload mode enabled - file changes will restart server")
    try:
        if server_reload:
            uvicorn.run(
                "snapper.server.app:create_app",
                factory=True,
                host=server_host,
                port=server_port,
                reload=server_reload,
                log_level="info",
                log_config=None,
                proxy_headers=server_proxy_headers,
                forwarded_allow_ips=server_forwarded_allow_ips,
                loop="uvloop",
            )
        else:
            uvicorn.run(
                create_app(),
                host=server_host,
                port=server_port,
                reload=server_reload,
                log_level="info",
                log_config=None,
                proxy_headers=server_proxy_headers,
                forwarded_allow_ips=server_forwarded_allow_ips,
                loop="uvloop",
            )
    except KeyboardInterrupt:
        typer.echo("\nShutting down gracefully...")


@app.command()
def broker(
    xsub: str | None = typer.Option(None, help="XSUB endpoint (publishers connect here)"),
    xpub: str | None = typer.Option(None, help="XPUB endpoint (subscribers connect here)"),
) -> None:
    """Run the ZMQ message broker for pub/sub communication.

    Args:
        xsub: XSUB endpoint for publishers.
        xpub: XPUB endpoint for subscribers.
    """
    broker_instance = ZmqBrokerThread(xsub_endpoint=xsub, xpub_endpoint=xpub)
    try:
        broker_instance.start()
        typer.echo("ZMQ Broker running - Press Ctrl+C to stop")
        typer.echo(f"XSUB: {broker_instance.xsub_endpoint}")
        typer.echo(f"XPUB: {broker_instance.xpub_endpoint}")
        stop_event = threading.Event()

        def signal_handler(signum: int, frame: Any) -> None:
            stop_event.set()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        stop_event.wait()
    except KeyboardInterrupt:
        typer.echo("Broker stopped by user")
    finally:
        broker_instance.stop()


@app.command()
def feed(
    symbols: str = typer.Option("BTC-USD", help="Comma-separated list of native symbols"),
) -> None:
    """Run the Kraken market data publisher.

    Args:
        symbols: Comma-separated list of trading symbols.
    """
    symbol_list = parse_symbols(symbols)

    async def run_feed() -> None:
        publisher = KrakenMarketDataPublisher(symbols=symbol_list)
        try:
            typer.echo(f"Starting feed for symbols: {symbol_list}")
            typer.echo("Publishing through broker")
            await publisher.start()
        except KeyboardInterrupt:
            typer.echo("Feed stopped by user")
        finally:
            await publisher.stop()

    asyncio.run(run_feed())


async def _await_shutdown_signal() -> None:
    """Block until SIGINT/SIGTERM, then return so callers can shut down.

    Registers asyncio signal handlers (instead of letting the default
    handler raise / terminate) so a containerised feed engine receiving
    ``docker stop`` (SIGTERM) unwinds its ``finally`` block and tears
    down its child publisher subprocesses cleanly.
    """
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)
    await stop_event.wait()


async def _await_feed_shutdown_or_failure(launcher: ProcessLauncherService) -> str | None:
    """Block until a shutdown signal or an unexpected publisher crash.

    Races :func:`_await_shutdown_signal` against
    :meth:`ProcessLauncherService.wait_for_feed_publisher_failure`,
    returning whichever resolves first and cancelling the loser (awaited
    so no pending task is left dangling).

    Returns:
        The name of the publisher that crashed when a supervised feed
        publisher died first; ``None`` when a SIGINT/SIGTERM shutdown
        signal arrived first.
    """
    shutdown_task = asyncio.ensure_future(_await_shutdown_signal())
    failure_task = asyncio.ensure_future(launcher.wait_for_feed_publisher_failure())
    done, pending = await asyncio.wait(
        {shutdown_task, failure_task}, return_when=asyncio.FIRST_COMPLETED
    )
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    if failure_task in done:
        return failure_task.result()
    return None


async def _run_feed_engine() -> str | None:
    """Run the dedicated market-data feed container until shutdown.

    Initialises a DB-backed settings service, discovers the process
    registry, syncs it to the database, then starts every enabled
    market-data publisher as its own OS subprocess
    (:meth:`ProcessLauncherService.start_feed_publishers`). Blocks until
    either a shutdown signal or an unexpected publisher crash, then stops
    all publishers and disposes the settings service. The ZMQ broker is
    NOT started here — the feed container connects to the backend's broker
    via the ``ZMQ_BROKER_*`` endpoints.

    A bus :class:`MessagePublisher` is wired into the launcher (mirroring
    the API container at ``snapper.server.app._build_user_service_publisher``)
    so the launcher's 5s native-process monitor can emit
    ``processes.events.summary.coord-<id>`` snapshots carrying each
    feed subprocess's sampled RSS/CPU. The publisher uses
    ``app_settings.zmq_broker_xsub`` exactly like the API. Publisher
    construction is wrapped defensively: a momentarily-unavailable broker
    must NEVER abort feed startup (the container has
    ``restart: unless-stopped`` and a startup exception would crash-loop
    every healthy publisher). ZMQ ``PUB.connect()`` is non-blocking and
    tolerates an absent peer, so connect itself is safe; on any
    construction failure the publisher degrades to ``None`` and the
    launcher silently skips metrics emission (its summary emit already
    no-ops on a missing publisher). The socket is closed and the context
    terminated in ``finally`` AFTER ``stop_all_processes`` so no monitor
    send races a closed socket.

    Returns:
        The name of a publisher that crashed (the caller exits non-zero
        so the orchestrator restarts the container), or ``None`` on a
        clean shutdown-signal teardown.
    """
    settings = get_settings()
    settings_service = await get_settings_service(settings.db_url, settings.zmq_broker_xsub)
    app_settings = get_settings_with_service(settings_service)
    discover_processes()
    launcher = ProcessLauncherService(app_settings)
    zmq_ctx: zmq.asyncio.Context | None = None
    publisher: MessagePublisher | None = None
    try:
        publisher, zmq_ctx = build_audit_publisher(app_settings.zmq_broker_xsub)
    except Exception as exc:
        typer.echo(
            f"Feed engine: summary publisher unavailable ({exc}); "
            "continuing without per-process metrics emission.",
            err=True,
        )
        shutdown_audit_publisher(publisher, zmq_ctx)
        zmq_ctx = None
        publisher = None
    launcher.set_msg_publisher(publisher)
    reconcile_task: asyncio.Task[None] | None = None
    command_listener: ProcessCommandListener | None = None
    try:
        await launcher.sync_registry_to_database()
        await launcher.start_feed_publishers()
        if (
            getattr(app_settings, "process_autostart_profile", None)
            is ProcessAutostartProfileEnum.FEED
        ):
            reconcile_task = asyncio.ensure_future(_reconcile_loop(launcher))
            command_listener = ProcessCommandListener(launcher)
            await command_listener.start(getattr(app_settings, "zmq_broker_xpub", ""))
        else:
            typer.echo(
                "Feed engine: reconcile loop disabled (PROCESS_AUTOSTART_PROFILE is not 'feed'); "
                "the loop is only safe when this node owns exactly the publishers.",
                err=True,
            )
        typer.echo(
            "Feed engine running — publishers spawned as separate processes. Ctrl+C to stop."
        )
        return await _await_feed_shutdown_or_failure(launcher)
    finally:
        if reconcile_task is not None:
            reconcile_task.cancel()
            await asyncio.gather(reconcile_task, return_exceptions=True)
        if command_listener is not None:
            await command_listener.stop()
        await launcher.stop_all_processes()
        await settings_service.shutdown()
        shutdown_audit_publisher(publisher, zmq_ctx)


@app.command(name="feed-engine")
def feed_engine() -> None:
    """Run the dedicated feed container: all market-data publishers as processes.

    Each market-data publisher is launched as its own OS subprocess
    (``mode=PROCESS``) so it gets a dedicated CPU core and event loop,
    off the FastAPI backend's loop. Intended to run as a separate
    container alongside the API/trading backend; set
    ``PROCESS_AUTOSTART_PROFILE=api`` on the backend so it no longer
    starts the publishers itself.

    Exits non-zero if a supervised publisher crashes so the orchestrator
    (``restart: unless-stopped``) restarts the whole container, respawning
    every publisher — otherwise a single venue's death would go unnoticed.
    """
    failed_publisher = asyncio.run(_run_feed_engine())
    if failed_publisher is not None:
        typer.echo(
            f"Feed publisher '{failed_publisher}' crashed; exiting for container restart.",
            err=True,
        )
        raise typer.Exit(code=1)


async def _run_strategies_engine() -> None:
    """Run the dedicated strategies container until shutdown.

    Mirrors :func:`_run_feed_engine` for role-STRATEGY processes:
    initialises the DB-backed settings service, discovers the registry
    (including ``STRATEGY_EXTRA_PACKAGES`` mounts), syncs it, then runs
    :meth:`ProcessLauncherService.start_all_processes` under the
    ``strategy`` autostart profile — strategies start THREAD-mode with
    the full boot scope enforcement, watchdog, and parking semantics
    they had inside the backend. A crashed strategy is watchdog-managed
    (parked on budget exhaustion), never container-fatal: strategies are
    role STRATEGY, so only a CORE startup failure exits non-zero.

    Two engine-local services keep the container a first-class bus
    citizen:

    - A decision-only AI-review listener (``bus.ai_review_decision``)
      so ``create_ai_review_and_await`` resumes on the fast path
      instead of degrading to its DB poll; delegate-offline and
      caps-violation handling deliberately stay on the FastAPI server.
    - A periodic summary loop emitting the launcher's cross-coordinator
      snapshot every 5s (THREAD-mode strategies have no native-process
      monitor, and the API's RemoteSummaryCache treats snapshots stale
      after 15s) — including the zero-strategies case.
    """
    settings = get_settings()
    settings_service = await get_settings_service(settings.db_url, settings.zmq_broker_xsub)
    app_settings = get_settings_with_service(settings_service)
    discover_processes()
    launcher = ProcessLauncherService(app_settings)
    zmq_ctx: zmq.asyncio.Context | None = None
    publisher: MessagePublisher | None = None
    try:
        publisher, zmq_ctx = build_audit_publisher(app_settings.zmq_broker_xsub)
    except Exception as exc:
        typer.echo(
            f"Strategies engine: summary publisher unavailable ({exc}); "
            "continuing without summary emission.",
            err=True,
        )
        shutdown_audit_publisher(publisher, zmq_ctx)
        zmq_ctx = None
        publisher = None
    launcher.set_msg_publisher(publisher)
    ai_service = get_ai_review_service()
    ai_service.set_msg_publisher(publisher)
    listener_started = False
    summary_task: asyncio.Task[None] | None = None
    reconcile_task: asyncio.Task[None] | None = None
    command_listener: ProcessCommandListener | None = None
    try:
        if app_settings.zmq_broker_xpub:
            await ai_service.start_bus_listener(
                app_settings.zmq_broker_xpub,
                topics=(_BUS_AI_REVIEW_DECISION_TOPIC,),
            )
            listener_started = True
        await launcher.sync_registry_to_database()
        await launcher.start_all_processes()
        summary_task = asyncio.ensure_future(_engine_summary_loop(launcher))
        if (
            getattr(app_settings, "process_autostart_profile", None)
            is ProcessAutostartProfileEnum.STRATEGY
        ):
            reconcile_task = asyncio.ensure_future(_reconcile_loop(launcher))
            command_listener = ProcessCommandListener(launcher)
            await command_listener.start(getattr(app_settings, "zmq_broker_xpub", ""))
        else:
            typer.echo(
                "Strategies engine: reconcile loop disabled (PROCESS_AUTOSTART_PROFILE is not "
                "'strategy'); the loop is only safe when this node owns exactly the strategies.",
                err=True,
            )
        typer.echo("Strategies engine running — role-STRATEGY processes started. Ctrl+C to stop.")
        await _await_shutdown_signal()
    finally:
        if summary_task is not None:
            summary_task.cancel()
            await asyncio.gather(summary_task, return_exceptions=True)
        if reconcile_task is not None:
            reconcile_task.cancel()
            await asyncio.gather(reconcile_task, return_exceptions=True)
        if command_listener is not None:
            await command_listener.stop()
        await launcher.stop_all_processes()
        if listener_started:
            await ai_service.stop_bus_listener()
        await settings_service.shutdown()
        shutdown_audit_publisher(publisher, zmq_ctx)


async def _engine_summary_loop(launcher: ProcessLauncherService) -> None:
    """Emit an engine launcher's summary snapshot every 5s, best-effort.

    Keeps the API-side RemoteSummaryCache (15s TTL) fresh for a
    dedicated coordinator, including when it currently owns zero
    workloads. An individual emission failure logs at the publisher
    layer and never stops the loop.
    """
    while True:
        with contextlib.suppress(Exception):
            await launcher.emit_summary_snapshot()
        await asyncio.sleep(5.0)


_strategies_summary_loop = _engine_summary_loop


_RECONCILE_INTERVAL_SECONDS = 10.0
"""How often a coordinator converges running processes to the DB desired-state."""


async def _reconcile_loop(launcher: ProcessLauncherService) -> None:
    """Converge running processes to the DB desired-state every ~10s, best-effort.

    The coordinator-side driver of the control plane: an operator
    enable/disable/restart (a ``PATCH /desired-state`` write) takes effect
    within one tick without a manual restart. A whole-pass failure is
    suppressed so a single bad cycle never stops the loop; per-process
    failures are already fail-soft inside ``reconcile_desired_state``.

    Args:
        launcher: The coordinator's process launcher service.
    """
    while True:
        with contextlib.suppress(Exception):
            await launcher.reconcile_desired_state()
        await asyncio.sleep(_RECONCILE_INTERVAL_SECONDS)


@app.command(name="strategies-engine")
def strategies_engine() -> None:
    """Run the dedicated strategies container: role-STRATEGY processes.

    Strategies run THREAD-mode inside this container exactly as they ran
    in the backend (boot wallet/grant scope enforcement, watchdog,
    parking), while the backend sets ``STRATEGIES_EMBEDDED=false`` so it
    stops owning them. Requires ``PROCESS_AUTOSTART_PROFILE=strategy``.

    Exits non-zero only when a CORE process fails to start (strategy
    crashes are watchdog-managed and, after the restart budget, parked —
    visible via the coord summary frames, never container-fatal).
    """
    try:
        asyncio.run(_run_strategies_engine())
    except CoreProcessStartupError as exc:
        typer.echo(f"Strategies engine startup failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc


async def _run_delegate_engine() -> None:
    """Run the dedicated delegate coordinator until shutdown.

    Discovers the standard registry and every configured extra package,
    syncs their process definitions, and starts only configs selected by
    the ``delegate`` autostart profile. The registered integration runner
    uses ``mode=PROCESS`` so the existing launcher owns subprocess status,
    restart, parking, desired-state reconciliation, and clean termination.

    This entrypoint wires process-management services only. Model clients,
    consult handling, and network egress are deliberately absent from this
    foundation.
    """
    settings = get_settings()
    if (
        getattr(settings, "process_autostart_profile", None)
        is not ProcessAutostartProfileEnum.DELEGATE
    ):
        typer.echo(
            "Delegate engine requires PROCESS_AUTOSTART_PROFILE=delegate; refusing to start.",
            err=True,
        )
        raise typer.Exit(code=1)
    settings_service = await get_settings_service(settings.db_url, settings.zmq_broker_xsub)
    app_settings = get_settings_with_service(settings_service)
    discover_processes()
    launcher = ProcessLauncherService(app_settings)
    zmq_ctx: zmq.asyncio.Context | None = None
    publisher: MessagePublisher | None = None
    try:
        publisher, zmq_ctx = build_audit_publisher(app_settings.zmq_broker_xsub)
    except Exception as exc:
        typer.echo(
            f"Delegate engine: summary publisher unavailable ({exc}); "
            "continuing without summary emission.",
            err=True,
        )
        shutdown_audit_publisher(publisher, zmq_ctx)
        zmq_ctx = None
        publisher = None
    launcher.set_msg_publisher(publisher)
    summary_task: asyncio.Task[None] | None = None
    reconcile_task: asyncio.Task[None] | None = None
    command_listener: ProcessCommandListener | None = None
    try:
        await launcher.sync_registry_to_database()
        await launcher.start_all_processes()
        summary_task = asyncio.ensure_future(_engine_summary_loop(launcher))
        reconcile_task = asyncio.ensure_future(_reconcile_loop(launcher))
        command_listener = ProcessCommandListener(launcher)
        await command_listener.start(getattr(app_settings, "zmq_broker_xpub", ""))
        typer.echo("Delegate engine running — managed delegate processes started. Ctrl+C to stop.")
        await _await_shutdown_signal()
    finally:
        if summary_task is not None:
            summary_task.cancel()
            await asyncio.gather(summary_task, return_exceptions=True)
        if reconcile_task is not None:
            reconcile_task.cancel()
            await asyncio.gather(reconcile_task, return_exceptions=True)
        if command_listener is not None:
            await command_listener.stop()
        await launcher.stop_all_processes()
        await settings_service.shutdown()
        shutdown_audit_publisher(publisher, zmq_ctx)


@app.command(name="delegate-engine")
def delegate_engine() -> None:
    """Run generic delegate workloads as managed subprocesses.

    Requires ``PROCESS_AUTOSTART_PROFILE=delegate`` and a loadable extra
    package that registers the generic runner. The command only operates
    process lifecycle plumbing; no model, consult, or egress behavior is
    implemented here.
    """
    try:
        asyncio.run(_run_delegate_engine())
    except CoreProcessStartupError as exc:
        typer.echo(f"Delegate engine startup failed: {exc}", err=True)
        raise typer.Exit(code=1) from exc


@app.command(name="zmq-logger")
def zmq_logger(
    log_payload: bool = typer.Option(
        False, "--payload/--no-payload", help="Log full payload (default: metadata only)"
    ),
    log_file: bool = typer.Option(
        True, "--file/--no-file", help="Write to audit file (default: yes)"
    ),
    audit_file: str | None = typer.Option(
        None, "--audit-file", help="Path to audit file (default: data/zmq_audit.jsonl)"
    ),
    max_length: int = typer.Option(200, "--max-length", help="Max payload preview length"),
) -> None:
    """Monitor and log ZMQ message traffic.

    Args:
        log_payload: Log full message payload.
        log_file: Write messages to audit file.
        audit_file: Path to the audit output file.
        max_length: Maximum payload preview length in logs.
    """

    async def run_logger() -> None:
        logger_instance = ZmqMessageLogger(
            log_to_file=log_file,
            log_payload=log_payload,
            max_payload_length=max_length,
            audit_file=audit_file,
        )
        try:
            typer.echo("Starting ZMQ Message Logger")
            typer.echo(f"   Log payload: {log_payload}")
            typer.echo(f"   Log to file: {log_file}")
            if log_file:
                typer.echo(f"   Audit file: {logger_instance.audit_path}")
            typer.echo("   Subscribing to: ALL MESSAGES")
            typer.echo("")
            typer.echo("Press Ctrl+C to stop and show statistics")
            await logger_instance.start()
        except KeyboardInterrupt:
            typer.echo("\nZMQ Logger stopped by user")
        finally:
            """No cleanup action required."""

    asyncio.run(run_logger())


@app.command()
def executor(
    exchange: Annotated[
        str,
        typer.Option(
            "--exchange",
            "-e",
            help="Exchange to run executor for (kraken, walutomat)",
        ),
    ] = ExchangeEnum.KRAKEN,
) -> None:
    """Run the exchange order executor service.

    Args:
        exchange: Exchange name (kraken, walutomat).
    """

    async def run_executor() -> None:
        service_map: dict[
            str,
            type[KrakenOrderExecutor] | type[WalutomatOrderExecutor],
        ] = {
            ExchangeEnum.KRAKEN: KrakenOrderExecutor,
            ExchangeEnum.WALUTOMAT: WalutomatOrderExecutor,
        }
        if exchange.lower() not in service_map:
            typer.echo(f"Error: Unknown exchange '{exchange}'. Choose: kraken, walutomat")
            raise typer.Exit(1)
        service: KrakenOrderExecutor | WalutomatOrderExecutor = service_map[exchange.lower()]()
        try:
            typer.echo(f"Starting {exchange} execution service")
            status = service.get_status()
            typer.echo(f"Orders SUB: {status['broker_xpub']}")
            typer.echo(f"Fills PUB: {status['broker_xsub']}")
            await service.start()
        except KeyboardInterrupt:
            typer.echo(f"{exchange} execution service stopped by user")
        finally:
            await service.stop()

    asyncio.run(run_executor())


@app.command()
def init_admin(
    username: str = typer.Option("admin", help="Admin username"),
    password: str = typer.Option(
        ...,
        prompt=True,
        hide_input=True,
        confirmation_prompt=True,
        help="Admin password (prompted with confirmation when omitted; never echoed)",
    ),
) -> None:
    """Create the initial admin user.

    Args:
        username: Admin username.
        password: Admin password. Never echoed back to stdout; never accepted
            as a hardcoded default so an operator running ``snapper init-admin``
            with no flags cannot accidentally create the admin user with the
            old hardcoded default credential documented in pre-2026-05-27 guides.
    """

    async def create_admin_user() -> None:
        user_service = UserService()
        existing_user = await user_service.get_user_by_username(username)
        if existing_user:
            typer.echo(f"User '{username}' already exists!")
            return
        await user_service.create_user(
            username=username, password=password, role=UserRole.ADMIN, is_active=True
        )
        typer.echo(f"Admin user '{username}' created successfully!")
        typer.echo(f"Username: {username}")
        typer.echo("Please change the password after first login")

    asyncio.run(create_admin_user())


@app.command()
def list_users() -> None:
    """List all system users."""

    async def show_users() -> None:
        user_service = UserService()
        users = await user_service.get_all_users(include_inactive=True)
        typer.echo("System Users:")
        typer.echo("=" * 50)
        for user in users:
            status = "Active" if user.is_active else "Inactive"
            typer.echo(f"{user.username:<15} | {user.role:<10} | {status}")
        typer.echo("=" * 50)
        typer.echo(
            "Forgot a password? Use 'snapper reset-password <username>' "
            "(prompts hidden). The dev seed leaves every account on the "
            "rotate-on-first-login placeholder password, so any login "
            "must reset that placeholder."
        )

    asyncio.run(show_users())


@app.command()
def reset_password(
    username: str = typer.Argument(..., help="Username to reset password for"),
    new_password: str = typer.Option("", help="New password (if empty, will prompt)"),
) -> None:
    """Reset a user's password.

    Args:
        username: Username to reset password for.
        new_password: New password (prompts if empty).
    """
    if not new_password:
        new_password = typer.prompt("Enter new password", hide_input=True)
        confirm = typer.prompt("Confirm password", hide_input=True)
        if new_password != confirm:
            typer.echo("Passwords don't match!")
            return

    async def reset_user_password() -> None:
        user_service = UserService()
        try:
            await user_service.reset_password_by_username(username, new_password)
            typer.echo(f"Password reset for user '{username}'")
            typer.echo(f"Username: {username}")
        except ValueError as e:
            typer.echo(str(e))
        except Exception as e:
            typer.echo(f"Failed to reset password: {e}")

    asyncio.run(reset_user_password())


@app.command(name="update-kraken-symbols")
def update_kraken_symbols(
    force: bool = typer.Option(False, "--force", "-f", help=_FORCE_UPDATE_HELP),
) -> None:
    """Sync Kraken symbol mappings from exchange API.

    Args:
        force: Force update even if recently updated.
    """

    async def run_symbol_update() -> None:
        updater = KrakenSymbolUpdaterService(force=force)
        try:
            typer.echo("Starting Kraken symbol mapping update...")
            await updater.start()
            typer.echo("Symbol mappings updated successfully")
        except Exception as e:
            typer.echo(f"Error updating symbol mappings: {e}")
            raise typer.Exit(code=1) from e

    asyncio.run(run_symbol_update())


@app.command(name="update-kraken-market-snapshot")
def update_kraken_market_snapshot() -> None:
    """Update Kraken market snapshots with current prices."""
    typer.echo("Updating Kraken market snapshots...")
    try:
        run_snapshot_update()
        typer.echo("Kraken market snapshot update complete!")
    except Exception as e:
        typer.echo(f"Error updating Kraken market snapshots: {e}")
        raise typer.Exit(code=1) from e


@app.command(name="update-kraken-futures-market-snapshot")
def update_kraken_futures_market_snapshot() -> None:
    """Update Kraken Futures market snapshots with current prices."""
    typer.echo("Updating Kraken Futures market snapshots...")
    try:
        run_kraken_futures_snapshot_update()
        typer.echo("Kraken Futures market snapshot update complete!")
    except Exception as e:
        typer.echo(f"Error updating Kraken Futures market snapshots: {e}")
        raise typer.Exit(code=1) from e


@app.command(name="update-kraken-equities-market-snapshot")
def update_kraken_equities_market_snapshot() -> None:
    """Update Kraken Equities market snapshots with current prices."""
    typer.echo("Updating Kraken Equities market snapshots...")
    try:
        run_kraken_equities_snapshot_update()
        typer.echo("Kraken Equities market snapshot update complete!")
    except Exception as e:
        typer.echo(f"Error updating Kraken Equities market snapshots: {e}")
        raise typer.Exit(code=1) from e


@app.command(name="update-kraken-futures-symbols")
def update_kraken_futures_symbols(
    force: bool = typer.Option(False, "--force", "-f", help=_FORCE_UPDATE_HELP),
) -> None:
    """Sync Kraken Futures (crypto) symbol mappings from exchange API.

    Args:
        force: Force update even if recently updated.
    """

    async def run_futures_update() -> None:
        updater = KrakenFuturesSymbolUpdaterService(force=force)
        try:
            typer.echo("Starting Kraken Futures symbol mapping update...")
            await updater.start()
            typer.echo("Kraken Futures symbol mappings updated successfully")
        except Exception as e:
            typer.echo(f"Error updating Kraken Futures symbol mappings: {e}")
            raise typer.Exit(code=1) from e

    asyncio.run(run_futures_update())


@app.command(name="update-kraken-equities-symbols")
def update_kraken_equities_symbols(
    force: bool = typer.Option(False, "--force", "-f", help=_FORCE_UPDATE_HELP),
) -> None:
    """Sync Kraken Equities (FCM Futures) symbol mappings from API.

    Args:
        force: Force update even if recently updated.
    """

    async def run_equities_update() -> None:
        updater = KrakenEquitiesSymbolUpdaterService(force=force)
        try:
            typer.echo("Starting Kraken Equities symbol mapping update...")
            await updater.start()
            typer.echo("Kraken Equities symbol mappings updated successfully")
        except Exception as e:
            typer.echo(f"Error updating Kraken Equities symbol mappings: {e}")
            raise typer.Exit(code=1) from e

    asyncio.run(run_equities_update())


@app.command(name="update-walutomat-symbols")
def update_walutomat_symbols(
    force: bool = typer.Option(False, "--force", "-f", help=_FORCE_UPDATE_HELP),
) -> None:
    """Sync Walutomat symbol mappings from exchange API.

    Args:
        force: Force update even if recently updated.
    """

    async def run_walutomat_update() -> None:
        updater = WalutomatSymbolUpdaterService(update_threshold_hours=168, force=force)
        try:
            typer.echo("Starting Walutomat symbol mapping update...")
            await updater.start()
            typer.echo("Walutomat symbol mappings updated successfully")
        except Exception as e:
            typer.echo(f"Error updating Walutomat symbol mappings: {e}")
            raise typer.Exit(code=1) from e

    asyncio.run(run_walutomat_update())


@app.command(name="update-polygon-symbols")
def update_polygon_symbols(
    force: bool = typer.Option(False, "--force", "-f", help=_FORCE_UPDATE_HELP),
    insert_new: bool = typer.Option(
        False, "--insert-new", help="Insert new symbols (default: UPDATE existing only)"
    ),
) -> None:
    """Sync Polygon symbol mappings from API.

    Args:
        force: Force update even if recently updated.
        insert_new: Insert new symbols instead of only updating existing.
    """

    async def run_polygon_update() -> None:
        updater = PolygonSymbolUpdaterService(
            update_threshold_hours=168, force=force, insert_new=insert_new
        )
        try:
            typer.echo("Starting Polygon symbol mapping update...")
            typer.echo("   (This may take 10-15 minutes to download 44k+ symbols from API)")
            await updater.start()
            typer.echo("Polygon symbol mappings updated successfully")
        except Exception as e:
            typer.echo(f"Error updating Polygon symbol mappings: {e}")
            raise typer.Exit(code=1) from e

    asyncio.run(run_polygon_update())


@app.command(name="update-walutomat-market-snapshot")
def update_walutomat_market_snapshot() -> None:
    """Update Walutomat market snapshots with current prices."""
    typer.echo("Updating Walutomat market snapshots...")
    try:
        run_walutomat_snapshot_update()
        typer.echo("Walutomat market snapshot update complete!")
    except Exception as e:
        typer.echo(f"Error updating Walutomat market snapshots: {e}")
        raise typer.Exit(code=1) from e


def _verify_encryption_services(
    old_encryption: SettingsEncryptionService,
    new_encryption: SettingsEncryptionService,
) -> None:
    """Verify both old and new encryption services can round-trip a test value.

    Args:
        old_encryption: Current encryption service.
        new_encryption: New encryption service.

    Raises:
        ValueError: If either service fails the round-trip verification.
    """
    test_value = "test-rotation-verification"
    old_decrypted = old_encryption.decrypt(old_encryption.encrypt(test_value))
    new_decrypted = new_encryption.decrypt(new_encryption.encrypt(test_value))
    if old_decrypted != test_value or new_decrypted != test_value:
        raise ValueError("Encryption verification failed - check your passwords")


async def _rotate_single_setting(
    setting: Any,
    old_encryption: SettingsEncryptionService,
    new_encryption: SettingsEncryptionService,
    dry_run: bool,
    session: Any,
    tracker: SequenceTracker,
) -> bool:
    """Re-encrypt a single setting from old to new encryption.

    Args:
        setting: Database Setting row with key and value.
        old_encryption: Current encryption service.
        new_encryption: New encryption service.
        dry_run: If True, skip writing back the new value.
        session: Active async database session.
        tracker: SequenceTracker for stamping session_id and sequence_id.

    Returns:
        True if the setting was rotated, False if skipped.

    Raises:
        Exception: Propagates decryption/encryption errors when not in dry-run.
    """
    if not setting.value:
        typer.echo(f"Skipping empty setting: {setting.key}")
        return False
    try:
        decrypted_value = old_encryption.decrypt(setting.value)
        new_encrypted_value = new_encryption.encrypt(decrypted_value)
        typer.echo(f"Rotating: {setting.key}")
        if not dry_run:
            now = datetime.now(UTC)
            await close_and_insert(
                session=session,
                model=Setting,
                match_filters=[Setting.key == setting.key],
                new_values={
                    "key": setting.key,
                    "value": new_encrypted_value,
                    "category": setting.category,
                    "description": setting.description,
                    "is_encrypted": setting.is_encrypted,
                    "updated_by": setting.updated_by,
                    "session_id": tracker.session_id,
                    "sequence_id": tracker.next_sequence("settings"),
                },
                bus_time=now,
            )
        return not dry_run
    except Exception as e:
        typer.echo(f"Failed to rotate {setting.key}: {e}")
        if not dry_run:
            raise
        return False


async def _commit_rotation_results(
    session: Any,
    changes_made: int,
    total_settings: int,
    dry_run: bool,
) -> None:
    """Commit rotation results and print summary.

    Args:
        session: Active database session.
        changes_made: Number of settings successfully rotated.
        total_settings: Total number of encrypted settings found.
        dry_run: Whether this was a dry run.
    """
    if not dry_run and changes_made > 0:
        await session.commit()
        typer.echo(f"Successfully rotated {changes_made} encrypted settings")
        typer.echo()
        typer.echo(
            "IMPORTANT: Update MASTER_PASSWORD in your .env (or process environment) "
            "to the value you just entered, then restart the application. The value "
            "is intentionally NOT printed here so it does not land in shell history, "
            "scrollback, or CI logs."
        )
    elif dry_run:
        typer.echo(f"DRY RUN: Would rotate {total_settings} settings")


async def _run_encryption_rotation(
    new_master_password: str,
    old_master_password: str | None,
    dry_run: bool,
) -> None:
    """Execute the encryption rotation workflow.

    Args:
        new_master_password: New master password for encryption.
        old_master_password: Current master password (None to read from bootstrap).
        dry_run: Show changes without applying them.
    """
    try:
        bootstrap = BootstrapSettingsLoader()
        current_password = old_master_password or bootstrap.master_password
        typer.echo("Starting encryption rotation...")
        typer.echo(f"Current password: {'***' if current_password else 'None'}")
        typer.echo(f"New password: {'***' if new_master_password else 'None'}")
        if dry_run:
            typer.echo("DRY RUN MODE - No changes will be made")
        old_encryption = SettingsEncryptionService(current_password)
        new_encryption = SettingsEncryptionService(new_master_password)
        _verify_encryption_services(old_encryption, new_encryption)
        typer.echo("Encryption parameters verified")
        poolclass = NullPool if "sqlite" in bootstrap.db_url else None
        engine = create_async_engine(bootstrap.db_url, poolclass=poolclass)
        session_factory = async_sessionmaker(engine)
        rotation_tracker = SequenceTracker()
        async with session_factory() as session:
            result = await session.execute(
                sa.select(Setting).where(Setting.is_encrypted, *where_active_now(Setting))
            )
            encrypted_settings = result.scalars().all()
            typer.echo(f"Found {len(encrypted_settings)} encrypted settings to rotate")
            if not encrypted_settings:
                typer.echo("No encrypted settings found - nothing to rotate")
                return
            changes_made = 0
            for s in encrypted_settings:
                if await _rotate_single_setting(
                    s, old_encryption, new_encryption, dry_run, session, rotation_tracker
                ):
                    changes_made += 1
            await _commit_rotation_results(
                session,
                changes_made,
                len(encrypted_settings),
                dry_run,
            )
        await engine.dispose()
    except Exception as e:
        typer.echo(f"Failed to rotate encryption: {e}")
        if not dry_run:
            typer.echo("Database may be in inconsistent state - restore from backup if needed")


@app.command(name="settings-rotate-encryption")
def settings_rotate_encryption(
    new_master_password: str = typer.Option(
        ...,
        "--new-password",
        prompt=True,
        hide_input=True,
        confirmation_prompt=True,
        help=(
            "New master password (prompted with confirmation when omitted; "
            "avoids leaving the value in shell history)"
        ),
    ),
    old_master_password: str = typer.Option(
        "",
        "--old-password",
        hide_input=True,
        help=(
            "Current master password. Default empty triggers the env/bootstrap "
            "lookup path; pass on the command line ONLY when scripting in a "
            "session whose history is not retained (otherwise the value leaks)."
        ),
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be changed without making changes"
    ),
) -> None:
    """Rotate encryption keys for encrypted settings.

    Args:
        new_master_password: New master password for encryption.
        old_master_password: Current master password.
        dry_run: Show changes without applying them.
    """
    asyncio.run(_run_encryption_rotation(new_master_password, old_master_password, dry_run))


@app.command(name="polygon-backfill-aggregates")
def polygon_backfill_aggregates(
    symbols: Annotated[
        list[str] | None,
        typer.Option("--symbol", "-s", help="Symbols to backfill (default: from settings)"),
    ] = None,
    all_mapped: bool = typer.Option(
        False, "--all", help="Backfill all symbols with Polygon mapping"
    ),
    multiplier: int = typer.Option(1, "--multiplier", "-m", help="Timeframe multiplier"),
    timespan: str = typer.Option("minute", "--timespan", "-t", help="Timespan: minute, hour, day"),
    days_back: int | None = typer.Option(None, "--days", "-d", help="Days back (from settings)"),
    resume: bool = typer.Option(True, _CLI_RESUME_FLAG, help="Resume from last timestamp"),
    save_csv: bool = typer.Option(True, "--csv/--no-csv", help="Save to CSV.gz files"),
) -> None:
    """Backfill historical aggregate data from Polygon API.

    Args:
        symbols: List of symbols to backfill.
        all_mapped: Backfill all symbols with Polygon mapping.
        multiplier: Timeframe multiplier.
        timespan: Timespan unit.
        days_back: Number of days to backfill.
        resume: Resume from last saved timestamp.
        save_csv: Save data to CSV.gz files.
    """

    async def run_aggregates_backfill() -> None:
        actual_days = days_back if days_back is not None else 30
        service = PolygonAggregatesBackfillService(
            symbols=symbols,
            all_mapped=all_mapped,
            multiplier=multiplier,
            timespan=timespan,
            days_back=actual_days,
            resume=resume,
            save_csv=save_csv,
        )
        try:
            symbol_source = (
                _CLI_SYMBOL_SOURCE_ALL_MAPPED if all_mapped else _CLI_SYMBOL_SOURCE_SETTINGS
            )
            msg = (
                f"Starting Polygon aggregates backfill "
                f"({multiplier}{timespan}, {actual_days} days, {symbol_source})..."
            )
            typer.echo(msg)
            await service.start()
            typer.echo("Polygon aggregates backfill complete!")
        except Exception as e:
            typer.echo(f"Error during Polygon aggregates backfill: {e}")
            raise typer.Exit(code=1) from e

    asyncio.run(run_aggregates_backfill())


@app.command(name="polygon-repair-splits")
def polygon_repair_splits(
    symbols: Annotated[
        list[str] | None,
        typer.Option("--symbol", "-s", help="Restrict the check to these native symbols"),
    ] = None,
    lookback_days: int = typer.Option(
        45, "--lookback-days", help="Trailing window of split executions to inspect"
    ),
    window_days: int = typer.Option(
        730, "--window-days", help="Re-fetch window in days (plan minute-data lookback limit)"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Detect and report stale split bases without repairing"
    ),
) -> None:
    """Detect and repair stale split-price bases in polygon equities.

    Pulls recent split events from the Polygon reference endpoint,
    confirms via the 1d close history which symbols actually carry an
    unadjusted break, and for those runs the full repair chain:
    re-fetch (adjusted), cache prune, SCD2 supersede, reload,
    re-synthesis, verification. Idempotent - repaired or uniformly
    based symbols are skipped. MUST run on the host (writes the
    data/polygon CSV cache, which containers mount read-only).

    Args:
        symbols: Restrict the check to these native symbols.
        lookback_days: Trailing window of split executions to inspect.
        window_days: Re-fetch window in days.
        dry_run: Detect and report without repairing.
    """

    async def run_split_repair() -> None:
        try:
            typer.echo(
                f"Starting polygon split repair (lookback {lookback_days}d, "
                f"window {window_days}d{', DRY-RUN' if dry_run else ''})..."
            )
            summary = await run_polygon_split_repair(
                symbols=symbols,
                lookback_days=lookback_days,
                window_days=window_days,
                dry_run=dry_run,
            )
            typer.echo(
                f"Splits seen: {summary.splits_seen}, in universe: "
                f"{len(summary.candidates)}, clean: {len(summary.clean)}, "
                f"repaired: {len(summary.repaired)}, unverified: "
                f"{len(summary.unverified)}, undetectable: "
                f"{len(summary.undetectable)}, pruned files: {summary.pruned_files}"
            )
            if summary.undetectable:
                typer.echo(
                    f"Micro-splits skipped (verify manually): {', '.join(summary.undetectable)}"
                )
            confirmed = [c for c in summary.candidates if c.break_day is not None]
            for candidate in confirmed:
                typer.echo(
                    f"  {candidate.native_symbol}: split "
                    f"{candidate.event.execution_date.isoformat()} "
                    f"{candidate.event.split_from}:{candidate.event.split_to}, "
                    f"break at {candidate.break_day}"
                )
            if summary.unverified:
                typer.echo(f"UNVERIFIED after repair: {', '.join(summary.unverified)}")
                raise typer.Exit(code=1)
            typer.echo("Polygon split repair complete!")
        except typer.Exit:
            raise
        except Exception as e:
            typer.echo(f"Error during polygon split repair: {e}")
            raise typer.Exit(code=1) from e

    asyncio.run(run_split_repair())


@app.command(name="polygon-load-csv")
def polygon_load_csv(
    symbols: Annotated[
        list[str] | None,
        typer.Option("--symbol", "-s", help="Symbols to load from cache (default: from settings)"),
    ] = None,
    all_mapped: bool = typer.Option(
        False, "--all", help="Load every archive symbol present in the cache"
    ),
    timespan: str = typer.Option("day", "--timespan", "-t", help="Timespan: minute, hour, day"),
    since: str | None = typer.Option(
        None, "--since", help="Earliest day to load, inclusive (YYYY-MM-DD)"
    ),
    until: str | None = typer.Option(
        None, "--until", help="Latest day to load, inclusive (YYYY-MM-DD)"
    ),
) -> None:
    """Load cached Polygon CSV candles into the database (cache-only).

    Reads the existing on-disk CSV cache and upserts candles into the
    database without contacting the Polygon API.

    Args:
        symbols: Symbols to load (native or Polygon format).
        all_mapped: Load every archive symbol present in the cache.
        timespan: Timespan unit selecting the cache subtree.
        since: Earliest day to load, inclusive (YYYY-MM-DD).
        until: Latest day to load, inclusive (YYYY-MM-DD).
    """
    since_day = date_type.fromisoformat(since) if since is not None else None
    until_day = date_type.fromisoformat(until) if until is not None else None

    async def run_csv_load() -> None:
        service = PolygonCsvLoaderService(
            symbols=symbols,
            all_mapped=all_mapped,
            timespan=timespan,
            since=since_day,
            until=until_day,
        )
        try:
            symbol_source = (
                _CLI_SYMBOL_SOURCE_ALL_MAPPED if all_mapped else _CLI_SYMBOL_SOURCE_SETTINGS
            )
            typer.echo(f"Starting Polygon CSV load ({timespan}, {symbol_source})...")
            await service.start()
            typer.echo("Polygon CSV load complete!")
        except Exception as e:
            typer.echo(f"Error during Polygon CSV load: {e}")
            raise typer.Exit(code=1) from e

    asyncio.run(run_csv_load())


@app.command(name="polygon-load-grouped-candles")
def polygon_load_grouped_candles(
    exchange: str = typer.Option(
        ...,
        "--exchange",
        "-e",
        help="Venue the persisted 1d bars live under (the leg's live-read/warmup "
        "venue, e.g. kraken) — NOT polygon",
    ),
    cut_date: str = typer.Option(
        ...,
        "--cut-date",
        help="First UTC day (YYYY-MM-DD) synthesized live persistence may own; "
        "backfill writes only days strictly before it",
    ),
    symbols: Annotated[
        list[str] | None,
        typer.Option("--symbol", "-s", help="Native symbols to load (default: from settings)"),
    ] = None,
    all_mapped: bool = typer.Option(False, "--all", help="Load every Polygon-mapped native symbol"),
    lookback_days: int = typer.Option(
        800, "--lookback-days", help="Calendar-day cap on the backward cache walk per symbol"
    ),
) -> None:
    """Load cached Polygon grouped-daily rows as 1d native candles (cache-only).

    Reads the on-disk grouped-daily cache and upserts each leg's days strictly
    before ``--cut-date`` as ``source='native', complete=True`` 1d candles under
    the ``--exchange`` venue, so the persisted plane holds the daily history the
    single-source read cutover and DB-first warmup (which resolve that same
    venue) depend on. Never contacts the Polygon API. The cut date keeps native
    backfill and synthesized live bars on disjoint day ranges.

    Args:
        exchange: Venue the persisted bars live under (e.g. ``kraken``); the
            leg's live-read/warmup venue, never ``polygon``.
        cut_date: First UTC day synthesized live persistence may own (YYYY-MM-DD).
        symbols: Native symbols to load (default: settings Polygon instruments).
        all_mapped: Load every Polygon-mapped native symbol.
        lookback_days: Calendar-day cap on the backward cache walk per symbol.
    """
    cut_day = date_type.fromisoformat(cut_date)
    venue = ExchangeEnum(exchange)

    async def run_grouped_candle_load() -> None:
        service = PolygonGroupedCandleLoaderService(
            symbols=symbols,
            exchange=venue,
            cut_date=cut_day,
            all_mapped=all_mapped,
            lookback_days=lookback_days,
        )
        try:
            symbol_source = (
                _CLI_SYMBOL_SOURCE_ALL_MAPPED if all_mapped else _CLI_SYMBOL_SOURCE_SETTINGS
            )
            typer.echo(
                f"Starting Polygon grouped-daily candle load "
                f"(exchange={venue.value}, cut_date={cut_day.isoformat()}, {symbol_source})..."
            )
            await service.start()
            typer.echo("Polygon grouped-daily candle load complete!")
        except Exception as e:
            typer.echo(f"Error during Polygon grouped-daily candle load: {e}")
            raise typer.Exit(code=1) from e

    asyncio.run(run_grouped_candle_load())


def _resolve_verify_candle_coverage_symbols(
    symbols: list[str] | None, configured_symbols: list[str]
) -> list[str]:
    """Given CLI and configured symbols, when verifying coverage, then choose symbols.

    Args:
        symbols: Explicit ``--symbol`` values from the CLI.
        configured_symbols: Settings-derived Polygon instruments.

    Returns:
        The native symbols to verify.

    Raises:
        typer.Exit: When no explicit symbols can be resolved.
    """
    syms = symbols if symbols else configured_symbols
    if not syms or syms == ["*"]:
        typer.echo("No explicit symbols to verify; pass --symbol")
        raise typer.Exit(code=1)
    return syms


def _echo_candle_coverage_report(report: CoverageReport) -> None:
    """Given a coverage report, when the CLI completes, then print its verdicts.

    Args:
        report: Coverage report returned by the application verifier.

    Returns:
        None.

    Raises:
        typer.Exit: When any entry is incomplete.
    """
    for entry in report.entries:
        status = "PASS" if entry.ok else "FAIL"
        typer.echo(
            f"[{status}] {entry.symbol} {entry.timeframe} ({entry.bars} bars): {entry.reason}"
        )
    if not report.ok:
        typer.echo("Candle coverage INCOMPLETE — not ready for single-source cutover")
        raise typer.Exit(code=1)
    typer.echo("Candle coverage OK")


async def _run_verify_candle_coverage_cmd(
    *,
    venue: ExchangeEnum,
    cut_day: date_type,
    symbols: list[str] | None,
    timeframes: list[str],
    min_bars: int,
    writer_lag_seconds: int,
) -> None:
    """Given parsed CLI options, when invoked, then run the coverage verifier.

    Args:
        venue: Live venue selected for verification.
        cut_day: Synthesis ownership boundary.
        symbols: Explicit CLI symbols, or None to use settings.
        timeframes: Timeframes selected for verification.
        min_bars: Minimum verification window depth.
        writer_lag_seconds: Grace seconds for writer flush latency.

    Returns:
        None.

    Raises:
        typer.Exit: When symbols cannot be resolved or coverage is incomplete.
        Exception: Propagates verifier failures unchanged.
    """
    settings = get_settings()
    syms = _resolve_verify_candle_coverage_symbols(
        symbols, list(settings.instruments.get(ExchangeEnum.POLYGON, []))
    )
    repo = get_repository(settings.db_url)
    try:
        report = await verify_candle_coverage(
            repo=repo,
            cache=None,
            exchange=venue,
            native_symbols=syms,
            timeframes=timeframes,
            cut_date=cut_day,
            as_of=datetime.now(UTC),
            writer_lag_s=writer_lag_seconds,
            min_bars=min_bars,
        )
    finally:
        engine = getattr(repo, "engine", None)
        if engine is not None:
            await engine.dispose()
    _echo_candle_coverage_report(report)


@app.command(name="verify-candle-coverage")
def verify_candle_coverage_cmd(
    exchange: str = typer.Option(
        ...,
        "--exchange",
        "-e",
        help="Live venue the persisted bars + read path resolve under (e.g. kraken)",
    ),
    cut_date: str = typer.Option(
        ..., "--cut-date", help="Synthesis ownership boundary (YYYY-MM-DD); decides 1d provenance"
    ),
    symbols: Annotated[
        list[str] | None,
        typer.Option("--symbol", "-s", help="Native symbols to verify (default: from settings)"),
    ] = None,
    timeframes: Annotated[
        list[str] | None,
        typer.Option("--timeframe", "-t", help="Timeframes to verify (default: 5m..1d)"),
    ] = None,
    min_bars: int = typer.Option(30, "--min-bars", help="Minimum persisted rows required per pair"),
    writer_lag_seconds: int = typer.Option(
        120,
        "--writer-lag-seconds",
        help="Grace seconds for the writer to flush the just-closed bar",
    ),
) -> None:
    """Verify the persisted candle plane is ready for the single-source cutover.

    Read-only, structural only. For each ``(symbol, timeframe)`` under
    ``--exchange`` it checks the persisted plane has a fresh, gap-free, complete,
    correctly-tagged canonical window (``1d`` native before ``--cut-date``,
    synthesized at/after, with the seam verified contiguous). Exits 1 when any
    pair is incomplete, so it can gate the slice-4 read cutover. The
    ``derive_snaps`` OHLCV-parity rung needs a genuinely live (SUB-warm) cache as
    an independent witness, so it is NOT run here (deferred to an in-process
    slice-4 cutover check); the standalone CLI is structural-only.

    Args:
        exchange: Live venue the persisted bars + read path resolve under (never
            ``polygon``).
        cut_date: Synthesis ownership boundary (YYYY-MM-DD).
        symbols: Native symbols to verify (default: settings Polygon instruments).
        timeframes: Timeframes to verify (default: 5m/15m/30m/1h/4h/1d).
        min_bars: Minimum window depth (slots) verified per pair.
        writer_lag_seconds: Grace seconds for the writer to flush the just-closed bar.
    """
    venue = ExchangeEnum(exchange)
    if venue == ExchangeEnum.POLYGON:
        typer.echo(
            "polygon is the CSV cache segment, not a read venue; verify under the live venue"
        )
        raise typer.Exit(code=1)
    cut_day = date_type.fromisoformat(cut_date)
    tfs = timeframes if timeframes else list(VERIFIABLE_TIMEFRAMES)
    asyncio.run(
        _run_verify_candle_coverage_cmd(
            venue=venue,
            cut_day=cut_day,
            symbols=symbols,
            timeframes=tfs,
            min_bars=min_bars,
            writer_lag_seconds=writer_lag_seconds,
        )
    )


@app.command(name="kraken-futures-backfill-candles")
def kraken_futures_backfill_candles(
    symbols: Annotated[
        list[str] | None,
        typer.Option("--symbol", "-s", help="Native symbols to backfill (e.g., BTC-USD-PERP)"),
    ] = None,
    all_symbols: bool = typer.Option(False, "--all", help="Backfill all Kraken Futures symbols"),
    timeframe: str = typer.Option(
        "1h", "--timeframe", "-t", help="Candle interval: 1m, 1h, 4h, 1d"
    ),
    days_back: int = typer.Option(90, "--days", "-d", help="Days back to fetch"),
    resume: bool = typer.Option(True, _CLI_RESUME_FLAG, help="Resume from latest candle"),
) -> None:
    """Backfill historical OHLCV candles from Kraken Futures.

    Args:
        symbols: List of native symbols to backfill.
        all_symbols: Backfill all mapped Kraken Futures symbols.
        timeframe: Candle interval string.
        days_back: Number of days to backfill.
        resume: Resume from last stored candle timestamp.
    """

    async def run_backfill() -> None:
        service = KrakenFuturesAggregatesBackfillService(
            symbols=symbols,
            all_symbols=all_symbols,
            timeframe=timeframe,
            days_back=days_back,
            resume=resume,
        )
        try:
            symbol_source = (
                _CLI_SYMBOL_SOURCE_ALL_MAPPED if all_symbols else _CLI_SYMBOL_SOURCE_SETTINGS
            )
            typer.echo(
                f"Starting Kraken Futures candle backfill "
                f"({timeframe}, {days_back} days, {symbol_source})..."
            )
            await service.start()
            typer.echo("Kraken Futures candle backfill complete!")
        except Exception as e:
            typer.echo(f"Error during Kraken Futures candle backfill: {e}")
            raise typer.Exit(code=1) from e

    asyncio.run(run_backfill())


@app.command(name="kraken-equities-backfill-candles")
def kraken_equities_backfill_candles(
    symbols: Annotated[
        list[str] | None,
        typer.Option("--symbol", "-s", help="Native symbols to backfill (e.g., MNQM6-CME)"),
    ] = None,
    all_symbols: bool = typer.Option(False, "--all", help="Backfill all Kraken Equities symbols"),
    timeframe: str = typer.Option(
        "1h", "--timeframe", "-t", help="Candle interval: 1m, 5m, 15m, 30m, 1h, 1d"
    ),
    days_back: int = typer.Option(30, "--days", "-d", help="Days back to fetch"),
    resume: bool = typer.Option(True, _CLI_RESUME_FLAG, help="Resume from latest candle"),
) -> None:
    """Backfill historical OHLCV candles from Kraken Equities (TradFi FCM).

    Hits the internal ``iapi.kraken.com/api/internal/markets/{ws_symbol}/
    ticker/history`` endpoint (market-data-only; no order placement).
    Response is ~10-minute delayed per FCM policy. The endpoint is
    undocumented and may change without notice; per-chunk upstream
    failures surface as ``RuntimeError`` so outages are distinguishable
    from legitimately-empty candle windows.

    Args:
        symbols: List of native symbols to backfill.
        all_symbols: Backfill all mapped Kraken Equities symbols.
        timeframe: Candle interval accepted by the iapi endpoint.
        days_back: Number of days to backfill.
        resume: Resume from last stored candle timestamp.
    """

    async def run_backfill() -> None:
        service = KrakenEquitiesAggregatesBackfillService(
            symbols=symbols,
            all_symbols=all_symbols,
            timeframe=timeframe,
            days_back=days_back,
            resume=resume,
        )
        try:
            symbol_source = (
                _CLI_SYMBOL_SOURCE_ALL_MAPPED if all_symbols else _CLI_SYMBOL_SOURCE_SETTINGS
            )
            typer.echo(
                f"Starting Kraken Equities candle backfill "
                f"({timeframe}, {days_back} days, {symbol_source})..."
            )
            await service.start()
            typer.echo("Kraken Equities candle backfill complete!")
        except Exception as e:
            typer.echo(f"Error during Kraken Equities candle backfill: {e}")
            raise typer.Exit(code=1) from e

    asyncio.run(run_backfill())


@app.command(name="backfill-candles-from-trades")
def backfill_candles_from_trades(
    exchange: Annotated[str, typer.Option("--exchange", help="Exchange to backfill")],
    start: Annotated[str, typer.Option("--start", help="UTC start date or datetime")],
    end: Annotated[str, typer.Option("--end", help="UTC end date or datetime")],
    symbols: Annotated[
        list[str] | None,
        typer.Option("--symbol", "-s", help="Native symbols to backfill"),
    ] = None,
    all_symbols: bool = typer.Option(False, "--all", help="Backfill all active symbols"),
) -> None:
    """Backfill calculated 1-minute candles from persisted trades.

    Args:
        exchange: Exchange whose trades should be aggregated.
        start: Inclusive UTC event-time lower bound.
        end: Inclusive UTC event-time upper bound.
        symbols: Native symbols to backfill.
        all_symbols: Backfill all active symbols on the exchange.
    """

    async def run_backfill() -> None:
        try:
            service = TradeCandleBackfillService(
                exchange=ExchangeEnum(exchange),
                start=_parse_utc(start),
                end=_parse_utc(end),
                symbols=symbols,
                all_symbols=all_symbols,
            )
            symbol_source = _CLI_SYMBOL_SOURCE_ALL_MAPPED if all_symbols else "selected symbols"
            typer.echo(
                f"Starting trade candle backfill ({exchange}, {start} to {end}, {symbol_source})..."
            )
            await service.start()
            typer.echo("Trade candle backfill complete!")
        except Exception as e:
            typer.echo(f"Error during trade candle backfill: {e}")
            raise typer.Exit(code=1) from e

    asyncio.run(run_backfill())


@app.command(name="backfill-synthesized-candles")
def backfill_synthesized_candles(
    exchange: Annotated[str, typer.Option("--exchange", help="Exchange to backfill")],
    start: Annotated[str, typer.Option("--start", help="UTC start date or datetime")],
    end: Annotated[str, typer.Option("--end", help="UTC end date or datetime")],
    symbols: Annotated[
        list[str] | None,
        typer.Option("--symbol", "-s", help="Native symbols to backfill"),
    ] = None,
    all_symbols: bool = typer.Option(False, "--all", help="Backfill all active symbols"),
    timeframes: Annotated[
        str,
        typer.Option("--timeframes", help="Comma-separated higher timeframes to synthesize"),
    ] = "5m,15m,30m,1h,4h,1d",
    cut_date: Annotated[
        str | None,
        typer.Option("--cut-date", help="UTC date where synthesized 1d ownership begins"),
    ] = None,
) -> None:
    """Backfill synthesized higher-timeframe candles from persisted 1m candles.

    Args:
        exchange: Exchange whose 1m candles should be aggregated.
        start: Inclusive UTC 1m candle lower bound.
        end: UTC upper bound used to seal higher-timeframe windows.
        symbols: Native symbols to backfill.
        all_symbols: Backfill all active symbols on the exchange.
        timeframes: Comma-separated higher timeframe labels.
        cut_date: First UTC day synthesized ``1d`` rows may own.
    """

    async def run_backfill() -> None:
        try:
            requested_timeframes = [
                timeframe.strip() for timeframe in timeframes.split(",") if timeframe.strip()
            ]
            parsed_cut_date = date_type.fromisoformat(cut_date) if cut_date is not None else None
            service = SynthesizedCandleBackfillService(
                exchange=ExchangeEnum(exchange),
                start=_parse_utc(start),
                end=_parse_utc(end),
                symbols=symbols,
                all_symbols=all_symbols,
                timeframes=requested_timeframes,
                cut_date=parsed_cut_date,
            )
            symbol_source = _CLI_SYMBOL_SOURCE_ALL_MAPPED if all_symbols else "selected symbols"
            typer.echo(
                f"Starting synthesized candle backfill "
                f"({exchange}, {start} to {end}, {timeframes}, {symbol_source})..."
            )
            await service.start()
            typer.echo("Synthesized candle backfill complete!")
        except Exception as e:
            typer.echo(f"Error during synthesized candle backfill: {e}")
            raise typer.Exit(code=1) from e

    asyncio.run(run_backfill())


@app.command(name="update-kraken-futures-funding-rates")
def update_kraken_futures_funding_rates(
    symbols: Annotated[
        list[str] | None,
        typer.Option(
            "--symbol",
            "-s",
            help="Native perpetual symbols (e.g., BTC-USD-PERP)",
        ),
    ] = None,
    all_symbols: bool = typer.Option(
        False,
        "--all",
        help="Backfill all Kraken Futures perpetuals",
    ),
) -> None:
    """Backfill historical funding rates for Kraken Futures perpetuals.

    Fetches all available historical rates from the Kraken Futures API
    and persists them to the funding_rates table. Duplicates are
    silently skipped via the partial unique index.

    Args:
        symbols: List of native perpetual symbols to backfill.
        all_symbols: Backfill all mapped Kraken Futures perpetuals.
    """

    async def run_funding_backfill() -> None:
        service = KrakenFuturesFundingBackfillService(
            symbols=symbols,
            all_symbols=all_symbols,
        )
        try:
            symbol_source = (
                _CLI_SYMBOL_SOURCE_ALL_MAPPED if all_symbols else _CLI_SYMBOL_SOURCE_SETTINGS
            )
            typer.echo(f"Starting Kraken Futures funding rate backfill ({symbol_source})...")
            await service.start()
            typer.echo("Kraken Futures funding rate backfill complete!")
        except Exception as e:
            typer.echo(f"Error during Kraken Futures funding rate backfill: {e}")
            raise typer.Exit(code=1) from e

    asyncio.run(run_funding_backfill())


@app.command(name="polygon-backfill-grouped")
def polygon_backfill_grouped(
    market_type: str = typer.Option(
        "crypto", "--market", "-m", help="Market type: crypto, stocks, fx"
    ),
    days: int = typer.Option(3, "--days", "-d", help="Number of recent days to fetch"),
    locale: str = typer.Option("global", "--locale", "-l", help="Locale: global, us"),
    save_csv: bool = typer.Option(True, "--csv/--no-csv", help="Save to CSV.gz files"),
    adjusted: bool = typer.Option(True, "--adjusted/--unadjusted", help="Use adjusted prices"),
) -> None:
    """Backfill grouped daily data from Polygon API.

    Args:
        market_type: Market type (crypto, stocks, fx).
        days: Number of recent days to fetch.
        locale: Market locale (global, us).
        save_csv: Save data to CSV.gz files.
        adjusted: Use adjusted prices.
    """

    async def run_grouped_backfill() -> None:
        service = PolygonGroupedDailyBackfillService(
            market_type=market_type,
            days=days,
            locale=locale,
            save_csv=save_csv,
            adjusted=adjusted,
        )
        try:
            msg = f"Starting Polygon grouped daily backfill ({market_type}, {days} days)..."
            typer.echo(msg)
            await service.start()
            typer.echo("Polygon grouped daily backfill complete!")
        except Exception as e:
            typer.echo(f"Error during Polygon grouped backfill: {e}")
            raise typer.Exit(code=1) from e

    asyncio.run(run_grouped_backfill())


@app.command(name="archive")
def archive_data(
    table: str = typer.Option(
        "candles",
        help="Table to archive (candles, candles-audit, orders, symbols, ... or any event/state table).",
    ),
    exchange: str | None = typer.Option(None, help="Exchange filter (e.g. polygon, kraken)."),
    symbol: str | None = typer.Option(None, help="Native symbol filter (e.g. BTC-USD)."),
    timeframe: str = typer.Option("1m", help="Candle timeframe (e.g. 1m, 1h, 1d)."),
    day: str | None = typer.Option(None, help="Single day to archive (YYYY-MM-DD)."),
    from_date: str | None = typer.Option(None, "--from", help="Start of date range (YYYY-MM-DD)."),
    to_date: str | None = typer.Option(None, "--to", help="End of date range (YYYY-MM-DD)."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Report counts without writing files."),
    purge: bool = typer.Option(
        False,
        "--purge",
        help=(
            "Delete exported rows from DB after writing. Not supported for "
            "executions: they are the replay evidence for the spot "
            "reconciliation counted proof and are never deleted."
        ),
    ),
    closed_only: bool = typer.Option(
        False, "--closed-only", help="Only closed SCD2 versions (candles-audit and state tables)."
    ),
    output_dir: str = typer.Option("data", help="Base output directory."),
) -> None:
    """Export data to CSV archive files.

    For candles: polygon-compatible cache files under
    ``{output_dir}/{exchange}/cache/{timespan}/{archive_symbol}/{year}/``.

    For candles-audit: all SCD2 versions with full temporal metadata
    under ``{output_dir}/archive/candles/{exchange}/{archive_symbol}/{year}/``.

    For event tables (ticks, trades, signals, executions, telemetry,
    control): full audit rows under ``{output_dir}/archive/{table}/...``.

    Merges with existing files and deduplicates rows.

    Args:
        table: Table to archive.
        exchange: Exchange filter.
        symbol: Native symbol filter (resolved to archive_symbol).
        timeframe: Candle timeframe (candles/candles-audit only).
        day: Single day to archive.
        from_date: Start of date range.
        to_date: End of date range.
        dry_run: Count only.
        purge: Delete exported rows from DB. Refused for executions.
        closed_only: Only closed SCD2 versions (candles-audit only).
        output_dir: Base output directory.
    """
    valid_tables = {"candles", "candles-audit", *EVENT_TABLES, *STATE_TABLES}
    if table not in valid_tables:
        typer.echo(f"Error: unknown table '{table}'")
        raise typer.Exit(code=1)

    if table == "candles" and purge:
        typer.echo("Error: --purge is not supported for candle cache export")
        raise typer.Exit(code=1)

    if table == "executions" and purge:
        typer.echo(
            "Error: --purge is not supported for executions; they are the "
            "replay evidence for the spot reconciliation counted proof and "
            "cannot be safely deleted concurrently with anchor writes"
        )
        raise typer.Exit(code=1)

    scd2_tables = {"candles-audit", *STATE_TABLES}
    if table in scd2_tables and purge and not closed_only:
        typer.echo(f"Error: --purge requires --closed-only for {table}")
        raise typer.Exit(code=1)

    if day is not None:
        start = date_type.fromisoformat(day)
        end = start
    elif from_date is not None and to_date is not None:
        start = date_type.fromisoformat(from_date)
        end = date_type.fromisoformat(to_date)
    else:
        typer.echo("Error: provide --day or --from + --to")
        raise typer.Exit(code=1)

    bootstrap = BootstrapSettingsLoader()
    repo = DatabaseRepository(bootstrap.db_url)
    archive_symbol_filter = _resolve_archive_symbol_filter(repo, symbol)
    result = _run_archive_export(
        table,
        repo,
        Path(output_dir),
        archive_symbol_filter,
        exchange,
        timeframe,
        start,
        end,
        closed_only,
        dry_run,
        purge,
    )
    msg = f"Done: {result.rows_exported} rows in {result.files_written} files"
    if result.rows_purged > 0:
        msg += f" ({result.rows_purged} purged)"
    typer.echo(msg)
    repo.dispose()


def _resolve_archive_symbol_filter(
    repo: DatabaseRepository,
    symbol: str | None,
) -> str | None:
    """Resolve --symbol CLI argument to archive_symbol via DB lookup."""
    if symbol is None:
        return None
    result = repo.resolve_native_to_archive_symbol(symbol)
    if result is None:
        typer.echo(f"Warning: symbol '{symbol}' not found in DB, using safe_path fallback")
        return safe_path(symbol)
    return result


def _run_archive_export(
    table: str,
    repo: DatabaseRepository,
    output_dir: Path,
    archive_symbol_filter: str | None,
    exchange: str | None,
    timeframe: str,
    start: date_type,
    end: date_type,
    closed_only: bool,
    dry_run: bool,
    purge: bool,
) -> ExportResult:
    """Dispatch archive export to the appropriate archiver class."""
    label = "Dry run:" if dry_run else "Exporting"

    if table == "candles":
        typer.echo(f"{label} candle cache ({timeframe}) from {start} to {end}...")
        return CandleCacheArchiver(repo, output_dir).export(
            exchange=exchange,
            archive_symbol=archive_symbol_filter,
            timeframe=timeframe,
            day_start=start,
            day_end=end,
            dry_run=dry_run,
        )

    if table == "candles-audit":
        mode = "closed-only" if closed_only else "all versions"
        typer.echo(f"{label} candle audit ({timeframe}, {mode}) from {start} to {end}...")
        return CandleAuditArchiver(repo, output_dir).export(
            exchange=exchange,
            archive_symbol=archive_symbol_filter,
            timeframe=timeframe,
            day_start=start,
            day_end=end,
            closed_only=closed_only,
            dry_run=dry_run,
            purge=purge,
        )

    if table in EVENT_TABLES:
        typer.echo(f"{label} {table} from {start} to {end}...")
        return EventArchiver(repo, output_dir).export(
            table=table,
            exchange=exchange,
            archive_symbol=archive_symbol_filter,
            day_start=start,
            day_end=end,
            dry_run=dry_run,
            purge=purge,
        )

    mode = "closed-only" if closed_only else "all versions"
    typer.echo(f"{label} {table} ({mode}) from {start} to {end}...")
    return StateArchiver(repo, output_dir).export(
        table=table,
        day_start=start,
        day_end=end,
        closed_only=closed_only,
        dry_run=dry_run,
        purge=purge,
    )


@app.command(name="restore")
def restore_data(
    table: str = typer.Option(..., help="Table name to restore into."),
    file: str | None = typer.Option(None, help="Single CSV file to restore."),
    directory: str | None = typer.Option(None, "--dir", help="Directory of CSV files to restore."),
    source: str = typer.Option("audit", help="Restore source (audit)."),
) -> None:
    """Restore archived CSV data back into the database.

    Reads CSV files exported by ``snapper archive`` and inserts rows
    back into the database, deduplicating against existing data.

    Args:
        table: Table name.
        file: Single CSV file path.
        directory: Directory to scan recursively for CSV files.
        source: Restore mode (audit for full history).
    """
    if file is None and directory is None:
        typer.echo("Error: provide --file or --dir")
        raise typer.Exit(code=1)

    paths: list[Path] = []
    if file is not None:
        p = Path(file)
        if not p.exists():
            typer.echo(f"Error: file not found: {file}")
            raise typer.Exit(code=1)
        paths.append(p)
    if directory is not None:
        d = Path(directory)
        if not d.is_dir():
            typer.echo(f"Error: directory not found: {directory}")
            raise typer.Exit(code=1)
        paths.extend(sorted(d.rglob("*.csv")))

    if not paths:
        typer.echo("No CSV files found")
        raise typer.Exit(code=1)

    bootstrap = BootstrapSettingsLoader()
    repo = DatabaseRepository(bootstrap.db_url)
    restorer = ArchiveRestorer(repo)
    typer.echo(f"Restoring {table} from {len(paths)} file(s) (source={source})...")
    result = restorer.restore(table=table, paths=paths, source=source)
    typer.echo(
        f"Done: {result.rows_inserted} inserted, {result.rows_skipped} skipped "
        f"({result.files_processed} files)"
    )
    repo.dispose()


@app.command(name="update-underlyings")
def update_underlyings(
    force: Annotated[bool, typer.Option("--force", "-f")] = False,
) -> None:
    """Sync underlying asset mappings from YAML definition file.

    Args:
        force: Bypass safety guard for stale cleanup (>25% removal).
    """

    async def run_update() -> None:
        bootstrap = BootstrapSettingsLoader()
        updater = UnderlyingUpdater(db_url=bootstrap.db_url, force=force)
        typer.echo("Updating underlying asset mappings...")
        await updater.run()
        typer.echo("Underlying asset mappings updated successfully")

    asyncio.run(run_update())


@app.command(name="build-continuous")
def build_continuous(
    ticker: Annotated[str, typer.Argument(help="Underlying asset ticker (e.g. SPX, GOLD)")],
    exchange: Annotated[str, typer.Argument(help="Exchange to source contracts from")],
    contract_family: Annotated[str, typer.Argument(help="Product root (e.g. ES, GC)")],
    timeframe: Annotated[str, typer.Option("--timeframe", "-t")] = "1d",
    method: Annotated[str, typer.Option("--method", "-m")] = "panama",
    start: Annotated[str, typer.Option("--start")] = "",
    end: Annotated[str, typer.Option("--end")] = "",
    rollover_days: Annotated[int, typer.Option("--rollover-days", min=0, max=365)] = 0,
) -> None:
    """Build and display continuous contract series for an underlying.

    Example: snapper build-continuous SPX kraken_equities ES --timeframe 1d

    Args:
        ticker: Underlying asset ticker (e.g. SPX, GOLD).
        exchange: Exchange to source contracts from.
        contract_family: Product root (e.g. ES, GC).
        timeframe: Candle timeframe (e.g. 1d, 1h).
        method: Adjustment method (unadjusted, ratio, panama).
        start: Series start time (ISO format). Defaults to 365 days ago.
        end: Series end time (ISO format). Defaults to now.
        rollover_days: Days before expiry to roll. Defaults to 0.
    """

    async def run_build() -> None:
        bootstrap = BootstrapSettingsLoader()
        repo = get_repository(bootstrap.db_url)
        builder = ContinuousContractBuilder(repository=repo)
        now = datetime.now(UTC)
        s = _parse_utc(start) if start else now - timedelta(days=365)
        e = _parse_utc(end) if end else now
        underlying = await repo.get_underlying_by_ticker(ticker, now)
        if underlying is None:
            typer.echo(f"Underlying not found: {ticker}", err=True)
            raise typer.Exit(code=1)
        result = await builder.build(
            underlying_public_id=underlying["public_id"],
            exchange=exchange,
            contract_family=contract_family,
            timeframe=timeframe,
            start=s,
            end=e,
            method=method,
            rollover_days_before=rollover_days,
            as_of=now,
        )
        typer.echo(
            f"Contracts used: {len(result.contracts_used)} ({', '.join(result.contracts_used)})"
        )
        typer.echo(f"Total bars: {len(result.candles)}")
        typer.echo(f"Roll points: {len(result.roll_points)}")
        for rp in result.roll_points:
            typer.echo(
                f"  {rp.from_contract} -> {rp.to_contract} "
                f"at {rp.roll_at.isoformat()} (adj={rp.adjustment})"
            )
        if result.failed_roll:
            typer.echo(
                f"WARNING: Series truncated at {result.failed_roll.from_contract} "
                f"-> {result.failed_roll.to_contract}"
            )

    asyncio.run(run_build())


@app.command(name="backtest-run")
def backtest_run(
    strategy: Annotated[str, typer.Option("--strategy", help="Strategy class name")],
    instrument: Annotated[str, typer.Option("--instrument", help="Native instrument symbol")],
    exchange: Annotated[str, typer.Option("--exchange", help="Exchange name")],
    start: Annotated[str, typer.Option("--start", help="Start date (ISO format)")],
    end: Annotated[str, typer.Option("--end", help="End date (ISO format)")],
    timeframe: Annotated[str, typer.Option("--timeframe")] = "1h",
    initial_cash: Annotated[float, typer.Option("--initial-cash")] = 10_000.0,
    wallet: Annotated[str, typer.Option("--wallet", help="Wallet public ID")] = "cli",
    params_json: Annotated[str, typer.Option("--params", help="Strategy params as JSON")] = "{}",
    execution_mode: Annotated[str, typer.Option("--execution-mode")] = "direct_db",
    fill_model: Annotated[str, typer.Option("--fill-model")] = "market",
    slippage_bps: Annotated[float, typer.Option("--slippage-bps")] = 0.0,
    commission_bps: Annotated[float, typer.Option("--commission-bps")] = 0.0,
) -> None:
    """Run a backtest synchronously via CLI.

    Creates a backtest run record, executes the engine, and persists results.
    Unlike the API endpoint, this runs in-process without the process framework.

    Args:
        strategy: Registered strategy class name.
        instrument: Native instrument symbol (resolved to its public ID for
            persistence; the engine config keys candles off the native symbol).
        exchange: Exchange name for candle data.
        start: Backtest start date in ISO format.
        end: Backtest end date in ISO format.
        timeframe: Candle timeframe (default: 1h).
        initial_cash: Starting cash balance (default: 10000).
        wallet: Wallet public ID for run ownership.
        params_json: Strategy parameters as JSON string.
        execution_mode: Engine type (direct_db / zmq_replay).
        fill_model: Fill simulation model.
        slippage_bps: Per-fill slippage in basis points.
        commission_bps: Per-fill commission in basis points.
    """
    start_dt = _parse_utc(start)
    end_dt = _parse_utc(end)
    strategy_params: dict[str, Any] = json_mod.loads(params_json)

    async def run_backtest() -> None:
        bootstrap = BootstrapSettingsLoader()
        repo = get_repository(bootstrap.db_url)
        bt_repo = BacktestRepository(cast(Any, repo).session_factory)
        tracker = SequenceTracker()
        now = datetime.now(UTC)

        resolved_instrument_public_id = await repo.get_instrument_public_id_by_symbol(
            native_symbol=instrument,
            exchange=exchange,
            as_of=now,
        )
        if resolved_instrument_public_id is None:
            typer.echo(
                f"Error: no active instrument resolves for symbol '{instrument}' "
                f"on exchange '{exchange}'",
                err=True,
            )
            raise typer.Exit(1)

        config = BacktestConfig(
            strategy_class=strategy,
            instruments={exchange: [instrument]},
            start_date=start_dt,
            end_date=end_dt,
            wallet_public_id=wallet,
            initial_balance=initial_cash,
            timeframe=timeframe,
            strategy_params=strategy_params,
            execution_mode=BacktestExecutionMode(execution_mode),
            fill_model=BacktestFillModel(fill_model),
            slippage_bps=slippage_bps,
            commission_bps=commission_bps,
        )
        try:
            config_hash = compute_fingerprint(config, for_pairing=True)
        except Exception:
            config_hash = None

        _, public_id = await bt_repo.create_run(
            row={
                "wallet_public_id": wallet,
                "strategy_name": strategy,
                "strategy_params": strategy_params,
                "instrument_public_id": resolved_instrument_public_id,
                "exchange": exchange,
                "timeframe": timeframe,
                "start_date": start_dt,
                "end_date": end_dt,
                "initial_cash": initial_cash,
                "status": "running",
                "execution_mode": execution_mode,
                "fill_model": fill_model,
                "slippage_bps": slippage_bps,
                "commission_bps": commission_bps,
                "config_hash": config_hash,
                "created_by_user_id": "cli",
                "session_id": tracker.session_id,
                "sequence_id": tracker.next_sequence("cli"),
                "timestamp": now,
            },
            bus_time=now,
            session_id=tracker.session_id,
            sequence_id=tracker.next_sequence("cli"),
        )
        typer.echo(f"Created backtest run: {public_id}")

        try:
            engine = DirectDbEngine(repo, now)
            collector = ResultCollector()
            await engine.run(public_id, config, collector)

            persist_now = datetime.now(UTC)
            if collector.signals:
                await bt_repo.insert_signals_batch(
                    collector.signals,
                    bus_time=persist_now,
                    session_id=tracker.session_id,
                    sequence_id=tracker.next_sequence("cli"),
                )
            if collector.trades:
                await bt_repo.insert_trades_batch(
                    collector.trades,
                    bus_time=persist_now,
                    session_id=tracker.session_id,
                    sequence_id=tracker.next_sequence("cli"),
                )
            if collector.equity_points:
                await bt_repo.insert_equity_points_batch(
                    collector.equity_points,
                    bus_time=persist_now,
                    session_id=tracker.session_id,
                    sequence_id=tracker.next_sequence("cli"),
                )

            metrics = compute_metrics(
                collector.equity_points, collector.trades, initial_balance=initial_cash
            )
            await bt_repo.insert_result(
                BacktestResultInsertRow(
                    run_public_id=public_id,
                    total_trades=metrics.total_trades,
                    winning_trades=metrics.winning_trades,
                    losing_trades=metrics.losing_trades,
                    total_pnl=metrics.total_pnl,
                    max_drawdown=metrics.max_drawdown,
                    sharpe_ratio=metrics.sharpe_ratio,
                    win_rate=metrics.win_rate,
                    profit_factor=metrics.profit_factor,
                    final_equity=metrics.final_equity,
                    max_equity=metrics.max_equity,
                    sortino_ratio=metrics.sortino_ratio,
                    cagr=metrics.cagr,
                    calmar_ratio=metrics.calmar_ratio,
                    expectancy=metrics.expectancy,
                    avg_trade_pnl=metrics.avg_trade_pnl,
                    max_drawdown_duration_seconds=metrics.max_drawdown_duration_seconds,
                    exposure_ratio=metrics.exposure_ratio,
                    turnover_ratio=metrics.turnover_ratio,
                    extra_metrics={},
                    session_id=tracker.session_id,
                    sequence_id=tracker.next_sequence("cli"),
                    timestamp=persist_now,
                ),
                bus_time=persist_now,
                session_id=tracker.session_id,
                sequence_id=tracker.next_sequence("cli"),
            )
            for warning in metrics.warnings:
                await bt_repo.insert_event(
                    row={
                        "run_public_id": public_id,
                        "event_type": "metric_warning",
                        "detail": {"metric": warning.metric, "reason": warning.reason},
                        "session_id": tracker.session_id,
                        "sequence_id": tracker.next_sequence("cli"),
                        "timestamp": persist_now,
                    },
                    bus_time=persist_now,
                    session_id=tracker.session_id,
                    sequence_id=tracker.next_sequence("cli"),
                )

            final_now = datetime.now(UTC)
            await bt_repo.update_run_status(
                public_id=public_id,
                new_status="completed",
                bus_time=final_now,
                session_id=tracker.session_id,
                sequence_id=tracker.next_sequence("cli"),
                completed_at=final_now,
            )
            typer.echo(
                f"Backtest completed: {metrics.total_trades} trades, "
                f"PnL={metrics.total_pnl:.2f}, "
                f"Sharpe={metrics.sharpe_ratio:.3f}, "
                f"MaxDD={metrics.max_drawdown:.2%}"
            )
        except Exception as exc:
            fail_now = datetime.now(UTC)
            await bt_repo.update_run_status(
                public_id=public_id,
                new_status="failed",
                bus_time=fail_now,
                session_id=tracker.session_id,
                sequence_id=tracker.next_sequence("cli"),
                error=str(exc)[:1024],
            )
            typer.echo(f"Backtest failed: {exc}", err=True)
            raise typer.Exit(code=1) from exc

    asyncio.run(run_backtest())


@app.command(name="backtest-list")
def backtest_list(
    strategy_filter: Annotated[str | None, typer.Option("--strategy")] = None,
    status_filter: Annotated[str | None, typer.Option("--status")] = None,
    wallet_filter: Annotated[str | None, typer.Option("--wallet")] = None,
    limit: Annotated[int, typer.Option("--limit")] = 20,
) -> None:
    """List backtest runs with optional filters.

    Args:
        strategy_filter: Filter by strategy name.
        status_filter: Filter by status.
        wallet_filter: Filter by wallet public ID.
        limit: Maximum runs to show.
    """

    async def run_list() -> None:
        bootstrap = BootstrapSettingsLoader()
        repo = get_repository(bootstrap.db_url)
        bt_repo = BacktestRepository(cast(Any, repo).session_factory)
        now = datetime.now(UTC)

        runs = await bt_repo.list_runs(
            as_of=now,
            wallet_public_id=wallet_filter,
            strategy=strategy_filter,
            status=status_filter,
            limit=limit,
        )
        if not runs:
            typer.echo("No backtest runs found.")
            return

        for run in runs:
            status_str = run["status"]
            typer.echo(
                f"  {run['public_id'][:12]}  "
                f"{run['strategy_name']:20s}  "
                f"{status_str:18s}  "
                f"{run['instrument_public_id']:12s}  "
                f"{run['exchange']:10s}  "
                f"{run['start_date'].strftime('%Y-%m-%d')} → "
                f"{run['end_date'].strftime('%Y-%m-%d')}"
            )
        typer.echo(f"\n{len(runs)} run(s) shown.")

    asyncio.run(run_list())


@app.command(name="backtest-cancel")
def backtest_cancel(
    run_id: Annotated[str, typer.Argument(help="Run public ID to cancel")],
) -> None:
    """Cancel a pending or running backtest run.

    Args:
        run_id: Public ID of the backtest run.
    """

    async def run_cancel() -> None:
        bootstrap = BootstrapSettingsLoader()
        repo = get_repository(bootstrap.db_url)
        bt_repo = BacktestRepository(cast(Any, repo).session_factory)
        tracker = SequenceTracker()
        now = datetime.now(UTC)

        run = await bt_repo.get_run(run_id, as_of=now)
        if run is None:
            typer.echo(f"Run not found: {run_id}", err=True)
            raise typer.Exit(code=1)
        if run["status"] not in ("pending", "running"):
            typer.echo(f"Cannot cancel run in status '{run['status']}'", err=True)
            raise typer.Exit(code=1)

        await bt_repo.update_run_status(
            public_id=run_id,
            new_status="cancel_requested",
            bus_time=now,
            session_id=tracker.session_id,
            sequence_id=tracker.next_sequence("cli"),
        )
        typer.echo(f"Cancel requested for run {run_id}")

    asyncio.run(run_cancel())


@app.command(name="backtest-rerun")
def backtest_rerun(
    run_id: Annotated[str, typer.Argument(help="Run public ID to re-run")],
) -> None:
    """Re-run a backtest with the same configuration.

    Args:
        run_id: Public ID of the original backtest run.
    """
    original: BacktestRunRow | None = None

    async def load_original() -> BacktestRunRow | None:
        bootstrap = BootstrapSettingsLoader()
        repo = get_repository(bootstrap.db_url)
        bt_repo = BacktestRepository(cast(Any, repo).session_factory)
        now = datetime.now(UTC)
        return await bt_repo.get_run(run_id, as_of=now)

    original = asyncio.run(load_original())
    if original is None:
        typer.echo(f"Run not found: {run_id}", err=True)
        raise typer.Exit(code=1)

    original_instrument = original.get("instrument") or original["instrument_public_id"]
    typer.echo(
        f"Re-running {original['strategy_name']} on "
        f"{original_instrument} "
        f"({original['start_date'].strftime('%Y-%m-%d')} → "
        f"{original['end_date'].strftime('%Y-%m-%d')})"
    )

    backtest_run(
        strategy=original["strategy_name"],
        instrument=original_instrument,
        exchange=original["exchange"],
        start=original["start_date"].isoformat(),
        end=original["end_date"].isoformat(),
        timeframe=original["timeframe"],
        initial_cash=original["initial_cash"],
        wallet=original["wallet_public_id"],
        params_json=json_mod.dumps(original.get("strategy_params", {})),
        execution_mode=original.get("execution_mode", "direct_db"),
        fill_model=original.get("fill_model", "market"),
        slippage_bps=original.get("slippage_bps", 0.0),
        commission_bps=original.get("commission_bps", 0.0),
    )


@app.command()
def notify() -> None:
    """Run the iOS Push Foundation sidecar (domain events -> APNs HTTP/2).

    Long-running process. Evaluates registered notify rules, fans each
    resulting alert out to the target user's active devices via the
    outbox-backed ``NotifySidecar``, and retries server/throttled failures
    on its own 30-second scheduler. The same process periodically recovers
    missing pages for durable open portfolio-drift episodes. Configuration
    is read from the ``apns_*`` settings seeded via
    ``proprietary/data/seed/{dev,prod}.toml`` and the recovery interval env
    var.

    Run under systemd / K8s with ``Restart=always`` — the sidecar
    exits only on unhandled errors or SIGINT, and the outbox drain
    on the next start recovers any queued deliveries left behind.
    """

    async def _run() -> None:
        bootstrap = get_bootstrap_settings()
        repo = get_repository(bootstrap.db_url)
        settings_svc = await get_settings_service(bootstrap.db_url, bootstrap.zmq_broker_xsub)
        apns_config = load_apns_config(settings_svc)
        apns_pool = build_apns_client_pool(apns_config)

        zmq_ctx = zmq.asyncio.Context()
        sub_sock = zmq_ctx.socket(zmq.SUB)
        apply_hwm(sub_sock, rcvhwm=HWM_ORDER_FLOW)
        broker_addr = bootstrap.zmq_broker_xpub
        sub_sock.connect(broker_addr)
        subscriber = ValidatedSubscriber(sub_sock)
        pub_sock = zmq_ctx.socket(zmq.PUB)
        apply_hwm(pub_sock, sndhwm=HWM_AUDIT)
        pub_sock.connect(bootstrap.zmq_broker_xsub)
        tracker = SequenceTracker()
        publisher = MessagePublisher(ValidatedPublisher(pub_sock), tracker)

        sidecar = NotifySidecar(
            subscriber=subscriber,
            repo=repo,
            apns=apns_pool,
            apns_topic=apns_config.topic,
            tracker=tracker,
            publisher=publisher,
            push_beta_provider=lambda: parse_push_beta_config(
                settings_svc.get_setting(PUSH_BETA_SETTING_KEY)
            ),
        )
        typer.echo(
            f"snapper-notify: connected to {broker_addr}; topic={apns_config.topic}"
            f"; env={apns_config.environment}"
        )
        try:
            await sidecar.start()
        except KeyboardInterrupt:
            typer.echo("snapper-notify: stopped by user")
        finally:
            await sidecar.stop()
            sub_sock.close()
            pub_sock.close()
            zmq_ctx.term()

    asyncio.run(_run())


app.command(name="dev-mint-pat")(dev_mint_pat)
app.add_typer(annulment_app, name="annulment")
app.add_typer(token_app, name="token")


@app.command(
    context_settings={
        "allow_extra_args": True,
        "ignore_unknown_options": True,
    }
)
def egress(ctx: typer.Context) -> None:
    """Run the snapper-egress sidecar (kernel WireGuard + SOCKS5).

    Extra args after ``snapper egress`` are forwarded verbatim to the
    egress entrypoint's argparse parser. Currently supports
    ``--instance-id <name>`` (see ``src/snapper/egress/__main__.py``).
    The CLI subcommand is the unified-image dispatch path: under
    ``ENTRYPOINT ["snapper"]``, the sidecar service runs
    ``command: ["egress"]`` in compose and lands here, while the
    module entrypoint ``python -m snapper.egress`` remains supported
    for bare-shell invocations.

    Args:
        ctx: Typer context with ``allow_extra_args=True``; ``ctx.args``
            carries the operator-supplied argv tail (e.g.
            ``["--instance-id", "snapper-egress-prod"]``) and is
            forwarded as the ``argv`` parameter to
            :func:`snapper.egress.__main__.main`. Passing the explicit
            list (instead of ``argv=None``) prevents the egress parser
            from accidentally receiving the literal ``"egress"`` token
            via ``sys.argv[1:]`` fallback.

    Raises:
        typer.Exit: Always — wraps the egress entrypoint's integer
            return code so Typer propagates it as the process exit
            code. A bare ``return rc`` would NOT propagate.
    """
    if _egress_main is None:
        typer.echo(
            f"snapper egress is unavailable in this environment: {_egress_import_error}",
            err=True,
        )
        raise typer.Exit(code=1)

    raise typer.Exit(code=_egress_main(list(ctx.args)))


_RECONCILE_ALIASES_EXCHANGES: dict[str, str] = {
    ExchangeEnum.KRAKEN: "kraken",
    ExchangeEnum.KRAKEN_FUTURES: "kraken_futures",
    ExchangeEnum.KRAKEN_EQUITIES: "kraken_equities",
    ExchangeEnum.WALUTOMAT: "walutomat",
    ExchangeEnum.POLYGON: "polygon",
}
"""Exchanges that the alias reconcile backfill supports.

Each entry maps the ``ExchangeEnum`` value (used as the
``symbol_aliases.exchange`` column) to the human-readable name shown
in the CLI report. The five symbol updaters that populate aliases
are listed here; adding a new exchange means populating its
capabilities in the updater AND adding it to this dict so the
backfill can flush stale aliases on first deploy."""


@app.command(name="reconcile-symbol-aliases")
def reconcile_symbol_aliases(
    exchange: str = typer.Option(
        "all",
        "--exchange",
        "-e",
        help="Exchange to reconcile (kraken, kraken_futures, kraken_equities, walutomat, polygon, all).",
    ),
) -> None:
    """Close SCD2 alias rows for symbols whose capability is deactivated.

    One-shot backfill to close stale aliases left behind before
    steady-state alias closure was wired into the symbol updaters.
    Steady-state alias closure is handled by each symbol updater's
    own ``run-static`` invocation (which calls
    :func:`SymbolUpdaterService._reconcile_aliases` in the same DB
    transaction as ``_reconcile_capabilities``); this CLI exists so
    operators can flush any historical inconsistency in one shot
    without waiting for the next scheduled symbol update.

    Idempotent: re-running closes zero rows once steady state is
    reached. Safe to run against production; the DB write is a
    targeted ``UPDATE`` of `symbol_aliases.known_to` for rows whose
    owning ``symbol_exchange_capabilities`` row has both
    ``can_market_data = False`` AND ``can_trade = False`` at the
    moment the CLI runs.

    Args:
        exchange: Exchange to reconcile. Use ``all`` to process every
            registered exchange (the default). Use a specific
            exchange name (``kraken``, ``kraken_futures`` etc.) to
            restrict the scope.

    Raises:
        typer.Exit: With code 1 if the exchange name is not
            recognised. With code 0 on success (including a
            zero-close run).
    """
    bootstrap = BootstrapSettingsLoader()
    repo = DatabaseRepository(bootstrap.db_url)
    targets: list[tuple[str, str]] = []
    if exchange == "all":
        targets = list(_RECONCILE_ALIASES_EXCHANGES.items())
    else:
        match = next(
            ((k, v) for k, v in _RECONCILE_ALIASES_EXCHANGES.items() if v == exchange),
            None,
        )
        if match is None:
            typer.echo(
                f"Unknown exchange: {exchange!r}. "
                f"Choose from {sorted(_RECONCILE_ALIASES_EXCHANGES.values())} or 'all'."
            )
            raise typer.Exit(code=1)
        targets = [match]
    now = datetime.now(UTC)
    total_closed = 0
    per_exchange: list[tuple[str, int]] = []
    with repo.get_session() as session:
        for exchange_enum, exchange_label in targets:
            closed = SymbolUpdaterService._reconcile_aliases(session, exchange_enum, now)
            per_exchange.append((exchange_label, closed))
            total_closed += closed
        session.commit()
    for exchange_label, closed in per_exchange:
        typer.echo(f"{exchange_label}: closed {closed} alias rows")
    typer.echo(f"Total: closed {total_closed} alias rows across {len(targets)} exchange(s)")
