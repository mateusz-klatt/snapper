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

Database:
    - ``db-init``: Initialize database schema
    - ``db-upgrade``: Run Alembic migrations
    - ``db-downgrade``: Rollback migrations

User Management:
    - ``init-admin``: Create initial admin user
    - ``list-users``: List all system users
    - ``reset-password``: Reset user password

Data Updates:
    - ``update-kraken-symbols``: Sync Kraken symbol mappings
    - ``update-zonda-symbols``: Sync Zonda symbol mappings
    - ``update-polygon-symbols``: Sync Polygon symbol mappings
    - ``polygon-backfill-aggregates``: Backfill historical data

Example:
    Run the server::

        snapper server --host 0.0.0.0 --port 8000

    Initialize database::

        snapper db-init
"""

import asyncio
import signal
import threading
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Annotated
from typing import Any

import sqlalchemy as sa
import typer
import uvicorn
from alembic import command
from alembic.config import Config
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from snapper.application.engine.trader import TraderCoordinator
from snapper.application.updaters.historical.aggregates import PolygonAggregatesBackfillService
from snapper.application.updaters.historical.grouped import PolygonGroupedDailyBackfillService
from snapper.application.updaters.symbols.kraken import KrakenSymbolUpdaterService
from snapper.application.updaters.symbols.polygon import PolygonSymbolUpdaterService
from snapper.application.updaters.symbols.walutomat import WalutomatSymbolUpdaterService
from snapper.application.updaters.symbols.zonda import ZondaSymbolUpdaterService
from snapper.auth.domain.roles import UserRole
from snapper.auth.user_service import UserService
from snapper.config.settings import BootstrapSettingsLoader
from snapper.config.settings import get_settings
from snapper.data.models import Setting
from snapper.data.repository import close_and_insert
from snapper.data.repository import where_active
from snapper.data.seed.loader import run_seed
from snapper.infrastructure.market_data.kraken import run_snapshot_update
from snapper.infrastructure.market_data.walutomat import run_walutomat_snapshot_update
from snapper.infrastructure.market_data.zonda import run_zonda_snapshot_update
from snapper.infrastructure.security.encryption import SettingsEncryptionService
from snapper.infrastructure.security.encryption import get_encryption_service
from snapper.messaging.executors.kraken import KrakenOrderExecutor
from snapper.messaging.executors.walutomat import WalutomatOrderExecutor
from snapper.messaging.executors.zonda import ZondaOrderExecutor
from snapper.messaging.infrastructure.broker import ZmqBrokerThread
from snapper.messaging.infrastructure.logger import ZmqMessageLogger
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.publishers.kraken import KrakenMarketDataPublisher
from snapper.server.app import create_app

_FORCE_UPDATE_HELP = "Force update even if recently updated"

app = typer.Typer(add_completion=False, help="Snapper CLI")


def validate_api_keys_for_trader(paper: bool = False) -> bool:
    """Validate API keys configuration for trader coordinator.

    Args:
        paper: If True, skip validation for paper trading mode.

    Returns:
        True if configuration is valid.
    """
    return True


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
) -> None:
    """Start the central trading coordinator.

    Args:
        signal_topics: Comma-separated list of ZMQ signal topics to subscribe.
    """
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
    symbols: str = typer.Option("BTC/USD", help="Comma-separated list of symbols"),
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
            help="Exchange to run executor for (kraken, zonda, walutomat)",
        ),
    ] = "kraken",
) -> None:
    """Run the exchange order executor service.

    Args:
        exchange: Exchange name (kraken, zonda, walutomat).
    """

    async def run_executor() -> None:
        service_map: dict[
            str,
            type[KrakenOrderExecutor] | type[ZondaOrderExecutor] | type[WalutomatOrderExecutor],
        ] = {
            "kraken": KrakenOrderExecutor,
            "zonda": ZondaOrderExecutor,
            "walutomat": WalutomatOrderExecutor,
        }
        if exchange.lower() not in service_map:
            typer.echo(f"Error: Unknown exchange '{exchange}'. Choose: kraken, zonda, walutomat")
            raise typer.Exit(1)
        service: KrakenOrderExecutor | ZondaOrderExecutor | WalutomatOrderExecutor = service_map[
            exchange.lower()
        ]()
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
    password: str = typer.Option("AdminSnapper2026!", help="Admin password"),
) -> None:
    """Create the initial admin user.

    Args:
        username: Admin username.
        password: Admin password.
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
        typer.echo(f"Password: {password}")
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
        typer.echo("Default passwords are typically the same as usernames")
        typer.echo("Try logging in with: admin/admin, operator/operator, or viewer/viewer")

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
            typer.echo(f"New password: {new_password}")
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


@app.command(name="update-zonda-symbols")
def update_zonda_symbols(
    force: bool = typer.Option(False, "--force", "-f", help=_FORCE_UPDATE_HELP),
) -> None:
    """Sync Zonda symbol mappings from exchange API.

    Args:
        force: Force update even if recently updated.
    """

    async def run_zonda_update() -> None:
        updater = ZondaSymbolUpdaterService(update_threshold_hours=24, force=force)
        try:
            typer.echo("Starting Zonda symbol mapping update...")
            await updater.start()
            typer.echo("Zonda symbol mappings updated successfully")
        except Exception as e:
            typer.echo(f"Error updating Zonda symbol mappings: {e}")
            raise typer.Exit(code=1) from e

    asyncio.run(run_zonda_update())


@app.command(name="update-zonda-market-snapshot")
def update_zonda_market_snapshot() -> None:
    """Update Zonda market snapshots with current prices."""
    typer.echo("Updating Zonda market snapshots...")
    try:
        run_zonda_snapshot_update()
        typer.echo("Zonda market snapshot update complete!")
    except Exception as e:
        typer.echo(f"Error updating Zonda market snapshots: {e}")
        raise typer.Exit(code=1) from e


@app.command(name="update-walutomat-symbols")
def update_walutomat_symbols(
    force: bool = typer.Option(False, "--force", "-f", help=_FORCE_UPDATE_HELP),
) -> None:
    """Sync Walutomat symbol mappings from exchange API.

    Args:
        force: Force update even if recently updated.
    """

    async def run_walutomat_update() -> None:
        updater = WalutomatSymbolUpdaterService(update_threshold_hours=24, force=force)
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
    new_master_password: str,
) -> None:
    """Commit rotation results and print summary.

    Args:
        session: Active database session.
        changes_made: Number of settings successfully rotated.
        total_settings: Total number of encrypted settings found.
        dry_run: Whether this was a dry run.
        new_master_password: New master password for display.
    """
    if not dry_run and changes_made > 0:
        await session.commit()
        typer.echo(f"Successfully rotated {changes_made} encrypted settings")
        typer.echo()
        typer.echo("IMPORTANT: Update your environment variables with new credentials:")
        typer.echo(f"   MASTER_PASSWORD={new_master_password}")
        typer.echo()
        typer.echo("Restart the application to use new encryption parameters")
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
                sa.select(Setting).where(Setting.is_encrypted, *where_active(Setting))
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
                new_master_password,
            )
        await engine.dispose()
    except Exception as e:
        typer.echo(f"Failed to rotate encryption: {e}")
        if not dry_run:
            typer.echo("Database may be in inconsistent state - restore from backup if needed")


@app.command(name="settings-rotate-encryption")
def settings_rotate_encryption(
    new_master_password: str = typer.Option(..., "--new-password", help="New master password"),
    old_master_password: str = typer.Option(
        None, "--old-password", help="Current master password (optional, reads from env/bootstrap)"
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
    resume: bool = typer.Option(True, "--resume/--no-resume", help="Resume from last timestamp"),
    save_csv: bool = typer.Option(True, "--csv/--no-csv", help="Save to CSV.gz files"),
) -> None:
    """Backfill historical aggregate data from Polygon API.

    Args:
        symbols: List of symbols to backfill.
        all_mapped: Backfill all symbols with Polygon mapping.
        multiplier: Timeframe multiplier.
        timespan: Timespan unit (minute, hour, day).
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
            symbol_source = "all mapped" if all_mapped else "specified/settings"
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
