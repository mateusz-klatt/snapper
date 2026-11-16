"""Own bounded PostgreSQL connections for one selected snapshot refresh worker.

This provider creates no application settings, global repository, schema or
background task. PostgreSQL must accept every requested startup setting; a
permission or version refusal is an activation failure, never a fallback.
Cooperative client cancellation and server limits complement external process
containment; neither is a receipt proving that an uncertain commit rolled back.
"""

import asyncio
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import asynccontextmanager
from contextlib import suppress
from dataclasses import dataclass

import asyncpg
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from snapper.core.market_snapshot_refresh_budget import RefreshBudget
from snapper.core.market_snapshot_refresh_scope import RefreshTargetMetadata
from snapper.core.market_snapshot_refresh_scope import normalize_refresh_instrument_ids
from snapper.data.repository import _market_snapshot_refresh_target_query
from snapper.data.repository import _market_snapshot_refresh_target_rows

CONNECT_MAX_SECONDS = 4.0
COMMAND_MAX_SECONDS = 4.0
SESSION_CLEANUP_RESERVE_SECONDS = 1.0
_SSL_MODES = frozenset({"disable", "allow", "prefer", "require", "verify-ca", "verify-full"})


class RefreshConnectionError(RuntimeError):
    """A fixed failure code that never includes a URL, SQL parameters or server text."""


@dataclass(frozen=True, slots=True, repr=False)
class _Endpoint:
    """Explicit connection operands excluded from automatic representation."""

    host: str
    port: int
    database: str
    user: str
    password: str
    ssl: str


def _endpoint(value: str) -> _Endpoint:
    """Admit explicit PostgreSQL authority without driver or environment fallbacks.

    Args:
        value: Private SQLAlchemy PostgreSQL asyncpg URL.

    Returns:
        Explicit credentials and authority, with only the supported SSL option.
    """
    try:
        url = make_url(value)
        ssl = url.query.get("ssl", "prefer")
        explicit = (url.host, url.database, url.username, url.password)
        if (
            url.drivername != "postgresql+asyncpg"
            or not all(isinstance(part, str) and part for part in explicit)
            or set(url.query).difference({"ssl"})
            or not isinstance(ssl, str)
            or ssl not in _SSL_MODES
        ):
            raise ValueError
        host, database, user, password = explicit
        if host is None or database is None or user is None or password is None:
            raise ValueError
        port = url.port if url.port is not None else 5432
        if not 1 <= port <= 65535 or host.strip() != host or "/" in host or "," in host:
            raise ValueError
        return _Endpoint(host, port, database, user, password, ssl)
    except (SQLAlchemyError, TypeError, ValueError):
        raise RefreshConnectionError("invalid_refresh_database_url") from None


def _server_settings(budget: RefreshBudget) -> dict[str, str]:
    """Compute startup limits from the original deadline for each physical connection.

    Args:
        budget: Original worker budget, never reconstructed from a duration.

    Returns:
        PostgreSQL settings that must all be accepted before any application SQL.
    """
    return {
        "timezone": "UTC",
        "statement_timeout": f"{budget.timeout_ms(COMMAND_MAX_SECONDS)}ms",
        "lock_timeout": f"{budget.timeout_ms(0.25)}ms",
        "transaction_timeout": f"{budget.timeout_ms(8.0)}ms",
        "idle_in_transaction_session_timeout": f"{budget.timeout_ms(1.0)}ms",
        "log_statement": "none",
        "log_min_duration_statement": "-1",
        "log_min_duration_sample": "-1",
        "log_transaction_sample_rate": "0",
        "log_parameter_max_length": "0",
        "log_parameter_max_length_on_error": "0",
        "log_min_error_statement": "panic",
    }


def _remaining(budget: RefreshBudget, allow_expired: bool) -> float:
    """Permit an already-expired cleanup attempt without granting more time."""
    try:
        return budget.remaining()
    except TimeoutError:
        if allow_expired:
            return 0.0
        raise


async def _bounded[ResultT](
    operation: Callable[[], Awaitable[ResultT]],
    budget: RefreshBudget,
    allow_expired: bool = False,
) -> ResultT:
    """Bound one owned operation and propagate swallowed external cancellation.

    Args:
        operation: Deferred operation, so expiry never creates an abandoned coroutine.
        budget: Original absolute deadline.
        allow_expired: Attempt final cleanup with zero extra time after expiry.

    Returns:
        The result only while the deadline remains live.
    """
    task = asyncio.current_task()
    if task is None:
        raise RefreshConnectionError("refresh_connection_requires_task")
    baseline = task.cancelling()
    timeout = asyncio.timeout(_remaining(budget, allow_expired))
    try:
        async with timeout:
            result = await operation()
    finally:
        if task.cancelling() > baseline:
            raise asyncio.CancelledError("Refresh connection cancelled")
    if timeout.expired():
        raise TimeoutError("Refresh connection deadline exhausted")
    budget.remaining()
    return result


@asynccontextmanager
async def _session_deadline(budget: RefreshBudget) -> AsyncIterator[None]:
    """Reserve cleanup inside the original deadline and detect swallowed cancellation."""
    task = asyncio.current_task()
    if task is None:
        raise RefreshConnectionError("refresh_connection_requires_task")
    baseline = task.cancelling()
    timeout = asyncio.timeout(budget.remaining(SESSION_CLEANUP_RESERVE_SECONDS))
    try:
        async with timeout:
            yield
    finally:
        if task.cancelling() > baseline:
            raise asyncio.CancelledError("Refresh connection cancelled")
    if timeout.expired():
        raise TimeoutError("Refresh session deadline exhausted")
    budget.remaining(SESSION_CLEANUP_RESERVE_SECONDS)


class RefreshConnection:
    """One credential-private engine and serial session provider for an inherited budget.

    Construction is disconnected. Sessions cannot overlap, and any database,
    cancellation or cleanup failure poisons this provider against further work.
    The caller must close the provider even after a failed or uncertain write.
    """

    def __init__(self, db_url: str, budget: RefreshBudget) -> None:
        """Create a disconnected owned engine with no pooled or global connection.

        Args:
            db_url: Explicit PostgreSQL asyncpg URL with host, database, user and password.
            budget: Already established work portion of the host's absolute deadline.
        """
        budget.remaining()
        self._endpoint = _endpoint(db_url)
        self._budget = budget
        self._closed = False
        self._active = False
        self._disposed = False
        try:
            self._engine = create_async_engine(
                "postgresql+asyncpg://",
                async_creator=self._connect,
                poolclass=NullPool,
                echo=False,
                hide_parameters=True,
                pool_pre_ping=False,
            )
            self._sessions = async_sessionmaker(
                self._engine, expire_on_commit=False, class_=AsyncSession
            )
        except Exception:
            raise RefreshConnectionError("refresh_engine_creation_failed") from None

    @property
    def dialect_name(self) -> str:
        """Return the exclusively admitted PostgreSQL dialect.

        Returns:
            PostgreSQL, with no database discovery or connection.
        """
        return "postgresql"

    def _admit(self) -> None:
        """Refuse expired, closed or concurrent use without opening a connection."""
        if self._closed:
            raise RefreshConnectionError("refresh_connection_closed")
        self._budget.remaining()

    async def _connect(self) -> asyncpg.Connection:
        """Acquire one physical connection with fresh remaining-budget startup settings.

        Returns:
            The owned asyncpg connection, without exposing connection exceptions.
        """
        self._admit()
        endpoint = self._endpoint
        acquired: list[asyncpg.Connection] = []

        async def open_connection() -> asyncpg.Connection:
            """Retain an acquired handle even when post-connect budget checks fail."""
            connection = await asyncpg.connect(
                host=endpoint.host,
                port=endpoint.port,
                database=endpoint.database,
                user=endpoint.user,
                password=endpoint.password,
                ssl=endpoint.ssl,
                passfile="",
                timeout=self._budget.timeout_seconds(CONNECT_MAX_SECONDS),
                command_timeout=self._budget.timeout_seconds(COMMAND_MAX_SECONDS),
                server_settings=_server_settings(self._budget),
            )
            acquired.append(connection)
            return connection

        try:
            return await _bounded(open_connection, self._budget)
        except asyncio.CancelledError:
            self._closed = True
            raise
        except TimeoutError:
            self._closed = True
            raise TimeoutError("Refresh database connection deadline exhausted") from None
        except Exception:
            self._closed = True
            raise RefreshConnectionError("refresh_database_connection_failed") from None
        finally:
            if self._closed:
                for connection in acquired:
                    with suppress(Exception):
                        connection.terminate()

    async def _close_session(self, session: AsyncSession) -> None:
        """Close an owned session even when cleanup starts at an expired deadline."""
        try:
            await _bounded(session.close, self._budget, allow_expired=True)
        except BaseException:
            self._closed = True
            raise

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Yield a serial bounded session and close it before releasing local ownership.

        Yields:
            An explicit session whose transaction remains owned by its caller.
        """
        self._admit()
        if self._active:
            raise RefreshConnectionError("refresh_session_already_active")
        self._budget.remaining(SESSION_CLEANUP_RESERVE_SECONDS)
        try:
            session = self._sessions()
        except Exception:
            self._closed = True
            raise RefreshConnectionError("refresh_session_creation_failed") from None
        self._active = True
        failed = False
        try:
            async with _session_deadline(self._budget):
                yield session
        except BaseException as error:
            failed = True
            self._closed = True
            if isinstance(error, TimeoutError):
                raise TimeoutError("Refresh database operation deadline exhausted") from None
            if isinstance(error, (SQLAlchemyError, asyncpg.PostgresError, OSError)):
                raise RefreshConnectionError("refresh_database_operation_failed") from None
            raise
        finally:
            try:
                await self._close_session(session)
            except asyncio.CancelledError:
                raise
            except Exception:
                if not failed:
                    raise RefreshConnectionError("refresh_session_cleanup_failed") from None
            finally:
                self._active = False

    async def resolve_market_snapshot_refresh_targets(
        self, instrument_public_ids: list[str]
    ) -> list[RefreshTargetMetadata]:
        """Read the established bounded resolver without duplicating its SQL policy.

        Args:
            instrument_public_ids: Explicit main UUIDs, validated before any connection.

        Returns:
            Existing six-field required and excluded candidate metadata.
        """
        identities = list(normalize_refresh_instrument_ids(instrument_public_ids))
        async with self.session() as session:
            result = await _bounded(
                lambda: session.execute(_market_snapshot_refresh_target_query(identities)),
                self._budget,
            )
            return _market_snapshot_refresh_target_rows(result.all())

    async def aclose(self) -> None:
        """Dispose only this engine, without retries or a renewed cleanup allowance."""
        if self._disposed:
            return
        self._closed = True
        if self._active:
            raise RefreshConnectionError("refresh_session_still_active")
        try:
            await _bounded(self._engine.dispose, self._budget, allow_expired=True)
        except asyncio.CancelledError:
            raise
        except Exception:
            raise RefreshConnectionError("refresh_engine_cleanup_failed") from None
        self._disposed = True

    async def __aenter__(self) -> RefreshConnection:
        """Admit the disconnected provider without opening a database session.

        Returns:
            This provider for one caller-owned context.
        """
        self._admit()
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception: BaseException | None,
        traceback: object,
    ) -> None:
        """Close only owned resources while preserving an existing failure.

        Args:
            exception_type: Active exception class, when the body failed.
            exception: Active failure, which cleanup must never convert to success.
            traceback: Opaque exception traceback owned by Python.
        """
        try:
            await self.aclose()
        except asyncio.CancelledError:
            raise
        except Exception:
            if exception is None:
                raise
