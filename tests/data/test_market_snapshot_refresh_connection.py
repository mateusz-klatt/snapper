"""Exercise isolated refresh connection ownership without opening a database socket."""

import asyncio
from contextlib import suppress
from dataclasses import dataclass
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import asyncpg
import pytest
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.pool import NullPool

from snapper.core.market_snapshot_refresh_budget import RefreshBudget
from snapper.data import market_snapshot_refresh_connection as connection_module
from snapper.data.market_snapshot_refresh_connection import RefreshConnection
from snapper.data.market_snapshot_refresh_connection import RefreshConnectionError

_URL = "postgresql+asyncpg://reader:private-test-password@database.invalid:5544/snapper"
_ID = "019f7bf2-1b40-7170-994f-395286fc39f7"


@dataclass
class _Clock:
    """Advance inherited budget deterministically without delaying tests."""

    now: float = 0.0

    def __call__(self) -> float:
        """Return the controlled monotonic instant."""
        return self.now


@dataclass
class _Harness:
    """Typed test handles for the only engine, session and physical connection."""

    engine_factory: MagicMock
    sessions: MagicMock
    engine: MagicMock
    session: MagicMock
    connect: AsyncMock
    physical: MagicMock


@pytest.fixture
def harness(monkeypatch: pytest.MonkeyPatch) -> _Harness:
    """Replace SQLAlchemy construction and asyncpg transport before provider creation."""
    engine = MagicMock(spec=AsyncEngine)
    engine.dispose = AsyncMock()
    session = MagicMock(spec=AsyncSession)
    session.close = AsyncMock()
    session.execute = AsyncMock()
    sessions = MagicMock(return_value=session)
    engine_factory = MagicMock(return_value=engine)
    physical = MagicMock(spec=asyncpg.Connection)
    connect = AsyncMock(return_value=physical)
    monkeypatch.setattr(connection_module, "create_async_engine", engine_factory)
    monkeypatch.setattr(connection_module, "async_sessionmaker", MagicMock(return_value=sessions))
    monkeypatch.setattr(connection_module.asyncpg, "connect", connect)
    return _Harness(engine_factory, sessions, engine, session, connect, physical)


def _budget(clock: _Clock | None = None) -> RefreshBudget:
    """Construct an inherited synthetic budget without a real database dependency."""
    return RefreshBudget(45.0, clock or _Clock())


@pytest.mark.asyncio
async def test_construction_and_context_are_disconnected_and_credential_private(
    harness: _Harness,
) -> None:
    """Own a credential-free engine descriptor and dispose only that engine once.

    Given: Explicit credentials and a mocked engine are admitted within the inherited budget.
    When: The provider is constructed, entered, closed and closed again.
    Then: No transport opens, representations omit credentials and only its engine is disposed once.
    """
    provider = RefreshConnection(_URL, _budget())
    assert provider.dialect_name == "postgresql"
    assert "private-test-password" not in repr(provider)
    assert "private-test-password" not in repr(provider._endpoint)
    harness.connect.assert_not_awaited()
    arguments = harness.engine_factory.call_args
    assert arguments.args == ("postgresql+asyncpg://",)
    assert arguments.kwargs["poolclass"] is NullPool
    assert arguments.kwargs["echo"] is False
    assert arguments.kwargs["hide_parameters"] is True
    assert arguments.kwargs["pool_pre_ping"] is False
    assert arguments.kwargs["async_creator"] == provider._connect
    async with provider as entered:
        assert entered is provider
        harness.connect.assert_not_awaited()
    await provider.aclose()
    harness.engine.dispose.assert_awaited_once_with()
    with pytest.raises(RefreshConnectionError, match="^refresh_connection_closed$"):
        await provider.__aenter__()


@pytest.mark.parametrize(
    "url",
    [
        "broken://private-test-password",
        "sqlite+aiosqlite:///private-test-password",
        "postgresql://user:private-test-password@host/db",
        "postgresql+asyncpg://host/db",
        "postgresql+asyncpg://user@host/db",
        "postgresql+asyncpg://user:private-test-password@/db",
        "postgresql+asyncpg://user:private-test-password@host",
        "postgresql+asyncpg://user:private-test-password@host:0/db",
        "postgresql+asyncpg://user:private-test-password@host:65536/db",
        "postgresql+asyncpg://user:private-test-password@host:abc/db",
        "postgresql+asyncpg://user:private-test-password@a,b/db",
        "postgresql+asyncpg://user:private-test-password@host/db?options=anything",
        "postgresql+asyncpg://user:private-test-password@host/db?ssl=unknown",
        "postgresql+asyncpg://user:private-test-password@host/db?ssl=require&ssl=prefer",
    ],
)
def test_invalid_url_refuses_before_engine_creation(harness: _Harness, url: str) -> None:
    """Reject incomplete authority and unsupported settings without exposing operands.

    Given: A URL omits required authority or contains unsupported driver settings.
    When: The disconnected provider validates that URL.
    Then: A fixed sanitized failure precedes every engine construction attempt.
    """
    with pytest.raises(RefreshConnectionError, match="^invalid_refresh_database_url$") as error:
        RefreshConnection(url, _budget())
    assert error.value.__suppress_context__ is True
    harness.engine_factory.assert_not_called()


def test_engine_creation_failure_is_sanitized(harness: _Harness) -> None:
    """Hide driver initialization error text rather than printing connection operands.

    Given: Engine construction raises an exception containing the private database URL.
    When: The provider attempts to construct its owned engine.
    Then: The caller receives only the fixed engine creation failure code.
    """
    harness.engine_factory.side_effect = RuntimeError(_URL)
    with pytest.raises(RefreshConnectionError, match="^refresh_engine_creation_failed$"):
        RefreshConnection(_URL, _budget())


def test_expired_budget_precedes_url_and_engine_work(harness: _Harness) -> None:
    """Refuse a consumed host budget without parsing or constructing resources.

    Given: The inherited deadline has already passed.
    When: Provider construction begins.
    Then: Budget refusal occurs before the engine factory is called.
    """
    with pytest.raises(TimeoutError):
        RefreshConnection(_URL, RefreshBudget(1.0, lambda: 2.0))
    harness.engine_factory.assert_not_called()


@pytest.mark.asyncio
async def test_every_physical_connect_recomputes_remaining_limits(harness: _Harness) -> None:
    """Bind explicit authority, disable statement logging and shrink every new connection.

    Given: Two physical connections share one controlled absolute deadline.
    When: The clock advances between the first and second connection attempts.
    Then: Explicit authority and logging controls remain while every timeout shrinks to time left.
    """
    clock = _Clock()
    provider = RefreshConnection(_URL + "?ssl=require", _budget(clock))
    assert await provider._connect() is harness.physical
    first = harness.connect.call_args.kwargs
    assert first["host"] == "database.invalid"
    assert first["port"] == 5544
    assert first["database"] == "snapper"
    assert first["user"] == "reader"
    assert first["password"] == "private-test-password"
    assert first["ssl"] == "require"
    assert first["passfile"] == ""
    assert first["timeout"] == first["command_timeout"] == 4.0
    assert first["server_settings"] == {
        "timezone": "UTC",
        "statement_timeout": "4000ms",
        "lock_timeout": "250ms",
        "transaction_timeout": "8000ms",
        "idle_in_transaction_session_timeout": "1000ms",
        "log_statement": "none",
        "log_min_duration_statement": "-1",
        "log_min_duration_sample": "-1",
        "log_transaction_sample_rate": "0",
        "log_parameter_max_length": "0",
        "log_parameter_max_length_on_error": "0",
        "log_min_error_statement": "panic",
    }
    clock.now = 44.875
    await provider._connect()
    second = harness.connect.call_args.kwargs
    assert second["timeout"] == second["command_timeout"] == 0.125
    settings = second["server_settings"]
    assert isinstance(settings, dict)
    for name in (
        "statement_timeout",
        "lock_timeout",
        "transaction_timeout",
        "idle_in_transaction_session_timeout",
    ):
        assert settings[name] == "125ms"


@pytest.mark.asyncio
async def test_default_port_and_ssl_are_explicit(harness: _Harness) -> None:
    """Supply driver values explicitly instead of inheriting PGPORT or PGSSLMODE.

    Given: An admitted URL omits its optional port and SSL query value.
    When: The provider invokes the mocked physical connector.
    Then: It explicitly supplies port 5432 and SSL mode prefer to the driver.
    """
    provider = RefreshConnection(_URL.replace(":5544", ""), _budget())
    await provider._connect()
    assert harness.connect.call_args.kwargs["port"] == 5432
    assert harness.connect.call_args.kwargs["ssl"] == "prefer"


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [PermissionError(_URL), asyncpg.PostgresError(_URL)])
async def test_startup_failure_has_no_weakened_retry(harness: _Harness, error: Exception) -> None:
    """Treat denied logging settings or connection errors as a poisoned provider.

    Given: PostgreSQL startup or connection admission raises a permission or database error.
    When: A connection attempt fails and the caller attempts another connection.
    Then: The first error is sanitized and the poisoned provider refuses without retrying transport.
    """
    harness.connect.side_effect = error
    provider = RefreshConnection(_URL, _budget())
    with pytest.raises(RefreshConnectionError, match="^refresh_database_connection_failed$"):
        await provider._connect()
    with pytest.raises(RefreshConnectionError, match="^refresh_connection_closed$"):
        await provider._connect()
    harness.connect.assert_awaited_once()
    await provider.aclose()


@pytest.mark.asyncio
async def test_blocking_connection_is_bounded_and_poisons_provider(harness: _Harness) -> None:
    """Stop an unresponsive mocked transport inside the inherited deadline.

    Given: The mocked physical connector waits indefinitely.
    When: Its inherited absolute budget expires.
    Then: Connection acquisition raises a sanitized timeout and poisons the provider.
    """

    async def blocked(**arguments: object) -> asyncpg.Connection:
        """Wait forever without opening a real connection."""
        await asyncio.Event().wait()
        raise AssertionError(arguments)

    harness.connect.side_effect = blocked
    budget = RefreshBudget(asyncio.get_running_loop().time() + 0.02)
    provider = RefreshConnection(_URL, budget)
    with pytest.raises(TimeoutError, match="^Refresh database connection deadline exhausted$"):
        await provider._connect()
    assert provider._closed is True


@pytest.mark.asyncio
async def test_late_acquired_connection_is_terminated(harness: _Harness) -> None:
    """Retain and terminate a returned connection when post-await time already expired.

    Given: The connector returns a handle after consuming the remaining synthetic budget.
    When: The provider checks the deadline after acquisition.
    Then: It reports timeout and terminates the retained physical handle exactly once.
    """
    clock = _Clock()

    async def late(**arguments: object) -> asyncpg.Connection:
        """Consume the remaining synthetic budget before returning a handle."""
        assert arguments["timeout"] == 4.0
        clock.now = 46.0
        return cast(asyncpg.Connection, harness.physical)

    harness.connect.side_effect = late
    provider = RefreshConnection(_URL, _budget(clock))
    with pytest.raises(TimeoutError):
        await provider._connect()
    harness.physical.terminate.assert_called_once_with()


@pytest.mark.asyncio
async def test_connect_preserves_swallowed_external_cancellation(harness: _Harness) -> None:
    """Reject a late connection even when a transport swallows caller cancellation.

    Given: The connector swallows a delivered caller cancellation and returns a handle.
    When: The provider observes the outstanding cancellation request.
    Then: Cancellation propagates, the handle is terminated and the provider stays poisoned.
    """

    async def swallowed(**arguments: object) -> asyncpg.Connection:
        """Return a handle after swallowing one pending caller cancellation."""
        assert arguments["database"] == "snapper"
        task = asyncio.current_task()
        assert task is not None
        task.cancel()
        try:
            await asyncio.sleep(0)
        except asyncio.CancelledError:
            return cast(asyncpg.Connection, harness.physical)
        raise AssertionError("Cancellation was not delivered")

    harness.connect.side_effect = swallowed
    provider = RefreshConnection(_URL, _budget())
    with pytest.raises(asyncio.CancelledError):
        await asyncio.create_task(provider._connect())
    harness.physical.terminate.assert_called_once_with()
    assert provider._closed is True


@pytest.mark.asyncio
async def test_sessions_are_serial_owned_and_closed(harness: _Harness) -> None:
    """Refuse overlapping work without disposing an engine that still owns a session.

    Given: One provider already owns an active session.
    When: A nested session is requested and a later serial session follows normal closure.
    Then: Overlap is refused while both admitted serial sessions close through the owned provider.
    """
    provider = RefreshConnection(_URL, _budget())
    async with provider.session() as session:
        assert session is harness.session
        with pytest.raises(RefreshConnectionError, match="^refresh_session_already_active$"):
            async with provider.session():
                raise AssertionError("Overlapping session was admitted")
    harness.session.close.assert_awaited_once_with()
    async with provider.session():
        pass
    assert harness.session.close.await_count == 2
    await provider.aclose()


@pytest.mark.asyncio
async def test_dispose_refuses_active_session_and_prevents_later_work(harness: _Harness) -> None:
    """An attempted owner close cannot silently detach an active session.

    Given: The provider still owns an active session.
    When: Disposal is requested before and after that session closes.
    Then: Active disposal is refused and engine disposal happens only after session cleanup.
    """
    provider = RefreshConnection(_URL, _budget())
    async with provider.session():
        with pytest.raises(RefreshConnectionError, match="^refresh_session_still_active$"):
            await provider.aclose()
    harness.engine.dispose.assert_not_awaited()
    await provider.aclose()
    harness.engine.dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_session_creation_error_is_sanitized(harness: _Harness) -> None:
    """Refuse a broken session factory without leaving an active ownership marker.

    Given: The session factory raises an exception containing private connection operands.
    When: The provider tries to admit its first session.
    Then: A sanitized refusal leaves no active session marker and permits owned engine cleanup.
    """
    harness.sessions.side_effect = RuntimeError(_URL)
    provider = RefreshConnection(_URL, _budget())
    with pytest.raises(RefreshConnectionError, match="^refresh_session_creation_failed$"):
        async with provider.session():
            raise AssertionError("Session was admitted")
    assert provider._active is False
    await provider.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error", [SQLAlchemyError(_URL), asyncpg.PostgresError(_URL), OSError(_URL)]
)
async def test_session_database_errors_are_sanitized_and_closed(
    harness: _Harness, error: Exception
) -> None:
    """Hide database operands while closing the exact failed session.

    Given: An admitted session raises a database or operating-system exception with private text.
    When: The session context unwinds and another session is requested.
    Then: The failure is sanitized, the original session closes and later work is refused.
    """
    provider = RefreshConnection(_URL, _budget())
    with pytest.raises(RefreshConnectionError, match="^refresh_database_operation_failed$"):
        async with provider.session():
            raise error
    harness.session.close.assert_awaited_once()
    with pytest.raises(RefreshConnectionError, match="^refresh_connection_closed$"):
        async with provider.session():
            raise AssertionError("Poisoned session was admitted")


@pytest.mark.asyncio
async def test_body_failure_survives_cleanup_failure(harness: _Harness) -> None:
    """Retain the original domain error while poisoning failed cleanup.

    Given: The session body refuses its domain operation and session cleanup also fails.
    When: The provider unwinds the failed session.
    Then: The original domain refusal survives and local ownership becomes inactive and poisoned.
    """
    harness.session.close.side_effect = RuntimeError(_URL)
    provider = RefreshConnection(_URL, _budget())
    with pytest.raises(ValueError, match="^domain_refusal$"):
        async with provider.session():
            raise ValueError("domain_refusal")
    assert provider._active is False
    assert provider._closed is True


@pytest.mark.asyncio
async def test_cleanup_failure_cannot_return_success(harness: _Harness) -> None:
    """Sanitize failed session cleanup even when the body returned normally.

    Given: A session body finishes normally but its close operation raises private error text.
    When: The provider exits the session context.
    Then: A fixed cleanup failure prevents the body from being reported as successful.
    """
    harness.session.close.side_effect = RuntimeError(_URL)
    provider = RefreshConnection(_URL, _budget())
    with pytest.raises(RefreshConnectionError, match="^refresh_session_cleanup_failed$"):
        async with provider.session():
            pass


@pytest.mark.asyncio
async def test_session_budget_consumption_attempts_cleanup_without_extension(
    harness: _Harness,
) -> None:
    """A body cannot consume the deadline and still report a successful closed session.

    Given: An admitted session shares a controlled absolute budget with cleanup.
    When: The body advances the clock beyond that deadline.
    Then: Timeout propagates, cleanup is attempted without extra time and the provider is poisoned.
    """
    clock = _Clock()
    provider = RefreshConnection(_URL, _budget(clock))
    with pytest.raises(TimeoutError):
        async with provider.session():
            clock.now = 46.0
    harness.session.close.assert_awaited_once()
    assert provider._closed is True


@pytest.mark.asyncio
async def test_session_blocking_body_and_close_are_bounded(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reserve cleanup inside a short inherited budget without changing pytest limits.

    Given: A session has a short inherited budget with a smaller cleanup reserve.
    When: Its body blocks indefinitely.
    Then: The body times out and the owned close operation is attempted inside that budget.
    """
    monkeypatch.setattr(connection_module, "SESSION_CLEANUP_RESERVE_SECONDS", 0.01)
    budget = RefreshBudget(asyncio.get_running_loop().time() + 0.03)
    provider = RefreshConnection(_URL, budget)
    with pytest.raises(TimeoutError):
        async with provider.session():
            await asyncio.Event().wait()
    harness.session.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_session_detects_swallowed_external_cancellation(harness: _Harness) -> None:
    """The session owner cannot convert a swallowed caller cancellation into success.

    Given: A session body deliberately swallows one delivered caller cancellation.
    When: The provider checks cancellation state while leaving the body.
    Then: Cancellation remains observable and the owned session still receives cleanup.
    """
    provider = RefreshConnection(_URL, _budget())

    async def body() -> None:
        """Swallow a delivered cancellation only inside the controlled session body."""
        async with provider.session():
            task = asyncio.current_task()
            assert task is not None
            task.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.sleep(0)

    with pytest.raises(asyncio.CancelledError):
        await asyncio.create_task(body())
    harness.session.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_resolver_reuses_existing_bounded_projection(harness: _Harness) -> None:
    """Return the same six metadata fields without a second resolver SQL implementation.

    Given: Equivalent UUID spellings identify one instrument and the query returns six metadata fields.
    When: The provider runs the shared resolver query and projection.
    Then: The query uses canonical identity and the cap-plus-one limit, preserving the exact metadata.
    """
    result = MagicMock()
    result.all.return_value = [(_ID, "BTC/EUR", "kraken", "BTC", "EUR", "crypto")]
    harness.session.execute.return_value = result
    provider = RefreshConnection(_URL, _budget())
    rows = await provider.resolve_market_snapshot_refresh_targets([_ID.upper(), _ID])
    assert rows == [
        {
            "instrument_public_id": _ID,
            "native_symbol": "BTC/EUR",
            "exchange": "kraken",
            "base": "BTC",
            "quote": "EUR",
            "asset_type": "crypto",
        }
    ]
    query = harness.session.execute.call_args.args[0]
    parameters = query.compile().params
    assert parameters["param_1"] == 1001
    assert parameters["public_id_1"] == [_ID]
    harness.session.close.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("identities", [[], ["bad"], [" " + _ID], [_ID] * 101])
async def test_invalid_resolver_inputs_precede_session_creation(
    harness: _Harness, identities: list[str]
) -> None:
    """Reject malformed and oversized requests before any database boundary.

    Given: Requested identities are empty, malformed, padded or exceed the raw input cap.
    When: Explicit target resolution is requested.
    Then: Validation fails before the provider creates any session.
    """
    provider = RefreshConnection(_URL, _budget())
    with pytest.raises(ValueError):
        await provider.resolve_market_snapshot_refresh_targets(identities)
    harness.sessions.assert_not_called()


@pytest.mark.asyncio
async def test_resolver_overflow_refuses_instead_of_truncating(harness: _Harness) -> None:
    """Preserve the existing cap-plus-one overflow witness and close the session.

    Given: The existing resolver returns one row beyond the accepted expansion cap.
    When: The provider projects those candidate rows.
    Then: It refuses overflow instead of truncating the scope and closes the owned session.
    """
    result = MagicMock()
    result.all.return_value = [(_ID, "BTC/EUR", "kraken", "BTC", "EUR", "crypto")] * 1001
    harness.session.execute.return_value = result
    provider = RefreshConnection(_URL, _budget())
    with pytest.raises(ValueError, match="Expanded refresh scope exceeds"):
        await provider.resolve_market_snapshot_refresh_targets([_ID])
    harness.session.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_engine_disposal_failure_is_non_success(harness: _Harness) -> None:
    """Sanitize owned disposal errors and prevent subsequent database work.

    Given: Owned engine disposal raises an exception containing private connection text.
    When: An otherwise successful provider context exits.
    Then: A sanitized cleanup failure leaves the provider closed without claiming disposal success.
    """
    harness.engine.dispose.side_effect = RuntimeError(_URL)
    provider = RefreshConnection(_URL, _budget())
    with pytest.raises(RefreshConnectionError, match="^refresh_engine_cleanup_failed$"):
        async with provider:
            pass
    assert provider._disposed is False
    assert provider._closed is True


@pytest.mark.asyncio
async def test_existing_body_error_survives_engine_disposal_failure(harness: _Harness) -> None:
    """Disposal failure never hides the original non-success classification.

    Given: The provider body raises a domain failure and engine disposal also fails.
    When: The provider context unwinds.
    Then: The original domain failure remains the caller-visible outcome.
    """
    harness.engine.dispose.side_effect = RuntimeError(_URL)
    provider = RefreshConnection(_URL, _budget())
    with pytest.raises(ValueError, match="^body_failed$"):
        async with provider:
            raise ValueError("body_failed")


@pytest.mark.asyncio
async def test_engine_disposal_remains_inside_original_deadline(harness: _Harness) -> None:
    """Attempt cleanup at expiry without renewing the host's work budget.

    Given: The inherited deadline expires after disconnected provider construction.
    When: The caller requests engine disposal.
    Then: Disposal is attempted with no extension and cannot be reported as successful cleanup.
    """
    clock = _Clock()
    provider = RefreshConnection(_URL, _budget(clock))
    clock.now = 46.0
    with pytest.raises(RefreshConnectionError, match="^refresh_engine_cleanup_failed$"):
        await provider.aclose()
    harness.engine.dispose.assert_awaited_once()


@pytest.mark.asyncio
async def test_disposal_cancellation_propagates(harness: _Harness) -> None:
    """External cancellation is never converted into a sanitized ordinary failure.

    Given: Owned engine disposal raises cancellation.
    When: The provider context exits.
    Then: Cancellation propagates directly and the provider remains closed to later work.
    """
    harness.engine.dispose.side_effect = asyncio.CancelledError()
    provider = RefreshConnection(_URL, _budget())
    with pytest.raises(asyncio.CancelledError):
        await provider.__aexit__(None, None, None)
    assert provider._closed is True


@pytest.mark.asyncio
async def test_cleanup_cancellation_propagates(harness: _Harness) -> None:
    """Keep cancellation observable when it arrives during session cleanup.

    Given: A session body succeeds but the owned close operation raises cancellation.
    When: The session context exits.
    Then: Cancellation reaches the caller and the active ownership marker is cleared.
    """
    harness.session.close.side_effect = asyncio.CancelledError()
    provider = RefreshConnection(_URL, _budget())
    with pytest.raises(asyncio.CancelledError):
        async with provider.session():
            pass
    assert provider._active is False


@pytest.mark.asyncio
async def test_bounded_helper_refuses_missing_task(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reject operation and session ownership without an asynchronous task.

    Given: The task lookup reports no owning asynchronous task.
    When: The operation and session deadline helpers attempt admission.
    Then: Both refuse with the fixed ownership error before invoking the deferred operation.
    """
    operation = AsyncMock()
    monkeypatch.setattr(connection_module.asyncio, "current_task", lambda: None)
    with pytest.raises(RefreshConnectionError, match="^refresh_connection_requires_task$"):
        await connection_module._bounded(operation, _budget())
    with pytest.raises(RefreshConnectionError, match="^refresh_connection_requires_task$"):
        async with connection_module._session_deadline(_budget()):
            raise AssertionError("No task was admitted")
    operation.assert_not_awaited()
