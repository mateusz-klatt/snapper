"""Public Futures discovery retries and real-worker completion ownership."""

import asyncio
import threading
from collections.abc import Callable
from collections.abc import Generator
from concurrent.futures import Executor
from functools import partial
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import ccxt
import pytest
import requests

import snapper.infrastructure.exchanges.kraken_futures_discovery as discovery
from snapper.infrastructure.exchanges.errors import RestPoolDispatchError
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.network.egress_models import EgressPoolConfig
from snapper.infrastructure.network.egress_models import RouteConfig
from snapper.infrastructure.network.egress_pool import EgressPool
from snapper.infrastructure.network.egress_pool import configure_egress_pool
from snapper.infrastructure.network.egress_pool import reset_egress_pool


@pytest.fixture
def public_pool() -> Generator[EgressPool]:
    """Use one real direct public route without performing any network I/O."""
    reset_egress_pool()
    pool = configure_egress_pool(
        EgressPoolConfig(enabled=True, routes=[RouteConfig(id="default", kind="direct")])
    )
    assert pool is not None
    yield pool
    reset_egress_pool()


def _response(method: str, url: str, status: int) -> requests.Response:
    """Return a completed HTTP response for the installed CCXT parser.

    Args:
        method: Actual public request method supplied by CCXT.
        url: Actual public endpoint supplied by CCXT, never fetched.
        status: Completed HTTP status for this response.

    Returns:
        Requests response retaining structured status and request metadata.
    """
    response = requests.Response()
    response.status_code = status
    response.reason = "Service Unavailable" if status == 503 else "OK"
    response.url = url
    response.request = requests.Request(method, url).prepare()
    response.headers["Content-Type"] = "application/json"
    response._content = b'{"result":"success","instruments":[]}'
    return response


def _http_responder(status: int) -> MagicMock:
    """Build a typed, observable in-memory Requests boundary.

    Args:
        status: Completed HTTP status supplied to the installed CCXT parser.

    Returns:
        Mock request callable retaining its invocation count.
    """

    def request(method: str, url: str, **_kwargs: object) -> requests.Response:
        """Complete the SDK request without making any HTTP connection."""
        return _response(method, url, status)

    return MagicMock(side_effect=request)


@pytest.mark.parametrize("original_cancelled", [False, True])
async def test_terminal_worker_and_dispatcher_race_preserves_cancellation_priority(
    original_cancelled: bool,
) -> None:
    """A cancellation at the final completion boundary propagates after both settle.

    Given: an actual worker and its dispatcher are draining an original failure,
    When: another cancellation arrives exactly as both become terminal,
    Then: the original cancellation retains identity or supersedes the prior failure.
    """
    loop = asyncio.get_running_loop()
    started = asyncio.Event()
    release = threading.Event()

    def sync_call() -> None:
        """Keep the actual worker alive until completion is explicitly admitted."""
        loop.call_soon_threadsafe(started.set)
        if not release.wait(timeout=5):
            raise RuntimeError("test worker was not released")

    attempt = discovery._PublicDiscoveryAttempt(sync_call)
    dispatcher: asyncio.Future[None] = loop.create_future()
    original: BaseException = (
        asyncio.CancelledError("original cancellation")
        if original_cancelled
        else RestPoolDispatchError("original dispatcher failure")
    )
    original_cause = RuntimeError("original cancellation cause")
    if original_cancelled:
        original.__cause__ = original_cause
    worker = threading.Thread(target=attempt.run)
    worker.start()
    await started.wait()
    drain = asyncio.create_task(attempt._close_and_drain(dispatcher, original))

    def finish_dispatcher_and_cancel(_completed: asyncio.Future[None]) -> None:
        """Cancel the owner before its shield continuation observes terminal states."""
        dispatcher.set_exception(RuntimeError("terminal dispatcher failure"))
        drain.cancel("terminal cancellation")

    attempt._terminal.add_done_callback(finish_dispatcher_and_cancel)
    try:
        await asyncio.sleep(0)
        release.set()
        with pytest.raises(asyncio.CancelledError) as caught:
            await drain
        if original_cancelled:
            assert caught.value is original
            assert caught.value.args == ("original cancellation",)
            assert caught.value.__cause__ is original_cause
        else:
            assert caught.value.__cause__ is original
            assert caught.value.args == ("terminal cancellation",)
        assert attempt._terminal.done()
        assert dispatcher.done()
    finally:
        release.set()
        worker.join(timeout=5)
        if not dispatcher.cancelled() and dispatcher.done():
            dispatcher.exception()
        if not drain.done():
            drain.cancel()
            with pytest.raises(asyncio.CancelledError):
                await drain
    assert not worker.is_alive()


async def test_dispatcher_cleanup_failure_cannot_replace_original_cancellation() -> None:
    """The owned caller cancellation survives a dispatcher's failing final cleanup.

    Given: an unstarted discovery dispatcher is still waiting when its owner cancels,
    When: stopping that dispatcher raises a separate cleanup error,
    Then: discovery consumes that terminal error and preserves the original cancellation.
    """
    started = asyncio.Event()

    async def dispatch() -> None:
        """Model a dispatcher whose final teardown fails after cancellation admission."""
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            raise RuntimeError("dispatcher teardown failed")

    attempt = discovery._PublicDiscoveryAttempt(MagicMock())
    task = asyncio.create_task(dispatch())
    await started.wait()
    original = asyncio.CancelledError("original owner cancellation")
    with pytest.raises(asyncio.CancelledError) as caught:
        await attempt._close_and_drain(task, original)
    assert caught.value is original
    assert task.done()
    assert not task.cancelled()
    assert isinstance(task.exception(), RuntimeError)


async def test_repeated_owner_cancellation_cannot_interrupt_dispatcher_finalizer() -> None:
    """The dispatcher receives one cancellation while its owner keeps draining.

    Given: dispatcher cancellation starts an event-controlled asynchronous finalizer,
    When: its discovery owner is cancelled repeatedly before that finalizer completes,
    Then: no repeated dispatcher cancellation interrupts its exact terminal cleanup.
    """
    started = asyncio.Event()
    cleanup_started = asyncio.Event()
    cleanup_release = asyncio.Event()
    cleanup_finished = asyncio.Event()

    async def dispatch() -> None:
        """Expose a finalizer that must finish despite later owner cancellations."""
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleanup_started.set()
            await cleanup_release.wait()
            cleanup_finished.set()

    attempt = discovery._PublicDiscoveryAttempt(MagicMock())
    task = asyncio.create_task(dispatch())
    await started.wait()
    original = asyncio.CancelledError("original owner cancellation")
    drain = asyncio.create_task(attempt._close_and_drain(task, original))
    try:
        await cleanup_started.wait()
        for _ in range(3):
            drain.cancel("repeated owner cancellation")
            await asyncio.sleep(0)
            assert not task.done()
            assert not drain.done()
            assert not cleanup_finished.is_set()
    finally:
        cleanup_release.set()
        with pytest.raises(asyncio.CancelledError) as caught:
            await drain
    assert caught.value is original
    assert cleanup_finished.is_set()
    assert task.done()
    assert task.cancelled()


@pytest.mark.parametrize("cancellations", [1, 3])
@pytest.mark.parametrize("terminal_status", [None, 503])
async def test_cancelled_discovery_keeps_session_until_real_worker_terminal(
    monkeypatch: pytest.MonkeyPatch,
    public_pool: EgressPool,
    cancellations: int,
    terminal_status: int | None,
) -> None:
    """Cancellation cannot release the client's lifecycle ahead of its real worker.

    Given: actual bounded-pool discovery is blocked on an explicit thread event,
    When: its connect owner is cancelled once or repeatedly,
    Then: cancellation, session close and pool cleanup wait for worker completion.
    """
    client = KrakenFuturesExchangeClient()
    started = asyncio.Event()
    terminal = asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    events: list[str] = []
    original_load = client._ccxt_client.load_markets
    backoff = AsyncMock()

    def request(method: str, url: str, **_kwargs: object) -> requests.Response:
        """Return a real completed 503 only after the controlled worker is released."""
        return _response(method, url, 503)

    close = MagicMock(side_effect=partial(events.append, "session-close"))

    def load_markets() -> dict[str, object]:
        """Hold a real pool worker until the test explicitly permits completion."""
        events.append("worker-start")
        loop.call_soon_threadsafe(started.set)
        try:
            if not release.wait(timeout=5):
                raise RuntimeError("test worker was not released")
            if terminal_status == 503:
                return cast(dict[str, object], original_load())
            return {}
        finally:
            events.append("worker-terminal")
            loop.call_soon_threadsafe(terminal.set)

    load = MagicMock(side_effect=load_markets)
    monkeypatch.setattr(client._ccxt_client, "load_markets", load)
    monkeypatch.setattr(client._ccxt_client.session, "request", request)
    monkeypatch.setattr(client._ccxt_client.session, "close", close)
    monkeypatch.setattr(discovery, "sleep", backoff)
    connect = asyncio.create_task(client.connect())
    try:
        await started.wait()
        for _ in range(cancellations):
            connect.cancel()
            await asyncio.sleep(0)
        assert not terminal.is_set()
        close.assert_not_called()
        assert not connect.done()
    finally:
        release.set()
        await terminal.wait()
        with pytest.raises(asyncio.CancelledError):
            await connect
        client._shutdown_rest_pool()
    close.assert_called_once_with()
    load.assert_called_once_with()
    backoff.assert_not_awaited()
    assert events.index("worker-terminal") < events.index("session-close")
    assert public_pool.status_snapshot().routes[0].in_use_count == 0
    assert client._rest_pool_closed


@pytest.mark.parametrize("cancel_during_drain", [False, True])
async def test_ambiguous_submit_keeps_session_until_actual_routed_worker_terminal(
    monkeypatch: pytest.MonkeyPatch, public_pool: EgressPool, cancel_during_drain: bool
) -> None:
    """A lost dispatcher future cannot be mistaken for a completed routed worker.

    Given: the real pool starts work before submit reports an ambiguous failure,
    When: connect receives RestPoolDispatchError while that worker is still blocked,
    Then: cleanup waits for the real routed call and the original error still propagates.
    """
    client = KrakenFuturesExchangeClient()
    started = asyncio.Event()
    terminal = asyncio.Event()
    started_sync = threading.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    original_submit = loop.run_in_executor
    queued: list[asyncio.Future[object]] = []
    close = MagicMock()

    def load_markets() -> dict[str, object]:
        """Expose the real worker start before the submit shim raises."""
        started_sync.set()
        loop.call_soon_threadsafe(started.set)
        try:
            if not release.wait(timeout=5):
                raise RuntimeError("test worker was not released")
            return {}
        finally:
            loop.call_soon_threadsafe(terminal.set)

    def enqueue_then_fail(
        executor: Executor | None, call: Callable[..., object], *args: object
    ) -> asyncio.Future[object]:
        """Retain an actual submitted future while reporting post-enqueue failure."""
        queued.append(original_submit(executor, call, *args))
        if not started_sync.wait(timeout=5):
            raise RuntimeError("test worker did not start")
        raise RuntimeError("can't start new thread")

    monkeypatch.setattr(client._ccxt_client, "load_markets", load_markets)
    monkeypatch.setattr(client._ccxt_client.session, "close", close)
    monkeypatch.setattr(loop, "run_in_executor", enqueue_then_fail)
    connect = asyncio.create_task(client.connect())
    try:
        await started.wait()
        await asyncio.sleep(0)
        if cancel_during_drain:
            for _ in range(2):
                connect.cancel()
                await asyncio.sleep(0)
        close.assert_not_called()
        assert not connect.done()
    finally:
        release.set()
        await terminal.wait()
        expected = asyncio.CancelledError if cancel_during_drain else RestPoolDispatchError
        with pytest.raises(expected):
            await connect
        for future in queued:
            await future
        client._shutdown_rest_pool()
    close.assert_called_once_with()
    assert public_pool.status_snapshot().routes[0].in_use_count == 0


async def test_late_callback_after_ambiguous_submit_cannot_start_sdk_or_route(
    monkeypatch: pytest.MonkeyPatch, public_pool: EgressPool
) -> None:
    """Closed admission fences a real callback that has not entered the routed call.

    Given: a real worker is held immediately before the callback enters discovery,
    When: submit reports failure and connect has already cleaned up,
    Then: releasing that late callback cannot reserve a route or start CCXT.
    """
    client = KrakenFuturesExchangeClient()
    callback_started = asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    original_submit = loop.run_in_executor
    queued: list[asyncio.Future[object]] = []
    load = MagicMock(return_value={})
    reserve = MagicMock(wraps=public_pool.reserve)

    def enqueue_delayed_then_fail(
        executor: Executor | None, call: Callable[..., object], *args: object
    ) -> asyncio.Future[object]:
        """Start a real worker but defer admission until after the dispatcher error."""

        def delayed_callback() -> object:
            """Hold the already-running worker before its actual submitted callback."""
            loop.call_soon_threadsafe(callback_started.set)
            if not release.wait(timeout=5):
                raise RuntimeError("test callback was not released")
            return call(*args)

        queued.append(original_submit(executor, delayed_callback))
        raise RuntimeError("can't start new thread")

    monkeypatch.setattr(client._ccxt_client, "load_markets", load)
    monkeypatch.setattr(public_pool, "reserve", reserve)
    monkeypatch.setattr(loop, "run_in_executor", enqueue_delayed_then_fail)
    try:
        with pytest.raises(RestPoolDispatchError, match="can't start new thread"):
            await client.connect()
        await callback_started.wait()
        assert client._rest_pool_closed
    finally:
        release.set()
        for future in queued:
            await future
        client._shutdown_rest_pool()
    load.assert_not_called()
    reserve.assert_not_called()


async def test_completed_public_503_retries_through_installed_ccxt_and_real_router(
    monkeypatch: pytest.MonkeyPatch, public_pool: EgressPool
) -> None:
    """A completed public HTTP 503 is retried after its route has been released.

    Given: installed pinned CCXT receives a completed 503 then a valid public response,
    When: Futures connects through the actual bounded worker and public router,
    Then: discovery succeeds with two separately balanced route reservations.
    """
    assert ccxt.__version__ == "4.5.91"
    client = KrakenFuturesExchangeClient()
    statuses = iter([503, 200])
    observed: list[int] = []
    reserve = MagicMock(wraps=public_pool.reserve)
    record = MagicMock(wraps=client._record_rest_call)

    def request(method: str, url: str, **_kwargs: object) -> requests.Response:
        """Complete the real CCXT HTTP boundary entirely in memory."""
        status = next(statuses)
        observed.append(status)
        assert method == "GET"
        assert public_pool.status_snapshot().routes[0].in_use_count == 1
        return _response(method, url, status)

    monkeypatch.setattr(client._ccxt_client.session, "request", request)
    monkeypatch.setattr(public_pool, "reserve", reserve)
    monkeypatch.setattr(client, "_record_rest_call", record)
    try:
        await client.connect()
        assert observed == [503, 200]
        assert reserve.call_count == 2
        assert record.call_count == 2
        assert client._market_client is not None
        assert public_pool.status_snapshot().routes[0].in_use_count == 0
    finally:
        await client.disconnect()


@pytest.mark.parametrize("status", [400, 401, 403, 404, 429, 500, 502, 504])
async def test_other_completed_http_statuses_are_never_retried(
    monkeypatch: pytest.MonkeyPatch, public_pool: EgressPool, status: int
) -> None:
    """Only completed 503, never another authentication or service status, can retry.

    Given: a completed public response has a non-503 HTTP status,
    When: Futures discovery processes it through pinned CCXT,
    Then: the original failure propagates after one released route.
    """
    client = KrakenFuturesExchangeClient()
    request = _http_responder(status)
    backoff = AsyncMock()
    monkeypatch.setattr(client._ccxt_client.session, "request", request)
    monkeypatch.setattr(discovery, "sleep", backoff)
    try:
        with pytest.raises(ccxt.BaseError):
            await client.connect()
        request.assert_called_once()
        backoff.assert_not_awaited()
        assert public_pool.status_snapshot().routes[0].in_use_count == 0
        assert client._rest_pool_closed
    finally:
        await client.disconnect()


async def test_three_terminal_503_attempts_exhaust_with_balanced_routes_and_capped_backoff(
    monkeypatch: pytest.MonkeyPatch, public_pool: EgressPool
) -> None:
    """Three completed failures consume three routes and delays of one then two seconds.

    Given: the public endpoint completes every call with HTTP 503,
    When: discovery exhausts its fixed attempt cap,
    Then: exactly three calls and one then two second delays occur with no held route.
    """
    client = KrakenFuturesExchangeClient()
    request = _http_responder(503)
    delays: list[float] = []
    reserve = MagicMock(wraps=public_pool.reserve)
    native_retries = client._ccxt_client.options.get("maxRetriesOnFailure")

    async def backoff(delay: float) -> None:
        """Observe released routing between completed worker attempts."""
        assert public_pool.status_snapshot().routes[0].in_use_count == 0
        delays.append(delay)

    monkeypatch.setattr(client._ccxt_client.session, "request", request)
    monkeypatch.setattr(public_pool, "reserve", reserve)
    monkeypatch.setattr(discovery, "sleep", backoff)
    try:
        with pytest.raises(ccxt.ExchangeNotAvailable):
            await client.connect()
        assert request.call_count == 3
        assert reserve.call_count == 3
        assert delays == [1.0, 2.0]
        assert client._ccxt_client.options.get("maxRetriesOnFailure") == native_retries
        assert client._ccxt_client.timeout == 30000
        assert public_pool.status_snapshot().routes[0].in_use_count == 0
        assert client._rest_pool_closed
    finally:
        await client.disconnect()


async def test_cancellation_during_backoff_starts_no_new_discovery(
    monkeypatch: pytest.MonkeyPatch, public_pool: EgressPool
) -> None:
    """A backoff cancellation closes after the completed worker without another attempt.

    Given: one completed 503 has released its public route,
    When: the connect owner is cancelled during asynchronous backoff,
    Then: the client closes without another SDK attempt.
    """
    client = KrakenFuturesExchangeClient()
    request = _http_responder(503)
    backoff_started = asyncio.Event()
    close = MagicMock()

    async def backoff(_delay: float) -> None:
        """Hold retry admission outside all worker and route ownership."""
        assert public_pool.status_snapshot().routes[0].in_use_count == 0
        backoff_started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(client._ccxt_client.session, "request", request)
    monkeypatch.setattr(client._ccxt_client.session, "close", close)
    monkeypatch.setattr(discovery, "sleep", backoff)
    connect = asyncio.create_task(client.connect())
    await backoff_started.wait()
    connect.cancel()
    with pytest.raises(asyncio.CancelledError):
        await connect
    request.assert_called_once()
    close.assert_called_once_with()
    assert public_pool.status_snapshot().routes[0].in_use_count == 0
    assert client._rest_pool_closed


@pytest.mark.parametrize(
    "error_type", [ccxt.ExchangeNotAvailable, ccxt.RequestTimeout, RuntimeError, TypeError]
)
async def test_exception_text_cannot_authorize_retry_without_completed_http_503(
    monkeypatch: pytest.MonkeyPatch, public_pool: EgressPool, error_type: type[Exception]
) -> None:
    """Network/programming classes or text mentioning 503 do not establish an HTTP response.

    Given: an SDK or programming exception merely contains 503 text,
    When: discovery evaluates the failure without a structured HTTP response,
    Then: the exception propagates without backoff or another attempt.
    """
    client = KrakenFuturesExchangeClient()
    load = MagicMock(side_effect=error_type("503 Service Unavailable"))
    backoff = AsyncMock()
    monkeypatch.setattr(client._ccxt_client, "load_markets", load)
    monkeypatch.setattr(discovery, "sleep", backoff)
    with pytest.raises(error_type):
        await client.connect()
    load.assert_called_once_with()
    backoff.assert_not_awaited()
    assert public_pool.status_snapshot().routes[0].in_use_count == 0
    assert client._rest_pool_closed


@pytest.mark.parametrize("request_method", [None, "POST"])
async def test_503_without_actual_get_request_is_not_retried(
    monkeypatch: pytest.MonkeyPatch, public_pool: EgressPool, request_method: str | None
) -> None:
    """A completed error without public GET metadata fails closed.

    Given: an HTTP 503 response lacks actual public GET request evidence,
    When: the installed CCXT error reaches discovery,
    Then: one call fails closed without retry.
    """
    client = KrakenFuturesExchangeClient()
    backoff = AsyncMock()

    def request(method: str, url: str, **_kwargs: object) -> requests.Response:
        """Provide an HTTP response with missing or non-GET request evidence."""
        response = _response(method, url, 503)
        if request_method is None:
            response.request = None
        else:
            response.request.method = request_method
        return response

    call = MagicMock(side_effect=request)
    monkeypatch.setattr(client._ccxt_client.session, "request", call)
    monkeypatch.setattr(discovery, "sleep", backoff)
    with pytest.raises(ccxt.ExchangeNotAvailable):
        await client.connect()
    call.assert_called_once()
    backoff.assert_not_awaited()
    assert public_pool.status_snapshot().routes[0].in_use_count == 0


async def test_dispatcher_503_without_started_public_worker_cannot_retry() -> None:
    """An SDK-shaped dispatcher error is not proof that this discovery worker ran.

    Given: a dispatcher reports an HTTP-shaped error without admitting its callback,
    When: discovery classifies the failure,
    Then: no SDK call or retry can occur.
    """
    sync_call = MagicMock()
    record = MagicMock()

    async def dispatch(_call: Callable[[], None]) -> None:
        """Refuse scheduling with HTTP-shaped context, without starting the callback."""
        try:
            _response("GET", "https://example.invalid/public", 503).raise_for_status()
        except requests.HTTPError:
            raise ccxt.ExchangeNotAvailable("untrusted dispatcher 503") from None

    with pytest.raises(ccxt.ExchangeNotAvailable):
        await discovery.load_futures_public_markets(
            dispatch=dispatch, sync_call=sync_call, record_call=record
        )
    sync_call.assert_not_called()
    record.assert_called_once_with()


async def test_discovery_can_succeed_on_its_last_bounded_503_attempt(
    monkeypatch: pytest.MonkeyPatch, public_pool: EgressPool
) -> None:
    """Two completed 503s leave no route held before the third attempt succeeds.

    Given: two public requests complete with 503 before a successful response,
    When: discovery reaches its third and final permitted attempt,
    Then: success follows two bounded waits and separately released routes.
    """
    client = KrakenFuturesExchangeClient()
    statuses = iter([503, 503, 200])
    observed: list[int] = []
    delays: list[float] = []

    def request(method: str, url: str, **_kwargs: object) -> requests.Response:
        """Supply exactly the finite public response sequence."""
        status = next(statuses)
        observed.append(status)
        assert public_pool.status_snapshot().routes[0].in_use_count == 1
        return _response(method, url, status)

    async def backoff(delay: float) -> None:
        """Assert the previous worker released its complete public route."""
        assert public_pool.status_snapshot().routes[0].in_use_count == 0
        delays.append(delay)

    monkeypatch.setattr(client._ccxt_client.session, "request", request)
    monkeypatch.setattr(discovery, "sleep", backoff)
    try:
        await client.connect()
        assert observed == [503, 503, 200]
        assert delays == [1.0, 2.0]
        assert client._market_client is not None
        assert public_pool.status_snapshot().routes[0].in_use_count == 0
    finally:
        await client.disconnect()


async def test_http_error_context_without_response_never_authorizes_retry(
    monkeypatch: pytest.MonkeyPatch, public_pool: EgressPool
) -> None:
    """An HTTP-shaped SDK failure without a structured response remains terminal.

    Given: an SDK exception has HTTP error context but no completed response,
    When: the routed worker fails,
    Then: discovery propagates the error without retry.
    """
    client = KrakenFuturesExchangeClient()
    backoff = AsyncMock()

    def load_markets() -> dict[str, object]:
        """Provide incomplete HTTP evidence through the actual routed worker."""
        try:
            raise requests.HTTPError("no completed response")
        except requests.HTTPError:
            raise ccxt.ExchangeNotAvailable("503 text is not a response") from None

    load = MagicMock(side_effect=load_markets)
    monkeypatch.setattr(client._ccxt_client, "load_markets", load)
    monkeypatch.setattr(discovery, "sleep", backoff)
    with pytest.raises(ccxt.ExchangeNotAvailable):
        await client.connect()
    load.assert_called_once_with()
    backoff.assert_not_awaited()
    assert public_pool.status_snapshot().routes[0].in_use_count == 0


async def test_cancel_before_route_admission_fences_late_real_worker_callback(
    monkeypatch: pytest.MonkeyPatch, public_pool: EgressPool
) -> None:
    """Cancellation can close an unstarted route without letting a late callback use CCXT.

    Given: a real callback is held before entering the routed operation,
    When: connect is cancelled and the callback is later released,
    Then: closed admission prevents both route reservation and SDK execution.
    """
    client = KrakenFuturesExchangeClient()
    callback_started = asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    original_submit = loop.run_in_executor
    queued: list[asyncio.Future[object]] = []
    load = MagicMock(return_value={})
    reserve = MagicMock(wraps=public_pool.reserve)

    def enqueue_delayed(
        executor: Executor | None, call: Callable[..., object], *args: object
    ) -> asyncio.Future[object]:
        """Start a real callback which has not yet entered the admission wrapper."""

        def delayed_callback() -> object:
            """Wait outside the routed call until cancellation closes its admission."""
            loop.call_soon_threadsafe(callback_started.set)
            if not release.wait(timeout=5):
                raise RuntimeError("test callback was not released")
            return call(*args)

        future = original_submit(executor, delayed_callback)
        queued.append(future)
        return future

    monkeypatch.setattr(client._ccxt_client, "load_markets", load)
    monkeypatch.setattr(public_pool, "reserve", reserve)
    monkeypatch.setattr(loop, "run_in_executor", enqueue_delayed)
    connect = asyncio.create_task(client.connect())
    try:
        await callback_started.wait()
        connect.cancel()
        with pytest.raises(asyncio.CancelledError):
            await connect
        assert client._rest_pool_closed
    finally:
        release.set()
        for future in queued:
            if not future.cancelled():
                await future
        client._shutdown_rest_pool()
    load.assert_not_called()
    reserve.assert_not_called()
