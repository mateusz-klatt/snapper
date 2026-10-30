"""Owned completion boundary for public Kraken Futures market discovery."""

import asyncio
import threading
from asyncio import sleep
from collections.abc import Awaitable
from collections.abc import Callable
from typing import Never

from ccxt.base.errors import ExchangeNotAvailable
from requests import HTTPError

_PUBLIC_DISCOVERY_BACKOFF_S = (1.0, 2.0)


class _PublicDiscoveryAttempt:
    """Own admission and actual completion of one entire routed synchronous call."""

    def __init__(self, sync_call: Callable[[], None]) -> None:
        """Capture one routed call and its event-loop completion receiver.

        Args:
            sync_call: Existing public routing operation, including route release.
        """
        self._sync_call = sync_call
        self._loop = asyncio.get_running_loop()
        self._terminal: asyncio.Future[None] = self._loop.create_future()
        self._lock = threading.Lock()
        self._started = False
        self._closed = False

    def run(self) -> None:
        """Enter the whole routed call once admission is still open."""
        with self._lock:
            if self._closed:
                return
            self._started = True
        try:
            self._sync_call()
        finally:
            self._loop.call_soon_threadsafe(self._terminal.set_result, None)

    async def execute(self, dispatch: Callable[[Callable[[], None]], Awaitable[None]]) -> None:
        """Keep dispatcher completion distinct from actual worker completion.

        Args:
            dispatch: Existing client-owned bounded blocking dispatcher.

        Raises:
            BaseException: Original failure after owned work is terminal or fenced.
        """
        task = asyncio.ensure_future(dispatch(self.run))
        try:
            await asyncio.shield(task)
        except BaseException as error:
            await self._close_and_drain(task, error)

    def retryable_503(self, error: ExchangeNotAvailable) -> bool:
        """Recognize a completed public GET 503 from the pinned CCXT HTTP boundary.

        Args:
            error: SDK error whose immediate HTTP context retains structured status.

        Returns:
            True only after this routed worker completed an actual public GET 503.
        """
        context = error.__context__
        if not (self._started and self._terminal.done() and isinstance(context, HTTPError)):
            return False
        response = context.response
        return (
            response is not None
            and response.status_code == 503
            and response.request is not None
            and response.request.method == "GET"
        )

    async def _close_and_drain(self, task: asyncio.Future[None], error: BaseException) -> Never:
        """Drain owned completion before directly propagating failure or cancellation.

        Args:
            task: Dispatcher future, which may fail before its real worker ends.
            error: Original failure whose cancellation identity has precedence.

        Raises:
            BaseException: Original failure, or cancellation received while draining.
        """
        with self._lock:
            self._closed = True
            started = self._started
        cancelled = False
        dispatcher_cancel_requested = False
        while not self._completed(task, started):
            try:
                completion, dispatcher_cancel_requested = self._next_completion(
                    task, started, dispatcher_cancel_requested
                )
                await asyncio.shield(completion)
            except asyncio.CancelledError as cancellation:
                cancelled = True
                if self._completed(task, started):
                    self._consume_failure(task)
                    cancellation = (
                        error if isinstance(error, asyncio.CancelledError) else cancellation
                    )
                    raise cancellation from (
                        cancellation.__cause__ if cancellation is error else error
                    )
            except Exception:
                cancelled = True
        self._consume_failure(task)
        if cancelled and not isinstance(error, asyncio.CancelledError):
            raise asyncio.CancelledError from error
        raise error

    def _completed(self, task: asyncio.Future[None], started: bool) -> bool:
        """Require the actual routed worker and dispatcher to be terminal."""
        return (not started or self._terminal.done()) and task.done()

    def _next_completion(
        self, task: asyncio.Future[None], started: bool, dispatcher_cancel_requested: bool
    ) -> tuple[asyncio.Future[None], bool]:
        """Wait for the worker before cancelling any still-pending dispatcher."""
        if started and not self._terminal.done():
            return self._terminal, dispatcher_cancel_requested
        if not dispatcher_cancel_requested:
            task.cancel()
        return task, True

    @staticmethod
    def _consume_failure(task: asyncio.Future[None]) -> None:
        """Retrieve a terminal dispatcher failure without replacing its original owner."""
        if not task.cancelled():
            task.exception()


async def load_futures_public_markets(
    *,
    dispatch: Callable[[Callable[[], None]], Awaitable[None]],
    sync_call: Callable[[], None],
    record_call: Callable[[], None],
) -> None:
    """Retry only completed public GET 503 discovery, with three bounded attempts.

    Each attempt uses the unchanged public router. Backoff is asynchronous,
    one then two seconds. Cancellation closes unstarted admission or drains
    the exact started routed worker before client cleanup; it never retries.

    Args:
        dispatch: Existing bounded client dispatcher without changed cancellation policy.
        sync_call: Entire unchanged public route, SDK call and route cleanup.
        record_call: Existing REST-attempt metric callback.
    """
    index = 0
    while True:
        attempt = _PublicDiscoveryAttempt(sync_call)
        record_call()
        try:
            await attempt.execute(dispatch)
            return
        except ExchangeNotAvailable as error:
            if not attempt.retryable_503(error) or index == len(_PUBLIC_DISCOVERY_BACKOFF_S):
                raise
            await sleep(_PUBLIC_DISCOVERY_BACKOFF_S[index])
            index += 1
