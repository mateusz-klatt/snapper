"""Retain native source ownership through cancellation and ambiguous executor enqueue failures.

Only the two existing authenticated source reads use this local ownership rule.
The actual egress dispatcher and SDK call remain unchanged. Cancellation waits
for a started source callback; socket reads may outlast between-chunk elapsed
checks, so this module promises ownership rather than a hard duration limit.
"""

import asyncio
import threading
from collections.abc import Callable
from typing import Literal

from kraken.futures import User

from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)
from snapper.infrastructure.exchanges.kraken_rest_egress import futures_sdk_proxy_target
from snapper.infrastructure.exchanges.kraken_rest_egress import route_kraken_rest_sync_call


class _NativeSourceReadRefusedError(RuntimeError):
    """An abandoned or already-successful source cannot perform another HTTP request."""


class _OwnedNativeSource[ResultT]:
    """Separate complete routed-callback cleanup from the dispatcher's enqueue acknowledgement."""

    def __init__(self, read: Callable[[], ResultT]) -> None:
        """Own the exact source callback, one synchronized start decision and its eventual result."""
        self._lock = threading.Lock()
        self._loop = asyncio.get_running_loop()
        self.completed: asyncio.Future[None] = self._loop.create_future()
        self._read: Callable[[], ResultT] | None = read
        self._started = False
        self._result: tuple[ResultT] | None = None

    def invoke(self) -> ResultT:
        """Keep the actual router's pre-send fallback while refusing abandoned or completed source I/O."""
        with self._lock:
            read = self._read
            if read is None:
                raise _NativeSourceReadRefusedError("owned_native_source_read_not_startable")
        result = read()
        with self._lock:
            self._read = None
            self._result = (result,)
        return result

    def run(self, routed: Callable[[], ResultT]) -> ResultT:
        """Signal completion only after the original router restores proxy state and releases its lock."""
        with self._lock:
            if self._read is None:
                raise _NativeSourceReadRefusedError("owned_native_source_route_not_startable")
            self._started = True
        try:
            return routed()
        finally:
            with self._lock:
                self._read = None
            self._loop.call_soon_threadsafe(self.completed.set_result, None)

    def abandon(self) -> bool:
        """Close the HTTP start gate before awaiting and report whether the actual routed worker began."""
        with self._lock:
            self._read = None
            return self._started

    def dispose(self, callback: Callable[[ResultT], None] | None) -> None:
        """Clear an unreturned original result and close its actual token when the caller requires it."""
        with self._lock:
            result = self._result
            self._result = None
        if result is not None and callback is not None:
            callback(result[0])


class _CallerCancellation:
    """Retain the first cancellation delivered while awaiting an uncancelled completion promise."""

    def __init__(self) -> None:
        """Start without inferring a signal from any previously handled cancellation count."""
        self.first: asyncio.CancelledError | None = None

    def observe(self, interrupted: asyncio.CancelledError) -> None:
        """Keep the first real caller delivery while later requests merely extend owned cleanup."""
        if self.first is None:
            self.first = interrupted


def _dispatch_completion[ResultT](task: asyncio.Task[ResultT]) -> asyncio.Future[None]:
    """Expose terminal dispatch completion without forwarding child cancellation or its exception."""
    completion: asyncio.Future[None] = asyncio.get_running_loop().create_future()

    def completed(result: asyncio.Task[ResultT]) -> None:
        """Retrieve any dispatch failure and resolve the independently owned completion promise."""
        if not result.cancelled():
            result.exception()
        completion.set_result(None)

    task.add_done_callback(completed)
    return completion


async def _await_owned_completion(
    completion: asyncio.Future[None], cancellation: _CallerCancellation
) -> None:
    """Observe only caller cancellation because the shielded promise never carries child errors."""
    try:
        await asyncio.shield(completion)
    except asyncio.CancelledError as interrupted:
        cancellation.observe(interrupted)
        raise


async def _settle_owned_future(
    completion: asyncio.Future[None], cancellation: _CallerCancellation
) -> None:
    """Join an independently resolved completion promise through repeated actual caller cancellation."""
    while not completion.done():
        try:
            await _await_owned_completion(completion, cancellation)
        except asyncio.CancelledError:
            continue


async def read_owned_native_source[ResultT](
    client: KrakenFuturesExchangeClient,
    user: User,
    operation: Literal["prepare_native_private_key_identity", "get_native_accounts_source"],
    read: Callable[[], ResultT],
    dispose_cancelled: Callable[[ResultT], None] | None = None,
) -> ResultT:
    """Own one actual native source through async cancellation and uncertain enqueue acknowledgement.

    Args:
        client: Actual selected Futures client whose existing dispatcher owns the REST executor.
        user: Same original SDK User already validated by the source operation.
        operation: One of the two existing fixed authenticated source-read operation names.
        read: Exact synchronous SDK source call under the original router and its eligible pre-send
            fallback policy, without any new application retry.
        dispose_cancelled: Nonthrowing synchronous cleanup of an actual late result, including one
            arriving after dispatch failure.

    Returns:
        The original successful source result, with cleanup responsibility transferred to the caller.

    Raises:
        BaseException: Delivered cancellation or the original dispatch error after routed work joins.
    """
    source = _OwnedNativeSource(read)
    caller = _CallerCancellation()
    target = futures_sdk_proxy_target(user)

    def routed() -> ResultT:
        """Run the unchanged original router with its original selected client, target and proxy lock."""
        return route_kraken_rest_sync_call(
            exchange=str(client.exchange_name),
            operation=operation,
            kind="private_idempotent_read",
            target=target,
            proxy_lock=client._rest_egress_proxy_lock,
            sync_call=source.invoke,
        )

    task = asyncio.create_task(client._dispatch_blocking(source.run, routed))
    completion = _dispatch_completion(task)
    try:
        await _await_owned_completion(completion, caller)
        return task.result()
    except BaseException:
        started = source.abandon()
        await _settle_owned_future(completion, caller)
        if started:
            await _settle_owned_future(source.completed, caller)
        source.dispose(dispose_cancelled)
        cancellation = caller.first
        if cancellation is None:
            raise
    cancellation.__context__ = None
    raise cancellation from None
