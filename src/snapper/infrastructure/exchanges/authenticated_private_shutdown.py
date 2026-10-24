"""Strongly supervised opt-in shutdown without mistaking attempted cancellation for a joined receiver.

A cancelled caller cannot cancel this separately owned cleanup. Until the
actual tracked receive tasks terminate, ownership remains explicitly unresolved
and the client slot must remain occupied. This is not an authentication proof.
"""

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from typing import Never
from typing import SupportsIndex


class PrivateShutdownUnresolvedError(RuntimeError):
    """The actual receive dependency cannot be joined by the task requesting its own shutdown."""


class OwnedPrivateShutdown:
    """Keep one original cleanup and its incomplete ownership alive across caller cancellation."""

    __slots__ = (
        "_stop",
        "_force",
        "_receivers",
        "_quiesce",
        "_release",
        "_task",
        "_released",
        "_failure",
    )

    def __init__(
        self,
        stop: Callable[[], Awaitable[None]],
        force: Callable[[], Awaitable[None]],
        receivers: Callable[[], tuple[asyncio.Task[None], ...]],
        quiesce: Callable[[], None],
        release: Callable[[], None],
    ) -> None:
        """Own only the original client teardown functions and actual tracked receive tasks."""
        self._stop = stop
        self._force = force
        self._receivers = receivers
        self._quiesce = quiesce
        self._release = release
        self._task: asyncio.Task[None] | None = None
        self._released = False
        self._failure: str | None = None

    def __repr__(self) -> str:
        """Hide callable closures and their actual SDK/key objects from ordinary logging."""
        return "<OwnedPrivateShutdown:opaque>"

    def __reduce_ex__(self, protocol: SupportsIndex) -> Never:
        """Refuse serialization of live source ownership and callback closures."""
        raise TypeError("Private shutdown ownership cannot be serialized")

    def __deepcopy__(self, memo: dict[int, object]) -> Never:
        """Refuse copying live source ownership into an unrelated cleanup owner."""
        raise TypeError("Private shutdown ownership cannot be copied")

    @property
    def resolved(self) -> bool:
        """Expose completion only after actual receive joins and original token release.

        Returns:
            Whether cleanup reached its real final ownership release, not merely a timeout or cancel.
        """
        return self._released

    @property
    def failure(self) -> str | None:
        """Expose fixed cleanup status without retaining a provider exception or private traceback.

        Returns:
            A safe fixed error classification, or absence of a recorded cleanup error.
        """
        return self._failure

    async def close(self) -> None:
        """Await one strongly owned shutdown without creating a receiver-to-itself dependency cycle.

        Raises:
            PrivateShutdownUnresolvedError: If this actual receive task would await its own completion.
            asyncio.CancelledError: Caller cancellation leaves the independent cleanup supervised.
        """
        if asyncio.current_task() in self._receivers():
            raise PrivateShutdownUnresolvedError("private_socket_close_from_receive_task")
        if self._task is None:
            self._quiesce()
            self._task = asyncio.create_task(self._finish())
            _SUPERVISED_SHUTDOWNS.add(self)
            self._task.add_done_callback(self._completed)
        await asyncio.shield(self._task)

    async def _finish(self) -> None:
        """Join all actual tracked receives before releasing the original key capability."""
        try:
            await self._stop()
        except BaseException:
            self._failure = "private_socket_original_close_failed"
            try:
                await self._force()
            except BaseException:
                self._failure = "private_socket_forced_close_failed"
        finally:
            tasks = self._receivers()
            for task in tasks:
                if not task.done():
                    task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self._release()
            self._released = True

    def _completed(self, task: asyncio.Task[None]) -> None:
        """Retrieve the real cleanup result and retain only a fixed failure classification."""
        if task.cancelled():
            self._failure = "private_socket_cleanup_cancelled"
        elif task.exception() is not None:
            self._failure = "private_socket_cleanup_failed"
        if self._released:
            _SUPERVISED_SHUTDOWNS.discard(self)


_SUPERVISED_SHUTDOWNS: set[OwnedPrivateShutdown] = set()
