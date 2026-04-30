"""Opt-in Python tracemalloc controller with auto-stop deadline.

Off by default: enabling tracemalloc costs 5-10% CPU while active and
holds extra metadata in process memory. Operators arm it briefly via
the REST endpoint to capture the ``python_traced`` byte counter for
the ``native_bytes = rss - python_traced`` "native dark matter"
diagnostic, then auto-stop fires after a bounded window.

Default duration 600 seconds, hard max 3600 seconds. The auto-stop
runs as a separate :class:`asyncio.Task` that sleeps the deadline
then calls :meth:`stop`. Calling :meth:`start` while already armed
replaces the deadline (cancels the previous timer) so consecutive
arm calls don't stack.
"""

import asyncio
import contextlib
import tracemalloc
from typing import Final

DEFAULT_DURATION_SECONDS: Final = 600.0
MAX_DURATION_SECONDS: Final = 3600.0


def _clamp_duration(duration_s: float) -> float:
    """Clamp ``duration_s`` to ``(0, MAX_DURATION_SECONDS]``.

    Negative or zero collapses to the default; values beyond the cap
    clamp to the cap.
    """
    if duration_s <= 0:
        return DEFAULT_DURATION_SECONDS
    if duration_s > MAX_DURATION_SECONDS:
        return MAX_DURATION_SECONDS
    return duration_s


class TracemallocController:
    """Wraps :mod:`tracemalloc` with an auto-stop deadline.

    Methods are coroutines so the lifecycle integrates with the
    surrounding asyncio sampler loop (the snapshotter calls
    :meth:`traced_bytes` on every sample). All state is held on the
    instance — there is no module-level singleton, so tests can
    instantiate fresh controllers per case.
    """

    def __init__(self) -> None:
        """Create an inactive controller."""
        self._auto_stop_task: asyncio.Task[None] | None = None

    def is_active(self) -> bool:
        """Return True iff tracemalloc is currently tracing.

        Returns:
            ``True`` when ``tracemalloc.is_tracing()`` is true (the
            controller armed it, or another caller did).
        """
        return tracemalloc.is_tracing()

    def traced_bytes(self) -> int | None:
        """Return current Python-tracked byte count, or ``None`` when inactive.

        ``None`` (rather than 0) when inactive is the operator-facing
        contract: "not measured" vs "measured zero" — both are valid
        states but mean different things.

        Returns:
            Bytes currently tracked by tracemalloc when active; ``None``
            when tracemalloc is off.
        """
        if not tracemalloc.is_tracing():
            return None
        traced, _peak = tracemalloc.get_traced_memory()
        return traced

    async def start(self, duration_s: float = DEFAULT_DURATION_SECONDS) -> None:
        """Arm tracemalloc with an auto-stop deadline.

        Calling while already armed REPLACES the deadline — the
        previous auto-stop timer is cancelled and a fresh one starts
        with the new duration.

        Args:
            duration_s: Seconds before auto-stop fires. Clamped to
                ``(0, MAX_DURATION_SECONDS]`` (default 600, cap 3600).
        """
        clamped = _clamp_duration(duration_s)
        await self._cancel_pending_auto_stop()
        if not tracemalloc.is_tracing():
            tracemalloc.start()
        self._auto_stop_task = asyncio.create_task(self._auto_stop_after(clamped))

    async def stop(self) -> None:
        """Disarm tracemalloc + cancel any pending auto-stop deadline."""
        await self._cancel_pending_auto_stop()
        if tracemalloc.is_tracing():
            tracemalloc.stop()

    async def _auto_stop_after(self, duration_s: float) -> None:
        """Sleep ``duration_s`` then disarm tracemalloc.

        On :class:`asyncio.CancelledError` (caller invoked :meth:`stop`
        before the deadline), the cancellation propagates after marking
        the task as done — Python asyncio convention requires re-raise
        for cooperative cancellation.
        """
        await asyncio.sleep(duration_s)
        if tracemalloc.is_tracing():
            tracemalloc.stop()
        self._auto_stop_task = None

    async def _cancel_pending_auto_stop(self) -> None:
        """Cancel + await the pending auto-stop task if any."""
        task = self._auto_stop_task
        if task is None or task.done():
            self._auto_stop_task = None
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        self._auto_stop_task = None
