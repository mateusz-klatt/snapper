"""Async-safe in-memory ring buffer for SystemMetricsSnapshot history.

Single-writer (the snapshotter sampler task) + multi-reader (route
handlers) wrapped around :class:`collections.deque(maxlen=N)` with an
:class:`asyncio.Lock` for read consistency. Append is O(1) with
oldest-first eviction at cap; slice is O(N) over the deque, which is
acceptable for cap=17280 (24h at 5s) and the read frequency of an
operator dashboard.

The slice contract takes ISO-8601 ``datetime`` bounds at the route
layer; the route handler converts the query strings to ``datetime``
before calling :meth:`MetricsRingBuffer.slice`, so the buffer itself
sees only typed datetimes and never parses strings.
"""

import asyncio
from collections import deque
from datetime import datetime
from typing import Final

from snapper.application.system_metrics.snapshot_types import SystemMetricsSnapshot

DEFAULT_HISTORY_CAP: Final = 17280


class MetricsRingBuffer:
    """Bounded FIFO of :class:`SystemMetricsSnapshot` with async-safe access.

    Cap is configurable via the ``maxlen`` constructor argument; default
    17280 ≈ 24h at the standard 5-second sampling interval. At the cap,
    :meth:`append` evicts the oldest snapshot before adding the new one
    (deque's standard semantics under ``maxlen``).
    """

    def __init__(self, maxlen: int = DEFAULT_HISTORY_CAP) -> None:
        """Create an empty buffer with bound ``maxlen``.

        Args:
            maxlen: Maximum snapshot count retained. Beyond this the
                oldest entry is evicted on each append.
        """
        if maxlen <= 0:
            raise ValueError(f"maxlen must be positive, got {maxlen}")
        self._maxlen = maxlen
        self._buf: deque[SystemMetricsSnapshot] = deque(maxlen=maxlen)
        self._lock = asyncio.Lock()

    @property
    def maxlen(self) -> int:
        """Return the configured cap.

        Returns:
            Maximum snapshot count retained before oldest-first eviction.
        """
        return self._maxlen

    async def append(self, snapshot: SystemMetricsSnapshot) -> None:
        """Append a snapshot, evicting the oldest if at cap.

        Holds the buffer lock for the append to keep concurrent
        readers from observing a torn intermediate state.

        Args:
            snapshot: New :class:`SystemMetricsSnapshot` to push onto
                the buffer.
        """
        async with self._lock:
            self._buf.append(snapshot)

    async def latest(self) -> SystemMetricsSnapshot | None:
        """Return the most recently appended snapshot, or ``None`` if empty.

        Returns:
            Most recent snapshot, or ``None`` when no snapshot has been
            appended yet.
        """
        async with self._lock:
            if not self._buf:
                return None
            return self._buf[-1]

    async def slice(
        self,
        since: datetime,
        until: datetime,
        limit: int,
    ) -> list[SystemMetricsSnapshot]:
        """Return snapshots whose ``bus_time`` falls within ``[since, until]``.

        The returned list preserves chronological order (oldest first).
        ``limit`` clamps the result count from the END of the window
        (most recent N within the window) — operators inspecting recent
        history get the freshest data first.

        Args:
            since: Inclusive lower bound (UTC datetime).
            until: Inclusive upper bound (UTC datetime).
            limit: Maximum snapshots returned. Must be positive.

        Returns:
            Filtered list, possibly empty.
        """
        if limit <= 0:
            raise ValueError(f"limit must be positive, got {limit}")
        async with self._lock:
            matched = [snap for snap in self._buf if since <= snap["bus_time"] <= until]
        if len(matched) <= limit:
            return matched
        return matched[-limit:]

    async def size(self) -> int:
        """Return current number of snapshots held.

        Returns:
            Snapshot count in the buffer (0 to ``maxlen``).
        """
        async with self._lock:
            return len(self._buf)
