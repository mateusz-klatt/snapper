"""Periodic driver for AI-review maintenance.

The FastAPI lifespan owns one instance. Each guarded pass resolves a
repository and calls the public :meth:`AiReviewService.maintenance_tick`
seam, which reaps expired reviews before scanning for offline-delegate
fanout.

Configuration:

* ``AI_REVIEW_MAINTENANCE_INTERVAL_SECONDS`` controls the cadence. The
  default and minimum are one minute because both operations are bounded
  safety-net scans and do not need to become hot database polls.
"""

import asyncio
import contextlib
import logging
import os
from collections.abc import Callable
from datetime import datetime
from typing import Final
from typing import Protocol

from snapper.data.repository import Repository

logger = logging.getLogger(__name__)

DEFAULT_INTERVAL_SECONDS: Final = 60.0
MIN_INTERVAL_SECONDS: Final = 60.0

_INTERVAL_ENV_VAR: Final = "AI_REVIEW_MAINTENANCE_INTERVAL_SECONDS"

ENV_VARS: Final[frozenset[str]] = frozenset({_INTERVAL_ENV_VAR})
"""Public allowlist of environment variables owned by this service."""


class _MaintenanceTarget(Protocol):
    """Public AI-review maintenance seam needed by the driver."""

    async def maintenance_tick(
        self,
        *,
        repo: Repository,
        now: datetime | None = None,
    ) -> tuple[int, int]:
        """Run one reaper pass followed by one offline-scanner pass."""
        ...


def _resolve_interval(env_value: str | None) -> float:
    """Parse the cadence, defaulting invalid input and clamping low values.

    Args:
        env_value: Raw environment value, or ``None`` when unset.

    Returns:
        Positive cadence in seconds, clamped to
        :data:`MIN_INTERVAL_SECONDS`.
    """
    if env_value is None:
        return DEFAULT_INTERVAL_SECONDS
    try:
        parsed = float(env_value)
    except ValueError:
        return DEFAULT_INTERVAL_SECONDS
    if parsed <= 0:
        return DEFAULT_INTERVAL_SECONDS
    return max(parsed, MIN_INTERVAL_SECONDS)


class AiReviewMaintenanceService:
    """Periodically run the AI-review reaper and offline scanner."""

    def __init__(
        self,
        *,
        service: _MaintenanceTarget,
        repository_factory: Callable[[], Repository],
        interval_seconds: float | None = None,
    ) -> None:
        """Wire dependencies and resolve the periodic cadence.

        Args:
            service: AI-review service exposing the maintenance seam.
            repository_factory: Factory providing a repository per pass.
            interval_seconds: Explicit cadence override; ``None`` reads
                ``AI_REVIEW_MAINTENANCE_INTERVAL_SECONDS``.
        """
        if interval_seconds is None:
            interval_seconds = _resolve_interval(os.environ.get(_INTERVAL_ENV_VAR))
        self._service = service
        self._repository_factory = repository_factory
        self._interval_seconds = interval_seconds
        self._stopping = asyncio.Event()
        self._loop_task: asyncio.Task[None] | None = None

    @property
    def interval_seconds(self) -> float:
        """Return the configured periodic cadence.

        Returns:
            Seconds between periodic passes.
        """
        return self._interval_seconds

    async def start(self) -> None:
        """Take one defensive eager pass and spawn the periodic loop."""
        await self._guarded_tick()
        self._stopping.clear()
        self._loop_task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """Signal, cancel, and await the loop when it exists."""
        self._stopping.set()
        task = self._loop_task
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._loop_task = None

    async def _loop(self) -> None:
        """Sleep the configured cadence and run passes until stopped."""
        while not self._stopping.is_set():
            await asyncio.sleep(self._interval_seconds)
            if self._stopping.is_set():
                return
            await self._guarded_tick()

    async def _guarded_tick(self) -> None:
        """Run one pass and preserve the loop after any failure."""
        try:
            await self._tick()
        except Exception:
            logger.exception("AiReviewMaintenanceService tick failed; continuing on next tick")

    async def _tick(self, now: datetime | None = None) -> tuple[int, int]:
        """Resolve a repository and run both maintenance operations.

        Args:
            now: Reference instant override for deterministic tests.

        Returns:
            Reaped-review and dispatched-fanout counts.
        """
        return await self._service.maintenance_tick(
            repo=self._repository_factory(),
            now=now,
        )
