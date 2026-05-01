"""Async scheduler that periodically invokes :class:`RetentionService.run_once`.

Lifecycle:

  * :meth:`start` reads ``RETENTION_DISABLED``; when truthy, returns
    without running an eager pass and without spawning the loop.
    Otherwise: takes ONE eager :meth:`RetentionService.run_once` so the
    metrics route never sees an empty buffer after lifespan startup,
    then spawns the sampler loop.
  * :meth:`_loop` sleeps the configured interval, then calls
    ``run_once``. ``run_once`` is designed to swallow per-policy
    errors, but the loop itself wraps the call in a defensive
    try/except so a bug in ``run_once`` itself logs + the next tick
    still runs (Codex re-review NEW MAJOR fix).
  * :meth:`stop` signals the loop, cancels + awaits the task, then
    closes the underlying :class:`RetentionService`.
"""

import asyncio
import contextlib
import logging
import os
from pathlib import Path

from snapper.application.retention.policies import resolve_disabled
from snapper.application.retention.policies import resolve_interval
from snapper.application.retention.policies import resolve_output_dir
from snapper.application.retention.service import RetentionRunSummary
from snapper.application.retention.service import RetentionService

logger = logging.getLogger(__name__)

_INTERVAL_ENV_VAR = "RETENTION_INTERVAL_SECONDS"
_DISABLED_ENV_VAR = "RETENTION_DISABLED"
_OUTPUT_DIR_ENV_VAR = "RETENTION_OUTPUT_DIR"


class RetentionScheduler:
    """Owns the :class:`RetentionService` + drives the async sampler loop."""

    def __init__(
        self,
        *,
        db_url: str,
        base_dir: Path | None = None,
        service: RetentionService | None = None,
        interval_seconds: float | None = None,
        disabled: bool | None = None,
    ) -> None:
        """Wire dependencies for the scheduler.

        Args:
            db_url: SQLAlchemy URL for the underlying sync repository.
            base_dir: Filesystem root for archive output. ``None``
                reads ``RETENTION_OUTPUT_DIR`` env var (default
                ``"data"``).
            service: Override for tests; ``None`` builds a fresh
                :class:`RetentionService`.
            interval_seconds: Loop period in seconds. ``None`` reads
                ``RETENTION_INTERVAL_SECONDS`` env var (default 3600).
            disabled: Disable flag override; ``None`` reads
                ``RETENTION_DISABLED`` env var.
        """
        if base_dir is None:
            base_dir = Path(resolve_output_dir(os.environ.get(_OUTPUT_DIR_ENV_VAR)))
        if interval_seconds is None:
            interval_seconds = resolve_interval(os.environ.get(_INTERVAL_ENV_VAR))
        if disabled is None:
            disabled = resolve_disabled(os.environ.get(_DISABLED_ENV_VAR))
        self._db_url = db_url
        self._base_dir = base_dir
        self._service = service or RetentionService(db_url=db_url, base_dir=base_dir)
        self._interval_seconds = interval_seconds
        self._disabled = disabled
        self._stopping = asyncio.Event()
        self._loop_task: asyncio.Task[None] | None = None

    @property
    def disabled(self) -> bool:
        """Return whether this scheduler is in the disabled state.

        Returns:
            ``True`` iff ``RETENTION_DISABLED`` was truthy at
            construction time; the eager run + loop are skipped and
            the metrics route reports the disabled detail.
        """
        return self._disabled

    @property
    def interval_seconds(self) -> float:
        """Return the configured loop period in seconds.

        Returns:
            Seconds the loop sleeps between ticks.
        """
        return self._interval_seconds

    @property
    def last_run_summary(self) -> RetentionRunSummary | None:
        """Return the underlying service's most recent run summary, or ``None``.

        Returns:
            The latest :class:`RetentionRunSummary` populated by the
            eager run + every subsequent tick, or ``None`` until the
            eager run completes (or always ``None`` when disabled).
        """
        return self._service.last_run_summary

    async def start(self) -> None:
        """Take one eager :meth:`RetentionService.run_once` then spawn the loop.

        When ``self.disabled`` is true, returns immediately without
        running an eager pass and without spawning the loop — the
        scheduler is parked.
        """
        if self._disabled:
            logger.info("RetentionScheduler: disabled (RETENTION_DISABLED=true); skipping start")
            return
        await self._service.run_once()
        self._stopping.clear()
        self._loop_task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        """Signal the loop to exit, await its task, then close the service.

        Tolerates a partial-init state where :meth:`start` was never
        called (e.g. when the scheduler was constructed in disabled
        mode) — the service is still closed so the underlying repo's
        engine is disposed cleanly.
        """
        self._stopping.set()
        task = self._loop_task
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._loop_task = None
        await self._service.close()

    async def _loop(self) -> None:
        """Sleep ``interval_seconds`` then call ``run_once``; repeat until cancelled.

        Defensive try/except: ``run_once`` is contractually
        non-raising (it swallows per-policy errors), but a bug in
        ``run_once`` itself MUST NOT kill the loop. On exception the
        loop logs + sleeps again.
        """
        while not self._stopping.is_set():
            await asyncio.sleep(self._interval_seconds)
            if self._stopping.is_set():
                return
            try:
                await self._service.run_once()
            except Exception:
                logger.exception("RetentionService.run_once raised; continuing on next tick")
