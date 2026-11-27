"""Periodic public market snapshot refresher process.

Runs inside the process manager and periodically calls the asynchronous core
of the one-shot CLI snapshot updaters across all supported public venues so
``market_snapshots`` rows stay fresher than USDConverter's 300-second
staleness threshold without contacting private/authenticated exchange APIs.
"""

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Sequence
from time import monotonic

from loguru import logger

from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.process_parameters import MarketSnapshotRefresherParameters
from snapper.application.process_manager.registry import register_process
from snapper.config.settings import AppSettings
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRestartPolicyEnum
from snapper.core.types import ProcessRoleEnum
from snapper.infrastructure.market_data import kraken as kraken_market_data
from snapper.infrastructure.market_data import kraken_equities as kraken_equities_market_data
from snapper.infrastructure.market_data import kraken_futures as kraken_futures_market_data
from snapper.infrastructure.market_data import walutomat as walutomat_market_data
from snapper.utils.logging import set_log_context

DEFAULT_INTERVAL_SECONDS: float = 120.0

VenueSnapshotUpdater = Callable[[], Awaitable[None]]


def _default_venue_updaters() -> tuple[tuple[str, VenueSnapshotUpdater], ...]:
    """Return ordered public venue snapshot updater callables.

    Returns:
        Sequence of ``(venue_name, async_updater)`` pairs matching the
        one-shot market snapshot CLI commands.
    """
    return (
        ("kraken", kraken_market_data.async_update_snapshots),
        ("kraken_futures", kraken_futures_market_data.async_update_snapshots),
        ("kraken_equities", kraken_equities_market_data.async_update_snapshots),
        ("walutomat", walutomat_market_data.async_update_snapshots),
    )


@register_process(
    "market_snapshot_refresher",
    method="start",
    description="Periodic public market snapshot refresher",
    priority=25,
    lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
    role=ProcessRoleEnum.CORE,
    tags=("market-data", "snapshots", "maintenance"),
    parameters_model=MarketSnapshotRefresherParameters,
    enabled=False,
    mode=ProcessModeEnum.THREAD,
    restart_policy=ProcessRestartPolicyEnum.ON_FAILURE,
)
class MarketSnapshotRefresherProcess(RegisterableProcess):
    """Long-running process that refreshes ``market_snapshots`` periodically.

    Executes the public snapshot collection paths shared with the CLI updater
    commands sequentially on each tick and sleeps the remaining configured
    interval so ticks never overlap and shutdown occurs cleanly on stop or
    task cancellation.
    """

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, object]:
        """Return default JSON-serializable constructor parameters.

        Args:
            settings: Application settings instance supplied by the registry
                syncer.

        Returns:
            Default parameter dictionary with ``interval_seconds``.
        """
        _ = settings
        return {"interval_seconds": DEFAULT_INTERVAL_SECONDS}

    def __init__(
        self,
        interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
        venue_updaters: Sequence[tuple[str, VenueSnapshotUpdater]] | None = None,
    ) -> None:
        """Initialize the market snapshot refresher process.

        Args:
            interval_seconds: Interval in seconds between tick starts (30 to
                290, default 120).
            venue_updaters: Optional explicit sequence of ``(venue, coroutine)``
                pairs overriding the default venue snapshot updaters.
        """
        validated = MarketSnapshotRefresherParameters(interval_seconds=interval_seconds)
        self._interval_seconds: float = validated.interval_seconds
        self._venue_updaters: tuple[tuple[str, VenueSnapshotUpdater], ...] | None = (
            tuple(venue_updaters) if venue_updaters is not None else None
        )
        self._running: bool = False
        self._tick_lock: asyncio.Lock = asyncio.Lock()
        self._sleep_task: asyncio.Task[None] | None = None
        self._tick_count: int = 0

    def get_status(self) -> dict[str, object]:
        """Return current runtime status for process manager reporting.

        Returns:
            Status dictionary with running flag, interval, and tick count.
        """
        return {
            "running": self._running,
            "interval_seconds": self._interval_seconds,
            "tick_count": self._tick_count,
        }

    async def run_once(self) -> None:
        """Execute a single sequential refresh pass over every configured venue.

        A failing venue logs a warning including the exception type and never
        prevents subsequent venues from executing in the same tick.
        """
        async with self._tick_lock:
            updaters = (
                self._venue_updaters
                if self._venue_updaters is not None
                else _default_venue_updaters()
            )
            for venue_name, updater in updaters:
                try:
                    await updater()
                except Exception as exc:
                    logger.warning(
                        "Market snapshot refresh failed for venue={} error_type={}: {}",
                        venue_name,
                        type(exc).__name__,
                        exc,
                    )
            self._tick_count += 1

    async def start(self) -> None:
        """Start the periodic public market snapshot refresh loop.

        Runs the first refresh tick immediately and sleeps the remaining
        configured interval before each subsequent tick. Exits cleanly on
        ``stop()`` or propagates ``asyncio.CancelledError`` on cancellation.
        """
        if self._running:
            return
        set_log_context("snap:refresh")
        self._running = True
        try:
            while self._running:
                started_at = monotonic()
                await self.run_once()
                if not self._running:
                    break
                await self._sleep_for_remaining(started_at)
        except asyncio.CancelledError:
            logger.info("Market snapshot refresher cancelled")
            raise
        finally:
            self._running = False

    async def _sleep_for_remaining(self, started_at: float) -> None:
        """Sleep only the remaining interval time after a tick finishes.

        Args:
            started_at: Monotonic clock timestamp captured before the tick ran.
        """
        elapsed = monotonic() - started_at
        remaining = max(0.0, self._interval_seconds - elapsed)
        sleep_task = asyncio.create_task(asyncio.sleep(remaining))
        self._sleep_task = sleep_task
        try:
            await sleep_task
        except asyncio.CancelledError:
            if self._running:
                raise
        finally:
            self._sleep_task = None

    async def stop(self) -> None:
        """Stop the periodic refresh loop and cancel any pending interval sleep."""
        logger.info("Stopping market snapshot refresher")
        self._running = False
        sleep_task = self._sleep_task
        if sleep_task is not None and not sleep_task.done():
            sleep_task.cancel()
