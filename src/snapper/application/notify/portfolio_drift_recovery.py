"""Durable periodic recovery for lost portfolio-drift bus events.

The scanner reads only sentinel-current open drift episodes. Missing owner
pages are built by the exact Stage 1 helper and handed to the notify
sidecar's normal persistence and delivery path. It never changes portfolio,
reconciliation, order, position, or trading state.
"""

import asyncio
import contextlib
import math
import os
from collections.abc import Awaitable
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from typing import Final

from loguru import logger

from snapper.application.notify.portfolio_drift_paging import PORTFOLIO_DRIFT_OPEN_MISMATCH_COUNT
from snapper.application.notify.portfolio_drift_paging import build_open_portfolio_drift_alert_rows
from snapper.data.repository import Repository
from snapper.data.repository_types import AlertEventInsertRow
from snapper.data.repository_types import PortfolioDriftEpisodeRow

DEFAULT_INTERVAL_SECONDS: Final[float] = 60.0
_INTERVAL_ENV_VAR: Final[str] = "PORTFOLIO_DRIFT_RECOVERY_INTERVAL_SECONDS"
ENV_VARS: Final[frozenset[str]] = frozenset({_INTERVAL_ENV_VAR})

type AlertRowEmitter = Callable[[AlertEventInsertRow, datetime], Awaitable[None]]


def _resolve_interval(env_value: str | None) -> float:
    """Parse the recovery cadence with a safe default.

    Args:
        env_value: Raw interval environment value, or ``None`` when unset.

    Returns:
        Positive interval in seconds, or :data:`DEFAULT_INTERVAL_SECONDS`
        when unset, malformed, or non-positive.
    """
    if env_value is None:
        return DEFAULT_INTERVAL_SECONDS
    try:
        parsed = float(env_value)
    except ValueError:
        return DEFAULT_INTERVAL_SECONDS
    if not math.isfinite(parsed) or parsed <= 0:
        return DEFAULT_INTERVAL_SECONDS
    return parsed


class PortfolioDriftRecoveryScanner:
    """Recover missing open-episode pages from durable database evidence."""

    def __init__(
        self,
        *,
        repo: Repository,
        emit_alert_row: AlertRowEmitter,
        interval_seconds: float | None = None,
    ) -> None:
        """Wire the scanner, sink, and environment-configurable cadence.

        Args:
            repo: Repository used only for episode, ownership, and dedup reads.
            emit_alert_row: Existing notify-sidecar persistence and fanout sink.
            interval_seconds: Cadence override. ``None`` reads
                ``PORTFOLIO_DRIFT_RECOVERY_INTERVAL_SECONDS`` with a 60-second
                default.
        """
        raw_interval = (
            os.environ.get(_INTERVAL_ENV_VAR) if interval_seconds is None else str(interval_seconds)
        )
        self._repo = repo
        self._emit_alert_row = emit_alert_row
        self._interval_seconds = _resolve_interval(raw_interval)
        self._stopping = asyncio.Event()
        self._loop_task: asyncio.Task[None] | None = None

    @property
    def interval_seconds(self) -> float:
        """Return the configured scan cadence.

        Returns:
            Seconds between recovery passes.
        """
        return self._interval_seconds

    async def start(self) -> None:
        """Take one guarded eager scan and spawn the sleep-first loop."""
        self._stopping.clear()
        await self._guarded_run_once()
        if self._stopping.is_set():
            return
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
        """Sleep the configured interval and run guarded scans until stopped."""
        while not self._stopping.is_set():
            await asyncio.sleep(self._interval_seconds)
            if self._stopping.is_set():
                return
            await self._guarded_run_once()

    async def _guarded_run_once(self) -> None:
        """Run one pass while containing every pass-level exception."""
        try:
            await self.run_once()
        except Exception:
            logger.exception("portfolio_drift_recovery: scan failed; continuing on next tick")

    async def run_once(self, now: datetime | None = None) -> None:
        """Scan and page every current open episode missing its owner alert.

        Args:
            now: Entry-boundary timestamp override for deterministic tests;
                ``None`` uses the current UTC time.
        """
        reference_now = now if now is not None else datetime.now(UTC)
        episodes = await self._repo.list_open_portfolio_drift_episodes()
        for episode in episodes:
            if episode["status"] != "open":
                continue
            try:
                await self._recover_episode(episode, reference_now)
            except Exception:
                logger.exception(
                    "portfolio_drift_recovery: episode {episode} failed; continuing pass",
                    episode=episode["public_id"],
                )

    async def _recover_episode(
        self,
        episode: PortfolioDriftEpisodeRow,
        now: datetime,
    ) -> None:
        """Build and emit missing recipient rows for one open episode.

        Args:
            episode: Sentinel-current open episode projection.
            now: One entry-boundary timestamp shared across the scan pass.
        """
        rows = await build_open_portfolio_drift_alert_rows(
            repo=self._repo,
            wallet_public_id=episode["wallet_public_id"],
            exchange=episode["exchange"],
            mode=episode["mode"],
            episode_public_id=episode["public_id"],
            opened_at=episode["opened_at"],
            mismatch_count=PORTFOLIO_DRIFT_OPEN_MISMATCH_COUNT,
            now=now,
        )
        for row in rows:
            try:
                await self._emit_alert_row(row, now)
            except Exception:
                logger.exception(
                    "portfolio_drift_recovery: recipient {user} for episode {episode}"
                    " failed; continuing episode",
                    user=row["user_public_id"],
                    episode=episode["public_id"],
                )
