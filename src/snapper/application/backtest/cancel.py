"""Cooperative cancel probe shared between Direct-DB and ZMQ replay engines.

Callers invoke ``await probe.check()`` at safe cancellation points (between
time-batches in Direct-DB, per received candle in ZMQ replay). The probe
throttles DB reads via ``cancel_poll_ms`` and bounds each DB call with
``probe_timeout_s`` — a slow database does not stall the engine.

Design note: under SQLite + aiosqlite lock contention, ``asyncio.wait_for``
does not hard-bound the underlying DB wait. A stuck probe therefore delays
cancel detection by up to the next poll tick; it does NOT lose the cancel
request — the next tick re-probes against a fresh ``cancel_requested``
status read.
"""

import asyncio
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from time import monotonic

from loguru import logger

from snapper.core.types import BacktestRunStatusEnum
from snapper.data.backtest_repository import BacktestRepository


@dataclass
class CancelProbe:
    """Throttled, bounded poll of ``backtest_runs.status`` for cancel requests."""

    bt_repo: BacktestRepository
    run_public_id: str
    cancel_poll_ms: int = 500
    probe_timeout_s: float = 1.0
    _last_check_ms: float = field(default=0.0, init=False)

    async def check(self) -> None:
        """Probe the DB; raise CancelledError when cancel_requested observed.

        Called at safe cancellation points by both engines. Throttled by
        ``cancel_poll_ms`` so a tight engine loop does not hammer the DB.
        Each DB call is bounded by ``probe_timeout_s`` — a stalled probe
        logs a warning and returns; the next tick re-probes.

        Raises:
            asyncio.CancelledError: When the run's status is
                ``cancel_requested``.
        """
        now_ms = monotonic() * 1000.0
        if now_ms - self._last_check_ms < self.cancel_poll_ms:
            return
        self._last_check_ms = now_ms
        try:
            run = await asyncio.wait_for(
                self.bt_repo.get_run(self.run_public_id, as_of=datetime.now(UTC)),
                timeout=self.probe_timeout_s,
            )
        except TimeoutError:
            logger.warning(
                "Backtest {} cancel probe timed out after {}s — will retry next tick",
                self.run_public_id[:8],
                self.probe_timeout_s,
            )
            return
        if run is not None and run["status"] == BacktestRunStatusEnum.CANCEL_REQUESTED:
            raise asyncio.CancelledError
