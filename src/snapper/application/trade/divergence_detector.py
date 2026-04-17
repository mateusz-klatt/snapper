"""Shadow-write divergence observability for the dual-write -> durable cutover.

In-memory counters + periodic-log sink. Enabled by the
``enable_divergence_detector`` database setting (default False). The
seven counters give operators the data they need to decide whether the
durable-command cutover is safe to lock in:

- ``commands_created_total`` — how many ``TradeCommand`` rows the
  engines have written.
- ``commands_published_dual_write_total`` — direct-ZMQ publishes in
  dual-write mode.
- ``commands_published_durable_notified_total`` — engine-side notifies
  to the outbox.
- ``commands_published_durable_dispatched_total`` — dispatcher-side
  actual publishes (gap vs notified is the silent-ZMQ-loss signal).
- ``venue_events_observed_total`` — fills seen by the trader.
- ``reconciliation_ok_total`` — clean reconciliation cycles.
- ``reconciliation_failure_total`` — circuit-breaker-tripping
  failures.

The detector is flag-gated opt-in so the hot path stays zero-cost when
disabled (all four hook sites guard with ``if self._divergence_detector
is not None``). Each observe call is O(1) — integer increments on
per-label dicts. The periodic snapshot loop emits at INFO every
``snapshot_interval_s`` seconds (default 60 s) and is expected to run
as an asyncio task spawned by ``TraderCoordinator.start()``.

Plan: ``proprietary/plans/plan_shadow_write_divergence.md`` v1.3.
"""

import asyncio
from typing import Literal

from loguru import logger

PublishPath = Literal["dual_write", "durable_notified", "durable_dispatched"]
ReconVerdict = Literal["ok", "failure"]


class DivergenceDetector:
    """In-memory aggregator for dual-write vs durable cutover metrics.

    All increment methods are O(1). Nothing allocates per-call beyond
    an int-dict lookup. Safe to hold the single instance across the
    trade-runtime lifetime.
    """

    def __init__(self, snapshot_interval_s: float = 60.0) -> None:
        """Build an empty detector.

        Args:
            snapshot_interval_s: Period (seconds) between INFO logs from
                ``periodic_snapshot_loop``.
        """
        self._snapshot_interval_s = snapshot_interval_s
        self._commands_created: int = 0
        self._commands_published_dual_write: int = 0
        self._commands_published_durable_notified: int = 0
        self._commands_published_durable_dispatched: int = 0
        self._venue_events_observed: int = 0
        self._reconciliation_ok: int = 0
        self._reconciliation_failure: int = 0

    def observe_command_created(self, exchange: str, shard_key: str) -> None:
        """Count a ``TradeCommand`` row insert.

        Args:
            exchange: Exchange name (for future per-exchange breakouts).
            shard_key: Engine shard key (for future per-shard breakouts).
        """
        del exchange, shard_key
        self._commands_created += 1

    def observe_command_published(self, path: PublishPath, exchange: str, shard_key: str) -> None:
        """Count a publish event on one of the three dispatch paths.

        Args:
            path: ``"dual_write"`` (engine ZMQ send), ``"durable_notified"``
                (engine outbox.notify), or ``"durable_dispatched"``
                (dispatcher actual publish).
            exchange: Exchange name.
            shard_key: Engine shard key.
        """
        del exchange, shard_key
        if path == "dual_write":
            self._commands_published_dual_write += 1
        elif path == "durable_notified":
            self._commands_published_durable_notified += 1
        else:
            self._commands_published_durable_dispatched += 1

    def observe_venue_event(self, shard_key: str, venue_event_id: int) -> None:
        """Count a fill observed by the trader's fill-sync path.

        Args:
            shard_key: Engine shard key.
            venue_event_id: Watermark id of the event (reserved for
                future gap-detection breakouts).
        """
        del shard_key, venue_event_id
        self._venue_events_observed += 1

    def observe_reconciliation_verdict(self, shard_key: str, verdict: ReconVerdict) -> None:
        """Count a reconciliation cycle outcome.

        Args:
            shard_key: Shard key carrying the verdict.
            verdict: ``"ok"`` for clean cycles, ``"failure"`` for
                circuit-breaker-tripping failures.
        """
        del shard_key
        if verdict == "ok":
            self._reconciliation_ok += 1
        else:
            self._reconciliation_failure += 1

    def snapshot(self) -> dict[str, int]:
        """Return the current counter totals as a flat dict.

        Returns:
            Dict keyed by the seven canonical counter names documented
            in the module docstring.
        """
        return {
            "commands_created_total": self._commands_created,
            "commands_published_dual_write_total": self._commands_published_dual_write,
            "commands_published_durable_notified_total": self._commands_published_durable_notified,
            "commands_published_durable_dispatched_total": (
                self._commands_published_durable_dispatched
            ),
            "venue_events_observed_total": self._venue_events_observed,
            "reconciliation_ok_total": self._reconciliation_ok,
            "reconciliation_failure_total": self._reconciliation_failure,
        }

    async def periodic_snapshot_loop(self) -> None:
        """Log snapshot counters every ``snapshot_interval_s`` seconds until cancelled.

        Emits one INFO log per interval. Safe to run as an asyncio task
        alongside the trader and outbox dispatcher — the sleep uses the
        event loop so cancellation is prompt.
        """
        try:
            while True:
                await asyncio.sleep(self._snapshot_interval_s)
                snap = self.snapshot()
                logger.info("DivergenceDetector snapshot: {}", snap)
        except asyncio.CancelledError:
            logger.info("DivergenceDetector snapshot loop cancelled")
            raise
