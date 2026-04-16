"""Backtest progress emitter — state-tracked throttled WS publisher.

Emits ``BacktestProgressData`` events to the 4-segment ZMQ topic family
``backtest.{wallet_public_id}.{run_public_id}.{event}``. The existing
ZMQ→WS bridge forwards those envelopes to every subscribed client
scoped by the wallet segment (see plan §2.1.2 RBAC matrix).

Emit ordering invariants (plan §2.3):

- ``on_started`` — fires exactly once at ``event="started"`` before the
  first candle. Bypasses the throttle.
- ``on_candle_processed`` — each call bumps the internal
  ``candles_done`` counter and:
  1. Checks 25 / 50 / 75 pct milestone buckets. Each bucket fires at
     most once per run even if recrossed (``_milestones_seen`` dedup).
     Milestones bypass the throttle and do NOT consume the throttle
     window (orthogonal to ``progress`` events).
  2. Gate-checks the 250 ms throttle for a ``progress`` event.
- ``on_terminal`` — fires exactly once for one of
  ``{completed, failed, cancelled}`` after the engine exits. Bypasses
  the throttle.

When ``total_candles`` is ``None`` or ``<= 0`` (the runner's count
query failed or the run is empty), ``progress_pct`` stays ``0.0`` and
milestones are silently disabled. The ``started`` / ``progress`` /
terminal events still fire.

The emitter is transport-agnostic: it writes into a ``PublishFn``
callable injected by the owner. In production the runner wires this
to ``MessagePublisher.send``; tests inject an in-memory collector.
"""

import time
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from typing import Literal
from uuid import uuid7

from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import BacktestProgressData
from snapper.messaging.schemas.data import BacktestProgressEvent

_PROGRESS_STREAM = "backtest_progress"
_DEFAULT_THROTTLE_MS = 250
_MILESTONE_THRESHOLDS: dict[Literal["25pct", "50pct", "75pct"], float] = {
    "25pct": 0.25,
    "50pct": 0.50,
    "75pct": 0.75,
}


PublishFn = Callable[[str, BacktestProgressData], Awaitable[None]]


async def noop_publish(_topic: str, _data: BacktestProgressData) -> None:
    """No-op publish — used when the runner has no ZMQ publisher wired.

    Keeps the emitter lifecycle intact (counters still advance,
    milestones still dedup) so a runner without WS transport never
    crashes, just silently drops the payloads.

    Args:
        _topic: Unused topic name.
        _data: Unused progress payload.
    """


@dataclass
class BacktestProgressEmitter:
    """State-tracked progress emitter scoped to a single run.

    Attributes:
        run_public_id: UUID7 of the backtest run.
        wallet_public_id: UUID7 of the owning wallet.
        total_candles: Expected candle count; ``None`` disables
            milestones and pins ``progress_pct`` at 0.0.
        tracker: Shared ``SequenceTracker`` for session_id + monotonic
            sequence_id allocation.
        publish: Transport-agnostic async callable — ``(topic, data)``.
        throttle_ms: Minimum gap between two ``progress`` events
            (default 250 ms — matches TOPIC_REGISTRY throttle).
    """

    run_public_id: str
    wallet_public_id: str
    total_candles: int | None
    tracker: SequenceTracker
    publish: PublishFn = field(default=noop_publish)
    throttle_ms: int = _DEFAULT_THROTTLE_MS
    _last_emit_monotonic_ms: float = 0.0
    _milestones_seen: set[str] = field(default_factory=set)
    _candles_done: int = 0
    _signals_count: int = 0
    _trades_count: int = 0
    _last_equity: float = 0.0
    _started: bool = False
    _terminal_emitted: bool = False

    @property
    def topic_prefix(self) -> str:
        """ZMQ topic prefix shared by every event this emitter produces.

        Returns:
            Dotted prefix ``backtest.{wallet_public_id}.{run_public_id}``.
        """
        return f"backtest.{self.wallet_public_id}.{self.run_public_id}"

    def _progress_pct(self) -> float:
        """Compute progress fraction in [0.0, 1.0]; 0.0 when total unknown."""
        if self.total_candles is None or self.total_candles <= 0:
            return 0.0
        return min(1.0, self._candles_done / self.total_candles)

    def _build(
        self,
        event: BacktestProgressEvent,
        milestone: Literal["25pct", "50pct", "75pct"] | None,
    ) -> BacktestProgressData:
        """Construct a provenance-stamped ``BacktestProgressData`` event."""
        return BacktestProgressData(
            type="backtest_progress",
            public_id=str(uuid7()),
            timestamp=datetime.now(UTC),
            session_id=self.tracker.session_id,
            sequence_id=self.tracker.next_sequence(_PROGRESS_STREAM),
            run_public_id=self.run_public_id,
            wallet_public_id=self.wallet_public_id,
            event=event,
            milestone=milestone,
            candles_done=self._candles_done,
            total_candles=self.total_candles,
            signals_count=self._signals_count,
            trades_count=self._trades_count,
            equity=self._last_equity,
            progress_pct=self._progress_pct(),
        )

    async def _emit(
        self,
        event: BacktestProgressEvent,
        milestone: Literal["25pct", "50pct", "75pct"] | None = None,
    ) -> None:
        """Build + publish a single progress event on the event-specific topic."""
        data = self._build(event, milestone)
        topic = f"{self.topic_prefix}.{event}"
        await self.publish(topic, data)

    async def on_started(self) -> None:
        """Emit ``started`` once at the top of the run.

        No-op on a second call — the invariant is exactly-once per run.
        """
        if self._started:
            return
        self._started = True
        await self._emit("started")

    async def on_candle_processed(
        self, *, equity: float, signals_count: int, trades_count: int
    ) -> None:
        """Advance counters and emit milestones + throttled progress.

        Called once per candle batch processed by the engine. Milestones
        are checked FIRST and fire at most once per bucket (dedup via
        ``_milestones_seen``); they bypass the throttle window so a
        milestone crossing and a throttled progress sample at the same
        step cannot collide. After milestones, a regular ``progress``
        event is gated on the 250 ms throttle window.

        Args:
            equity: Current portfolio equity for this sample.
            signals_count: Cumulative signal count from the collector.
            trades_count: Cumulative trade count from the collector.
        """
        self._candles_done += 1
        self._signals_count = signals_count
        self._trades_count = trades_count
        self._last_equity = equity

        if self.total_candles and self.total_candles > 0:
            fraction = self._candles_done / self.total_candles
            for bucket, threshold in _MILESTONE_THRESHOLDS.items():
                if fraction >= threshold and bucket not in self._milestones_seen:
                    self._milestones_seen.add(bucket)
                    await self._emit("milestone", milestone=bucket)

        now_ms = time.monotonic() * 1000.0
        if now_ms - self._last_emit_monotonic_ms >= self.throttle_ms:
            self._last_emit_monotonic_ms = now_ms
            await self._emit("progress")

    async def on_terminal(self, event: Literal["completed", "failed", "cancelled"]) -> None:
        """Emit a terminal event exactly once.

        Safe to call on every runner path — a second call after a prior
        terminal emission is a no-op so the cancel + failure handlers
        can both call into it defensively.

        Args:
            event: Terminal event kind — one of
                ``completed``, ``failed``, ``cancelled``.
        """
        if self._terminal_emitted:
            return
        self._terminal_emitted = True
        await self._emit(event)
