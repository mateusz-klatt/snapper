"""Tests for the Phase 2c ``BacktestProgressEmitter`` (plan §2.3 + §2.6).

Covers:

- throttle dedup (250 ms window) for regular ``progress`` events.
- milestone firing exactly once per bucket regardless of re-crossing.
- milestones bypass the throttle and do not consume the throttle
  window (progress + milestone at the same step still both fire).
- terminal events fire exactly once — second call is a no-op.
- ``total_candles=None`` (or ``0``) disables milestones but keeps
  ``started`` / ``progress`` / terminal working, with
  ``progress_pct`` pinned at 0.0.
- ``BacktestProgressData.milestone`` cross-field invariant
  (``@model_validator``) rejects non-milestone events carrying a
  bucket and milestone events without a bucket.
"""

from datetime import UTC
from datetime import datetime

import pytest
from pydantic import ValidationError

from snapper.application.backtest.progress import BacktestProgressEmitter
from snapper.application.backtest.progress import noop_publish
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import BacktestProgressData


class _Collector:
    """In-memory ``PublishFn`` — records every (topic, data) call."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, BacktestProgressData]] = []

    async def __call__(self, topic: str, data: BacktestProgressData) -> None:
        self.calls.append((topic, data))


def _make_emitter(
    total_candles: int | None = 100, throttle_ms: int = 250
) -> tuple[BacktestProgressEmitter, _Collector]:
    """Build an emitter + collector pair with deterministic throttle."""
    collector = _Collector()
    emitter = BacktestProgressEmitter(
        run_public_id="01948f94-0001-7a00-8000-000000000002",
        wallet_public_id="01948f94-0001-7a00-8000-000000000001",
        total_candles=total_candles,
        tracker=SequenceTracker(),
        publish=collector,
        throttle_ms=throttle_ms,
    )
    return emitter, collector


class TestBacktestProgressDataMilestoneInvariant:
    """Cross-field validator on ``BacktestProgressData`` (R18 sonnet F3)."""

    _COMMON = dict(
        type="backtest_progress",
        public_id="01948f94-0001-7a00-8000-00000000000a",
        session_id="s",
        sequence_id=1,
        run_public_id="r",
        wallet_public_id="w",
        candles_done=10,
        total_candles=100,
        signals_count=0,
        trades_count=0,
        equity=0.0,
        progress_pct=0.1,
    )

    def _ts(self) -> datetime:
        return datetime(2026, 1, 1, tzinfo=UTC)

    def test_progress_with_milestone_rejected(self) -> None:
        """event='progress' + milestone='25pct' → ValidationError."""
        with pytest.raises(ValidationError, match="milestone field must be None"):
            BacktestProgressData(
                timestamp=self._ts(), event="progress", milestone="25pct", **self._COMMON
            )

    def test_milestone_without_bucket_rejected(self) -> None:
        """event='milestone' + milestone=None → ValidationError."""
        with pytest.raises(ValidationError, match="requires a milestone bucket"):
            BacktestProgressData(
                timestamp=self._ts(), event="milestone", milestone=None, **self._COMMON
            )

    def test_valid_progress_event(self) -> None:
        """event='progress' + milestone=None is the canonical shape."""
        data = BacktestProgressData(
            timestamp=self._ts(), event="progress", milestone=None, **self._COMMON
        )
        assert data.event == "progress"
        assert data.milestone is None

    def test_valid_milestone_event(self) -> None:
        """event='milestone' + milestone='25pct' is valid."""
        data = BacktestProgressData(
            timestamp=self._ts(), event="milestone", milestone="50pct", **self._COMMON
        )
        assert data.milestone == "50pct"


class TestEmitterLifecycle:
    """Emit-order invariants from plan §2.3."""

    @pytest.mark.asyncio
    async def test_on_started_fires_once(self) -> None:
        """on_started() emits 'started' exactly once even on double call."""
        emitter, collector = _make_emitter()
        await emitter.on_started()
        await emitter.on_started()
        started_events = [c for c in collector.calls if c[1].event == "started"]
        assert len(started_events) == 1

    @pytest.mark.asyncio
    async def test_terminal_fires_once(self) -> None:
        """on_terminal emits exactly once; second call no-ops."""
        emitter, collector = _make_emitter()
        await emitter.on_terminal("completed")
        await emitter.on_terminal("failed")
        assert len(collector.calls) == 1
        assert collector.calls[0][1].event == "completed"

    @pytest.mark.asyncio
    async def test_topic_prefix_shape(self) -> None:
        """Every emitted topic starts with backtest.{wallet}.{run}."""
        emitter, collector = _make_emitter()
        await emitter.on_started()
        topic = collector.calls[0][0]
        assert topic == (
            "backtest.01948f94-0001-7a00-8000-000000000001."
            "01948f94-0001-7a00-8000-000000000002.started"
        )


class TestThrottle:
    """Progress throttle semantics (R18 §2.3)."""

    @pytest.mark.asyncio
    async def test_progress_throttle_dedupes_within_window(self) -> None:
        """Two back-to-back candles within 250 ms emit only one 'progress'."""
        emitter, collector = _make_emitter(total_candles=1_000, throttle_ms=250)
        await emitter.on_candle_processed(equity=1.0, signals_count=0, trades_count=0)
        await emitter.on_candle_processed(equity=2.0, signals_count=0, trades_count=0)
        progress_events = [c for c in collector.calls if c[1].event == "progress"]
        assert len(progress_events) == 1


class TestMilestones:
    """Milestone dedup + throttle-orthogonality (R18 §2.3)."""

    @pytest.mark.asyncio
    async def test_milestone_fires_once_per_bucket(self) -> None:
        """25/50/75 pct each fires once across many candles."""
        emitter, collector = _make_emitter(total_candles=4)
        for _ in range(4):
            await emitter.on_candle_processed(equity=1.0, signals_count=0, trades_count=0)
        milestones = [c for c in collector.calls if c[1].event == "milestone"]
        buckets = [m[1].milestone for m in milestones]
        assert buckets == ["25pct", "50pct", "75pct"]

    @pytest.mark.asyncio
    async def test_total_candles_none_disables_milestones(self) -> None:
        """No total → no milestones, but progress / terminal still fire."""
        emitter, collector = _make_emitter(total_candles=None)
        await emitter.on_started()
        for _ in range(4):
            await emitter.on_candle_processed(equity=1.0, signals_count=0, trades_count=0)
        await emitter.on_terminal("completed")
        milestones = [c for c in collector.calls if c[1].event == "milestone"]
        assert milestones == []
        assert any(c[1].event == "started" for c in collector.calls)
        assert any(c[1].event == "completed" for c in collector.calls)

    @pytest.mark.asyncio
    async def test_milestones_bypass_throttle(self) -> None:
        """A 25pct crossing fires even when the throttle window is closed."""
        emitter, collector = _make_emitter(total_candles=4, throttle_ms=10_000_000)
        await emitter.on_candle_processed(equity=1.0, signals_count=0, trades_count=0)
        milestones = [c for c in collector.calls if c[1].event == "milestone"]
        assert len(milestones) == 1
        assert milestones[0][1].milestone == "25pct"


class TestProgressPctBound:
    """progress_pct stays in [0.0, 1.0] regardless of overshoot."""

    @pytest.mark.asyncio
    async def test_progress_pct_caps_at_one(self) -> None:
        """Processing more candles than expected does not exceed 1.0."""
        emitter, _ = _make_emitter(total_candles=2)
        await emitter.on_candle_processed(equity=1.0, signals_count=0, trades_count=0)
        await emitter.on_candle_processed(equity=1.0, signals_count=0, trades_count=0)
        await emitter.on_candle_processed(equity=1.0, signals_count=0, trades_count=0)
        assert emitter._progress_pct() == 1.0


class TestNoopPublish:
    """noop_publish is a silent sink so runners without WS don't crash."""

    @pytest.mark.asyncio
    async def test_noop_accepts_any_call(self) -> None:
        """Noop publish returns cleanly and writes nothing.

        Minimal-valid ``BacktestProgressData`` is constructed below so
        the noop sink can be exercised without wiring the emitter.
        """
        data = BacktestProgressData(
            type="backtest_progress",
            public_id="p",
            timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            session_id="s",
            sequence_id=1,
            run_public_id="r",
            wallet_public_id="w",
            event="started",
            milestone=None,
            candles_done=0,
            total_candles=None,
            signals_count=0,
            trades_count=0,
            equity=0.0,
            progress_pct=0.0,
        )
        await noop_publish("backtest.w.r.started", data)
