"""BacktestReplayStrategy factory — dynamic subclass with 4 backtest overrides.

Used by the ZMQ replay engine (Phase 2b-core Step 7) to wrap any concrete
``BaseStrategy`` subclass in a stateless replay shim. The wrapped strategy
runs the same indicator + signal logic as in production but receives
candles from a per-run replay broker instead of the live bus, and never
publishes orders or heartbeats.

Four overrides relative to ``BaseStrategy``:

1. ``start()``: skips ``_subscribe_inputs`` (which sleeps and subscribes to
   system topics) and skips ``_heartbeat_task``. Calls a market-only
   ``_replay_subscribe`` and the standard ``_setup_publisher`` then creates
   ``_listen_task`` AFTER the subscriber is wired so the publisher's
   echo-ack handshake cannot race the listener.

2. ``_replay_subscribe()``: NEW method (not an override of any base
   method). Subscribes the SUB socket to ONLY the market topics from
   ``self.inputs`` — no system topic leakage, no ``await asyncio.sleep(0)``.

3. ``_setup_publisher()``: connects the strategy's PUB socket to the per-
   run replay broker's XSUB endpoint instead of the live bus, so any
   signals the strategy emits stay isolated from production traffic.

4. ``_listen_loop()``: receives multipart frames, hard-skips non-market
   topics (defence in depth), recognizes the warmup sentinel and ACKs it
   into ``state.acked_topics`` (firing ``state.subscriber_ready`` when
   every expected topic has been seen), buffers real candles by
   ``open_at``, flushes per-time-batch through
   :func:`snapper.application.backtest.batch_processor.process_time_batch`,
   bumps ``drain.on_processed`` inside the market guard, and on exception
   sets ``self._running = False`` before re-raising so the base class's
   stop-side invariant survives.

The factory captures ``state``, ``drain``, ``local_xsub``, ``local_xpub``
and ``live_xsub`` as closure variables on the class definition. Python
captures these by reference at definition time and resolves them at call
time — safe because the factory is single-shot (returns one class, caller
constructs one instance).
"""

import asyncio
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime

import zmq
import zmq.asyncio
from loguru import logger

from snapper.application.backtest.batch_processor import CandleEvent
from snapper.application.backtest.batch_processor import process_time_batch
from snapper.application.backtest.cancel import CancelProbe
from snapper.application.backtest.config import BacktestConfig
from snapper.application.backtest.drain import DrainCoordinator
from snapper.application.backtest.progress import BacktestProgressEmitter
from snapper.application.backtest.result_collector import ResultCollector
from snapper.application.portfolio.models import PortfolioTracker
from snapper.data.repository_types import CandleRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.publishers.replay_publisher import WARMUP_PUBLIC_ID
from snapper.messaging.schemas.data import CandleData
from snapper.strategies.base import BaseStrategy
from snapper.strategies.models import StrategyConfig


@dataclass
class BacktestReplayState:
    """Per-run state shared between engine, strategy, and publisher.

    All references are mutated in place so the engine can read the final
    portfolio and equity points after ``await strategy._listen_task``.
    The ``subscriber_ready`` event and the ``expected_topics`` /
    ``acked_topics`` pair are the per-topic echo-ack handshake's shared
    memory: the strategy sets ``subscriber_ready`` only when
    ``acked_topics >= expected_topics``.
    """

    run_public_id: str
    config: BacktestConfig
    snapshot_as_of: datetime
    pending_batch: list[CandleEvent]
    portfolio: PortfolioTracker
    latest_closes: dict[str, float]
    collector: ResultCollector
    tracker: SequenceTracker
    expected_topics: frozenset[str]
    acked_topics: set[str] = field(default_factory=set)
    subscriber_ready: asyncio.Event = field(default_factory=asyncio.Event)
    cancel_probe: CancelProbe | None = None
    emitter: BacktestProgressEmitter | None = None


def _candle_data_to_event(data: CandleData) -> CandleEvent:
    """Inverse of ``candle_row_to_data`` for replay-side processing."""
    row: CandleRow = {
        "open_at": data.open_at,
        "timeframe": data.timeframe,
        "open": data.open,
        "high": data.high,
        "low": data.low,
        "close": data.close,
        "volume": data.volume,
        "vwap": data.vwap,
        "trades": data.trades,
        "public_id": data.public_id,
        "timestamp": data.timestamp,
        "session_id": data.session_id,
        "sequence_id": data.sequence_id,
    }
    return CandleEvent(
        open_at=data.open_at,
        exchange=str(data.exchange),
        instrument=data.instrument,
        row=row,
    )


def make_backtest_replay_strategy(
    inner_class: type[BaseStrategy],
    inner_config: StrategyConfig,
    *,
    state: BacktestReplayState,
    drain: DrainCoordinator,
    local_xsub: str,
    local_xpub: str,
) -> BaseStrategy:
    """Build a single-use replay-wrapped instance of ``inner_class``.

    Args:
        inner_class: Concrete ``BaseStrategy`` subclass to wrap.
        inner_config: Fully-built ``StrategyConfig``. The ``inputs`` list
            must contain the market topics the engine declared in
            ``state.expected_topics`` so ``_replay_subscribe`` subscribes
            to exactly those topics.
        state: Per-run shared state.
        drain: Shared drain coordinator.
        local_xsub: tcp endpoint of the per-run broker's XSUB socket
            (where the strategy's PUB socket connects to publish signals
            into the local broker, never the live bus).
        local_xpub: tcp endpoint of the per-run broker's XPUB socket
            (where the strategy's SUB socket connects to receive replay
            candles).

    Returns:
        A ready-to-``start`` strategy instance.
    """

    async def _start_override(self: BaseStrategy) -> None:
        """Stateless replay start — no heartbeat, no system subs, no sleeps."""
        self._running = True
        if not self.zmq_context:
            self.zmq_context = zmq.asyncio.Context()
        _replay_subscribe(self)
        await _setup_publisher_override(self)
        self._listen_task = asyncio.create_task(_listen_loop_override(self))

    def _replay_subscribe(self: BaseStrategy) -> None:
        """Subscribe SUB only to market topics. No sleep, no system subs."""
        assert self.zmq_context is not None
        raw_sub = self.zmq_context.socket(zmq.SUB)
        raw_sub.connect(local_xpub)
        self.subscriber = ValidatedSubscriber(raw_sub)
        for topic in self.inputs:
            if not topic.startswith("market."):
                continue
            self.subscriber.subscribe(topic)

    async def _setup_publisher_override(self: BaseStrategy) -> None:
        """Connect publisher to the local replay broker, never the live bus."""
        assert self.zmq_context is not None
        raw_pub = self.zmq_context.socket(zmq.PUB)
        raw_pub.connect(local_xsub)
        self.publisher = ValidatedPublisher(raw_pub)
        await asyncio.sleep(0)

    async def _process_warmup_or_buffer(
        self: BaseStrategy, topic_str: str, payload_bytes: bytes
    ) -> None:
        """Single message handling: warmup ACK or real candle batching."""
        data = CandleData.model_validate_json(payload_bytes.decode())
        if data.public_id == WARMUP_PUBLIC_ID:
            state.acked_topics.add(topic_str)
            if state.acked_topics >= state.expected_topics:
                state.subscriber_ready.set()
            return
        event = _candle_data_to_event(data)
        if state.pending_batch and state.pending_batch[0].open_at != event.open_at:
            await process_time_batch(
                batch=state.pending_batch,
                run_public_id=state.run_public_id,
                config=state.config,
                strategy=self,
                portfolio=state.portfolio,
                latest_closes=state.latest_closes,
                collector=state.collector,
                tracker=state.tracker,
                snapshot_as_of=state.snapshot_as_of,
                emitter=state.emitter,
            )
            state.pending_batch = []
        state.pending_batch.append(event)
        drain.on_processed()
        if state.cancel_probe is not None:
            await state.cancel_probe.check()

    async def _listen_loop_override(self: BaseStrategy) -> None:
        """Echo-ack detection + per-time-batch flush + cleanup re-raise."""
        assert self.subscriber is not None
        try:
            while self._running:
                topic_str, payload_bytes = await self.subscriber.recv_multipart()
                if not topic_str.startswith("market."):
                    continue
                await _process_warmup_or_buffer(self, topic_str, payload_bytes)
        except asyncio.CancelledError:
            raise
        except Exception:
            self._running = False
            logger.exception("backtest replay strategy listen loop failed")
            raise

    overrides: dict[str, object] = {
        "start": _start_override,
        "_replay_subscribe": _replay_subscribe,
        "_setup_publisher": _setup_publisher_override,
        "_listen_loop": _listen_loop_override,
        "__doc__": "Replay-shimmed strategy with 4 backtest overrides.",
    }
    mixin_cls = type("_BacktestReplayMixin", (inner_class,), overrides)
    instance = mixin_cls(inner_config)
    if not isinstance(instance, BaseStrategy):
        raise TypeError(f"factory produced non-BaseStrategy instance: {type(instance).__name__}")
    return instance
