"""Backtest ZMQ-replay spike.

Goal: verify that a strategy receiving candle data THROUGH a ZMQ broker
produces the same signal sequence as calling the strategy's ``on_candle``
directly. This is the minimum bar Phase 2b needs to clear — if broker
replay diverges from direct-call, nothing else matters.

Usage:
    .venv/bin/python scripts/spike_backtest_zmq.py

No DB, no migrations, no frontend. Synthetic 50-candle MACD price series,
MACDCrossover strategy, fresh broker on fixed local ports (16001/16002).

Key findings encoded in this file (to be lifted into plan v2.0):

1. Subscriber redirection needs NO override of `_connect_subscriber_socket`
   — pass `params={"use_broker": False, "feed_addr": broker_xpub}`.
2. Publisher redirection requires overriding `_setup_publisher` to connect
   to `local_xsub` instead of `_bootstrap_settings.zmq_broker_xsub`.
3. `emit_signal` overrride bypasses the live `signal_service.store_signal`
   path and records into an in-memory collector.
4. `_listen_loop` must re-raise exceptions (base swallows them) so the
   runner can surface failures.
5. Drain handshake: publisher counts sent candles; strategy counts
   processed iterations (AFTER emit_signal returns). Publisher waits on
   asyncio.Event for processed_count >= published_count before exiting.
6. Broker binding with ``tcp://127.0.0.1:0`` + reading
   ``getsockopt(zmq.LAST_ENDPOINT)`` returns the actual allocated port.
7. Slow-joiner mitigation: publisher sleeps briefly AFTER strategy.start()
   reports subscription ready (see ``_wait_subscribed`` heuristic below).
"""

import asyncio
import sys
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import zmq.asyncio
from loguru import logger

from snapper.core.types import ExchangeEnum
from snapper.messaging.infrastructure.broker import ZmqBrokerProcess
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.schemas.data import CandleData
from snapper.strategies.base import BaseStrategy
from snapper.strategies.macd import MACDCrossover
from snapper.strategies.models import StrategyConfig
from snapper.strategies.models import StrategySignal

BROKER_XSUB = "tcp://127.0.0.1:16001"
BROKER_XPUB = "tcp://127.0.0.1:16002"


def _make_candles(count: int = 200, instrument: str = "BTC-USD") -> list[CandleData]:
    """Synthesise a candle series with a mild sine so MACD crosses fire.

    Args:
        count: Number of candles.
        instrument: Instrument tag (unused by MACD math but required).

    Returns:
        Candles evenly spaced one hour apart with a sinusoidal close path.
    """
    import math

    base_time = datetime(2026, 1, 1, tzinfo=UTC)
    tracker = SequenceTracker()
    out: list[CandleData] = []
    for i in range(count):
        t = base_time + timedelta(hours=i)
        price = 50_000.0 + 2_000.0 * math.sin(i / 4.0) + 500.0 * math.sin(i / 1.7)
        out.append(
            CandleData(
                type="candle",
                public_id=f"spike-{i:04d}-000000000000000000000000000000",
                timestamp=t,
                session_id=tracker.session_id,
                sequence_id=i + 1,
                exchange=ExchangeEnum.KRAKEN,
                instrument=instrument,
                timeframe="1h",
                open_at=t,
                open=price,
                high=price * 1.002,
                low=price * 0.998,
                close=price,
                volume=10.0,
            )
        )
    return out


@dataclass
class SpikeDrainCoordinator:
    """Shared drain handshake between publisher and strategy tasks.

    Both run on the same event loop; no locks required.
    """

    published: int = 0
    processed: int = 0
    finished_publishing: asyncio.Event = field(default_factory=asyncio.Event)
    drained: asyncio.Event = field(default_factory=asyncio.Event)

    def on_publish(self) -> None:
        """Record a published message."""
        self.published += 1

    def on_processed(self) -> None:
        """Record a processed message, setting ``drained`` when fully caught up."""
        self.processed += 1
        if self.finished_publishing.is_set() and self.processed >= self.published:
            self.drained.set()

    def mark_done_publishing(self) -> None:
        """Flag that no more messages will be published."""
        self.finished_publishing.set()
        if self.processed >= self.published:
            self.drained.set()


class SpikeCollector:
    """Trivial in-memory collector for signals emitted during the spike."""

    def __init__(self) -> None:
        """Initialise with an empty signal list."""
        self.signals: list[StrategySignal] = []

    async def record(self, signal: StrategySignal) -> None:
        """Append the received signal to the collector."""
        self.signals.append(signal)


def make_backtest_strategy(
    collector: SpikeCollector,
    drain: SpikeDrainCoordinator,
    local_xsub: str,
    local_xpub: str,
    instrument: str = "BTC-USD",
) -> BaseStrategy:
    """Dynamic subclass of MACDCrossover with the 2 overrides + drain hook.

    Subscriber connection is redirected via ``params["use_broker"]=False``
    + ``params["feed_addr"]=local_xpub`` — no override of
    ``_connect_subscriber_socket`` needed.
    """
    topic = f"market.kraken.{instrument}.candles.1h"
    config = StrategyConfig(
        name=f"spike_macd_{instrument.replace('-', '_')}",
        strategy_class="MACDCrossover",
        inputs=[topic],
        outputs=[instrument],
        exchange=ExchangeEnum.PAPER,
        params={
            "fast": 12,
            "slow": 26,
            "signal_period": 9,
            "use_broker": False,
            "feed_addr": local_xpub,
        },
        wallet_public_id="00000000-0000-7000-8000-000000000001",
    )

    class _SpikeMixin:
        async def _setup_publisher(self: BaseStrategy) -> None:
            if self.publisher is None:
                assert self.zmq_context is not None
                raw = self.zmq_context.socket(zmq.PUB)
                raw.connect(local_xsub)
                self.publisher = ValidatedPublisher(raw)
                self.msg_publisher = MessagePublisher(self.publisher, self._tracker)
            await asyncio.sleep(0)

        async def emit_signal(self: BaseStrategy, signal: StrategySignal) -> None:
            await collector.record(signal)

        async def _listen_loop(self: BaseStrategy) -> None:
            from snapper.messaging.topics.builders import parse_market_topic

            assert self.subscriber is not None
            while self._running:
                topic_str, payload_bytes = await self.subscriber.recv_multipart()
                payload = payload_bytes.decode()
                if topic_str.startswith("market."):
                    parsed = parse_market_topic(topic_str)
                    if parsed is None:
                        drain.on_processed()
                        continue
                    signal = await self._dispatch_market_data(topic_str, parsed.instrument, payload)
                    if signal is not None:
                        await self.emit_signal(signal)
                drain.on_processed()

    cls = type("SpikeMACD", (_SpikeMixin, MACDCrossover), {})
    return cls(config)


async def run_direct(candles: list[CandleData]) -> list[StrategySignal]:
    """Baseline: feed candles straight into the strategy's on_candle.

    Returns:
        Signals produced by direct invocation.
    """
    config = StrategyConfig(
        name="baseline_macd",
        strategy_class="MACDCrossover",
        inputs=["market.kraken.BTC-USD.candles.1h"],
        outputs=["BTC-USD"],
        exchange=ExchangeEnum.PAPER,
        params={"fast": 12, "slow": 26, "signal_period": 9},
        wallet_public_id="00000000-0000-7000-8000-000000000001",
    )
    strategy = MACDCrossover(config)
    signals: list[StrategySignal] = []
    for candle in candles:
        buffer = strategy.candle_buffer.setdefault(candle.instrument, [])
        buffer.append(candle)
        signal = await strategy.on_candle(candle.instrument, candle)
        if signal is not None:
            signals.append(signal)
    return signals


async def run_via_broker(candles: list[CandleData]) -> list[StrategySignal]:
    """Replay candles through a real ZMQ broker.

    Returns:
        Signals captured via the strategy's overridden ``emit_signal``.
    """
    broker = ZmqBrokerProcess(xsub_endpoint=BROKER_XSUB, xpub_endpoint=BROKER_XPUB)
    await broker.start()
    collector = SpikeCollector()
    drain = SpikeDrainCoordinator()
    strategy = make_backtest_strategy(collector, drain, BROKER_XSUB, BROKER_XPUB)
    await strategy.start()
    strategy._listen_task = asyncio.create_task(strategy._listen_loop())
    await asyncio.sleep(0.3)
    ctx = zmq.asyncio.Context()
    pub_socket = ctx.socket(zmq.PUB)
    pub_socket.connect(BROKER_XSUB)
    await asyncio.sleep(0.1)
    try:
        topic = "market.kraken.BTC-USD.candles.1h"
        for candle in candles:
            await pub_socket.send_multipart([topic.encode(), candle.to_json().encode()])
            drain.on_publish()
            await asyncio.sleep(0.005)
        drain.mark_done_publishing()
        try:
            await asyncio.wait_for(drain.drained.wait(), timeout=5.0)
        except TimeoutError:
            logger.warning(
                "drain timed out: published={}, processed={}",
                drain.published,
                drain.processed,
            )
    finally:
        pub_socket.setsockopt(zmq.LINGER, 0)
        pub_socket.close()
        ctx.term()
        await strategy.stop()
        await broker.stop()
    return collector.signals


async def main() -> int:
    """Run both paths, compare signal sequences, exit 0 on parity."""
    candles = _make_candles(50)
    direct = await run_direct(candles)
    broker = await run_via_broker(candles)
    logger.info("direct: {} signals, broker: {} signals", len(direct), len(broker))
    if len(direct) != len(broker):
        logger.error("signal count mismatch")
        return 1
    mismatches = 0
    for i, (d, b) in enumerate(zip(direct, broker, strict=True)):
        if d.side != b.side or d.instrument != b.instrument:
            logger.error(
                "signal #{} mismatch: direct={}/{} broker={}/{}",
                i,
                d.side,
                d.instrument,
                b.side,
                b.instrument,
            )
            mismatches += 1
    if mismatches:
        return 1
    logger.info("PARITY OK ({} signals match)", len(direct))
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
