"""Backtest ZMQ-replay spike (v2 — extended).

Goal (v1, already verified): strategy via ZMQ broker emits same signal
sequence as direct on_candle call.

Goal (v2, this file): reproduce DirectDbEngine's FULL per-timestamp batch
semantics over ZMQ, covering the four areas Copilot R1 flagged as
unverified by v1:

1. Multi-instrument batch equity — ``latest_closes`` dict updated with
   all instruments at a timestamp BEFORE equity is recorded once per
   timestamp (mirror of ``direct_engine.py:_process_time_batch``).
2. Fill simulation + PortfolioTracker mutation — same ``simulate_market_fill``
   used by DirectDbEngine, mixin owns portfolio state.
3. ``start_date`` gating — candles before ``config.start_date`` feed
   indicators but do NOT contribute signals / trades / equity.
4. Correct listen-task lifecycle — ``strategy.start()`` already spawns
   ``_listen_task`` via ``_subscribe_inputs``; the spike must consume
   that task, not create a second one.

Parity target: ZMQ path and the in-file reference engine produce
identical signals, trades, and equity curves (field-for-field) on a
two-instrument (BTC + ETH) 200-candle fixture.
"""

import asyncio
import math
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any

import zmq.asyncio
from loguru import logger

from snapper.application.backtest.fill_model import BacktestFill
from snapper.application.backtest.fill_model import simulate_market_fill
from snapper.application.portfolio.models import PortfolioTracker
from snapper.core.types import ExchangeEnum
from snapper.messaging.infrastructure.broker import ZmqBrokerProcess
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import CandleData
from snapper.strategies.base import BaseStrategy
from snapper.strategies.macd import MACDCrossover
from snapper.strategies.models import StrategyConfig
from snapper.strategies.models import StrategySignal

BROKER_XSUB = "tcp://127.0.0.1:16001"
BROKER_XPUB = "tcp://127.0.0.1:16002"
START_DATE = datetime(2026, 1, 2, tzinfo=UTC)
INSTRUMENTS = ("BTC-USD", "ETH-USD")
WARMUP_AND_RUN_CANDLES = 200


@dataclass(frozen=True)
class SpikeCandle:
    """In-memory candle fixture used by both reference and ZMQ paths."""

    open_at: datetime
    exchange: str
    instrument: str
    close: float


@dataclass
class SpikeSignalRecord:
    """Artifact persisted on signal emission."""

    signal_time: datetime
    signal_type: str
    instrument: str
    price: float


@dataclass
class SpikeTradeRecord:
    """Artifact persisted on simulated fill."""

    executed_at: datetime
    instrument: str
    side: str
    quantity: float
    price: float
    fee: float
    pnl: float | None
    position_after: float


@dataclass
class SpikeEquityRecord:
    """Artifact persisted once per processed timestamp."""

    point_time: datetime
    equity: float
    cash: float
    position_value: float


@dataclass
class SpikeCollector:
    """Matches ResultCollector's shape — lists of record artifacts."""

    signals: list[SpikeSignalRecord] = field(default_factory=list)
    trades: list[SpikeTradeRecord] = field(default_factory=list)
    equity: list[SpikeEquityRecord] = field(default_factory=list)


@dataclass
class SpikeDrainCoordinator:
    """Publisher/consumer handshake on a single event loop; no locks."""

    published: int = 0
    processed: int = 0
    finished_publishing: asyncio.Event = field(default_factory=asyncio.Event)
    drained: asyncio.Event = field(default_factory=asyncio.Event)

    def on_publish(self) -> None:
        """Record a published message."""
        self.published += 1

    def on_processed(self) -> None:
        """Record a processed message; set drained when fully caught up."""
        self.processed += 1
        if self.finished_publishing.is_set() and self.processed >= self.published:
            self.drained.set()

    def mark_done_publishing(self) -> None:
        """Flag publisher exhaustion; may set drained immediately."""
        self.finished_publishing.set()
        if self.processed >= self.published:
            self.drained.set()


def make_candles() -> list[SpikeCandle]:
    """Synthesise two correlated price paths so MACD crosses fire.

    Returns:
        Flat list of SpikeCandle across BTC-USD and ETH-USD instruments,
        interleaved per timestamp for WARMUP_AND_RUN_CANDLES hours.
    """
    base_time = datetime(2026, 1, 1, tzinfo=UTC)
    out: list[SpikeCandle] = []
    for i in range(WARMUP_AND_RUN_CANDLES):
        t = base_time + timedelta(hours=i)
        btc_price = 50_000.0 + 2_000.0 * math.sin(i / 4.0) + 500.0 * math.sin(i / 1.7)
        eth_price = 3_000.0 + 150.0 * math.sin((i + 2) / 4.0) + 40.0 * math.sin(i / 1.9)
        out.append(SpikeCandle(open_at=t, exchange="kraken", instrument="BTC-USD", close=btc_price))
        out.append(SpikeCandle(open_at=t, exchange="kraken", instrument="ETH-USD", close=eth_price))
    return out


def _candle_to_payload(candle: SpikeCandle) -> str:
    """Build the JSON payload the strategy expects on a candle topic."""
    tracker = SequenceTracker()
    data = CandleData(
        type="candle",
        public_id=f"spike-{candle.instrument}-{int(candle.open_at.timestamp()):010d}",
        timestamp=candle.open_at,
        session_id=tracker.session_id,
        sequence_id=1,
        exchange=ExchangeEnum.KRAKEN,
        instrument=candle.instrument,
        timeframe="1h",
        open_at=candle.open_at,
        open=candle.close,
        high=candle.close * 1.002,
        low=candle.close * 0.998,
        close=candle.close,
        volume=10.0,
    )
    return data.model_dump_json()


def _topic(candle: SpikeCandle) -> str:
    return f"market.{candle.exchange}.{candle.instrument}.candles.1h"


def _make_strategy(instruments: tuple[str, ...], feed_addr: str | None) -> BaseStrategy:
    """Build a MACDCrossover subscribed to one or two instruments.

    When ``feed_addr`` is None the strategy runs with no ZMQ wiring
    (reference engine path). When set, the strategy subscribes to the
    local broker via the existing ``use_broker=False + feed_addr`` hook
    — no override of _connect_subscriber_socket needed.
    """
    topics = [f"market.kraken.{instr}.candles.1h" for instr in instruments]
    params: dict[str, Any] = {"fast": 12, "slow": 26, "signal_period": 9}
    if feed_addr is not None:
        params["use_broker"] = False
        params["feed_addr"] = feed_addr
    config = StrategyConfig(
        name=f"spike_macd_{'_'.join(instruments)}".replace("-", "_"),
        strategy_class="MACDCrossover",
        inputs=topics,
        outputs=list(instruments),
        exchange=ExchangeEnum.PAPER,
        params=params,
        wallet_public_id="00000000-0000-7000-8000-000000000001",
    )
    return MACDCrossover(config)


async def _process_batch(
    *,
    batch: list[SpikeCandle],
    strategy: BaseStrategy,
    portfolio: PortfolioTracker,
    latest_closes: dict[str, float],
    collector: SpikeCollector,
    start_date: datetime,
) -> None:
    """Exact mirror of DirectDbEngine._process_time_batch for in-memory candles."""
    for candle in batch:
        latest_closes[candle.instrument] = candle.close

    signals_and_candles: list[tuple[StrategySignal, SpikeCandle]] = []
    for candle in batch:
        payload = _candle_to_payload(candle)
        signal = await strategy._handle_candle_data(candle.instrument, payload)
        if signal is not None:
            signals_and_candles.append((signal, candle))

    if batch[0].open_at < start_date:
        return

    for signal, candle in signals_and_candles:
        fill = _apply_fill(signal, candle, portfolio)
        if fill is not None:
            collector.trades.append(
                SpikeTradeRecord(
                    executed_at=fill.fill_at,
                    instrument=fill.instrument,
                    side=fill.side,
                    quantity=fill.size,
                    price=fill.price,
                    fee=fill.fee,
                    pnl=fill.pnl,
                    position_after=portfolio.position_qty(fill.instrument),
                )
            )
        collector.signals.append(
            SpikeSignalRecord(
                signal_time=candle.open_at,
                signal_type=str(signal.side),
                instrument=candle.instrument,
                price=candle.close,
            )
        )

    position_value = sum(
        portfolio.position_qty(instr) * latest_closes.get(instr, 0.0) for instr in latest_closes
    )
    collector.equity.append(
        SpikeEquityRecord(
            point_time=batch[0].open_at,
            equity=portfolio.cash + position_value,
            cash=portfolio.cash,
            position_value=position_value,
        )
    )


def _apply_fill(
    signal: StrategySignal, candle: SpikeCandle, portfolio: PortfolioTracker
) -> BacktestFill | None:
    """Call the shared fill simulator with the spike's fixed knobs."""
    return simulate_market_fill(
        exchange=candle.exchange,
        instrument=candle.instrument,
        side=str(signal.side),
        close_price=candle.close,
        fill_at=candle.open_at,
        portfolio=portfolio,
        slippage_bps=0.0,
        commission_bps=0.0,
        signal_strength=getattr(signal, "strength", None),
        signal_reason=getattr(signal, "reason", None),
    )


async def run_reference(candles: list[SpikeCandle]) -> SpikeCollector:
    """Reference engine — drives the strategy directly, mirrors DirectDbEngine.

    Args:
        candles: Flat list of SpikeCandle across all instruments, ordered
            by ``open_at`` and interleaved per timestamp.

    Returns:
        Collector with signals/trades/equity produced by Direct-DB-style
        batch processing.
    """
    strategy = _make_strategy(INSTRUMENTS, feed_addr=None)
    portfolio = PortfolioTracker(cash=10_000.0)
    latest_closes: dict[str, float] = {}
    collector = SpikeCollector()

    batch: list[SpikeCandle] = []
    prev_time: datetime | None = None
    for candle in candles:
        if prev_time is not None and candle.open_at != prev_time and batch:
            await _process_batch(
                batch=batch,
                strategy=strategy,
                portfolio=portfolio,
                latest_closes=latest_closes,
                collector=collector,
                start_date=START_DATE,
            )
            batch = []
        batch.append(candle)
        prev_time = candle.open_at
    if batch:
        await _process_batch(
            batch=batch,
            strategy=strategy,
            portfolio=portfolio,
            latest_closes=latest_closes,
            collector=collector,
            start_date=START_DATE,
        )
    return collector


class _BacktestReplayState:
    """Container holding mixin state so inner closures mutate in place."""

    def __init__(self) -> None:
        self.pending_batch: list[SpikeCandle] = []
        self.portfolio = PortfolioTracker(cash=10_000.0)
        self.latest_closes: dict[str, float] = {}
        self.collector = SpikeCollector()


def _build_backtest_strategy_class(
    state: _BacktestReplayState,
    drain: SpikeDrainCoordinator,
) -> type[BaseStrategy]:
    """Dynamically subclass MACDCrossover with a buffering listen loop."""

    class _SpikeBacktestMACD(MACDCrossover):
        async def _listen_loop(self: BaseStrategy) -> None:
            """Buffer by timestamp, flush on boundary, re-raise exceptions."""
            assert self.subscriber is not None
            while self._running:
                topic_str, payload_bytes = await self.subscriber.recv_multipart()
                payload = payload_bytes.decode()
                if topic_str.startswith("market."):
                    data = CandleData.from_json(payload)
                    candle = SpikeCandle(
                        open_at=data.open_at,
                        exchange=str(data.exchange),
                        instrument=data.instrument,
                        close=float(data.close),
                    )
                    if state.pending_batch and state.pending_batch[0].open_at != candle.open_at:
                        await _process_batch(
                            batch=state.pending_batch,
                            strategy=self,
                            portfolio=state.portfolio,
                            latest_closes=state.latest_closes,
                            collector=state.collector,
                            start_date=START_DATE,
                        )
                        state.pending_batch = []
                    state.pending_batch.append(candle)
                drain.on_processed()

    return _SpikeBacktestMACD


async def run_via_broker(candles: list[SpikeCandle]) -> SpikeCollector:
    """ZMQ replay — buffer candles by timestamp, flush batches, mirror Direct-DB.

    Args:
        candles: Flat list of SpikeCandle across all instruments, ordered
            by ``open_at`` and interleaved per timestamp. Published one at
            a time through the local broker.

    Returns:
        Collector with signals/trades/equity produced on the broker path,
        which should match ``run_reference`` field-for-field.
    """
    broker = ZmqBrokerProcess(xsub_endpoint=BROKER_XSUB, xpub_endpoint=BROKER_XPUB)
    await broker.start()
    state = _BacktestReplayState()
    drain = SpikeDrainCoordinator()
    topics = [f"market.kraken.{instr}.candles.1h" for instr in INSTRUMENTS]
    config = StrategyConfig(
        name="spike_macd_zmq",
        strategy_class="MACDCrossover",
        inputs=topics,
        outputs=list(INSTRUMENTS),
        exchange=ExchangeEnum.PAPER,
        params={
            "fast": 12,
            "slow": 26,
            "signal_period": 9,
            "use_broker": False,
            "feed_addr": BROKER_XPUB,
        },
        wallet_public_id="00000000-0000-7000-8000-000000000001",
    )
    strategy_cls = _build_backtest_strategy_class(state, drain)
    strategy = strategy_cls(config)
    await strategy.start()
    await asyncio.sleep(0.3)

    ctx = zmq.asyncio.Context()
    pub_socket = ctx.socket(zmq.PUB)
    pub_socket.connect(BROKER_XSUB)
    await asyncio.sleep(0.1)
    try:
        for candle in candles:
            await pub_socket.send_multipart(
                [_topic(candle).encode(), _candle_to_payload(candle).encode()]
            )
            drain.on_publish()
            await asyncio.sleep(0.002)
        drain.mark_done_publishing()
        try:
            await asyncio.wait_for(drain.drained.wait(), timeout=10.0)
        except TimeoutError:
            logger.warning(
                "drain timeout: published={} processed={}", drain.published, drain.processed
            )
        if state.pending_batch:
            await _process_batch(
                batch=state.pending_batch,
                strategy=strategy,
                portfolio=state.portfolio,
                latest_closes=state.latest_closes,
                collector=state.collector,
                start_date=START_DATE,
            )
            state.pending_batch = []
    finally:
        pub_socket.setsockopt(zmq.LINGER, 0)
        pub_socket.close()
        ctx.term()
        await strategy.stop()
        await broker.stop()
    return state.collector


def _equal_signals(a: list[SpikeSignalRecord], b: list[SpikeSignalRecord]) -> bool:
    if len(a) != len(b):
        return False
    for x, y in zip(a, b, strict=True):
        if (x.signal_time, x.signal_type, x.instrument, round(x.price, 6)) != (
            y.signal_time,
            y.signal_type,
            y.instrument,
            round(y.price, 6),
        ):
            return False
    return True


def _equal_trades(a: list[SpikeTradeRecord], b: list[SpikeTradeRecord]) -> bool:
    if len(a) != len(b):
        return False
    for x, y in zip(a, b, strict=True):
        if (x.executed_at, x.instrument, x.side) != (y.executed_at, y.instrument, y.side):
            return False
        if abs(x.quantity - y.quantity) > 1e-10 or abs(x.price - y.price) > 1e-10:
            return False
        if abs(x.fee - y.fee) > 1e-10 or abs(x.position_after - y.position_after) > 1e-10:
            return False
    return True


def _equal_equity(a: list[SpikeEquityRecord], b: list[SpikeEquityRecord]) -> bool:
    if len(a) != len(b):
        return False
    for x, y in zip(a, b, strict=True):
        if x.point_time != y.point_time:
            return False
        if abs(x.equity - y.equity) > 1e-10:
            return False
        if abs(x.cash - y.cash) > 1e-10:
            return False
        if abs(x.position_value - y.position_value) > 1e-10:
            return False
    return True


def main() -> int:
    """Compare reference vs broker-replay on the same 2-instrument fixture.

    Returns:
        0 on full parity; 1 on any mismatch in signals, trades, or equity.
    """
    return asyncio.run(_main_async())


async def _main_async() -> int:
    """Async body of the CLI entry; see :func:`main`.

    Returns:
        0 on full parity; 1 on any mismatch in signals, trades, or equity.
    """
    candles = make_candles()
    logger.info("fixture: {} candles ({} per instrument)", len(candles), len(candles) // 2)

    ref = await run_reference(candles)
    via = await run_via_broker(candles)

    logger.info(
        "reference: {} signals, {} trades, {} equity points",
        len(ref.signals),
        len(ref.trades),
        len(ref.equity),
    )
    logger.info(
        "broker:    {} signals, {} trades, {} equity points",
        len(via.signals),
        len(via.trades),
        len(via.equity),
    )

    ok = True
    if not _equal_signals(ref.signals, via.signals):
        logger.error("SIGNALS MISMATCH")
        ok = False
    if not _equal_trades(ref.trades, via.trades):
        logger.error("TRADES MISMATCH")
        ok = False
    if not _equal_equity(ref.equity, via.equity):
        logger.error("EQUITY MISMATCH")
        ok = False
    if ok:
        logger.info(
            "PARITY OK (signals={} trades={} equity={})",
            len(ref.signals),
            len(ref.trades),
            len(ref.equity),
        )
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
