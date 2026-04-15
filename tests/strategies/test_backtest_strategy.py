"""Tests for BacktestReplayStrategy factory + 4 overrides."""

import asyncio
import json
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
import zmq
import zmq.asyncio

from snapper.application.backtest.config import BacktestConfig
from snapper.application.backtest.drain import DrainCoordinator
from snapper.application.backtest.endpoints import allocate_replay_endpoints
from snapper.application.backtest.result_collector import ResultCollector
from snapper.application.portfolio.models import PortfolioTracker
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.publishers.replay_publisher import WARMUP_PUBLIC_ID
from snapper.strategies.backtest_strategy import BacktestReplayState
from snapper.strategies.backtest_strategy import make_backtest_replay_strategy
from snapper.strategies.base import BaseStrategy
from snapper.strategies.models import StrategyConfig
from snapper.strategies.models import StrategySignal

NOW = datetime(2026, 1, 1, tzinfo=UTC)
T1 = NOW
T2 = NOW.replace(hour=1)


class _NoopStrategy(BaseStrategy):
    """Concrete BaseStrategy that records calls and never emits signals."""

    handled_calls: list[tuple[str, str]]

    def __init__(self, config: StrategyConfig) -> None:
        super().__init__(config)
        self.handled_calls = []

    async def reset(self) -> None:
        """Reset is a no-op for the test strategy."""

    async def _handle_candle_data(self, instrument: str, payload: str) -> StrategySignal | None:
        """Record but do not emit."""
        self.handled_calls.append((instrument, payload))
        return None


def _make_state(
    *,
    expected_topics: frozenset[str],
    config: BacktestConfig | None = None,
) -> BacktestReplayState:
    """Build a fresh BacktestReplayState for one test."""
    cfg = config or _make_config()
    return BacktestReplayState(
        run_public_id="run-1",
        config=cfg,
        snapshot_as_of=NOW,
        pending_batch=[],
        portfolio=PortfolioTracker(cash=10000.0),
        latest_closes={},
        collector=ResultCollector(),
        tracker=SequenceTracker(),
        expected_topics=expected_topics,
    )


def _make_config(instruments: dict[str, list[str]] | None = None) -> BacktestConfig:
    """Build a BacktestConfig MagicMock."""
    config = MagicMock(spec=BacktestConfig)
    config.instruments = instruments or {"kraken": ["BTC-USD"]}
    config.timeframe = "1h"
    config.start_date = NOW
    config.end_date = T2
    config.slippage_bps = 0.0
    config.commission_bps = 0.0
    config.strategy_params = {}
    return config


def _strategy_config(inputs: list[str]) -> StrategyConfig:
    """Build a StrategyConfig matching the engine's wire-up for the factory."""
    return StrategyConfig(
        name="bt_test",
        strategy_class="noop",
        inputs=inputs,
        outputs=["BTC-USD"],
        params={},
    )


def _candle_payload(open_at: datetime, close: float, instrument: str = "BTC-USD") -> bytes:
    """Build a real CandleData JSON payload."""
    return json.dumps(
        {
            "type": "candle",
            "public_id": f"candle-{open_at.isoformat()}",
            "timestamp": open_at.isoformat(),
            "session_id": "s1",
            "sequence_id": 1,
            "instrument": instrument,
            "exchange": "kraken",
            "timeframe": "1h",
            "open_at": open_at.isoformat(),
            "open": close - 1,
            "high": close + 1,
            "low": close - 2,
            "close": close,
            "volume": 1000.0,
            "vwap": None,
            "trades": None,
        }
    ).encode()


def _warmup_payload(instrument: str = "BTC-USD") -> bytes:
    """Build a warmup CandleData payload bytes with the sentinel public_id."""
    return json.dumps(
        {
            "type": "candle",
            "public_id": WARMUP_PUBLIC_ID,
            "timestamp": NOW.isoformat(),
            "session_id": "warmup",
            "sequence_id": 0,
            "instrument": instrument,
            "exchange": "kraken",
            "timeframe": "1h",
            "open_at": NOW.isoformat(),
            "open": 0.0,
            "high": 0.0,
            "low": 0.0,
            "close": 0.0,
            "volume": 0.0,
            "vwap": None,
            "trades": None,
        }
    ).encode()


@pytest.mark.asyncio
class TestStrategyFactory:
    """Factory wiring + override behaviour."""

    @pytest.mark.timeout(15)
    async def test_start_skips_heartbeat_and_uses_local_endpoints(self) -> None:
        """start() must not create a heartbeat task or touch the live broker."""
        broker, endpoints = await allocate_replay_endpoints()
        try:
            state = _make_state(expected_topics=frozenset({"market.kraken.BTC-USD.candles.1h"}))
            drain = DrainCoordinator()
            strategy = make_backtest_replay_strategy(
                inner_class=_NoopStrategy,
                inner_config=_strategy_config(inputs=["market.kraken.BTC-USD.candles.1h"]),
                state=state,
                drain=drain,
                local_xsub=endpoints.xsub,
                local_xpub=endpoints.xpub,
            )
            await strategy.start()
            try:
                assert strategy._heartbeat_task is None
                assert strategy._listen_task is not None
                assert strategy.subscriber is not None
                assert strategy.publisher is not None
            finally:
                listen = strategy._listen_task
                if listen is not None:
                    listen.cancel()
                    with pytest.raises((asyncio.CancelledError, BaseException)):
                        await asyncio.wait_for(listen, timeout=2.0)
                await strategy.stop()
        finally:
            await asyncio.wait_for(broker.stop(), timeout=2.0)

    @pytest.mark.timeout(15)
    async def test_warmup_ack_sets_subscriber_ready_only_after_all_topics(self) -> None:
        """subscriber_ready fires only when every expected topic has been ACKed."""
        broker, endpoints = await allocate_replay_endpoints()
        try:
            expected = frozenset(
                {
                    "market.kraken.BTC-USD.candles.1h",
                    "market.kraken.ETH-USD.candles.1h",
                }
            )
            state = _make_state(expected_topics=expected)
            drain = DrainCoordinator()
            strategy = make_backtest_replay_strategy(
                inner_class=_NoopStrategy,
                inner_config=_strategy_config(inputs=list(expected)),
                state=state,
                drain=drain,
                local_xsub=endpoints.xsub,
                local_xpub=endpoints.xpub,
            )
            await strategy.start()
            await broker.wait_for_subscription(b"market.", timeout=3.0)
            try:
                import zmq
                import zmq.asyncio

                ctx = zmq.asyncio.Context()
                pub = ctx.socket(zmq.PUB)
                pub.connect(endpoints.xsub)
                try:
                    btc_topic = "market.kraken.BTC-USD.candles.1h"
                    eth_topic = "market.kraken.ETH-USD.candles.1h"
                    for _ in range(50):
                        await pub.send_multipart([btc_topic.encode(), _warmup_payload("BTC-USD")])
                        if btc_topic in state.acked_topics:
                            break
                        await asyncio.sleep(0.02)
                    assert state.acked_topics == {btc_topic}
                    assert not state.subscriber_ready.is_set()
                    for _ in range(50):
                        await pub.send_multipart([eth_topic.encode(), _warmup_payload("ETH-USD")])
                        if state.subscriber_ready.is_set():
                            break
                        await asyncio.sleep(0.02)
                    assert state.subscriber_ready.is_set()
                    assert state.acked_topics == set(expected)
                finally:
                    pub.setsockopt(zmq.LINGER, 0)
                    pub.close()
                    ctx.term()
            finally:
                listen = strategy._listen_task
                if listen is not None:
                    listen.cancel()
                    with pytest.raises((asyncio.CancelledError, BaseException)):
                        await asyncio.wait_for(listen, timeout=2.0)
                await strategy.stop()
        finally:
            await asyncio.wait_for(broker.stop(), timeout=2.0)

    @pytest.mark.timeout(15)
    async def test_real_candle_buffers_and_increments_drain(self) -> None:
        """A real candle is buffered to pending_batch and drain.on_processed bumps."""
        broker, endpoints = await allocate_replay_endpoints()
        try:
            state = _make_state(expected_topics=frozenset({"market.kraken.BTC-USD.candles.1h"}))
            drain = DrainCoordinator()
            strategy = make_backtest_replay_strategy(
                inner_class=_NoopStrategy,
                inner_config=_strategy_config(inputs=["market.kraken.BTC-USD.candles.1h"]),
                state=state,
                drain=drain,
                local_xsub=endpoints.xsub,
                local_xpub=endpoints.xpub,
            )
            await strategy.start()
            await broker.wait_for_subscription(b"market.", timeout=3.0)
            try:
                import zmq
                import zmq.asyncio

                ctx = zmq.asyncio.Context()
                pub = ctx.socket(zmq.PUB)
                pub.connect(endpoints.xsub)
                try:
                    topic = "market.kraken.BTC-USD.candles.1h"
                    for _ in range(50):
                        await pub.send_multipart([topic.encode(), _candle_payload(T1, 100.0)])
                        if state.pending_batch:
                            break
                        await asyncio.sleep(0.02)
                    assert len(state.pending_batch) >= 1
                    assert state.pending_batch[0].open_at == T1
                    assert drain.processed_count >= 1
                    assert drain.published_count == 0
                finally:
                    pub.setsockopt(zmq.LINGER, 0)
                    pub.close()
                    ctx.term()
            finally:
                listen = strategy._listen_task
                if listen is not None:
                    listen.cancel()
                    with pytest.raises((asyncio.CancelledError, BaseException)):
                        await asyncio.wait_for(listen, timeout=2.0)
                await strategy.stop()
        finally:
            await asyncio.wait_for(broker.stop(), timeout=2.0)

    @pytest.mark.timeout(15)
    async def test_time_batch_boundary_flushes_pending(self) -> None:
        """A new open_at flushes the prior batch through process_time_batch."""
        broker, endpoints = await allocate_replay_endpoints()
        try:
            state = _make_state(expected_topics=frozenset({"market.kraken.BTC-USD.candles.1h"}))
            drain = DrainCoordinator()
            strategy = make_backtest_replay_strategy(
                inner_class=_NoopStrategy,
                inner_config=_strategy_config(inputs=["market.kraken.BTC-USD.candles.1h"]),
                state=state,
                drain=drain,
                local_xsub=endpoints.xsub,
                local_xpub=endpoints.xpub,
            )
            await strategy.start()
            await broker.wait_for_subscription(b"market.", timeout=3.0)
            try:
                import zmq
                import zmq.asyncio

                ctx = zmq.asyncio.Context()
                pub = ctx.socket(zmq.PUB)
                pub.connect(endpoints.xsub)
                try:
                    topic = "market.kraken.BTC-USD.candles.1h"
                    for _ in range(50):
                        await pub.send_multipart([topic.encode(), _candle_payload(T1, 100.0)])
                        if state.pending_batch:
                            break
                        await asyncio.sleep(0.02)
                    assert state.pending_batch and state.pending_batch[0].open_at == T1
                    await pub.send_multipart([topic.encode(), _candle_payload(T2, 101.0)])
                    for _ in range(100):
                        if state.pending_batch and state.pending_batch[0].open_at == T2:
                            break
                        await asyncio.sleep(0.02)
                    assert len(state.pending_batch) == 1
                    assert state.pending_batch[0].open_at == T2
                    assert abs(state.latest_closes.get("BTC-USD", 0.0) - 100.0) < 1e-9
                finally:
                    pub.setsockopt(zmq.LINGER, 0)
                    pub.close()
                    ctx.term()
            finally:
                listen = strategy._listen_task
                if listen is not None:
                    listen.cancel()
                    with pytest.raises((asyncio.CancelledError, BaseException)):
                        await asyncio.wait_for(listen, timeout=2.0)
                await strategy.stop()
        finally:
            await asyncio.wait_for(broker.stop(), timeout=2.0)

    @pytest.mark.timeout(15)
    async def test_start_reuses_existing_context_and_skips_non_market_inputs(self) -> None:
        """Injected context is reused and non-market inputs never subscribe."""
        broker, endpoints = await allocate_replay_endpoints()
        existing_ctx = zmq.asyncio.Context()
        try:
            state = _make_state(expected_topics=frozenset({"market.kraken.BTC-USD.candles.1h"}))
            drain = DrainCoordinator()
            strategy = make_backtest_replay_strategy(
                inner_class=_NoopStrategy,
                inner_config=_strategy_config(
                    inputs=["system.health", "market.kraken.BTC-USD.candles.1h"]
                ),
                state=state,
                drain=drain,
                local_xsub=endpoints.xsub,
                local_xpub=endpoints.xpub,
            )
            strategy.zmq_context = existing_ctx
            await strategy.start()
            try:
                assert strategy.zmq_context is existing_ctx
                await broker.wait_for_subscription(b"market.", timeout=3.0)
                with pytest.raises(TimeoutError):
                    await broker.wait_for_subscription(b"system.", timeout=0.2)
            finally:
                listen = strategy._listen_task
                if listen is not None:
                    listen.cancel()
                    with pytest.raises((asyncio.CancelledError, BaseException)):
                        await asyncio.wait_for(listen, timeout=2.0)
                await strategy.stop()
        finally:
            existing_ctx.term()
            await asyncio.wait_for(broker.stop(), timeout=2.0)

    @pytest.mark.timeout(10)
    async def test_listen_loop_skips_non_market_frames(self) -> None:
        """The replay listen loop ignores non-market frames."""
        state = _make_state(expected_topics=frozenset({"market.kraken.BTC-USD.candles.1h"}))
        strategy = make_backtest_replay_strategy(
            inner_class=_NoopStrategy,
            inner_config=_strategy_config(inputs=["market.kraken.BTC-USD.candles.1h"]),
            state=state,
            drain=DrainCoordinator(),
            local_xsub="inproc://local-xsub",
            local_xpub="inproc://local-xpub",
        )
        strategy.subscriber = MagicMock()
        strategy.subscriber.recv_multipart = AsyncMock(
            side_effect=[("system.health", b"ignored"), asyncio.CancelledError()]
        )
        strategy._running = True

        with pytest.raises(asyncio.CancelledError):
            await strategy._listen_loop()

        assert state.pending_batch == []

    @pytest.mark.timeout(10)
    async def test_listen_loop_checks_cancel_probe_for_market_frames(self) -> None:
        """A processed market frame probes cancellation when configured."""
        state = _make_state(expected_topics=frozenset({"market.kraken.BTC-USD.candles.1h"}))
        state.cancel_probe = AsyncMock()
        strategy = make_backtest_replay_strategy(
            inner_class=_NoopStrategy,
            inner_config=_strategy_config(inputs=["market.kraken.BTC-USD.candles.1h"]),
            state=state,
            drain=DrainCoordinator(),
            local_xsub="inproc://local-xsub",
            local_xpub="inproc://local-xpub",
        )
        strategy.subscriber = MagicMock()
        strategy.subscriber.recv_multipart = AsyncMock(
            side_effect=[
                ("market.kraken.BTC-USD.candles.1h", _candle_payload(T1, 100.0)),
                asyncio.CancelledError(),
            ]
        )
        strategy._running = True

        with pytest.raises(asyncio.CancelledError):
            await strategy._listen_loop()

        state.cancel_probe.check.assert_awaited_once()
        assert len(state.pending_batch) == 1

    @pytest.mark.timeout(10)
    async def test_listen_loop_marks_strategy_stopped_on_exception(self) -> None:
        """Unexpected listen-loop errors clear the running flag before bubbling up."""
        state = _make_state(expected_topics=frozenset({"market.kraken.BTC-USD.candles.1h"}))
        strategy = make_backtest_replay_strategy(
            inner_class=_NoopStrategy,
            inner_config=_strategy_config(inputs=["market.kraken.BTC-USD.candles.1h"]),
            state=state,
            drain=DrainCoordinator(),
            local_xsub="inproc://local-xsub",
            local_xpub="inproc://local-xpub",
        )
        strategy.subscriber = MagicMock()
        strategy.subscriber.recv_multipart = AsyncMock(side_effect=RuntimeError("listen failed"))
        strategy._running = True

        with pytest.raises(RuntimeError, match="listen failed"):
            await strategy._listen_loop()

        assert strategy._running is False

    @pytest.mark.timeout(10)
    async def test_listen_loop_returns_immediately_when_not_running(self) -> None:
        """A stopped strategy exits the replay listen loop without polling sockets."""
        state = _make_state(expected_topics=frozenset({"market.kraken.BTC-USD.candles.1h"}))
        strategy = make_backtest_replay_strategy(
            inner_class=_NoopStrategy,
            inner_config=_strategy_config(inputs=["market.kraken.BTC-USD.candles.1h"]),
            state=state,
            drain=DrainCoordinator(),
            local_xsub="inproc://local-xsub",
            local_xpub="inproc://local-xpub",
        )
        strategy.subscriber = MagicMock()
        strategy._running = False

        await strategy._listen_loop()

        strategy.subscriber.recv_multipart.assert_not_called()

    async def test_factory_rejects_non_base_strategy_instance(self) -> None:
        """Factory rejects classes that do not produce a BaseStrategy instance."""
        await asyncio.sleep(0)

        class _NotBase:
            def __init__(self, config: StrategyConfig) -> None:
                self.config = config

        with pytest.raises(TypeError, match="non-BaseStrategy"):
            make_backtest_replay_strategy(
                inner_class=_NotBase,
                inner_config=_strategy_config(inputs=["market.kraken.BTC-USD.candles.1h"]),
                state=_make_state(expected_topics=frozenset({"market.kraken.BTC-USD.candles.1h"})),
                drain=DrainCoordinator(),
                local_xsub="inproc://local-xsub",
                local_xpub="inproc://local-xpub",
            )
