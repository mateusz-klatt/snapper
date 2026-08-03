"""Tests for ReplayPublisher echo-ack handshake + drain semantics."""

import asyncio
import json
from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import zmq
import zmq.asyncio

from snapper.application.backtest.config import BacktestConfig
from snapper.application.backtest.drain import BacktestDrainTimeoutError
from snapper.application.backtest.drain import BacktestReadinessTimeoutError
from snapper.application.backtest.drain import DrainCoordinator
from snapper.application.backtest.endpoints import allocate_replay_endpoints
from snapper.messaging.infrastructure.broker import ZmqBrokerProcess
from snapper.messaging.publishers import replay_publisher as rp
from snapper.messaging.publishers.replay_publisher import WARMUP_PUBLIC_ID
from snapper.messaging.publishers.replay_publisher import ReplayPublisher

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _candle_row(open_at: datetime, close: float = 100.0) -> dict[str, Any]:
    """Build a minimal CandleRow dict."""
    return {
        "open_at": open_at,
        "timeframe": "1h",
        "open": close - 1,
        "high": close + 1,
        "low": close - 2,
        "close": close,
        "volume": 1000.0,
        "vwap": None,
        "trades": None,
        "public_id": f"candle-{open_at.isoformat()}",
        "timestamp": open_at,
        "session_id": "s1",
        "sequence_id": 1,
    }


def _make_config(instruments: dict[str, list[str]] | None = None) -> BacktestConfig:
    """Build a BacktestConfig MagicMock matching what the publisher inspects."""
    config = MagicMock(spec=BacktestConfig)
    config.instruments = instruments or {"kraken": ["BTC-USD"]}
    config.timeframe = "1h"
    config.end_date = NOW
    config.target_execution_exchange = None
    return config


async def _strategy_harness(
    broker: ZmqBrokerProcess,
    topics: list[str],
    drain: DrainCoordinator,
    subscriber_ready: asyncio.Event,
    *,
    expected_real_candles: int,
    ack_after_retry: int = 1,
    never_ack: bool = False,
    process_real: bool = True,
) -> int:
    """Run a SUB socket that mimics the strategy mixin's ACK behaviour.

    Returns the number of real candles received. Sets subscriber_ready
    only after every topic in ``topics`` has been ACKed at least once,
    optionally skipping the first ``ack_after_retry-1`` rounds to test
    retry behaviour. When ``process_real`` is False the harness still
    drains the socket but never calls ``drain.on_processed``, simulating
    a strategy that ACKs warmup but stalls before consuming real candles
    — used to drive the drain-timeout path.
    """
    ctx = zmq.asyncio.Context()
    sub = ctx.socket(zmq.SUB)
    sub.connect(broker.xpub_endpoint)
    for topic in topics:
        sub.setsockopt(zmq.SUBSCRIBE, topic.encode())
    async with asyncio.timeout(3.0):
        await broker.wait_for_subscription(b"market.")
    acked: set[str] = set()
    real_count = 0
    seen_warmup_rounds_per_topic: dict[str, int] = dict.fromkeys(topics, 0)
    try:
        while True:
            try:
                topic_bytes, payload = await asyncio.wait_for(sub.recv_multipart(), timeout=10.0)
            except TimeoutError:
                break
            topic_str = topic_bytes.decode()
            data = json.loads(payload.decode())
            if data["public_id"] == WARMUP_PUBLIC_ID:
                seen_warmup_rounds_per_topic[topic_str] = (
                    seen_warmup_rounds_per_topic.get(topic_str, 0) + 1
                )
                if never_ack:
                    continue
                if seen_warmup_rounds_per_topic[topic_str] < ack_after_retry:
                    continue
                acked.add(topic_str)
                if acked >= set(topics):
                    subscriber_ready.set()
                continue
            real_count += 1
            if process_real:
                drain.on_processed()
            if process_real and real_count >= expected_real_candles:
                break
    finally:
        sub.setsockopt(zmq.LINGER, 0)
        sub.close()
        ctx.term()
    return real_count


@pytest.mark.asyncio
class TestReplayPublisher:
    """Echo-ack handshake, streaming, drain, and cleanup paths."""

    @pytest.mark.timeout(15)
    async def test_handshake_then_streams_all_candles(self) -> None:
        """First-attempt ack: every published candle is processed."""
        broker, endpoints = await allocate_replay_endpoints()
        try:
            t1 = NOW
            t2 = NOW.replace(hour=1)
            repo = AsyncMock()
            repo.get_candles = AsyncMock(return_value=[_candle_row(t1, 100), _candle_row(t2, 101)])
            config = _make_config()
            drain = DrainCoordinator()
            ready = asyncio.Event()
            publisher = ReplayPublisher(
                local_xsub=endpoints.xsub,
                repository=repo,
                config=config,
                snapshot_as_of=NOW,
                drain=drain,
                subscriber_ready=ready,
            )
            harness = asyncio.create_task(
                _strategy_harness(
                    broker,
                    topics=["market.kraken.BTC-USD.candles.1h"],
                    drain=drain,
                    subscriber_ready=ready,
                    expected_real_candles=2,
                )
            )
            await asyncio.wait_for(publisher.start(), timeout=10.0)
            count = await asyncio.wait_for(harness, timeout=2.0)
            assert count == 2
            assert drain.published_count == 2
            assert drain.processed_count == 2
            assert drain.drained.is_set()
        finally:
            await asyncio.wait_for(broker.stop(), timeout=2.0)

    @pytest.mark.timeout(15)
    async def test_handshake_completes_after_retry(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Strategy ACKs only on round 3 — publisher completes without raising."""
        broker, endpoints = await allocate_replay_endpoints()
        try:
            repo = AsyncMock()
            repo.get_candles = AsyncMock(return_value=[_candle_row(NOW, 100)])
            config = _make_config()
            drain = DrainCoordinator()
            ready = asyncio.Event()
            publisher = ReplayPublisher(
                local_xsub=endpoints.xsub,
                repository=repo,
                config=config,
                snapshot_as_of=NOW,
                drain=drain,
                subscriber_ready=ready,
            )
            monkeypatch.setattr(rp, "WARMUP_READY_TIMEOUT_S", 0.2)
            harness = asyncio.create_task(
                _strategy_harness(
                    broker,
                    topics=["market.kraken.BTC-USD.candles.1h"],
                    drain=drain,
                    subscriber_ready=ready,
                    expected_real_candles=1,
                    ack_after_retry=3,
                )
            )
            async with asyncio.timeout(3.0):
                await broker.wait_for_subscription(b"market.")
            await asyncio.wait_for(publisher.start(), timeout=10.0)
            await asyncio.wait_for(harness, timeout=2.0)
            assert drain.processed_count == 1
        finally:
            await asyncio.wait_for(broker.stop(), timeout=2.0)

    @pytest.mark.timeout(15)
    async def test_handshake_timeout_when_strategy_never_acks(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Strategy that never ACKs forces BacktestReadinessTimeoutError."""
        broker, endpoints = await allocate_replay_endpoints()
        try:
            repo = AsyncMock()
            repo.get_candles = AsyncMock(return_value=[_candle_row(NOW, 100)])
            config = _make_config()
            drain = DrainCoordinator()
            ready = asyncio.Event()
            publisher = ReplayPublisher(
                local_xsub=endpoints.xsub,
                repository=repo,
                config=config,
                snapshot_as_of=NOW,
                drain=drain,
                subscriber_ready=ready,
            )
            monkeypatch.setattr(rp, "WARMUP_READY_TIMEOUT_S", 0.1)
            harness = asyncio.create_task(
                _strategy_harness(
                    broker,
                    topics=["market.kraken.BTC-USD.candles.1h"],
                    drain=drain,
                    subscriber_ready=ready,
                    expected_real_candles=1,
                    never_ack=True,
                )
            )
            handshake_attempt = publisher.start()
            with pytest.raises(BacktestReadinessTimeoutError) as exc_info:
                await asyncio.wait_for(handshake_attempt, timeout=10.0)
            assert "5 retries" in str(exc_info.value)
            assert "market.kraken.BTC-USD.candles.1h" in str(exc_info.value)
            harness.cancel()
            with pytest.raises((asyncio.CancelledError, TimeoutError, BaseException)):
                await asyncio.wait_for(harness, timeout=2.0)
        finally:
            await asyncio.wait_for(broker.stop(), timeout=2.0)

    @pytest.mark.timeout(20)
    async def test_drain_timeout_when_processed_lags(self) -> None:
        """If the strategy stops processing mid-stream, drain raises with counters."""
        broker, endpoints = await allocate_replay_endpoints()
        try:
            t1 = NOW
            t2 = NOW.replace(hour=1)
            repo = AsyncMock()
            repo.get_candles = AsyncMock(return_value=[_candle_row(t1, 100), _candle_row(t2, 101)])
            config = _make_config()
            drain = DrainCoordinator()
            ready = asyncio.Event()
            publisher = ReplayPublisher(
                local_xsub=endpoints.xsub,
                repository=repo,
                config=config,
                snapshot_as_of=NOW,
                drain=drain,
                subscriber_ready=ready,
            )

            from snapper.messaging.publishers import replay_publisher as rp

            original_drain_timeout = rp.DRAIN_TIMEOUT_S
            rp.DRAIN_TIMEOUT_S = 0.5
            try:
                harness = asyncio.create_task(
                    _strategy_harness(
                        broker,
                        topics=["market.kraken.BTC-USD.candles.1h"],
                        drain=drain,
                        subscriber_ready=ready,
                        expected_real_candles=2,
                        process_real=False,
                    )
                )
                stream_attempt = publisher.start()
                with pytest.raises(BacktestDrainTimeoutError) as exc_info:
                    await asyncio.wait_for(stream_attempt, timeout=10.0)
                assert "published=2" in str(exc_info.value)
                assert "processed=" in str(exc_info.value)
                harness.cancel()
                with pytest.raises((asyncio.CancelledError, TimeoutError, BaseException)):
                    await asyncio.wait_for(harness, timeout=2.0)
            finally:
                rp.DRAIN_TIMEOUT_S = original_drain_timeout
        finally:
            await asyncio.wait_for(broker.stop(), timeout=2.0)

    @pytest.mark.timeout(15)
    async def test_socket_closed_on_handshake_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Context.term() is reached even when handshake raises."""
        broker, endpoints = await allocate_replay_endpoints()
        try:
            repo = AsyncMock()
            repo.get_candles = AsyncMock(return_value=[])
            config = _make_config()
            drain = DrainCoordinator()
            ready = asyncio.Event()
            publisher = ReplayPublisher(
                local_xsub=endpoints.xsub,
                repository=repo,
                config=config,
                snapshot_as_of=NOW,
                drain=drain,
                subscriber_ready=ready,
            )
            monkeypatch.setattr(rp, "WARMUP_READY_TIMEOUT_S", 0.1)
            first_start_attempt = publisher.start()
            with pytest.raises(BacktestReadinessTimeoutError):
                await asyncio.wait_for(first_start_attempt, timeout=10.0)
            second_publisher = ReplayPublisher(
                local_xsub=endpoints.xsub,
                repository=repo,
                config=config,
                snapshot_as_of=NOW,
                drain=DrainCoordinator(),
                subscriber_ready=asyncio.Event(),
            )
            second_start_attempt = second_publisher.start()
            with pytest.raises(BacktestReadinessTimeoutError):
                await asyncio.wait_for(second_start_attempt, timeout=10.0)
        finally:
            await asyncio.wait_for(broker.stop(), timeout=2.0)

    @pytest.mark.timeout(10)
    async def test_context_terminated_when_socket_creation_fails(self) -> None:
        """start() terminates the ZMQ context even before a socket exists."""
        repo = AsyncMock()
        config = _make_config()
        drain = DrainCoordinator()
        ready = asyncio.Event()
        publisher = ReplayPublisher(
            local_xsub="tcp://127.0.0.1:1",
            repository=repo,
            config=config,
            snapshot_as_of=NOW,
            drain=drain,
            subscriber_ready=ready,
        )
        mock_ctx = MagicMock()
        mock_ctx.socket.side_effect = RuntimeError("socket create failed")

        with (
            patch(
                "snapper.messaging.publishers.replay_publisher.zmq.asyncio.Context",
                return_value=mock_ctx,
            ),
            pytest.raises(RuntimeError, match="socket create failed"),
        ):
            await publisher.start()

        mock_ctx.term.assert_called_once()

    @pytest.mark.timeout(15)
    async def test_published_candle_carries_topic_field(self) -> None:
        """Real (non-warmup) candle payloads carry ``topic`` matching the wire topic.

        Chokepoint contract: the replay publisher routes every
        ``StrictDataSchema``-derived send through ``publish_to(topic)``,
        so consumers see the routing key on the payload itself instead
        of having to read the ZMQ frame header.
        """
        broker, endpoints = await allocate_replay_endpoints()
        try:
            repo = AsyncMock()
            repo.get_candles = AsyncMock(return_value=[_candle_row(NOW, 100)])
            config = _make_config()
            drain = DrainCoordinator()
            ready = asyncio.Event()
            publisher = ReplayPublisher(
                local_xsub=endpoints.xsub,
                repository=repo,
                config=config,
                snapshot_as_of=NOW,
                drain=drain,
                subscriber_ready=ready,
            )
            expected_topic = "market.kraken.BTC-USD.candles.1h"
            ctx = zmq.asyncio.Context()
            sub = ctx.socket(zmq.SUB)
            sub.connect(broker.xpub_endpoint)
            sub.setsockopt(zmq.SUBSCRIBE, expected_topic.encode())
            try:
                async with asyncio.timeout(3.0):
                    await broker.wait_for_subscription(b"market.")
                publish_task = asyncio.create_task(publisher.start())
                seen_real_with_topic = False
                async with asyncio.timeout(10.0):
                    while not seen_real_with_topic:
                        topic_bytes, payload = await sub.recv_multipart()
                        topic_str = topic_bytes.decode()
                        data = json.loads(payload.decode())
                        if data["public_id"] == WARMUP_PUBLIC_ID:
                            ready.set()
                            drain.mark_done_publishing()
                            continue
                        assert data["topic"] == expected_topic, (
                            f"replay-published candle on topic {topic_str!r} carries "
                            f"data['topic']={data.get('topic')!r}; expected stamped value"
                        )
                        seen_real_with_topic = True
                        drain.on_processed()
                drain.drained.set()
                await asyncio.wait_for(publish_task, timeout=2.0)
            finally:
                sub.setsockopt(zmq.LINGER, 0)
                sub.close()
                ctx.term()
        finally:
            await asyncio.wait_for(broker.stop(), timeout=2.0)


class TestBuildWarmupCandleErrorPaths:
    """Unit tests for ``ReplayPublisher._build_warmup_candle`` validation guards."""

    @staticmethod
    def _make_publisher() -> ReplayPublisher:
        """Construct a minimal ReplayPublisher for unit-level helper tests."""
        return ReplayPublisher(
            local_xsub="inproc://test",
            repository=AsyncMock(),
            config=_make_config(),
            snapshot_as_of=NOW,
            drain=DrainCoordinator(),
            subscriber_ready=asyncio.Event(),
        )

    def test_unparseable_topic_raises_value_error(self) -> None:
        """An unparseable market topic raises ``ValueError`` with the topic.

        Covers the ``parsed is None`` guard — ``parse_market_topic``
        returns ``None`` for malformed topics, and the warmup builder
        cannot proceed without a parsed exchange/instrument.
        """
        publisher = self._make_publisher()
        with pytest.raises(ValueError, match=r"Cannot parse warmup market topic: invalid"):
            publisher._build_warmup_candle("invalid")

    def test_topic_without_timeframe_raises_value_error(self) -> None:
        """A parseable but timeframe-less topic raises ``ValueError``.

        Covers the ``parsed.timeframe is None`` guard — ticks topics
        parse successfully but have no timeframe, which a warmup
        candle payload requires.
        """
        publisher = self._make_publisher()
        topic = "market.kraken.BTC-USD.ticks"
        with pytest.raises(ValueError, match=r"Warmup topic missing timeframe: " + topic):
            publisher._build_warmup_candle(topic)
