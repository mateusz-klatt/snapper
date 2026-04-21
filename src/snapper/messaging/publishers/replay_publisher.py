"""ReplayPublisher — streams historical candles into a per-run ZMQ broker.

Used by the backtest ZMQ replay engine. Connects a
PUB socket to the per-run XSUB endpoint (allocated in
mod:`snapper.application.backtest.endpoints`), proves the strategy is
actually receiving on every expected topic via an echo-ack handshake
then streams real candles in time-sorted order.
Echo-ack handshake
    The PUB socket has an asynchronous filter map that becomes populated
    once an upstream subscription frame arrives, but the PUB → SUB filter
    can lag behind even a successful XPUB_VERBOSE observation. The plan
    therefore proves end-to-end delivery on **every** subscribed topic
    rather than any one. The strategy mixin records each warmup it
    receives into ``state.acked_topics`` and only sets ``subscriber_ready``
    when the set equals ``state.expected_topics``. The publisher retries
    warmup on **all** topics each round (no sleep — the retry IS the
    delay). After ``WARMUP_MAX_RETRIES`` rounds of
    ``WARMUP_READY_TIMEOUT_S`` seconds each, the publisher gives up with
    ``BacktestReadinessTimeoutError``.
Streaming + drain
    Iterates ``iter_sorted_candle_chunks`` and publishes each candle on
    ``market.{exchange}.{instrument}.candles.{timeframe}``. Each publish
    bumps ``DrainCoordinator.on_publish``. After the last candle the
    publisher calls ``mark_done_publishing()`` and awaits ``drained.wait()``
    bounded by ``DRAIN_TIMEOUT_S``. Drain failure raises
    ``BacktestDrainTimeoutError`` with the published / processed counters
    in the message so an operator can tell publisher-stall from
    strategy-death.
The publisher always closes the socket and terminates the context in a
``finally`` block, regardless of which phase raised.
"""

import asyncio
from datetime import UTC
from datetime import datetime
from typing import Final

import zmq
import zmq.asyncio
from loguru import logger

from snapper.application.backtest.batch_processor import candle_row_to_data
from snapper.application.backtest.config import BacktestConfig
from snapper.application.backtest.direct_engine import iter_sorted_candle_chunks
from snapper.application.backtest.drain import BacktestDrainTimeoutError
from snapper.application.backtest.drain import BacktestReadinessTimeoutError
from snapper.application.backtest.drain import DrainCoordinator
from snapper.data.repository import Repository

WARMUP_PUBLIC_ID: Final[str] = "00000000-0000-7000-8000-000000000000"
WARMUP_READY_TIMEOUT_S: Final[float] = 1.0
WARMUP_MAX_RETRIES: Final[int] = 5
DRAIN_TIMEOUT_S: Final[float] = 10.0


class ReplayPublisher:
    """Publishes historical candles into a per-run replay broker.

    Single-shot lifecycle: construct, ``await start()``, done. The instance
    is not reusable — the ZMQ context is terminated on exit.
    """

    def __init__(
        self,
        *,
        local_xsub: str,
        repository: Repository,
        config: BacktestConfig,
        snapshot_as_of: datetime,
        drain: DrainCoordinator,
        subscriber_ready: asyncio.Event,
    ) -> None:
        """Wire the publisher to a running replay broker and coordinator.

        Args:
            local_xsub: tcp endpoint of the per-run broker's XSUB socket.
            repository: Source of historical candle rows.
            config: Backtest configuration providing the instrument set,
                timeframe, and end_date for the candle iterator.
            snapshot_as_of: Bitemporal snapshot anchor for all DB reads.
            drain: Shared coordinator. The publisher calls ``on_publish``
                per candle and ``mark_done_publishing`` at end-of-stream.
            subscriber_ready: Event the strategy sets once every expected
                topic has been ACKed via the echo-ack handshake.
        """
        self._local_xsub = local_xsub
        self._repository = repository
        self._config = config
        self._snapshot_as_of = snapshot_as_of
        self._drain = drain
        self._subscriber_ready = subscriber_ready

    def _build_market_topics(self) -> list[str]:
        """Compute the deterministic topic set the strategy will subscribe to.

        Mirrors ``ZmqReplayEngine.expected_topics`` so the handshake is
        symmetrical: the publisher sends a warmup on every topic the
        strategy is waiting on, and only those topics.
        """
        return [
            f"market.{exchange}.{instrument}.candles.{self._config.timeframe}"
            for exchange, instruments in self._config.instruments.items()
            for instrument in instruments
        ]

    def _build_warmup_candle(self, topic: str) -> bytes:
        """Build a warmup CandleData payload bytes for ``topic``.

        Uses the well-known ``WARMUP_PUBLIC_ID`` sentinel so the strategy
        mixin's ``_listen_loop`` can detect and ACK it without buffering
        or counting it as a real candle. Topic structure
        (``market.{exchange}.{instrument}.candles.{timeframe}``) is parsed
        back to populate the schema so the payload validates.
        """
        parts = topic.split(".")
        exchange = parts[1]
        instrument = parts[2]
        timeframe = parts[4]
        anchor = datetime.now(UTC)
        return (
            "{"
            f'"type":"candle",'
            f'"public_id":"{WARMUP_PUBLIC_ID}",'
            f'"timestamp":"{anchor.isoformat()}",'
            f'"session_id":"backtest-warmup",'
            f'"sequence_id":0,'
            f'"instrument":"{instrument}",'
            f'"exchange":"{exchange}",'
            f'"timeframe":"{timeframe}",'
            f'"open_at":"{anchor.isoformat()}",'
            f'"open":0.0,"high":0.0,"low":0.0,"close":0.0,"volume":0.0,'
            f'"vwap":null,"trades":null'
            "}"
        ).encode()

    async def _handshake(self, socket: zmq.asyncio.Socket) -> None:
        """Run the per-topic echo-ack handshake.

        Retries warmup on every subscribed topic each round until the
        strategy sets ``subscriber_ready`` (only after ACKing every
        expected topic). No ``asyncio.sleep`` — the retry round IS the
        delay; ``wait_for(event, timeout)`` returns immediately on the
        event so this stays as fast as possible.

        Raises:
            BacktestReadinessTimeoutError: After ``WARMUP_MAX_RETRIES``
                rounds without all expected topics being ACKed.
        """
        topics = self._build_market_topics()
        timeout_message = (
            f"strategy did not ack warmup after {WARMUP_MAX_RETRIES} retries "
            f"({WARMUP_READY_TIMEOUT_S}s each); topics={topics}"
        )
        for attempt in range(1, WARMUP_MAX_RETRIES + 1):
            for topic in topics:
                warmup = self._build_warmup_candle(topic)
                await socket.send_multipart([topic.encode(), warmup])
            try:
                await asyncio.wait_for(
                    self._subscriber_ready.wait(),
                    timeout=WARMUP_READY_TIMEOUT_S,
                )
                logger.debug(
                    "replay publisher: handshake OK on attempt {} across {} topics",
                    attempt,
                    len(topics),
                )
                return
            except TimeoutError:
                continue
        raise BacktestReadinessTimeoutError(timeout_message)

    async def _stream_candles(self, socket: zmq.asyncio.Socket) -> None:
        """Stream every historical candle in time-sorted order.

        Each publish bumps ``drain.on_publish`` so the strategy knows how
        many candles to wait for. After the last candle, signals
        ``mark_done_publishing`` and waits on ``drained.wait()`` bounded
        by ``DRAIN_TIMEOUT_S``.

        Raises:
            BacktestDrainTimeoutError: If the strategy fails to catch up
                to the published count within the drain budget.
        """
        async for chunk in iter_sorted_candle_chunks(
            self._config, self._repository, self._snapshot_as_of
        ):
            for event in chunk:
                data = candle_row_to_data(event, self._config.timeframe)
                topic = (
                    f"market.{event.exchange}.{event.instrument}"
                    f".candles.{self._config.timeframe}"
                )
                await socket.send_multipart([topic.encode(), data.model_dump_json().encode()])
                self._drain.on_publish()
        self._drain.mark_done_publishing()
        try:
            await asyncio.wait_for(self._drain.drained.wait(), timeout=DRAIN_TIMEOUT_S)
        except TimeoutError as e:
            raise BacktestDrainTimeoutError(
                f"drain timeout after {DRAIN_TIMEOUT_S}s: "
                f"published={self._drain.published_count} "
                f"processed={self._drain.processed_count}"
            ) from e

    async def start(self) -> None:
        """Run the publisher: handshake, stream, drain. Always cleans up.

        ZMQ context and PUB socket are created here and torn down in a
        ``finally`` block regardless of which phase raised, so the engine
        does not leak ports if a backtest fails partway through.
        """
        ctx = zmq.asyncio.Context()
        socket: zmq.asyncio.Socket | None = None
        try:
            socket = ctx.socket(zmq.PUB)
            socket.connect(self._local_xsub)
            await self._handshake(socket)
            await self._stream_candles(socket)
        finally:
            if socket is not None:
                socket.setsockopt(zmq.LINGER, 0)
                socket.close()
            ctx.term()
