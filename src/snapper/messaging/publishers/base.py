"""Base class for market data publisher services.

Provides common functionality for ZeroMQ-based publishers that stream
real-time market data from exchanges.
"""

import asyncio
import contextlib
import math
from abc import ABC
from abc import abstractmethod
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from time import monotonic
from typing import Any
from typing import Final
from typing import cast
from uuid import uuid7

import zmq
import zmq.asyncio
from loguru import logger
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.services.settings import SettingsService
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_service
from snapper.config.settings import get_settings_with_service
from snapper.core.types import AllExchange
from snapper.core.types import HealthStatusEnum
from snapper.core.types import MarketDataExchange
from snapper.core.types import MarketDataType
from snapper.core.types import MarketDataTypeEnum
from snapper.core.types import TradeSideEnum
from snapper.data.repository import Repository
from snapper.data.repository import get_repository
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import TickUpsertRow
from snapper.data.repository_types import TradeUpsertRow
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import TickerUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.symbols.functions import resolve_symbol_public_id
from snapper.infrastructure.symbols.mapper import SymbolMapperService
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import HWM_MARKET_DATA
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.data import CandleData
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.schemas.data import SettingChangedData
from snapper.messaging.schemas.data import TickData
from snapper.messaging.schemas.data import TradeData
from snapper.messaging.schemas.messages import MarketDataMessage
from snapper.messaging.topics.builders import heartbeat_topic_from_component
from snapper.messaging.topics.builders import market_topic
from snapper.utils.logging import set_log_context

_EXCHANGE_NOT_INIT_MSG = "Exchange client not initialized"
_REPO_NOT_INIT_MSG = "Repository not initialized"
_STREAM_END: Final = object()

_TICK_WRITE_QUEUE_MAX = 20_000
_TICK_WRITER_DROP_LOG_INTERVAL_S = 1.0
_TICK_WRITER_SHUTDOWN_POLL_S = 0.5
_tick_writer_drop_counters: dict[str, list[float]] = {}

_CANDLE_WRITE_QUEUE_MAX = 5_000
_CANDLE_WRITER_DROP_LOG_INTERVAL_S = 1.0
_CANDLE_WRITER_SHUTDOWN_POLL_S = 0.5
_candle_writer_drop_counters: dict[str, list[float]] = {}

_TRADE_WRITE_QUEUE_MAX = 5_000
_TRADE_WRITER_DROP_LOG_INTERVAL_S = 1.0
_TRADE_WRITER_SHUTDOWN_POLL_S = 0.5
_trade_writer_drop_counters: dict[str, list[float]] = {}


def _enqueue_or_drop_oldest_tick_write(
    queue: asyncio.Queue[TickUpsertRow], row: TickUpsertRow, label: str
) -> None:
    """Put a tick row on the writer queue, dropping the oldest if full.

    Distinct from the exchange-client-level drop helper because:

    1. Overflow here means "ZMQ subscribers already got the tick; the DB
       persistence path fell behind" — a different operator failure mode
       than the upstream WS-queue drop, so the log label and counter
       deliberately call out "persistence backlog".
    2. ``asyncio.Queue.join()`` requires one ``task_done()`` per
       successful ``put_nowait()``; the evicted row would otherwise leave
       an outstanding count and deadlock graceful shutdown. The helper
       calls ``task_done()`` for the evicted row to keep the join
       balance correct.

    Args:
        queue: Bounded writer queue.
        row: Tick row to enqueue for persistence.
        label: Human-readable label (typically the exchange name).
    """
    try:
        queue.put_nowait(row)
    except asyncio.QueueFull:
        counters = _tick_writer_drop_counters.setdefault(label, [0.0, 0.0])
        counters[0] += 1
        now = monotonic()
        if now - counters[1] >= _TICK_WRITER_DROP_LOG_INTERVAL_S:
            logger.warning(
                f"{label}:tick-writer queue full, dropped {int(counters[0])} rows "
                f"in last {now - counters[1]:.1f}s "
                f"(persistence backlog — ticks delivered to ZMQ subscribers but not persisted)"
            )
            counters[0] = 0.0
            counters[1] = now
        evicted = queue.get_nowait()
        queue.task_done()
        del evicted
        queue.put_nowait(row)


def _enqueue_or_drop_oldest_candle_write(
    queue: asyncio.Queue[CandleUpsertRow], row: CandleUpsertRow, label: str
) -> None:
    """Put a candle row on the writer queue, dropping the oldest if full.

    Mirrors :func:`_enqueue_or_drop_oldest_tick_write`; see that
    docstring for the rationale. The drop log carries a
    ``candle-writer`` label so operators can distinguish it from
    upstream WS-queue drops and from tick-writer drops.

    Args:
        queue: Bounded writer queue.
        row: Candle row to enqueue for persistence.
        label: Human-readable label (typically the exchange name).
    """
    try:
        queue.put_nowait(row)
    except asyncio.QueueFull:
        counters = _candle_writer_drop_counters.setdefault(label, [0.0, 0.0])
        counters[0] += 1
        now = monotonic()
        if now - counters[1] >= _CANDLE_WRITER_DROP_LOG_INTERVAL_S:
            logger.warning(
                f"{label}:candle-writer queue full, dropped {int(counters[0])} rows "
                f"in last {now - counters[1]:.1f}s "
                f"(persistence backlog — candles delivered to ZMQ subscribers but not persisted)"
            )
            counters[0] = 0.0
            counters[1] = now
        evicted = queue.get_nowait()
        queue.task_done()
        del evicted
        queue.put_nowait(row)


def _enqueue_or_drop_oldest_trade_write(
    queue: asyncio.Queue[TradeUpsertRow], row: TradeUpsertRow, label: str
) -> None:
    """Put a trade row on the writer queue, dropping the oldest if full.

    Mirrors :func:`_enqueue_or_drop_oldest_tick_write`; see that
    docstring for the rationale. The drop log carries a
    ``trade-writer`` label so operators can distinguish it from
    upstream WS-queue drops and from tick/candle-writer drops.

    Args:
        queue: Bounded writer queue.
        row: Trade row to enqueue for persistence.
        label: Human-readable label (typically the exchange name).
    """
    try:
        queue.put_nowait(row)
    except asyncio.QueueFull:
        counters = _trade_writer_drop_counters.setdefault(label, [0.0, 0.0])
        counters[0] += 1
        now = monotonic()
        if now - counters[1] >= _TRADE_WRITER_DROP_LOG_INTERVAL_S:
            logger.warning(
                f"{label}:trade-writer queue full, dropped {int(counters[0])} rows "
                f"in last {now - counters[1]:.1f}s "
                f"(persistence backlog — trades delivered to ZMQ subscribers but not persisted)"
            )
            counters[0] = 0.0
            counters[1] = now
        evicted = queue.get_nowait()
        queue.task_done()
        del evicted
        queue.put_nowait(row)


def _cleanup_pending_future(fut: asyncio.Future[Any] | None) -> None:
    """Cancel or consume a pending anext() future to avoid leaked task warnings.

    Must be called in the finally block of every producer loop so that
    pending futures do not produce 'Task exception was never retrieved'
    warnings when the exchange disconnects after shutdown.

    Args:
        fut: The persistent future from ensure_future(anext(iterator)),
             or None if already consumed.
    """
    if fut is None:
        return
    if not fut.done():
        fut.cancel()
    else:
        with contextlib.suppress(StopAsyncIteration, asyncio.CancelledError, Exception):
            fut.result()


class MarketDataPublisherService[T: ExchangeClientBase](RegisterableProcess, ABC):
    """Base service for publishing market data via ZMQ messaging."""

    def __init__(self, symbols: list[str]) -> None:
        """Initialize the instance.

        Args:
            symbols: List of trading symbols to subscribe to.
        """
        self.settings = get_settings()
        self._input_symbols = symbols
        self.symbols = self._validate_symbols(symbols)
        if not self.symbols:
            logger.warning(
                f"{self.__class__.__name__}: No valid symbols provided; "
                "publisher will idle until updated"
            )
        self.pub_endpoint = self.settings.zmq_broker_xsub
        self.context: zmq.asyncio.Context | None = None
        self.publisher: ValidatedPublisher | None = None
        self.msg_publisher: MessagePublisher | None = None
        self._tracker: SequenceTracker = SequenceTracker()
        self.subscriber: ValidatedSubscriber | None = None
        self.running = False
        self.heartbeat_seq = 0
        self._last_data_timestamps: dict[str, float] = {}
        self._unknown_symbols_logged: set[str] = set()
        self.repository: Repository | None = None
        self._instrument_cache: dict[str, str] = {}
        self._candle_id_cache: dict[tuple[str, str], tuple[datetime, str]] = {}
        self._exchange_client: T | None = None
        self._flush_errors: dict[str, int] = {"candle": 0, "tick": 0, "trade": 0}
        self._candle_batch_max_rows: int = 100
        self._tick_batch_max_rows: int = 500
        self._trade_batch_max_rows: int = 500
        self._batch_max_age_s: float = 0.05
        self._tick_write_queue: asyncio.Queue[TickUpsertRow] = asyncio.Queue(
            maxsize=_TICK_WRITE_QUEUE_MAX
        )
        self._tick_consumer_task: asyncio.Task[None] | None = None
        self._tick_writer_task: asyncio.Task[None] | None = None
        self._tick_writer_session: AsyncSession | None = None
        self._candle_write_queue: asyncio.Queue[CandleUpsertRow] = asyncio.Queue(
            maxsize=_CANDLE_WRITE_QUEUE_MAX
        )
        self._candle_consumer_tasks: list[asyncio.Task[None]] = []
        self._candle_writer_task: asyncio.Task[None] | None = None
        self._candle_writer_session: AsyncSession | None = None
        self._trade_write_queue: asyncio.Queue[TradeUpsertRow] = asyncio.Queue(
            maxsize=_TRADE_WRITE_QUEUE_MAX
        )
        self._trade_consumer_task: asyncio.Task[None] | None = None
        self._trade_writer_task: asyncio.Task[None] | None = None
        self._trade_writer_session: AsyncSession | None = None

    @abstractmethod
    def _create_exchange_client(self) -> T:
        """Create and return the exchange client instance.

        Returns:
            Configured exchange client for the specific exchange.
        """
        ...

    @abstractmethod
    def _get_exchange_name(self) -> AllExchange:
        """Return the name identifier for the exchange.

        Returns:
            Exchange name used in topics and logging.
        """
        ...

    @abstractmethod
    def _validate_symbols(self, symbols: list[str]) -> list[str]:
        """Validate and filter symbols for the exchange.

        Args:
            symbols: List of symbols to validate.

        Returns:
            List of valid symbols accepted by the exchange.
        """
        ...

    def _invalidate_symbol_cache(self) -> None:
        """Trigger cache invalidation for symbol mapper."""
        SymbolMapperService.get_instance().trigger_cache_invalidation(fail_fast=False)

    def _get_process_name(self) -> str:
        """Return the process name for log context.

        Override for multi-instance publishers (e.g. paper per-source).

        Returns:
            Process name string used in logging and heartbeats.
        """
        return f"pub:{self._get_exchange_name()}"

    async def start(self) -> None:
        """Start the publisher service and connect to exchange."""
        exchange_name = self._get_exchange_name()
        process_name = self._get_process_name()
        set_log_context(process_name)
        if self.running:
            logger.warning(f"{process_name}: Already running")
            return
        bootstrap_settings = get_settings()
        settings_service = await get_settings_service(
            bootstrap_settings.db_url,
            bootstrap_settings.zmq_broker_xsub,
        )
        self.settings = get_settings_with_service(settings_service)
        logger.info(f"{process_name}: AppSettings initialized with database access")
        self._candle_batch_max_rows = self.settings.write_buffer_candle_max_rows
        self._tick_batch_max_rows = self.settings.write_buffer_tick_max_rows
        self._trade_batch_max_rows = self.settings.write_buffer_trade_max_rows
        self._batch_max_age_s = self.settings.write_buffer_flush_ms / 1000.0
        self.repository = get_repository(self.settings.db_url)
        self._candle_id_cache = await self.repository.get_latest_candle_ids(as_of=datetime.now(UTC))
        logger.info(
            f"{process_name}: Loaded candle ID cache with {len(self._candle_id_cache)} entries"
        )
        max_symbols = self._get_max_symbols_per_connection()
        if len(self.symbols) > max_symbols > 0:
            logger.warning(
                f"{process_name} has {len(self.symbols)} symbols, but "
                f"{exchange_name} WebSocket limit is {max_symbols} symbols per connection. "
                f"Consider running multiple instances (first {max_symbols} symbols will be used)."
            )
        self.context = zmq.asyncio.Context()
        raw_pub_socket = self.context.socket(zmq.PUB)
        apply_hwm(raw_pub_socket, sndhwm=HWM_MARKET_DATA)
        raw_pub_socket.connect(self.pub_endpoint)
        self.publisher = ValidatedPublisher(raw_pub_socket)
        self.msg_publisher = MessagePublisher(self.publisher, self._tracker)
        logger.info(f"{process_name}: Connected to broker: {self.pub_endpoint}")
        raw_sub_socket = self.context.socket(zmq.SUB)
        raw_sub_socket.connect(self.settings.zmq_broker_xpub)
        self.subscriber = ValidatedSubscriber(raw_sub_socket)
        self.subscriber.subscribe("system.symbol_aliases")
        self.subscriber.subscribe("system.settings")
        logger.info(
            f"{process_name}: Subscribed to system.symbol_aliases, system.settings from "
            f"{self.settings.zmq_broker_xpub}"
        )
        self._exchange_client = self._create_exchange_client()
        await self._exchange_client.connect()
        logger.info(f"{process_name}: Exchange client connected (anonymous, public data)")
        self.running = True
        tasks: list[asyncio.Task[Any]] = []
        tasks.append(asyncio.create_task(self._heartbeat_loop()))
        tasks.append(asyncio.create_task(self._symbol_aliases_loop()))
        symbols_to_subscribe = self.symbols[:max_symbols] if max_symbols > 0 else self.symbols
        timeframes = self.settings.timeframes
        self._candle_consumer_tasks = [
            asyncio.create_task(self._candle_loop(symbols_to_subscribe, timeframe))
            for timeframe in timeframes
        ]
        tasks.extend(self._candle_consumer_tasks)
        self._candle_writer_task = asyncio.create_task(self._candle_writer_loop())
        tasks.append(self._candle_writer_task)
        self._tick_consumer_task = asyncio.create_task(self._tick_loop(symbols_to_subscribe))
        tasks.append(self._tick_consumer_task)
        self._tick_writer_task = asyncio.create_task(self._tick_writer_loop())
        tasks.append(self._tick_writer_task)
        if self._supports_public_trades():
            self._trade_consumer_task = asyncio.create_task(self._trade_loop(symbols_to_subscribe))
            tasks.append(self._trade_consumer_task)
            self._trade_writer_task = asyncio.create_task(self._trade_writer_loop())
            tasks.append(self._trade_writer_task)
        else:
            logger.info(f"{process_name}: Trade loop disabled (exchange has no public trade feed)")
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info(f"{process_name}: Tasks cancelled")
            raise

    async def stop(self) -> None:
        """Stop the publisher service and disconnect from exchange.

        Shutdown ordering — Codex+Copilot review 2026-05-11:

        1. Flip ``self.running = False`` so every loop sees the stop
           signal at its next checkpoint.
        2. Await the tick consumer task so no new rows enter the writer
           queue while we drain.
        3. ``await self._tick_write_queue.join()`` blocks until every
           queued row has been matched by a ``task_done()`` call —
           safe now because the consumer task is done, no new puts will
           happen, and the writer's drain guard keeps it running until
           the queue is empty.
        4. Await the writer task so the final batch flushes and the
           coroutine returns cleanly.
        5. Existing exchange/publisher/subscriber/context disposal as
           before.

        The early-return guard is keyed on ``running`` AND the absence
        of any writer task / non-empty queue so a partially-initialised
        publisher (e.g. start failure during setup) still gets a chance
        to drain.
        """
        has_pending_writer = (
            self._tick_writer_task is not None
            or (self._tick_write_queue is not None and not self._tick_write_queue.empty())
            or self._candle_writer_task is not None
            or (self._candle_write_queue is not None and not self._candle_write_queue.empty())
            or self._trade_writer_task is not None
            or (self._trade_write_queue is not None and not self._trade_write_queue.empty())
        )
        if not self.running and not has_pending_writer:
            return
        self.running = False
        if self._tick_consumer_task is not None:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._tick_consumer_task
            self._tick_consumer_task = None
        if self._tick_write_queue is not None:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._tick_write_queue.join()
        if self._tick_writer_task is not None:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._tick_writer_task
            self._tick_writer_task = None
        if self._candle_consumer_tasks:
            for candle_consumer_task in self._candle_consumer_tasks:
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await candle_consumer_task
            self._candle_consumer_tasks = []
        if self._candle_write_queue is not None:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._candle_write_queue.join()
        if self._candle_writer_task is not None:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._candle_writer_task
            self._candle_writer_task = None
        if self._trade_consumer_task is not None:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._trade_consumer_task
            self._trade_consumer_task = None
        if self._trade_write_queue is not None:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._trade_write_queue.join()
        if self._trade_writer_task is not None:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._trade_writer_task
            self._trade_writer_task = None
        if self._exchange_client:
            await self._exchange_client.disconnect()
            self._exchange_client = None
        if self.publisher:
            self.publisher.setsockopt(zmq.LINGER, 0)
            self.publisher.close()
        if self.subscriber:
            self.subscriber.setsockopt(zmq.LINGER, 0)
            self.subscriber.close()
        if self.context:
            self.context.term()
        exchange_name = self._get_exchange_name()
        logger.info(f"{exchange_name}_feed_publisher: Stopped")

    def _supports_public_trades(self) -> bool:
        """Return whether this exchange provides a public trade feed.

        Override to False for exchanges that do not expose trade tape
        (e.g. Walutomat). When False, the trade loop is not started.

        Returns:
            True if the exchange supports public trade subscriptions.
        """
        return True

    def _get_max_symbols_per_connection(self) -> int:
        """Return maximum symbols per WebSocket connection (0 = unlimited).

        Returns:
            Maximum number of symbols allowed per connection, or 0 for unlimited.
        """
        return 0

    def _get_heartbeat_component(self) -> str:
        """Return component name for heartbeat messages.

        Override for compound identities (e.g. paper.kraken).

        Returns:
            Component name string for heartbeat topic and data message.
        """
        return f"feed.{self._get_exchange_name()}"

    def _build_data_topic(
        self, symbol: str, data_type: MarketDataType, *, timeframe: str | None = None
    ) -> str:
        """Build market data topic string.

        Override for non-standard topic formats (e.g. paper with source_exchange).

        Args:
            symbol: Native symbol identifier.
            data_type: Market data type (candles, ticks, trades).
            timeframe: Optional candle timeframe.

        Returns:
            Fully-qualified ZMQ topic string.
        """
        exchange = self._get_exchange_name()
        return market_topic(exchange, symbol, data_type, timeframe=timeframe)

    def _get_data_exchange(self) -> MarketDataExchange:
        """Return exchange name for market data envelopes.

        Override when envelope exchange differs from topic exchange
        (e.g. paper publisher reports source exchange in payloads).

        Returns:
            Exchange name for CandleData/TickData/TradeData.
        """
        return cast(MarketDataExchange, self._get_exchange_name())

    async def _ensure_instrument(self, native_symbol: str) -> str | None:
        """Resolve instrument_public_id for a native symbol, using cache.

        Args:
            native_symbol: Native exchange symbol (e.g. 'BTC-USD').

        Returns:
            The instrument_public_id string, or None if the symbol cannot be
            split or has no active Symbol row.
        """
        cached = self._instrument_cache.get(native_symbol)
        if cached is not None:
            return cached
        assert self.repository is not None, _REPO_NOT_INIT_MSG
        now = datetime.now(UTC)
        symbol_pid = await resolve_symbol_public_id(self.repository, native_symbol, as_of=now)
        if symbol_pid is None:
            logger.warning(f"MarketDataPublisherService: No active Symbol row for {native_symbol}")
            return None
        _id, instrument_public_id = await self.repository.ensure_instrument(
            symbol_public_id=symbol_pid,
            exchange=self._get_exchange_name(),
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence("instruments"),
            timestamp=now,
        )
        self._instrument_cache[native_symbol] = instrument_public_id
        return instrument_public_id

    def _resolve_candle_public_id(
        self, instrument_public_id: str, timeframe: str, open_at: datetime
    ) -> str:
        """Resolve the public_id for a candle from the in-memory cache.

        If the cache holds a matching (instrument_public_id, timeframe) entry
        whose open_at equals the incoming value, the existing public_id is
        reused.  Otherwise a new UUID7 is generated and the cache is updated.

        Args:
            instrument_public_id: Instrument public identity string.
            timeframe: Candle timeframe (e.g. '1m').
            open_at: Candle interval start time.

        Returns:
            The public_id string to use for this candle on ZMQ and in the DB.
        """
        cache_key = (instrument_public_id, timeframe)
        cached = self._candle_id_cache.get(cache_key)
        if cached is not None and cached[0] == open_at:
            return cached[1]
        public_id = str(uuid7())
        self._candle_id_cache[cache_key] = (open_at, public_id)
        return public_id

    def _batch_age_remaining(
        self, batch_start: float | None, ev_loop: asyncio.AbstractEventLoop
    ) -> float:
        """Return seconds until the current batch should be flushed by age.

        Args:
            batch_start: Monotonic timestamp when the first row entered the
                batch, or None if the batch is empty.
            ev_loop: Running event loop (for monotonic clock).
        """
        if batch_start is None:
            return self._batch_max_age_s
        return max(0.0, self._batch_max_age_s - (ev_loop.time() - batch_start))

    @staticmethod
    def _track_batch_start(batch_start: float | None, ev_loop: asyncio.AbstractEventLoop) -> float:
        """Return existing batch_start or current loop time for a new batch.

        Args:
            batch_start: Current batch start timestamp, or None if empty.
            ev_loop: Running event loop (for monotonic clock).
        """
        if batch_start is None:
            return ev_loop.time()
        return batch_start

    async def _candle_loop(self, symbols: list[str], timeframe: str) -> None:
        """Subscribe to candle data, publish to ZMQ, and hand off DB rows to the writer.

        Ingest-only mirror of :meth:`_tick_loop` (HV2-H7 pattern):
        drains the exchange WS iterator, publishes to ZMQ via
        :meth:`_process_candle`, and enqueues the resulting row on
        :attr:`_candle_write_queue` for the dedicated writer task.
        Never awaits ``_flush_candle_batch`` — that responsibility
        lives entirely in :meth:`_candle_writer_loop` so SQLite
        commit latency cannot block the WS-queue consumer.

        Uses the candle ID cache to ensure the same public_id is
        used on ZMQ and in the database for a given (instrument,
        timeframe, open_at) window.

        Args:
            symbols: List of symbols to subscribe to for candle data.
            timeframe: Candle timeframe interval (e.g., '1m', '5m', '1h').
        """
        if not self._exchange_client:
            logger.error(_EXCHANGE_NOT_INIT_MSG)
            return
        exchange = self._get_data_exchange()
        exchange_label = self._get_exchange_name()
        iterator = self._exchange_client.subscribe_candles(symbols, timeframe).__aiter__()
        next_fut: asyncio.Future[CandleUpdate | object] | None = None
        try:
            while self.running:
                next_fut = next_fut or asyncio.ensure_future(anext(iterator, _STREAM_END))
                done, _ = await asyncio.wait({next_fut}, timeout=self._batch_max_age_s)
                if not done:
                    continue
                next_fut = None
                candle = done.pop().result()
                if candle is _STREAM_END:
                    break
                row = await self._process_candle(cast(CandleUpdate, candle), exchange, timeframe)
                if row is not None:
                    _enqueue_or_drop_oldest_candle_write(
                        self._candle_write_queue, row, exchange_label
                    )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Candle loop error for {symbols}: {e}")
        finally:
            _cleanup_pending_future(next_fut)

    async def _candle_writer_loop(self) -> None:
        """Drain :attr:`_candle_write_queue` and flush candles to the database.

        Mirror of :meth:`_tick_writer_loop` — see that docstring for the
        drain / ``task_done`` / ``timeout <= 0`` invariants. Candle
        volume is ~10-50× lower than ticks (one row per
        ``timeframe`` window per instrument), so the queue cap is
        ``_CANDLE_WRITE_QUEUE_MAX``.
        """
        batch: list[CandleUpsertRow] = []
        batch_start: float | None = None
        ev_loop = asyncio.get_event_loop()
        async with self._open_candle_writer_session() as writer_session:
            self._candle_writer_session = writer_session
            try:
                while self.running or not self._candle_write_queue.empty() or batch:
                    if not self.running and self._candle_write_queue.empty() and batch:
                        await self._flush_candle_writer_batch(batch)
                        batch_start = None
                        continue
                    timeout = self._batch_age_remaining(batch_start, ev_loop)
                    if timeout <= 0.0:
                        await self._flush_candle_writer_batch(batch)
                        batch_start = None
                        continue
                    try:
                        row = await asyncio.wait_for(
                            self._candle_write_queue.get(),
                            timeout=min(timeout, _CANDLE_WRITER_SHUTDOWN_POLL_S),
                        )
                    except TimeoutError:
                        if (
                            batch_start is None
                            or self._batch_age_remaining(batch_start, ev_loop) <= 0.0
                        ):
                            await self._flush_candle_writer_batch(batch)
                            batch_start = None
                        continue
                    batch.append(row)
                    batch_start = self._track_batch_start(batch_start, ev_loop)
                    if len(batch) >= self._candle_batch_max_rows:
                        await self._flush_candle_writer_batch(batch)
                        batch_start = None
            finally:
                self._candle_writer_session = None

    @contextlib.asynccontextmanager
    async def _open_candle_writer_session(self) -> AsyncIterator[AsyncSession | None]:
        """Open the candle writer task's long-lived database session.

        Mirror of :meth:`_open_tick_writer_session`. When the
        publisher has a real repository attached, yields a single
        ``AsyncSession`` reused across every candle flush so the
        per-flush connection-acquire cost amortises over the writer
        task's lifetime.

        Yields:
            ``AsyncSession`` bound to the repository's writer
            connection, or ``None`` when no repository is available.
        """
        if self.repository is None:
            yield None
            return
        async with self.repository.session() as session:
            yield session

    async def _flush_candle_writer_batch(self, batch: list[CandleUpsertRow]) -> None:
        """Flush the candle writer batch and balance the queue ``task_done`` debt.

        Mirror of :meth:`_flush_tick_writer_batch`. Captures
        ``flushed_count`` before awaiting the flush so a future
        mutation of ``batch`` during ``_flush_candle_batch`` cannot
        desync the ``task_done`` count.

        Args:
            batch: In-flight write batch. Cleared in place on a
                successful flush; left empty otherwise.
        """
        if not batch:
            return
        flushed_count = len(batch)
        await self._flush_candle_batch(batch)
        for _ in range(flushed_count):
            self._candle_write_queue.task_done()
        batch.clear()

    async def _process_candle(
        self,
        candle: CandleUpdate,
        exchange: MarketDataExchange,
        timeframe: str,
    ) -> CandleUpsertRow | None:
        """Build candle message, publish to ZMQ, and return a DB row.

        Publish-after-row: the row is computed first so that an
        instrument-resolution failure cannot waste a ZMQ publish on
        a candle that won't be persisted. Returns ``None`` when
        instrument resolution fails (unknown symbol) — ZMQ is then
        skipped too because no callers (frontend / strategy / backtest)
        can correlate an unknown-instrument candle to a Snapper
        Symbol downstream.

        Args:
            candle: Raw candle update from exchange client.
            exchange: Exchange name for message provenance.
            timeframe: Candle timeframe interval.

        Returns:
            ``CandleUpsertRow`` ready for the writer queue, or
            ``None`` when the instrument could not be resolved.
        """
        native_symbol = candle.symbol
        instrument_public_id = await self._ensure_instrument(native_symbol)
        if instrument_public_id is None:
            return None
        public_id = self._resolve_candle_public_id(
            instrument_public_id, timeframe, candle.interval_begin
        )
        topic = self._build_data_topic(
            native_symbol, MarketDataTypeEnum.CANDLES, timeframe=timeframe
        )
        received_at = datetime.now(UTC)
        candle_msg = CandleData(
            public_id=public_id,
            timestamp=received_at,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence(topic),
            exchange=exchange,
            instrument=native_symbol,
            volume=candle.volume,
            timeframe=timeframe,
            open_at=candle.interval_begin,
            open=candle.open,
            high=candle.high,
            low=candle.low,
            close=candle.close,
            vwap=candle.vwap,
            trades=candle.trades,
        )
        row = self._build_candle_row(candle_msg, instrument_public_id)
        await self._publish_message(topic, candle_msg)
        self._last_data_timestamps[native_symbol] = received_at.timestamp() * 1000
        return row

    async def _tick_loop(self, symbols: list[str]) -> None:
        """Subscribe to tick data, publish to ZMQ, and hand off DB rows to the writer.

        Ingest-only: drains the exchange WS queue, publishes to ZMQ via
        ``_process_tick``, and enqueues the resulting row on
        :attr:`_tick_write_queue` for the dedicated writer task. Never
        awaits ``_flush_tick_batch`` — that responsibility lives entirely
        in :meth:`_tick_writer_loop` so SQLite commit latency cannot
        block the WS-queue consumer.

        Args:
            symbols: List of symbols to subscribe to for tick data.
        """
        if not self._exchange_client:
            logger.error(_EXCHANGE_NOT_INIT_MSG)
            return
        exchange = self._get_data_exchange()
        exchange_label = self._get_exchange_name()
        iterator = self._exchange_client.subscribe_ticks(symbols).__aiter__()
        next_fut: asyncio.Future[TickerUpdate | object] | None = None
        try:
            while self.running:
                next_fut = next_fut or asyncio.ensure_future(anext(iterator, _STREAM_END))
                done, _ = await asyncio.wait({next_fut}, timeout=self._batch_max_age_s)
                if not done:
                    continue
                next_fut = None
                message = done.pop().result()
                if message is _STREAM_END:
                    break
                row = await self._process_tick(cast(TickerUpdate, message), exchange)
                if row is not None:
                    _enqueue_or_drop_oldest_tick_write(self._tick_write_queue, row, exchange_label)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Tick loop error for {symbols}: {e}")
        finally:
            _cleanup_pending_future(next_fut)

    async def _tick_writer_loop(self) -> None:
        """Drain :attr:`_tick_write_queue` and flush ticks to the database.

        Drain semantics: the loop continues as long as the publisher is
        running OR the write queue still has items OR an in-flight batch
        hasn't been flushed yet. This guarantees a graceful shutdown
        cannot leave persisted-but-not-flushed rows in memory.

        ``task_done()`` accounting: every successful ``put`` on
        :attr:`_tick_write_queue` must be matched by exactly one
        ``task_done()`` call somewhere downstream — either at flush time
        (one per row in the batch) or at drop-oldest time (one per
        evicted row, handled by :func:`_enqueue_or_drop_oldest_tick_write`).
        Without this, :meth:`asyncio.Queue.join` in :meth:`stop` would
        deadlock.

        ``timeout <= 0`` branch: :meth:`_batch_age_remaining` legitimately
        returns ``0.0`` when the batch is already past its age budget.
        ``asyncio.wait_for(get(), timeout=0.0)`` would raise
        ``TimeoutError`` even with items queued, so the loop flushes the
        current batch immediately and `continue`s instead.
        """
        batch: list[TickUpsertRow] = []
        batch_start: float | None = None
        ev_loop = asyncio.get_event_loop()
        async with self._open_tick_writer_session() as writer_session:
            self._tick_writer_session = writer_session
            try:
                while self.running or not self._tick_write_queue.empty() or batch:
                    if not self.running and self._tick_write_queue.empty() and batch:
                        await self._flush_tick_writer_batch(batch)
                        batch_start = None
                        continue
                    timeout = self._batch_age_remaining(batch_start, ev_loop)
                    if timeout <= 0.0:
                        await self._flush_tick_writer_batch(batch)
                        batch_start = None
                        continue
                    try:
                        row = await asyncio.wait_for(
                            self._tick_write_queue.get(),
                            timeout=min(timeout, _TICK_WRITER_SHUTDOWN_POLL_S),
                        )
                    except TimeoutError:
                        if (
                            batch_start is None
                            or self._batch_age_remaining(batch_start, ev_loop) <= 0.0
                        ):
                            await self._flush_tick_writer_batch(batch)
                            batch_start = None
                        continue
                    batch.append(row)
                    batch_start = self._track_batch_start(batch_start, ev_loop)
                    if len(batch) >= self._tick_batch_max_rows:
                        await self._flush_tick_writer_batch(batch)
                        batch_start = None
            finally:
                self._tick_writer_session = None

    @contextlib.asynccontextmanager
    async def _open_tick_writer_session(self) -> AsyncIterator[AsyncSession | None]:
        """Open the writer task's long-lived database session.

        When the publisher has a real repository attached (production
        path), yields an ``AsyncSession`` opened from
        ``self.repository.session()`` that the writer reuses across every
        flush. Holding one session for the writer's lifetime amortises
        the SQLite per-flush connection-acquire cost — under ``NullPool``
        a fresh session opens a new connection every time, which
        dominated the publisher's CPU once Core bulk insert eliminated
        the ORM overhead (Codex 2026-05-11 post-HV2-H7 review).

        When the repository is absent (tests / partial setup) yields
        ``None``; ``_flush_tick_batch`` then falls back to the no-session
        ``repository.upsert_ticks(batch)`` path, which is still correct
        but commits per flush instead of per writer-lifetime.

        Yields:
            ``AsyncSession`` bound to the repository's writer connection,
            or ``None`` when no repository is available.
        """
        if self.repository is None:
            yield None
            return
        async with self.repository.session() as session:
            yield session

    async def _flush_tick_writer_batch(self, batch: list[TickUpsertRow]) -> None:
        """Flush the writer batch and balance the queue's ``task_done`` debt.

        Extracted from :meth:`_tick_writer_loop` so the same flush +
        accounting sequence is reused by all four exit branches (shutdown
        drain, age-expired, get-timeout, size-trigger) without
        duplicating the loop body.

        Captures ``flushed_count`` **before** awaiting the flush so a
        future mutation of ``batch`` during ``_flush_tick_batch`` cannot
        desync the ``task_done`` count (Codex reviewer guardrail).

        Args:
            batch: In-flight write batch. Cleared in place on a
                successful flush; left empty otherwise. Callers reset
                their own ``batch_start`` cursor after this returns.
        """
        if not batch:
            return
        flushed_count = len(batch)
        await self._flush_tick_batch(batch)
        for _ in range(flushed_count):
            self._tick_write_queue.task_done()
        batch.clear()

    async def _process_tick(
        self, message: TickerUpdate, exchange: MarketDataExchange
    ) -> TickUpsertRow | None:
        """Build tick message, publish to ZMQ, and return a DB row.

        Publish-first: ZMQ delivery happens before instrument resolution.
        Returns None when instrument is unknown (ZMQ still delivered).

        Args:
            message: Raw ticker update from exchange client.
            exchange: Exchange name for message provenance.
        """
        native_symbol = message.symbol
        topic = self._build_data_topic(native_symbol, MarketDataTypeEnum.TICKS)
        received_at = datetime.now(UTC)
        tick_msg = TickData(
            public_id=str(uuid7()),
            timestamp=received_at,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence(topic),
            exchange=exchange,
            instrument=native_symbol,
            volume=message.volume,
            bid=message.bid if not math.isclose(message.bid, 0.0) else None,
            ask=message.ask if not math.isclose(message.ask, 0.0) else None,
            last=message.last,
            is_delayed=message.is_delayed,
            is_extended_hours=message.is_extended_hours,
        )
        await self._publish_message(topic, tick_msg)
        self._last_data_timestamps[native_symbol] = received_at.timestamp() * 1000
        instrument_public_id = await self._ensure_instrument(native_symbol)
        if instrument_public_id is None:
            return None
        return self._build_tick_row(tick_msg, instrument_public_id)

    async def _trade_loop(self, symbols: list[str]) -> None:
        """Subscribe to trade data, publish to ZMQ, and hand off DB rows to the writer.

        Ingest-only mirror of :meth:`_tick_loop` (HV2-H7 pattern):
        drains the exchange WS iterator, publishes to ZMQ via
        :meth:`_process_trade`, and enqueues the resulting row on
        :attr:`_trade_write_queue` for the dedicated writer task.
        Never awaits ``_flush_trade_batch`` — that responsibility
        lives entirely in :meth:`_trade_writer_loop` so SQLite
        commit latency cannot block the WS-queue consumer.

        Args:
            symbols: List of symbols to subscribe to for trade data.
        """
        if not self._exchange_client:
            logger.error(_EXCHANGE_NOT_INIT_MSG)
            return
        exchange = self._get_data_exchange()
        exchange_label = self._get_exchange_name()
        iterator = self._exchange_client.subscribe_trades(symbols).__aiter__()
        next_fut: asyncio.Future[TradeUpdate | object] | None = None
        try:
            while self.running:
                next_fut = next_fut or asyncio.ensure_future(anext(iterator, _STREAM_END))
                done, _ = await asyncio.wait({next_fut}, timeout=self._batch_max_age_s)
                if not done:
                    continue
                next_fut = None
                trade = done.pop().result()
                if trade is _STREAM_END:
                    break
                row = await self._process_trade(cast(TradeUpdate, trade), exchange)
                if row is not None:
                    _enqueue_or_drop_oldest_trade_write(
                        self._trade_write_queue, row, exchange_label
                    )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Trade loop error for {symbols}: {e}")
        finally:
            _cleanup_pending_future(next_fut)

    async def _trade_writer_loop(self) -> None:
        """Drain :attr:`_trade_write_queue` and flush trades to the database.

        Mirror of :meth:`_tick_writer_loop` — see that docstring for the
        drain / ``task_done`` / ``timeout <= 0`` invariants. Trade
        volume is ~10-50× lower than ticks (one row per executed
        trade reported by the exchange), so the queue cap is
        ``_TRADE_WRITE_QUEUE_MAX``.
        """
        batch: list[TradeUpsertRow] = []
        batch_start: float | None = None
        ev_loop = asyncio.get_event_loop()
        async with self._open_trade_writer_session() as writer_session:
            self._trade_writer_session = writer_session
            try:
                while self.running or not self._trade_write_queue.empty() or batch:
                    if not self.running and self._trade_write_queue.empty() and batch:
                        await self._flush_trade_writer_batch(batch)
                        batch_start = None
                        continue
                    timeout = self._batch_age_remaining(batch_start, ev_loop)
                    if timeout <= 0.0:
                        await self._flush_trade_writer_batch(batch)
                        batch_start = None
                        continue
                    try:
                        row = await asyncio.wait_for(
                            self._trade_write_queue.get(),
                            timeout=min(timeout, _TRADE_WRITER_SHUTDOWN_POLL_S),
                        )
                    except TimeoutError:
                        if (
                            batch_start is None
                            or self._batch_age_remaining(batch_start, ev_loop) <= 0.0
                        ):
                            await self._flush_trade_writer_batch(batch)
                            batch_start = None
                        continue
                    batch.append(row)
                    batch_start = self._track_batch_start(batch_start, ev_loop)
                    if len(batch) >= self._trade_batch_max_rows:
                        await self._flush_trade_writer_batch(batch)
                        batch_start = None
            finally:
                self._trade_writer_session = None

    @contextlib.asynccontextmanager
    async def _open_trade_writer_session(self) -> AsyncIterator[AsyncSession | None]:
        """Open the trade writer task's long-lived database session.

        Mirror of :meth:`_open_tick_writer_session`. When the
        publisher has a real repository attached, yields a single
        ``AsyncSession`` reused across every trade flush so the
        per-flush connection-acquire cost amortises over the writer
        task's lifetime.

        Yields:
            ``AsyncSession`` bound to the repository's writer
            connection, or ``None`` when no repository is available.
        """
        if self.repository is None:
            yield None
            return
        async with self.repository.session() as session:
            yield session

    async def _flush_trade_writer_batch(self, batch: list[TradeUpsertRow]) -> None:
        """Flush the trade writer batch and balance the queue ``task_done`` debt.

        Mirror of :meth:`_flush_tick_writer_batch`. Captures
        ``flushed_count`` before awaiting the flush so a future
        mutation of ``batch`` during ``_flush_trade_batch`` cannot
        desync the ``task_done`` count.

        Args:
            batch: In-flight write batch. Cleared in place on a
                successful flush; left empty otherwise.
        """
        if not batch:
            return
        flushed_count = len(batch)
        await self._flush_trade_batch(batch)
        for _ in range(flushed_count):
            self._trade_write_queue.task_done()
        batch.clear()

    async def _process_trade(
        self, trade: TradeUpdate, exchange: MarketDataExchange
    ) -> TradeUpsertRow | None:
        """Build trade message, publish to ZMQ, and return a DB row.

        Publish-first: ZMQ delivery happens before instrument resolution.
        Returns None when instrument is unknown (ZMQ still delivered).

        Args:
            trade: Raw trade update from exchange client.
            exchange: Exchange name for message provenance.
        """
        native_symbol = trade.symbol
        topic = self._build_data_topic(native_symbol, MarketDataTypeEnum.TRADES)
        received_at = datetime.now(UTC)
        trade_msg = TradeData(
            public_id=str(uuid7()),
            timestamp=received_at,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence(topic),
            exchange=exchange,
            instrument=native_symbol,
            executed_at=trade.timestamp,
            price=trade.price,
            volume=trade.quantity,
            side=trade.side if trade.side in (TradeSideEnum.BUY, TradeSideEnum.SELL) else None,
            trade_id=trade.trade_id,
        )
        await self._publish_message(topic, trade_msg)
        self._last_data_timestamps[native_symbol] = received_at.timestamp() * 1000
        instrument_public_id = await self._ensure_instrument(native_symbol)
        if instrument_public_id is None:
            return None
        return self._build_trade_row(trade_msg, instrument_public_id)

    async def _publish_message(
        self, topic: str, message: MarketDataMessage
    ) -> MarketDataMessage | None:
        """Send a complete market data message to ZMQ topic.

        Args:
            topic: ZMQ topic string for message routing.
            message: Complete market data message with provenance set.

        Returns:
            The message if sent, or None if not published.
        """
        if not self.msg_publisher or not self.running:
            return None
        try:
            await self.msg_publisher.send(topic, message)
            logger.debug(f"Published {message.type} for {topic}")
            return message
        except Exception as e:
            if topic in self._unknown_symbols_logged:
                logger.debug(f"Error publishing message (already-logged topic '{topic}'): {e}")
            else:
                logger.error(f"Error publishing message: {e}")
                self._unknown_symbols_logged.add(topic)
            return None

    async def _heartbeat_loop(self) -> None:
        """Periodically publish heartbeat messages with status."""
        component_name = self._get_heartbeat_component()
        while self.running:
            try:
                await asyncio.sleep(self.settings.zmq_heartbeat_interval_ms / 1000.0)
                if not self.running:
                    break
                self.heartbeat_seq += 1
                current_time = datetime.now(UTC).timestamp() * 1000
                max_lag_ms = 0
                for symbol in self.symbols:
                    last_data_time = self._last_data_timestamps.get(symbol, current_time)
                    lag_ms = int(current_time - last_data_time)
                    max_lag_ms = max(max_lag_ms, lag_ms)
                hb_topic = heartbeat_topic_from_component(component_name)
                hb_msg = HeartbeatData(
                    public_id=str(uuid7()),
                    timestamp=datetime.now(UTC),
                    session_id=self._tracker.session_id,
                    sequence_id=self._tracker.next_sequence(hb_topic),
                    component=component_name,
                    sequence=self.heartbeat_seq,
                    status=(
                        HealthStatusEnum.WARNING
                        if any(self._flush_errors.values())
                        else HealthStatusEnum.HEALTHY
                    ),
                    lag_ms=max_lag_ms,
                    meta={
                        "symbols": list(self.symbols),
                        "symbol_count": len(self.symbols),
                        "running": self.running,
                    },
                )
                await self._publish_heartbeat(hb_topic, hb_msg)
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")

    async def _publish_heartbeat(self, topic: str, message: HeartbeatData) -> None:
        """Send a complete heartbeat message to ZMQ.

        Args:
            topic: Heartbeat ZMQ topic string.
            message: Complete HeartbeatData with provenance set.
        """
        if not self.msg_publisher or not self.running:
            return
        try:
            await self.msg_publisher.send(topic, message)
        except Exception as e:
            logger.error(f"Error publishing heartbeat: {e}")

    def _build_candle_row(
        self, candle_msg: CandleData, instrument_public_id: str
    ) -> CandleUpsertRow:
        """Build a CandleUpsertRow from a published CandleData message.

        Args:
            candle_msg: Published CandleData with OHLCV fields.
            instrument_public_id: Resolved instrument identity.

        Returns:
            Fully materialized row dict ready for repository upsert.
        """
        return {
            "public_id": candle_msg.public_id,
            "instrument_public_id": instrument_public_id,
            "open_at": candle_msg.open_at,
            "timestamp": candle_msg.timestamp,
            "timeframe": candle_msg.timeframe or "1m",
            "open": candle_msg.open if candle_msg.open is not None else candle_msg.close,
            "high": candle_msg.high if candle_msg.high is not None else candle_msg.close,
            "low": candle_msg.low if candle_msg.low is not None else candle_msg.close,
            "close": candle_msg.close if candle_msg.close is not None else 0.0,
            "volume": float(candle_msg.volume),
            "vwap": candle_msg.vwap if candle_msg.vwap is not None else candle_msg.close,
            "trades": candle_msg.trades if candle_msg.trades is not None else 0,
            "session_id": candle_msg.session_id,
            "sequence_id": candle_msg.sequence_id,
        }

    def _build_tick_row(self, tick_msg: TickData, instrument_public_id: str) -> TickUpsertRow:
        """Build a TickUpsertRow from a published TickData message.

        Args:
            tick_msg: Published TickData with bid/ask/last/volume.
            instrument_public_id: Resolved instrument identity.

        Returns:
            Fully materialized row dict ready for repository upsert.
        """
        return {
            "public_id": tick_msg.public_id,
            "instrument_public_id": instrument_public_id,
            "timestamp": tick_msg.timestamp,
            "bid": tick_msg.bid,
            "ask": tick_msg.ask,
            "last": tick_msg.last,
            "volume": tick_msg.volume,
            "session_id": tick_msg.session_id,
            "sequence_id": tick_msg.sequence_id,
        }

    def _build_trade_row(self, trade_msg: TradeData, instrument_public_id: str) -> TradeUpsertRow:
        """Build a TradeUpsertRow from a published TradeData message.

        Args:
            trade_msg: Published TradeData with price/volume/side.
            instrument_public_id: Resolved instrument identity.

        Returns:
            Fully materialized row dict ready for repository upsert.
        """
        return {
            "public_id": trade_msg.public_id,
            "instrument_public_id": instrument_public_id,
            "timestamp": trade_msg.timestamp,
            "executed_at": trade_msg.executed_at,
            "price": trade_msg.price,
            "size": trade_msg.volume,
            "side": trade_msg.side or "",
            "trade_id": trade_msg.trade_id,
            "session_id": trade_msg.session_id,
            "sequence_id": trade_msg.sequence_id,
        }

    async def _flush_candle_batch(self, batch: list[CandleUpsertRow]) -> None:
        """Flush candle batch to DB. On IntegrityError, retry row-by-row.

        Args:
            batch: List of candle rows to persist.
        """
        if not batch:
            return
        assert self.repository is not None, _REPO_NOT_INIT_MSG
        try:
            await self.repository.upsert_candles(batch)
            self._flush_errors["candle"] = 0
        except IntegrityError:
            await self._flush_candle_batch_row_by_row(batch)
        except Exception as e:
            self._flush_errors["candle"] += 1
            logger.error(f"Candle batch flush failed ({len(batch)} rows): {e}")

    async def _flush_candle_batch_row_by_row(self, batch: list[CandleUpsertRow]) -> None:
        """Fallback: retry each candle individually to isolate bad rows.

        Args:
            batch: List of candle rows where batch upsert failed.
        """
        assert self.repository is not None, _REPO_NOT_INIT_MSG
        had_generic_error = False
        for row in batch:
            try:
                await self.repository.upsert_candles([row])
            except IntegrityError as exc:
                logger.warning(
                    f"Candle upsert skipped: instrument={row.get('instrument_public_id')}, "
                    f"open_at={row.get('open_at')}, error={exc}"
                )
            except Exception as e:
                had_generic_error = True
                self._flush_errors["candle"] += 1
                logger.error(f"Candle row flush failed: {e}")
        if not had_generic_error:
            self._flush_errors["candle"] = 0

    async def _flush_tick_batch(self, batch: list[TickUpsertRow]) -> None:
        """Flush tick batch to DB (append-only, no conflict possible).

        Uses the writer task's long-lived session when present so the
        per-flush cost is just BEGIN/INSERT/COMMIT on a held connection
        instead of opening a new one through ``NullPool`` every time.
        Falls back to the repository's own-session path when called
        outside the writer loop (tests, ad-hoc flushes).

        Args:
            batch: List of tick rows to persist.
        """
        if not batch:
            return
        assert self.repository is not None, _REPO_NOT_INIT_MSG
        try:
            writer_session = self._tick_writer_session
            if writer_session is not None:
                await self.repository.upsert_ticks(batch, session=writer_session)
                await writer_session.commit()
            else:
                await self.repository.upsert_ticks(batch)
            self._flush_errors["tick"] = 0
        except Exception as e:
            self._flush_errors["tick"] += 1
            logger.error(f"Tick batch flush failed ({len(batch)} rows): {e}")
            writer_session = self._tick_writer_session
            if writer_session is not None:
                with contextlib.suppress(Exception):
                    await writer_session.rollback()

    async def _flush_trade_batch(self, batch: list[TradeUpsertRow]) -> None:
        """Flush trade batch to DB (ON CONFLICT DO NOTHING).

        Args:
            batch: List of trade rows to persist.
        """
        if not batch:
            return
        assert self.repository is not None, _REPO_NOT_INIT_MSG
        try:
            await self.repository.upsert_trades(batch)
            self._flush_errors["trade"] = 0
        except Exception as e:
            self._flush_errors["trade"] += 1
            logger.error(f"Trade batch flush failed ({len(batch)} rows): {e}")

    async def _symbol_aliases_loop(self) -> None:
        """Listen for system messages and handle cache invalidation."""
        if not self.subscriber:
            logger.error("MarketDataPublisherService: No subscriber for system messages listener")
            return
        exchange = self._get_exchange_name()
        logger.info(f"{exchange}_feed_publisher: Starting system messages listener")
        try:
            while self.running:
                try:
                    async with asyncio.timeout(1.0):
                        topic, payload = await self.subscriber.recv_multipart()
                    if topic == "system.symbol_aliases":
                        logger.info(
                            f"{exchange}_feed_publisher: Received symbol_aliases update, "
                            "refreshing cache"
                        )
                        self._invalidate_symbol_cache()
                    elif topic == "system.settings":
                        self._handle_settings_update(payload, exchange)
                except TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"{exchange}_feed_publisher system messages listener error: {e}")
                    await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"{exchange}_feed_publisher system messages loop crashed: {e}")

    def _handle_settings_update(self, payload: bytes, exchange: str) -> None:
        """Handle settings update message and refresh cached settings.

        Args:
            payload: Raw bytes containing the settings change data.
            exchange: Exchange name for logging context.
        """
        try:
            envelope = SettingChangedData.from_json(payload.decode("utf-8"))
            settings_service = SettingsService.get_instance()
            if settings_service:
                parsed_value = settings_service._parse_value(envelope.value)
                settings_service._cache[envelope.key] = parsed_value
                logger.info(
                    f"{exchange}_feed_publisher: Setting {envelope.key} updated via ZMQ event"
                )
        except Exception as e:
            logger.error(f"{exchange}_feed_publisher: Error handling settings update: {e}")

    def get_status(self) -> dict[str, Any]:
        """Return the current status of the publisher service.

        Returns:
            Dictionary containing running state, symbols, endpoint, and metrics.
        """
        return {
            "running": self.running,
            "symbols": self.symbols,
            "broker_endpoint": self.pub_endpoint,
            "heartbeat_seq": self.heartbeat_seq,
            "exchange": self._get_exchange_name(),
            "flush_errors": dict(self._flush_errors),
        }
